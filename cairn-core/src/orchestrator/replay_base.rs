//! Which branch a branch is replayed onto, once the base it recorded may be gone.
//!
//! `jobs.base_branch` holds a NAME, and a name outlives the thing it names. A
//! child issue records its parent's integration branch there; when the parent
//! merges, that branch is deleted, and from then on every surface that asks
//! where the child stands relative to its base gets a resolution failure instead
//! of an answer — including the `cairn:~/rebase` replay, the one action that
//! could repair it. Refusing there is the worst available reading of the
//! evidence: a deleted base is not an unknown, it is a base that has been folded
//! into something this branch is still supposed to land on.
//!
//! So a recorded base the store cannot produce falls through to where the work
//! must actually merge: the target of its open pull request, then the project's
//! default branch. The ordering is not a guess. GitHub retargets an open pull
//! request onto the merged-into branch when its base is deleted, so the pull
//! request's target is the most specific surviving statement of where this work
//! is going, and the project default is the floor beneath it.
//!
//! A base that fails to RESOLVE is a different fact from one that is absent, and
//! it stops the search dead. A conflicted bookmark name resolves to several
//! commits at once, and a broken store resolves nothing at all; in neither case
//! is anything known about where this branch stands, and moving a branch's
//! ancestry on the strength of an unknown is the damage this module exists to
//! avoid.
//!
//! That rule covers the fallbacks, not just the recorded base. The fallback is
//! the branch the work would actually be moved ONTO, so passing over one the
//! store cannot pin down — in favour of something less specific that happens to
//! answer — is the same guess made about the destination itself.

use std::fmt;
use std::path::Path;

use cairn_db::turso::params;

use crate::jj::JjEnv;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Where a resolved base came from, so a caller can say why it is landing there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BaseSource {
    /// The branch the job (or its open conflict session) recorded.
    Recorded,
    /// The base ref this branch's open pull request targets.
    PullRequestTarget,
    /// The project's default branch.
    ProjectDefault,
}

impl BaseSource {
    /// How the branch is described where it is named to an agent.
    pub(crate) fn describe(self) -> &'static str {
        match self {
            BaseSource::Recorded => "the base this branch recorded",
            BaseSource::PullRequestTarget => "the branch its pull request targets",
            BaseSource::ProjectDefault => "the project's default branch",
        }
    }
}

/// The branch a replay lands on, and the commit it resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBase {
    pub(crate) branch: String,
    pub(crate) commit: String,
    pub(crate) source: BaseSource,
    /// The recorded base this resolution had to move past, present exactly when
    /// that recorded name no longer names anything in the store.
    pub(crate) superseded: Option<String>,
}

/// Why no base could be named. Each variant is a distinct situation with a
/// distinct remedy, which is the point: the single collapsed "did not resolve"
/// these replaced was unactionable precisely because it covered all three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BaseResolutionError {
    /// The job never recorded a base branch, and no candidate stood in for one.
    NothingRecorded { branch: String },
    /// A candidate is there and the store cannot say where — a conflicted
    /// bookmark name, or a store that is not answering. Nothing is known, so
    /// nothing is substituted and no further candidate is tried.
    ///
    /// This covers the fallbacks as well as the recorded base, and deliberately
    /// so: the fallback is the branch the work would actually be moved ONTO, so
    /// skipping one whose state is unknown in favour of a less specific
    /// destination is the same guess the recorded-base refusal exists to
    /// prevent, made about the destination itself.
    Unresolvable {
        branch: String,
        /// The name whose resolution failed.
        candidate: String,
        /// Where that name came from, which is what makes the refusal legible:
        /// a recorded base that cannot be pinned is a different report from a
        /// fallback that cannot be pinned.
        source: BaseSource,
        diagnostic: String,
    },
    /// The recorded base is gone and nothing survives to replace it.
    NoSurvivingBase {
        branch: String,
        recorded: String,
        tried: Vec<String>,
    },
}

impl fmt::Display for BaseResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaseResolutionError::NothingRecorded { branch } => write!(
                f,
                "`{branch}` has no base branch recorded, so there is nothing to replay it onto."
            ),
            BaseResolutionError::Unresolvable {
                branch,
                candidate,
                source: BaseSource::Recorded,
                diagnostic,
            } => write!(
                f,
                "`{branch}` records the base `{candidate}`, and the store could not say which \
                 commit that names: {diagnostic}. A name resolving to several commits at once is \
                 the usual cause. Nothing was substituted for it, because a branch whose base is \
                 unknown must not be moved onto a base someone guessed."
            ),
            BaseResolutionError::Unresolvable {
                branch,
                candidate,
                source,
                diagnostic,
            } => write!(
                f,
                "`{branch}` records a base that no longer exists in the store, so it would fall \
                 back to `{candidate}`, {} — and the store could not say which commit THAT names \
                 either: {diagnostic}. A name resolving to several commits at once is the usual \
                 cause. Nothing beyond it was tried: the fallback exists to find where this work \
                 is still going, not to keep asking until some branch answers.",
                source.describe()
            ),
            BaseResolutionError::NoSurvivingBase {
                branch,
                recorded,
                tried,
            } => {
                write!(
                    f,
                    "`{branch}` records the base `{recorded}`, which no longer exists in the \
                     store — the usual cause is that its parent merged and the branch was \
                     deleted. "
                )?;
                if tried.is_empty() {
                    write!(
                        f,
                        "Nothing else names where this work is going: it has no open pull request \
                         and its project records no default branch."
                    )
                } else {
                    write!(
                        f,
                        "The branches that would stand in for it do not resolve either ({}), so \
                         there is nowhere to replay it onto.",
                        tried
                            .iter()
                            .map(|name| format!("`{name}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
        }
    }
}

/// Every branch that could be this job's base, newest statement first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BaseCandidates {
    /// What the job recorded — or, when a conflict session is open, the base
    /// that session was opened against.
    pub(crate) recorded: Option<String>,
    /// The base ref this branch's open pull request targets.
    pub(crate) pull_request_target: Option<String>,
    /// The project's default branch.
    pub(crate) project_default: Option<String>,
}

impl BaseCandidates {
    /// Override the recorded base with the one an open conflict session named.
    /// A session's base is the more specific fact: it is the branch the rolled
    /// back rebase was actually aimed at.
    pub(crate) fn recorded_from_session(mut self, session_base: Option<&str>) -> Self {
        if let Some(base) = session_base.filter(|base| !base.is_empty()) {
            self.recorded = Some(base.to_string());
        }
        self
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

/// Read the base candidates for one job.
///
/// The pull request is filtered to an OPEN one: a merged or closed pull request
/// states where work already went, not where this branch is going.
pub(crate) async fn load_base_candidates(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<BaseCandidates> {
    let mut rows = conn
        .query(
            "SELECT j.base_branch,
                    p.default_branch,
                    (SELECT mr.target_branch
                       FROM merge_requests mr
                      WHERE mr.project_id = j.project_id
                        AND mr.source_branch = j.branch
                        AND mr.status NOT IN ('merged', 'closed')
                      ORDER BY mr.opened_at DESC
                      LIMIT 1)
             FROM jobs j JOIN projects p ON p.id = j.project_id
             WHERE j.id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(BaseCandidates::default());
    };
    Ok(BaseCandidates {
        recorded: non_empty(row.opt_text(0)?),
        project_default: non_empty(row.opt_text(1)?),
        pull_request_target: non_empty(row.opt_text(2)?),
    })
}

/// [`load_base_candidates`] for a caller holding the database rather than a
/// connection.
pub(crate) async fn load_base_candidates_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<BaseCandidates, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move { load_base_candidates(conn, &job_id).await })
    })
    .await
    .map_err(|error| format!("load replay base candidates: {error}"))
}

/// Resolve one branch name to a single commit, keeping "absent" (`Ok(None)`)
/// distinct from "could not tell" (`Err`).
///
/// The exact-bookmark form is asked first because it is the only one whose
/// emptiness is unambiguous: `jj log -r <name>` ERRORS on a name that is not
/// there, which would make a deleted base indistinguishable from a broken store.
/// A recorded base is not always a local bookmark, though — `HEAD` and
/// `origin/...` both appear — so a name the bookmark form does not know is put
/// to the general resolver before it is called gone.
fn resolve_one(jj: &JjEnv, store: &Path, name: &str) -> Result<Option<String>, String> {
    match crate::jj::bookmark_commit_checked(jj, store, name) {
        Ok(Some(commit)) => return Ok(Some(commit)),
        Ok(None) => {}
        Err(error) => return Err(error),
    }
    match crate::jj::revset_commit_checked(jj, store, name) {
        Ok(commit) => Ok(commit),
        // The general resolver refuses an unknown name rather than returning
        // nothing, so its error cannot be told apart from a genuine failure here
        // — but the bookmark form already answered that question cleanly, and it
        // said the name is not a branch in this store.
        Err(error) => {
            log::debug!("replay base `{name}` did not resolve as a revset either: {error}");
            Ok(None)
        }
    }
}

/// Answer "onto what, then?" for one branch.
///
/// Performs jj reads; callers holding the store lock should keep holding it, and
/// callers on a read path may run without one.
pub(crate) fn resolve_base(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    candidates: &BaseCandidates,
) -> Result<ResolvedBase, BaseResolutionError> {
    resolve_with(|name| resolve_one(jj, store, name), branch, candidates)
}

/// The decision, separated from the asking.
///
/// What makes this module correct is not which candidate it prefers but WHEN it
/// refuses, and every refusal turns on a distinction only the store can draw —
/// absent versus unanswerable. Coaxing a real store into each of those failures
/// per candidate is not something a test can do honestly, so the ordering and
/// the refusals live here, where a stub can state exactly what the store said,
/// and [`resolve_one`] is left to be exercised against a real one.
fn resolve_with(
    mut ask: impl FnMut(&str) -> Result<Option<String>, String>,
    branch: &str,
    candidates: &BaseCandidates,
) -> Result<ResolvedBase, BaseResolutionError> {
    let Some(recorded) = candidates.recorded.clone() else {
        return Err(BaseResolutionError::NothingRecorded {
            branch: branch.to_string(),
        });
    };

    let unresolvable = |candidate: &str, source, diagnostic| BaseResolutionError::Unresolvable {
        branch: branch.to_string(),
        candidate: candidate.to_string(),
        source,
        diagnostic,
    };

    match ask(&recorded) {
        Ok(Some(commit)) => {
            return Ok(ResolvedBase {
                branch: recorded,
                commit,
                source: BaseSource::Recorded,
                superseded: None,
            })
        }
        Ok(None) => {}
        Err(diagnostic) => return Err(unresolvable(&recorded, BaseSource::Recorded, diagnostic)),
    }

    let mut tried: Vec<String> = Vec::new();
    for (candidate, source) in [
        (
            candidates.pull_request_target.as_deref(),
            BaseSource::PullRequestTarget,
        ),
        (
            candidates.project_default.as_deref(),
            BaseSource::ProjectDefault,
        ),
    ] {
        let Some(candidate) = candidate else { continue };
        if candidate == recorded || tried.iter().any(|seen| seen == candidate) {
            continue;
        }
        match ask(candidate) {
            Ok(Some(commit)) => {
                return Ok(ResolvedBase {
                    branch: candidate.to_string(),
                    commit,
                    source,
                    superseded: Some(recorded),
                })
            }
            Ok(None) => tried.push(candidate.to_string()),
            // Not skipped in favour of the next candidate. This name is where
            // the work would be MOVED, so passing over one the store cannot
            // pin down, to land on something less specific, is the recorded
            // base's guess made about the destination — and the refusal that
            // follows would go on to claim these names "do not resolve", which
            // is a different and untrue statement about a name nobody could
            // read.
            Err(diagnostic) => return Err(unresolvable(candidate, source, diagnostic)),
        }
    }

    Err(BaseResolutionError::NoSurvivingBase {
        branch: branch.to_string(),
        recorded,
        tried,
    })
}

/// Persist a base the resolver had to fall back to.
///
/// The recorded name has been PROVEN gone, so keeping it is keeping a fact that
/// is no longer true — and every other surface that reads the column (the PR's
/// target, the diff range, the next base advance) would go on failing the same
/// way. Advisory: a branch whose replay is otherwise ready is not blocked on
/// recording where it landed.
pub(crate) async fn repoint_recorded_base(db: &LocalDb, job_id: &str, base_branch: &str) {
    if let Err(error) = db
        .execute(
            "UPDATE jobs SET base_branch = ?2, updated_at = ?3 WHERE id = ?1",
            params![job_id, base_branch, chrono::Utc::now().timestamp()],
        )
        .await
    {
        log::warn!("could not re-point job {job_id} onto base `{base_branch}`: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("replay-base-test.db").await
    }

    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
              VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-1', 'proj-1', 1, 'Child', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-child', 'proj-1', 'issue-1', 'running', 'agent/child', 'agent/parent', 1, 1);
            ",
        )
        .await
        .unwrap();
    }

    async fn candidates_for(db: &LocalDb, job_id: &str) -> BaseCandidates {
        let job_id = job_id.to_string();
        db.read(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move { load_base_candidates(conn, &job_id).await })
        })
        .await
        .unwrap()
    }

    /// The candidate set is the whole answer to "where could this branch be
    /// going?", so it has to carry all three statements at once.
    #[tokio::test]
    async fn candidates_carry_the_record_the_pull_request_and_the_project_default() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-1', 'job-child', 'proj-1', 'issue-1', 'PR', 'agent/child', 'main', 'open', 1, 1)",
            (),
        )
        .await
        .unwrap();

        let candidates = candidates_for(&db, "job-child").await;
        assert_eq!(candidates.recorded.as_deref(), Some("agent/parent"));
        assert_eq!(candidates.pull_request_target.as_deref(), Some("main"));
        assert_eq!(candidates.project_default.as_deref(), Some("main"));
    }

    /// A resolved pull request states where work already went. Reading it as a
    /// live destination would aim a replay at a branch this work has no
    /// remaining relationship with.
    #[tokio::test]
    async fn a_closed_pull_request_is_not_a_destination() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute(
            "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-1', 'job-child', 'proj-1', 'issue-1', 'PR', 'agent/child', 'release', 'merged', 1, 1)",
            (),
        )
        .await
        .unwrap();

        let candidates = candidates_for(&db, "job-child").await;
        assert_eq!(candidates.pull_request_target, None);
    }

    /// The empty string is what an unset column looks like in practice, and it
    /// resolves to nothing at all — so it must never reach the resolver as a
    /// name to look up.
    #[tokio::test]
    async fn an_empty_recorded_base_is_no_base_at_all() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute(
            "UPDATE jobs SET base_branch = '' WHERE id = 'job-child'",
            (),
        )
        .await
        .unwrap();

        let candidates = candidates_for(&db, "job-child").await;
        assert_eq!(candidates.recorded, None);
    }

    /// An open session names the base the rolled-back rebase was aimed at, which
    /// outranks the job's own column.
    #[test]
    fn a_session_base_overrides_the_recorded_one() {
        let candidates = BaseCandidates {
            recorded: Some("agent/parent".into()),
            ..BaseCandidates::default()
        };
        assert_eq!(
            candidates
                .clone()
                .recorded_from_session(Some("integration"))
                .recorded
                .as_deref(),
            Some("integration")
        );
        // An absent or empty session base leaves the record standing rather
        // than blanking it.
        assert_eq!(
            candidates.clone().recorded_from_session(None).recorded,
            candidates.recorded
        );
        assert_eq!(
            candidates.clone().recorded_from_session(Some("")).recorded,
            candidates.recorded
        );
    }

    /// Re-pointing is what makes the correction outlast the request that made
    /// it; without it every later surface trips over the same dead name.
    #[tokio::test]
    async fn re_pointing_replaces_the_dead_name_on_the_job() {
        let db = migrated_db().await;
        seed(&db).await;

        repoint_recorded_base(&db, "job-child", "main").await;

        assert_eq!(
            candidates_for(&db, "job-child").await.recorded.as_deref(),
            Some("main")
        );
    }

    /// The refusals are read by an agent deciding what to do next, so each has
    /// to name the situation it is actually in.
    #[test]
    fn each_refusal_names_its_own_situation() {
        let nothing = BaseResolutionError::NothingRecorded {
            branch: "agent/child".into(),
        }
        .to_string();
        assert!(nothing.contains("no base branch recorded"), "{nothing}");

        let unresolvable = BaseResolutionError::Unresolvable {
            branch: "agent/child".into(),
            candidate: "main".into(),
            source: BaseSource::Recorded,
            diagnostic: "resolved to more than one commit".into(),
        }
        .to_string();
        assert!(
            unresolvable.contains("could not say which commit")
                && unresolvable.contains("Nothing was substituted"),
            "an unknown base is reported as unknown, not quietly replaced: {unresolvable}"
        );

        // The same failure on a FALLBACK reads differently: the recorded base
        // is already known gone, and what could not be read is the destination.
        let fallback = BaseResolutionError::Unresolvable {
            branch: "agent/child".into(),
            candidate: "main".into(),
            source: BaseSource::PullRequestTarget,
            diagnostic: "resolved to more than one commit".into(),
        }
        .to_string();
        assert!(
            fallback.contains("pull request targets")
                && fallback.contains("THAT names")
                && fallback.contains("Nothing beyond it was tried"),
            "a fallback nobody could read is named as such, and stops the search: {fallback}"
        );

        let gone = BaseResolutionError::NoSurvivingBase {
            branch: "agent/child".into(),
            recorded: "agent/parent".into(),
            tried: vec!["main".into()],
        }
        .to_string();
        assert!(
            gone.contains("no longer exists") && gone.contains("`main`"),
            "a gone base names what was tried in its place: {gone}"
        );
    }

    // ---- The decision ----------------------------------------------------
    //
    // Which candidate wins, and — the part that actually keeps branches safe —
    // when the search stops instead of moving on.

    /// A store stub: each name answers with exactly what jj would have said,
    /// and every name asked is recorded so a test can prove what was NOT asked.
    struct Store {
        answers: Vec<(&'static str, Result<Option<String>, String>)>,
        asked: std::cell::RefCell<Vec<String>>,
    }

    impl Store {
        fn new(answers: Vec<(&'static str, Result<Option<String>, String>)>) -> Self {
            Store {
                answers,
                asked: std::cell::RefCell::new(Vec::new()),
            }
        }

        fn ask(&self, name: &str) -> Result<Option<String>, String> {
            self.asked.borrow_mut().push(name.to_string());
            self.answers
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map(|(_, answer)| answer.clone())
                // A name the fixture said nothing about is one the store does
                // not have.
                .unwrap_or(Ok(None))
        }
    }

    fn present(commit: &str) -> Result<Option<String>, String> {
        Ok(Some(commit.to_string()))
    }

    fn absent() -> Result<Option<String>, String> {
        Ok(None)
    }

    fn unreadable() -> Result<Option<String>, String> {
        Err("resolved to more than one commit (conflicted bookmark name)".to_string())
    }

    fn candidates(recorded: &str, pull_request: Option<&str>, default: &str) -> BaseCandidates {
        BaseCandidates {
            recorded: Some(recorded.to_string()),
            pull_request_target: pull_request.map(str::to_string),
            project_default: Some(default.to_string()),
        }
    }

    /// The case the fallback exists for, stated without a store: the parent
    /// merged, its branch is gone, and the pull request's target is where the
    /// work is still going.
    #[test]
    fn a_gone_base_prefers_the_pull_requests_target_over_the_default() {
        let store = Store::new(vec![
            ("agent/parent", absent()),
            ("release/2", present("commit-release")),
            ("main", present("commit-main")),
        ]);

        let resolved = resolve_with(
            |name| store.ask(name),
            "agent/child",
            &candidates("agent/parent", Some("release/2"), "main"),
        )
        .expect("the pull request names where this work is going");

        assert_eq!(resolved.branch, "release/2");
        assert_eq!(resolved.source, BaseSource::PullRequestTarget);
        assert_eq!(
            store.asked.borrow().as_slice(),
            ["agent/parent", "release/2"],
            "the default is never reached once something more specific answers"
        );
    }

    /// The refusal the safety rule is FOR, and the one that was missing: the
    /// recorded base is confirmed gone, and the destination that would replace
    /// it cannot be read. Landing on the project default here would move a
    /// branch's ancestry onto a base chosen because the real one was
    /// unreadable — a guess wearing a fallback's clothes.
    #[test]
    fn an_unreadable_fallback_refuses_instead_of_reaching_past_it() {
        let store = Store::new(vec![
            ("agent/parent", absent()),
            ("release/2", unreadable()),
            ("main", present("commit-main")),
        ]);

        let error = resolve_with(
            |name| store.ask(name),
            "agent/child",
            &candidates("agent/parent", Some("release/2"), "main"),
        )
        .expect_err("an unreadable destination is not a destination");

        assert_eq!(
            error,
            BaseResolutionError::Unresolvable {
                branch: "agent/child".into(),
                candidate: "release/2".into(),
                source: BaseSource::PullRequestTarget,
                diagnostic: "resolved to more than one commit (conflicted bookmark name)".into(),
            },
            "the refusal names the candidate nobody could read, not the recorded base"
        );
        assert_eq!(
            store.asked.borrow().as_slice(),
            ["agent/parent", "release/2"],
            "the project default is never even asked, so it cannot be silently substituted"
        );
    }

    /// A name nobody could read must not be reported as a name that resolves to
    /// nothing. `NoSurvivingBase` says the fallbacks "do not resolve either",
    /// which is a claim about the branches, and it is only ever true of
    /// candidates the store positively answered about.
    #[test]
    fn no_surviving_base_is_reserved_for_candidates_that_were_actually_absent() {
        let store = Store::new(vec![
            ("agent/parent", absent()),
            ("release/2", absent()),
            ("main", absent()),
        ]);

        let error = resolve_with(
            |name| store.ask(name),
            "agent/child",
            &candidates("agent/parent", Some("release/2"), "main"),
        )
        .expect_err("nothing survives");

        assert_eq!(
            error,
            BaseResolutionError::NoSurvivingBase {
                branch: "agent/child".into(),
                recorded: "agent/parent".into(),
                tried: vec!["release/2".into(), "main".into()],
            }
        );
    }

    /// A pull request that targets the dead branch itself — the ordinary state
    /// of a Cairn row GitHub has retargeted but Cairn has not — is not a
    /// second chance at the same name.
    #[test]
    fn a_fallback_repeating_the_recorded_base_is_skipped() {
        let store = Store::new(vec![
            ("agent/parent", absent()),
            ("main", present("commit-main")),
        ]);

        let resolved = resolve_with(
            |name| store.ask(name),
            "agent/child",
            &candidates("agent/parent", Some("agent/parent"), "main"),
        )
        .expect("the project default still answers");

        assert_eq!(resolved.source, BaseSource::ProjectDefault);
        assert_eq!(
            store.asked.borrow().as_slice(),
            ["agent/parent", "main"],
            "a name already proven gone is not asked about twice"
        );
    }

    // ---- Against a real store -------------------------------------------
    //
    // Resolution is the half of this module that only the store can answer, and
    // the distinction it turns on — a name that is ABSENT versus a name the
    // resolver cannot pin down — exists nowhere but in jj's own replies.

    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::jj::{ensure_project_store, JjEnv};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A store holding `main` and, one commit ahead of it, the `agent/parent`
    /// integration branch — the shape a child issue's base is in for as long as
    /// its parent has not merged.
    struct StoreFixture {
        _home: TempDir,
        _project: TempDir,
        jj: JjEnv,
        store: PathBuf,
        main: String,
        parent: String,
    }

    /// `None` when jj is not resolvable on this machine, matching the skip
    /// convention the rest of the real-store suites use.
    fn store_fixture() -> Option<StoreFixture> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        init_project(project.path());
        let main = git_stdout(project.path(), &["rev-parse", "HEAD"]);
        // The parent commit lives on its own branch, so `main` stays where a
        // project default actually sits: behind the integration branch cut from
        // it. A fixture that puts both on `main` cannot tell the two apart.
        git(project.path(), &["checkout", "-q", "-b", "agent/parent"]);
        std::fs::write(project.path().join("parent.rs"), "parent\n").unwrap();
        git(project.path(), &["add", "-A"]);
        git(project.path(), &["commit", "-q", "-m", "parent"]);
        let parent = git_stdout(project.path(), &["rev-parse", "HEAD"]);
        git(project.path(), &["checkout", "-q", "main"]);

        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        // Both git branches import as store bookmarks, so the store already
        // holds `main` and `agent/parent` at the commits git put them on.
        ensure_project_store(&jj, &store, project.path()).unwrap();
        Some(StoreFixture {
            _home: home,
            _project: project,
            jj,
            store,
            main,
            parent,
        })
    }

    /// While the recorded base is still there, nothing else gets a say. A
    /// fallback that outranked a live base would silently move branches off
    /// integration branches that are working exactly as intended.
    #[test]
    #[serial_test::serial(jj)]
    fn a_live_recorded_base_outranks_every_fallback() {
        let Some(fx) = store_fixture() else {
            eprintln!("skipping a_live_recorded_base_outranks_every_fallback: no jj");
            return;
        };

        let resolved = resolve_base(
            &fx.jj,
            &fx.store,
            "agent/child",
            &BaseCandidates {
                recorded: Some("agent/parent".into()),
                pull_request_target: Some("main".into()),
                project_default: Some("main".into()),
            },
        )
        .expect("a base that is present resolves");

        assert_eq!(resolved.branch, "agent/parent");
        assert_eq!(resolved.commit, fx.parent);
        assert_eq!(resolved.source, BaseSource::Recorded);
        assert_eq!(resolved.superseded, None, "nothing was superseded");
    }

    /// The defect this module exists for: the parent merged, its branch was
    /// deleted, and the child's recorded base now names nothing. The pull
    /// request's target is where the work is still going, so that is where the
    /// replay lands.
    #[test]
    #[serial_test::serial(jj)]
    fn a_deleted_base_falls_through_to_the_pull_requests_target() {
        let Some(fx) = store_fixture() else {
            eprintln!("skipping a_deleted_base_falls_through_to_the_pull_requests_target: no jj");
            return;
        };

        let resolved = resolve_base(
            &fx.jj,
            &fx.store,
            "agent/child",
            &BaseCandidates {
                recorded: Some("agent/deleted-parent".into()),
                pull_request_target: Some("main".into()),
                project_default: Some("main".into()),
            },
        )
        .expect("a base that is gone is not the end of the road");

        assert_eq!(resolved.branch, "main");
        assert_eq!(resolved.commit, fx.main);
        assert_eq!(resolved.source, BaseSource::PullRequestTarget);
        assert_eq!(
            resolved.superseded.as_deref(),
            Some("agent/deleted-parent"),
            "the dead name is carried back so the caller can say what happened"
        );
    }

    /// A branch with no pull request of its own — every child whose parent
    /// merged before it ever opened one — still has a floor to stand on.
    #[test]
    #[serial_test::serial(jj)]
    fn a_deleted_base_falls_through_to_the_project_default() {
        let Some(fx) = store_fixture() else {
            eprintln!("skipping a_deleted_base_falls_through_to_the_project_default: no jj");
            return;
        };

        let resolved = resolve_base(
            &fx.jj,
            &fx.store,
            "agent/child",
            &BaseCandidates {
                recorded: Some("agent/deleted-parent".into()),
                pull_request_target: None,
                project_default: Some("main".into()),
            },
        )
        .expect("the project default is the floor");

        assert_eq!(resolved.branch, "main");
        assert_eq!(resolved.source, BaseSource::ProjectDefault);
    }

    /// Nothing survives, so nothing is invented. The refusal has to name the
    /// recorded base and what was tried, because the agent reading it is
    /// deciding whether this is a bug or a branch nobody can place.
    #[test]
    #[serial_test::serial(jj)]
    fn a_base_with_no_survivor_refuses_and_names_what_it_tried() {
        let Some(fx) = store_fixture() else {
            eprintln!("skipping a_base_with_no_survivor_refuses_and_names_what_it_tried: no jj");
            return;
        };

        let error = resolve_base(
            &fx.jj,
            &fx.store,
            "agent/child",
            &BaseCandidates {
                recorded: Some("agent/deleted-parent".into()),
                pull_request_target: Some("agent/also-gone".into()),
                project_default: Some("release/never-existed".into()),
            },
        )
        .expect_err("there is nowhere to land");

        let BaseResolutionError::NoSurvivingBase { tried, .. } = &error else {
            panic!("a gone base with no survivor is not any other kind of failure: {error:?}");
        };
        assert_eq!(tried, &["agent/also-gone", "release/never-existed"]);
    }
}
