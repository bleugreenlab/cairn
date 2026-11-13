//! Declared regenerable paths (cairn/7110) on a real store: the replay takes
//! the branch's side and marks it, the hold, and how a regeneration settles.
//! The batch itself is a fake runner installed for the store; it seals the way
//! a branch commit does, so what the branch ends with is real.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::preparation::tests::{git, harness_or_skip, Harness, BRANCH};
use super::preparation::{BusyReason, PreparationPurpose, PreparationRefusal};
use super::regenerate::{
    self, BatchOutcome, CommandEnding, RegeneratedDelta, RegenerationJob, RegenerationRunner,
};
use crate::orchestrator::Orchestrator;

const PROJECT: &str = "proj-1";
const SETTLE: Duration = Duration::from_secs(60);
const CONFIG: &str = "landing:\n  regenerate:\n  - paths: ['*.gen']\n    regenerate: derive\n    \
                      check: derive --check\n    timeout: 30\n";

/// What the fake batch does on one dispatch.
enum Plan {
    /// Both commands pass and the regeneration writes `out.gen`.
    Pass(&'static str),
    /// The regenerate command exits with this code.
    Fail(i32),
    /// Before the result lands, the branch moves: by an owner commit to
    /// `out.gen` when given, else to an unrelated path.
    Supersede(Option<&'static str>),
}

struct Fake {
    plans: Mutex<VecDeque<Plan>>,
    runs: AtomicUsize,
    /// Each run takes one permit first, so a test decides when it starts.
    gate: Option<Arc<tokio::sync::Semaphore>>,
    jj: crate::jj::JjEnv,
    ws: PathBuf,
    store: PathBuf,
}

impl Fake {
    fn install(
        h: &Harness,
        gate: Option<Arc<tokio::sync::Semaphore>>,
        plans: Vec<Plan>,
    ) -> Arc<Self> {
        let fake = Arc::new(Self {
            plans: Mutex::new(plans.into()),
            runs: AtomicUsize::new(0),
            gate,
            jj: h.jj.clone(),
            ws: h.branch_ws.clone(),
            store: h.store.clone(),
        });
        regenerate::install_test_runner(&h.store, fake.clone());
        fake
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }
}

impl RegenerationRunner for Fake {
    fn run<'a>(
        &'a self,
        _orch: &'a Orchestrator,
        job: &'a RegenerationJob,
    ) -> BoxFuture<'a, BatchOutcome> {
        Box::pin(async move {
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            self.runs.fetch_add(1, Ordering::SeqCst);
            let plan = self
                .plans
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Plan::Pass("derived\n"));
            let ended = |exit: i32, output: &str| {
                Ok(CommandEnding {
                    exit: Some(exit),
                    timed_out: false,
                    output: output.to_string(),
                })
            };
            let request_id = Some(format!("request-{}-{}", job.row.id, self.runs()));
            let delta = |content: Option<&'static str>,
                         superseding: Option<Option<&'static str>>| {
                Some(Box::new(FakeDelta {
                    path: job.row.stale[0].path.clone(),
                    jj: self.jj.clone(),
                    ws: self.ws.clone(),
                    store: self.store.clone(),
                    head: job.row.installed_head.clone(),
                    content,
                    superseding,
                }) as Box<dyn RegeneratedDelta>)
            };
            match plan {
                Plan::Fail(exit) => BatchOutcome::Ran {
                    request_id,
                    regenerate: ended(exit, "error: the wasm32 target is not installed"),
                    check: ended(1, "stale"),
                    delta: None,
                },
                Plan::Pass(content) => BatchOutcome::Ran {
                    request_id,
                    regenerate: ended(0, ""),
                    check: ended(0, ""),
                    delta: delta(Some(content), None),
                },
                Plan::Supersede(owner_edit) => BatchOutcome::Ran {
                    request_id,
                    regenerate: ended(0, ""),
                    check: ended(0, ""),
                    delta: delta(None, Some(owner_edit)),
                },
            }
        })
    }
}

struct FakeDelta {
    /// The row's stale path, which a passing regeneration rewrites.
    path: String,
    jj: crate::jj::JjEnv,
    ws: PathBuf,
    store: PathBuf,
    head: String,
    content: Option<&'static str>,
    superseding: Option<Option<&'static str>>,
}

/// Commit `content` to `file` in the branch workspace, on the branch's tip.
fn seal_file(
    jj: &crate::jj::JjEnv,
    ws: &std::path::Path,
    file: &str,
    content: &str,
    message: &str,
    author: Option<&cairn_vcs::PublicationIdentity>,
) {
    let _ = crate::jj::update_stale(jj, ws);
    std::fs::write(ws.join(file), content).unwrap();
    if let Err(error) = crate::jj::seal(jj, ws, message, author) {
        assert!(error.contains("behind its branch tip"), "seal: {error}");
        jj.run(ws, &["new", BRANCH], "start on the bookmark tip")
            .unwrap();
        std::fs::write(ws.join(file), content).unwrap();
        crate::jj::seal(jj, ws, message, author).unwrap();
    }
}

impl RegeneratedDelta for FakeDelta {
    fn seal<'a>(
        self: Box<Self>,
        _orch: &'a Orchestrator,
        branch: &'a str,
        message: &'a str,
        admits: &'a (dyn Fn(&str) -> bool + Send + Sync),
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        assert!(admits(&self.path), "the rule admits its own output");
        assert!(!admits("src-a.txt"), "and nothing else");
        let (branch, message) = (branch.to_string(), message.to_string());
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                if let Some(owner_edit) = self.superseding {
                    let (file, content) = match owner_edit {
                        Some(content) => ("out.gen", content),
                        None => ("elsewhere.txt", "an owner's commit\n"),
                    };
                    seal_file(&self.jj, &self.ws, file, content, "owner commit", None);
                    return Ok(None);
                }
                if crate::jj::bookmark_commit(&self.jj, &self.store, &branch).as_deref()
                    != Some(self.head.as_str())
                {
                    return Ok(None);
                }
                let author = cairn_vcs::PublicationIdentity::new(
                    cairn_vcs::MANAGED_IDENTITY_NAME,
                    cairn_vcs::MANAGED_IDENTITY_EMAIL,
                );
                seal_file(
                    &self.jj,
                    &self.ws,
                    &self.path,
                    self.content.expect("a passing regeneration writes"),
                    &message,
                    Some(&author),
                );
                Ok(crate::jj::bookmark_commit(&self.jj, &self.store, &branch))
            })
            .await
            .unwrap()
        })
    }
}

fn show(h: &Harness, commit: &str, path: &str) -> String {
    git(&h.repo, &["show", &format!("{commit}:{path}")])
}

/// Main declares `*.gen` regenerable and regenerates `out.gen` from `src-a`;
/// the branch regenerates it from `src-b`. With `shared`, both also change
/// `shared.txt`, a conflict no rule covers.
fn diverge(h: &Harness, shared: bool) {
    std::fs::create_dir_all(h.main_ws.join(".cairn")).unwrap();
    h.advance_main(".cairn/config.yaml", CONFIG);
    h.advance_main("src-a.txt", "a\n");
    h.advance_main("out.gen", "derived from a\n");
    if shared {
        h.advance_main("shared.txt", "main's\n");
    }
    h.commit(&h.branch_ws, BRANCH, "src-b.txt", "b\n");
    h.commit(&h.branch_ws, BRANCH, "out.gen", "derived from b\n");
    if shared {
        h.commit(&h.branch_ws, BRANCH, "shared.txt", "branch's\n");
    }
}

/// The branch's newest regeneration once it settled; an ungated fake may
/// settle it before the wait begins.
async fn settled(h: &Harness) -> regenerate::Regeneration {
    if let Some(row) = regenerate::wait_for_settle(&h.db, PROJECT, BRANCH, Some(SETTLE))
        .await
        .unwrap()
    {
        return row;
    }
    regenerate::latest(&h.db, PROJECT, BRANCH)
        .await
        .unwrap()
        .expect("a regeneration was recorded")
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_regenerable_conflict_lands_the_branch_side_and_one_batch_regenerates_it() {
    let h = harness_or_skip!("regenerate-both.db");
    diverge(&h, false);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let fake = Fake::install(
        &h,
        Some(gate.clone()),
        vec![Plan::Pass("derived from a and b\n")],
    );

    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(prepared.advanced);
    assert_eq!(show(&h, &prepared.head_after, "out.gen"), "derived from b");
    assert_eq!(show(&h, &prepared.head_after, "src-a.txt"), "a");
    assert_eq!(show(&h, &prepared.head_after, "src-b.txt"), "b");
    assert!(
        prepared
            .warnings
            .iter()
            .any(|warning| warning.contains("stale until regenerated")),
        "{:?}",
        prepared.warnings
    );
    // The replay returned with the batch not yet started: it never waits.
    assert_eq!(fake.runs(), 0);
    let row = regenerate::holding(&h.db, PROJECT, BRANCH)
        .await
        .unwrap()
        .expect("the taken side is marked");
    assert_eq!(
        row.stale
            .iter()
            .map(|stale| stale.path.as_str())
            .collect::<Vec<_>>(),
        ["out.gen"]
    );
    assert_eq!(row.installed_head, prepared.head_after);

    // Every writer is held meanwhile, and told why.
    let merge = h.prepare(PreparationPurpose::OwnerMerge).await.unwrap_err();
    assert!(
        matches!(&merge, PreparationRefusal::Busy(BusyReason::Regenerating(hold))
            if hold.contains("regenerating `*.gen` after base advance; settles at")),
        "{merge}"
    );

    gate.add_permits(1);
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUCCEEDED, "{row:?}");
    assert_eq!(fake.runs(), 1, "exactly one batch");
    let head = h.tip(BRANCH);
    assert_eq!(row.commit.as_deref(), Some(head.as_str()));
    assert_eq!(
        git(&h.repo, &["log", "-1", "--format=%s|%an", &head]),
        format!(
            "regenerate *.gen after base advance|{}",
            cairn_vcs::MANAGED_IDENTITY_NAME
        )
    );
    assert_eq!(show(&h, &head, "out.gen"), "derived from a and b");
    // Settled, the hold is gone.
    h.prepare(PreparationPurpose::OwnerMerge).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_conflict_outside_the_globs_is_a_conflict_as_before() {
    let h = harness_or_skip!("regenerate-outside.db");
    diverge(&h, true);
    let fake = Fake::install(&h, None, Vec::new());
    let before = h.tip(BRANCH);

    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    let PreparationRefusal::Conflict(conflict) = &refusal else {
        panic!("expected a conflict: {refusal}");
    };
    let mut paths = conflict.paths.clone();
    paths.sort();
    assert_eq!(paths, ["out.gen", "shared.txt"]);
    assert_eq!(h.tip(BRANCH), before, "nothing was installed");
    assert!(regenerate::latest(&h.db, PROJECT, BRANCH)
        .await
        .unwrap()
        .is_none());
    assert_eq!(fake.runs(), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_rule_only_the_branch_declares_settles_nothing() {
    let h = harness_or_skip!("regenerate-branch-rule.db");
    h.advance_main("out.gen", "derived from a\n");
    std::fs::create_dir_all(h.branch_ws.join(".cairn")).unwrap();
    h.commit(&h.branch_ws, BRANCH, ".cairn/config.yaml", CONFIG);
    h.commit(&h.branch_ws, BRANCH, "out.gen", "derived from b\n");
    let fake = Fake::install(&h, None, Vec::new());

    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Conflict(conflict) if conflict.paths == ["out.gen"]),
        "{refusal}"
    );
    assert_eq!(fake.runs(), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_failed_regeneration_releases_the_hold_and_leaves_the_named_fact() {
    let h = harness_or_skip!("regenerate-failed.db");
    diverge(&h, false);
    let fake = Fake::install(&h, None, vec![Plan::Fail(3), Plan::Pass("fresh\n")]);

    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::FAILED);
    let failure = row.failure.clone().unwrap();
    assert_eq!(
        (failure.command.as_str(), failure.exit),
        ("derive", Some(3))
    );
    assert!(failure.output_tail.contains("wasm32"), "{failure:?}");
    let line =
        "`*.gen` is stale: regeneration failed (`derive`, exit 3); run `derive` on the branch";
    assert_eq!(row.stale_line().as_deref(), Some(line));
    assert!(regenerate::holding(&h.db, PROJECT, BRANCH)
        .await
        .unwrap()
        .is_none());
    assert_eq!(h.tip(BRANCH), prepared.head_after, "nothing was sealed");
    assert_eq!(
        regenerate::stale_lines(&h.orch, &h.db, PROJECT, BRANCH).await,
        [line]
    );
    let notices: Vec<String> = h
        .messages()
        .await
        .into_iter()
        .map(|(content, _)| content)
        .filter(|content| content.contains("regeneration failed"))
        .collect();
    assert_eq!(notices.len(), 1, "the owner is told once: {notices:?}");
    assert!(notices[0].contains(line) && notices[0].contains("wasm32"));
    // The marker never refuses a merge.
    h.prepare(PreparationPurpose::OwnerMerge).await.unwrap();

    // The owner runs it again from the rebase page.
    let answer = regenerate::redispatch(&h.orch, h.db.clone(), PROJECT, &h.store, BRANCH)
        .await
        .unwrap();
    assert!(answer.contains("Regenerating `*.gen`"), "{answer}");
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUCCEEDED);
    assert_eq!(fake.runs(), 2);
    assert_eq!(show(&h, &h.tip(BRANCH), "out.gen"), "fresh");
    assert!(regenerate::stale_lines(&h.orch, &h.db, PROJECT, BRANCH)
        .await
        .is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_superseded_result_runs_once_more_only_while_the_taken_bytes_stand() {
    let h = harness_or_skip!("regenerate-superseded.db");
    diverge(&h, false);
    let fake = Fake::install(&h, None, vec![Plan::Supersede(None), Plan::Pass("again\n")]);
    h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUCCEEDED, "{row:?}");
    assert_eq!(fake.runs(), 2, "one more run at the moved head");
    let head = h.tip(BRANCH);
    assert_eq!(show(&h, &head, "out.gen"), "again");
    assert_eq!(show(&h, &head, "elsewhere.txt"), "an owner's commit");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_superseded_result_whose_paths_changed_is_dropped() {
    let h = harness_or_skip!("regenerate-changed.db");
    diverge(&h, false);
    let fake = Fake::install(&h, None, vec![Plan::Supersede(Some("by hand\n"))]);
    h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUPERSEDED, "{row:?}");
    assert_eq!(fake.runs(), 1);
    assert_eq!(show(&h, &h.tip(BRANCH), "out.gen"), "by hand");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_capture_never_takes_a_side_and_asks_for_the_owner_replay() {
    let h = harness_or_skip!("regenerate-capture.db");
    diverge(&h, false);
    let fake = Fake::install(&h, None, vec![Plan::Pass("captured\n")]);
    let before = h.tip(BRANCH);

    let refusal = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    let PreparationRefusal::RegenerationPending(pending) = &refusal else {
        panic!("expected a pending regeneration: {refusal}");
    };
    assert_eq!(pending.paths, ["out.gen"]);
    assert!(refusal
        .to_string()
        .contains("waiting on regeneration of `out.gen`"));
    assert_eq!(pending.head, before, "the capture installed nothing");

    // The owner preparation it asked for installs and regenerates.
    let row = tokio::time::timeout(SETTLE, async {
        loop {
            match regenerate::latest(&h.db, PROJECT, BRANCH).await.unwrap() {
                Some(row) if !row.holds() => break row,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("the owner replay ran");
    assert_eq!(row.state, regenerate::SUCCEEDED, "{row:?}");
    assert_eq!(fake.runs(), 1);
    assert_eq!(show(&h, &h.tip(BRANCH), "out.gen"), "captured");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_registry_list_and_a_regenerable_pair_settle_in_one_recompute() {
    let h = harness_or_skip!("regenerate-mixed.db");
    let list = |items: &[&str]| {
        format!(
            "pub const ITEMS: &[&str] = &[\n{}];\n",
            items
                .iter()
                .map(|item| format!("    \"{item}\",\n"))
                .collect::<String>()
        )
    };
    let tip = h.advance_main("registry.rs", &list(&["base"]));
    h.set_bookmark(BRANCH, &tip);
    std::fs::create_dir_all(h.main_ws.join(".cairn")).unwrap();
    h.advance_main(
        ".cairn/config.yaml",
        &format!("{CONFIG}  merge:\n  - paths: [registry.rs]\n    kind: ordered-set\n"),
    );
    h.advance_main("registry.rs", &list(&["base", "incoming"]));
    h.advance_main("out.gen", "derived from incoming\n");
    h.commit(
        &h.branch_ws,
        BRANCH,
        "registry.rs",
        &list(&["base", "owner"]),
    );
    h.commit(&h.branch_ws, BRANCH, "out.gen", "derived from owner\n");
    let fake = Fake::install(&h, None, vec![Plan::Pass("derived from both\n")]);

    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    let registry = show(&h, &prepared.head_after, "registry.rs");
    assert!(
        registry.contains("\"owner\"") && registry.contains("\"incoming\""),
        "{registry}"
    );
    assert_eq!(
        show(&h, &prepared.head_after, "out.gen"),
        "derived from owner"
    );
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUCCEEDED, "{row:?}");
    assert_eq!(
        row.stale
            .iter()
            .map(|stale| stale.path.as_str())
            .collect::<Vec<_>>(),
        ["out.gen"],
        "only the regenerable path was taken"
    );
    assert_eq!(fake.runs(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn every_rule_of_one_install_regenerates_one_after_another() {
    let h = harness_or_skip!("regenerate-two-rules.db");
    std::fs::create_dir_all(h.main_ws.join(".cairn")).unwrap();
    h.advance_main(
        ".cairn/config.yaml",
        "landing:\n  regenerate:\n  - paths: ['a-*.gen']\n    regenerate: derive-a\n    \
         check: derive-a --check\n  - paths: ['b-*.gen']\n    regenerate: derive-b\n    \
         check: derive-b --check\n",
    );
    h.advance_main("a-out.gen", "main's a\n");
    h.advance_main("b-out.gen", "main's b\n");
    h.commit(&h.branch_ws, BRANCH, "a-out.gen", "branch's a\n");
    h.commit(&h.branch_ws, BRANCH, "b-out.gen", "branch's b\n");
    let fake = Fake::install(
        &h,
        None,
        vec![Plan::Pass("fresh a\n"), Plan::Pass("fresh b\n")],
    );

    h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    tokio::time::timeout(SETTLE, async {
        while regenerate::holding(&h.db, PROJECT, BRANCH)
            .await
            .unwrap()
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("both rules settle");
    assert_eq!(fake.runs(), 2, "each rule's batch ran");
    let head = h.tip(BRANCH);
    assert_eq!(show(&h, &head, "a-out.gen"), "fresh a");
    assert_eq!(show(&h, &head, "b-out.gen"), "fresh b");
    let subjects = git(&h.repo, &["log", "-2", "--format=%s", &head]);
    assert!(
        subjects.contains("regenerate a-*.gen after base advance")
            && subjects.contains("regenerate b-*.gen after base advance"),
        "{subjects}"
    );
}

/// A rule the restart tests record rows for directly, as a dead process left
/// them.
fn orphan_rule() -> crate::config::project_settings::LandingRegenerateRule {
    crate::config::project_settings::LandingRegenerateRule {
        paths: vec!["*.gen".into()],
        regenerate: "derive".into(),
        check: "derive --check".into(),
        executor: None,
        timeout: Some(30),
        lane: None,
    }
}

async fn orphan(h: &Harness, installed_head: &str) -> i64 {
    let blob = git(
        &h.repo,
        &["rev-parse", &format!("{}:out.gen", h.tip(BRANCH))],
    );
    let rule = orphan_rule();
    let ids = regenerate::record_install(
        &h.db,
        PROJECT,
        &h.store,
        BRANCH,
        &[(
            &rule,
            vec![regenerate::StalePath {
                path: "out.gen".into(),
                blob: Some(blob),
            }],
        )],
        installed_head,
        &h.tip("main"),
    )
    .await
    .unwrap();
    // The process that recorded it is gone.
    regenerate::forget_live_rows();
    ids[0]
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_restart_retires_a_row_whose_install_never_happened() {
    let h = harness_or_skip!("regenerate-restart-uninstalled.db");
    h.commit(&h.branch_ws, BRANCH, "out.gen", "taken\n");
    let elsewhere = h.advance_main("src-a.txt", "a\n");
    let fake = Fake::install(&h, None, Vec::new());
    let id = orphan(&h, &elsewhere).await;
    assert!(regenerate::holding(&h.db, PROJECT, BRANCH)
        .await
        .unwrap()
        .is_some());

    regenerate::recover_after_restart(&h.orch).await;
    let row = settled(&h).await;
    assert_eq!(row.id, id);
    assert_eq!(row.state, regenerate::SUPERSEDED, "{row:?}");
    assert_eq!(
        fake.runs(),
        0,
        "nothing ran for an install that never happened"
    );
    // The branch is free again, and the owner may regenerate on demand.
    h.prepare(PreparationPurpose::OwnerMerge).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_restart_resumes_a_regeneration_whose_batch_died_with_it() {
    let h = harness_or_skip!("regenerate-restart-running.db");
    let head = h.commit(&h.branch_ws, BRANCH, "out.gen", "taken\n");
    let fake = Fake::install(&h, None, vec![Plan::Pass("recovered\n")]);
    let id = orphan(&h, &head).await;
    h.db.execute(
        "UPDATE branch_regenerations SET state = 'running', request_id = 'lost', dispatches = 1
          WHERE id = ?1",
        cairn_db::turso::params![id],
    )
    .await
    .unwrap();
    let held = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(held, PreparationRefusal::Busy(BusyReason::Regenerating(_))),
        "{held}"
    );

    regenerate::recover_after_restart(&h.orch).await;
    let row = settled(&h).await;
    assert_eq!(row.state, regenerate::SUCCEEDED, "{row:?}");
    assert_eq!(fake.runs(), 1);
    assert_eq!(show(&h, &h.tip(BRANCH), "out.gen"), "recovered");
}
