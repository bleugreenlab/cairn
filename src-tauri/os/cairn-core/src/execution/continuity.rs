//! Execution-layer continuity: what a successor execution does with committed
//! work earlier executions on the same issue left on their branches.
//!
//! An execution is not the only attempt an issue gets. When one dies for a
//! reason that has nothing to do with its work — a provider fault, a wedged
//! runner, a `prepare_job` that could not take a ref lock — its commits stay
//! reachable on the branch it minted. The successor mints a branch of its own,
//! and until this module existed that branch was created at the issue's base as
//! though the earlier commits did not exist: the default outcome of restarting a
//! failed execution was a fresh agent reimplementing work that already existed,
//! and the only thing between that and a duplicate implementation was somebody
//! reading the dead builder's transcript and writing a paragraph.
//!
//! The rule here is that a successor never silently starts behind reachable
//! committed work. It either carries the latest valid committed logical head
//! forward, or — when "latest" is genuinely undefined — refuses and names the
//! choice. Either way the successor's opening context says which prior
//! executions left commits, where those commits are, and whether they were
//! inherited.
//!
//! Three things it deliberately does not do. It does not resurrect a terminal
//! execution: failed and cancelled executions stay immutable evidence, and the
//! successor gets its own execution identity and ordinary lifecycle. It does not
//! key eligibility on tidy death metadata — the specimen that motivated this
//! (cairn/4602) had a `failed` job whose session row was still `open` with a
//! NULL terminal reason while its commits were perfectly reachable, so
//! REACHABILITY is the test, not the tidiness of the death. And it never claims
//! uncommitted work: what was not committed did not survive, and saying
//! otherwise would be worse than saying nothing.

use std::collections::HashMap;

use cairn_db::turso::params;

use crate::models::Continuity;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Whether a prior execution's committed head is still work the successor could
/// take, or something that has already been answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Commits the base branch does not contain. The only adoptable state.
    Reachable,
    /// The head is an ancestor of (or equal to) the base branch: its work has
    /// already landed, so there is nothing ahead to carry.
    Landed,
    /// A closed merge request names this branch — the work was reviewed and
    /// explicitly not taken. Adopting it would silently reinstate it.
    Rejected,
    /// A LATER execution already made an explicit continuity decision that this
    /// head was on the wrong side of. Someone said "supersede this" once, and a
    /// decision that only bound the execution that made it would let the very
    /// next retry undo it.
    Superseded,
}

/// One prior execution's branch-minting job, as continuity needs to see it: an
/// immutable node reference, the branch it minted, and the commit that branch
/// actually resolves to in the store right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorWork {
    pub job_id: String,
    /// `cairn://p/{project}/{number}/{seq}/{segment}` — the immutable coordinate
    /// a reader follows to see what that execution did. `None` only for rows old
    /// enough to predate durable URI segments.
    pub node_uri: Option<String>,
    pub branch: String,
    /// The commit the branch resolves to in the store. A branch that no longer
    /// resolves never becomes a `PriorWork` at all.
    pub head: String,
    pub disposition: Disposition,
}

impl PriorWork {
    /// How this candidate is named in a refusal, and therefore what a caller may
    /// spell back as `continuity: "<coordinate>"`.
    pub fn label(&self) -> String {
        match &self.node_uri {
            Some(uri) => format!(
                "{uri} (branch `{}`, commit {})",
                self.branch,
                short(&self.head)
            ),
            None => format!("branch `{}` at commit {}", self.branch, short(&self.head)),
        }
    }

    /// Whether an explicit `from` coordinate names this candidate. A node URI, a
    /// branch name, or any unambiguous commit-id prefix all address it, because
    /// all three appear in the refusal a caller is answering.
    fn named_by(&self, coordinate: &str) -> bool {
        let coordinate = coordinate.trim();
        if coordinate.is_empty() {
            return false;
        }
        if self.node_uri.as_deref() == Some(coordinate) || self.branch == coordinate {
            return true;
        }
        coordinate.len() >= 7 && self.head.starts_with(coordinate)
    }
}

/// A commit id at the length the rest of Cairn's prose uses.
fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// What a branch-minting job should do about the prior work it found.
///
/// Every variant except [`Continuation::NotApplicable`] carries the full
/// candidate list, including the ones that were not adopted: the successor's
/// context names what exists whether or not it was inherited, because an
/// unadopted head that nobody mentions is exactly the silence this module
/// exists to break.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Continuation {
    /// This job does not mint a branch, or already carries one. Continuity has
    /// no question to answer.
    NotApplicable,
    /// No prior execution left work the base branch does not already contain.
    Fresh { prior: Vec<PriorWork> },
    /// Start the minted branch at `adopted.head` instead of the base, so the
    /// successor's history opens containing that execution's commits.
    Adopt {
        adopted: PriorWork,
        prior: Vec<PriorWork>,
    },
    /// The launch asked to start clean. Prior work exists and is named, but
    /// deliberately not carried.
    Declined { prior: Vec<PriorWork> },
    /// Several unrelated committed heads: no single one contains the rest, so
    /// "the latest valid committed logical head" does not exist and picking one
    /// would discard the others' commits. The caller decides.
    Undecided { candidates: Vec<PriorWork> },
    /// An explicit `from` coordinate that names nothing adoptable.
    Unresolved {
        coordinate: String,
        candidates: Vec<PriorWork>,
    },
}

impl Continuation {
    /// The commit a fresh mint starts at instead of the base branch.
    pub fn adopt_head(&self) -> Option<&str> {
        match self {
            Continuation::Adopt { adopted, .. } => Some(adopted.head.as_str()),
            _ => None,
        }
    }

    /// The refusal, when the successor must not begin an ordinary turn until
    /// someone chooses. `None` means preparation proceeds.
    ///
    /// This lands on the run's `error_message` under the `job_preparation_failed`
    /// exit reason, which the executions resource already renders in full — so
    /// the prior commits are surfaced where a person deciding is already looking.
    pub fn refusal(&self) -> Option<String> {
        match self {
            Continuation::Undecided { candidates } => {
                let mut out = String::from(
                    "Refusing to start behind committed work: this issue's earlier executions left \
                     several unrelated committed heads, so there is no single latest one to carry \
                     forward. Adopting one would discard the others' commits.\n\nCandidates:\n",
                );
                for candidate in candidates
                    .iter()
                    .filter(|c| c.disposition == Disposition::Reachable)
                {
                    out.push_str(&format!("- {}\n", candidate.label()));
                }
                out.push_str(
                    "\nStart a new execution naming the decision: `continuity: \"<node URI, branch, \
                     or commit above>\"` to carry that one forward, or `continuity: \"clean\"` to \
                     start at the base branch and supersede all of them.",
                );
                Some(out)
            }
            Continuation::Unresolved {
                coordinate,
                candidates,
            } => {
                let mut out = format!(
                    "Refusing to start: `continuity: \"{coordinate}\"` names no adoptable committed \
                     head on this issue.\n"
                );
                if candidates.is_empty() {
                    out.push_str(
                        "No earlier execution of this issue left a branch that still resolves in \
                         the store. Use `continuity: \"clean\"` to start at the base branch.",
                    );
                } else {
                    out.push_str("\nWhat this issue's earlier executions left:\n");
                    for candidate in candidates {
                        out.push_str(&format!(
                            "- {} — {}\n",
                            candidate.label(),
                            match candidate.disposition {
                                Disposition::Reachable => "adoptable",
                                Disposition::Landed => "already contained in the base branch",
                                Disposition::Rejected => "explicitly rejected (closed PR)",
                                Disposition::Superseded =>
                                    "superseded by a later execution's explicit decision",
                            }
                        ));
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }

    /// The section appended to the successor's opening prompt.
    ///
    /// Announcement is not a substitute for inheritance — when adoption is safe
    /// it has already happened by the time this renders, and this says so. What
    /// announcement is for is the rest: naming heads that were NOT carried and
    /// why, so an agent who could otherwise only discover them by suspecting
    /// they exist reads them in its first turn instead.
    ///
    /// `None` when there is nothing true to say.
    pub fn briefing(&self) -> Option<String> {
        let (lead, prior) = match self {
            Continuation::NotApplicable => return None,
            Continuation::Fresh { prior } if prior.is_empty() => return None,
            Continuation::Fresh { prior } => (
                "Earlier executions of this issue left branches, but none of them holds work to \
                 carry into yours, so your branch starts at the base. Each is listed below with \
                 the reason."
                    .to_string(),
                prior,
            ),
            Continuation::Adopt { adopted, prior } => (
                format!(
                    "**Your branch already contains {}'s commits.** It was created at {}, the head \
                     of `{}`, rather than at the base branch — so `git log` on your branch shows \
                     that work as history you inherited, not as something still to do. Read it \
                     before you plan: build on it rather than reimplementing it, and if it \
                     conflicts with your instructions, say so rather than silently redoing it. \
                     Anything that execution left UNCOMMITTED did not survive and is not here.",
                    adopted
                        .node_uri
                        .as_deref()
                        .unwrap_or(adopted.branch.as_str()),
                    short(&adopted.head),
                    adopted.branch,
                ),
                prior,
            ),
            Continuation::Declined { prior } => (
                "This execution was started with `continuity: \"clean\"`: earlier executions of \
                 this issue left committed work, and it was deliberately NOT carried into your \
                 branch, which starts at the base. Treat that work as superseded unless the issue \
                 says otherwise."
                    .to_string(),
                prior,
            ),
            // A refusal never reaches a prompt — preparation stops first.
            Continuation::Undecided { .. } | Continuation::Unresolved { .. } => return None,
        };

        let mut out = String::from("## Prior executions of this issue\n\n");
        out.push_str(&lead);
        out.push_str("\n\n");
        let adopted_job = match self {
            Continuation::Adopt { adopted, .. } => Some(adopted.job_id.as_str()),
            _ => None,
        };
        for work in prior {
            let inherited = Some(work.job_id.as_str()) == adopted_job;
            out.push_str(&format!(
                "- {} — {}\n",
                work.label(),
                match (inherited, work.disposition) {
                    (true, _) => "inherited: these commits are in your branch's history",
                    (false, Disposition::Reachable) =>
                        "NOT inherited: reachable commits your branch does not contain",
                    (false, Disposition::Landed) =>
                        "not inherited: already contained in the base branch",
                    (false, Disposition::Rejected) =>
                        "not inherited: explicitly rejected (closed PR)",
                    (false, Disposition::Superseded) =>
                        "not inherited: superseded by a later execution's explicit decision",
                }
            ));
        }
        out.push_str(
            "\nRead a prior execution's node URI for its transcript, and \
             `cairn://p/PROJECT/NUMBER/changed` for what it changed.\n",
        );
        Some(out)
    }
}

// ---------------------------------------------------------------------------

/// Decide what a branch-minting successor does with the prior work it found.
///
/// Pure over an injected ancestry oracle so the whole policy is testable without
/// a jj store. `descends(tip, dest)` is true when `tip` descends from or equals
/// `dest` — the same relation `jj::revset_descends_from` computes.
///
/// Note what is NOT disqualifying: a head that has diverged from the base
/// branch. A branch behind an advanced base is the ordinary condition of every
/// live agent branch, and the base-advance machinery reconciles it once the
/// branch is live again. Refusing there would make the refusal the common case
/// and the adoption the rare one, which is the wrong way round.
pub fn decide(
    mode: &Continuity,
    base: &str,
    candidates: Vec<PriorWork>,
    descends: impl Fn(&str, &str) -> bool,
) -> Continuation {
    let candidates: Vec<PriorWork> = candidates
        .into_iter()
        .map(|mut work| {
            if work.disposition == Disposition::Reachable && descends(base, &work.head) {
                work.disposition = Disposition::Landed;
            }
            work
        })
        .collect();

    let live: Vec<&PriorWork> = candidates
        .iter()
        .filter(|work| work.disposition == Disposition::Reachable)
        .collect();

    // An explicit coordinate may reach past a `Rejected` or `Superseded`
    // classification: both of those record somebody's earlier decision, and this
    // is somebody deciding now. It may NOT name a head the base already contains
    // — that would start the successor behind its own base, which is never what
    // anyone means.
    if let Continuity::From(coordinate) = mode {
        let named = candidates
            .iter()
            .find(|work| work.disposition != Disposition::Landed && work.named_by(coordinate));
        return match named {
            Some(work) => Continuation::Adopt {
                adopted: work.clone(),
                prior: candidates.clone(),
            },
            None => Continuation::Unresolved {
                coordinate: coordinate.clone(),
                candidates,
            },
        };
    }

    if live.is_empty() {
        return Continuation::Fresh { prior: candidates };
    }
    if *mode == Continuity::Clean {
        return Continuation::Declined { prior: candidates };
    }

    // The latest valid committed logical head is the one that CONTAINS every
    // other. Where one exists, adopting it leaves no commits behind, which is
    // what makes automatic adoption safe rather than a guess.
    let latest = live.iter().find(|work| {
        live.iter()
            .all(|other| other.job_id == work.job_id || descends(&work.head, &other.head))
    });
    match latest {
        Some(work) => Continuation::Adopt {
            adopted: (*work).clone(),
            prior: candidates.clone(),
        },
        None => Continuation::Undecided { candidates },
    }
}

/// A prior branch-minting job as the database knows it, before the store is
/// asked where its branch actually is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorCandidate {
    pub job_id: String,
    pub node_uri: Option<String>,
    pub branch: String,
    /// A closed merge request names this branch: reviewed and not taken.
    pub rejected: bool,
    /// A later execution's explicit decision already put this head behind it.
    pub superseded: bool,
}

/// Where an execution sits in its issue's series: one key, ordered
/// lexicographically, for the whole series.
///
/// `executions.seq` is the authority and is set on every issue execution the
/// current code creates. A row without one predates the column, and therefore
/// predates every sequenced row — which is exactly what `Option`'s own ordering
/// says, since `None < Some(_)`. `started_at` and then `id` break ties among
/// rows that share a rung, so the order is strict and does not depend on the
/// order SQLite happened to return rows in.
///
/// The comparison must be a genuine total order, not a pairwise rule: it filters
/// candidates, selects the supersession boundary by maximum, and sorts the
/// briefing. An earlier version compared `seq` when both sides had one and fell
/// back to `started_at` otherwise, which is intransitive as soon as sequenced
/// and sequence-less rows coexist — `(Some(1), 300)`, `(None, 200)`,
/// `(Some(2), 100)` form a cycle under it, and a cycle here marks the wrong
/// heads superseded. Deriving the order instead of hand-writing it is what
/// makes that class of bug unavailable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExecutionOrder {
    seq: Option<i64>,
    started_at: i64,
    id: String,
}

/// Every branch-minting job of an EARLIER execution of `issue_id`, newest
/// execution first.
///
/// `parent_job_id IS NULL` is what distinguishes a job that MINTED its branch
/// from one that inherited it: job creation records a parent only for nodes
/// whose behaviour inherits, so a delegated task riding its parent's branch is
/// excluded rather than counted as a second head on the same commits. Jobs of
/// the successor's own execution are excluded too — a sibling node in the same
/// execution is concurrency, not history.
///
/// Deliberately says nothing about job or session STATUS. The specimen this was
/// built from had a `failed` job whose session row was still open with a NULL
/// terminal reason, and its commits were reachable and valid; keying on tidy
/// death metadata would have skipped exactly the work that needed carrying.
pub async fn prior_branch_bearing_jobs(
    db: &LocalDb,
    issue_id: &str,
    execution_id: &str,
) -> DbResult<Vec<PriorCandidate>> {
    let issue_id = issue_id.to_string();
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut identity = conn
                .query(
                    "SELECT p.key, i.number
                     FROM issues i JOIN projects p ON p.id = i.project_id
                     WHERE i.id = ?1 LIMIT 1",
                    params![issue_id.as_str()],
                )
                .await?;
            let (project_key, number) = match identity.next().await? {
                Some(row) => (row.text(0)?, row.i64(1)? as i32),
                None => return Ok(Vec::new()),
            };

            // The branch-minting jobs first, because whether an execution
            // actually minted is what decides if its stated decision counts.
            let mut rows = conn
                .query(
                    "SELECT j.id, j.branch, j.uri_segment, j.execution_id,
                            (SELECT mr.status FROM merge_requests mr
                              WHERE mr.source_branch = j.branch
                              ORDER BY mr.updated_at DESC LIMIT 1)
                     FROM jobs j
                     WHERE j.issue_id = ?1
                       AND j.parent_job_id IS NULL
                       AND j.branch IS NOT NULL
                     ORDER BY j.created_at DESC",
                    params![issue_id.as_str()],
                )
                .await?;
            struct MintedJob {
                job_id: String,
                branch: String,
                segment: Option<String>,
                execution_id: String,
                rejected: bool,
            }
            let mut minted: Vec<MintedJob> = Vec::new();
            let mut minting_executions: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            while let Some(row) = rows.next().await? {
                let Some(execution) = row.opt_text(3)? else {
                    continue;
                };
                minting_executions.insert(execution.clone());
                minted.push(MintedJob {
                    job_id: row.text(0)?,
                    branch: row.text(1)?,
                    segment: row.opt_text(2)?,
                    execution_id: execution,
                    rejected: row.opt_text(4)?.as_deref() == Some("closed"),
                });
            }

            // Every execution of the issue, with where it sits in the series and
            // whether it made an explicit continuity decision. `json_extract`
            // reads the one snapshot field this needs rather than deserializing
            // whole snapshots: `auto` is the default, absent is a snapshot older
            // than the field, and anything else — `clean` or a `{"from":…}`
            // object — is somebody having decided.
            let mut execution_rows = conn
                .query(
                    "SELECT id, seq, started_at, json_extract(snapshot, '$.continuity')
                     FROM executions WHERE issue_id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            let mut orders: HashMap<String, ExecutionOrder> = HashMap::new();
            let mut decisions: Vec<ExecutionOrder> = Vec::new();
            let mut current: Option<ExecutionOrder> = None;
            while let Some(row) = execution_rows.next().await? {
                let id = row.text(0)?;
                let order = ExecutionOrder {
                    seq: row.opt_i64(1)?,
                    started_at: row.i64(2)?,
                    id: id.clone(),
                };
                // A decision counts only once it was ENACTED. An execution that
                // stated one and then never minted a branch — refused because its
                // coordinate named nothing, or dead before preparation finished —
                // carried nothing and settled nothing, and letting a request that
                // failed suppress a predecessor's real commits would be the exact
                // silent loss this module exists to prevent.
                let decided = row
                    .opt_text(3)?
                    .is_some_and(|continuity| continuity != "auto")
                    && minting_executions.contains(&id);
                if id == execution_id {
                    current = Some(order);
                } else {
                    if decided {
                        decisions.push(order.clone());
                    }
                    orders.insert(id, order);
                }
            }
            let Some(current) = current else {
                return Ok(Vec::new());
            };

            // The supersession boundary: the LATEST earlier execution that made
            // an explicit decision. Everything before it was already settled, and
            // a decision that bound only the execution that made it would be undone
            // by the very next retry.
            let boundary = decisions.into_iter().filter(|order| *order < current).max();

            let mut candidates: Vec<(ExecutionOrder, PriorCandidate)> = Vec::new();
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for job in minted {
                let MintedJob {
                    job_id,
                    branch,
                    segment,
                    execution_id,
                    rejected,
                } = job;
                let Some(order) = orders.get(&execution_id).cloned() else {
                    continue;
                };
                // A job of the CURRENT execution is a sibling, not history, and a
                // job of a later one is the successor's own future: an execution
                // prepared out of order must never adopt a newer execution's head.
                if order >= current {
                    continue;
                }
                if branch.is_empty() || !seen.insert(branch.clone()) {
                    continue;
                }
                let node_uri = match (segment, order.seq) {
                    (Some(segment), Some(seq)) => Some(cairn_common::uri::build_node_uri(
                        &project_key,
                        number,
                        seq as i32,
                        &segment,
                    )),
                    _ => None,
                };
                let superseded = boundary.as_ref().is_some_and(|boundary| order < *boundary);
                candidates.push((
                    order,
                    PriorCandidate {
                        job_id,
                        node_uri,
                        branch,
                        rejected,
                        superseded,
                    },
                ));
            }
            // Newest execution first, which is the order the successor's briefing
            // reads in.
            candidates.sort_by(|(left, _), (right, _)| right.cmp(left));
            Ok(candidates
                .into_iter()
                .map(|(_, candidate)| candidate)
                .collect())
        })
    })
    .await
}

/// Ask the store where each candidate's branch actually is, dropping the ones it
/// can no longer produce.
///
/// A branch that does not resolve is not a candidate at all: whatever it once
/// held cannot be carried, and reporting it as inheritable would be the same
/// class of lie as claiming uncommitted work survived.
pub fn resolve_heads(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    candidates: Vec<PriorCandidate>,
) -> Vec<PriorWork> {
    candidates
        .into_iter()
        .filter_map(|candidate| {
            let head = crate::jj::bookmark_commit(jj, store, &candidate.branch).or_else(|| {
                log::info!(
                    "[continuity] prior branch {} does not resolve in the store; nothing of it can \
                     be carried forward",
                    candidate.branch
                );
                None
            })?;
            Some(PriorWork {
                job_id: candidate.job_id,
                node_uri: candidate.node_uri,
                branch: candidate.branch,
                head,
                disposition: match (candidate.rejected, candidate.superseded) {
                    (true, _) => Disposition::Rejected,
                    (false, true) => Disposition::Superseded,
                    (false, false) => Disposition::Reachable,
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Distinct 40-character commit ids whose PREFIXES differ, so a test can
    /// address one the way a person answering a refusal would.
    fn commit(tag: &str) -> String {
        format!("{tag}{}", "0".repeat(40 - tag.len()))
    }

    /// `descends(tip, dest)` over an explicit ancestry: `(descendant, ancestor)`
    /// pairs, plus the reflexive case every real revset relation has.
    fn ancestry(pairs: &[(&str, &str)]) -> impl Fn(&str, &str) -> bool {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(tip, dest)| (commit(tip), commit(dest)))
            .collect();
        move |tip: &str, dest: &str| tip == dest || owned.iter().any(|(a, b)| a == tip && b == dest)
    }

    fn work(job: &str, seq: i64, head: &str) -> PriorWork {
        PriorWork {
            job_id: job.to_string(),
            node_uri: Some(format!("cairn://p/cairn/4602/{seq}/builder")),
            branch: format!("agent/cairn-4602-builder-{}", seq - 1),
            head: commit(head),
            disposition: Disposition::Reachable,
        }
    }

    /// The specimen this module was built for: one dead execution, one branch,
    /// reachable commits the base does not contain. Nothing about the shape of
    /// its death is consulted.
    #[test]
    fn a_single_reachable_head_is_carried_forward() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![work("job-1", 1, "dead")],
            ancestry(&[("dead", "base")]),
        );
        assert_eq!(outcome.adopt_head(), Some(commit("dead").as_str()));
        assert!(outcome.refusal().is_none());
    }

    /// Two executions, the later containing the earlier: "latest" is defined, so
    /// adopting it leaves no commits behind and needs no decision.
    #[test]
    fn the_head_that_contains_every_other_is_the_latest() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![work("job-2", 2, "later"), work("job-1", 1, "early")],
            ancestry(&[("later", "base"), ("early", "base"), ("later", "early")]),
        );
        assert_eq!(outcome.adopt_head(), Some(commit("later").as_str()));
    }

    /// Two executions whose commits are unrelated. Picking either discards the
    /// other's work, so the successor is blocked and both are named.
    #[test]
    fn unrelated_heads_refuse_rather_than_discard_one() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![work("job-2", 2, "forkb"), work("job-1", 1, "forka")],
            ancestry(&[("forka", "base"), ("forkb", "base")]),
        );
        assert_eq!(outcome.adopt_head(), None);
        let refusal = outcome.refusal().expect("an undecidable topology blocks");
        assert!(refusal.contains(&commit("forka")[..12]), "{refusal}");
        assert!(refusal.contains(&commit("forkb")[..12]), "{refusal}");
        assert!(refusal.contains("continuity"), "{refusal}");
    }

    /// A prior head the base already contains holds nothing to carry — the
    /// ordinary shape of an issue whose earlier PR merged.
    #[test]
    fn a_head_already_in_the_base_is_not_carried() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![work("job-1", 1, "landed")],
            ancestry(&[("base", "landed")]),
        );
        assert_eq!(outcome.adopt_head(), None);
        assert!(outcome.refusal().is_none(), "nothing to decide");
        let briefing = outcome.briefing().expect("the head is still named");
        assert!(
            briefing.contains("already contained in the base branch"),
            "{briefing}"
        );
    }

    /// Work a reviewer explicitly closed is not silently reinstated by a retry.
    #[test]
    fn explicitly_rejected_work_is_never_adopted_automatically() {
        let mut rejected = work("job-1", 1, "dead");
        rejected.disposition = Disposition::Rejected;
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![rejected],
            ancestry(&[("dead", "base")]),
        );
        assert_eq!(outcome.adopt_head(), None);
        let briefing = outcome.briefing().expect("a rejected head is still named");
        assert!(briefing.contains("explicitly rejected"), "{briefing}");
    }

    /// The intentional start-over. The work exists, is reachable, and is
    /// deliberately not carried — and the successor is told exactly that rather
    /// than left to infer it from an empty branch.
    #[test]
    fn clean_declines_and_says_the_prior_work_was_superseded() {
        let outcome = decide(
            &Continuity::Clean,
            &commit("base"),
            vec![work("job-1", 1, "dead")],
            ancestry(&[("dead", "base")]),
        );
        assert_eq!(outcome.adopt_head(), None);
        assert!(
            outcome.refusal().is_none(),
            "an explicit choice never blocks"
        );
        let briefing = outcome.briefing().expect("declining is still announced");
        assert!(briefing.contains("superseded"), "{briefing}");
        assert!(briefing.contains("NOT inherited"), "{briefing}");
    }

    /// Answering the refusal: every spelling the refusal itself used resolves.
    #[test]
    fn an_explicit_coordinate_settles_an_undecidable_topology() {
        let candidates = vec![work("job-2", 2, "forkb"), work("job-1", 1, "forka")];
        let relation = &[("forka", "base"), ("forkb", "base")];
        for coordinate in [
            "cairn://p/cairn/4602/1/builder",
            "agent/cairn-4602-builder-0",
            &commit("forka")[..8],
        ] {
            let outcome = decide(
                &Continuity::From(coordinate.to_string()),
                &commit("base"),
                candidates.clone(),
                ancestry(relation),
            );
            assert_eq!(
                outcome.adopt_head(),
                Some(commit("forka").as_str()),
                "`{coordinate}` should name execution 1's head"
            );
        }
    }

    /// A coordinate naming nothing adoptable blocks rather than quietly falling
    /// back to the base — the caller asked for something specific and did not
    /// get it.
    #[test]
    fn a_from_coordinate_that_names_nothing_refuses() {
        let outcome = decide(
            &Continuity::From("agent/never-existed".to_string()),
            &commit("base"),
            vec![work("job-1", 1, "dead")],
            ancestry(&[("dead", "base")]),
        );
        let refusal = outcome.refusal().expect("an unmatched coordinate blocks");
        assert!(refusal.contains("agent/never-existed"), "{refusal}");
        assert!(refusal.contains("adoptable"), "{refusal}");
    }

    /// The successor's briefing states inheritance as an accomplished fact and
    /// refuses to claim anything about work that was never committed.
    #[test]
    fn an_adoption_briefing_states_the_commits_are_already_in_history() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![work("job-1", 1, "dead")],
            ancestry(&[("dead", "base")]),
        );
        let briefing = outcome.briefing().expect("an adoption is announced");
        assert!(
            briefing.contains("already contains"),
            "inheritance is stated as done, not proposed: {briefing}"
        );
        assert!(
            briefing.contains("cairn://p/cairn/4602/1/builder"),
            "the immutable node reference is named: {briefing}"
        );
        assert!(
            briefing.contains(&commit("dead")[..12]),
            "the immutable commit reference is named: {briefing}"
        );
        assert!(
            briefing.contains("UNCOMMITTED did not survive"),
            "unrecoverable work is never claimed as inherited: {briefing}"
        );
    }

    /// A head a later execution already set aside is not silently reconsidered
    /// by a retry that never thought about continuity.
    #[test]
    fn a_superseded_head_is_not_adopted_by_a_later_auto_retry() {
        let mut superseded = work("job-1", 1, "dead");
        superseded.disposition = Disposition::Superseded;
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            vec![superseded],
            ancestry(&[("dead", "base")]),
        );
        assert_eq!(outcome.adopt_head(), None);
        let briefing = outcome
            .briefing()
            .expect("a superseded head is still named");
        assert!(briefing.contains("superseded"), "{briefing}");
    }

    /// Naming a coordinate is somebody deciding NOW, so it reaches past an
    /// earlier classification. It still cannot name a head the base contains,
    /// which would start the successor behind its own base.
    #[test]
    fn an_explicit_coordinate_overrides_a_prior_classification_but_not_the_base() {
        for disposition in [Disposition::Superseded, Disposition::Rejected] {
            let mut set_aside = work("job-1", 1, "dead");
            set_aside.disposition = disposition;
            let outcome = decide(
                &Continuity::From("agent/cairn-4602-builder-0".to_string()),
                &commit("base"),
                vec![set_aside],
                ancestry(&[("dead", "base")]),
            );
            assert_eq!(
                outcome.adopt_head(),
                Some(commit("dead").as_str()),
                "an explicit decision reaches past {disposition:?}"
            );
        }

        let outcome = decide(
            &Continuity::From("agent/cairn-4602-builder-0".to_string()),
            &commit("base"),
            vec![work("job-1", 1, "landed")],
            ancestry(&[("base", "landed")]),
        );
        assert!(
            outcome.refusal().is_some(),
            "a head the base already contains is never a start point"
        );
    }

    /// With no prior execution at all there is nothing to say, and the opening
    /// prompt of a first execution is unchanged.
    #[test]
    fn a_first_execution_is_told_nothing() {
        let outcome = decide(
            &Continuity::Auto,
            &commit("base"),
            Vec::new(),
            ancestry(&[]),
        );
        assert_eq!(outcome, Continuation::Fresh { prior: Vec::new() });
        assert!(outcome.briefing().is_none());
    }
}

/// The enumeration half, against a real migrated database.
///
/// Both invariants pinned here were once wrong in ways the pure policy tests
/// above could never have caught, because both live entirely in which rows come
/// back: a `clean` decision that bound only the execution that made it, and a
/// candidate set that said "a prior execution" but only checked "a different
/// execution".
#[cfg(test)]
mod enumeration_tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };

    fn snapshot_with(continuity: Continuity) -> String {
        let mut snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "build".to_string(),
                name: "Build".to_string(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![],
                edges: vec![],
            },
            Default::default(),
            Default::default(),
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        );
        snapshot.continuity = continuity;
        snapshot.to_json().unwrap()
    }

    /// One issue whose executions are described by `(seq, started_at,
    /// continuity, branch)` — a `None` branch is an execution that minted
    /// nothing. Execution ids are `exec-{seq}`.
    async fn seed(series: &[(i64, i64, Continuity, Option<&str>)]) -> LocalDb {
        let db = crate::storage::migrated_test_db("continuity-enumeration.db").await;
        let series: Vec<(i64, i64, String, Option<String>)> = series
            .iter()
            .map(|(seq, started, continuity, branch)| {
                (
                    *seq,
                    *started,
                    snapshot_with(continuity.clone()),
                    branch.map(ToString::to_string),
                )
            })
            .collect();
        db.write(move |conn| {
            let series = series.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj-1','default','Proj','proj','/repo',1,1)",
                    (),
                ).await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1','proj-1',4602,'Retry me','active',1,1)",
                    (),
                ).await?;
                for (seq, started, snapshot, branch) in &series {
                    let exec_id = format!("exec-{seq}");
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, lifecycle)
                         VALUES (?1,'build','issue-1','proj-1','failed',?2,?3,?4,'superseded')",
                        params![exec_id.as_str(), started, seq, snapshot.as_str()],
                    ).await?;
                    if let Some(branch) = branch {
                        conn.execute(
                            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, branch, created_at, updated_at)
                             VALUES (?1,?2,'builder-node','issue-1','proj-1','Builder','builder','failed',?3,?4,?4)",
                            params![format!("job-{seq}"), exec_id.as_str(), branch.as_str(), started],
                        ).await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    fn branches(candidates: &[PriorCandidate]) -> Vec<&str> {
        candidates.iter().map(|c| c.branch.as_str()).collect()
    }

    /// The ordinary retry: execution 2 sees execution 1's branch, newest first,
    /// adoptable.
    #[tokio::test(flavor = "current_thread")]
    async fn a_successor_sees_its_predecessors_branch() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (2, 20, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        assert_eq!(branches(&found), vec!["agent/b-0"]);
        assert!(!found[0].superseded);
        assert_eq!(
            found[0].node_uri.as_deref(),
            Some("cairn://p/proj/4602/1/builder"),
            "the immutable node reference names the execution that left the work"
        );
    }

    /// Review finding 1. A `clean` decision at execution 2 must still bind at
    /// execution 3, which never thought about continuity at all — otherwise the
    /// very next retry reinstates exactly what somebody explicitly set aside.
    #[tokio::test(flavor = "current_thread")]
    async fn a_clean_decision_still_binds_the_retry_after_it() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (2, 20, Continuity::Clean, Some("agent/b-1")),
            (3, 30, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-3")
            .await
            .unwrap();

        assert_eq!(
            branches(&found),
            vec!["agent/b-1", "agent/b-0"],
            "both are still ENUMERATED — supersession is never silence"
        );
        assert!(
            found[1].superseded,
            "execution 1's head stays on the far side of execution 2's decision"
        );
        assert!(
            !found[0].superseded,
            "the deciding execution's own branch is not superseded by its own decision"
        );
    }

    /// An explicit coordinate draws the same boundary a `clean` does: it too is
    /// somebody deciding what this issue's series carries forward.
    #[tokio::test(flavor = "current_thread")]
    async fn naming_a_coordinate_draws_the_same_boundary() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (
                2,
                20,
                Continuity::From("agent/b-0".to_string()),
                Some("agent/b-1"),
            ),
            (3, 30, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-3")
            .await
            .unwrap();
        assert!(
            found
                .iter()
                .find(|c| c.branch == "agent/b-0")
                .unwrap()
                .superseded
        );
    }

    /// A decision counts once ENACTED, not once stated. An execution that named
    /// a coordinate and then never minted a branch — refused because the
    /// coordinate matched nothing, or dead before preparation finished — carried
    /// nothing and settled nothing, so a later retry must still see the real
    /// commits below it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_decision_that_never_minted_a_branch_draws_no_boundary() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            // Execution 2 asked for a coordinate and never got a branch.
            (2, 20, Continuity::From("agent/typo".to_string()), None),
            (3, 30, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-3")
            .await
            .unwrap();
        assert_eq!(branches(&found), vec!["agent/b-0"]);
        assert!(
            !found[0].superseded,
            "a request that failed must not suppress a predecessor's real commits"
        );
    }

    /// The same rule for `clean`: stated, never enacted, so it settles nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn a_clean_that_never_started_draws_no_boundary() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (2, 20, Continuity::Clean, None),
            (3, 30, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-3")
            .await
            .unwrap();
        assert_eq!(branches(&found), vec!["agent/b-0"]);
        assert!(!found[0].superseded);
    }

    /// A boundary only reaches back. Execution 3's own `clean` says nothing
    /// about what execution 2 was allowed to see.
    #[tokio::test(flavor = "current_thread")]
    async fn a_later_decision_does_not_reach_backwards() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (2, 20, Continuity::Auto, None),
            (3, 30, Continuity::Clean, Some("agent/b-2")),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        assert_eq!(branches(&found), vec!["agent/b-0"]);
        assert!(!found[0].superseded);
    }

    /// Review finding 2. An older execution prepared LATE — pending while a
    /// newer one already minted and committed — must not adopt the newer
    /// execution's head, which would reverse successor direction entirely.
    #[tokio::test(flavor = "current_thread")]
    async fn an_execution_prepared_out_of_order_never_adopts_a_newer_head() {
        let db = seed(&[
            (1, 10, Continuity::Auto, Some("agent/b-0")),
            (2, 20, Continuity::Auto, Some("agent/b-1")),
            (3, 30, Continuity::Auto, Some("agent/b-2")),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        assert_eq!(
            branches(&found),
            vec!["agent/b-0"],
            "execution 2 is the successor of 1 and the predecessor of 3, never the reverse"
        );
    }

    /// Sequence is the authority, not row order or start time, when both sides
    /// have one — a clock skew between rows must not reorder a series.
    #[tokio::test(flavor = "current_thread")]
    async fn sequence_outranks_start_time() {
        let db = seed(&[
            (1, 900, Continuity::Auto, Some("agent/b-0")),
            (2, 100, Continuity::Auto, None),
        ])
        .await;
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        assert_eq!(branches(&found), vec!["agent/b-0"]);
    }

    /// The ordering must be a genuine total order across sequenced and legacy
    /// sequence-less rows together, because it filters candidates, selects the
    /// supersession boundary by maximum, and sorts the briefing.
    ///
    /// The timestamps here are the intransitive triple: under a rule that
    /// compares `seq` when both sides have one and `started_at` otherwise,
    /// exec-1 precedes exec-2 by sequence, exec-2 precedes the legacy row by
    /// time, and the legacy row precedes exec-1 by time — a cycle, which makes
    /// the boundary depend on SQLite's row order. An unsequenced row predates
    /// the column and so predates every sequenced one.
    #[tokio::test(flavor = "current_thread")]
    async fn a_legacy_sequenceless_row_orders_transitively_against_sequenced_ones() {
        let db = seed(&[
            (1, 300, Continuity::Auto, Some("agent/b-0")),
            (2, 100, Continuity::Auto, Some("agent/b-1")),
        ])
        .await;
        // The legacy row: no sequence, and a timestamp between the two above.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, lifecycle)
                     VALUES ('exec-legacy','build','issue-1','proj-1','failed',200,NULL,'{}','superseded')",
                    (),
                ).await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, branch, created_at, updated_at)
                     VALUES ('job-legacy','exec-legacy','builder-node','issue-1','proj-1','Builder','builder','failed','agent/legacy',200,200)",
                    (),
                ).await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        // From the newest execution, every other row is history, and the order
        // is newest-first with the unsequenced row at the bottom.
        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        assert_eq!(
            branches(&found),
            vec!["agent/b-0", "agent/legacy"],
            "an unsequenced row is older than every sequenced one, whatever its clock said"
        );

        // And from the unsequenced row, nothing is: it is the oldest rung, so it
        // has no predecessor to adopt rather than a cycle's worth of them.
        let from_legacy = prior_branch_bearing_jobs(&db, "issue-1", "exec-legacy")
            .await
            .unwrap();
        assert!(from_legacy.is_empty(), "got {:?}", branches(&from_legacy));
    }

    /// A boundary drawn by an unsequenced row still binds every row it precedes,
    /// so the legacy fallback is not a hole in supersession.
    #[tokio::test(flavor = "current_thread")]
    async fn the_boundary_is_stable_across_mixed_sequencing() {
        let db = seed(&[
            (1, 300, Continuity::Clean, Some("agent/b-0")),
            (2, 100, Continuity::Auto, None),
        ])
        .await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, lifecycle)
                     VALUES ('exec-legacy','build','issue-1','proj-1','failed',200,NULL,'{}','superseded')",
                    (),
                ).await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, branch, created_at, updated_at)
                     VALUES ('job-legacy','exec-legacy','builder-node','issue-1','proj-1','Builder','builder','failed','agent/legacy',200,200)",
                    (),
                ).await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let found = prior_branch_bearing_jobs(&db, "issue-1", "exec-2")
            .await
            .unwrap();
        let legacy = found.iter().find(|c| c.branch == "agent/legacy").unwrap();
        let decider = found.iter().find(|c| c.branch == "agent/b-0").unwrap();
        assert!(
            legacy.superseded,
            "the legacy row sits before execution 1's clean decision"
        );
        assert!(
            !decider.superseded,
            "the deciding execution's own branch is not superseded by its own decision"
        );
    }
}
