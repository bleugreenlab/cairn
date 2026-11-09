//! Verified rollback of a budgeted store transaction, and the coordinates an
//! unverified rollback leaves for targeted repair.
//!
//! A budgeted transaction (one automatic reconcile phase under the store lock)
//! snapshots the local bookmark view and the backing `refs/heads/*` each time it
//! reads its operation id, so a rollback to that operation can be checked
//! against exactly what the store held before. [`restore_recorded`] restores the
//! operation immediately, under the original lock, and then proves the result:
//! every local bookmark back at its snapshot target, and every touched bookmark's
//! Git head with it. What it cannot prove becomes [`RecoveryCoordinates`] in the
//! transaction's ledger, which the orchestrator persists as a durable fence
//! before releasing the lock.
//!
//! Two scopes, decided from evidence rather than from the error text:
//!
//! * [`RecoveryScope::Branch`]: the local view and the complete set of touched
//!   bookmarks are known, and only their positions or Git exports are
//!   unverified. The deferred repair ([`repair_branch_recovery`]) moves only
//!   those bookmarks and refs, on the current view, by compare-and-swap.
//! * [`RecoveryScope::Store`]: the restore ran with no affirmative outcome, a
//!   killed writer was not proven gone, the view could not be read, or a
//!   touched position cannot be restored automatically. Nothing may write the
//!   store until [`validate_store_recovery`] establishes a coherent view.
//!
//! Deferred repair never replays `jj op restore`: a historical whole-view
//! restore after the lock was released would erase work committed since.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cairn_platform::process::SurvivingProcess;
use serde::{Deserialize, Serialize};

use super::export::backing_branch_heads;
use super::packed_refs::PackedRefs;
use super::{export_git_preserving_checkout, export_git_verified, rollback_incomplete, JjEnv};
use crate::{BookmarkState, LocalBookmarkView, RecordedBookmarkRestore};

type Heads = BTreeMap<String, String>;

/// The store as one budgeted transaction found it at `pre_op`.
#[derive(Debug, Clone)]
pub(crate) struct RollbackSnapshot {
    pub(crate) pre_op: String,
    pub(crate) view: Result<LocalBookmarkView, String>,
    pub(crate) git: Result<Heads, String>,
}

impl RollbackSnapshot {
    /// Read in process, so the snapshot costs no subprocess. The view must sit
    /// at the operation `jj op log` just named; anything else means the lineage
    /// the rollback relies on is not the one it read, and is recorded as such.
    pub(crate) fn capture(store: &Path, pre_op: &str) -> Self {
        let view = crate::read_local_bookmark_view(store).and_then(|view| {
            if view.operation_id == pre_op {
                Ok(view)
            } else {
                Err(format!(
                    "the in-process view read operation {} where `jj op log` read {pre_op}",
                    view.operation_id
                ))
            }
        });
        Self {
            pre_op: pre_op.to_string(),
            view,
            git: backing_branch_heads(store),
        }
    }
}

/// What one budgeted transaction has done that its rollback must know.
#[derive(Debug, Default)]
pub(crate) struct OperationLedger {
    /// Subprocesses this transaction actually spawned. A restore whose failure
    /// left this unchanged provably never ran.
    pub(crate) spawns: u64,
    /// Writers killed at a deadline and not proven gone.
    pub(crate) survivors: Vec<SurvivingProcess>,
    pub(crate) snapshots: Vec<RollbackSnapshot>,
    pub(crate) recoveries: Vec<RecoveryCoordinates>,
}

impl OperationLedger {
    pub(crate) fn remember(&mut self, snapshot: RollbackSnapshot) {
        self.snapshots
            .retain(|existing| existing.pre_op != snapshot.pre_op);
        self.snapshots.push(snapshot);
    }

    fn snapshot(&self, pre_op: &str) -> Option<RollbackSnapshot> {
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.pre_op == pre_op)
            .cloned()
    }

    /// Every recorded recovery, plus a store-scope one for a surviving writer
    /// no rollback accounted for: a writer that may still run fences the store
    /// whether or not anything tried to undo it.
    pub(crate) fn take_recoveries(&mut self) -> Vec<RecoveryCoordinates> {
        let mut recoveries = std::mem::take(&mut self.recoveries);
        let accounted: BTreeSet<u32> = recoveries
            .iter()
            .flat_map(|recovery| recovery.survivors.iter().map(|survivor| survivor.pid))
            .collect();
        let unaccounted: Vec<SurvivorRecord> = self
            .survivors
            .iter()
            .filter(|survivor| !accounted.contains(&survivor.pid))
            .map(SurvivorRecord::from)
            .collect();
        if !unaccounted.is_empty() {
            let pre = self.snapshots.first();
            recoveries.push(RecoveryCoordinates {
                scope: RecoveryScope::Store,
                phase: RollbackFailurePhase::UnknownChildTermination,
                pre_op: pre.map(|snapshot| snapshot.pre_op.clone()),
                pre_view: pre.and_then(|snapshot| {
                    snapshot
                        .view
                        .as_ref()
                        .ok()
                        .map(|view| view.bookmarks.clone())
                }),
                pre_git: pre.and_then(|snapshot| snapshot.git.clone().ok()),
                failed_view: None,
                bookmarks: Vec::new(),
                survivors: unaccounted,
                evidence: vec![
                    "a killed jj writer was not proven gone before the cleanup deadline".into(),
                ],
            });
        }
        recoveries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryScope {
    /// Only the recorded bookmarks are fenced; the rest of the store is known.
    Branch,
    /// The store's view is not established; nothing may write it.
    Store,
}

impl RecoveryScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Store => "store",
        }
    }
}

/// Where the immediate rollback stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackFailurePhase {
    /// A killed writer was not proven gone, so no restore was attempted.
    UnknownChildTermination,
    /// The restore was refused before any process was spawned. The forward
    /// mutation it was meant to undo may still be in place.
    RestoreNotSpawned,
    /// The restore ran and gave no affirmative outcome.
    RestoreOutcomeUnknown,
    /// The restore exited successfully and the view does not match the
    /// snapshot.
    RestoreUnverified,
    /// The view was restored and the Git export of a touched bookmark is not
    /// proven.
    ExportUnverified,
    /// No usable snapshot to verify against.
    SnapshotMissing,
    /// A deferred targeted repair outlived its allowance. Its in-process
    /// worker may still commit, so the store is fenced until it is known to
    /// have finished and the view is validated.
    RepairInterrupted,
}

impl RollbackFailurePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnknownChildTermination => "unknown_child_termination",
            Self::RestoreNotSpawned => "restore_not_spawned",
            Self::RestoreOutcomeUnknown => "restore_outcome_unknown",
            Self::RestoreUnverified => "restore_unverified",
            Self::ExportUnverified => "export_unverified",
            Self::SnapshotMissing => "snapshot_missing",
            Self::RepairInterrupted => "repair_interrupted",
        }
    }
}

/// One touched bookmark: where it was, where the failure left it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookmarkRecovery {
    pub bookmark: String,
    /// Its target before the attempt; the repair puts it back here.
    pub pre: BookmarkState,
    /// `refs/heads/<bookmark>` before the attempt, `None` when absent.
    pub pre_git: Option<String>,
    /// Its target as the failure left it, before any restore ran.
    pub failed: BookmarkState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurvivorRecord {
    pub pid: u32,
    pub process_group: u32,
    pub start_token: Option<String>,
    pub outstanding: String,
}

impl From<&SurvivingProcess> for SurvivorRecord {
    fn from(survivor: &SurvivingProcess) -> Self {
        Self {
            pid: survivor.pid,
            process_group: survivor.process_group,
            start_token: survivor.start_token.clone(),
            outstanding: survivor.outstanding.clone(),
        }
    }
}

/// What an unverified rollback leaves, enough to repair it without guessing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCoordinates {
    pub scope: RecoveryScope,
    pub phase: RollbackFailurePhase,
    /// The operation the immediate rollback targeted. Diagnostic only: no
    /// deferred path restores it.
    pub pre_op: Option<String>,
    /// Every local bookmark before the attempt, for a store-scope validation.
    #[serde(default)]
    pub pre_view: Option<BTreeMap<String, BookmarkState>>,
    #[serde(default)]
    pub pre_git: Option<Heads>,
    /// Every local bookmark as the failure left it, when readable.
    #[serde(default)]
    pub failed_view: Option<BTreeMap<String, BookmarkState>>,
    /// The touched bookmarks; complete for a branch scope.
    pub bookmarks: Vec<BookmarkRecovery>,
    pub survivors: Vec<SurvivorRecord>,
    /// The checks that decided the scope, in order.
    pub evidence: Vec<String>,
}

impl RecoveryCoordinates {
    pub fn affected(&self) -> impl Iterator<Item = &str> {
        self.bookmarks
            .iter()
            .map(|bookmark| bookmark.bookmark.as_str())
    }

    pub fn describe(&self) -> String {
        let affected: Vec<&str> = self.affected().collect();
        format!(
            "{} recovery ({}) from operation {}; affected bookmark(s): {}{}",
            self.scope.as_str(),
            self.phase.as_str(),
            self.pre_op.as_deref().unwrap_or("<unknown>"),
            if affected.is_empty() {
                "<not established>".to_string()
            } else {
                affected.join(", ")
            },
            if self.survivors.is_empty() {
                String::new()
            } else {
                format!(
                    "; surviving writer pid(s) {}",
                    self.survivors
                        .iter()
                        .map(|survivor| survivor.pid.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        )
    }
}

/// The bookmarks whose targets differ between two views, and so the ones the
/// transaction moved: under the store lock every local-view change between the
/// snapshot and the failure is the transaction's own.
pub(crate) fn touched(pre: &LocalBookmarkView, failed: &LocalBookmarkView) -> Vec<String> {
    pre.bookmarks
        .keys()
        .chain(failed.bookmarks.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|name| pre.state(name) != failed.state(name))
        .cloned()
        .collect()
}

struct Attempt<'a> {
    jj: &'a JjEnv,
    op_id: &'a str,
    snapshot: Option<RollbackSnapshot>,
    failed: Result<LocalBookmarkView, String>,
    evidence: Vec<String>,
}

impl Attempt<'_> {
    fn pre_view(&self) -> Option<&LocalBookmarkView> {
        self.snapshot.as_ref().and_then(|s| s.view.as_ref().ok())
    }

    fn pre_git(&self) -> Option<&Heads> {
        self.snapshot.as_ref().and_then(|s| s.git.as_ref().ok())
    }

    fn coordinates(
        &self,
        scope: RecoveryScope,
        phase: RollbackFailurePhase,
        bookmarks: &[String],
    ) -> RecoveryCoordinates {
        let failed = self.failed.as_ref().ok();
        RecoveryCoordinates {
            scope,
            phase,
            pre_op: Some(self.op_id.to_string()),
            pre_view: self.pre_view().map(|view| view.bookmarks.clone()),
            pre_git: self.pre_git().cloned(),
            failed_view: failed.map(|view| view.bookmarks.clone()),
            bookmarks: bookmarks
                .iter()
                .map(|name| BookmarkRecovery {
                    bookmark: name.clone(),
                    pre: self.pre_view().map(|v| v.state(name)).unwrap_or_default(),
                    pre_git: self.pre_git().and_then(|git| git.get(name).cloned()),
                    failed: failed.map(|v| v.state(name)).unwrap_or_default(),
                })
                .collect(),
            survivors: self
                .jj
                .ledger()
                .survivors
                .iter()
                .map(SurvivorRecord::from)
                .collect(),
            evidence: self.evidence.clone(),
        }
    }

    /// Branch scope when the touched set is complete and every position in it
    /// can be restored by compare-and-swap; store scope otherwise.
    fn scope_for(&mut self, touched: Option<&[String]>) -> RecoveryScope {
        let Some(touched) = touched else {
            self.evidence
                .push("the touched bookmark set could not be established".into());
            return RecoveryScope::Store;
        };
        if touched.is_empty() {
            self.evidence
                .push("no bookmark difference explains the failure".into());
            return RecoveryScope::Store;
        }
        let Some(pre) = self.pre_view() else {
            return RecoveryScope::Store;
        };
        let conflicted: Vec<&String> = touched
            .iter()
            .filter(|name| pre.state(name).is_conflicted())
            .collect();
        if !conflicted.is_empty() {
            self.evidence.push(format!(
                "touched bookmark(s) {} were conflicted before the attempt, which targeted \
                 repair does not restore",
                conflicted
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            return RecoveryScope::Store;
        }
        RecoveryScope::Branch
    }

    fn record(
        mut self,
        phase: RollbackFailurePhase,
        touched: Option<Vec<String>>,
        cause: &str,
    ) -> String {
        // A writer killed at any step of this transaction, the export after a
        // successful restore included, may still be writing anywhere in the
        // store. That outranks whatever the bookmark comparison found.
        let (scope, phase) = if self.jj.writer_may_survive() {
            self.evidence.push(
                "a killed jj writer was not proven gone, so the scope is the whole store".into(),
            );
            (
                RecoveryScope::Store,
                RollbackFailurePhase::UnknownChildTermination,
            )
        } else {
            (self.scope_for(touched.as_deref()), phase)
        };
        let coordinates = self.coordinates(scope, phase, touched.as_deref().unwrap_or_default());
        let message = rollback_incomplete(&format!(
            "rolling the store back to operation {} did not complete: {cause}. Recorded {}. \
             Nothing may publish or rewrite the affected bookmarks until the recorded \
             recovery verifies them.",
            self.op_id,
            coordinates.describe()
        ));
        log::error!(
            "jj rollback: {message}; evidence: {}",
            coordinates.evidence.join("; ")
        );
        self.jj.ledger().recoveries.push(coordinates);
        message
    }
}

/// Restore `op_id` under the caller's store lock and prove the restore, or
/// record why it is not proven. Budgeted environments only.
pub(crate) fn restore_recorded(jj: &JjEnv, store: &Path, op_id: &str) -> Result<(), String> {
    let snapshot = jj.ledger().snapshot(op_id);
    let mut attempt = Attempt {
        jj,
        op_id,
        snapshot,
        failed: crate::read_local_bookmark_view(store),
        evidence: Vec::new(),
    };
    let touched_known = match (attempt.pre_view(), attempt.failed.as_ref()) {
        (Some(pre), Ok(failed)) => Some(touched(pre, failed)),
        _ => None,
    };
    if let Err(error) = &attempt.failed {
        attempt
            .evidence
            .push(format!("the failed view could not be read: {error}"));
    }
    match attempt.snapshot.as_ref().map(|s| (&s.view, &s.git)) {
        None => attempt
            .evidence
            .push("no snapshot was captured for this operation".into()),
        Some((Err(error), _)) => attempt
            .evidence
            .push(format!("the snapshot view is unusable: {error}")),
        Some((_, Err(error))) => attempt
            .evidence
            .push(format!("the snapshot git heads are unusable: {error}")),
        Some(_) => {}
    }

    if jj.writer_may_survive() {
        attempt
            .evidence
            .push("a killed writer was not proven gone; no restore was attempted".into());
        let message = attempt.record(
            RollbackFailurePhase::UnknownChildTermination,
            None,
            "a timed-out writer may still be running",
        );
        return Err(message);
    }

    let spawns_before = jj.ledger().spawns;
    let restored = super::packed_refs::with_packed_refs_patience_for(
        jj,
        PackedRefs::JjStore(store),
        "jj op restore",
        || {
            jj.run(
                store,
                &[
                    "op",
                    "restore",
                    "--what",
                    "repo",
                    op_id,
                    "--ignore-working-copy",
                ],
                "jj op restore",
            )
        },
    );
    if let Err(error) = restored {
        if jj.writer_may_survive() {
            attempt
                .evidence
                .push("the restore itself was killed and not proven gone".into());
            return Err(attempt.record(
                RollbackFailurePhase::UnknownChildTermination,
                None,
                &error,
            ));
        }
        if jj.ledger().spawns == spawns_before {
            attempt
                .evidence
                .push("the restore was refused before any process was spawned".into());
            if touched_known.as_ref().is_some_and(Vec::is_empty) {
                // Nothing in the local view moved, so there is nothing to undo
                // and no Git head followed a move.
                log::warn!(
                    "jj rollback: restore to {op_id} was refused before spawning, and no local \
                     bookmark had moved since it: {error}"
                );
                return Ok(());
            }
            return Err(attempt.record(
                RollbackFailurePhase::RestoreNotSpawned,
                touched_known,
                &error,
            ));
        }
        attempt
            .evidence
            .push("the restore was spawned and gave no affirmative outcome".into());
        return Err(attempt.record(RollbackFailurePhase::RestoreOutcomeUnknown, None, &error));
    }

    let Some(pre) = attempt.pre_view().cloned() else {
        // Restore anyway is right; proving it is not possible, so there is
        // nothing exact to name and only the global export applies.
        let _ = export_git_preserving_checkout(jj, store, true, "jj git export (after op restore)");
        return Err(attempt.record(
            RollbackFailurePhase::SnapshotMissing,
            None,
            "the restore ran, and there is no snapshot to verify it against",
        ));
    };
    match crate::read_local_bookmark_view(store) {
        Ok(after) if after.bookmarks == pre.bookmarks => {}
        Ok(after) => {
            let differing = touched(&pre, &after);
            attempt.evidence.push(format!(
                "after the restore, bookmark(s) {} differ from the snapshot",
                differing.join(", ")
            ));
            return Err(attempt.record(
                RollbackFailurePhase::RestoreUnverified,
                None,
                "the restored view does not match the snapshot",
            ));
        }
        Err(error) => {
            attempt
                .evidence
                .push(format!("the restored view could not be read: {error}"));
            return Err(attempt.record(
                RollbackFailurePhase::RestoreUnverified,
                None,
                "the restored view could not be read",
            ));
        }
    }

    // Every touched bookmark must export to its restored target, and every
    // bookmark whose Git head agreed with it before the attempt must still.
    let touched_set: BTreeSet<String> = touched_known
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let pre_git = attempt.pre_git().cloned();
    let must_match: Vec<String> = pre
        .bookmarks
        .iter()
        .filter(|(name, state)| {
            touched_set.contains(*name)
                || pre_git
                    .as_ref()
                    .is_some_and(|git| git.get(*name).map(String::as_str) == state.commit())
        })
        .map(|(name, _)| name.clone())
        .chain(
            touched_set
                .iter()
                .filter(|name| !pre.bookmarks.contains_key(*name))
                .cloned(),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let expect: Vec<(&str, &str)> = touched_set
        .iter()
        .filter_map(|name| Some((name.as_str(), pre.bookmarks.get(name)?.commit()?)))
        .collect();
    // A named export requires exact expectations and cannot delete a ref. When
    // no touched bookmark has a restored target to name (nothing moved, or the
    // restore re-established an absence), the global export publishes the
    // restored view; `must_match` below verifies the outcome either way.
    let exported = if expect.is_empty() {
        export_git_preserving_checkout(jj, store, true, "jj git export (after op restore)")
    } else {
        export_git_verified(jj, store, true, "jj git export (after op restore)", &expect)
    };
    let mut unverified: Vec<String> = Vec::new();
    if let Err(error) = &exported {
        attempt
            .evidence
            .push(format!("the export after the restore failed: {error}"));
    }
    match backing_branch_heads(store) {
        Ok(git) => {
            for name in &must_match {
                let expected = pre.state(name);
                if git.get(name).map(String::as_str) != expected.commit() {
                    unverified.push(name.clone());
                }
            }
        }
        Err(error) => {
            attempt.evidence.push(format!(
                "the git heads could not be read after the export: {error}"
            ));
            unverified.extend(must_match.iter().cloned());
        }
    }
    if exported.is_ok() && unverified.is_empty() {
        return Ok(());
    }
    if !unverified.is_empty() {
        attempt.evidence.push(format!(
            "git head(s) of {} do not name the restored target",
            unverified.join(", ")
        ));
    }
    // The local view is proven; what remains is the export of named bookmarks.
    let affected: Option<Vec<String>> = touched_known.map(|touched| {
        touched
            .into_iter()
            .chain(unverified)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    });
    let cause = exported
        .err()
        .unwrap_or_else(|| "the restored git heads are not verified".into());
    Err(attempt.record(RollbackFailurePhase::ExportUnverified, affected, &cause))
}

/// What a deferred repair or validation established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairVerdict {
    /// Every recorded bookmark and Git head is at its pre-attempt position and
    /// nothing else moved. The fence may be cleared.
    Verified { operation_id: Option<String> },
    /// The repair did not complete; the fence stays, with this reason.
    Refused(String),
}

/// Put a branch recovery's bookmarks and Git heads back, on the current view.
///
/// The caller holds the store lock and has established no writer of the failed
/// attempt survives. Compare-and-swap per bookmark from exactly the recorded
/// failed or pre-attempt state, a scoped export of exactly those names, then
/// verification that the recorded bookmarks and heads are at their targets and
/// that every other bookmark and head is where it was before this repair.
/// `merged_missing` identifies host-attested merged, unheld branches whose
/// recorded Git heads have disappeared. Recreate those heads at their saved
/// targets, rather than requiring ancestry that squash merges do not preserve.
pub fn repair_branch_recovery(
    store: &Path,
    coordinates: &RecoveryCoordinates,
    recovery_id: &str,
    merged_missing: &BTreeSet<String>,
) -> RepairVerdict {
    if coordinates.scope != RecoveryScope::Branch || coordinates.bookmarks.is_empty() {
        return RepairVerdict::Refused(
            "only a branch recovery with recorded bookmarks is repaired".into(),
        );
    }
    // Defensive: a branch record never carries a survivor, but one that does
    // is not repaired while that writer may still run.
    if let Some(reason) = coordinates.survivors.iter().find_map(survivor_outstanding) {
        return RepairVerdict::Refused(reason);
    }
    let before_view = match crate::read_local_bookmark_view(store) {
        Ok(view) => view,
        Err(error) => return RepairVerdict::Refused(format!("read the current view: {error}")),
    };
    let before_git = match backing_branch_heads(store) {
        Ok(git) => git,
        Err(error) => {
            return RepairVerdict::Refused(format!("read the current git heads: {error}"))
        }
    };
    for bookmark in &coordinates.bookmarks {
        if merged_missing.contains(&bookmark.bookmark)
            && (bookmark.pre.commit().is_none()
                || bookmark.pre_git.as_deref() != bookmark.pre.commit())
        {
            return RepairVerdict::Refused(format!(
                "cannot recreate `{}`: its recorded Git head differs from its saved bookmark",
                bookmark.bookmark
            ));
        }
    }
    let restores: Vec<RecordedBookmarkRestore> = coordinates
        .bookmarks
        .iter()
        .map(|bookmark| RecordedBookmarkRestore {
            bookmark: bookmark.bookmark.clone(),
            target: bookmark.pre.clone(),
            replaceable: vec![bookmark.failed.clone()],
            recreate_missing_git_head: merged_missing.contains(&bookmark.bookmark),
        })
        .collect();
    let outcome = match crate::restore_recorded_bookmarks(
        store,
        &restores,
        &format!("cairn: targeted recovery {recovery_id}"),
    ) {
        Ok(outcome) => outcome,
        Err(error) => return RepairVerdict::Refused(error),
    };
    if !outcome.export_failures.is_empty() {
        return RepairVerdict::Refused(format!(
            "the scoped export refused: {}",
            outcome.export_failures.join("; ")
        ));
    }
    verify_repaired(store, coordinates, &before_view, &before_git)
        .map(|()| RepairVerdict::Verified {
            operation_id: Some(outcome.operation_id),
        })
        .unwrap_or_else(RepairVerdict::Refused)
}

fn verify_repaired(
    store: &Path,
    coordinates: &RecoveryCoordinates,
    before_view: &LocalBookmarkView,
    before_git: &Heads,
) -> Result<(), String> {
    let after_view = crate::read_local_bookmark_view(store)
        .map_err(|error| format!("read the repaired view: {error}"))?;
    let after_git = backing_branch_heads(store)
        .map_err(|error| format!("read the repaired git heads: {error}"))?;
    let recorded: BTreeSet<&str> = coordinates.affected().collect();
    let mut problems = Vec::new();
    for bookmark in &coordinates.bookmarks {
        let name = bookmark.bookmark.as_str();
        let state = after_view.state(name);
        if state != bookmark.pre {
            problems.push(format!("`{name}` is at {state}, not {}", bookmark.pre));
        }
        let head = after_git.get(name).map(String::as_str);
        if head != bookmark.pre.commit() {
            problems.push(format!(
                "refs/heads/{name} is at {}, not {}",
                head.unwrap_or("<absent>"),
                bookmark.pre.commit().unwrap_or("<absent>")
            ));
        }
    }
    for name in before_view
        .bookmarks
        .keys()
        .chain(after_view.bookmarks.keys())
        .collect::<BTreeSet<_>>()
    {
        if !recorded.contains(name.as_str()) && before_view.state(name) != after_view.state(name) {
            problems.push(format!(
                "unrelated bookmark `{name}` moved during the repair"
            ));
        }
    }
    for name in before_git
        .keys()
        .chain(after_git.keys())
        .collect::<BTreeSet<_>>()
    {
        if !recorded.contains(name.as_str()) && before_git.get(name) != after_git.get(name) {
            problems.push(format!(
                "unrelated refs/heads/{name} moved during the repair"
            ));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the repair is not verified: {}",
            problems.join("; ")
        ))
    }
}

/// What validating a store-scope recovery established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreValidation {
    /// The view is coherent and agrees with its pre-attempt snapshot,
    /// including every Git head the snapshot held consistent: nothing to repair.
    Resolved,
    /// The view is coherent, and these bookmarks still need targeted repair.
    Downgrade(RecoveryCoordinates),
    /// Still not established, with the reason.
    Uncertain(String),
}

/// Affirmatively establish a store-scope recovery's view, or say why not.
///
/// Each recorded survivor must be proven gone first. Then every local bookmark
/// must sit either where the snapshot had it or, for a bookmark the attempt
/// moved, where the failure left it, all of a piece: an `op restore` is one
/// operation, so a coherent view is wholly before or wholly after it. Any other
/// position is unexplained and keeps the fence.
pub fn validate_store_recovery(store: &Path, coordinates: &RecoveryCoordinates) -> StoreValidation {
    for survivor in &coordinates.survivors {
        if let Some(reason) = survivor_outstanding(survivor) {
            return StoreValidation::Uncertain(reason);
        }
    }
    let Some(pre) = coordinates.pre_view.as_ref() else {
        return StoreValidation::Uncertain(
            "no pre-attempt view was recorded, so the current view cannot be checked".into(),
        );
    };
    let current = match crate::read_local_bookmark_view(store) {
        Ok(view) => view,
        Err(error) => return StoreValidation::Uncertain(format!("read the current view: {error}")),
    };
    let git = match backing_branch_heads(store) {
        Ok(git) => git,
        Err(error) => return StoreValidation::Uncertain(format!("read the git heads: {error}")),
    };
    let pre_view = LocalBookmarkView {
        operation_id: coordinates.pre_op.clone().unwrap_or_default(),
        bookmarks: pre.clone(),
    };
    let failed_view = coordinates
        .failed_view
        .as_ref()
        .map(|failed| LocalBookmarkView {
            operation_id: String::new(),
            bookmarks: failed.clone(),
        });
    let moved = failed_view
        .as_ref()
        .map(|failed| touched(&pre_view, failed));
    let differing = touched(&pre_view, &current);
    let restored = differing.is_empty();
    let at_failure = match (&failed_view, &moved) {
        (Some(failed), Some(moved)) => !moved.is_empty() && touched(failed, &current).is_empty(),
        _ => false,
    };
    if !restored && !at_failure {
        return StoreValidation::Uncertain(format!(
            "bookmark(s) {} sit neither where the snapshot had them nor where the failure \
             left them",
            differing.join(", ")
        ));
    }
    let pre_git = coordinates.pre_git.clone().unwrap_or_default();
    let mut affected: BTreeSet<String> = moved
        .clone()
        .unwrap_or_default()
        .into_iter()
        .chain(coordinates.affected().map(str::to_string))
        .collect();
    if restored {
        affected.retain(|name| git.get(name).map(String::as_str) != pre_view.state(name).commit());
        for (name, state) in pre {
            if pre_git.get(name).map(String::as_str) == state.commit()
                && git.get(name).map(String::as_str) != state.commit()
            {
                affected.insert(name.clone());
            }
        }
        if affected.is_empty() {
            return StoreValidation::Resolved;
        }
    }
    let mut downgraded = coordinates.clone();
    downgraded.scope = RecoveryScope::Branch;
    downgraded.survivors.clear();
    let failed = failed_view.unwrap_or_else(|| pre_view.clone());
    downgraded.bookmarks = affected
        .into_iter()
        .map(|name| BookmarkRecovery {
            pre: pre_view.state(&name),
            pre_git: pre_git.get(&name).cloned(),
            failed: failed.state(&name),
            bookmark: name,
        })
        .collect();
    if downgraded
        .bookmarks
        .iter()
        .any(|bookmark| bookmark.pre.is_conflicted())
    {
        return StoreValidation::Uncertain(
            "a touched bookmark was conflicted before the attempt".into(),
        );
    }
    downgraded.evidence.push(format!(
        "validated: the view is coherent ({}); {} bookmark(s) owe targeted repair",
        if restored {
            "restored"
        } else {
            "as the failure left it"
        },
        downgraded.bookmarks.len()
    ));
    StoreValidation::Downgrade(downgraded)
}

/// Store-scope coordinates for a targeted repair that outlived its allowance.
///
/// The repair is one jj transaction, so the store is wholly before it or
/// wholly after it. The baselines are taken from the CURRENT view, with each
/// recorded bookmark at its failed state (repair not committed) or at its
/// pre-attempt target (repair committed), so a later validation can tell the
/// two apart without mistaking work admitted since the original failure for
/// an unexplained movement. A view that cannot be read leaves no baseline, and
/// the fence then stays until it can be established.
pub fn interrupted_repair_coordinates(
    store: &Path,
    branch: &RecoveryCoordinates,
) -> RecoveryCoordinates {
    let mut coordinates = branch.clone();
    coordinates.scope = RecoveryScope::Store;
    coordinates.phase = RollbackFailurePhase::RepairInterrupted;
    match (
        crate::read_local_bookmark_view(store),
        backing_branch_heads(store),
    ) {
        (Ok(view), Ok(git)) => {
            let mut pre = view.bookmarks.clone();
            let failed = view.bookmarks.clone();
            let mut pre_git = git;
            for bookmark in &mut coordinates.bookmarks {
                bookmark.failed = view.state(&bookmark.bookmark);
                let name = bookmark.bookmark.clone();
                if bookmark.pre.is_absent() {
                    pre.remove(&name);
                } else {
                    pre.insert(name.clone(), bookmark.pre.clone());
                }
                match bookmark.pre.commit() {
                    Some(commit) => {
                        pre_git.insert(name, commit.to_string());
                    }
                    None => {
                        pre_git.remove(&name);
                    }
                }
            }
            coordinates.pre_view = Some(pre);
            coordinates.failed_view = Some(failed);
            coordinates.pre_git = Some(pre_git);
        }
        (view, git) => {
            coordinates.pre_view = None;
            coordinates.failed_view = None;
            if let Err(error) = view {
                coordinates
                    .evidence
                    .push(format!("the view could not be read at escalation: {error}"));
            }
            if let Err(error) = git {
                coordinates.evidence.push(format!(
                    "the git heads could not be read at escalation: {error}"
                ));
            }
        }
    }
    coordinates.evidence.push(
        "a targeted repair outlived its allowance; its worker may still commit, so the whole \
         store is fenced until it has finished and the view is validated"
            .into(),
    );
    coordinates
}

/// Why a recorded survivor is not proven gone, or `None` when it is.
fn survivor_outstanding(survivor: &SurvivorRecord) -> Option<String> {
    use cairn_common::process_identity::{process_presence, ProcessPresence};
    let leader = match survivor.start_token.as_deref() {
        Some(token) => process_presence(survivor.pid, token),
        None if cairn_common::process_identity::process_is_alive(survivor.pid) => {
            ProcessPresence::Unverifiable
        }
        None => ProcessPresence::Gone,
    };
    if !leader.is_gone() {
        return Some(format!(
            "writer pid {} is not proven gone ({leader:?})",
            survivor.pid
        ));
    }
    #[cfg(unix)]
    match cairn_common::process_identity::process_group_members(survivor.process_group) {
        Ok(members) if members.is_empty() => None,
        Ok(members) => Some(format!(
            "process group {} still holds {} live process(es)",
            survivor.process_group,
            members.len()
        )),
        Err(error) => Some(format!(
            "process group {} membership unreadable: {error}",
            survivor.process_group
        )),
    }
    #[cfg(not(unix))]
    None
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::super::*;
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    const FEATURE: &str = "agent/cairn-1-builder-0";
    const OTHER: &str = "agent/cairn-2-builder-0";

    struct Fixture {
        _home: TempDir,
        proj: TempDir,
        wts: TempDir,
        jj: JjEnv,
        store: std::path::PathBuf,
        main: String,
        feature: String,
    }

    /// A store whose `FEATURE` carries one commit over `main`, exported.
    fn fixture() -> Option<Fixture> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        let wts = TempDir::new().unwrap();
        init_project(proj.path());
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("feature");
        add_workspace(&jj, &store, &ws, FEATURE, "main", None).unwrap();
        std::fs::write(ws.join("feature.rs"), "feature\n").unwrap();
        seal(&jj, &ws, "feature work", None).unwrap();
        let main = bookmark_commit(&jj, &store, "main").unwrap();
        let feature = bookmark_commit(&jj, &store, FEATURE).unwrap();
        export_git_verified(&jj, &store, true, "fixture export", &[(FEATURE, &feature)]).unwrap();
        assert_ne!(main, feature);
        Some(Fixture {
            _home: home,
            proj,
            wts,
            jj,
            store,
            main,
            feature,
        })
    }

    impl Fixture {
        fn git_head(&self, branch: &str) -> Option<String> {
            backing_branch_heads(&self.store)
                .unwrap()
                .get(branch)
                .cloned()
        }

        /// The forward mutation a failed attempt leaves: `branch` moved to
        /// `target` and exported, so the store and Git both carry it.
        fn move_and_export(&self, jj: &JjEnv, branch: &str, target: &str) {
            jj.run(
                &self.store,
                &[
                    "bookmark",
                    "set",
                    branch,
                    "-r",
                    target,
                    "--allow-backwards",
                    "--ignore-working-copy",
                ],
                "test forward mutation",
            )
            .unwrap();
            export_git_verified(
                jj,
                &self.store,
                true,
                "test forward export",
                &[(branch, target)],
            )
            .unwrap();
        }

        fn budgeted(&self) -> JjEnv {
            self.jj.with_operation_budget(Duration::from_secs(60))
        }
    }

    fn only_recovery(jj: &JjEnv) -> RecoveryCoordinates {
        let mut recoveries = jj.take_recoveries();
        assert_eq!(recoveries.len(), 1, "{recoveries:?}");
        recoveries.remove(0)
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_verified_rollback_restores_bookmark_and_git_and_records_nothing() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        assert_eq!(fx.git_head(FEATURE).as_deref(), Some(fx.main.as_str()));

        restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap();
        assert!(jj.take_recoveries().is_empty());
        // Read outside the transaction: its forward clones are barred once
        // cleanup has started.
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.feature.clone())
        );
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
    }

    /// The 531e1867 shape: the forward rebase moved and exported the branch,
    /// then the restore was refused before spawning. That refusal made no
    /// change, and the forward mutation is still in place.
    #[test]
    #[serial_test::serial(jj)]
    fn a_restore_refused_before_spawning_after_a_forward_move_records_a_branch_recovery() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        jj.expire_cleanup_for_test();

        let error = restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        assert!(error.contains(ROLLBACK_INCOMPLETE), "{error}");
        let recovery = only_recovery(&jj);
        assert_eq!(recovery.scope, RecoveryScope::Branch);
        assert_eq!(recovery.phase, RollbackFailurePhase::RestoreNotSpawned);
        assert_eq!(recovery.pre_op.as_deref(), Some(op.as_str()));
        assert_eq!(
            recovery.bookmarks,
            vec![BookmarkRecovery {
                bookmark: FEATURE.into(),
                pre: BookmarkState::normal(&fx.feature),
                pre_git: Some(fx.feature.clone()),
                failed: BookmarkState::normal(&fx.main),
            }]
        );
        // Nothing ran: the branch is still where the failure left it.
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.main.clone())
        );

        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.feature.clone())
        );
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
        // Repeating a verified repair changes nothing and verifies again.
        let again = repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        assert!(matches!(again, RepairVerdict::Verified { .. }), "{again:?}");
    }

    /// The c43a1f1e shape: `op restore` completed and the export after it did
    /// not. The view is known; only the Git head is unverified. Later
    /// unrelated work must survive the targeted repair exactly.
    #[test]
    #[serial_test::serial(jj)]
    fn a_restore_whose_export_fails_is_repaired_without_touching_later_unrelated_work() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        let lock = fx
            .proj
            .path()
            .join(".git/refs/heads")
            .join(format!("{FEATURE}.lock"));
        std::fs::write(&lock, "").unwrap();

        let error = restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        assert!(error.contains(ROLLBACK_INCOMPLETE), "{error}");
        let recovery = only_recovery(&jj);
        assert_eq!(recovery.scope, RecoveryScope::Branch, "{recovery:?}");
        assert_eq!(recovery.phase, RollbackFailurePhase::ExportUnverified);
        assert_eq!(recovery.affected().collect::<Vec<_>>(), vec![FEATURE]);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.feature.clone())
        );
        assert_eq!(
            fx.git_head(FEATURE),
            Some(fx.main.clone()),
            "git kept the failed tip"
        );

        // Unrelated work lands after the failure, as the lock is released.
        let ws = fx.wts.path().join("other");
        add_workspace(&fx.jj, &fx.store, &ws, OTHER, "main", None).unwrap();
        std::fs::write(ws.join("other.rs"), "other\n").unwrap();
        seal(&fx.jj, &ws, "unrelated work", None).unwrap();
        let other = bookmark_commit(&fx.jj, &fx.store, OTHER).unwrap();
        // Named, so the still-locked FEATURE ref is not part of this export.
        export_git_verified(
            &fx.jj,
            &fx.store,
            true,
            "unrelated export",
            &[(OTHER, &other)],
        )
        .unwrap();
        assert_eq!(fx.git_head(OTHER).as_deref(), Some(other.as_str()));

        // The repair is refused while Git still refuses the ref, and retried.
        let refused =
            repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        assert!(matches!(refused, RepairVerdict::Refused(_)), "{refused:?}");
        std::fs::remove_file(&lock).unwrap();
        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, OTHER),
            Some(other.clone())
        );
        assert_eq!(
            fx.git_head(OTHER),
            Some(other),
            "unrelated git ref survives exactly"
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_repair_refuses_a_bookmark_that_moved_after_the_failure() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        jj.expire_cleanup_for_test();
        restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        let recovery = only_recovery(&jj);

        // Someone committed new work on the branch since.
        let ws = fx.wts.path().join("again");
        add_workspace(&fx.jj, &fx.store, &ws, OTHER, "main", None).unwrap();
        std::fs::write(ws.join("new.rs"), "new\n").unwrap();
        seal(&fx.jj, &ws, "new work", None).unwrap();
        let new_work = bookmark_commit(&fx.jj, &fx.store, OTHER).unwrap();
        fx.move_and_export(&fx.jj, FEATURE, &new_work);

        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        let RepairVerdict::Refused(reason) = verdict else {
            panic!("a moved bookmark must refuse the repair: {verdict:?}");
        };
        assert!(reason.contains("did not record"), "{reason}");
        assert_eq!(bookmark_commit(&fx.jj, &fx.store, FEATURE), Some(new_work));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_two_bookmark_recovery_recreates_a_merged_deleted_head() {
        let Some(fx) = fixture() else { return };
        // The feature tip is not an ancestor of main, as with a squash landing.
        fx.move_and_export(&fx.jj, OTHER, &fx.feature);
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        jj.expire_cleanup_for_test();
        restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        let mut recovery = only_recovery(&jj);
        recovery.phase = RollbackFailurePhase::ExportUnverified;
        recovery.bookmarks.push(BookmarkRecovery {
            bookmark: OTHER.into(),
            pre: BookmarkState::normal(&fx.feature),
            pre_git: Some(fx.feature.clone()),
            failed: BookmarkState::normal(&fx.feature),
        });
        git(
            fx.proj.path(),
            &[
                "update-ref",
                "-d",
                &format!("refs/heads/{OTHER}"),
                &fx.feature,
            ],
        );
        let merged_missing = BTreeSet::from([OTHER.to_string()]);
        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "merged-retirement", &merged_missing);
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        for name in [FEATURE, OTHER] {
            assert_eq!(fx.git_head(name), Some(fx.feature.clone()));
            assert_eq!(
                bookmark_commit(&fx.jj, &fx.store, name),
                Some(fx.feature.clone())
            );
        }
        let interrupted = interrupted_repair_coordinates(&fx.store, &recovery);
        assert_eq!(
            validate_store_recovery(&fx.store, &interrupted),
            StoreValidation::Resolved
        );
        // A subsequent foreign Git movement cannot disappear into the baseline.
        git(
            fx.proj.path(),
            &["update-ref", &format!("refs/heads/{OTHER}"), &fx.main],
        );
        assert!(matches!(
            validate_store_recovery(&fx.store, &interrupted),
            StoreValidation::Downgrade(_)
        ));
        let verdict = repair_branch_recovery(&fx.store, &recovery, "foreign-head", &merged_missing);
        assert!(matches!(verdict, RepairVerdict::Refused(_)), "{verdict:?}");
        assert_eq!(fx.git_head(OTHER), Some(fx.main.clone()));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn an_absent_head_requires_merge_evidence_and_an_unchanged_saved_bookmark() {
        let Some(fx) = fixture() else { return };
        let mut recovery = RecoveryCoordinates {
            scope: RecoveryScope::Branch,
            phase: RollbackFailurePhase::ExportUnverified,
            pre_op: None,
            pre_view: None,
            pre_git: None,
            failed_view: None,
            bookmarks: vec![BookmarkRecovery {
                bookmark: FEATURE.into(),
                pre: BookmarkState::normal(&fx.feature),
                pre_git: Some(fx.feature.clone()),
                failed: BookmarkState::normal(&fx.main),
            }],
            survivors: vec![],
            evidence: vec![],
        };
        git(
            fx.proj.path(),
            &[
                "update-ref",
                "-d",
                &format!("refs/heads/{FEATURE}"),
                &fx.feature,
            ],
        );
        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "missing-head", &BTreeSet::new());
        assert!(matches!(verdict, RepairVerdict::Refused(_)), "{verdict:?}");
        let merged_missing = BTreeSet::from([FEATURE.to_string()]);
        recovery.bookmarks[0].pre_git = Some(fx.main.clone());
        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "inconsistent-head", &merged_missing);
        assert!(matches!(verdict, RepairVerdict::Refused(_)), "{verdict:?}");
        recovery.bookmarks[0].pre_git = Some(fx.feature.clone());
        let verdict = repair_branch_recovery(&fx.store, &recovery, "merged-head", &merged_missing);
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
        // Even a recorded failed target is not proof that a deleted merged
        // branch's saved local bookmark is still held.
        fx.move_and_export(&fx.jj, FEATURE, &fx.main);
        git(
            fx.proj.path(),
            &[
                "update-ref",
                "-d",
                &format!("refs/heads/{FEATURE}"),
                &fx.main,
            ],
        );
        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "moved-bookmark", &merged_missing);
        assert!(matches!(verdict, RepairVerdict::Refused(_)), "{verdict:?}");
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.main.clone())
        );
        assert_eq!(fx.git_head(FEATURE), None);
    }

    /// A bookmark the attempt created is put back to absent, ref and all.
    #[test]
    #[serial_test::serial(jj)]
    fn a_recorded_absence_is_restored_by_deleting_the_bookmark_and_its_ref() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, OTHER, &fx.feature);
        assert_eq!(fx.git_head(OTHER), Some(fx.feature.clone()));
        jj.expire_cleanup_for_test();
        restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        let recovery = only_recovery(&jj);
        assert_eq!(recovery.bookmarks[0].pre, BookmarkState::default());

        let verdict =
            repair_branch_recovery(&fx.store, &recovery, "test-recovery", &BTreeSet::new());
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        assert_eq!(bookmark_commit(&fx.jj, &fx.store, OTHER), None);
        assert_eq!(fx.git_head(OTHER), None);
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
    }

    /// A killed writer not proven gone: no restore runs against it, nothing
    /// else spawns, and the store is fenced until validation proves it gone.
    #[test]
    #[serial_test::serial(jj)]
    fn a_surviving_writer_fences_the_store_and_blocks_the_restore() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        // A real, since-exited process stands in for the survivor, so the
        // later validation can prove it gone by its identity.
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let token = cairn_common::process_identity::process_start_token(pid);
        jj.inject_survivor_for_test(SurvivingProcess {
            pid,
            process_group: pid,
            start_token: token,
            outstanding: "leader not reaped (test)".into(),
        });

        let error = restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        assert!(error.contains(ROLLBACK_INCOMPLETE), "{error}");
        let recovery = only_recovery(&jj);
        assert_eq!(recovery.scope, RecoveryScope::Store);
        assert_eq!(
            recovery.phase,
            RollbackFailurePhase::UnknownChildTermination
        );
        assert_eq!(recovery.survivors[0].pid, pid);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.main.clone()),
            "no restore ran against a surviving writer"
        );
        let refused = jj
            .cleanup_env()
            .run(&fx.store, &["op", "log", "-n", "1"], "probe")
            .unwrap_err();
        assert!(refused.contains("not proven gone"), "{refused}");

        child.wait().unwrap();
        let StoreValidation::Downgrade(branch) = validate_store_recovery(&fx.store, &recovery)
        else {
            panic!("a coherent view with the writer gone downgrades");
        };
        assert_eq!(branch.scope, RecoveryScope::Branch);
        assert_eq!(branch.affected().collect::<Vec<_>>(), vec![FEATURE]);
        let verdict = repair_branch_recovery(&fx.store, &branch, "test-recovery", &BTreeSet::new());
        assert!(
            matches!(verdict, RepairVerdict::Verified { .. }),
            "{verdict:?}"
        );
        assert_eq!(fx.git_head(FEATURE), Some(fx.feature.clone()));
    }

    /// The c43a1f1e shape with a twist: the restore completes and the export
    /// after it is killed and not proven gone. The touched set is ordinary, but
    /// a writer may still be exporting anywhere, so the scope is the store.
    #[test]
    #[serial_test::serial(jj)]
    fn a_writer_left_by_the_export_after_a_restore_fences_the_store() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        let snapshot = jj.ledger().snapshot(&op);
        jj.inject_survivor_for_test(SurvivingProcess {
            pid: u32::MAX - 2,
            process_group: u32::MAX - 2,
            start_token: None,
            outstanding: "export leader not reaped (test)".into(),
        });
        let attempt = Attempt {
            jj: &jj,
            op_id: &op,
            snapshot,
            failed: crate::read_local_bookmark_view(&fx.store),
            evidence: Vec::new(),
        };
        let message = attempt.record(
            RollbackFailurePhase::ExportUnverified,
            Some(vec![FEATURE.to_string()]),
            "jj git export (after op restore) was killed",
        );
        assert!(message.contains(ROLLBACK_INCOMPLETE), "{message}");
        let recovery = only_recovery(&jj);
        assert_eq!(recovery.scope, RecoveryScope::Store);
        assert_eq!(
            recovery.phase,
            RollbackFailurePhase::UnknownChildTermination
        );
        assert_eq!(recovery.survivors.len(), 1);
        // Even forced through as a branch record, the repair refuses while
        // the writer is not proven gone.
        let mut forced = recovery.clone();
        forced.scope = RecoveryScope::Branch;
        forced.bookmarks = vec![BookmarkRecovery {
            bookmark: FEATURE.into(),
            pre: BookmarkState::normal(&fx.feature),
            pre_git: Some(fx.feature.clone()),
            failed: BookmarkState::normal(&fx.main),
        }];
        // This test process stands in for a writer that is verifiably alive.
        forced.survivors[0].pid = std::process::id();
        forced.survivors[0].start_token =
            cairn_common::process_identity::process_start_token(std::process::id());
        let verdict = repair_branch_recovery(&fx.store, &forced, "test-recovery", &BTreeSet::new());
        assert!(matches!(verdict, RepairVerdict::Refused(_)), "{verdict:?}");
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, FEATURE),
            Some(fx.main.clone()),
            "nothing was repaired while the writer may run"
        );
    }

    /// A view that sits neither before nor after the attempt stays fenced.
    #[test]
    #[serial_test::serial(jj)]
    fn a_store_validation_refuses_an_unexplained_movement() {
        let Some(fx) = fixture() else { return };
        let jj = fx.budgeted();
        let op = operation_id(&jj, &fx.store).unwrap();
        fx.move_and_export(&jj, FEATURE, &fx.main);
        jj.inject_survivor_for_test(SurvivingProcess {
            pid: u32::MAX - 1,
            process_group: u32::MAX - 1,
            start_token: None,
            outstanding: "test".into(),
        });
        restore_operation(&jj.cleanup_env(), &fx.store, &op).unwrap_err();
        let recovery = only_recovery(&jj);
        fx.move_and_export(&fx.jj, "main", &fx.feature);

        let verdict = validate_store_recovery(&fx.store, &recovery);
        assert!(
            matches!(verdict, StoreValidation::Uncertain(_)),
            "{verdict:?}"
        );
    }
}
