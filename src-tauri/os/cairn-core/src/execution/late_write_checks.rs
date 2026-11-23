//! Which write wave's late summary is worth a message of its own.
//!
//! A committing call whose when:write checks outlive its inline wait answers
//! pending, and the wave's summary reaches the agent later as a `[Write checks]`
//! message. An agent committing in a burst leaves one such wave per commit, and
//! they tend to settle together: a shared queue drains, or one runner stop
//! cancels them all in the same second. Only the newest wave describes the
//! branch the agent is working on, so a wave that settles after a newer wave of
//! the same job has planned checks is superseded: its summary is not sent, and
//! the newer wave's late summary names it in one line instead.
//!
//! Supersession is decided by planning order, not by how the older wave ended.
//! Write lanes carry no purpose key, so a newer commit never cancels an older
//! wave's lanes; a superseded wave may have finished, failed or been cancelled
//! for its own reasons. A wave that planned nothing (a commit no when:write
//! check matches) supersedes nothing, so a failure on the last source commit is
//! never hidden behind a docs-only commit that ran no checks.
//!
//! A superseded wave's passes and cancellations fold silently into a count, but
//! a genuine failure is still news unless a newer wave of the job recorded its
//! own verdict for that lane: a newer wave cancelled before that lane ran says
//! nothing about it, and the newer tree most likely still carries the failure.
//! So each lane remembers the newest wave that recorded a verdict on it, and a
//! superseded failure that is still that newest verdict is named in the fold
//! line by commit and lane. When the newest wave has already finished, there is
//! no summary left to fold into, and a superseded wave holding such a failure
//! delivers its own summary instead of disappearing.
//!
//! Runtime-only, one registry per orchestrator. A job's state is dropped when
//! its last open wave ends, so what a wave answered inline never folds into a
//! later summary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

/// How many superseded commits a fold line names before it only counts them.
const FOLDED_COMMITS_NAMED: usize = 8;

/// How long a commit id reads in a late summary.
pub(crate) const SHORT_COMMIT_LEN: usize = 12;

#[derive(Debug, Default)]
struct JobWaves {
    /// Waves opened for the job that have not ended.
    open: usize,
    /// The newest of the job's waves that planned checks.
    newest_planned: Option<u64>,
    /// Whether that newest planned wave has settled late or ended, leaving no
    /// summary of its own for a superseded failure to fold into.
    newest_finished: bool,
    /// Per lane, the newest wave that recorded a verdict on it.
    lane_verdicts: HashMap<String, u64>,
    /// The waves superseded since a late summary last named them.
    superseded: SupersededWaves,
}

impl JobWaves {
    /// Whether `failure` is still the newest verdict on its lane.
    fn is_current(&self, failure: &FoldedFailure) -> bool {
        self.lane_verdicts.get(&failure.lane) == Some(&failure.seq)
    }
}

#[derive(Debug, Default)]
struct State {
    next_wave: u64,
    jobs: HashMap<String, JobWaves>,
}

/// The registry: one per orchestrator, runtime-only.
#[derive(Debug, Default)]
pub(crate) struct LateWriteChecks {
    state: Mutex<State>,
}

impl LateWriteChecks {
    /// Open a wave for `job_id`. `wave_id` is the committing call that started
    /// it, which is unique across restarts where the registry's own order is
    /// not, so it is what a delivery is keyed by.
    pub(crate) fn open(self: &Arc<Self>, job_id: &str, wave_id: &str) -> WaveTicket {
        self.open_ordered(job_id, wave_id, None)
    }

    /// Recreate ordering evidence without assigning a restarted wave a newer
    /// place than a wave that already delivered.
    pub(crate) fn open_recovered(
        self: &Arc<Self>,
        job_id: &str,
        wave_id: &str,
        order: i64,
    ) -> WaveTicket {
        self.open_ordered(job_id, wave_id, Some(order as u64))
    }

    fn open_ordered(
        self: &Arc<Self>,
        job_id: &str,
        wave_id: &str,
        restored: Option<u64>,
    ) -> WaveTicket {
        let mut state = self.lock();
        let seq = restored.unwrap_or_else(|| {
            state
                .next_wave
                .max(chrono::Utc::now().timestamp_millis() as u64)
        });
        state.next_wave = state.next_wave.max(seq + 1);
        state.jobs.entry(job_id.to_string()).or_default().open += 1;
        drop(state);
        WaveTicket {
            registry: self.clone(),
            job_id: job_id.to_string(),
            wave_id: wave_id.to_string(),
            seq,
            opened_at_ms: seq as i64,
            commit: OnceLock::new(),
            failed: Mutex::new(Vec::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The earlier waves one late summary stands in for.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SupersededWaves {
    /// Their commits, short, in the order they settled, at most
    /// [`FOLDED_COMMITS_NAMED`].
    pub(crate) commits: Vec<String>,
    /// How many there were in all.
    pub(crate) total: usize,
    /// Their genuine failures no newer wave has recorded a verdict over. At
    /// most one per lane once delivered.
    pub(crate) failures: Vec<FoldedFailure>,
}

impl SupersededWaves {
    fn push(&mut self, commit: Option<&str>, failures: Vec<FoldedFailure>) {
        self.total += 1;
        if let (Some(commit), true) = (commit, self.commits.len() < FOLDED_COMMITS_NAMED) {
            self.commits.push(short_commit(commit).to_string());
        }
        self.failures.extend(failures);
    }
}

/// One lane a superseded wave failed, at the commit it failed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FoldedFailure {
    /// The failing wave's commit, short, or `None` when it never planned.
    pub(crate) commit: Option<String>,
    pub(crate) lane: String,
    seq: u64,
}

/// A lane's recorded verdict, as supersession weighs it. A lane that recorded
/// none (cancelled, an infrastructure failure, suppressed) is not reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneVerdict {
    Passed,
    Failed,
}

/// What becomes of a wave's late summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LateDisposition {
    /// A newer wave of the job planned checks: not sent.
    Superseded,
    /// Sent, naming the superseded waves it stands in for, if any.
    Deliver(Option<SupersededWaves>),
}

/// One write wave's place in its job's order. The wave and its late delivery
/// share it; the wave has ended once both have let go of it.
#[derive(Debug)]
pub(crate) struct WaveTicket {
    registry: Arc<LateWriteChecks>,
    job_id: String,
    wave_id: String,
    seq: u64,
    opened_at_ms: i64,
    commit: OnceLock<String>,
    /// The lanes this wave genuinely failed.
    failed: Mutex<Vec<String>>,
}

impl WaveTicket {
    /// The verdicts the wave recorded, by lane. Each one makes this wave the
    /// newest word on its lane unless a newer wave already recorded there.
    pub(crate) fn recorded<'a>(&self, verdicts: impl IntoIterator<Item = (&'a str, LaneVerdict)>) {
        let mut failed = Vec::new();
        let mut state = self.registry.lock();
        let job = state.jobs.entry(self.job_id.clone()).or_default();
        for (lane, verdict) in verdicts {
            let newest = job
                .lane_verdicts
                .entry(lane.to_string())
                .or_insert(self.seq);
            *newest = (*newest).max(self.seq);
            if verdict == LaneVerdict::Failed {
                failed.push(lane.to_string());
            }
        }
        drop(state);
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(failed);
    }

    /// This wave's failures that are still the newest verdict on their lane.
    fn current_failures(&self, job: &JobWaves) -> Vec<FoldedFailure> {
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|lane| FoldedFailure {
                commit: self.commit().map(|commit| short_commit(commit).to_string()),
                lane: lane.clone(),
                seq: self.seq,
            })
            .filter(|failure| job.is_current(failure))
            .collect()
    }

    /// The wave planned checks for `commit`. From now on it supersedes every
    /// wave of its job opened before it.
    pub(crate) fn planned(&self, commit: &str) {
        let _ = self.commit.set(commit.to_string());
        let mut state = self.registry.lock();
        let job = state.jobs.entry(self.job_id.clone()).or_default();
        if job.newest_planned.is_none_or(|newest| newest < self.seq) {
            job.newest_planned = Some(self.seq);
            job.newest_finished = false;
        }
    }

    /// Decide this wave's late summary, recording it as superseded when a
    /// newer wave of its job has planned checks.
    pub(crate) fn settle_late(&self) -> LateDisposition {
        let mut state = self.registry.lock();
        let Some(job) = state.jobs.get_mut(&self.job_id) else {
            return LateDisposition::Deliver(None);
        };
        let failures = self.current_failures(job);
        if job.newest_planned.is_some_and(|newest| newest > self.seq) {
            if !job.newest_finished {
                job.superseded.push(self.commit(), failures);
                return LateDisposition::Superseded;
            }
            // The newest wave has spoken already. A failure no newer wave
            // re-checked is still the latest word on its lane, so it is sent.
            if failures.is_empty() {
                return LateDisposition::Superseded;
            }
        }
        if job.newest_planned == Some(self.seq) {
            job.newest_finished = true;
        }
        let mut superseded = std::mem::take(&mut job.superseded);
        superseded
            .failures
            .retain(|failure| job.is_current(failure));
        LateDisposition::Deliver((superseded.total > 0).then_some(superseded))
    }

    /// The commit the wave evaluated, once it planned.
    pub(crate) fn commit(&self) -> Option<&str> {
        self.commit.get().map(String::as_str)
    }

    /// A delivered wave remains ordering/verdict evidence, never a delivery task.
    pub(crate) fn restore_finished(&self) {
        let mut state = self.registry.lock();
        if let Some(job) = state.jobs.get_mut(&self.job_id) {
            if job.newest_planned == Some(self.seq) {
                job.newest_finished = true;
            }
        }
    }

    pub(crate) fn opened_at_ms(&self) -> i64 {
        self.opened_at_ms
    }

    pub(crate) fn wave_id(&self) -> &str {
        &self.wave_id
    }

    pub(crate) fn job_id(&self) -> &str {
        &self.job_id
    }
}

impl Drop for WaveTicket {
    fn drop(&mut self) {
        let mut state = self.registry.lock();
        if let Some(job) = state.jobs.get_mut(&self.job_id) {
            if job.newest_planned == Some(self.seq) {
                job.newest_finished = true;
            }
            job.open = job.open.saturating_sub(1);
            if job.open == 0 {
                state.jobs.remove(&self.job_id);
            }
        }
    }
}

/// `commit` cut to the length a late summary names it by.
pub(crate) fn short_commit(commit: &str) -> &str {
    commit.get(..SHORT_COMMIT_LEN).unwrap_or(commit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(n: usize) -> String {
        format!("{n:0>40}")
    }

    /// Eight commits in a burst, each wave planning before the next opens, all
    /// settling late together: the newest is the one summary sent, and it
    /// names the seven it stands in for.
    #[test]
    fn a_burst_of_eight_sends_only_the_newest_wave_naming_the_rest() {
        let registry = Arc::new(LateWriteChecks::default());
        let tickets: Vec<_> = (0..8)
            .map(|n| {
                let ticket = registry.open("job-a", &format!("toolu_{n}"));
                ticket.planned(&commit(n));
                ticket
            })
            .collect();
        let dispositions: Vec<_> = tickets.iter().map(WaveTicket::settle_late).collect();
        assert!(dispositions[..7]
            .iter()
            .all(|d| *d == LateDisposition::Superseded));
        assert_eq!(
            dispositions[7],
            LateDisposition::Deliver(Some(SupersededWaves {
                commits: (0..7).map(|n| commit(n)[..12].to_string()).collect(),
                total: 7,
                failures: Vec::new(),
            }))
        );
    }

    /// The newest wave may finish first. The older ones that settle after it
    /// are still superseded, and nothing is left to fold them into.
    #[test]
    fn an_older_wave_settling_after_the_newest_is_still_superseded() {
        let registry = Arc::new(LateWriteChecks::default());
        let older = registry.open("job-a", "toolu_old");
        older.planned(&commit(1));
        let newer = registry.open("job-a", "toolu_new");
        newer.planned(&commit(2));
        assert_eq!(newer.settle_late(), LateDisposition::Deliver(None));
        assert_eq!(older.settle_late(), LateDisposition::Superseded);
    }

    /// A newer commit no when:write check matched ran nothing, so the older
    /// wave's verdict is still the latest word on the branch.
    #[test]
    fn a_newer_wave_that_planned_nothing_supersedes_nothing() {
        let registry = Arc::new(LateWriteChecks::default());
        let older = registry.open("job-a", "toolu_old");
        older.planned(&commit(1));
        let _docs_only = registry.open("job-a", "toolu_docs");
        assert_eq!(older.settle_late(), LateDisposition::Deliver(None));
    }

    #[test]
    fn another_jobs_waves_supersede_nothing() {
        let registry = Arc::new(LateWriteChecks::default());
        let mine = registry.open("job-a", "toolu_a");
        mine.planned(&commit(1));
        let theirs = registry.open("job-b", "toolu_b");
        theirs.planned(&commit(2));
        assert_eq!(mine.settle_late(), LateDisposition::Deliver(None));
    }

    /// A newer wave that answered inline never settles late, and the waves it
    /// superseded must not resurface in some later wave's summary once every
    /// wave of the job has ended.
    #[test]
    fn a_jobs_state_ends_with_its_last_open_wave() {
        let registry = Arc::new(LateWriteChecks::default());
        let older = registry.open("job-a", "toolu_old");
        older.planned(&commit(1));
        let inline = registry.open("job-a", "toolu_inline");
        inline.planned(&commit(2));
        assert_eq!(older.settle_late(), LateDisposition::Superseded);
        drop(older);
        drop(inline);
        assert!(registry.lock().jobs.is_empty());

        let later = registry.open("job-a", "toolu_later");
        later.planned(&commit(3));
        assert_eq!(later.settle_late(), LateDisposition::Deliver(None));
    }

    #[test]
    fn a_fold_names_a_bounded_number_of_commits_and_counts_the_rest() {
        let registry = Arc::new(LateWriteChecks::default());
        let tickets: Vec<_> = (0..20)
            .map(|n| {
                let ticket = registry.open("job-a", &format!("toolu_{n}"));
                ticket.planned(&commit(n));
                ticket
            })
            .collect();
        for ticket in &tickets[..19] {
            assert_eq!(ticket.settle_late(), LateDisposition::Superseded);
        }
        let LateDisposition::Deliver(Some(folded)) = tickets[19].settle_late() else {
            panic!("the newest wave is delivered with its fold");
        };
        assert_eq!(folded.total, 19);
        assert_eq!(folded.commits.len(), FOLDED_COMMITS_NAMED);
    }

    fn failed_lanes(disposition: &LateDisposition) -> Vec<(Option<String>, String)> {
        let LateDisposition::Deliver(Some(folded)) = disposition else {
            panic!("expected a delivery with a fold, got {disposition:?}");
        };
        folded
            .failures
            .iter()
            .map(|failure| (failure.commit.clone(), failure.lane.clone()))
            .collect()
    }

    /// A fails rust-fmt; B is cancelled before any verdict. A's failure is the
    /// newest word on rust-fmt, so B's fold names it.
    #[test]
    fn a_superseded_failure_no_newer_verdict_covers_is_folded_by_name() {
        let registry = Arc::new(LateWriteChecks::default());
        let a = registry.open("job-a", "toolu_a");
        a.planned(&commit(1));
        let b = registry.open("job-a", "toolu_b");
        b.planned(&commit(2));
        a.recorded([
            ("rust-fmt", LaneVerdict::Failed),
            ("rust-lint", LaneVerdict::Passed),
        ]);
        assert_eq!(a.settle_late(), LateDisposition::Superseded);
        let delivered = b.settle_late();
        assert_eq!(
            failed_lanes(&delivered),
            vec![(Some(commit(1)[..12].to_string()), "rust-fmt".to_string())]
        );
    }

    /// A newer wave's own verdict on the lane is the truth, whichever wave
    /// settles or records first.
    #[test]
    fn a_newer_verdict_on_the_lane_retires_a_superseded_failure() {
        for b_records_first in [false, true] {
            let registry = Arc::new(LateWriteChecks::default());
            let a = registry.open("job-a", "toolu_a");
            a.planned(&commit(1));
            let b = registry.open("job-a", "toolu_b");
            b.planned(&commit(2));
            if b_records_first {
                b.recorded([("rust-fmt", LaneVerdict::Passed)]);
                a.recorded([("rust-fmt", LaneVerdict::Failed)]);
                assert_eq!(a.settle_late(), LateDisposition::Superseded);
            } else {
                a.recorded([("rust-fmt", LaneVerdict::Failed)]);
                assert_eq!(a.settle_late(), LateDisposition::Superseded);
                b.recorded([("rust-fmt", LaneVerdict::Passed)]);
            }
            assert!(failed_lanes(&b.settle_late()).is_empty());
        }
    }

    /// Of two superseded waves failing the same lane, only the newer failure
    /// is named.
    #[test]
    fn a_lane_failed_by_two_superseded_waves_is_named_once() {
        let registry = Arc::new(LateWriteChecks::default());
        let tickets: Vec<_> = (0..3)
            .map(|n| {
                let ticket = registry.open("job-a", &format!("toolu_{n}"));
                ticket.planned(&commit(n));
                ticket
            })
            .collect();
        tickets[0].recorded([("rust-fmt", LaneVerdict::Failed)]);
        tickets[1].recorded([("rust-fmt", LaneVerdict::Failed)]);
        tickets[0].settle_late();
        tickets[1].settle_late();
        assert_eq!(
            failed_lanes(&tickets[2].settle_late()),
            vec![(Some(commit(1)[..12].to_string()), "rust-fmt".to_string())]
        );
    }

    /// The newest wave finished before an older one settled: there is no fold
    /// left, so the older wave's still-current failure is sent on its own,
    /// while an older wave with nothing current stays silent.
    #[test]
    fn a_current_failure_settling_after_the_newest_finished_is_sent_on_its_own() {
        let registry = Arc::new(LateWriteChecks::default());
        let failing = registry.open("job-a", "toolu_failing");
        failing.planned(&commit(1));
        let passing = registry.open("job-a", "toolu_passing");
        passing.planned(&commit(2));
        let newest = registry.open("job-a", "toolu_newest");
        newest.planned(&commit(3));
        failing.recorded([("rust-fmt", LaneVerdict::Failed)]);
        passing.recorded([("rust-lint", LaneVerdict::Passed)]);
        assert_eq!(newest.settle_late(), LateDisposition::Deliver(None));
        assert_eq!(passing.settle_late(), LateDisposition::Superseded);
        assert_eq!(failing.settle_late(), LateDisposition::Deliver(None));
    }
    #[test]
    fn a_recovered_delivered_pass_covers_an_older_failure_arriving_after_restart() {
        let registry = Arc::new(LateWriteChecks::default());
        let old = registry.open_recovered("job", "old", 10);
        old.planned("old-commit");
        let newer = registry.open_recovered("job", "new", 20);
        newer.planned("new-commit");
        newer.recorded([("suite", LaneVerdict::Passed)]);
        newer.restore_finished();
        old.recorded([("suite", LaneVerdict::Failed)]);
        assert_eq!(old.settle_late(), LateDisposition::Superseded);
        // Repeated evidence restoration must not move B past a live newer C.
        let latest = registry.open_recovered("job", "latest", 30);
        latest.planned("latest-commit");
        let again = registry.open_recovered("job", "new", 20);
        again.planned("new-commit");
        again.recorded([("suite", LaneVerdict::Passed)]);
        again.restore_finished();
        latest.recorded([("suite", LaneVerdict::Failed)]);
        assert!(matches!(latest.settle_late(), LateDisposition::Deliver(_)));
    }
}
