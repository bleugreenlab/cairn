use super::*;

pub(super) fn check_phase_status(
    phase: Option<&str>,
    label: Option<&str>,
    diagnostic: Option<String>,
) -> CheckPhaseStatus {
    let label = match phase {
        Some("queued") => Some("queued".to_string()),
        _ => label.map(str::to_string),
    };
    debug_assert!(label.as_deref().is_none_or(|value| {
        !value.contains('·') && !value.to_ascii_lowercase().contains("behind")
    }));
    CheckPhaseStatus {
        phase: phase.map(str::to_string),
        label,
        diagnostic,
        ..CheckPhaseStatus::default()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CheckPhaseStatus {
    pub(crate) phase: Option<String>,
    pub(crate) label: Option<String>,
    pub(crate) diagnostic: Option<String>,
    /// The machine running the batch, once one has claimed it.
    pub(crate) executor: Option<String>,
    /// When the current phase began: when the batch started waiting, was
    /// claimed, or started being prepared.
    pub(crate) since_unix_ms: Option<u64>,
    /// What holds a queued batch (`declined by local (host pressure: cpu
    /// 97%)`, or every machine of its latest placement pass with its status),
    /// without the wait forecast the full diagnostic leads with. `None` when
    /// no pass was recorded and no machine has declined it.
    pub(super) held_by: Option<String>,
}

pub(super) type CheckStatusNotify =
    Arc<dyn Fn(Vec<CheckStatusEntry>, CheckPhaseStatus) + Send + Sync>;

/// What a one-item batch's observer hears while the batch runs: the placement
/// request it was presented as, each change of where it stands (queued, the
/// machine that claimed it), and each scrubbed output chunk as it arrives. A
/// publication leg records these into its ledger row and log while the build
/// is still running.
#[derive(Debug, Clone)]
pub(crate) enum BatchProgress {
    Request(String),
    Phase(CheckPhaseStatus),
    Output(String),
}

pub(crate) type BatchObserver = Arc<dyn Fn(BatchProgress) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct CheckStatusBoard {
    pub(super) entries: Arc<std::sync::Mutex<Vec<CheckStatusEntry>>>,
    /// The phase of a board whose lanes share one request: a lease, or a
    /// one-item board.
    pub(super) phase: Arc<std::sync::Mutex<CheckPhaseStatus>>,
    /// Each lane submitted as its own request, by plan index, with where that
    /// request stands. Empty while every lane shares [`Self::phase`].
    pub(super) lanes: Arc<std::sync::Mutex<BTreeMap<usize, CheckPhaseStatus>>>,
    pub(super) durability_errors:
        Arc<std::sync::Mutex<Vec<crate::execution::checks_turn_end::ReviewDurabilityError>>>,
    /// The lane this handle reports for, when it is one lane's request.
    pub(super) lane: Option<usize>,
    pub(super) notify: CheckStatusNotify,
    /// Whether the check engine, not the executor's per-item report, owns
    /// this board's verdicts. See [`Self::for_review_wave`].
    pub(super) engine_owned: bool,
    /// Who hears this board's request, phases and output, when anyone does.
    /// See [`Self::observed`].
    pub(super) observer: Option<BatchObserver>,
}

impl CheckStatusBoard {
    pub(super) fn new(plans: &[(CheckPlan, String)], notify: CheckStatusNotify) -> Self {
        Self {
            entries: Arc::new(std::sync::Mutex::new(
                plans
                    .iter()
                    .enumerate()
                    .map(|(index, (plan, _))| CheckStatusEntry {
                        index,
                        name: plan.name.clone(),
                        state: "pending".into(),
                        annotation: None,
                        cached: false,
                        since_unix_ms: None,
                    })
                    .collect(),
            )),
            phase: Arc::new(std::sync::Mutex::new(CheckPhaseStatus::default())),
            lanes: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            durability_errors: Arc::new(std::sync::Mutex::new(Vec::new())),
            lane: None,
            notify,
            engine_owned: false,
            observer: None,
        }
    }

    /// The board of a one-item batch whose caller follows it as it runs: the
    /// request it is presented as, every phase (queued, claimed by a machine),
    /// and its output chunk by chunk, all delivered to `observer`.
    pub(crate) fn observed(name: &str, observer: BatchObserver) -> Self {
        let phases = observer.clone();
        Self {
            entries: Arc::new(std::sync::Mutex::new(vec![CheckStatusEntry {
                index: 0,
                name: name.to_string(),
                state: "pending".into(),
                annotation: None,
                cached: false,
                since_unix_ms: None,
            }])),
            phase: Arc::new(std::sync::Mutex::new(CheckPhaseStatus::default())),
            lanes: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            durability_errors: Arc::new(std::sync::Mutex::new(Vec::new())),
            lane: None,
            notify: Arc::new(move |_, phase| phases(BatchProgress::Phase(phase))),
            engine_owned: false,
            observer: Some(observer),
        }
    }

    /// Whether a caller follows this board's output, so the fleet must hand
    /// it the batch's process reports.
    pub(crate) fn is_observed(&self) -> bool {
        self.observer.is_some()
    }

    pub(crate) fn note_request(&self, request_id: &str) {
        if let Some(observer) = &self.observer {
            observer(BatchProgress::Request(request_id.to_string()));
        }
    }

    pub(crate) fn note_output(&self, chunk: &str) {
        if let Some(observer) = &self.observer {
            observer(BatchProgress::Output(chunk.to_string()));
        }
    }

    /// The board a turn-end review wave publishes while it runs, indexed by
    /// plan position like the wave's stream ids.
    ///
    /// Its verdicts belong to the check engine, which records each lane as
    /// its own request settles: an item the executor stopped (a silent tree, a
    /// cancel) is a non-verdict whatever its exit code says. So an executor's
    /// per-item completion marks a lane `finished`, an interim state annotated
    /// with its exit and duration, never `passed` or `failed`. `on_change` runs
    /// after every transition.
    pub(crate) fn for_review_wave(
        plans: &[(CheckPlan, String)],
        on_change: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let mut board = Self::new(plans, Arc::new(move |_, _| on_change()));
        board.engine_owned = true;
        board
    }

    pub(crate) fn engine_owns_verdicts(&self) -> bool {
        self.engine_owned
    }

    /// Every lane's current entry, in plan order.
    pub(crate) fn snapshot(&self) -> Vec<CheckStatusEntry> {
        self.entries.lock().unwrap().clone()
    }

    pub(crate) fn record_durability_error(
        &self,
        failure: crate::execution::checks_turn_end::ReviewDurabilityError,
    ) {
        let mut failures = self
            .durability_errors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !failures.contains(&failure) {
            failures.push(failure);
        }
        drop(failures);
        self.emit();
    }

    #[cfg(test)]
    pub(crate) fn durability_errors(
        &self,
    ) -> Vec<crate::execution::checks_turn_end::ReviewDurabilityError> {
        self.durability_errors
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// The handle one lane's own request reports through: its dispatch phases
    /// are that lane's alone, while entries stay shared with the whole board.
    pub(crate) fn for_lane(&self, index: usize) -> Self {
        self.lanes.lock().unwrap().entry(index).or_default();
        Self {
            lane: Some(index),
            ..self.clone()
        }
    }

    /// The entries that share the lane's request: the lane alone when it is
    /// its own request, else every lane that is not.
    pub(crate) fn lane_snapshot(&self, name: &str) -> Vec<CheckStatusEntry> {
        let entries = self.snapshot();
        let Some(entry) = entries.iter().find(|entry| entry.name == name) else {
            return Vec::new();
        };
        let lanes = self.lanes.lock().unwrap();
        if lanes.contains_key(&entry.index) {
            vec![entry.clone()]
        } else {
            entries
                .into_iter()
                .filter(|entry| !lanes.contains_key(&entry.index))
                .collect()
        }
    }

    /// Where the lane's request stands.
    pub(crate) fn lane_phase(&self, name: &str) -> CheckPhaseStatus {
        let index = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.index);
        let lanes = self.lanes.lock().unwrap();
        index
            .and_then(|index| lanes.get(&index).cloned())
            .unwrap_or_else(|| self.phase.lock().unwrap().clone())
    }

    /// The machine this handle's request runs on. A whole board of separate
    /// requests names one only when every request has the same claim.
    pub(crate) fn executor(&self) -> Option<String> {
        let lanes = self.lanes.lock().unwrap();
        if let Some(lane) = self.lane {
            return lanes.get(&lane).and_then(|phase| phase.executor.clone());
        }
        let Some(first) = lanes.values().next() else {
            return self.phase.lock().unwrap().executor.clone();
        };
        let executor = first.executor.clone()?;
        lanes
            .values()
            .all(|phase| phase.executor.as_ref() == Some(&executor))
            .then_some(executor)
    }

    /// The lane `update` applies to: this handle's own, else the lane `index`
    /// names when it is its own request.
    fn phase_lane(&self, index: Option<usize>) -> Option<usize> {
        let lanes = self.lanes.lock().unwrap();
        self.lane
            .or_else(|| index.filter(|index| lanes.contains_key(index)))
    }

    pub(super) fn update_phase(
        &self,
        index: Option<usize>,
        update: impl FnOnce(&mut CheckPhaseStatus),
    ) {
        match self.phase_lane(index) {
            Some(lane) => update(self.lanes.lock().unwrap().entry(lane).or_default()),
            None => update(&mut self.phase.lock().unwrap()),
        }
    }

    /// Whether every lane sharing `index`'s request holds a recorded state.
    pub(super) fn request_is_recorded(&self, index: usize) -> bool {
        let lane = self.phase_lane(Some(index));
        let lanes = self.lanes.lock().unwrap();
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| match lane {
                Some(lane) => lane == entry.index,
                None => !lanes.contains_key(&entry.index),
            })
            .all(|entry| matches!(entry.state.as_str(), "passed" | "failed" | "unverified"))
    }

    pub(super) fn emit(&self) {
        let entries = self.entries.lock().unwrap().clone();
        let phase = match self.lane {
            Some(lane) => self
                .lanes
                .lock()
                .unwrap()
                .get(&lane)
                .cloned()
                .unwrap_or_default(),
            None => self.phase.lock().unwrap().clone(),
        };
        (self.notify)(entries, phase);
    }

    pub(super) fn emit_initial(&self) {
        self.emit();
    }

    pub(crate) fn transition(&self, index: usize, state: &str, annotation: Option<String>) {
        self.transition_with_cache(index, state, annotation, false);
    }

    pub(super) fn transition_cached(&self, index: usize, state: &str, annotation: Option<String>) {
        self.transition_with_cache(index, state, annotation, true);
    }

    pub(super) fn transition_with_cache(
        &self,
        index: usize,
        state: &str,
        annotation: Option<String>,
        cached: bool,
    ) {
        {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get_mut(index) {
                // Stamped on a change of state only: every output chunk re-marks
                // a lane `running`, and that must not restart its clock.
                if entry.state != state || entry.since_unix_ms.is_none() {
                    entry.since_unix_ms = Some(chrono::Utc::now().timestamp_millis().max(0) as u64);
                }
                entry.state = state.to_string();
                entry.annotation = annotation;
                entry.cached = cached;
            }
        }
        if state == "running" {
            // A phase already running carries the machine and start time its
            // claim named; a lane's own start (and every output chunk after
            // it) must not wipe them back to a bare `running checks`.
            self.update_phase(Some(index), |phase| {
                if phase.phase.as_deref() != Some("running") {
                    phase.phase = Some("running".into());
                    phase.label = Some("running checks".into());
                    phase.diagnostic = None;
                    phase.held_by = None;
                }
            });
        } else if self.request_is_recorded(index) {
            self.update_phase(Some(index), |phase| *phase = CheckPhaseStatus::default());
        }
        self.emit();
    }

    pub(super) fn set_phase(
        &self,
        phase: Option<&str>,
        label: Option<&str>,
        diagnostic: Option<String>,
    ) {
        self.update_phase(None, |status| {
            *status = check_phase_status(phase, label, diagnostic)
        });
        self.emit();
    }

    /// Show a phase [`DispatchNarrator`] rendered, machine and time included.
    pub(super) fn show_phase(&self, status: CheckPhaseStatus) {
        self.update_phase(None, |phase| *phase = status);
        self.emit();
    }
}

/// How far a write-check wave has got toward a verdict, as the committing
/// call's inline wait reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum WaveStage {
    /// Planned, with nothing dispatched yet.
    #[default]
    Planning,
    /// Dispatching: resolving the build slot, preparing the check's inputs.
    Preparing,
    /// In the queue, claimed by no machine.
    Queued,
    /// A machine claimed a lane, or a lane started.
    Running,
}

impl WaveStage {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Preparing => "preparing",
            Self::Queued => "queued",
            Self::Running => "running",
        }
    }
}

/// Where a write-check wave stands, published from its status board to the
/// committing call's inline wait.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct WaveProgress {
    pub(super) stage: WaveStage,
    /// The machine running it, once one has claimed it.
    pub(super) executor: Option<String>,
    /// When the current stage began, when the board knows.
    pub(super) since_unix_ms: Option<u64>,
    /// What holds it, while it is not running.
    pub(super) reason: Option<String>,
    /// Whether any lane has ever been claimed. Latched: a wave that ran and
    /// then steps back (to prepare the batch that re-verifies its fix) stays
    /// admitted, however its snapshots coalesce on the way to the waiter.
    pub(super) admitted: bool,
}

/// The progress a board snapshot shows, given what was published before it.
///
/// A settled or idle board says nothing new and keeps the last stage: the
/// wave's outcome is what reports its end. A lane's own start resets the
/// board's phase to a bare `running`, so the machine the claim named is
/// carried forward while the wave stays running.
pub(super) fn wave_progress(
    previous: &WaveProgress,
    entries: &[CheckStatusEntry],
    phase: &CheckPhaseStatus,
) -> WaveProgress {
    let running_phase = phase.phase.as_deref() == Some("running");
    if running_phase || entries.iter().any(|entry| entry.state == "running") {
        let carried = (previous.stage == WaveStage::Running).then_some(previous);
        let claimed = running_phase.then_some(phase);
        return WaveProgress {
            stage: WaveStage::Running,
            executor: claimed
                .and_then(|phase| phase.executor.clone())
                .or_else(|| carried.and_then(|previous| previous.executor.clone())),
            since_unix_ms: claimed
                .and_then(|phase| phase.since_unix_ms)
                .or_else(|| carried.and_then(|previous| previous.since_unix_ms)),
            reason: None,
            admitted: true,
        };
    }
    match phase.phase.as_deref() {
        Some("queued") => WaveProgress {
            stage: WaveStage::Queued,
            executor: None,
            since_unix_ms: phase.since_unix_ms,
            reason: phase.held_by.clone().or_else(|| phase.diagnostic.clone()),
            admitted: previous.admitted,
        },
        Some("dispatching" | "provisioning") => WaveProgress {
            stage: WaveStage::Preparing,
            executor: None,
            since_unix_ms: phase.since_unix_ms,
            reason: phase.diagnostic.clone(),
            admitted: previous.admitted,
        },
        _ => previous.clone(),
    }
}

/// A write wave's status sink: every board snapshot goes to `emit` (the UI's
/// `check-status` event) and, as [`WaveProgress`], to the committing call's
/// inline wait. Publishing never blocks on that waiter, which may be gone.
pub(super) fn wave_status_notify(
    emit: impl Fn(Vec<CheckStatusEntry>, CheckPhaseStatus) + Send + Sync + 'static,
    progress: tokio::sync::watch::Sender<WaveProgress>,
) -> CheckStatusNotify {
    Arc::new(move |checks, phase_status| {
        progress.send_if_modified(|current| {
            let next = wave_progress(current, &checks, &phase_status);
            let changed = *current != next;
            *current = next;
            changed
        });
        emit(checks, phase_status);
    })
}
