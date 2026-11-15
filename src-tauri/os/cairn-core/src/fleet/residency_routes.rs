use super::*;

pub(super) fn residency_runtime_config_request(residency: &CellResidency) -> CellRequest {
    residency_placement_request(&ResidencyAcquireRequest {
        holder: residency.holder.clone(),
        repository: residency.repository.clone(),
        executor: None,
        owner_ref: residency.owner_ref.clone(),
        selector: residency.selector.clone(),
        initial_base_commit: residency.current_base_commit.clone(),
        footprint: residency.footprint,
        death_policy: residency.death_policy.clone(),
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: unix_time_ms().saturating_add(30_000),
        waiting_since_unix_ms: unix_time_ms(),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(super) struct ResidencyRoute {
    pub(super) holder: ResidencyHolder,
    pub(super) repository: RepositoryLocator,
    pub(super) executor_id: String,
    pub(super) pending: bool,
}

/// A residency result's account of its cell, on the link it arrived on.
#[derive(Clone, Debug)]
pub(super) struct GrantedCell {
    pub(super) generation: u64,
    pub(super) cell: PersistentCellState,
    pub(super) recorded_unix_ms: u64,
}

/// How long a granted cell outlives reports from its executor that lack it.
///
/// Not a wait: nothing is held back for it. It bounds how long the runner
/// takes an executor's answer over that executor's later snapshots, which can
/// trail the answer by as long as the executor's runtime is stalled. Erring
/// long is cheap: a cell that really died inside the window gets one
/// operation routed to its executor, which refuses it with `NotFound` or
/// `StaleEpoch` itself. Erring short refuses operations on a cell that exists
/// because a report was slow.
pub(super) const GRANT_CONFIRMATION_GRACE_MS: u64 = 60_000;

/// How long an acquisition whose reattachment found no environment waits
/// before resolving its route again, so a snapshot still reporting a cell the
/// executor is releasing cannot spin it.
pub(super) const REATTACH_REFUSED_PACE_MS: u64 = 250;

/// What [`Fleet::reattach_residency`] leaves the acquisition to do.
pub(super) enum Reattached {
    /// The executor's answer, which is the acquisition's.
    Answered(Box<ResidencyResult>),
    /// The route named an environment its executor no longer holds and is
    /// gone; place the acquisition again.
    RouteForgotten,
}

/// The executor and cell a non-acquiring residency operation addresses.
#[derive(Debug)]
pub(super) struct RoutedCell {
    pub(super) selected: SelectedExecutor,
    pub(super) residency: CellResidency,
    pub(super) cell_epoch: u64,
    pub(super) occupancy: cairn_common::executor_protocol::CellOccupancy,
}

/// How long an execution environment outlives its executor's link being down
/// before the runner concludes the environment is gone and rebuilds it.
///
/// This is the whole distance between a link that flapped and a machine that
/// left, and both mistakes it sits between are real. Too short and a reconnect
/// window costs an agent its uncommitted files and resident processes; too long
/// and a job is parked on a machine that is never coming back — which is the
/// failure this bound was introduced to end, and which ran for hours.
///
/// Five minutes, matching the grace an executor gives an owner that stopped
/// answering before reclaiming its cell: the two are the same judgement made
/// from opposite sides of the link, and they should not disagree about how long
/// silence takes to mean absence.
pub(crate) const ENVIRONMENT_LEASE_GRACE_MS: u64 = 5 * 60 * 1000;

/// An execution environment the runner rebuilt because the executor session
/// holding the previous one ended.
///
/// Recorded so the loss is stated rather than absorbed. A rebuilt environment is
/// a fresh checkout at the state the durable side holds, so committed work is
/// intact and everything only the old environment had — uncommitted files,
/// installed packages, resident shells — is not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EnvironmentEviction {
    /// The machine the lost environment was resident on, by the name a
    /// placement request would address it with.
    pub(crate) executor: String,
    pub(crate) cause: EvictionCause,
    /// The live processes the old environment held, as its machine last
    /// reported them and worded the way an agent addresses them
    /// (``terminal `dev` ``). Empty when the machine is gone and took its
    /// report with it, which the note then says generically.
    pub(crate) lost_processes: Vec<String>,
}

/// Why an environment was given up and rebuilt elsewhere.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EvictionCause {
    /// The executor session holding it ended and did not come back within
    /// the lease grace.
    Disconnected { absent_ms: u64 },
    /// The executor stayed in the connection table, so nothing ever ended its
    /// session, but it stopped answering for longer than the lease grace. A
    /// half-open remote socket looks exactly like this until the operating
    /// system notices, which can be never.
    Silent { silent_ms: u64, generation: u64 },
    /// The machine holding it cannot prepare cells for its project, so every
    /// batch pinned there would fail before running. `detail` is the
    /// placement rejection that says why.
    CannotPrepareProject { project_id: String, detail: String },
    /// An operator drained the machine holding it, and nothing live held it
    /// there: no terminal, no REPL, no batch in flight. A job's next batch is
    /// new work, which a drain refuses, so the idle environment is released
    /// and acquired on a machine that is not draining.
    Drained,
}

/// How long a machine stays withdrawn from an owner after refusing to provision
/// its environment, when its session outlasts that. A refusal from its storage
/// can clear without the machine reconnecting (another process letting go of a
/// directory), and a withdrawal that outlived its basis would push the owner off
/// a machine that could now serve it.
pub(crate) const HOME_WITHDRAWAL_MS: u64 = 15 * 60 * 1000;

/// A machine withdrawn from one owner's placement because it answered that
/// owner's fresh acquisition with a failure of its own storage.
///
/// The verdict is about the machine as that answer found it, so it holds only
/// while the session that gave the answer lasts and for at most
/// [`HOME_WITHDRAWAL_MS`]: a reconnect is a re-measured machine, and one that is
/// not attached at all is placed nowhere anyway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WithdrawnHome {
    pub(crate) executor_id: String,
    pub(crate) generation: u64,
    pub(crate) withdrawn_at_unix_ms: u64,
}

impl WithdrawnHome {
    /// Whether this withdrawal still binds, given the generation the machine
    /// is attached under now (`None` when it is not attached).
    pub(crate) fn stands(&self, live_generation: Option<u64>, now_unix_ms: u64) -> bool {
        live_generation == Some(self.generation)
            && now_unix_ms.saturating_sub(self.withdrawn_at_unix_ms) < HOME_WITHDRAWAL_MS
    }
}

/// How long the machine a job's home last answered from is remembered. Long
/// enough to span a stretch of a thread being idle, and bounded so that jobs which
/// never acquire again do not keep their entries for the runner's life.
pub(super) const PREVIOUS_HOME_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// The machine a job's home last answered from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreviousHome {
    pub(super) executor_id: String,
    pub(super) recorded_at_unix_ms: u64,
}

/// What an acquisition prices its fresh home by, beyond the machine's forecast.
/// Runner-side and never on the wire, which is why this rides beside
/// [`ResidencyAcquireRequest`] rather than on it.
#[derive(Clone, Debug, Default)]
pub(crate) struct HomePrices {
    pub(crate) home_affinity: Option<HomeAffinity>,
    pub(crate) scratch_sync: Option<ScratchSyncEstimate>,
}

/// A final acquisition result, with what the machines before it answered. A
/// refusal keeps its kind and outcome, so it classifies as it would have alone.
/// A success needs no account: the batch's first line names the machine it ran
/// on, and the refusals before it stay in the log and the placement record.
pub(super) fn naming_earlier_refusals(
    result: ResidencyResult,
    refused: &[String],
) -> ResidencyResult {
    match result {
        ResidencyResult::Failed {
            kind,
            diagnostic,
            cell_outcome,
        } if !refused.is_empty() => ResidencyResult::Failed {
            kind,
            diagnostic: format!("{}. {diagnostic}", refused.join(". ")),
            cell_outcome,
        },
        other => other,
    }
}

/// The refusal an owner reads when the machine granted its environment refused
/// to provision it: the machine, its own words (which name the cell), and that
/// the owner was withdrawn from it and goes elsewhere next.
pub(super) fn home_withdrawal_diagnostic(
    holder: &ResidencyHolder,
    machine: &str,
    refusal: &str,
) -> String {
    let minutes = HOME_WITHDRAWAL_MS / 60_000;
    let refused = home_refusal_sentence(holder, machine, refusal);
    match holder {
        ResidencyHolder::Job { .. } => format!(
            "{refused}. The job is withdrawn from {machine}, so its next batch acquires its environment on another machine; {machine} is considered for it again once it reconnects, or after {minutes} minutes"
        ),
        _ => format!(
            "{refused}. It is withdrawn from {machine}, so its next acquisition is placed on another machine; {machine} is considered for it again once it reconnects, or after {minutes} minutes"
        ),
    }
}

/// Which machine refused to provision an owner's environment, in its own words.
pub(super) fn home_refusal_sentence(
    holder: &ResidencyHolder,
    machine: &str,
    refusal: &str,
) -> String {
    match holder {
        ResidencyHolder::Job { .. } => {
            format!("{machine} refused to provision this job's execution environment: {refusal}")
        }
        other => format!(
            "{machine} refused to provision the execution environment of {}: {refusal}",
            describe_holder(other)
        ),
    }
}

/// Whether the machine a residency route names is answering.
///
/// The one liveness rule for routing. Presence in the connection table is not
/// it: a connection that stays attached while its machine says nothing is in
/// the table and cannot serve a batch, and reading presence as liveness is
/// what pinned a job to a silent machine indefinitely (cairn/4931).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RouteAnswer {
    Live,
    Unanswered(Unanswered),
}

/// A route whose machine is not answering, and since when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Unanswered {
    /// When the machine last answered: its last progress while it is still
    /// attached, or the moment its link was first seen absent.
    pub(super) since_unix_ms: u64,
    pub(super) cause: UnansweredCause,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UnansweredCause {
    /// Not in the connection table, or in it with its sender closed.
    Disconnected,
    /// In the table on an open sender, with no progress past the liveness
    /// bound.
    Silent { generation: u64 },
}

/// Whether this connection's peer has been silent past `bound_ms`, counting
/// only time the runner was listening.
///
/// THE staleness rule. Placement's candidate filter, residency dispatch and
/// routing, claimed-attempt accounting, colocated remediation, the executor
/// status every page shows, and removal all judge a link by it, so none of
/// them can decide a machine is answering that another has already given up
/// on, and none can charge a peer with silence the runner was not there to
/// hear. See [`link_clock`] for the rule itself.
pub(super) fn judge_link(
    entry: &ExecutorConnectionState,
    now_unix_ms: u64,
    bound_ms: u64,
) -> link_clock::LinkJudgement {
    entry.link_clock.judge(
        link_clock::monotonic_now_ns(),
        now_unix_ms.saturating_sub(entry.last_progress_unix_ms),
        bound_ms,
    )
}

/// The peer's silence past the placement liveness bound, or `None` when it is
/// fresh or the runner was not listening.
pub(super) fn link_silence_ms(entry: &ExecutorConnectionState, now_unix_ms: u64) -> Option<u64> {
    match judge_link(entry, now_unix_ms, EXECUTOR_PROGRESS_FRESHNESS_MS) {
        link_clock::LinkJudgement::PeerSilent { silence_ms } => Some(silence_ms),
        _ => None,
    }
}

/// When a closed link last delivered a frame, counting only time the runner was
/// awake. Its reader has ended, so no listening rule applies, and the wall clock
/// would charge the machine for every hour the runner's own host slept.
pub(super) fn last_heard_unix_ms(entry: &ExecutorConnectionState, now_unix_ms: u64) -> u64 {
    entry
        .link_clock
        .received_silence_ms(link_clock::monotonic_now_ns())
        .map_or(entry.last_progress_unix_ms, |silence_ms| {
            now_unix_ms.saturating_sub(silence_ms)
        })
}

/// An owner in the words the executors page uses for it.
pub(super) fn describe_holder(holder: &ResidencyHolder) -> String {
    match holder {
        ResidencyHolder::Job { job_id } => format!("job {job_id}"),
        other => other.storage_key(),
    }
}

/// A runner-clock instant as UTC wall time, for prose a reader lines up
/// against logs.
pub(super) fn describe_utc_ms(unix_ms: u64) -> String {
    i64::try_from(unix_ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            || format!("{unix_ms}ms"),
            |at| at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        )
}

/// Milliseconds as minutes and seconds, for prose about how long a machine
/// has been quiet.
pub(crate) fn describe_quiet_ms(ms: u64) -> String {
    let seconds = ms / 1_000;
    match seconds / 60 {
        0 => format!("{seconds}s"),
        minutes => format!("{minutes}m{:02}s", seconds % 60),
    }
}

/// The live resident processes one machine's last snapshot reports in an
/// owner's environment, named the way an agent addresses them.
pub(super) fn live_resident_process_labels(
    entry: &ExecutorConnectionState,
    holder: &ResidencyHolder,
) -> Vec<String> {
    entry
        .snapshot
        .cells
        .iter()
        .filter(|cell| {
            cell.residency
                .as_ref()
                .is_some_and(|residency| residency.holder == *holder)
        })
        .flat_map(|cell| cell.occupancy.processes.values())
        .filter(|process| process.is_live())
        .map(|process| resident_process_label(&process.kind))
        .collect()
}

/// A resident process named the way an agent addresses it.
pub(super) fn resident_process_label(
    kind: &cairn_common::executor_protocol::ResidentProcessKind,
) -> String {
    use cairn_common::executor_protocol::ResidentProcessKind;
    match kind {
        ResidentProcessKind::Terminal { slug } => format!("terminal `{slug}`"),
        ResidentProcessKind::Repl { slug } => format!("REPL `{slug}`"),
        ResidentProcessKind::Service { name, .. } if !name.is_empty() => {
            format!("service `{name}`")
        }
        ResidentProcessKind::Service { .. } => "a service process".into(),
        ResidentProcessKind::DevInstance { .. } => "a dev instance".into(),
        ResidentProcessKind::WorkflowRuntime { workflow } => format!("workflow `{workflow}`"),
    }
}

/// The live work holding one cell on its machine, named the way an agent
/// addresses it: its live resident processes, and a run batch in flight (a
/// prior epoch's batch still draining counts as one). Empty means nothing binds
/// the environment to the machine it is on.
pub(super) fn cell_live_work(
    occupancy: &cairn_common::executor_protocol::CellOccupancy,
) -> Vec<String> {
    let mut held: Vec<String> = occupancy
        .live_processes()
        .map(|(_, process)| resident_process_label(&process.kind))
        .collect();
    if occupancy.command.is_some() || occupancy.draining.is_some() {
        held.push("a run batch in flight".into());
    }
    held
}

/// The live work holding an owner's environment on one machine, as that
/// machine last reported it.
pub(super) fn environment_live_work(
    entry: &ExecutorConnectionState,
    holder: &ResidencyHolder,
) -> Vec<String> {
    entry
        .snapshot
        .cells
        .iter()
        .filter(|cell| {
            cell.residency
                .as_ref()
                .is_some_and(|residency| residency.holder == *holder)
        })
        .flat_map(|cell| cell_live_work(&cell.occupancy))
        .collect()
}

/// What a drain leaves on a machine: the execution environments resident
/// there, and which of them live work holds in place.
///
/// A drain refuses new work, and a job's next batch is new work even when the
/// job's environment is already on the machine. Only what is live inside an
/// environment (a terminal, a REPL, a batch in flight) is the resident work a
/// drain leaves alone. So an idle environment is released at its job's next
/// batch and acquired elsewhere, and a held one stays until its work stops.
/// These counts say how far from empty a drained machine is, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainResidents {
    /// Execution environments resident on the machine.
    pub environments: usize,
    /// The environments live work holds there, by owner.
    pub held: Vec<HeldEnvironment>,
}

/// One environment a drained machine keeps because live work holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldEnvironment {
    /// The owner, as the executors page names it (`job ...`).
    pub owner: String,
    /// What holds it, the way an agent addresses it (``terminal `dev` ``).
    pub held_by: Vec<String>,
}

impl DrainResidents {
    /// Count what one machine's snapshot reports.
    pub fn of(snapshot: &FleetSnapshot) -> Self {
        let mut residents = Self::default();
        for cell in &snapshot.cells {
            let Some(residency) = cell.residency.as_ref() else {
                continue;
            };
            residents.environments += 1;
            let held_by = cell_live_work(&cell.occupancy);
            if !held_by.is_empty() {
                residents.held.push(HeldEnvironment {
                    owner: describe_holder(&residency.holder),
                    held_by,
                });
            }
        }
        // Prose an agent reads; the snapshot's cell order means nothing.
        residents.held.sort_by(|a, b| a.owner.cmp(&b.owner));
        residents
    }

    /// `refusing new work; N resident environments, M held by live processes`.
    pub fn headline(&self) -> String {
        format!(
            "refusing new work; {} resident environment{}, {} held by live processes",
            self.environments,
            if self.environments == 1 { "" } else { "s" },
            self.held.len()
        )
    }

    /// Who holds what: ``job a, by terminal `dev`; job b, by a run batch in flight``.
    pub fn holders(&self) -> String {
        self.held
            .iter()
            .map(|held| format!("{}, by {}", held.owner, held.held_by.join(", ")))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// What a drain write keeps, in one or two sentences.
    pub fn account(&self) -> String {
        if self.environments == 0 {
            return "Nothing is resident there.".into();
        }
        let idle = self.environments.saturating_sub(self.held.len());
        let mut text = format!(
            "It keeps {} resident environment{}, {} held by live processes",
            self.environments,
            if self.environments == 1 { "" } else { "s" },
            self.held.len()
        );
        if !self.held.is_empty() {
            text.push_str(&format!(
                " and left alone until that work stops ({})",
                self.holders()
            ));
        }
        match idle {
            0 => {}
            1 => text.push_str("; the idle one is released at its job's next batch and acquired on a machine that is not draining"),
            _ => text.push_str("; each idle one is released at its job's next batch and acquired on a machine that is not draining"),
        }
        text.push('.');
        text
    }
}

/// A job's environment that stays on a drained machine, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DrainedHome {
    /// The drained machine, by public name.
    pub(crate) machine: String,
    /// The live work holding the environment there. Empty when nothing live
    /// holds it and it stays only because no other machine can take it.
    pub(crate) held_by: Vec<String>,
}

impl DrainedHome {
    /// The sentence a batch placed there carries, beside its "Ran on" line.
    pub(crate) fn sentence(&self) -> String {
        match self.held_by.as_slice() {
            [] => format!(
                "**{}** is draining, and this batch was placed there because no other machine can take this job's environment.",
                self.machine
            ),
            held => format!(
                "**{}** is draining, and this batch was placed there because {} {} this job's environment there. Once nothing live holds it, the next batch moves to a machine that is not draining.",
                self.machine,
                held.join(", "),
                if held.len() == 1 { "holds" } else { "hold" }
            ),
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PersistentResidencyRoutes {
    #[serde(default)]
    pub(super) routes: Vec<ResidencyRoute>,
}

impl Fleet {
    pub(crate) fn with_residency_route_path(path: PathBuf) -> Self {
        // Routes recorded under the lease shape name owners this build cannot
        // address, and the cells behind them are retired at adoption anyway.
        // Remove the file rather than carrying a second parser for it.
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_file(parent.join("build-slot-lifetime-routes.json"));
        }
        let pool = Self {
            residency_route_path: Arc::new(Some(path.clone())),
            host_runtime: tokio::runtime::Handle::try_current().ok(),
            ..Self::default()
        };
        match load_residency_routes(&path) {
            Ok(routes) => *pool.residency_routes.lock().unwrap() = routes,
            Err(error) => *pool.residency_route_store_error.lock().unwrap() = Some(error),
        }
        pool
    }

    #[track_caller]
    pub(super) fn update_residency_routes<R>(
        &self,
        mutation: impl FnOnce(&mut HashMap<(String, String), ResidencyRoute>) -> R,
    ) -> Result<R, String> {
        let mut routes = self.residency_routes.lock().unwrap();
        let previous = routes.clone();
        let result = mutation(&mut routes);
        if *routes == previous {
            return Ok(result);
        }
        if let Some(path) = self.residency_route_path.as_ref() {
            let started = std::time::Instant::now();
            let persisted = persist_residency_routes(path, &routes);
            note_slow_section(
                "residency_routes.persist",
                std::panic::Location::caller(),
                started.elapsed(),
            );
            if let Err(error) = persisted {
                *routes = previous;
                *self.residency_route_store_error.lock().unwrap() = Some(error.clone());
                return Err(error);
            }
        }
        *self.residency_route_store_error.lock().unwrap() = None;
        Ok(result)
    }

    pub(super) fn ensure_residency_route_store_available(&self) -> Result<(), String> {
        if self.residency_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let Some(path) = self.residency_route_path.as_ref() else {
            *self.residency_route_store_error.lock().unwrap() = None;
            return Ok(());
        };
        let mut routes = self.residency_routes.lock().unwrap();
        if self.residency_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let recovered = load_residency_routes(path)
            .and_then(|recovered| persist_residency_routes(path, &recovered).map(|()| recovered));
        match recovered {
            Ok(recovered) => {
                *routes = recovered;
                *self.residency_route_store_error.lock().unwrap() = None;
                Ok(())
            }
            Err(error) => {
                *self.residency_route_store_error.lock().unwrap() = Some(error.clone());
                Err(error)
            }
        }
    }
}

pub(super) fn load_residency_routes(
    path: &Path,
) -> Result<HashMap<(String, String), ResidencyRoute>, String> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = std::fs::read(path)
        .map_err(|error| format!("read residency route authority {}: {error}", path.display()))?;
    let persisted: PersistentResidencyRoutes = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "parse residency route authority {}: {error}",
            path.display()
        )
    })?;
    let mut routes = HashMap::new();
    for route in persisted.routes {
        let key = (route.executor_id.clone(), route.holder.storage_key());
        if routes.insert(key, route).is_some() {
            return Err(format!(
                "residency route authority {} contains duplicate routes",
                path.display()
            ));
        }
    }
    Ok(routes)
}

pub(super) fn persist_residency_routes(
    path: &Path,
    routes: &HashMap<(String, String), ResidencyRoute>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "create residency route authority directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let mut persisted = PersistentResidencyRoutes {
        routes: routes.values().cloned().collect(),
    };

    persisted.routes.sort_by(|a, b| {
        (&a.executor_id, a.holder.storage_key()).cmp(&(&b.executor_id, b.holder.storage_key()))
    });
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|error| format!("serialize residency route authority: {error}"))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|error| {
        format!(
            "open residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.write_all(&bytes).map_err(|error| {
        format!(
            "write residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "sync residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        format!(
            "publish residency route authority {}: {error}",
            path.display()
        )
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaterializationReadCandidate {
    pub executor_id: String,
    pub generation: u64,
    pub cell_id: String,
    pub materialization_generation: Option<String>,
    pub fence: ResidencyFence,
}

/// Attempt-scoped response, pending route, and object authority. This guard is
/// dropped before the entry guard, so a successor never inherits a failed route.
pub(super) struct ResidencyDispatchGuard {
    pub(super) pool: Fleet,
    pub(super) correlation_id: String,
    pub(super) route: Option<ResidencyRoute>,
    pub(super) authority: Option<(
        Arc<crate::orchestrator::object_plane::ObjectPlaneState>,
        String,
        String,
        String,
        u64,
    )>,
}

impl Drop for ResidencyDispatchGuard {
    fn drop(&mut self) {
        self.pool
            .pending_residency
            .lock()
            .unwrap()
            .remove(&self.correlation_id);
        if let Some(route) = &self.route {
            self.pool.clear_pending_residency_route(route);
        }
        if let Some((plane, request_id, attempt_id, executor_id, generation)) = &self.authority {
            plane.revoke_request(request_id, attempt_id, executor_id, *generation);
        }
    }
}

pub(super) enum ResidencyGrantAnswer {
    Answered(Box<ResidencyResult>),
    Requeued,
    Gone(Box<CellOutcome>),
    LinkSilent,
    /// The claim survived a link gap and now binds to a returning generation.
    /// The answer to the correlation dispatched under the old one may have been
    /// the frame that link dropped, so the machine has to be asked again.
    Rebound {
        generation: u64,
    },
}

impl Fleet {
    /// Select an already-live resident materialization authorized for a run and
    /// matching its exact repository coordinate. Snapshot order never affects the
    /// result; no lease is acquired, renewed, pinned, or refreshed.
    ///
    /// Everything the read needs is taken from the cell that matched, never
    /// looked up again by cell ID. A cell ID is unique only within its project
    /// on one executor, because each project numbers its cells from its own
    /// sequence, so one snapshot routinely holds `slot-3` for two projects. A
    /// second lookup by ID can land on the other project's cell and fence the
    /// read against whoever holds that one.
    pub(crate) fn select_materialization_read_candidate(
        &self,
        run_id: &str,
        job_id: &str,
        project_id: &str,
        repository: &cairn_common::executor_protocol::RepositoryIdentity,
        base_commit: &str,
    ) -> Result<MaterializationReadCandidate, MaterializationReadFailureKind> {
        let connections = self.lock_connections();
        let mut candidates = Vec::new();
        for (executor_id, connection) in connections.iter() {
            for cell in &connection.snapshot.cells {
                if cell.project_id != project_id
                    || cell.lifecycle
                        != cairn_common::executor_protocol::PersistentCellLifecycle::Active
                {
                    continue;
                }
                let Some(residency) = cell.residency.as_ref() else {
                    continue;
                };
                if residency.phase != cairn_common::executor_protocol::ResidencyPhase::Active
                    || residency.current_base_commit != base_commit
                    || residency.repository.identity() != *repository
                {
                    continue;
                }
                // A job's own environment answers before an environment merely
                // serving the same project, and a workflow's before a dev
                // instance's, so the read lands on the most specific holder.
                let holder_rank = match residency.holder {
                    ResidencyHolder::Job { .. } => 0u8,
                    ResidencyHolder::ProjectTerminals { .. } => 1,
                    ResidencyHolder::Workflow { .. } => 2,
                    ResidencyHolder::DevInstance { .. } => 3,
                    ResidencyHolder::Service { .. } => 4,
                };
                let owner_ref_matches = residency.owner_ref.as_ref().is_some_and(|owner| {
                    owner.project_id == project_id && owner.job_id.as_deref() == Some(job_id)
                });
                let holder_id = match &residency.holder {
                    ResidencyHolder::Job { job_id } => job_id.clone(),
                    ResidencyHolder::DevInstance { instance_id } => instance_id.clone(),
                    ResidencyHolder::ProjectTerminals { project_id } => project_id.clone(),
                    ResidencyHolder::Workflow { run_id } => run_id.clone(),
                    ResidencyHolder::Service { service_id } => service_id.clone(),
                };
                let holder_id_matches =
                    residency.owner_ref.is_none() && (holder_id == job_id || holder_id == run_id);
                if !owner_ref_matches && !holder_id_matches {
                    continue;
                }
                let specificity = if owner_ref_matches { 0u8 } else { 1u8 };
                // Ranked on the first element; the connection generation and
                // the holder ride along to build the fence.
                candidates.push((
                    (
                        specificity,
                        holder_rank,
                        holder_id,
                        residency.holder.storage_key(),
                        executor_id,
                        &cell.cell_id,
                        &residency.incarnation_id,
                        cell.cell_epoch,
                        &cell.preparation_fingerprint,
                    ),
                    connection.generation,
                    &residency.holder,
                ));
            }
        }
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        let Some((selected, generation, holder)) = candidates.first() else {
            return Err(MaterializationReadFailureKind::NoActiveMaterializationLease);
        };
        if candidates
            .get(1)
            .is_some_and(|(next, _, _)| next == selected)
        {
            return Err(MaterializationReadFailureKind::MaterializationUnavailable);
        }
        let (_, _, _, _, executor_id, cell_id, incarnation_id, cell_epoch, fingerprint) = selected;
        Ok(MaterializationReadCandidate {
            executor_id: executor_id.to_string(),
            generation: *generation,
            cell_id: cell_id.to_string(),
            materialization_generation: Option::clone(fingerprint),
            fence: ResidencyFence {
                holder: ResidencyHolder::clone(holder),
                incarnation_id: incarnation_id.to_string(),
                cell_epoch: *cell_epoch,
            },
        })
    }

    /// Dispatch a bounded read to the exact executor generation selected from an
    /// authoritative fleet snapshot. This method never performs placement or any
    /// residency operation.
    pub async fn read_resident_materialization(
        &self,
        executor_id: &str,
        generation: u64,
        request: MaterializationReadRequest,
    ) -> MaterializationReadResult {
        let fail = |kind, diagnostic: &str| MaterializationReadResult::Failed {
            kind,
            diagnostic: diagnostic.to_string(),
        };
        let sender = {
            let connections = self.lock_connections();
            let Some(connection) = connections
                .get(executor_id)
                .filter(|connection| connection.generation == generation)
            else {
                return fail(
                    MaterializationReadFailureKind::MaterializationUnavailable,
                    "selected executor generation is unavailable",
                );
            };
            connection.sender.clone()
        };
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_materialization_reads.lock().unwrap().insert(
            correlation_id.clone(),
            PendingMaterializationRead {
                executor_id: executor_id.to_string(),
                generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::MaterializationReadRequest {
                correlation_id: correlation_id.clone(),
                request: request.clone(),
            })
            .is_err()
        {
            self.pending_materialization_reads
                .lock()
                .unwrap()
                .remove(&correlation_id);
            return fail(
                MaterializationReadFailureKind::MaterializationUnavailable,
                "executor connection closed during materialization read dispatch",
            );
        }
        let timeout = Duration::from_millis(
            request
                .deadline_unix_ms
                .saturating_sub(unix_time_ms())
                .max(1),
        );
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => fail(
                MaterializationReadFailureKind::MaterializationUnavailable,
                "executor dropped the materialization read response",
            ),
            Err(_) => {
                self.pending_materialization_reads
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                fail(
                    MaterializationReadFailureKind::DeadlineExceeded,
                    "materialization read response deadline elapsed",
                )
            }
        }
    }

    pub async fn operate_residency(
        &self,
        orch: &Orchestrator,
        operation: ResidencyOperation,
    ) -> ResidencyResult {
        // Boxed: both futures are large (placement, preparation, and route
        // resolution all live inside them), and a caller that awaits this
        // under `block_on` would otherwise carry their whole state on its
        // stack, which deep callers such as terminal start overflowed.
        match operation {
            ResidencyOperation::Acquire { request } => {
                self.acquire_priced_home(orch, request, HomePrices::default())
                    .await
            }
            operation => Box::pin(self.operate_residency_rpc(orch, operation)).await,
        }
    }

    /// Acquire an environment whose fresh home is priced by `prices`. Boxed for
    /// the same reason as [`Self::operate_residency`].
    pub(crate) async fn acquire_priced_home(
        &self,
        orch: &Orchestrator,
        request: ResidencyAcquireRequest,
        prices: HomePrices,
    ) -> ResidencyResult {
        Box::pin(self.acquire_residency(orch, request, prices)).await
    }

    /// Acquire an environment, treating a machine that refuses to provision it
    /// as one more claim rather than the end of the call: the machine is
    /// withdrawn, and the same acquisition goes to the next machine inside the
    /// same wait horizon. Only when none remains is the caller refused, and then
    /// the refusal names every machine that answered.
    pub(super) async fn acquire_residency(
        &self,
        orch: &Orchestrator,
        original: ResidencyAcquireRequest,
        prices: HomePrices,
    ) -> ResidencyResult {
        // Route resolution below may evict the existing home on what the
        // project's setup needs, so the setup it judges by must be the current
        // canonical one, not whatever the last preparation cached.
        //
        // Boxed: preparation's future is large, and inlining it into this
        // already-deep acquisition future overflowed the stack of a terminal
        // acquisition.
        Box::pin(self.refresh_project_setup(orch, &original)).await;
        let mut refused = Vec::new();
        loop {
            let mut retry = None;
            // Boxed for the same reason: this future holds placement, preparation
            // and route resolution across its awaits.
            let result =
                Box::pin(self.acquire_residency_attempt(orch, &original, &prices, &mut retry))
                    .await;
            match retry {
                Some(diagnostic) => refused.push(diagnostic),
                None => return naming_earlier_refusals(result, &refused),
            }
        }
    }

    /// Reattach an owner to the environment its route says `executor_id`
    /// holds: an acquisition sent with no queue grant, which the executor
    /// answers only by converging on a residency it still has.
    ///
    /// The route can outlive the environment. An executor that reclaimed or
    /// released the residency (a lapsed heartbeat, a release still finishing
    /// that a snapshot reported mid-way) answers `NotFound`, and asking it
    /// again would get the same answer forever: the service supervisor's
    /// restarts were refused this way until the service was marked failed
    /// (cairn/6528). So that route is forgotten and the caller places the same
    /// acquisition through the queue, the one path that can build a fresh
    /// environment. Retrying is paced, because a snapshot taken before the
    /// release finished can report the cell again and restore the route, and
    /// bounded by the acquisition's wait horizon.
    pub(super) async fn reattach_residency(
        &self,
        orch: &Orchestrator,
        acquisition: ResidencyAcquireRequest,
        executor_id: &str,
        original: &ResidencyAcquireRequest,
    ) -> Reattached {
        let holder = acquisition.holder.clone();
        let result = self
            .operate_residency_rpc(
                orch,
                ResidencyOperation::Acquire {
                    request: acquisition,
                },
            )
            .await;
        if !matches!(
            result,
            ResidencyResult::Failed {
                kind: ResidencyFailureKind::NotFound,
                ..
            }
        ) {
            return Reattached::Answered(Box::new(result));
        }
        tracing::info!(
            %holder,
            executor = %self.executor_name_for_text(executor_id),
            "the routed executor holds no environment for this owner; placing it through the queue"
        );
        self.forget_unheld_route(&holder, executor_id);
        let remaining = original.wait_horizon_unix_ms.saturating_sub(unix_time_ms());
        if remaining == 0 {
            return Reattached::Answered(Box::new(result));
        }
        tokio::time::sleep(Duration::from_millis(
            remaining.min(REATTACH_REFUSED_PACE_MS),
        ))
        .await;
        Reattached::RouteForgotten
    }

    /// Drop the one route, and the grant evidence beside it, that says
    /// `executor_id` holds `holder`'s environment after it answered that it
    /// does not.
    pub(super) fn forget_unheld_route(&self, holder: &ResidencyHolder, executor_id: &str) {
        let key = (executor_id.to_owned(), holder.storage_key());
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.remove(&key);
            self.granted_cells.lock().unwrap().remove(&key);
        }) {
            tracing::error!(%error, "persist unheld residency route removal failed");
        }
    }

    /// One claim on one machine. `retry` is set, with the refusal's sentence,
    /// when the machine granted this attempt refused and another may take it.
    pub(super) async fn acquire_residency_attempt(
        &self,
        orch: &Orchestrator,
        original: &ResidencyAcquireRequest,
        prices: &HomePrices,
        retry: &mut Option<String>,
    ) -> ResidencyResult {
        let identity = (
            residency_queue_entry_id(&original.holder),
            RESIDENCY_ACQUIRE_ATTEMPT_ID.into(),
        );
        // Duplicate refusal is a signal to converge, not a failed acquisition.
        // Always inspect the live identity before consulting route authority:
        // the winner may already have published its still-pending route.
        let (entry_id, mut entry_rx, placement, prepared) = loop {
            let notified = self.binding_changed.notified();
            if let Some((entry_id, state)) = self
                .queue
                .with_entry_by_identity(&identity, |entry| (entry.id.clone(), entry.state.clone()))
            {
                let remaining = original.wait_horizon_unix_ms.saturating_sub(unix_time_ms());
                if remaining == 0 {
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        format!(
                            "wait horizon elapsed while acquisition entry {entry_id} was {state:?}"
                        ),
                        Some(self.wait_horizon_outcome(
                            &residency_placement_request(original),
                            &self.recorded_declines(&entry_id),
                        )),
                    );
                }
                tokio::select! {
                    _ = notified => {},
                    _ = tokio::time::sleep(Duration::from_millis(remaining.min(250))) => {},
                }
                continue;
            }
            let mut acquisition = original.clone();
            match self.resolve_residency_acquire_route(&mut acquisition) {
                Ok(Some(selected)) => {
                    // A winner may have entered between the first check and
                    // route resolution. Do not dispatch into its pending route.
                    if self
                        .queue
                        .with_entry_by_identity(&identity, |_| ())
                        .is_some()
                    {
                        continue;
                    }
                    match self
                        .reattach_residency(orch, acquisition, &selected.executor_id, original)
                        .await
                    {
                        Reattached::Answered(result) => return *result,
                        Reattached::RouteForgotten => continue,
                    }
                }
                Ok(None) => {}
                Err(failure) => return failure,
            }
            if unix_time_ms() >= original.wait_horizon_unix_ms {
                return residency_core_failure(
                    ResidencyFailureKind::Admission,
                    "execution environment acquisition wait horizon elapsed",
                    Some(self.wait_horizon_outcome(&residency_placement_request(original), &[])),
                );
            }
            let mut placement = residency_placement_request(&acquisition);
            let prepared = match self.prepare_execution(orch, &mut placement).await {
                Ok(prepared) => prepared,
                Err(outcome) => {
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        "prepare execution environment placement",
                        Some(outcome),
                    )
                }
            };
            if let Err(diagnostic) =
                require_colocated_population(&mut placement, &prepared.executor_config)
            {
                return residency_core_failure(ResidencyFailureKind::Admission, diagnostic, None);
            }
            // A machine that just refused to provision this owner's environment
            // would refuse it again, and an idle one claims every fresh
            // acquisition first. Placement looks past it until it lapses.
            let withdrawals = self.withdrawals_that_steer(&placement, &original.holder);
            placement.avoided_executor_ids.extend(withdrawals);
            placement.home_affinity = prices.home_affinity.clone();
            placement.scratch_sync = prices.scratch_sync.clone();
            let Ok((entry_id, entry_rx)) = self.queue.enqueue(
                queue::NewEntry {
                    request: placement.clone(),
                    admission_kind: CellAdmissionKind::Residency,
                    interactive: original.holder.is_interactive(),
                    // An acquisition is what provisions the environment, so
                    // it never names one to run inside.
                    execution_residency: None,
                    standing: false,
                },
                unix_time_ms(),
            ) else {
                continue;
            };
            // Preparing can yield for long enough for a winner to acquire and
            // leave the queue. Recheck authority under our new identity hold.
            let mut acquisition = original.clone();
            let routed = self.resolve_residency_acquire_route(&mut acquisition);
            if !matches!(routed, Ok(None)) {
                self.queue
                    .take(&identity, queue::TerminalState::Cancelled, unix_time_ms());
                self.binding_changed.notify_waiters();
                match routed {
                    Ok(Some(selected)) => {
                        match self
                            .reattach_residency(orch, acquisition, &selected.executor_id, original)
                            .await
                        {
                            Reattached::Answered(result) => return *result,
                            Reattached::RouteForgotten => continue,
                        }
                    }
                    Err(failure) => return failure,
                    Ok(None) => unreachable!(),
                }
            }
            break (entry_id, entry_rx, placement, prepared);
        };
        let mut entry_guard = EntryDropGuard {
            pool: self.clone(),
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            entry_id: entry_id.clone(),
            granted_to: None,
            cause: None,
            armed: true,
        };
        'offer: loop {
            // An entry comes back here only when it has returned to the queue,
            // so whatever claim follows is a new one. A grant delivered to an
            // earlier claim -- even by the same machine, reconnected -- says
            // nothing about it, and remembering it would make the guard Cancel
            // a claim no grant reached, leaving that claim's ticket standing.
            // A held claim rebound across a link gap never passes through here,
            // so it keeps its grant.
            entry_guard.granted_to = None;
            let Placement {
                selected,
                mut decision,
                ..
            } = match self
                .select_executor(
                    &placement,
                    None,
                    &prepared.placement_policy,
                    Some(&entry_id),
                )
                .await
            {
                Ok(placed) => placed,
                Err(outcome) => {
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        "select execution environment executor",
                        Some(outcome),
                    )
                }
            };
            // Each attempt starts from the submitted coordinate. A remote
            // decline must not erase a colocated path for the next candidate.
            let mut acquisition = original.clone();
            let mut attempt_request = placement.clone();
            attempt_request.repository = acquisition.repository.clone();
            let needs_objects = !selected.colocated
                && !matches!(
                    acquisition.repository,
                    RepositoryLocator::ScratchOnly { .. }
                );
            if needs_objects {
                let repository = acquisition.repository.identity();
                acquisition.repository = RepositoryLocator::ManagedObjects {
                    project_id: repository.project_id,
                    repository_id: repository.repository_id,
                    object_format: repository.object_format,
                };
                attempt_request.repository = acquisition.repository.clone();
                if let PlacementOutcome::Selected(selection) = &mut decision.outcome {
                    selection.object_transfer = Some(ObjectTransferCoordinate {
                        repository: attempt_request.repository.identity(),
                        request_id: identity.0.clone(),
                        attempt_id: identity.1.clone(),
                        executor_id: selected.executor_id.clone(),
                        connection_generation: selected.generation,
                    });
                }
            }
            // An acquisition is what provisions the environment, so it never
            // names one to run inside.
            match self
                .offer_once(&entry_id, &attempt_request, &selected, &mut decision)
                .await
            {
                OfferAnswer::Requeued => continue,
                OfferAnswer::Expired => {
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        "execution environment acquisition wait horizon elapsed",
                        Some(self.refuse_at_horizon(
                            &placement,
                            &prepared.placement_policy,
                            Some(&entry_id),
                            Vec::new(),
                        )),
                    )
                }
                OfferAnswer::Gone => {
                    withdraw_claim(&selected, &entry_id);
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        "execution environment acquisition left the queue",
                        Some(entry_rx.await.unwrap_or_else(|_| {
                            executor_unavailable("acquisition cancelled".into())
                        })),
                    );
                }
                OfferAnswer::Claimed => {}
            }
            let dispatch_error = {
                let connections = self.lock_connections();
                selected_link_dispatch_error(&connections, &selected, unix_time_ms())
            };
            if let Some(diagnostic) = dispatch_error {
                // A machine declaration may already have made this claim
                // offerable. A link gap alone never does.
                if matches!(
                    self.queue.state_of(&entry_id),
                    Some(queue::EntryState::Queued)
                ) {
                    continue;
                }
                return residency_core_failure(ResidencyFailureKind::Admission, diagnostic, None);
            }
            let route = ResidencyRoute {
                holder: acquisition.holder.clone(),
                repository: acquisition.repository.clone(),
                executor_id: selected.executor_id.clone(),
                pending: true,
            };
            let moved_off_drain = self
                .environment_evictions
                .lock()
                .unwrap()
                .get(&acquisition.holder.storage_key())
                .filter(|eviction| eviction.cause == EvictionCause::Drained)
                .map(|eviction| eviction.executor.clone());
            if let Some(from) = moved_off_drain {
                tracing::info!(
                    holder = %acquisition.holder,
                    %from,
                    to = %self.executor_name_for_text(&selected.executor_id),
                    "an idle execution environment left a drained machine and is acquired on another"
                );
            }
            let correlation_id = uuid::Uuid::new_v4().to_string();
            let mut dispatch = ResidencyDispatchGuard {
                pool: self.clone(),
                correlation_id: correlation_id.clone(),
                route: Some(route.clone()),
                authority: None,
            };
            if let Err(error) = self.reserve_pending_residency_route(route) {
                return residency_core_failure(
                    ResidencyFailureKind::Persistence,
                    format!("persist pending residency route authority: {error}"),
                    None,
                );
            }
            let (tx, rx) = oneshot::channel();
            self.pending_residency.lock().unwrap().insert(
                correlation_id.clone(),
                PendingLifetimeResult {
                    executor_id: selected.executor_id.clone(),
                    generation: selected.generation,
                    waiter: tx,
                    granted_entry_id: Some(entry_id.clone()),
                },
            );
            match self.queue.state_of(&entry_id) {
                Some(queue::EntryState::Queued) => {
                    drop(dispatch);
                    continue;
                }
                Some(queue::EntryState::Claimed {
                    executor_id,
                    generation,
                    ..
                }) if executor_id == selected.executor_id && generation == selected.generation => {}
                _ => {
                    return residency_core_failure(
                        ResidencyFailureKind::Admission,
                        "residency acquisition claim changed before its grant",
                        None,
                    )
                }
            }
            // All fallible preparation and route persistence precede authority.
            // Nothing between authorization and send can return without revoking.
            if needs_objects {
                dispatch.authority = Some((
                    prepared.object_plane.clone(),
                    identity.0.clone(),
                    identity.1.clone(),
                    selected.executor_id.clone(),
                    selected.generation,
                ));
                prepared.object_plane.authorize_request(
                    &attempt_request,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            let granted_request = acquisition;
            if self
                .send_new_admission(
                    &selected,
                    &placement,
                    // Ordinary placement materializes a new environment.
                    false,
                    ExecutorMessage::ResidencyRequest {
                        correlation_id,
                        config: ResidencyRuntimeConfig::Install(prepared.executor_config.clone()),
                        operation: ResidencyOperation::Acquire {
                            request: granted_request.clone(),
                        },
                        granted_entry_id: Some(entry_id.clone()),
                    },
                )
                .is_err()
            {
                drop(dispatch);
                if matches!(
                    self.queue.state_of(&entry_id),
                    Some(queue::EntryState::Queued)
                ) {
                    continue;
                }
                return residency_core_failure(
                    ResidencyFailureKind::Admission,
                    "executor connection closed while granting residency acquisition",
                    None,
                );
            }
            entry_guard.granted_to = Some(selected.executor_id.clone());
            let mut response = rx;
            let mut dispatched_generation = selected.generation;
            loop {
                match self
                    .await_residency_grant(
                        &entry_id,
                        &mut response,
                        &mut entry_rx,
                        dispatched_generation,
                    )
                    .await
                {
                    ResidencyGrantAnswer::Rebound { generation } => {
                        if let Some(rx) = self.redispatch_rebound_acquisition(
                            &mut dispatch,
                            &entry_id,
                            &selected.executor_id,
                            generation,
                            &granted_request,
                            &prepared.executor_config,
                        ) {
                            response = rx;
                        }
                        dispatched_generation = generation;
                    }
                    ResidencyGrantAnswer::Requeued => {
                        drop(dispatch);
                        continue 'offer;
                    }
                    ResidencyGrantAnswer::Gone(outcome) => {
                        return residency_core_failure(
                            ResidencyFailureKind::Admission,
                            "execution environment acquisition left the queue",
                            Some(*outcome),
                        )
                    }
                    ResidencyGrantAnswer::LinkSilent => {
                        return residency_core_failure(
                            ResidencyFailureKind::Admission,
                            self.residency_silence_diagnostic(
                                &selected.executor_id,
                                selected.generation,
                            ),
                            Some(CellOutcome::Unavailable {
                                reason: CellUnavailableReason::Deadline {
                                    host_pressure: None,
                                    substrate: Some(self.executor_deadline_evidence(
                                        &selected.executor_id,
                                        selected.generation,
                                    )),
                                },
                                diagnostic: self.residency_silence_diagnostic(
                                    &selected.executor_id,
                                    selected.generation,
                                ),
                            }),
                        )
                    }
                    ResidencyGrantAnswer::Answered(result) => {
                        let another_remains = matches!(*result, ResidencyResult::Failed { .. })
                            && self.home_candidate_remains(&placement, &selected.executor_id);
                        let (result, withdrawn) = self.withdraw_refusing_home(
                            &granted_request.holder,
                            &selected,
                            PlacementScope::of(&placement).targeted() && !another_remains,
                            *result,
                        );
                        self.record_residency_result(
                            &selected.executor_id,
                            dispatched_generation,
                            &result,
                        );
                        // Clear a failed pending route before releasing the identity.
                        drop(dispatch);
                        // This caller owns the typed result; queue.take journals
                        // completion without manufacturing command execution metadata.
                        self.queue
                            .take(&identity, queue::TerminalState::Completed, unix_time_ms());
                        entry_guard.disarm();
                        self.binding_changed.notify_waiters();
                        // Only an answer that withdrew the machine continues:
                        // the runner's own persistence failures share its kind
                        // and must not loop. And only while a machine remains,
                        // because an untargeted request with every machine
                        // avoided would wait out a horizon of up to the batch
                        // ceiling for one to attach.
                        if withdrawn
                            && another_remains
                            && unix_time_ms() < placement.wait_horizon_unix_ms
                        {
                            if let ResidencyResult::Failed { diagnostic, .. } = &result {
                                *retry = Some(diagnostic.clone());
                            }
                        }
                        return result;
                    }
                }
            }
        }
    }

    /// Whether a machine other than the one that just refused could be placed
    /// on, by the same structural survey placement reads. Requirements the
    /// attempt already stamped count, so this is asked of the attempt's request.
    pub(super) fn home_candidate_remains(&self, placement: &CellRequest, refused_by: &str) -> bool {
        let mut request = placement.clone();
        request.avoided_executor_ids.push(refused_by.to_owned());
        self.any_home_candidate(&request)
    }

    pub(super) fn any_home_candidate(&self, request: &CellRequest) -> bool {
        let connections = self.lock_connections().clone();
        let unattached = self.unattached_enrolled_remotes();
        matches!(
            survey_candidates(
                &connections,
                &unattached,
                request,
                unix_time_ms(),
                &HashMap::new(),
                &AdmissionView::of_request(request),
            ),
            Ok(survey) if !survey.usable.is_empty()
        )
    }

    /// The owner's standing withdrawals this request looks past.
    ///
    /// A withdrawal steers placement among machines. It never decides the answer
    /// for a caller who declared where to go: a targeted request with nothing
    /// else to choose would be refused without its machine being asked, on the
    /// strength of one answer, for as long as the withdrawal stands.
    pub(super) fn withdrawals_that_steer(
        &self,
        placement: &CellRequest,
        holder: &ResidencyHolder,
    ) -> Vec<String> {
        let withdrawn = self.withdrawn_home_executors(holder);
        if withdrawn.is_empty() || !PlacementScope::of(placement).targeted() {
            return withdrawn;
        }
        let mut steered = placement.clone();
        steered
            .avoided_executor_ids
            .extend(withdrawn.iter().cloned());
        match self.any_home_candidate(&steered) {
            true => withdrawn,
            false => Vec::new(),
        }
    }

    /// Ask a machine whose link came back to converge on the acquisition it
    /// already holds.
    ///
    /// The executor's outgoing queue outlives its links, so a response produced
    /// during a gap is delivered on the next session — except the one frame the
    /// writer had already dequeued when the socket refused it. Nothing on the
    /// machine will send that answer again, and a claim whose cell is held is
    /// accounted for by every reconnect snapshot, so without this the original
    /// acquire waits for as long as the new link stays fresh. A plain acquire
    /// with the same declaration converges on the held cell without capacity
    /// and answers with the same state; if the acquisition is still in flight
    /// it queues behind it on the machine's own holder flight. The old
    /// correlation is forgotten first, so a late answer to it is dropped rather
    /// than delivered twice. `None` means no connection holds that generation
    /// right now; the caller keeps waiting and the next rebind asks again.
    pub(super) fn redispatch_rebound_acquisition(
        &self,
        dispatch: &mut ResidencyDispatchGuard,
        entry_id: &str,
        executor_id: &str,
        generation: u64,
        acquisition: &ResidencyAcquireRequest,
        config: &ExecutorConfig,
    ) -> Option<oneshot::Receiver<ResidencyResult>> {
        let selected = {
            let connections = self.lock_connections();
            connections
                .get(executor_id)
                .filter(|entry| entry.generation == generation)
                .map(selected_executor)
        }?;
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_residency.lock().unwrap();
            pending.remove(&dispatch.correlation_id);
            pending.insert(
                correlation_id.clone(),
                PendingLifetimeResult {
                    executor_id: executor_id.to_owned(),
                    generation,
                    waiter: tx,
                    granted_entry_id: Some(entry_id.to_owned()),
                },
            );
        }
        dispatch.correlation_id = correlation_id.clone();
        log::info!(
            "executor_id={executor_id} generation={generation} asking the returning machine to converge on claimed acquisition {entry_id}"
        );
        // A send the new link refuses leaves the waiter bound to this
        // generation; the link that replaces it rebinds the claim again.
        let _ = selected.sender.send(ExecutorMessage::ResidencyRequest {
            correlation_id,
            config: ResidencyRuntimeConfig::Install(config.clone()),
            operation: ResidencyOperation::Acquire {
                request: acquisition.clone(),
            },
            granted_entry_id: None,
        });
        Some(rx)
    }

    pub(super) async fn await_residency_grant(
        &self,
        entry_id: &str,
        response: &mut oneshot::Receiver<ResidencyResult>,
        entry_rx: &mut oneshot::Receiver<CellOutcome>,
        dispatched_generation: u64,
    ) -> ResidencyGrantAnswer {
        let mut deadline = Instant::now() + Duration::from_millis(RESIDENCY_RESPONSE_FLOOR_MS);
        let mut observed = Instant::now();
        loop {
            let notified = self.binding_changed.notified();
            let connection_ready = self.connection_ready.notified();
            let binding =
                match self.queue.state_of(entry_id) {
                    Some(queue::EntryState::Queued) => return ResidencyGrantAnswer::Requeued,
                    Some(queue::EntryState::Claimed {
                        executor_id,
                        generation,
                        ..
                    }) => (executor_id, generation),
                    _ => {
                        return ResidencyGrantAnswer::Gone(Box::new(entry_rx.await.unwrap_or_else(
                            |_| executor_unavailable("acquisition cancelled".into()),
                        )))
                    }
                };
            if binding.1 != dispatched_generation {
                return ResidencyGrantAnswer::Rebound {
                    generation: binding.1,
                };
            }
            let now = Instant::now();
            if self.residency_link_is_fresh(&binding.0, binding.1) {
                deadline += now.saturating_duration_since(observed);
            }
            observed = now;
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                return ResidencyGrantAnswer::LinkSilent;
            }
            tokio::select! {
                result = &mut *response => {
                    // MachineGone may resolve the old response and requeue the
                    // same entry in one sweep. Its new state wins that race.
                    match self.queue.state_of(entry_id) {
                        Some(queue::EntryState::Queued) => return ResidencyGrantAnswer::Requeued,
                        Some(queue::EntryState::Claimed { .. }) => return ResidencyGrantAnswer::Answered(Box::new(result.unwrap_or_else(|_|
                            residency_core_failure(ResidencyFailureKind::Admission, "executor dropped the residency acquisition response", None)))),
                        _ => return ResidencyGrantAnswer::Gone(Box::new(entry_rx.await.unwrap_or_else(|_| executor_unavailable("acquisition cancelled".into())))),
                    }
                },
                outcome = &mut *entry_rx => return ResidencyGrantAnswer::Gone(Box::new(outcome.unwrap_or_else(|_| executor_unavailable("acquisition cancelled".into())))),
                _ = notified => {},
                _ = connection_ready => {},
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(250))) => {},
            }
        }
    }

    /// Reattachments and the ten non-acquiring operations consume no capacity.
    /// A resident started with a declared command is fit-tested on that
    /// command as a batch of it would be, keyed by the command's class: a
    /// terminal running `bun run test:rust` compiles the same rustc. `None`
    /// when it fits, has no command, or nothing is known. An interactive
    /// shell declares nothing and is not tested; each batch run in it is.
    ///
    /// Kills this machine posted against earlier residents are learned first,
    /// so a terminal killed for memory protects the next one.
    pub(super) async fn resident_command_misfit(
        &self,
        db: Arc<cairn_db::storage::LocalDb>,
        selected: &SelectedExecutor,
        project_id: &str,
        launch_command: Option<&str>,
    ) -> Option<String> {
        let context = ReservationPlan::profile_context(selected);
        self.learn_resident_kills(db.clone(), &selected.executor_id, &context)
            .await;
        let command = launch_command?;
        let class = cairn_common::executor_protocol::CellCommandClass::classify(command);
        let suite = resource_profiles::suite_key(project_id, None, class)?;
        let evidence =
            resource_profiles::resolve_process_demand(db, None, Some(&suite), class, &context)
                .await;
        let connections = self.lock_connections();
        let entry = connections.get(&selected.executor_id)?;
        // A resident has no command identity, and the kills it posts carry its
        // class but not its command, so its class suite is the only record its
        // command has: what that suite measured, here or on another platform,
        // refuses as a measurement of it would. A batch's class suite only
        // defers (its own identity measures it on the first run), but a
        // terminal is pinned to its environment's machine, where deferring
        // would start it anyway. The project's largest rustc, another suite's
        // number, still only defers.
        let own_suite = suite.split_once('/').map(|(_, name)| name);
        let reason = match process_fit(&evidence, entry)? {
            ProcessFit::Refused(reason) => reason,
            ProcessFit::Deferred(misfit) if misfit.suite.as_deref() == own_suite => {
                PlacementRejectionReason::ProcessTooLarge(misfit)
            }
            _ => return None,
        };
        Some(format!(
            "{} cannot run this terminal's command: {}. Nothing was started; run the command as a batch, which placement sends to a machine that can hold it, or start the terminal on such a machine.",
            executor_public_name(entry),
            reason.describe()
        ))
    }

    /// Fold the out-of-memory kills `executor_id` posted against residents
    /// started with a declared command into those commands' suites, each kill
    /// once. A resident produces no completion, so this is the only way its
    /// kill teaches anything.
    ///
    /// Nothing here is the runner's own memory, nor the machine's occupancy:
    /// the kill itself carries its resident's project and declared command
    /// class, captured when the resident started, because a terminal whose
    /// command was its life exits and is released before this runs. Each
    /// kill is counted once by its stored identity (machine, time, pid), so a
    /// rebuilt runner learns later kills like any other and none twice.
    pub(super) async fn learn_resident_kills(
        &self,
        db: Arc<cairn_db::storage::LocalDb>,
        executor_id: &str,
        context: &resource_profiles::ProfileContext,
    ) {
        let (kills, memory_brake): (
            Vec<(
                String,
                cairn_common::executor_protocol::OomKillReport,
                Option<u64>,
            )>,
            bool,
        ) = {
            let connections = self.lock_connections();
            let Some(entry) = connections.get(executor_id) else {
                return;
            };
            let memory_brake = memory_brake_exists(entry);
            let total = entry
                .health
                .machine
                .memory
                .value()
                .map(|memory| memory.total_bytes);
            let kills = entry
                .health
                .memory_record
                .oom_kills
                .iter()
                .filter(|kill| {
                    unix_time_ms().saturating_sub(kill.killed_at_unix_ms)
                        <= resource_profiles::RESIDENT_KILL_LEARNING_HORIZON_MS
                })
                .filter_map(|kill| {
                    if !matches!(
                        kill.owner,
                        Some(cairn_common::executor_protocol::LiveUsageSubject::Resident { .. })
                    ) {
                        return None;
                    }
                    let suite = resource_profiles::suite_key(
                        kill.project_id.as_deref()?,
                        None,
                        kill.command_class?,
                    )?;
                    Some((suite, kill.clone(), total))
                })
                .collect();
            (kills, memory_brake)
        };
        for (suite, kill, total) in kills {
            resource_profiles::observe_suite_kill(
                db.clone(),
                &suite,
                context,
                &kill,
                total,
                memory_brake,
            )
            .await;
        }
    }

    /// What placement knows of `executor_id`'s admission beside one
    /// completion, for reading a kill in it.
    pub(super) fn machine_admission(
        &self,
        executor_id: &str,
        batch_siblings: u32,
    ) -> resource_profiles::MachineAdmission {
        resource_profiles::MachineAdmission {
            memory_brake: self
                .lock_connections()
                .get(executor_id)
                .is_some_and(memory_brake_exists),
            batch_siblings,
        }
    }

    pub(super) async fn operate_residency_rpc(
        &self,
        orch: &Orchestrator,
        mut operation: ResidencyOperation,
    ) -> ResidencyResult {
        let mut admission_request = None;
        let (selected, executor_config, object_request, object_plane) = match &mut operation {
            ResidencyOperation::Acquire {
                request: acquisition,
            } => {
                let selected = match self.resolve_residency_acquire_route(acquisition) {
                    Ok(Some(selected)) => selected,
                    Ok(None) => {
                        return residency_core_failure(
                            ResidencyFailureKind::Unavailable,
                            "execution environment route disappeared before reattachment",
                            None,
                        )
                    }
                    Err(failure) => return failure,
                };
                let mut placement = residency_placement_request(acquisition);
                admission_request = Some(placement.clone());
                let prepared = match self.prepare_execution(orch, &mut placement).await {
                    Ok(prepared) => prepared,
                    Err(outcome) => {
                        return residency_core_failure(
                            ResidencyFailureKind::Admission,
                            "prepare execution environment reattachment",
                            Some(outcome),
                        )
                    }
                };
                (
                    selected,
                    ResidencyRuntimeConfig::Install(prepared.executor_config),
                    None,
                    None,
                )
            }
            _ => {
                let Some(holder) = residency_operation_holder(&operation) else {
                    return residency_core_failure(
                        ResidencyFailureKind::InvalidDeclaration,
                        "this operation names no execution environment",
                        None,
                    );
                };
                let RoutedCell {
                    selected,
                    residency,
                    ..
                } = match self.resolve_residency_operation_route(holder) {
                    Ok(routed) => routed,
                    Err(refusal) => return refusal,
                };
                if let ResidencyOperation::StartProcess { process_key, .. } = &operation {
                    let mut request = residency_runtime_config_request(&residency);
                    request.request_id = format!("{}:start:{process_key}", request.request_id);
                    request.attempt_id = "start-process".into();
                    admission_request = Some(request);
                }
                // Judged before anything is prepared: a refusal costs nothing.
                if let ResidencyOperation::StartProcess { process, .. } = &operation {
                    if let Some(refusal) = self
                        .resident_command_misfit(
                            orch.db.local.clone(),
                            &selected,
                            &residency.repository.identity().project_id,
                            process.launch_command.as_deref(),
                        )
                        .await
                    {
                        return residency_core_failure(
                            ResidencyFailureKind::Admission,
                            refusal,
                            None,
                        );
                    }
                }
                let executor_config = if matches!(
                    operation,
                    ResidencyOperation::StartProcess { .. }
                        | ResidencyOperation::RefreshCheckout { .. }
                        | ResidencyOperation::MaterializeConflict { .. }
                ) {
                    let mut config_request = residency_runtime_config_request(&residency);
                    match self.prepare_execution(orch, &mut config_request).await {
                        Ok(prepared) => ResidencyRuntimeConfig::Install(prepared.executor_config),
                        Err(outcome) => {
                            return residency_core_failure(
                                ResidencyFailureKind::Admission,
                                "prepare execution environment operation",
                                Some(outcome),
                            )
                        }
                    }
                } else {
                    ResidencyRuntimeConfig::NotRequired
                };
                if !selected.colocated {
                    if let ResidencyOperation::RefreshCheckout {
                        fence, base_commit, ..
                    } = &operation
                    {
                        let request = residency_refresh_request(&residency, fence, base_commit);
                        (
                            selected,
                            executor_config,
                            Some(request),
                            Some(orch.object_plane.clone()),
                        )
                    } else {
                        (selected, executor_config, None, None)
                    }
                } else {
                    (selected, executor_config, None, None)
                }
            }
        };
        let dispatch_error = {
            let connections = self.lock_connections();
            selected_link_dispatch_error(&connections, &selected, unix_time_ms())
        };
        if let Some(diagnostic) = dispatch_error {
            return residency_core_failure(ResidencyFailureKind::Admission, diagnostic, None);
        }
        if let ResidencyOperation::StartProcess { process, .. } = &operation {
            if let Err(diagnostic) = crate::fleet::residency::refuse_runner_secret_in_start(
                process,
                orch.mcp_auth.get_secret_for_mcp().ok().as_deref(),
            ) {
                return residency_core_failure(
                    ResidencyFailureKind::InvalidState,
                    diagnostic,
                    None,
                );
            }
        }
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_residency.lock().unwrap().insert(
            correlation_id.clone(),
            PendingLifetimeResult {
                executor_id: selected.executor_id.clone(),
                generation: selected.generation,
                waiter: tx,
                granted_entry_id: None,
            },
        );
        let dispatch = ResidencyDispatchGuard {
            pool: self.clone(),
            correlation_id: correlation_id.clone(),
            route: None,
            authority: object_request.as_ref().zip(object_plane.as_ref()).map(
                |(request, plane)| {
                    (
                        plane.clone(),
                        request.request_id.clone(),
                        request.attempt_id.clone(),
                        selected.executor_id.clone(),
                        selected.generation,
                    )
                },
            ),
        };
        if let (Some(request), Some(plane)) = (&object_request, &object_plane) {
            plane.authorize_request(request, &selected.executor_id, selected.generation);
        }
        // How the start frames its stdout is recorded before the start goes
        // out, so the relay guard scrubs even its first chunk accordingly.
        if let ResidencyOperation::StartProcess {
            fence,
            process_key,
            process,
            ..
        } = &operation
        {
            self.resident_output.lock().unwrap().declare_stdout_framing(
                &selected.executor_id,
                &fence.holder.storage_key(),
                process_key,
                process.stdout_framing,
            );
        }
        // A terminal or REPL that holds its node's scratch syncs it over this
        // connection's object channel, at start and again when it exits.
        if let ResidencyOperation::StartProcess { process, .. } = &operation {
            if let Some(grant) = &process.node_scratch {
                orch.object_plane.scratch.authorize(
                    &selected.executor_id,
                    selected.generation,
                    &grant.scratch_id,
                );
            }
        }
        // A relayed process's context is bound to the link its start goes out
        // on; a stop or a release ends the contexts it ends the processes of.
        let relayed_start = match &operation {
            ResidencyOperation::StartProcess {
                process_key,
                process:
                    cairn_common::executor_protocol::ResidentProcessSpec {
                        relay: Some(grant), ..
                    },
                ..
            } => {
                if let Err(diagnostic) = self.resident_relays().bind(
                    &grant.relay_id,
                    &selected.executor_id,
                    selected.generation,
                ) {
                    self.pending_residency
                        .lock()
                        .unwrap()
                        .remove(&correlation_id);
                    return residency_core_failure(
                        ResidencyFailureKind::InvalidState,
                        diagnostic,
                        None,
                    );
                }
                Some((grant.relay_id.clone(), process_key.clone()))
            }
            ResidencyOperation::StopProcess { fence, process_key } => {
                self.resident_relays()
                    .revoke_process(&fence.holder, process_key);
                None
            }
            ResidencyOperation::Release { fence } => {
                self.resident_relays().revoke_holder(&fence.holder);
                None
            }
            _ => None,
        };
        let new_admission = matches!(
            &operation,
            ResidencyOperation::Acquire { .. } | ResidencyOperation::StartProcess { .. }
        );
        let message = ExecutorMessage::ResidencyRequest {
            correlation_id: correlation_id.clone(),
            config: executor_config,
            operation,
            granted_entry_id: None,
        };
        let sent = if new_admission {
            // The refusal's own words reach the caller: a standing drain names
            // its owner, reason and generation, which "connection closed"
            // would erase.
            self.send_new_admission(
                &selected,
                admission_request
                    .as_ref()
                    .expect("new admissions carry placement identity"),
                // A start inside, or a reattachment to, an environment whose
                // live route is this machine: resident work, never a new one.
                true,
                message,
            )
            .map_err(|refusal| refusal.diagnostic)
        } else {
            // Inputs, renewals and cleanup keep already admitted work alive.
            selected.sender.send(message).map_err(|_| {
                "executor connection closed while sending residency operation".to_string()
            })
        };
        if let Err(diagnostic) = sent {
            self.pending_residency
                .lock()
                .unwrap()
                .remove(&correlation_id);
            return residency_core_failure(ResidencyFailureKind::Admission, diagnostic, None);
        }
        let result = match self
            .await_link_bound_response(rx, &selected.executor_id, selected.generation)
            .await
        {
            LinkBoundResponse::Answered(result) => result,
            LinkBoundResponse::Dropped => residency_core_failure(
                ResidencyFailureKind::Admission,
                "executor dropped the residency operation response",
                None,
            ),
            LinkBoundResponse::LinkSilent => {
                self.pending_residency
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                // The executor stopped reporting. Whether it is contended or
                // wedged is not something this wait can tell on its own, so it
                // carries the executor's own substrate evidence and lets the
                // caller classify: a busy machine is worth presenting to again, a
                // stalled one is not. Without this the failure would be untyped,
                // and an untyped placement failure is a refusal by construction.
                let substrate =
                    self.executor_deadline_evidence(&selected.executor_id, selected.generation);
                let diagnostic =
                    self.residency_silence_diagnostic(&selected.executor_id, selected.generation);
                residency_core_failure(
                    ResidencyFailureKind::Admission,
                    diagnostic.clone(),
                    Some(CellOutcome::Unavailable {
                        reason: CellUnavailableReason::Deadline {
                            host_pressure: None,
                            substrate: Some(substrate),
                        },
                        diagnostic,
                    }),
                )
            }
        };
        self.record_residency_result(&selected.executor_id, selected.generation, &result);
        if let Some((relay_id, process_key)) = relayed_start {
            let started = match &result {
                ResidencyResult::State { cell } => cell
                    .occupancy
                    .processes
                    .get(&process_key)
                    .map(|process| process.generation),
                _ => None,
            };
            let mut relays = self.resident_relays();
            match started {
                Some(generation) => relays.learn_generation(&relay_id, generation),
                None => relays.revoke(&relay_id),
            }
        }
        drop(dispatch);
        result
    }

    /// Record what a residency result says about its cell, as answered on link
    /// `generation`.
    ///
    /// Every state result counts, not only a grant's: a later answer is a
    /// newer account of the same cell than any the executor has reported.
    pub(super) fn record_residency_result(
        &self,
        executor_id: &str,
        generation: u64,
        result: &ResidencyResult,
    ) {
        match result {
            ResidencyResult::State { cell } => {
                if let Some(residency) = cell.residency.as_ref() {
                    self.remember_previous_home(&residency.holder, executor_id);
                    let key = (executor_id.to_owned(), residency.holder.storage_key());
                    if let Err(error) = self.update_residency_routes(|routes| {
                        routes.insert(
                            key.clone(),
                            ResidencyRoute {
                                holder: residency.holder.clone(),
                                repository: residency.repository.clone(),
                                executor_id: executor_id.to_owned(),
                                pending: false,
                            },
                        );
                        // Under the route lock, so a snapshot applied at the
                        // same moment sees the route and its grant together.
                        self.granted_cells.lock().unwrap().insert(
                            key.clone(),
                            GrantedCell {
                                generation,
                                cell: cell.clone(),
                                recorded_unix_ms: unix_time_ms(),
                            },
                        );
                    }) {
                        tracing::error!(%error, "persist authoritative residency route failed");
                    }
                }
            }
            ResidencyResult::Released { holder, .. } => {
                let released = holder.storage_key();
                if let Err(error) = self.update_residency_routes(|routes| {
                    routes.retain(|(_, known_holder), _| known_holder != &released);
                    self.granted_cells
                        .lock()
                        .unwrap()
                        .retain(|(_, known_holder), _| known_holder != &released);
                }) {
                    tracing::error!(%error, "persist released residency route removal failed");
                }
            }
            // A materialization writes files into a cell that already exists, so
            // it neither establishes nor retires a residency route.
            ResidencyResult::ConflictMaterialized { .. }
            | ResidencyResult::AttachmentMaterialized { .. } => {}
            ResidencyResult::Failed { .. } => {}
        }
    }

    pub(super) fn remember_previous_home(&self, holder: &ResidencyHolder, executor_id: &str) {
        if !matches!(holder, ResidencyHolder::Job { .. }) {
            return;
        }
        let now = unix_time_ms();
        let mut homes = self.previous_homes.lock().unwrap();
        homes.retain(|_, home| {
            now.saturating_sub(home.recorded_at_unix_ms) < PREVIOUS_HOME_RETENTION_MS
        });
        homes.insert(
            holder.storage_key(),
            PreviousHome {
                executor_id: executor_id.to_owned(),
                recorded_at_unix_ms: now,
            },
        );
    }

    /// What leaving this thread's natural machines costs its home: the runner's
    /// own platform, and the machine that last held it.
    pub(crate) fn thread_home_affinity(&self, holder: &ResidencyHolder) -> HomeAffinity {
        HomeAffinity {
            runner_os: std::env::consts::OS.to_owned(),
            previous_executor_id: self
                .previous_homes
                .lock()
                .unwrap()
                .get(&holder.storage_key())
                .map(|home| home.executor_id.clone()),
        }
    }

    pub(super) fn reserve_pending_residency_route(
        &self,
        route: ResidencyRoute,
    ) -> Result<(), String> {
        debug_assert!(route.pending);
        self.update_residency_routes(|routes| {
            routes.insert(
                (route.executor_id.clone(), route.holder.storage_key()),
                route,
            );
        })
    }

    pub(super) fn clear_pending_residency_route(&self, route: &ResidencyRoute) {
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|key, known| {
                key != &(route.executor_id.clone(), route.holder.storage_key())
                    || !known.pending
                    || known.holder != route.holder
            });
        }) {
            tracing::error!(%error, "persist pending residency route removal failed");
        }
    }

    /// What is running inside a residency's cell, by the newest account the
    /// runner holds of it: resident processes, a batch in flight, a prior
    /// epoch still draining. `None` when no answering executor holds it.
    pub(crate) fn residency_occupancy(
        &self,
        holder: &ResidencyHolder,
    ) -> Option<cairn_common::executor_protocol::CellOccupancy> {
        self.resolve_residency_operation_route(holder)
            .ok()
            .map(|routed| routed.occupancy)
    }

    /// The live fence for a residency as the fleet currently observes it. This
    /// lets a caller that knows only the holder — job teardown, say — address it
    /// without carrying an incarnation and epoch of its own.
    pub fn residency_fence(&self, holder: &ResidencyHolder) -> Option<ResidencyFence> {
        self.resolve_residency_operation_route(holder)
            .ok()
            .map(|routed| ResidencyFence {
                holder: routed.residency.holder,
                incarnation_id: routed.residency.incarnation_id,
                cell_epoch: routed.cell_epoch,
            })
    }

    /// The executor and cell a non-acquiring operation on an owner's
    /// environment addresses, or the refusal that says why there is none.
    ///
    /// The route table is the authority on where the environment is. The cell
    /// comes from the newest account the runner holds on that route's current
    /// link: the residency result the executor last returned for it, until a
    /// report of the executor's own carries it, and that report after. Routing
    /// by reports alone refused a process start sent straight after the acquire
    /// that granted its cell, because the report showing the grant was still
    /// behind the executor's stalled runtime.
    ///
    /// Every refusal is `Unavailable` and names which routing fact it is: the
    /// environment's machine is not answering, its latest report no longer
    /// carries the environment, its acquisition has not been granted, or no
    /// route exists at all. None of them is a verdict on the operation itself.
    #[allow(clippy::result_large_err)]
    pub(super) fn resolve_residency_operation_route(
        &self,
        holder: &ResidencyHolder,
    ) -> Result<RoutedCell, ResidencyResult> {
        let mut routes = self
            .residency_routes
            .lock()
            .unwrap()
            .values()
            .filter(|route| route.holder == *holder)
            .cloned()
            .collect::<Vec<_>>();
        // The refusal names one route, and the map has no order of its own.
        routes.sort_by(|left, right| left.executor_id.cmp(&right.executor_id));
        let now = unix_time_ms();
        let mut routed = Vec::new();
        let mut unanswered = Vec::new();
        let mut unreported = Vec::new();
        let mut ungranted = Vec::new();
        for route in routes {
            match self.route_answer(&route.executor_id, now) {
                RouteAnswer::Live => match self.routed_cell(&route.executor_id, holder) {
                    Some(cell) => routed.push(cell),
                    None if route.pending => ungranted.push(route.executor_id),
                    None => unreported.push(route.executor_id),
                },
                RouteAnswer::Unanswered(_) if route.pending => ungranted.push(route.executor_id),
                RouteAnswer::Unanswered(quiet) => unanswered.push((route.executor_id, quiet)),
            }
        }
        if routed.len() > 1 {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "multiple executors report the same execution environment",
                None,
            ));
        }
        if let Some(cell) = routed.pop() {
            return Ok(cell);
        }
        let diagnostic = if let Some((executor_id, quiet)) = unanswered
            .iter()
            .min_by_key(|(_, quiet)| now.saturating_sub(quiet.since_unix_ms))
        {
            let state = match quiet.cause {
                UnansweredCause::Disconnected => "disconnected".to_string(),
                UnansweredCause::Silent { generation } => {
                    format!("connected but silent on link generation {generation}")
                }
            };
            format!(
                "{} holds this execution environment and has not answered since {} ({} ago; {state})",
                self.executor_name_for_text(executor_id),
                describe_utc_ms(quiet.since_unix_ms),
                describe_quiet_ms(now.saturating_sub(quiet.since_unix_ms)),
            )
        } else if let Some(executor_id) = unreported.first() {
            format!(
                "{}'s latest report no longer carries this execution environment",
                self.executor_name_for_text(executor_id)
            )
        } else if let Some(executor_id) = ungranted.first() {
            format!(
                "this execution environment's acquisition on {} has not been granted yet",
                self.executor_name_for_text(executor_id)
            )
        } else {
            "no executor holds a route to this execution environment".to_string()
        };
        Err(residency_core_failure(
            ResidencyFailureKind::Unavailable,
            diagnostic,
            None,
        ))
    }

    /// The cell an executor's current link accounts for this owner: the last
    /// residency result answered on that link while no later report has carried
    /// the cell, otherwise the link's latest report.
    ///
    /// The result goes first because it can only still be held if every report
    /// applied since it lacked the cell (one carrying it clears it), so it is
    /// never older than the report beside it.
    pub(super) fn routed_cell(
        &self,
        executor_id: &str,
        holder: &ResidencyHolder,
    ) -> Option<RoutedCell> {
        let granted = self
            .granted_cells
            .lock()
            .unwrap()
            .get(&(executor_id.to_owned(), holder.storage_key()))
            .cloned();
        let connections = self.lock_connections();
        let entry = connections.get(executor_id)?;
        let cell = granted
            .filter(|granted| granted.generation == entry.generation)
            .map(|granted| granted.cell)
            .or_else(|| {
                entry
                    .snapshot
                    .cells
                    .iter()
                    .find(|cell| {
                        cell.residency
                            .as_ref()
                            .is_some_and(|residency| residency.holder == *holder)
                    })
                    .cloned()
            })?;
        Some(RoutedCell {
            selected: selected_executor(entry),
            residency: cell.residency?,
            cell_epoch: cell.cell_epoch,
            occupancy: cell.occupancy,
        })
    }

    /// The executor currently hosting a residency, if any. Placement for a batch
    /// bound to that residency is not a choice: it must land on the executor that
    /// owns the cell.
    ///
    /// A live route answers first. An owner can briefly hold a route to a session
    /// that ended beside the one it just acquired on, and the route map has no
    /// order, so reading the first match could pin a batch to a machine that is
    /// gone while its environment answers from another.
    pub(crate) fn residency_route_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        self.live_residency_route_executor(holder).or_else(|| {
            self.residency_routes
                .lock()
                .unwrap()
                .values()
                .find(|route| route.holder == *holder)
                .map(|route| route.executor_id.clone())
        })
    }

    /// The executor a job home occupies, or the colocated executor as a guess
    /// before it has been routed. Placement chooses a fresh home among every
    /// machine the batch's selector admits, so the guess only decides whether a
    /// batch founds a home or leaves, and acquisition then says where the home
    /// actually is. A route naming an executor that is no
    /// longer connected names nothing this job can occupy: the cell it recorded
    /// died with that executor session, and the next acquisition rebuilds the
    /// environment wherever placement lands
    /// ([`Self::resolve_residency_acquire_route`]). Reading it as occupancy is
    /// how a batch came to be measured for compatibility against a machine that
    /// was gone.
    pub(crate) fn job_home_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        self.live_residency_route_executor(holder).or_else(|| {
            self.lock_connections()
                .values()
                .find(|entry| entry.colocated && !entry.sender.is_closed())
                .map(|entry| entry.identity.executor_id.clone())
        })
    }

    /// Where a job's environment is, as far as the runner can say before acquiring
    /// it.
    ///
    /// A recorded route is the authority even while its machine is away: the link
    /// may have dropped with the cell and its processes intact, and acquisition
    /// waits out exactly that grace. Only with no route at all does this fall back
    /// to the colocated machine as a guess, and it says so, because no cell and
    /// therefore no live process exists yet. The guess decides whether a batch
    /// founds a home or leaves; it does not decide where a founded home lands.
    pub(crate) fn predicted_job_home(&self, holder: &ResidencyHolder) -> Option<PredictedJobHome> {
        if let Some(executor_id) = self.residency_route_executor(holder) {
            return Some(PredictedJobHome {
                executor_id,
                recorded: true,
            });
        }
        self.job_home_executor(holder)
            .map(|executor_id| PredictedJobHome {
                executor_id,
                recorded: false,
            })
    }

    /// Whether an executor satisfies a selector, answered for a machine that is
    /// away as well as one that is here.
    ///
    /// A connected machine is judged on its live advertisement. An absent one can
    /// still be judged on what its enrollment records, its name and platform, and
    /// a mismatch there is certain. Toolchains, sandbox capability and containment
    /// are known only from a live advertisement, so a selector asking for them
    /// gets `None` from an absent machine: unknown, not a mismatch.
    pub(crate) fn recorded_executor_satisfies_selector(
        &self,
        executor_id: &str,
        selector: &ExecutorSelector,
    ) -> Option<bool> {
        if let Some(answer) = self.executor_satisfies_selector(executor_id, selector) {
            return Some(answer);
        }
        let (name, os) = if executor_id == COLOCATED_EXECUTOR_ID {
            (
                LOCAL_EXECUTOR_NAME.to_string(),
                std::env::consts::OS.to_string(),
            )
        } else {
            let enrolled = self.enrolled_remotes.lock().unwrap();
            let record = enrolled.get(executor_id)?;
            (record.name.clone(), record.os.clone())
        };
        if selector
            .name
            .as_deref()
            .is_some_and(|wanted| !executor_names_match(wanted, &name))
            || selector
                .os
                .as_deref()
                .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(&os))
        {
            return Some(false);
        }
        let needs_advertisement = !selector.required_toolchains.is_empty()
            || selector.requires_macos_nested_sandbox_write
            || selector.containment.is_some();
        (!needs_advertisement).then_some(true)
    }

    /// The executor a residency is routed to, only while that executor holds a
    /// live connection.
    pub(super) fn live_residency_route_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        let routed = self
            .residency_routes
            .lock()
            .unwrap()
            .values()
            .filter(|route| route.holder == *holder)
            .map(|route| route.executor_id.clone())
            .collect::<Vec<_>>();
        let now = unix_time_ms();
        routed
            .into_iter()
            .find(|executor_id| self.route_answer(executor_id, now) == RouteAnswer::Live)
    }

    /// What to call an executor in text a person or an agent reads, whether or
    /// not it is still connected. A lost environment has to name the machine it
    /// was on, and that machine is by definition not connected.
    pub(crate) fn executor_name_for_text(&self, executor_id: &str) -> String {
        if let Some(name) = self.executor_public_name(executor_id) {
            return name;
        }
        if executor_id == COLOCATED_EXECUTOR_ID {
            return LOCAL_EXECUTOR_NAME.to_string();
        }
        self.enrolled_remotes
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|record| record.name.clone())
            .unwrap_or_else(|| executor_id.to_string())
    }

    /// Every executor identity that answers to a public name: attached,
    /// enrolled but away, or the colocated executor. Empty when nothing this
    /// runner knows of carries the name.
    pub(crate) fn executor_ids_named(&self, name: &str) -> Vec<String> {
        let mut ids: std::collections::BTreeSet<String> = self
            .lock_connections()
            .iter()
            .filter(|(_, entry)| executor_names_match(name, &executor_public_name(entry)))
            .map(|(executor_id, _)| executor_id.clone())
            .collect();
        ids.extend(
            self.enrolled_remotes
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, record)| executor_names_match(name, &record.name))
                .map(|(executor_id, _)| executor_id.clone()),
        );
        if executor_names_match(name, LOCAL_EXECUTOR_NAME) {
            ids.insert(COLOCATED_EXECUTOR_ID.to_string());
        }
        ids.into_iter().collect()
    }

    /// The platform one machine advertises: its live connection's, or, while
    /// it is away, what its enrollment last recorded. `None` for an executor
    /// this runner does not know.
    pub(crate) fn executor_platform(&self, executor_id: &str) -> Option<ExecutorPlatform> {
        if let Some(entry) = self.lock_connections().get(executor_id) {
            return Some(ExecutorPlatform::advertised(
                &entry.advertisement.capabilities,
            ));
        }
        if executor_id == COLOCATED_EXECUTOR_ID {
            return Some(ExecutorPlatform::this_process());
        }
        self.enrolled_remotes
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|record| ExecutorPlatform {
                os: record.os.to_ascii_lowercase(),
                arch: record.arch.to_ascii_lowercase(),
            })
    }

    /// The platform the machine answering to a public name runs, from its live
    /// advertisement or, while it is away, its enrollment. `None` when nothing
    /// this runner knows of carries the name, or when machines of different
    /// platforms share it.
    pub(crate) fn executor_os_named(&self, name: &str) -> Option<String> {
        let mut platforms: std::collections::BTreeSet<String> = self
            .lock_connections()
            .values()
            .filter(|entry| executor_names_match(name, &executor_public_name(entry)))
            .map(|entry| entry.advertisement.capabilities.os.to_ascii_lowercase())
            .collect();
        platforms.extend(
            self.enrolled_remotes
                .lock()
                .unwrap()
                .values()
                .filter(|record| executor_names_match(name, &record.name))
                .map(|record| record.os.to_ascii_lowercase()),
        );
        if executor_names_match(name, LOCAL_EXECUTOR_NAME) {
            platforms.insert(std::env::consts::OS.to_string());
        }
        let mut platforms = platforms.into_iter();
        match (platforms.next(), platforms.next()) {
            (Some(os), None) => Some(os),
            _ => None,
        }
    }

    /// When this runner first observed that an executor was not connected.
    ///
    /// Stamped on disconnect, and stamped here for an executor this process has
    /// never seen — a route loaded from the authority file after a runner
    /// restart names a machine whose absence starts being counted the first time
    /// anyone asks for it, not at some instant nobody observed. Answered on the
    /// wall clock its callers compare against, less any time the host slept.
    pub(super) fn link_absent_since(&self, executor_id: &str, now_unix_ms: u64) -> u64 {
        let stamp = *self
            .link_absence_since
            .lock()
            .unwrap()
            .entry(executor_id.to_string())
            .or_insert_with(link_clock::monotonic_now_ns);
        now_unix_ms.saturating_sub(link_clock::awake_ms_since(stamp))
    }

    /// Stage a machine absent for `ago_ms` of awake time.
    #[cfg(test)]
    pub(crate) fn stage_link_absent(&self, executor_id: &str, ago_ms: u64) {
        self.link_absence_since.lock().unwrap().insert(
            executor_id.to_string(),
            link_clock::stamp_awake_ms_ago(ago_ms),
        );
    }

    /// Whether a machine has gone unanswered for the whole lease grace, by the
    /// rule eviction uses, so the owner of a resident there is never told
    /// something different from what placement is about to act on.
    pub(crate) fn machine_lost_past_grace(&self, executor_id: &str) -> bool {
        if self.executor_link_restoration(executor_id) == placement::LinkRestoration::Restoring {
            return false;
        }
        let now = unix_time_ms();
        matches!(
            self.route_answer(executor_id, now),
            RouteAnswer::Unanswered(unanswered)
                if now.saturating_sub(unanswered.since_unix_ms) >= ENVIRONMENT_LEASE_GRACE_MS
        )
    }

    /// Whether the link a residency failure came from is one the runner is still
    /// trying to restore.
    ///
    /// The environment's own executor answers this, not the colocated one: a job
    /// whose home is on a remote is neither helped nor bound by what the
    /// supervisor is doing about a machine it never asked for. A machine still
    /// inside its lease grace counts as restoring, because the runner has
    /// neither given up on it nor rebuilt the environment — and a caller that
    /// waits rather than refuses is precisely what that grace is for.
    pub(crate) fn residency_link_restoration(
        &self,
        holder: &ResidencyHolder,
    ) -> placement::LinkRestoration {
        let routed = self
            .residency_routes
            .lock()
            .unwrap()
            .values()
            .filter(|route| route.holder == *holder)
            .map(|route| route.executor_id.clone())
            .collect::<Vec<_>>();
        let now = unix_time_ms();
        let within_grace = routed.iter().any(|executor_id| {
            matches!(
                self.route_answer(executor_id, now),
                RouteAnswer::Unanswered(unanswered)
                    if now.saturating_sub(unanswered.since_unix_ms) < ENVIRONMENT_LEASE_GRACE_MS
            )
        });
        if within_grace {
            return placement::LinkRestoration::Restoring;
        }
        self.link_restoration()
    }

    /// Whether the machine a route names is answering, by the one staleness
    /// rule placement and dispatch use.
    pub(super) fn route_answer(&self, executor_id: &str, now_unix_ms: u64) -> RouteAnswer {
        let present = self.lock_connections().get(executor_id).map(|entry| {
            (
                entry.sender.is_closed(),
                entry.generation,
                last_heard_unix_ms(entry, now_unix_ms),
                link_silence_ms(entry, now_unix_ms),
            )
        });
        match present {
            // Read after the table lock is released: the absence clock has a
            // lock of its own, and attach takes the two separately.
            None => RouteAnswer::Unanswered(Unanswered {
                since_unix_ms: self.link_absent_since(executor_id, now_unix_ms),
                cause: UnansweredCause::Disconnected,
            }),
            Some((true, _, last_heard_unix_ms, _)) => RouteAnswer::Unanswered(Unanswered {
                since_unix_ms: last_heard_unix_ms,
                cause: UnansweredCause::Disconnected,
            }),
            // Silence as the link clock counted it: only while the runner was
            // listening, so a sleep does not spend the grace.
            Some((false, generation, _, Some(silence_ms))) => RouteAnswer::Unanswered(Unanswered {
                since_unix_ms: now_unix_ms.saturating_sub(silence_ms),
                cause: UnansweredCause::Silent { generation },
            }),
            Some((false, _, _, None)) => RouteAnswer::Live,
        }
    }

    /// Whether the runner has positive evidence it is bringing one executor's
    /// link back.
    ///
    /// Only the colocated executor is supervised by this runner, so only it can
    /// produce that evidence. A remote that stopped answering is read as gone
    /// until it attaches again, which is the reading that acts rather than the
    /// one that waits — the same default [`placement`] takes everywhere else.
    pub(super) fn executor_link_restoration(
        &self,
        executor_id: &str,
    ) -> placement::LinkRestoration {
        if executor_id == COLOCATED_EXECUTOR_ID {
            self.link_restoration()
        } else {
            placement::LinkRestoration::NotRestoring
        }
    }

    /// Report, once, that an owner's execution environment was rebuilt because
    /// the executor session holding the previous one ended.
    pub(crate) fn take_environment_eviction(
        &self,
        holder: &ResidencyHolder,
    ) -> Option<EnvironmentEviction> {
        self.environment_evictions
            .lock()
            .unwrap()
            .remove(&holder.storage_key())
    }

    /// Withdraw the machine a fresh acquisition was granted to from that
    /// owner's placement when it answered with a failure of its own storage,
    /// and say so in the refusal the owner reads.
    ///
    /// Such a failure is a fact about the machine (a slot authority it cannot
    /// quarantine, a checkout it cannot materialize), so an acquisition placed
    /// the same way meets it again. Before this, an idle machine claimed every
    /// fresh acquisition of a job whose home it could not build, and every
    /// batch of that job was refused before it ran, whatever the batch's own
    /// selector named, because each one starts by acquiring the home.
    ///
    /// Work the caller narrowed to this machine, by a pin, a name, or a selector
    /// nothing else satisfies, is not withdrawn from it: it has nowhere else to
    /// go, and one answer should not refuse the caller's own choice for the
    /// length of a withdrawal. It gets the machine's refusal, and the next batch
    /// asks again. The second value says whether this answer withdrew the
    /// machine, which is what entitles the acquisition to continue.
    pub(super) fn withdraw_refusing_home(
        &self,
        holder: &ResidencyHolder,
        selected: &SelectedExecutor,
        narrowed_to_this_machine: bool,
        result: ResidencyResult,
    ) -> (ResidencyResult, bool) {
        let ResidencyResult::Failed {
            kind,
            diagnostic,
            cell_outcome,
        } = result
        else {
            return (result, false);
        };
        if !placement::residency_failure_withdraws_home(&kind) {
            return (
                ResidencyResult::Failed {
                    kind,
                    diagnostic,
                    cell_outcome,
                },
                false,
            );
        }
        if narrowed_to_this_machine {
            let machine = self.executor_name_for_text(&selected.executor_id);
            return (
                ResidencyResult::Failed {
                    kind,
                    diagnostic: home_refusal_sentence(holder, &machine, &diagnostic),
                    cell_outcome,
                },
                false,
            );
        }
        let now = unix_time_ms();
        {
            let mut withdrawn = self.withdrawn_homes.lock().unwrap();
            // Owners that never acquire again would otherwise keep their
            // entries for the runner's life.
            withdrawn.retain(|_, homes| {
                homes.retain(|home| {
                    now.saturating_sub(home.withdrawn_at_unix_ms) < HOME_WITHDRAWAL_MS
                });
                !homes.is_empty()
            });
            let homes = withdrawn.entry(holder.storage_key()).or_default();
            homes.retain(|home| home.executor_id != selected.executor_id);
            homes.push(WithdrawnHome {
                executor_id: selected.executor_id.clone(),
                generation: selected.generation,
                withdrawn_at_unix_ms: now,
            });
        }
        let machine = self.executor_name_for_text(&selected.executor_id);
        tracing::warn!(
            %holder,
            executor = %machine,
            generation = selected.generation,
            ?kind,
            %diagnostic,
            "machine refused to provision an execution environment; withdrawn from this owner's placement"
        );
        (
            ResidencyResult::Failed {
                kind,
                diagnostic: home_withdrawal_diagnostic(holder, &machine, &diagnostic),
                cell_outcome,
            },
            true,
        )
    }

    /// The machines an owner's next fresh acquisition must not be placed on,
    /// forgetting every withdrawal whose basis has lapsed.
    pub(crate) fn withdrawn_home_executors(&self, holder: &ResidencyHolder) -> Vec<String> {
        let key = holder.storage_key();
        let Some(homes) = self.withdrawn_homes.lock().unwrap().get(&key).cloned() else {
            return Vec::new();
        };
        // Generations are read with the withdrawal map released, so this
        // never holds it across the connection table.
        let now = unix_time_ms();
        let (standing, lapsed): (Vec<_>, Vec<_>) = homes
            .into_iter()
            .partition(|home| home.stands(self.live_generation(&home.executor_id), now));
        if !lapsed.is_empty() {
            let mut withdrawn = self.withdrawn_homes.lock().unwrap();
            if let Some(homes) = withdrawn.get_mut(&key) {
                homes.retain(|home| !lapsed.contains(home));
                if homes.is_empty() {
                    withdrawn.remove(&key);
                }
            }
        }
        standing.into_iter().map(|home| home.executor_id).collect()
    }

    /// Whether one live executor satisfies the caller's complete selector.
    ///
    /// A missing result means the executor is not in the live fleet snapshot;
    /// callers must not interpret missing knowledge as selector compatibility.
    pub(crate) fn executor_satisfies_selector(
        &self,
        executor_id: &str,
        selector: &ExecutorSelector,
    ) -> Option<bool> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| matches_selector(entry, selector))
    }

    /// Where an acquisition for an owner that already has an environment must
    /// go, or `None` when it is free to be placed.
    ///
    /// A route is a **lease against a live executor session**, not a permanent
    /// binding. The cell it names lives inside that session, so a session that
    /// ended took the environment with it and the route that survives names
    /// nothing. Reading such a route as authority anyway is what stranded jobs
    /// indefinitely (CAIRN-4332): every batch resolved the dead binding and was
    /// refused structurally, while nothing ever removed the route, because route
    /// reconciliation rewrites an executor's routes from that executor's own
    /// snapshots and a disconnected executor sends none. The refusal was
    /// therefore permanent, on a fleet with live machines that could have
    /// rebuilt the environment in seconds.
    ///
    /// So a route with no live session behind it is evicted here and the
    /// acquisition falls through to ordinary placement, which materializes the
    /// environment again at the state the durable side holds. The one exception
    /// is a link the runner is actively restoring: that environment is not gone,
    /// it is seconds away, so the failure is capacity-shaped and the caller
    /// waits for it rather than moving.
    #[allow(clippy::result_large_err)]
    pub(super) fn resolve_residency_acquire_route(
        &self,
        request: &mut ResidencyAcquireRequest,
    ) -> Result<Option<SelectedExecutor>, ResidencyResult> {
        if let Err(error) = self.ensure_residency_route_store_available() {
            return Err(residency_core_failure(
                ResidencyFailureKind::Persistence,
                format!("residency route authority is unavailable: {error}"),
                None,
            ));
        }
        let existing = {
            let known = self.residency_routes.lock().unwrap();
            known
                .values()
                .filter(|route| route.holder == request.holder)
                .cloned()
                .collect::<Vec<_>>()
        };
        if existing.is_empty() {
            return Ok(None);
        }
        // A route counts as live by the same rule dispatch will judge it by. A
        // connection still in the table that has stopped answering would pass a
        // presence test here and then fail dispatch for staleness, refusing every
        // batch while nothing ever evicted it (cairn/4931).
        let now = unix_time_ms();
        let mut unanswered = Vec::new();
        let mut answering = Vec::new();
        for route in &existing {
            match self.route_answer(&route.executor_id, now) {
                RouteAnswer::Live => answering.push(route.clone()),
                RouteAnswer::Unanswered(quiet) => unanswered.push((route.clone(), quiet)),
            }
        }
        let mut live = {
            let connections = self.lock_connections();
            answering
                .into_iter()
                .filter_map(|route| {
                    connections
                        .get(&route.executor_id)
                        .map(|entry| (route.clone(), selected_executor(entry)))
                })
                .collect::<Vec<_>>()
        };
        // Two machines both claiming to hold one owner's environment right now is
        // a contradiction only they can resolve, and picking one would be picking
        // which live cell to orphan.
        if live.len() > 1 {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "multiple executors report the same execution environment",
                None,
            ));
        }
        let Some((route, selected)) = live.pop() else {
            self.evict_lost_environment(&request.holder, &unanswered)?;
            return Ok(None);
        };
        if route.repository.identity() != request.repository.identity() {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "this owner's execution environment already holds another repository",
                None,
            ));
        }
        // The route carries the coordinate the executor actually holds, which is
        // where an acquirer's own repository locator can legitimately differ
        // (a colocated path resolved differently, say). The identity above is
        // what makes them the same environment; this is the address.
        request.repository = route.repository.clone();
        // A home on a machine that cannot prepare its project fails every batch
        // pinned there before it runs, so it is not worth keeping: rebuild it
        // where the project can be prepared. Uncommitted state is lost, which
        // the eviction notice says, and is the lesser loss than a job that can
        // run nothing at all.
        let project_id = route.repository.identity().project_id;
        if let Some(reason) =
            self.home_preparation_rejection(&selected.executor_id, &route.repository, &project_id)
        {
            self.evict_home(
                &request.holder,
                &selected,
                EvictionCause::CannotPrepareProject {
                    project_id,
                    detail: reason.describe(),
                },
            )?;
            return Ok(None);
        }
        // The route is consulted before placement, and a batch pinned to its
        // home passes a drain by design so resident work can finish. Without
        // this the drain never saw a resident job's batches at all, and every
        // one of them landed on the machine the operator had drained.
        if self.drained_home_is_idle(&selected.executor_id, request) {
            self.evict_home(&request.holder, &selected, EvictionCause::Drained)?;
            return Ok(None);
        }
        // One live session answers for this environment, so a route left over
        // from a session that ended is stale by construction. Dropping it now
        // keeps a later reconnection from resurrecting an ambiguity that no
        // acquisition could resolve.
        self.forget_superseded_routes(&request.holder, &route.executor_id);
        Ok(Some(selected))
    }

    /// Answer for an environment whose every route names an executor that is not
    /// answering, and leave the route authority in the state that answer implies.
    ///
    /// Disconnected and silent are one case measured from one instant: when the
    /// machine last answered. A machine that stays attached while saying nothing
    /// cannot serve a batch any better than one that left, and a half-open
    /// remote socket can stay attached indefinitely, so presence earns no more
    /// grace than absence does.
    ///
    /// Absence from the connection map is not death. A transport that dropped
    /// leaves the executor process running and its cells intact, so treating the
    /// first missed connection as proof the environment is gone would turn every
    /// network blip into lost uncommitted files, packages, and resident
    /// processes. What separates the two is time: a link that flapped comes back,
    /// and a machine that left does not. So the environment is held for a grace
    /// period during which the failure is capacity-shaped and the caller waits,
    /// and only a machine still unreachable at the end of it is treated as gone.
    ///
    /// Evicting the routes is what makes the next step placement rather than
    /// refusal, and it is done before anything is placed so the two cannot
    /// disagree about how many environments this owner has.
    #[allow(clippy::result_large_err)]
    pub(super) fn evict_lost_environment(
        &self,
        holder: &ResidencyHolder,
        lost: &[(ResidencyRoute, Unanswered)],
    ) -> Result<(), ResidencyResult> {
        if let Some((restoring, _)) = lost.iter().find(|(route, _)| {
            self.executor_link_restoration(&route.executor_id)
                == placement::LinkRestoration::Restoring
        }) {
            // Not lost, just away: the supervisor says it is bringing this link
            // back, and rebuilding the environment somewhere else would abandon a
            // cell that is about to answer again. Carrying the placement evidence
            // is what makes this a wait rather than a refusal.
            let diagnostic = format!(
                "the machine holding this execution environment ({}) is reconnecting",
                self.executor_name_for_text(&restoring.executor_id)
            );
            return Err(residency_core_failure(
                ResidencyFailureKind::Admission,
                diagnostic.clone(),
                Some(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic,
                }),
            ));
        }
        let now = unix_time_ms();
        // The most recently heard machine decides: while any of this owner's
        // routes still has a machine that might answer, the environment is not
        // yet gone.
        let Some((deciding, quiet)) = lost
            .iter()
            .min_by_key(|(_, quiet)| now.saturating_sub(quiet.since_unix_ms))
        else {
            return Ok(());
        };
        let quiet_ms = now.saturating_sub(quiet.since_unix_ms);
        let lost_processes = self.resident_processes_held_for(holder, lost);
        if quiet_ms < ENVIRONMENT_LEASE_GRACE_MS {
            let machine = self.executor_name_for_text(&deciding.executor_id);
            // Rounded up: a deadline stated a second early is one the caller
            // can watch pass with nothing happening.
            let remaining =
                describe_quiet_ms((ENVIRONMENT_LEASE_GRACE_MS - quiet_ms).div_ceil(1_000) * 1_000);
            let state = match quiet.cause {
                UnansweredCause::Disconnected => format!(
                    "has not answered for {}; if it does not reconnect",
                    describe_quiet_ms(quiet_ms)
                ),
                UnansweredCause::Silent { .. } => format!(
                    "is connected but has not answered for {}; if it stays silent",
                    describe_quiet_ms(quiet_ms)
                ),
            };
            let cost = match lost_processes.as_slice() {
                [] => String::new(),
                processes => format!(
                    ", and {} running there would not survive",
                    processes.join(", ")
                ),
            };
            let diagnostic = format!(
                "the machine holding this execution environment ({machine}) {state}, this job's environment is rebuilt elsewhere in {remaining}{cost}"
            );
            return Err(residency_core_failure(
                ResidencyFailureKind::Admission,
                diagnostic.clone(),
                Some(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic,
                }),
            ));
        }
        let cause = match quiet.cause {
            UnansweredCause::Disconnected => EvictionCause::Disconnected {
                absent_ms: quiet_ms,
            },
            UnansweredCause::Silent { generation } => EvictionCause::Silent {
                silent_ms: quiet_ms,
                generation,
            },
        };
        // Named in a stable order, because this reaches an agent as prose and
        // the map these came from has no order of its own.
        let mut names = lost
            .iter()
            .map(|(route, _)| self.executor_name_for_text(&route.executor_id))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        let executor = names.join(", ");
        // A silent machine still holds the cell, and may start answering again
        // before the rebuild has reported in. Telling it now to let go keeps it
        // from re-recording this owner's environment beside the new one.
        let fences = self.residency_fences_held_on_silent(holder, lost);
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|_, route| route.holder != *holder);
        }) {
            return Err(residency_core_failure(
                ResidencyFailureKind::Persistence,
                format!("release the lost execution environment route: {error}"),
                None,
            ));
        }
        for (executor_id, generation, fence) in fences {
            self.release_residency_on(&executor_id, generation, fence);
        }
        for (route, quiet) in lost {
            let reason = match quiet.cause {
                UnansweredCause::Disconnected => format!(
                    "{}'s execution environment rebuilt elsewhere: disconnected {}",
                    describe_holder(holder),
                    describe_quiet_ms(now.saturating_sub(quiet.since_unix_ms))
                ),
                UnansweredCause::Silent { generation } => format!(
                    "{}'s execution environment rebuilt elsewhere: silent {} on generation {generation}",
                    describe_holder(holder),
                    describe_quiet_ms(now.saturating_sub(quiet.since_unix_ms))
                ),
            };
            self.record_remote_connection_transition(
                &route.executor_id,
                RemoteConnectionTransition {
                    occurred_at_unix_ms: now,
                    phase:
                        cairn_common::executor_protocol::RemoteConnectionPhase::EnvironmentEvicted,
                    generation: match quiet.cause {
                        UnansweredCause::Silent { generation } => Some(generation),
                        UnansweredCause::Disconnected => None,
                    },
                    reason: Some(reason),
                    remote_process_exit_status: None,
                    last_stderr: None,
                    memory_at_generation_end: None,
                },
            );
        }
        tracing::warn!(
            %holder,
            %executor,
            ?cause,
            "execution environment's machine stopped answering past the lease grace; rebuilding it through placement"
        );
        self.environment_evictions.lock().unwrap().insert(
            holder.storage_key(),
            EnvironmentEviction {
                executor,
                cause,
                lost_processes,
            },
        );
        Ok(())
    }

    /// Resolve the project's setup from canonical configuration now, updating
    /// the cached requirements and forgetting refusals recorded under a setup
    /// it no longer runs. If it cannot be resolved, the cache is dropped, so
    /// no home is evicted on a setup this runner cannot confirm.
    pub(super) async fn refresh_project_setup(
        &self,
        orch: &Orchestrator,
        acquisition: &ResidencyAcquireRequest,
    ) {
        let mut probe = residency_placement_request(acquisition);
        if !runs_project_setup(&probe.repository) {
            return;
        }
        if self.prepare_execution(orch, &mut probe).await.is_err() {
            self.known_project_requirements
                .lock()
                .unwrap()
                .remove(&probe.project_id);
        }
    }

    /// Why the live machine holding an environment for `project_id` cannot
    /// prepare that project's cells, or `None` if it can.
    ///
    /// The predictive half reads the project's last-known requirements,
    /// resolved by the most recent preparation for it on this runner. A build
    /// prerequisite verdict does not evict: a home runs only pinned batches,
    /// and one that needs rust is refused per batch by placement itself.
    ///
    /// A live checkout is never prepared, so it is never evicted for this, and
    /// a project whose setup this runner has not resolved yet evicts nothing:
    /// eviction costs the agent its uncommitted state, so it needs evidence.
    pub(super) fn home_preparation_rejection(
        &self,
        executor_id: &str,
        repository: &RepositoryLocator,
        project_id: &str,
    ) -> Option<PlacementRejectionReason> {
        if !runs_project_setup(repository) {
            return None;
        }
        let requirements = self
            .known_project_requirements
            .lock()
            .unwrap()
            .get(project_id)
            .cloned()?;
        let connections = self.lock_connections();
        let entry = connections.get(executor_id)?;
        setup_toolchain_deficiency(&entry.advertisement.capabilities, project_id, &requirements)
            .or_else(|| {
                applicable_refusal(entry, project_id, repository, &requirements).map(refused)
            })
    }

    /// Give up an environment whose live machine will not serve its next
    /// batch (it cannot prepare the project, or an operator drained it): forget
    /// the route, release the cell, and record why, so the acquisition falls
    /// through to placement and the agent is told what happened.
    #[allow(clippy::result_large_err)]
    pub(super) fn evict_home(
        &self,
        holder: &ResidencyHolder,
        selected: &SelectedExecutor,
        cause: EvictionCause,
    ) -> Result<(), ResidencyResult> {
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|_, route| route.holder != *holder);
        }) {
            return Err(residency_core_failure(
                ResidencyFailureKind::Persistence,
                format!("release the evicted execution environment route: {error}"),
                None,
            ));
        }
        let lost_processes = self
            .lock_connections()
            .get(&selected.executor_id)
            .map(|entry| live_resident_process_labels(entry, holder))
            .unwrap_or_default();
        let fence = self
            .connections
            .lock()
            .unwrap()
            .get(&selected.executor_id)
            .and_then(|entry| {
                entry.snapshot.cells.iter().find_map(|cell| {
                    cell.residency
                        .as_ref()
                        .filter(|residency| residency.holder == *holder)
                        .map(|residency| ResidencyFence {
                            holder: residency.holder.clone(),
                            incarnation_id: residency.incarnation_id.clone(),
                            cell_epoch: cell.cell_epoch,
                        })
                })
            });
        if let Some(fence) = fence {
            self.release_residency_on(&selected.executor_id, selected.generation, fence);
        }
        let executor = self.executor_name_for_text(&selected.executor_id);
        tracing::warn!(
            %holder,
            %executor,
            ?cause,
            "execution environment's machine will not serve its next batch; rebuilding it through placement"
        );
        self.environment_evictions.lock().unwrap().insert(
            holder.storage_key(),
            EnvironmentEviction {
                executor,
                cause,
                lost_processes,
            },
        );
        Ok(())
    }

    /// Whether an environment sits on a drained machine with nothing live
    /// holding it there, and another machine could take it.
    ///
    /// The same decision a terminal that names another machine makes
    /// ([`residency::decide_resident_placement`]), with the operator as the one
    /// who refused this machine: idle moves, live stays. An environment no
    /// other machine could take stays too, because releasing it would only
    /// rebuild it on this same machine and lose its state for nothing.
    pub(super) fn drained_home_is_idle(
        &self,
        executor_id: &str,
        acquisition: &ResidencyAcquireRequest,
    ) -> bool {
        let idle_on_drain = self
            .lock_connections()
            .get(executor_id)
            .is_some_and(|entry| {
                // Only a hold that refuses resident work moves an idle one off;
                // the self-update hold leaves it for its job's next batch.
                drain_projection::entry_refuses(entry, true)
                    && environment_live_work(entry, &acquisition.holder).is_empty()
            });
        idle_on_drain && self.environment_could_move(acquisition, executor_id)
    }

    /// Whether some machine other than `home` would take this acquisition now,
    /// by THE eligibility relation placement uses.
    ///
    /// The acquisition is judged as placement will judge it after preparation:
    /// holding the project's known setup requirements, and pinned to the
    /// colocated executor when the project populates cells from the live
    /// checkout. Approximate only where preparation has not run on this runner
    /// yet, and then it errs toward moving, which placement still bounds.
    pub(super) fn environment_could_move(
        &self,
        acquisition: &ResidencyAcquireRequest,
        home: &str,
    ) -> bool {
        if !repository_is_transferable(&acquisition.repository) {
            return false;
        }
        let mut request = residency_placement_request(acquisition);
        if runs_project_setup(&request.repository) {
            if let Some(known) = self
                .known_project_requirements
                .lock()
                .unwrap()
                .get(&request.project_id)
            {
                *request.project_requirements = known.clone();
            }
        }
        if !matches!(request.repository, RepositoryLocator::ScratchOnly { .. })
            && self
                .colocated_population_projects
                .lock()
                .unwrap()
                .contains(&request.project_id)
        {
            request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        }
        let now = unix_time_ms();
        self.lock_connections().iter().any(|(executor_id, entry)| {
            executor_id != home
                && candidate_rejection(entry, PlacementScope::of(&request), now).is_none()
        })
    }

    /// The drained machine an owner's environment stays on, and what keeps it
    /// there, or `None` when its home is not drained. A batch that runs there
    /// says so rather than leaving `Draining: yes` to contradict where it ran.
    pub(crate) fn drained_home_holding(&self, holder: &ResidencyHolder) -> Option<DrainedHome> {
        let executor_id = self.live_residency_route_executor(holder)?;
        let connections = self.lock_connections();
        let entry = connections
            .get(&executor_id)
            .filter(|entry| entry.drained)?;
        Some(DrainedHome {
            machine: executor_public_name(entry),
            held_by: environment_live_work(entry, holder),
        })
    }

    /// What a drain on this machine keeps, from its last snapshot. `None` for a
    /// machine that is not attached, which reports nothing to count.
    pub fn drain_residents(&self, executor_id: &str) -> Option<DrainResidents> {
        self.lock_connections()
            .get(executor_id)
            .map(|entry| DrainResidents::of(&entry.snapshot))
    }

    /// Remember whether a project populates its cells from the live checkout.
    /// Out of line for the reason [`Self::stamp_project_requirements`] is.
    #[inline(never)]
    pub(super) fn note_colocated_population(&self, project_id: &str, populates: bool) {
        let mut projects = self.colocated_population_projects.lock().unwrap();
        if populates {
            projects.insert(project_id.to_string());
        } else {
            projects.remove(project_id);
        }
    }

    /// The live processes an owner's environment holds on machines that are
    /// still attached but silent, as their last snapshots report them. A machine
    /// that disconnected took its report with it, so it contributes nothing.
    pub(super) fn resident_processes_held_for(
        &self,
        holder: &ResidencyHolder,
        lost: &[(ResidencyRoute, Unanswered)],
    ) -> Vec<String> {
        let connections = self.lock_connections();
        let mut processes = lost
            .iter()
            .filter_map(|(route, _)| connections.get(&route.executor_id))
            .flat_map(|entry| live_resident_process_labels(entry, holder))
            .collect::<Vec<_>>();
        processes.sort();
        processes.dedup();
        processes
    }

    /// The cells silent machines still hold for an owner, with the generation
    /// to address each machine on.
    pub(super) fn residency_fences_held_on_silent(
        &self,
        holder: &ResidencyHolder,
        lost: &[(ResidencyRoute, Unanswered)],
    ) -> Vec<(String, u64, ResidencyFence)> {
        let connections = self.lock_connections();
        lost.iter()
            .filter_map(|(route, quiet)| match quiet.cause {
                UnansweredCause::Silent { generation } => connections
                    .get(&route.executor_id)
                    .filter(|entry| entry.generation == generation)
                    .map(|entry| (route.executor_id.clone(), generation, entry)),
                UnansweredCause::Disconnected => None,
            })
            .flat_map(|(executor_id, generation, entry)| {
                entry.snapshot.cells.iter().filter_map(move |cell| {
                    cell.residency
                        .as_ref()
                        .filter(|residency| residency.holder == *holder)
                        .map(|residency| {
                            (
                                executor_id.clone(),
                                generation,
                                ResidencyFence {
                                    holder: residency.holder.clone(),
                                    incarnation_id: residency.incarnation_id.clone(),
                                    cell_epoch: cell.cell_epoch,
                                },
                            )
                        })
                })
            })
            .collect()
    }

    /// Tell an executor to let go of a cell whose owner now lives (or is about
    /// to live) somewhere else: superseded while its executor was away, or
    /// evicted because its machine cannot prepare its project.
    ///
    /// Deliberately fire-and-forget: the runner has no waiter for the response,
    /// and what matters is that the leftover stops answering for this owner,
    /// which is the executor's to reach. A lost message costs nothing that the
    /// cell's own death policy does not eventually reclaim.
    pub(super) fn release_residency_on(
        &self,
        executor_id: &str,
        generation: u64,
        fence: ResidencyFence,
    ) {
        tracing::warn!(
            executor = %executor_id,
            holder = %fence.holder,
            "releasing an execution environment whose owner now lives elsewhere"
        );
        let _ = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::ResidencyRequest {
                granted_entry_id: None,
                correlation_id: uuid::Uuid::new_v4().to_string(),
                config: ResidencyRuntimeConfig::NotRequired,
                operation: ResidencyOperation::Release { fence },
            },
        );
    }

    /// Forget every route for one owner except the one a live executor answers
    /// for.
    pub(super) fn forget_superseded_routes(
        &self,
        holder: &ResidencyHolder,
        live_executor_id: &str,
    ) {
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|_, route| {
                route.holder != *holder || route.executor_id == live_executor_id
            });
        }) {
            tracing::error!(%error, "persist superseded residency route removal failed");
        }
    }
}

/// Stable request identity for an acquisition; the runner queue mints a distinct
/// entry id for each fresh attempt at this holder.
pub(crate) fn residency_queue_entry_id(holder: &ResidencyHolder) -> String {
    format!("residency-acquire:{}", holder.storage_key())
}

/// The holder an acquisition request places, read back from the request
/// [`residency_placement_request`] builds: its attempt marks an acquisition
/// and its command is the holder's storage key.
/// A placement request as the fresh-pipe floor classifies it. An acquisition
/// rides placement as a request too, and is a residency, never a command.
pub(super) fn pipe_floor_work(request: &CellRequest) -> PipeFloorWork {
    let kind = if request.attempt_id == RESIDENCY_ACQUIRE_ATTEMPT_ID {
        CellAdmissionKind::Residency
    } else {
        CellAdmissionKind::Command
    };
    PipeFloorWork::of(request.priority, kind)
}

pub(super) fn acquired_residency_holder(request: &CellRequest) -> Option<ResidencyHolder> {
    (request.attempt_id == RESIDENCY_ACQUIRE_ATTEMPT_ID)
        .then(|| ResidencyHolder::parse_storage_key(&request.command))
        .flatten()
}

#[cfg(test)]
pub(crate) fn residency_placement_request_for_test(
    request: &ResidencyAcquireRequest,
) -> CellRequest {
    residency_placement_request(request)
}

/// The queue entry an acquisition creates, named deterministically so the runner
/// can report that it is still waiting for this exact entry.
pub(super) fn residency_placement_request(acquisition: &ResidencyAcquireRequest) -> CellRequest {
    CellRequest {
        request_id: residency_queue_entry_id(&acquisition.holder),
        attempt_id: RESIDENCY_ACQUIRE_ATTEMPT_ID.into(),
        project_id: acquisition.repository.project_id().to_string(),
        repository: if matches!(acquisition.holder, ResidencyHolder::DevInstance { .. }) {
            acquisition.repository.clone()
        } else {
            match &acquisition.repository {
                RepositoryLocator::ColocatedPath { .. } => {
                    let identity = acquisition.repository.identity();
                    RepositoryLocator::ManagedObjects {
                        project_id: identity.project_id,
                        repository_id: identity.repository_id,
                        object_format: identity.object_format,
                    }
                }
                repository => repository.clone(),
            }
        },
        base_commit: acquisition.initial_base_commit.clone(),
        command: acquisition.holder.storage_key(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: match acquisition.holder {
            ResidencyHolder::Service { .. } => PlacementWorkClass::Services,
            ResidencyHolder::DevInstance { .. } => PlacementWorkClass::DevInstances,
            ResidencyHolder::Job { .. }
            | ResidencyHolder::ProjectTerminals { .. }
            | ResidencyHolder::Workflow { .. } => PlacementWorkClass::AgentSessions,
        },
        owner: acquisition.owner_ref.clone().or_else(|| {
            Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: acquisition.repository.project_id().to_string(),
                project_key: None,
                issue_number: None,
                job_id: None,
                execution_seq: None,
                node_kind: None,
            })
        }),
        cwd: String::new(),
        env: Vec::new(),
        priority: acquisition.priority,
        wait_horizon_unix_ms: acquisition.wait_horizon_unix_ms,
        waiting_since_unix_ms: acquisition.waiting_since_unix_ms,
        timeout_ms: 0,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: Some(format!("residency:{}", acquisition.holder.storage_key())),
        executor: acquisition.executor.clone(),
        // A dev instance serves the operator's own machine: its ports, its
        // browser, its localhost. It has to run where they are.
        pinned_executor_id: matches!(acquisition.holder, ResidencyHolder::DevInstance { .. })
            .then(|| COLOCATED_EXECUTOR_ID.to_string()),
        // A residency stays on the executor that acquires it, but an unpinned
        // acquisition may choose any compatible live executor. Dev instances are
        // pinned above because their localhost surface is inherently colocated.
        placement_mobility: if matches!(acquisition.holder, ResidencyHolder::DevInstance { .. }) {
            PlacementMobility::PinnedOrColocated
        } else {
            PlacementMobility::SpillEligible
        },
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: acquisition.footprint.reservation(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    }
}

// Refresh transfers use a commit-specific attempt so concurrent requests cannot revoke each other.
pub(super) fn residency_refresh_request(
    residency: &CellResidency,
    fence: &ResidencyFence,
    base_commit: &str,
) -> CellRequest {
    let mut request = residency_placement_request(&ResidencyAcquireRequest {
        holder: residency.holder.clone(),
        repository: residency.repository.clone(),
        executor: None,
        owner_ref: residency.owner_ref.clone(),
        selector: residency.selector.clone(),
        initial_base_commit: residency.current_base_commit.clone(),
        footprint: residency.footprint,
        death_policy: residency.death_policy.clone(),
        priority: CellPriority::AgentInteractive,
        // A refresh is an operation on an environment that already exists, not a
        // wait for one to be created: it never enters admission, so this bounds
        // only the object-plane authorization it derives.
        wait_horizon_unix_ms: unix_time_ms().saturating_add(30_000),
        waiting_since_unix_ms: unix_time_ms(),
    });
    request.request_id = format!("residency-refresh:{}", residency.holder.storage_key());
    request.attempt_id = format!(
        "{}:{}:{}",
        fence.incarnation_id, fence.cell_epoch, base_commit
    );
    request.base_commit = base_commit.to_string();
    request
}

pub(super) fn residency_operation_holder(
    operation: &ResidencyOperation,
) -> Option<&ResidencyHolder> {
    match operation {
        ResidencyOperation::Acquire { .. } => None,
        ResidencyOperation::Reclaim { fence }
        | ResidencyOperation::Renew { fence }
        | ResidencyOperation::Release { fence }
        | ResidencyOperation::StartProcess { fence, .. }
        | ResidencyOperation::StopProcess { fence, .. }
        | ResidencyOperation::WriteProcessInput { fence, .. }
        | ResidencyOperation::ResizePty { fence, .. }
        | ResidencyOperation::MaterializeConflict { fence, .. }
        | ResidencyOperation::MaterializeAttachment { fence, .. }
        | ResidencyOperation::RefreshCheckout { fence, .. } => Some(&fence.holder),
    }
}

pub(super) fn residency_core_failure(
    kind: ResidencyFailureKind,
    diagnostic: impl Into<String>,
    outcome: Option<CellOutcome>,
) -> ResidencyResult {
    ResidencyResult::Failed {
        kind,
        diagnostic: diagnostic.into(),
        cell_outcome: outcome.map(Box::new),
    }
}
