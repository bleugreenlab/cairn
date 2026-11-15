use super::*;

pub(super) fn queued_placement_evidence(decision: &PlacementDecision) -> QueuedPlacementEvidence {
    let selected = match &decision.outcome {
        PlacementOutcome::Selected(selection) => Some(QueuedPlacementTarget {
            executor_name: selection.executor_name.clone(),
            executor_id: selection.executor_id.clone(),
            available_memory_bytes: selection
                .readings
                .memory
                .value()
                .map(|value| value.available_bytes),
            free_volume_bytes: selection
                .readings
                .volume
                .value()
                .map(|value| value.free_bytes),
            reservation: selection.reservation.clone(),
        }),
        PlacementOutcome::Refused { .. } | PlacementOutcome::Waiting { .. } => None,
    };
    QueuedPlacementEvidence {
        decided_at_unix_ms: decision.decided_at_unix_ms,
        mobility: decision.mobility,
        pinned_executor_id: decision.pinned_executor_id.clone(),
        selected,
        rejected: decision
            .rejected
            .iter()
            .map(|rejection| QueuedPlacementRejection {
                drain_blockers: rejection.drain_blockers.clone(),
                executor_name: rejection.executor_name.clone(),
                executor_id: rejection.executor_id.clone(),
                reason: rejection.reason.clone(),
            })
            .collect(),
    }
}

#[cfg(test)]
pub(super) fn placement_prediction(
    entry: &ExecutorConnectionState,
    warmth: CacheWarmthEvidence,
    queue: QueueForecast,
    run: DurationEstimate,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    placement_prediction_with_contention(
        entry,
        warmth,
        queue,
        run,
        resource_profiles::contention_prior(&candidate_load(entry)),
        sync_cost,
    )
}

pub(super) fn candidate_load(entry: &ExecutorConnectionState) -> ExecutionLoadContext {
    load_context(&entry.snapshot, &entry.health.host)
}

/// What a candidate would begin the work under, in the shape the learner
/// reads: the co-resident work its contention counted and the host stall its
/// prediction priced, including a reading too old to price.
pub(super) fn forecast_start(prediction: &PlacementPrediction) -> ExecutionLoadContext {
    use cairn_common::executor_protocol::HostStallPrice;
    let share = |price: &Option<HostStallPrice>| match price {
        Some(HostStallPrice::Priced { full_millis, .. }) => Some(*full_millis),
        _ => None,
    };
    let stale =
        |price: &Option<HostStallPrice>| matches!(price, Some(HostStallPrice::Stale { .. }));
    ExecutionLoadContext {
        co_resident_compile_jobs: prediction.contention.co_resident_compile_jobs,
        co_resident_light_jobs: prediction.contention.co_resident_light_jobs,
        cpu_utilization_millis: None,
        io_pressure_full_millis: share(&prediction.io_pressure),
        memory_pressure_full_millis: share(&prediction.memory_pressure),
        host_stall_stale: stale(&prediction.io_pressure) || stale(&prediction.memory_pressure),
    }
}

pub(super) fn missing_wait_reason_is_new(
    previous: &FleetSnapshot,
    queued: &QueuedCellRequest,
) -> bool {
    !previous.queued_requests.iter().any(|prior| {
        prior.request_id == queued.request_id
            && prior.attempt_id == queued.attempt_id
            && prior.substrate_hold.is_none()
    })
}

pub(super) fn deadline_evidence(
    now_unix_ms: u64,
    authoritative_last_progress_unix_ms: u64,
    evidence: ExecutorSubstrateEvidence,
) -> ExecutorSubstrateEvidence {
    if now_unix_ms.saturating_sub(authoritative_last_progress_unix_ms)
        <= EXECUTOR_PROGRESS_FRESHNESS_MS
    {
        evidence
    } else {
        ExecutorSubstrateEvidence {
            state: ExecutorSubstrateState::ConnectedStalled,
            since_unix_ms: authoritative_last_progress_unix_ms,
            last_progress_unix_ms: authoritative_last_progress_unix_ms,
            ..evidence
        }
    }
}

#[derive(Debug)]
pub(super) struct SelectedExecutor {
    pub(super) executor_id: String,
    pub(super) device_id: String,
    pub(super) generation: u64,
    pub(super) sender: mpsc::UnboundedSender<ExecutorMessage>,
    pub(super) colocated: bool,
    pub(super) capabilities: ExecutorCapabilities,
}

/// A machine chosen for one request, with the demand resolved for it and the
/// complete account of how it was chosen.
#[derive(Debug)]
pub(super) struct Placement {
    pub(super) selected: SelectedExecutor,
    pub(super) reservation: Option<resource_profiles::ResolvedResourceProfile>,
    pub(super) decision: PlacementDecision,
    /// Set when this work was placed on a machine that can never hold its
    /// measured memory, because no usable machine could.
    pub(super) above_ceiling: Option<AboveMemoryCeiling>,
}

/// Work placed on a machine its measured memory demand exceeds, at the clamp.
///
/// The scheduling rule is that a measurement prices placement and does not
/// gate work, so when nothing usable can hold a batch it still runs. What it
/// may not do is run there silently: that is the case that ends in the kernel's
/// out-of-memory killer, and an operator or agent reading the result must be
/// able to see the work was oversized for the fleet rather than conclude the
/// machine is flaky.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AboveMemoryCeiling {
    pub(crate) executor_name: String,
    pub(crate) ceiling: cairn_common::executor_protocol::MemoryCeiling,
    /// The largest ceiling among the machines placement evaluated, by name.
    pub(crate) largest: Option<(String, u64)>,
    /// Whether some evaluated machine is known to hold it but was not usable
    /// (measured-blind, say), which makes "no executor can hold" untrue.
    pub(crate) holdable_elsewhere: bool,
    /// Whether some evaluated machine has no measured fit either way, which
    /// makes "no executor can hold" a claim nothing supports.
    pub(crate) unknown_elsewhere: bool,
}

impl AboveMemoryCeiling {
    /// The sentence a run result carries.
    pub(crate) fn note(&self) -> String {
        use cairn_common::executor_protocol::describe_gib;
        let demand = describe_gib(self.ceiling.estimate_bytes);
        let ceiling = describe_gib(self.ceiling.ceiling_bytes);
        if self.holdable_elsewhere {
            return format!(
                "Note: this batch's measured memory (~{demand}) exceeds what **{}** can ever hold ({ceiling}), and no machine that can hold it was usable, so it was placed there at the clamp.",
                self.executor_name,
            );
        }
        if self.unknown_elsewhere {
            return format!(
                "Note: this batch's measured memory (~{demand}) exceeds what **{}** can ever hold ({ceiling}), and no usable machine is known to hold it, so it was placed there at the clamp.",
                self.executor_name,
            );
        }
        let largest = self
            .largest
            .as_ref()
            .map(|(name, bytes)| format!(" (largest ceiling {} on {name})", describe_gib(*bytes)))
            .unwrap_or_default();
        format!(
            "Note: no executor can hold this batch's measured ~{demand}{largest}; it was placed on **{}** at the clamp.",
            self.executor_name
        )
    }
}

/// What one placement pass concluded about a request that was not refused.
#[derive(Debug)]
pub(super) enum Selection {
    Placed(Box<Placement>),
    /// Nothing can take it this instant. `rejected` is the pass's account of
    /// every machine, which is what the request is owed if its horizon arrives
    /// before anything changes. A [`PlacementRejectionReason::Declined`] row
    /// in it means a capable machine is attached and has said "not yet", so
    /// the request is parked on that machine's next posted state.
    Waiting {
        rejected: Vec<PlacementRejection>,
    },
    /// This pass was waiting for an explicit release acknowledgement. Kept in
    /// the result because an acknowledgement may clear live queue state before
    /// the outer placement loop classifies this pass's wait.
    ReleasingHeadroom {
        rejected: Vec<PlacementRejection>,
    },
}

impl Selection {
    #[cfg(test)]
    pub(super) fn placed(self) -> Option<Placement> {
        match self {
            Self::Placed(placement) => Some(*placement),
            Self::Waiting { .. } | Self::ReleasingHeadroom { .. } => None,
        }
    }
}

/// A request that could be placed nowhere, carrying the same candidate
/// evaluation a successful placement would have.
#[derive(Debug)]
pub(super) struct RefusedPlacement {
    pub(super) decision: PlacementDecision,
    pub(super) diagnostic: String,
    pub(super) reason: CellUnavailableReason,
}

#[derive(Debug)]
pub(super) struct PlacementFailure {
    pub(super) diagnostic: String,
    pub(super) rejected: Vec<PlacementRejection>,
}

impl std::fmt::Display for PlacementFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.diagnostic.fmt(f)
    }
}

impl std::ops::Deref for PlacementFailure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.diagnostic
    }
}

impl From<PlacementFailure> for String {
    fn from(failure: PlacementFailure) -> Self {
        failure.diagnostic
    }
}

pub(super) fn refused_placement(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: &ActivePlacementPolicy,
    unpinned_winner: Option<PlacementCounterfactualWinner>,
    failure: PlacementFailure,
) -> RefusedPlacement {
    let PlacementFailure {
        diagnostic,
        rejected,
    } = failure;
    RefusedPlacement {
        decision: placement_decision(
            request,
            decided_at_unix_ms,
            Some(policy_evidence(request, policy, false)),
            unpinned_winner,
            PlacementOutcome::Refused {
                diagnostic: diagnostic.clone(),
            },
            rejected.clone(),
        ),
        diagnostic,
        reason: CellUnavailableReason::NoMatchingExecutor {
            rejected: rejected.iter().map(Into::into).collect(),
        },
    }
}

/// A pinned request whose home can never hold its measured memory, while the
/// machine placement would choose with the pin lifted can.
///
/// Refused at once rather than queued on the home: the home is healthy for
/// everything it can hold, but this work would only run it into its memory
/// limit, and waiting there waits for a machine to grow. The refusal names the
/// machine that can hold it, so the caller can run it there from the branch
/// head and leave the environment where it is. When no other usable machine can
/// hold it either, this answers `None` and the pin holds at the clamp.
pub(super) fn home_cannot_hold(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: &ActivePlacementPolicy,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    survey: &CandidateSurvey<'_>,
    unpinned_winner: Option<&PlacementCounterfactualWinner>,
) -> Option<RefusedPlacement> {
    let home_id = request.pinned_executor_id.as_deref()?;
    let home = survey
        .usable
        .iter()
        .find(|entry| entry.identity.executor_id == home_id)?;
    let ceiling = reservations.get(home_id)?.memory_fit.ceiling()?;
    let winner = unpinned_winner.filter(|winner| winner.executor_id != home_id)?;
    // The machine named must be known to hold the work, by a measured demand
    // under its own cap. A missing measurement is not that, and naming an
    // unmeasured machine as the safe place would pin the batch somewhere
    // possibly smaller than the home it left.
    if reservations
        .get(&winner.executor_id)
        .is_none_or(|resolved| resolved.memory_fit != resource_profiles::MemoryFit::Holds)
    {
        return None;
    }
    let home_name = executor_public_name(home);
    let diagnostic = format!(
        "{home_name}, which holds this job's execution environment, cannot run this batch: {}; {} can",
        ceiling.describe(),
        winner.executor_name
    );
    let mut rejected = vec![PlacementRejection {
        unreadable_memory: Vec::new(),
        drain_blockers: Vec::new(),
        prediction: None,
        executor_name: home_name.clone(),
        executor_id: home_id.to_string(),
        reason: PlacementRejectionReason::ExceedsMemoryCeiling {
            estimate_bytes: ceiling.estimate_bytes,
            ceiling_bytes: ceiling.ceiling_bytes,
            killed_here: ceiling.killed_here,
        },
    }];
    rejected.extend(survey.rejected.iter().cloned());
    Some(RefusedPlacement {
        decision: placement_decision(
            request,
            decided_at_unix_ms,
            Some(policy_evidence(request, policy, false)),
            Some(winner.clone()),
            PlacementOutcome::Refused {
                diagnostic: diagnostic.clone(),
            },
            rejected,
        ),
        diagnostic,
        reason: CellUnavailableReason::HomeCannotHold {
            home: home_name,
            ceiling,
            alternative_executor_id: winner.executor_id.clone(),
            alternative_executor_name: winner.executor_name.clone(),
        },
    })
}

/// Work about to run above its machine's memory ceiling: say so on every
/// machine that could not hold it either, and compose the note its result
/// carries. A pinned decision lists the other machines as pin mismatches, which
/// is true and beside the point: the useful fact is that none of them could
/// have held it, which is why the pin held.
pub(super) fn above_memory_ceiling(
    selection: &PlacementSelection,
    rejected: &mut [PlacementRejection],
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    connections: &HashMap<String, ExecutorConnectionState>,
) -> Option<AboveMemoryCeiling> {
    let ceiling = selection.memory_ceiling?;
    for rejection in rejected.iter_mut() {
        if let Some(other) = reservations
            .get(&rejection.executor_id)
            .and_then(|resolved| resolved.memory_fit.ceiling())
        {
            rejection.reason = PlacementRejectionReason::ExceedsMemoryCeiling {
                estimate_bytes: other.estimate_bytes,
                ceiling_bytes: other.ceiling_bytes,
                killed_here: other.killed_here,
            };
        }
    }
    let largest = reservations
        .iter()
        .filter_map(|(executor_id, resolved)| {
            resolved
                .memory_fit
                .ceiling()
                .map(|other| (executor_id, other.ceiling_bytes))
        })
        .max_by(|(left_id, left), (right_id, right)| {
            left.cmp(right).then_with(|| right_id.cmp(left_id))
        })
        .map(|(executor_id, bytes)| {
            let name = connections
                .get(executor_id)
                .map(executor_public_name)
                .unwrap_or_else(|| executor_id.clone());
            (name, bytes)
        });
    Some(AboveMemoryCeiling {
        executor_name: selection.executor_name.clone(),
        ceiling,
        largest,
        holdable_elsewhere: reservations
            .values()
            .any(|resolved| resolved.memory_fit == resource_profiles::MemoryFit::Holds),
        unknown_elsewhere: reservations
            .values()
            .any(|resolved| resolved.memory_fit == resource_profiles::MemoryFit::Unknown),
    })
}

pub(super) fn placement_decision(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: Option<PlacementPolicyEvidence>,
    unpinned_winner: Option<PlacementCounterfactualWinner>,
    outcome: PlacementOutcome,
    rejected: Vec<PlacementRejection>,
) -> PlacementDecision {
    PlacementDecision {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        decided_at_unix_ms,
        mobility: request.placement_mobility,
        selector: request
            .executor
            .as_ref()
            .filter(|selector| !selector.is_empty())
            .cloned(),
        pinned_executor_id: request.pinned_executor_id.clone(),
        unpinned_winner,
        binding: Vec::new(),
        binding_elided: 0,
        policy,
        outcome,
        rejected,
    }
}

/// What makes two placement decisions for one queue entry materially the same,
/// as one string: the outcome's kind, the machine it chose, and each machine
/// passed over with its reason's signature (see
/// [`placement_rejection_signature`]) and the drains holding it. `extra`
/// carries material facts the decision does not, such as a wait's room line.
///
/// Forecasts, readings and prose are left out: they change on every pass
/// without the situation changing. So is the binding chain: each binding step
/// is journaled as its own state row.
pub(super) fn decision_signature(
    decision: &PlacementDecision,
    extra: &[(String, String)],
) -> String {
    let outcome = match &decision.outcome {
        PlacementOutcome::Selected(selection) => format!("selected:{}", selection.executor_id),
        PlacementOutcome::Waiting { .. } => "waiting".to_string(),
        PlacementOutcome::Refused { .. } => "refused".to_string(),
    };
    let mut parts: Vec<(String, String)> = decision
        .rejected
        .iter()
        .map(|rejection| {
            let blockers: Vec<(&str, &str)> = rejection
                .drain_blockers
                .iter()
                .map(|blocker| (blocker.owner.as_str(), blocker.reason.as_str()))
                .collect();
            (
                rejection.executor_id.clone(),
                format!(
                    "{}:{blockers:?}",
                    placement_rejection_signature(&rejection.reason)
                ),
            )
        })
        .chain(extra.iter().cloned())
        .collect();
    parts.sort();
    format!("{outcome}|{parts:?}")
}

/// How many placement decisions the runner keeps. Bounded because this is a
/// window onto what the fleet is doing now, not an audit log.
pub(super) const RECENT_PLACEMENT_DECISIONS: usize = 32;

impl Fleet {
    /// What Cairn's own running work says about when the machines a batch could
    /// use will have room.
    ///
    /// Read per machine and then combined, rather than from the aggregate
    /// snapshot, because the aggregate deliberately loses executor attribution:
    /// "something somewhere finishes in ninety seconds" is not an answer to
    /// "when will THIS batch's machine have room".
    ///
    /// Eligibility is [`candidate_rejection`], the same relation placement
    /// itself applies — never a subset of it. A machine that matches the
    /// selector but does not serve the project, or whose link has closed, is one
    /// the work will never reach, and a forecast that read it would hand the
    /// batch a shorter wait than its real blocker needs, surface a refusal while
    /// that blocker was still finite, and print the wrong occupant's name on the
    /// row that went red. That is the failure this policy exists to end, so it
    /// must not be reintroduced by approximating where work can go.
    ///
    /// This reads placement state and takes no reservation. Which request is
    /// admitted, and when, remains the executor's try_admit alone (CAIRN-3268);
    /// a forecast only tells a caller how long its own patience is worth.
    pub(crate) fn occupancy_for(&self, scope: PlacementScope<'_>) -> occupancy::MachineOccupancy {
        self.occupancy_for_at(scope, unix_time_ms())
    }

    /// Every machine this fleet knows of, attached or enrolled, with the
    /// platform it advertises (or was enrolled as) and, once attached, its
    /// toolchains. A selector names an operating system but not an
    /// architecture, so work that needs one (a release build for a target
    /// triple) reads this to know which machines exist for it and steers the
    /// rest away through the request's avoid list.
    pub(crate) fn known_machine_platforms(&self) -> Vec<KnownMachinePlatform> {
        // Read before the connection lock is taken, as `unplaceable_reason` does.
        let mut machines: Vec<KnownMachinePlatform> = self
            .unattached_enrolled_remotes()
            .into_iter()
            .map(|machine| KnownMachinePlatform {
                role: self.machine_role(&machine.executor_id),
                executor_id: machine.executor_id,
                name: machine.name,
                os: machine.os.to_ascii_lowercase(),
                arch: machine.arch.to_ascii_lowercase(),
                toolchains: None,
                publish_isolation: None,
            })
            .collect();
        let connections = self.lock_connections();
        for (executor_id, entry) in connections.iter() {
            let capabilities = &entry.advertisement.capabilities;
            machines.push(KnownMachinePlatform {
                executor_id: executor_id.clone(),
                name: executor_public_name(entry),
                os: capabilities.os.to_ascii_lowercase(),
                arch: capabilities.arch.to_ascii_lowercase(),
                toolchains: Some(capabilities.toolchains.clone()),
                role: entry.machine_role,
                publish_isolation: capabilities.publish_isolation.clone(),
            });
        }
        machines
    }

    /// Why no machine this fleet knows of could ever take work that asks for
    /// `selector` and counts its verdict from `verdict_platforms`, or `None`
    /// while one could.
    ///
    /// "Knows of" is deliberately wider than "could place right now", which is
    /// [`candidate_rejection`]'s question. An attached machine counts when what
    /// it advertises matches. An enrolled machine that is not attached counts
    /// whenever its platform matches, because what it will advertise is unknown
    /// until it attaches: a detached machine is something to wait for, never
    /// proof of absence. The runner's own colocated executor counts on the
    /// runner's platform until it first attaches, so the attach race after a
    /// restart cannot read as a gap in the fleet. Everything that comes and goes
    /// while the fleet's makeup stays the same (staleness, drains, declines,
    /// load, preparation refusals) is placement's business, not this.
    ///
    /// Only work that names where it must run can be unplaceable: untargeted
    /// work runs anywhere, home included.
    pub(crate) fn unplaceable_reason(
        &self,
        selector: Option<&ExecutorSelector>,
        verdict_platforms: &[String],
    ) -> Option<String> {
        let selector = selector.filter(|selector| !selector.is_empty());
        if selector.is_none() && verdict_platforms.is_empty() {
            return None;
        }
        let admits_os = |os: &str| {
            selector
                .and_then(|selector| selector.os.as_deref())
                .is_none_or(|wanted| wanted.eq_ignore_ascii_case(os))
                && (verdict_platforms.is_empty()
                    || verdict_platforms
                        .iter()
                        .any(|platform| platform.eq_ignore_ascii_case(os)))
        };
        let admits_name = |name: &str| {
            selector
                .and_then(|selector| selector.name.as_deref())
                .is_none_or(|wanted| executor_names_match(wanted, name))
        };
        // Read before the connection lock is taken: it takes that lock and the
        // enrollment lock one at a time, and must not nest inside either.
        let unattached = self.unattached_enrolled_remotes();
        if unattached
            .iter()
            .any(|machine| admits_name(&machine.name) && admits_os(&machine.os))
        {
            return None;
        }
        let connections = self.lock_connections();
        if connections.values().any(|entry| {
            selector.is_none_or(|selector| matches_selector(entry, selector))
                && admits_os(&entry.advertisement.capabilities.os)
        }) {
            return None;
        }
        if !connections.values().any(|entry| entry.colocated)
            && admits_name(LOCAL_EXECUTOR_NAME)
            && admits_os(std::env::consts::OS)
        {
            return None;
        }
        let wanted = match (selector, verdict_platforms) {
            (Some(selector), []) => selector.describe(),
            (Some(selector), platforms) => {
                format!("{} on {}", selector.describe(), platforms.join(" or "))
            }
            (None, platforms) => format!("work on {}", platforms.join(" or ")),
        };
        // A containment or page-call requirement is invisible in a machine's
        // OS and toolchains, so each machine then also says which edge or
        // program it failed: the line an operator needs to know which machine
        // to fix.
        let measured_requirement = selector.filter(|selector| {
            selector.containment.is_some() || !selector.page_call_programs.is_empty()
        });
        let mut known: Vec<String> = connections
            .values()
            .map(|entry| {
                let capabilities = &entry.advertisement.capabilities;
                let mut toolchains = capabilities.toolchains.clone();
                toolchains.sort();
                let shortfall = measured_requirement
                    .and_then(|selector| selector_shortfall(entry, selector))
                    .map(|shortfall| format!("; {}", shortfall.describe()))
                    .unwrap_or_default();
                format!(
                    "{} ({}; {}{shortfall})",
                    executor_public_name(entry),
                    capabilities.os,
                    if toolchains.is_empty() {
                        "no toolchains".to_string()
                    } else {
                        toolchains.join(", ")
                    }
                )
            })
            .chain(
                unattached
                    .iter()
                    .map(|machine| format!("{} ({}; not attached)", machine.name, machine.os)),
            )
            .collect();
        known.sort();
        let known = if known.is_empty() {
            "none".to_string()
        } else {
            known.join(", ")
        };
        Some(format!(
            "No machine enrolled in this fleet, attached or not, satisfies {wanted}. Known machines: {known}."
        ))
    }

    /// A stable identity for the capability set this scope can reach right now.
    ///
    /// Routed through [`candidate_rejection`], the same eligibility relation
    /// placement uses, so "which machines could take this?" here and at
    /// placement cannot drift. Approximating it with a subset of that function's
    /// clauses is the bug its own doc warns about.
    ///
    /// What this deliberately EXCLUDES is everything that moves while the
    /// fleet's abilities stay the same: current load, connection generation,
    /// lease epoch, last-seen instants, warm roots, queue depth. That is what
    /// lets a standing structural gap — a Windows-only suite with no Windows
    /// machine — keep one identity across commits. An empty eligible set gets
    /// its own stable identity rather than none, which is what makes such a
    /// check converge, and what makes enrolling a matching machine re-arm it
    /// immediately.
    ///
    /// What it deliberately INCLUDES beyond the capabilities is whether each
    /// eligible machine is measurement-blind ([`placement_blindness`]): a
    /// machine whose readings placement cannot decide on is still eligible, so
    /// without this a counter charged while it was blind would be the same
    /// counter once its telemetry is fresh again, and a lane suppressed during
    /// a blind window would outlive the window. With it, the machine's
    /// recovery is itself the fleet change that re-arms the lane. Each eligible
    /// machine's repair epoch also participates: declaring it repaired re-arms
    /// its fleet-scoped lanes, without refreshing scopes that cannot reach it.
    pub(crate) fn capability_identity(
        &self,
        scope: PlacementScope<'_>,
        repair_epochs: &HashMap<String, i64>,
    ) -> String {
        use sha2::Digest as _;
        let now_unix_ms = unix_time_ms();
        let connections = self.lock_connections();
        let mut rows: Vec<String> = connections
            .values()
            .filter(|entry| candidate_rejection(entry, scope, now_unix_ms).is_none())
            .map(|entry| {
                let capabilities = &entry.advertisement.capabilities;
                let mut toolchains = capabilities.toolchains.clone();
                toolchains.sort();
                format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}",
                    entry.identity.executor_id,
                    repair_epochs
                        .get(&entry.identity.executor_id)
                        .copied()
                        .unwrap_or(0),
                    executor_public_name(entry),
                    capabilities.os,
                    capabilities.arch,
                    toolchains.join(","),
                    capabilities
                        .sandbox
                        .as_ref()
                        .is_some_and(SandboxCapabilityEvidence::check_lane_can_nest),
                    if placement_blindness(&entry.health.machine, now_unix_ms).is_some() {
                        "blind"
                    } else {
                        "seeing"
                    },
                )
            })
            .collect();
        rows.sort();
        format!("{:x}", sha2::Sha256::digest(rows.join("\n").as_bytes()))
    }

    /// The machines among `executor_ids` whose readings placement cannot decide
    /// on right now, each with the reason placement gives and how long it has
    /// been past its bound, sorted by name.
    ///
    /// Read through [`placement_blindness`], the judgement ranking itself
    /// makes, so a check death called measurement-blind is one placement
    /// refused for exactly that. A machine no longer attached is not blind: it
    /// is gone, which is a different population with a different repair.
    pub(crate) fn measurement_blindness(
        &self,
        executor_ids: &[String],
    ) -> Vec<MeasurementBlindness> {
        let now_unix_ms = unix_time_ms();
        let connections = self.lock_connections();
        let mut blind: Vec<MeasurementBlindness> = executor_ids
            .iter()
            .filter_map(|executor_id| {
                let entry = connections.get(executor_id)?;
                let reason = placement_blindness(&entry.health.machine, now_unix_ms)?;
                let blind_for_ms = match &reason {
                    PlacementRejectionReason::TelemetryStale {
                        age_ms,
                        stale_after_ms,
                        ..
                    } => Some(age_ms.saturating_sub(*stale_after_ms)),
                    _ => None,
                };
                Some(MeasurementBlindness {
                    executor_id: executor_id.clone(),
                    name: executor_public_name(entry),
                    reason: reason.describe(),
                    blind_for_ms,
                })
            })
            .collect();
        blind.sort_by(|left, right| left.name.cmp(&right.name));
        blind
    }

    /// `(executor_id, repair identity)` for every machine eligible to run this
    /// work right now, sorted by executor id so an avoid set derived from it is
    /// deterministic.
    ///
    /// Eligibility is [`candidate_rejection`]'s, the same relation placement
    /// uses, so a machine counted here and a machine placement would choose
    /// cannot drift apart.
    pub(crate) fn eligible_environments(
        &self,
        scope: PlacementScope<'_>,
        repair_epochs: &HashMap<String, i64>,
    ) -> Vec<(String, String)> {
        let now_unix_ms = unix_time_ms();
        let connections = self.lock_connections();
        let mut environments: Vec<(String, String)> = connections
            .values()
            .filter(|entry| candidate_rejection(entry, scope, now_unix_ms).is_none())
            .map(|entry| {
                let executor_id = entry.identity.executor_id.clone();
                let epoch = repair_epochs.get(&executor_id).copied().unwrap_or(0);
                let identity = environment_identity(entry, epoch);
                (executor_id, identity)
            })
            .collect();
        environments.sort();
        environments
    }

    /// Keep one placement decision, evicting the oldest past the bound.
    pub(super) fn record_placement_decision(&self, decision: PlacementDecision) {
        let signature = decision_signature(&decision, &[]);
        self.record_signed_placement_decision(decision, signature);
    }

    /// [`Self::record_placement_decision`] under a signature the caller built
    /// with [`decision_signature`], for facts the decision does not carry.
    fn record_signed_placement_decision(&self, decision: PlacementDecision, signature: String) {
        self.queue.record_placement_signed(&decision, signature);
        let now = std::time::Instant::now();
        let mut logged = self.placement_rejection_log.lock().unwrap();
        for rejection in &decision.rejected {
            // The log is a witness to each verdict, not to each pass: a request
            // re-placed every few hundred milliseconds would otherwise restate
            // every machine's rejection every time. A verdict is logged when
            // its kind changes and summarized periodically while it holds,
            // with its typed numbers; the bounded placement record and the
            // queue journal keep every pass.
            let signature = placement_rejection_signature(&rejection.reason);
            let line = match serde_json::to_string(rejection) {
                Ok(rejection) => format!(
                    "placement rejection request_id={} attempt_id={} rejection={}",
                    decision.request_id, decision.attempt_id, rejection
                ),
                Err(error) => format!(
                    "placement rejection request_id={} attempt_id={} serialization_error={}",
                    decision.request_id, decision.attempt_id, error
                ),
            };
            if let Some(line) = logged.record(
                now,
                (decision.request_id.clone(), rejection.executor_id.clone()),
                &signature,
                &line,
            ) {
                log::log!(
                    target: "cairn::fleet::placement",
                    placement_rejection_log_level(rejection),
                    "{line}"
                );
            }
        }
        drop(logged);
        let mut recent = self.recent_placements.lock().unwrap();
        if recent.len() >= RECENT_PLACEMENT_DECISIONS {
            recent.pop_front();
        }
        recent.push_back(decision);
    }

    /// What work waiting on `executor_id`'s self-update is told: the machine,
    /// that it is being updated, and how much of that is left.
    pub(super) fn update_wait_sentence(
        &self,
        executor_id: &str,
        name: &str,
        now_unix_ms: u64,
    ) -> String {
        use cairn_common::executor_protocol::describe_clock;
        let wait = self.self_update.lock().unwrap().update_wait(executor_id);
        let progress = match wait {
            Some(rollout::UpdateWait::Draining {
                holds,
                residents,
                returns_by_unix_ms,
            }) => {
                let state = match (holds.first(), residents.as_slice()) {
                    (Some(hold), residents) => format!(
                        "{} in flight, {}{}, {}{}",
                        holds.len(),
                        if holds.len() > 1 { "the longest " } else { "" },
                        hold.label,
                        crate::resources::executors::hold_remaining(hold, now_unix_ms),
                        match residents {
                            [] => String::new(),
                            residents =>
                                format!("; the restart ends {}", rollout::listed(residents)),
                        }
                    ),
                    (None, []) => "empty, about to install it".to_string(),
                    (None, [one]) => format!("only {one} remains, which the restart ends"),
                    (None, residents) => format!(
                        "only {} remain, which the restart ends",
                        rollout::listed(residents)
                    ),
                };
                let back = match returns_by_unix_ms {
                    Some(at) => format!(", expected back by {}", describe_clock(at)),
                    None => {
                ", with no safe restart time yet while work remains or the machine updating ahead has no bound"
                    .to_string()
            }
                };
                format!(" ({state}){back}")
            }
            Some(rollout::UpdateWait::Directed {
                at_unix_ms,
                returns_by_unix_ms,
            }) => format!(
                " (installing it, directed {} ago), expected back by {}",
                describe_quiet_ms(now_unix_ms.saturating_sub(at_unix_ms)),
                describe_clock(returns_by_unix_ms)
            ),
            None => String::new(),
        };
        format!(
            "{name} being emptied for an executor update{progress}, which ends on its own when it returns on the runner's build (cairn://executors/{name})"
        )
    }

    /// Every placement decision this machine took part in, newest first.
    pub(super) fn placements_naming(&self, executor_id: &str) -> Vec<PlacementDecision> {
        let mut values: Vec<_> = self
            .queue
            .live_placements()
            .into_iter()
            .filter(|decision| decision.mentions_executor(executor_id))
            .collect();
        for decision in self.recent_placements.lock().unwrap().iter().rev() {
            // The live entry duplicates its latest history row, not every
            // earlier outcome for the same attempt (for example wait → refuse).
            if decision.mentions_executor(executor_id) && !values.contains(decision) {
                values.push(decision.clone());
            }
        }
        values.sort_by(|a, b| {
            b.decided_at_unix_ms
                .cmp(&a.decided_at_unix_ms)
                .then_with(|| a.request_id.cmp(&b.request_id))
        });
        values
    }

    /// The public address of an attached executor, by internal identity.
    pub fn executor_public_name(&self, executor_id: &str) -> Option<String> {
        self.lock_connections()
            .get(executor_id)
            .map(executor_public_name)
    }

    /// Cached diagnostic from exactly the local connection the caller inspected.
    /// This does not ask the executor for a sample, nor retire an expired watch.
    pub(crate) fn memory_diagnostic(
        &self,
        expected_generation: u64,
        now_unix_ms: u64,
    ) -> Option<cairn_common::executor_protocol::MemoryDiagnosticEvidence> {
        self.lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .filter(|entry| entry.colocated && entry.generation == expected_generation)
            .and_then(|entry| entry.health.memory_diagnostic.as_ref())
            .filter(|evidence| evidence.expires_at_unix_ms > now_unix_ms)
            .cloned()
    }

    /// Offer this entry to the chosen machine and wait for its answer.
    pub(super) async fn offer_once(
        &self,
        entry_id: &str,
        attempt_request: &CellRequest,
        selected: &SelectedExecutor,
        decision: &mut PlacementDecision,
    ) -> OfferAnswer {
        let executor_name = self
            .executor_public_name(&selected.executor_id)
            .unwrap_or_else(|| selected.executor_id.clone());
        if !self.queue.offer(
            entry_id,
            &selected.executor_id,
            &executor_name,
            selected.generation,
            unix_time_ms(),
        ) {
            // The entry moved underneath this pass.
            return if matches!(
                self.queue.state_of(entry_id),
                Some(queue::EntryState::Queued)
            ) {
                OfferAnswer::Requeued
            } else {
                OfferAnswer::Gone
            };
        }
        let summary = self.queue.with_entry(entry_id, |entry| {
            entry.summary(
                attempt_request,
                attempt_request
                    .learned_estimate
                    .as_ref()
                    .and_then(|estimate| estimate.upper_duration_ms),
            )
        });
        let summary = summary.map(|mut summary| {
            summary.headroom =
                self.queue
                    .headroom_plan(entry_id, unix_time_ms())
                    .and_then(|plan| {
                        plan.target
                            .filter(|target| {
                                target.enabled
                                    && target.executor_id == selected.executor_id
                                    && target.generation == selected.generation
                            })
                            .map(|_| cairn_common::executor_protocol::HeadroomAuthorization {
                                reserve_from_unix_ms: plan.reserve_from_unix_ms,
                                forecast_ms: plan.forecast_ms,
                            })
                    });
            summary
        });
        let reached = summary.is_some_and(|summary| {
            match self.send_new_admission(
                selected,
                attempt_request,
                drain_projection::resident_work(attempt_request, &selected.executor_id),
                ExecutorMessage::Offer {
                    entry_id: entry_id.to_owned(),
                    summary,
                },
            ) {
                Ok(()) => true,
                Err(error) => {
                    if !error.blockers.is_empty() {
                        decision.outcome = PlacementOutcome::Refused {
                            diagnostic: error.diagnostic,
                        };
                        decision.rejected.push(PlacementRejection {
                            unreadable_memory: Vec::new(),
                            executor_name: executor_name.clone(),
                            executor_id: selected.executor_id.clone(),
                            reason: error.reason,
                            prediction: None,
                            drain_blockers: error.blockers,
                        });
                    }
                    false
                }
            }
        });
        if !reached {
            if let PlacementOutcome::Refused { diagnostic } = &decision.outcome {
                self.queue.retract_unsent_offer(
                    entry_id,
                    &selected.executor_id,
                    selected.generation,
                    diagnostic,
                    unix_time_ms(),
                );
                self.record_binding_chain(decision, entry_id);
                self.record_placement_decision(decision.clone());
                return OfferAnswer::Requeued;
            }
            self.queue.decline(
                entry_id,
                &selected.executor_id,
                selected.generation,
                DeclineReason::Unknown,
                Some("the link closed before the offer reached the machine".into()),
                None,
                None,
                unix_time_ms(),
            );
            self.record_binding_chain(decision, entry_id);
            self.record_placement_decision(decision.clone());
            return OfferAnswer::Requeued;
        }
        let answer = self
            .await_offer_answer(entry_id, attempt_request.wait_horizon_unix_ms)
            .await;
        self.record_binding_chain(decision, entry_id);
        self.record_placement_decision(decision.clone());
        answer
    }

    /// Wait for the machine an entry was offered to to answer.
    ///
    /// The answer is a state change on the entry rather than a message this
    /// function reads, because a claim can also be undone by a generation
    /// ending, and a single place has to observe all of it.
    pub(super) async fn await_offer_answer(
        &self,
        entry_id: &str,
        horizon_unix_ms: u64,
    ) -> OfferAnswer {
        loop {
            let notified = self.binding_changed.notified();
            let Some(state) = self.queue.state_of(entry_id) else {
                return OfferAnswer::Gone;
            };
            let now = unix_time_ms();
            match state {
                queue::EntryState::Claimed { .. } => return OfferAnswer::Claimed,
                queue::EntryState::Queued => return OfferAnswer::Requeued,
                queue::EntryState::Offered {
                    executor_id,
                    generation,
                    at_unix_ms,
                } => {
                    if now >= horizon_unix_ms {
                        return OfferAnswer::Expired;
                    }
                    // A machine that has not answered within the link-freshness
                    // budget is not answering this offer. The entry goes back to
                    // the queue rather than waiting out a longer clock, because
                    // another machine can take it now.
                    if now.saturating_sub(at_unix_ms) >= EXECUTOR_PROGRESS_FRESHNESS_MS {
                        self.queue.decline(
                            entry_id,
                            &executor_id,
                            generation,
                            DeclineReason::Unknown,
                            Some("the machine did not answer the offer".into()),
                            None,
                            None,
                            now,
                        );
                        return OfferAnswer::Requeued;
                    }
                    let remaining = horizon_unix_ms.saturating_sub(now);
                    let _ = tokio::time::timeout(
                        Duration::from_millis(remaining.clamp(1, 250)),
                        notified,
                    )
                    .await;
                }
            }
        }
    }

    /// The answer a requester is owed when nothing took its work in time.
    ///
    /// One place, because the same sentence is owed whether the horizon arrived
    /// while nothing could be chosen or while a chosen machine was deciding, and
    /// an operator reading it needs the fleet inventory either way.
    ///
    /// `declined` is every machine that refused this request, as
    /// [`PlacementRejectionReason::Declined`] rows. They are the most useful
    /// sentence in the refusal: the machine was attached and capable, and this
    /// is what it said.
    pub(super) fn wait_horizon_outcome(
        &self,
        request: &CellRequest,
        declined: &[PlacementRejection],
    ) -> CellOutcome {
        let connections = self.lock_connections().clone();
        let unattached = self.unattached_enrolled_remotes();
        let inventory = format!(
            "{} {}",
            known_executor_inventory(&connections),
            absent_enrolled_inventory(&connections, &unattached, request)
        );
        // A machine passed over because it cannot prepare the project is the
        // likeliest reason nothing became usable, and the one an operator can
        // act on, so it is named whatever else the refusal says.
        let surveyed = match survey_candidates(
            &connections,
            &unattached,
            request,
            unix_time_ms(),
            &HashMap::new(),
            &AdmissionView::of_request(request),
        ) {
            Ok(survey) => survey.rejected,
            Err(failure) => failure.rejected,
        };
        let rejected = account_from_journal(surveyed, declined);
        let declines = format!(
            "{}{}{}",
            describe_memory_rejections(declined)
                .map(|memory| format!(" Not placed where it cannot run: {memory}."))
                .unwrap_or_default(),
            describe_declines(declined),
            describe_preparation_rejections(&rejected)
        );
        let targeted = request.pinned_executor_id.is_some()
            || request
                .executor
                .as_ref()
                .is_some_and(|selector| !selector.is_empty());
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::NoMatchingExecutor {
                rejected: rejected.iter().map(Into::into).collect(),
            },
            diagnostic: if targeted {
                format!(
                    "no executor satisfying {} became usable before this request's wait horizon. Known executors: {inventory}.{declines} Read cairn://executors for live state.",
                    request
                        .executor
                        .as_ref()
                        .filter(|selector| !selector.is_empty())
                        .map(|selector| selector.describe())
                        .unwrap_or_else(|| "the machine holding this job's working tree".to_string())
                )
            } else if !request.verdict_platforms.is_empty() {
                // Trust is why nothing here was usable, so trust is what the
                // diagnostic has to name. An operator reading "no executor
                // became usable" beside a fleet of idle machines would have no
                // way to tell that they were passed over deliberately.
                format!(
                    "no executor on {} — the platform(s) this verdict counts from — became usable before this request's wait horizon. Known executors: {inventory}.{declines} Read cairn://executors for live state.",
                    request.verdict_platforms.join(", ")
                )
            } else if !declines.is_empty() {
                format!(
                    "no executor became usable before this request's wait horizon. Known executors: {inventory}.{declines} Read cairn://executors for live state."
                )
            } else {
                "no executor became usable before this request's wait horizon".to_string()
            },
        }
    }

    /// Answer, and record, a request whose horizon arrived before anything
    /// took it.
    ///
    /// The one path for both ways a horizon arrives: while placement was
    /// waiting for a usable machine, and while an offer was out. `rejected` is
    /// the last survey's account, when there was one. Every decline recorded on
    /// the entry joins it, superseded ones included, so a machine that declined,
    /// posted state, and then sat on the re-offer is still named. The sentence
    /// returned and the decision recorded are built from the same rows, so the
    /// placement view shows exactly what the requester was told.
    pub(super) fn refuse_at_horizon(
        &self,
        request: &CellRequest,
        policy: &ActivePlacementPolicy,
        entry_id: Option<&str>,
        mut rejected: Vec<PlacementRejection>,
    ) -> CellOutcome {
        if let Some(entry_id) = entry_id {
            rejected = account_from_journal(rejected, &self.recorded_declines(entry_id));
        }
        let outcome = self.wait_horizon_outcome(request, &rejected);
        if let CellOutcome::Unavailable { diagnostic, .. } = &outcome {
            let mut decision = placement_decision(
                request,
                unix_time_ms(),
                Some(policy_evidence(request, policy, false)),
                None,
                PlacementOutcome::Refused {
                    diagnostic: diagnostic.clone(),
                },
                rejected,
            );
            if let Some(entry_id) = entry_id {
                self.record_binding_chain(&mut decision, entry_id);
            }
            self.record_placement_decision(decision);
        }
        outcome
    }

    /// Every machine's latest refusal of this entry, as placement rejections.
    ///
    /// Superseded declines are included: a machine that posted state and has
    /// not answered the re-offer yet still last said this, and its age is part
    /// of the sentence.
    pub(super) fn recorded_declines(&self, entry_id: &str) -> Vec<PlacementRejection> {
        let declined = self
            .queue
            .with_entry(entry_id, |entry| entry.declined.clone())
            .unwrap_or_default();
        let mut rows: Vec<PlacementRejection> = declined
            .into_iter()
            .map(|(executor_id, decline)| PlacementRejection {
                unreadable_memory: Vec::new(),
                drain_blockers: Vec::new(),
                executor_name: self
                    .executor_public_name(&executor_id)
                    .unwrap_or_else(|| executor_id.clone()),
                executor_id,
                reason: declined_rejection(&decline),
                prediction: None,
            })
            .collect();
        rows.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
        rows
    }

    /// Wait for an executor this request can be placed on, bounded by the
    /// requester's horizon.
    ///
    /// `request` is immutable on purpose. Selection used to push the horizon
    /// forward for every interval it spent watching a supervisor come back,
    /// which is the deadline-pause model surviving in a second place: it made
    /// the number this function hands to the executor later than the instant the
    /// requester actually declared, and under a supervisor that keeps declaring
    /// itself fresh it advanced at wall-clock rate, so a batch that had stated
    /// its whole willingness to wait could be held past it without bound.
    ///
    /// Nothing pauses a horizon. A wait here ends when an executor appears or
    /// when the requester's own instant arrives, and a machine with nothing
    /// being supervised at all still fails immediately rather than waiting.
    ///
    /// `entry_id` names the queue entry this placement is for, whose declines
    /// are re-read on every pass. A decline is evidence about one machine now,
    /// not a statement that the machine does not exist: the request waits for
    /// that machine to post new state, bounded by its own horizon, and a
    /// decline cleared while it waits makes the machine offerable on the very
    /// next pass.
    #[allow(clippy::result_large_err)]
    pub(super) async fn select_executor(
        &self,
        request: &CellRequest,
        plan: Option<&ReservationPlan>,
        policy: &ActivePlacementPolicy,
        entry_id: Option<&str>,
    ) -> Result<Placement, CellOutcome> {
        // The reasons the last recorded wait named, so a request parked on them
        // is recorded once per change rather than on every pass.
        let mut recorded_wait: Option<String> = None;
        let mut last_rejected = Vec::new();
        loop {
            if unix_time_ms() >= request.wait_horizon_unix_ms {
                return Err(self.refuse_at_horizon(request, policy, entry_id, last_rejected));
            }
            // A machine that posted new state may have become willing, so its
            // report wakes this wait as surely as a new connection does. That is
            // the whole of the retry rule: posted state, not a clock.
            let notified = self.connection_ready.notified();
            let (declined, view) = entry_id
                .and_then(|id| {
                    self.queue
                        .with_entry(id, |entry| (entry.declined.clone(), entry.admission_view()))
                })
                .unwrap_or_else(|| (HashMap::new(), AdmissionView::of_request(request)));
            let selection = off_frame(|| {
                self.select_executor_once_with(
                    request,
                    plan,
                    policy,
                    &declined,
                    &view,
                    |request, entry| self.priced_sync_cost(request, entry),
                )
            })
            .await;
            let (rejected, releasing_headroom) = match selection {
                Ok(Selection::Placed(placement)) => return Ok(*placement),
                Err(refused) => {
                    let mut decision = refused.decision;
                    self.stamp_update_bounds(&mut decision.rejected);
                    self.record_placement_decision(decision);
                    return Err(CellOutcome::Unavailable {
                        reason: refused.reason,
                        diagnostic: refused.diagnostic,
                    });
                }
                Ok(Selection::Waiting { rejected }) => (rejected, false),
                Ok(Selection::ReleasingHeadroom { rejected }) => (rejected, true),
            };
            let mut rejected = rejected;
            self.stamp_update_bounds(&mut rejected);
            // The work went nowhere. A machine that refused it only for room
            // a model resident holds was otherwise able to take it (the room
            // is checked after every eligibility check), so placement's model
            // pass may evict that resident if it can give way. A machine
            // whose own decline of this entry still stands would not take it
            // either, and an executor's decline evicts nothing: it does not
            // say whose memory is short. Nor does work whose memory was never
            // measured: a guess waits for a model, it never displaces one.
            self.note_room_refusal(
                rejected
                    .iter()
                    .filter(|rejection| {
                        matches!(
                            rejection.reason,
                            PlacementRejectionReason::ModelResident {
                                demand_unmeasured: false,
                                ..
                            }
                        ) && !declined.get(&rejection.executor_id).is_some_and(|decline| {
                            self.lock_connections()
                                .get(&rejection.executor_id)
                                .is_some_and(|machine| {
                                    decline.stands_on(machine.generation, unix_time_ms())
                                })
                        })
                    })
                    .map(|rejection| rejection.executor_name.clone()),
            );
            // Work kept off the machines it cannot run on, or waiting on a
            // machine's update, is pending for a reason the queue view and the
            // machine's own page owe their reader, and nothing else records a
            // pass that placed nothing.
            let now = unix_time_ms();
            let updates: Vec<String> = rejected
                .iter()
                .filter(|rejection| {
                    matches!(
                        rejection.reason,
                        PlacementRejectionReason::UpdatePending { .. }
                    )
                })
                .map(|rejection| {
                    self.update_wait_sentence(&rejection.executor_id, &rejection.executor_name, now)
                })
                .collect();
            let takes_room_on = match entry_id {
                Some(_) => self.room_taken_first_on(
                    &(request.request_id.clone(), request.attempt_id.clone()),
                    now,
                ),
                None => Vec::new(),
            };
            let headroom = self.headroom_status(
                &(request.request_id.clone(), request.attempt_id.clone()),
                now,
            );
            let mut waiting = wait_decision(request, policy, &rejected, &updates, &takes_room_on);
            if let Some((_, sentence)) = &headroom {
                match waiting.as_mut().map(|decision| &mut decision.outcome) {
                    Some(PlacementOutcome::Waiting { diagnostic }) => {
                        *diagnostic = format!("{sentence}. {diagnostic}");
                    }
                    _ => {
                        waiting = Some(placement_decision(
                            request,
                            now,
                            Some(policy_evidence(request, policy, false)),
                            None,
                            PlacementOutcome::Waiting {
                                diagnostic: sentence.clone(),
                            },
                            rejected.clone(),
                        ));
                    }
                }
            }
            if let Some(mut waiting) = waiting {
                let mut extra: Vec<(String, String)> = takes_room_on
                    .iter()
                    .map(|name| (name.clone(), "takesRoomFirst".to_string()))
                    .collect();
                if let Some((hold_signature, _)) = headroom {
                    extra.push(("headroom".into(), hold_signature));
                }
                let signature = decision_signature(&waiting, &extra);
                if recorded_wait.as_ref() != Some(&signature) {
                    if let Some(entry_id) = entry_id {
                        self.record_binding_chain(&mut waiting, entry_id);
                    }
                    self.record_signed_placement_decision(waiting, signature.clone());
                    recorded_wait = Some(signature);
                }
            }
            // An exclusion is waited out like a decline: the work cannot go
            // where it was killed, and another machine attaching or freeing up
            // is what ends the wait.
            let parked_on: Vec<PlacementRejection> = rejected
                .iter()
                .filter(|rejection| {
                    waits_on_machine(&rejection.reason)
                        || excluded_by_memory_kill(&rejection.reason)
                })
                .cloned()
                .collect();
            let colocated = self.colocated_substrate();
            // A held generation is not a recovery in progress, however fresh its
            // declaration: it ends when another process exits, so waiting on it
            // parks a caller on something no progress of ours will release.
            let generation_held = colocated
                .as_ref()
                .filter(|evidence| evidence.state == ExecutorSubstrateState::GenerationHeld)
                .and_then(|evidence| evidence.diagnostic.clone());
            // A colocated machine whose pipe floor refuses new processes is not
            // a transient to wait on: its beat stays fresh while it is unable
            // to start anything, and untargeted work would sit out its horizon.
            let transient = colocated
                .filter(|evidence| {
                    evidence.state != ExecutorSubstrateState::GenerationHeld
                        && now.saturating_sub(evidence.last_progress_unix_ms)
                            <= EXECUTOR_PROGRESS_FRESHNESS_MS
                })
                .filter(|_| !self.colocated_pipe_floor_refuses(pipe_floor_work(request), now));
            let targeted = request.pinned_executor_id.is_some()
                || request
                    .executor
                    .as_ref()
                    .is_some_and(|selector| !selector.is_empty());
            // A machine that declined is attached, alive, and capable, which is
            // better evidence of something to wait for than any substrate
            // declaration.
            let headroom_pending = entry_id
                .and_then(|id| self.queue.headroom_plan(id, now))
                .is_some_and(|plan| plan.target.is_some());
            if transient.is_none()
                && !targeted
                && parked_on.is_empty()
                && !headroom_pending
                && !releasing_headroom
            {
                return Err(executor_unavailable(
                    generation_held.unwrap_or_else(|| NO_COLOCATED_EXECUTOR.into()),
                ));
            }
            let remaining = request.wait_horizon_unix_ms.saturating_sub(now);
            if remaining == 0 {
                // An untargeted request reaches here whenever a live substrate
                // never freed capacity within the horizon, so the diagnostic
                // cannot assume a selector was stated. When one was, the caller
                // is owed both what it asked for and what exists.
                return Err(self.refuse_at_horizon(request, policy, entry_id, rejected));
            }
            last_rejected = rejected;
            let _ = tokio::time::timeout(Duration::from_millis(remaining.clamp(1, 250)), notified)
                .await;
        }
    }

    /// The sync cost placement reads for `entry`: from the runner's price cache,
    /// never by walking the repository on the placement path.
    pub(super) fn priced_sync_cost(
        &self,
        request: &CellRequest,
        entry: &ExecutorConnectionState,
    ) -> SyncCost {
        repository_sync_cost(request, entry, |repository, roots, target| {
            self.placement_prices.sync_cost(repository, roots, target)
        })
    }

    #[allow(clippy::result_large_err)]
    pub(super) async fn select_executor_once_with(
        &self,
        request: &CellRequest,
        plan: Option<&ReservationPlan>,
        policy: &ActivePlacementPolicy,
        declined: &HashMap<String, queue::EntryDecline>,
        view: &AdmissionView,
        estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    ) -> Result<Selection, RefusedPlacement> {
        // Placement can inspect the local repository to estimate transfer cost
        // and the profile store to estimate demand. Snapshot the bounded executor
        // metadata first so that neither ever holds the connection lock needed by
        // transport-side heartbeat and snapshot processing.
        let connections = self.lock_connections().clone();
        let unattached = self.unattached_enrolled_remotes();
        let now = unix_time_ms();
        // Stage one: which machines could structurally take this at all, and
        // which of those have already said "not yet" to this entry. Keep the
        // actual survey's refusal while the shadow is ranked independently: an
        // unavailable home is the most important case for the counterfactual.
        let actual_survey =
            survey_candidates(&connections, &unattached, request, now, declined, view)
                .map(|survey| self.defer_to_provisioning_lines(survey, request, now))
                .map(|survey| self.defer_to_cadence_lanes(survey, now));
        // A decline/backoff describes availability now, not eligibility for an
        // aging reservation. Price a second survey through the SAME ranking
        // gates, without those historical refusals, to choose its one target.
        let headroom_survey = view.entry_id.as_deref().and_then(|id| {
            self.queue.headroom_plan(id, now)?;
            survey_candidates(
                &connections,
                &unattached,
                request,
                now,
                &HashMap::new(),
                view,
            )
            .map(|survey| self.defer_to_provisioning_lines(survey, request, now))
            .map(|survey| self.defer_to_cadence_lanes(survey, now))
            .ok()
        });
        let shadow_request = hermetic_shadow_request(request);
        let shadow_survey = shadow_request.as_ref().and_then(|shadow| {
            survey_candidates(&connections, &unattached, shadow, now, declined, view).ok()
        });
        // Stage two: what it would cost on each of them. Resource profiles are
        // executor-context keyed, so this is a per-candidate question -- and it
        // is asked only of candidates, so a request waiting for an executor to
        // attach asks nothing at all. Pure with respect to placement: nothing
        // here reserves anything.
        let mut reservations = HashMap::new();
        let mut predictions = HashMap::new();
        let mut sync_costs = HashMap::new();
        let oracle = DurationOracle {
            db: plan.map(|plan| plan.db.clone()),
        };
        let mut estimation_entries = actual_survey
            .as_ref()
            .map(|survey| survey.usable.clone())
            .unwrap_or_default();
        if let Some(survey) = headroom_survey.as_ref() {
            for entry in &survey.usable {
                if !estimation_entries
                    .iter()
                    .any(|candidate| candidate.identity.executor_id == entry.identity.executor_id)
                {
                    estimation_entries.push(entry);
                }
            }
        }
        if let Some(shadow) = shadow_survey.as_ref() {
            for entry in &shadow.usable {
                if !estimation_entries
                    .iter()
                    .any(|candidate| candidate.identity.executor_id == entry.identity.executor_id)
                {
                    estimation_entries.push(entry);
                }
            }
        }
        // Measured once per (candidate, request) here and handed to both
        // rankings below; a pair not yet priced is absent, which ranks as an
        // unknown distance, and is ready for the next pass.
        let cache_distances = candidate_cache_distances_with(
            &estimation_entries,
            request,
            now,
            |repository, head, target| {
                self.placement_prices
                    .cache_distance(repository, head, target)
            },
        );
        for entry in &estimation_entries {
            let context = ReservationPlan::context_for(
                &entry.identity.device_id,
                &entry.identity.executor_id,
                &entry.advertisement.capabilities,
            );
            let warmth = candidate_warmth(entry, request, now);
            let duration_context = resource_profiles::DurationContext {
                class: request.command_class,
                warmth: warmth.for_lookup(),
                now_unix_ms: now,
            };
            let run = match plan {
                Some(plan) => {
                    let mut resolved = off_frame(|| {
                        plan.resolve_for(
                            request,
                            &entry.identity.device_id,
                            &entry.identity.executor_id,
                            &entry.advertisement.capabilities,
                            duration_context,
                        )
                    })
                    .await;
                    self.record_memory_lookup_failures(&mut resolved, &entry.identity.executor_id);
                    let run = resolved.duration.clone();
                    reservations.insert(entry.identity.executor_id.clone(), resolved);
                    run
                }
                None => {
                    oracle
                        .predict(
                            request.command_resource_identity.as_ref(),
                            &context,
                            warmth.for_lookup(),
                            request.command_class,
                            now,
                        )
                        .await
                }
            };
            let load = candidate_load(entry);
            let contention = match &oracle.db {
                Some(db) => {
                    resource_profiles::resolve_contention(db.clone(), &context, &load, now).await
                }
                None => resource_profiles::contention_prior(&load),
            };
            let prices = QueuePrices::resolve(&oracle, entry, &context, now).await;
            let queue = forecast_queue_wait(entry, &prices, request, run.predicted_ms, now);
            let sync_cost = estimate(request, entry);
            sync_costs.insert(entry.identity.executor_id.clone(), sync_cost);
            let mut prediction = placement_prediction_with_contention(
                entry, warmth, queue, run, contention, sync_cost,
            );
            price_host_stall(&mut prediction, &entry.health.host, now);
            if let Some(db) = &oracle.db {
                let identities: Vec<&CommandResourceIdentity> = match plan {
                    Some(plan) if !plan.batch_identities.is_empty() => {
                        plan.batch_identities.iter().collect()
                    }
                    _ => request.command_resource_identity.iter().collect(),
                };
                if !identities.is_empty() {
                    price_reliability(
                        &mut prediction,
                        reliability::resolve(
                            db.clone(),
                            &identities,
                            &entry.identity.executor_id,
                            &executor_public_name(entry),
                            now,
                        )
                        .await,
                    );
                }
            }
            predictions.insert(entry.identity.executor_id.clone(), prediction);
        }
        // A machine that is only away for now (it declined this entry, is
        // backing off, updating, drained, or another request has its turn)
        // left the usable set before anything was priced for it, yet whether
        // one process of this work fits it is a fact about its size that its
        // momentary state does not change. Judge it the same way, so a hard
        // misfit everywhere is refused rather than waited on. Kept apart from
        // `reservations`, which ranking and the ceiling note read as the set
        // of machines that were offered a choice.
        let mut actual_survey = actual_survey;
        if let (Some(plan), Ok(survey)) = (plan, actual_survey.as_mut()) {
            let mut away_reservations = HashMap::new();
            for row in survey
                .rejected
                .iter()
                .filter(|row| away_for_now(&row.reason))
            {
                let Some(entry) = connections.get(&row.executor_id) else {
                    continue;
                };
                let duration_context = resource_profiles::DurationContext {
                    class: request.command_class,
                    warmth: candidate_warmth(entry, request, now).for_lookup(),
                    now_unix_ms: now,
                };
                let mut resolved = off_frame(|| {
                    plan.resolve_for(
                        request,
                        &entry.identity.device_id,
                        &entry.identity.executor_id,
                        &entry.advertisement.capabilities,
                        duration_context,
                    )
                })
                .await;
                self.record_memory_lookup_failures(&mut resolved, &row.executor_id);
                away_reservations.insert(row.executor_id.clone(), resolved);
            }
            judge_machines_away_for_now(&mut survey.rejected, &connections, &away_reservations);
        }
        // Stage three: rank on when each machine is predicted to answer. Budget
        // inspection and consumption share one short critical section, after all
        // asynchronous prediction work, so concurrent waves cannot spend the
        // same exploration slot.
        let cadence_lane = self.cadence_lane_restart(request);
        if let Some(id) = view.entry_id.as_deref() {
            let target = self.prepare_headroom_target(
                id,
                request,
                headroom_survey,
                &connections,
                SurveyRankingContext {
                    reservations: &reservations,
                    predictions: &predictions,
                    sync_costs: &sync_costs,
                    cache_distances: &cache_distances,
                    policy,
                    now_unix_ms: now,
                    exploration: &ExplorationState::default(),
                    cadence_lane: cadence_lane.as_ref(),
                },
            );
            if let Some(target) = target {
                if !target.enabled {
                    return Ok(Selection::ReleasingHeadroom {
                        rejected: actual_survey
                            .map(|survey| survey.rejected)
                            .unwrap_or_default(),
                    });
                }
                let mut offer_declines = declined.clone();
                let ready = self
                    .queue
                    .with_entry(id, |entry| {
                        entry.may_offer_to(&target.executor_id, target.generation, now)
                    })
                    .unwrap_or(false);
                if ready {
                    offer_declines.remove(&target.executor_id);
                }
                actual_survey = survey_candidates(
                    &connections,
                    &unattached,
                    request,
                    now,
                    &offer_declines,
                    view,
                )
                .map(|mut survey| {
                    survey.usable.retain(|machine| {
                        machine.identity.executor_id == target.executor_id
                            && machine.generation == target.generation
                    });
                    survey
                })
                .map(|survey| self.defer_to_provisioning_lines(survey, request, now))
                .map(|survey| self.defer_to_cadence_lanes(survey, now));
            }
        }
        let mut exploration_by_work_class = self.exploration_by_work_class.lock().unwrap();
        let exploration = exploration_by_work_class
            .entry(request.placement_work_class)
            .or_default();
        let unpinned_winner = match (shadow_request.as_ref(), shadow_survey) {
            (Some(shadow_request), Some(shadow_survey)) => rank_survey(
                shadow_request,
                shadow_survey,
                SurveyRankingContext {
                    reservations: &reservations,
                    predictions: &predictions,
                    sync_costs: &sync_costs,
                    cache_distances: &cache_distances,
                    policy,
                    now_unix_ms: now,
                    exploration,
                    cadence_lane: cadence_lane.as_ref(),
                },
            )
            .ok()
            .and_then(|draft| draft.selected)
            .map(|(_, selection)| PlacementCounterfactualWinner {
                executor_name: selection.executor_name,
                executor_id: selection.executor_id,
                reason: selection.reason,
                prediction: selection.prediction,
            }),
            _ => None,
        };
        let survey = match actual_survey {
            Ok(survey) => survey,
            Err(diagnostic) => {
                return Err(refused_placement(
                    request,
                    now,
                    policy,
                    unpinned_winner,
                    diagnostic,
                ));
            }
        };
        if let Some(refused) = home_cannot_hold(
            request,
            now,
            policy,
            &reservations,
            &survey,
            unpinned_winner.as_ref(),
        ) {
            return Err(refused);
        }
        // Ranking can still find nothing it may choose among the usable
        // machines (policy keeps work off one, telemetry blinds another). While
        // a capable machine's decline stands, that is not a refusal: the
        // request waits for the decliner, and the gates that excluded the rest
        // stay exactly as strict.
        let parked_on_decliner = survey
            .rejected
            .iter()
            .any(|rejection| waits_on_machine(&rejection.reason))
            .then(|| survey.rejected.clone());
        let draft = match rank_survey(
            request,
            survey,
            SurveyRankingContext {
                reservations: &reservations,
                predictions: &predictions,
                sync_costs: &sync_costs,
                cache_distances: &cache_distances,
                policy,
                now_unix_ms: now,
                exploration,
                cadence_lane: cadence_lane.as_ref(),
            },
        ) {
            Ok(draft) => draft,
            Err(_) if parked_on_decliner.is_some() => {
                return Ok(Selection::Waiting {
                    rejected: parked_on_decliner.unwrap_or_default(),
                });
            }
            Err(diagnostic) => {
                return Err(refused_placement(
                    request,
                    now,
                    policy,
                    unpinned_winner,
                    diagnostic,
                ));
            }
        };
        let Some((selected, selection)) = draft.selected else {
            return Ok(Selection::Waiting {
                rejected: draft.rejected,
            });
        };
        let is_current = self
            .lock_connections()
            .get(&selected.executor_id)
            .is_some_and(|entry| {
                entry.generation == selected.generation
                    && entry.sender.same_channel(&selected.sender)
                    && !entry.sender.is_closed()
            });
        if !is_current {
            // A reconnect can replace the selected generation while repository
            // and demand estimation are in flight. Let the outer placement loop
            // rank the fresh connection instead of dispatching through a retired
            // sender.
            return Ok(Selection::Waiting {
                rejected: draft.rejected,
            });
        }
        if request.placement_mobility.may_spill() && !placement_settled_by_caller(request) {
            exploration.record(
                (selection.reason == PlacementReason::BootstrapExploration)
                    .then_some(selection.executor_id.as_str()),
            );
            exploration.record_selection(selection.executor_id.as_str());
        }
        drop(exploration_by_work_class);
        let mut rejected = draft.rejected;
        let above_ceiling =
            above_memory_ceiling(&selection, &mut rejected, &reservations, &connections);
        let reservation = reservations.remove(&selected.executor_id);
        let decision = placement_decision(
            request,
            now,
            Some(draft.policy),
            unpinned_winner,
            PlacementOutcome::Selected(Box::new(selection)),
            rejected,
        );
        Ok(Selection::Placed(Box::new(Placement {
            selected,
            reservation,
            decision,
            above_ceiling,
        })))
    }

    /// Pass over every usable machine where this entry would provision a cell
    /// while an older request is ahead of it in that machine's line for the
    /// project's provisioning hold (see
    /// [`queue::ExecutionQueue::ahead_in_provisioning_line`]).
    ///
    /// When a hold clears, the machine posts state and every request parked
    /// behind it wakes on the same broadcast. Without this the first placement
    /// pass to reach an offer took the hold, however recently its request
    /// arrived, and one that had waited its whole horizon could lose every
    /// race until it was refused. Passing a machine over here is not a
    /// refusal: work free to go elsewhere does, and work that cannot waits for
    /// its turn. Work that runs inside a live residency or a checkout it
    /// already has provisions nothing there and never stands in the line.
    pub(super) fn defer_to_provisioning_lines<'a>(
        &self,
        mut survey: CandidateSurvey<'a>,
        request: &CellRequest,
        now_unix_ms: u64,
    ) -> CandidateSurvey<'a> {
        let Some(entry_id) = survey.view.entry_id.clone() else {
            return survey;
        };
        let view = survey.view.clone();
        let mut deferred = Vec::new();
        survey.usable.retain(|machine| {
            let posted = decline_condition::PostedState {
                snapshot: &machine.snapshot,
                health: &machine.health,
                colocated: machine.colocated,
            };
            if !view.provisions(request, posted) {
                return true;
            }
            let Some(ahead) = self.queue.ahead_in_provisioning_line(
                &entry_id,
                &machine.identity.executor_id,
                machine.generation,
                now_unix_ms,
            ) else {
                return true;
            };
            deferred.push(PlacementRejection {
                unreadable_memory: Vec::new(),
                drain_blockers: Vec::new(),
                prediction: None,
                executor_name: executor_public_name(machine),
                executor_id: machine.identity.executor_id.clone(),
                reason: PlacementRejectionReason::ProvisioningTurn {
                    project_id: request.project_id.clone(),
                    ahead_request_id: ahead.request_id,
                    ahead_waiting_since_unix_ms: ahead.waiting_since_unix_ms,
                },
            });
            false
        });
        survey.rejected.extend(deferred);
        survey
    }

    /// Live reservation evidence for a queued request, never inferred from its
    /// authorization. The stable key lets the journal record transitions without
    /// appending another decision for every second the age display advances.
    pub(super) fn headroom_status(
        &self,
        identity: &RequestIdentity,
        now_unix_ms: u64,
    ) -> Option<(String, String)> {
        let id = self
            .queue
            .with_entry_by_identity(identity, |entry| entry.id.clone())?;
        let plan = self.queue.headroom_plan(&id, now_unix_ms)?;
        let target = plan.target?;
        let connections = self.lock_connections();
        let machine = connections
            .get(&target.executor_id)
            .filter(|machine| machine.generation == target.generation)?;
        if !target.enabled {
            return Some((
                format!("releasing:{}:{}", target.executor_id, target.generation),
                format!(
                    "releasing headroom on {} for {} before choosing another machine",
                    executor_public_name(machine),
                    identity.0
                ),
            ));
        }
        let hold = machine
            .health
            .admission
            .headroom_reservations
            .iter()
            .find(|hold| {
                hold.entry_id == id
                    && hold.request_id == identity.0
                    && hold.attempt_id == identity.1
            })?;
        Some((
            format!(
                "{}:{}:{}:{:?}:{:?}:{}",
                target.executor_id,
                target.generation,
                hold.reserved_at_unix_ms,
                hold.reservation,
                hold.memory_floor_bytes,
                hold.protected
            ),
            hold.describe(&executor_public_name(machine), now_unix_ms),
        ))
    }

    /// Choose one target with ordinary placement's gates and prices, retaining
    /// it while it remains eligible so headroom can accumulate. Revocation is
    /// acknowledged by the old generation before another
    /// machine can receive authorization; stale liveness cannot reserve two.
    fn prepare_headroom_target(
        &self,
        id: &str,
        request: &CellRequest,
        survey: Option<CandidateSurvey<'_>>,
        connections: &HashMap<String, ExecutorConnectionState>,
        context: SurveyRankingContext<'_>,
    ) -> Option<queue::HeadroomTarget> {
        let plan = self.queue.headroom_plan(id, context.now_unix_ms)?;
        let choose = |survey| {
            rank_survey(request, survey, context)
                .ok()?
                .selected
                .filter(|(_, selection)| selection.memory_ceiling.is_none())
        };
        if let Some(mut target) = plan.target {
            let existing = survey.as_ref().and_then(|survey| {
                let mut one = survey.clone();
                one.usable.retain(|machine| {
                    machine.identity.executor_id == target.executor_id
                        && machine.generation == target.generation
                });
                choose(one)
            });
            if target.enabled && existing.is_some() {
                return Some(target);
            }
            let current = connections
                .get(&target.executor_id)
                .filter(|machine| machine.generation == target.generation);
            if let Some(machine) = current {
                if target.enabled {
                    target.enabled = false;
                    self.queue.set_headroom_target(id, Some(target.clone()));
                    let _ = machine.sender.send(ExecutorMessage::ReleaseHeadroom {
                        entry_id: id.to_string(),
                    });
                }
                return Some(target);
            }
            // A generation ending drops its standing holds. Otherwise only
            // HeadroomReleased may clear a target, never a cached empty list.
            self.queue.set_headroom_target(id, None);
        }
        let (selected, selection) = choose(survey?)?;
        self.queue.observe_queue_forecast(
            id,
            selection
                .prediction
                .as_ref()
                .and_then(|prediction| prediction.queue.predicted_ms()),
        );
        let plan = self.queue.headroom_plan(id, context.now_unix_ms)?;
        if context.now_unix_ms < plan.reserve_from_unix_ms {
            return None;
        }
        let target = queue::HeadroomTarget {
            executor_id: selected.executor_id,
            generation: selected.generation,
            enabled: true,
            offered: false,
        };
        self.queue.set_headroom_target(id, Some(target.clone()));
        Some(target)
    }

    /// The main-cadence lanes that hold precedence for a machine's freed room
    /// now. The purpose registry's lock is released before the queue's is
    /// taken, so the two are never held together.
    pub(super) fn cadence_precedence(&self) -> queue::CadencePrecedence {
        self.purposes.lock().unwrap().main_cadence_holders()
    }

    /// Pass over every usable machine whose freed room a main-cadence lane
    /// for an unobserved tip is waiting for (see
    /// [`queue::ExecutionQueue::ahead_for_room`]), unless this entry is
    /// interactive or is such a lane itself.
    ///
    /// Like the provisioning line, passing a machine over is not a refusal and
    /// withholds nothing already running: work free to go elsewhere does, and
    /// work that cannot waits until the lane has had its turn.
    pub(super) fn defer_to_cadence_lanes<'a>(
        &self,
        mut survey: CandidateSurvey<'a>,
        now_unix_ms: u64,
    ) -> CandidateSurvey<'a> {
        let Some(entry_id) = survey.view.entry_id.clone() else {
            return survey;
        };
        let precedence = self.cadence_precedence();
        if precedence.is_empty() {
            return survey;
        }
        let mut deferred = Vec::new();
        survey.usable.retain(|machine| {
            let Some(ahead) = self.queue.ahead_for_room(
                &entry_id,
                &machine.identity.executor_id,
                machine.generation,
                now_unix_ms,
                &precedence,
            ) else {
                return true;
            };
            let Some((check, platform)) = ahead.key.main_cadence_lane() else {
                return true;
            };
            deferred.push(PlacementRejection {
                unreadable_memory: Vec::new(),
                drain_blockers: Vec::new(),
                prediction: None,
                executor_name: executor_public_name(machine),
                executor_id: machine.identity.executor_id.clone(),
                reason: PlacementRejectionReason::CadenceTurn {
                    check: check.to_string(),
                    platform: platform.to_string(),
                    ahead_request_id: ahead.request_id,
                    ahead_waiting_since_unix_ms: ahead.waiting_since_unix_ms,
                },
            });
            false
        });
        survey.rejected.extend(deferred);
        survey
    }

    /// The public names of the machines whose freed room the request queued
    /// as `identity` takes first, because it is main's cadence lane for a tip
    /// with no complete row and it waits for those machines' room. Empty for
    /// every other request.
    pub(crate) fn room_taken_first_on(
        &self,
        identity: &RequestIdentity,
        now_unix_ms: u64,
    ) -> Vec<String> {
        let precedence = self.cadence_precedence();
        if !precedence.contains_key(identity) {
            return Vec::new();
        }
        let machines: HashMap<String, (u64, String)> = self
            .lock_connections()
            .iter()
            .map(|(executor_id, entry)| {
                (
                    executor_id.clone(),
                    (entry.generation, executor_public_name(entry)),
                )
            })
            .collect();
        let generations = machines
            .iter()
            .map(|(executor_id, (generation, _))| (executor_id.clone(), *generation))
            .collect();
        let mut names: Vec<String> = self
            .queue
            .room_taken_first_on(identity, &generations, now_unix_ms, &precedence)
            .into_iter()
            .filter_map(|executor_id| machines.get(&executor_id).map(|(_, name)| name.clone()))
            .collect();
        names.sort();
        names
    }

    /// `executor_id`'s room line on its current link: the main-cadence lanes
    /// that take its next freed room, and how many requests wait behind them.
    pub(crate) fn room_line(&self, executor_id: &str, now_unix_ms: u64) -> queue::RoomLine {
        let Some(generation) = self
            .lock_connections()
            .get(executor_id)
            .map(|entry| entry.generation)
        else {
            return queue::RoomLine::default();
        };
        let precedence = self.cadence_precedence();
        self.queue
            .room_line(executor_id, generation, now_unix_ms, &precedence)
    }

    #[cfg(test)]
    pub(super) async fn wait_for_executor(
        &self,
        wait_horizon_unix_ms: u64,
    ) -> Result<mpsc::UnboundedSender<ExecutorMessage>, String> {
        let request = CellRequest {
            request_id: String::new(),
            attempt_id: String::new(),
            project_id: String::new(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: String::new(),
                repository_id: String::new(),
                absolute_path: String::new(),
            },
            base_commit: String::new(),
            command: String::new(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms,
            waiting_since_unix_ms: 0,
            timeout_ms: 0,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
            scratch_sync: None,
            home_affinity: None,
            publication: None,
        };
        self.select_executor(
            &request,
            None,
            &ActivePlacementPolicy::default_profile(),
            None,
        )
        .await
        .map(|placement| placement.selected.sender)
        .map_err(|outcome| match outcome {
            CellOutcome::Unavailable { diagnostic, .. } => diagnostic,
            _ => "executor unavailable".into(),
        })
    }

    /// The machine that has claimed the live attempt of `request_id`, if one
    /// has. An attempt only offered to a machine names none.
    /// What a request no machine has claimed is waiting on, as one phrase:
    /// since when, the machine it is offered to, and what each machine that
    /// refused it said, in that machine's words. `None` once a machine has
    /// claimed it, and when the queue does not hold it.
    pub(crate) fn unplaced_wait(&self, request_id: &str) -> Option<String> {
        let wait = self.queue.unclaimed_wait(request_id)?;
        let since = chrono::DateTime::from_timestamp_millis(wait.queued_at_unix_ms as i64)
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_else(|| "an unknown time".to_string());
        let waited = unix_time_ms().saturating_sub(wait.queued_at_unix_ms) / 1000;
        let mut phrase = format!("waiting to be placed since {since} ({waited}s)");
        if let Some(executor_id) = wait.offered_to.as_deref() {
            phrase.push_str(&format!(
                ", offered to `{}`, which has not answered",
                self.executor_name_for_text(executor_id)
            ));
        }
        if let Some(diagnostic) = wait.placement_wait.as_deref() {
            phrase.push_str(&format!("; {diagnostic}"));
        } else if wait.declines.is_empty() && wait.offered_to.is_none() {
            phrase.push_str(", and no machine has refused it yet");
        }
        for (executor_id, decline) in &wait.declines {
            phrase.push_str(&format!(
                "; `{}` declined it ({:?})",
                self.executor_name_for_text(executor_id),
                decline.reason
            ));
            if let Some(detail) = decline.detail.as_deref() {
                phrase.push_str(&format!(": {detail}"));
            }
        }
        Some(phrase)
    }
}

#[cfg(test)]
pub(super) fn choose_executor(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
) -> Result<Option<SelectedExecutor>, String> {
    Ok(choose_executor_with(
        connections,
        request,
        &HashMap::new(),
        measured_sync_cost,
        // The fixture clock, not the wall clock. Every fixture machine stamps
        // its last progress at `NOW`, and placement rejects a connection that
        // has been silent past the freshness bound — so reading the real clock
        // here aged every fixture out of the fleet and refused these placements
        // for a reason none of them are about.
        NOW,
    )?
    .selected
    .map(|(selected, _)| selected))
}

/// The sync cost a fixture placement prices synchronously, walking the object
/// graph inline rather than through the runner's background cache.
#[cfg(test)]
pub(super) fn measured_sync_cost(
    request: &CellRequest,
    entry: &ExecutorConnectionState,
) -> SyncCost {
    repository_sync_cost(request, entry, |repository, roots, target| {
        pricing::missing_reachable_object_bytes(repository, target, roots)
            .map_or(SyncCost::Unknown, SyncCost::Known)
    })
}

/// Predict every usable candidate with no profile store behind it.
///
/// This is exactly what production does for a placement that carries no resource
/// plan: labeled class priors for every duration, and a real queue forecast
/// built from the facts each machine published. It is the sync path, so tests
/// exercise the same ordering, capacity model, and honesty rules production
/// does rather than a second implementation of them.
#[cfg(test)]
pub(super) fn prior_predictions(
    usable: &[&ExecutorConnectionState],
    request: &CellRequest,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> (
    HashMap<String, PlacementPrediction>,
    HashMap<String, SyncCost>,
) {
    let mut predictions = HashMap::new();
    let mut sync_costs = HashMap::new();
    for entry in usable {
        let context = ReservationPlan::context_for(
            &entry.identity.device_id,
            &entry.identity.executor_id,
            &entry.advertisement.capabilities,
        );
        let warmth = candidate_warmth(entry, request, now_unix_ms);
        let prices = QueuePrices::from_priors(entry, &context);
        let run = resource_profiles::unmeasured_duration(
            request.command_class,
            &context,
            request.command_resource_identity.as_ref(),
            warmth.for_lookup(),
            DurationFallback::NoProfileStore,
        );
        let queue = forecast_queue_wait(entry, &prices, request, run.predicted_ms, now_unix_ms);
        let sync_cost = estimate(request, entry);
        sync_costs.insert(entry.identity.executor_id.clone(), sync_cost);
        let mut prediction = placement_prediction_with_contention(
            entry,
            warmth,
            queue,
            run,
            resource_profiles::contention_prior(&candidate_load(entry)),
            sync_cost,
        );
        price_host_stall(&mut prediction, &entry.health.host, now_unix_ms);
        predictions.insert(entry.identity.executor_id.clone(), prediction);
    }
    (predictions, sync_costs)
}

/// What one pass of placement concluded: the machine it chose, and every machine
/// it passed over with the reason.
///
/// A draft rather than the decision, because two facts are only knowable after
/// selection: the object-transfer coordinate a remote execution travels under,
/// and the instant the decision was taken.
#[derive(Debug)]
pub(super) struct PlacementDraft {
    pub(super) selected: Option<(SelectedExecutor, PlacementSelection)>,
    pub(super) rejected: Vec<PlacementRejection>,
    pub(super) policy: PlacementPolicyEvidence,
}

/// Both placement stages back to back, for tests that have no demand to resolve
/// between them.
///
/// Production keeps them apart because estimating what the work costs is
/// asynchronous and belongs strictly between the two: `survey_candidates` names
/// what could take the work, the estimate is resolved for exactly those
/// machines, and `rank_survey` decides among them.
#[cfg(test)]
pub(super) fn choose_executor_with(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> Result<PlacementDraft, String> {
    choose_executor_with_policy(
        connections,
        request,
        reservations,
        &ActivePlacementPolicy::default_profile(),
        estimate,
        now_unix_ms,
    )
}

#[cfg(test)]
pub(super) fn choose_executor_with_policy(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    policy: &ActivePlacementPolicy,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> Result<PlacementDraft, String> {
    let survey = survey_candidates(
        connections,
        &[],
        request,
        now_unix_ms,
        &HashMap::new(),
        &AdmissionView::of_request(request),
    )?;
    let (predictions, sync_costs) =
        prior_predictions(&survey.usable, request, &estimate, now_unix_ms);
    let cache_distances = candidate_cache_distances_with(
        &survey.usable,
        request,
        now_unix_ms,
        |repo, head, target| cairn_common::cache_distance::cache_distance(repo, head, target).ok(),
    );
    rank_survey(
        request,
        survey,
        SurveyRankingContext {
            reservations,
            predictions: &predictions,
            sync_costs: &sync_costs,
            cache_distances: &cache_distances,
            policy,
            now_unix_ms,
            exploration: &ExplorationState::default(),
            cadence_lane: None,
        },
    )
    .map_err(Into::into)
}

/// Whether the caller settled placement itself, leaving policy nothing to
/// decide among.
///
/// True for a residency pin, and for a selector that names one machine. False
/// for a platform or toolchain constraint, which narrows the fleet and then
/// leaves the choice open — that is still policy choosing, and it is held to the
/// same evidence as an unconstrained request.
pub(super) fn placement_settled_by_caller(request: &CellRequest) -> bool {
    request.pinned_executor_id.is_some()
        || request
            .executor
            .as_ref()
            .is_some_and(|selector| selector.name.is_some())
}

/// The machines that survived the structural filters, and the ones that did not
/// with the reason each was passed over.
#[derive(Clone)]
pub(super) struct CandidateSurvey<'a> {
    pub(super) usable: Vec<&'a ExecutorConnectionState>,
    pub(super) rejected: Vec<PlacementRejection>,
    /// How the executor will judge this entry, which ranking reads each
    /// machine's posted hold against.
    pub(super) view: AdmissionView,
    /// Each machine that has declined this entry, superseded or not.
    pub(super) declined_here: HashMap<String, PriorDecline>,
    /// The same declines as the rows a refusal renders: each machine's last
    /// answer to this entry, with its own detail. A refusal composed here is
    /// built from these through [`account_from_journal`], not from the
    /// runner's reading of the machine at the moment it was composed.
    pub(super) journal: Vec<PlacementRejection>,
    /// Each surveyed machine's advertised memory budget, so ranking can ask
    /// whether a machine it would wait on could hold the work at all.
    pub(super) memory_budgets: HashMap<String, Option<u64>>,
}

/// The facts about a piece of work that decide which machines could
/// structurally take it.
///
/// Borrowed from a [`CellRequest`] at placement, and stated directly by a caller
/// that asks the same question before a request exists. An occupancy forecast is
/// exactly that caller: it must read only the machines the work could land on,
/// and "could land on" has to be the one relation placement uses or the two
/// answers drift — a forecast scoped more loosely than placement predicts relief
/// from a machine the work will never reach (CAIRN-3429).
#[derive(Clone, Copy)]
pub(crate) struct PlacementScope<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) repository: &'a RepositoryLocator,
    pub(crate) selector: Option<&'a ExecutorSelector>,
    pub(crate) pinned_executor_id: Option<&'a str>,
    pub(crate) mobility: PlacementMobility,
    pub(crate) verdict_platforms: &'a [String],
    /// Machines this work must not land on because a check it carries is
    /// infrastructure-suppressed on that machine's current environment. Where
    /// the work may NOT go is a third half of the same placement question the
    /// selector and the trusted platforms ask.
    pub(crate) avoided_executor_ids: &'a [String],
    /// What the project's setup and build need of any machine that would
    /// host this work, whether or not the work's own selector asks for it.
    pub(crate) project_requirements: &'a ProjectPlacementRequirements,
    /// The class of work asking, which a machine's role accepts or refuses.
    pub(crate) work_class: PlacementWorkClass,
    /// Whether the work compiles the project's Rust whatever its selector
    /// names: its command is a cargo build, test or lint, or it is a job's
    /// execution environment, whose shells and terminals are where an agent
    /// builds. An unpinned `bun run test:rust` selects nothing and runs in
    /// that environment, so without this a machine that cannot build the
    /// project got both (cairn/5909).
    pub(crate) compiles_rust: bool,
    /// What the work asks of a machine's fresh-pipe floor: an agent's short
    /// command passes recovery hysteresis, anything that stays does not. The
    /// executor applies the same classification, so placement never rejects
    /// what the machine would take.
    pub(crate) pipe_floor: PipeFloorWork,
    /// Set for a publication leg, which placement offers by role
    /// ([`MachineRole::takes_publication_legs`]) rather than by class, and
    /// only to a measured publisher when it carries the release secret.
    pub(crate) publication: Option<PublicationCustody>,
}

/// The opening of every refusal caused by a project config that cannot be
/// loaded. The surface that renders a refusal leads with this sentence, not
/// with its environment framing, because the fault is a file in the
/// repository rather than any machine.
pub(crate) const PROJECT_CONFIG_REFUSAL: &str =
    "this project's .cairn/config.yaml refuses every execution in the project";

/// The refusal for a project whose canonical execution policy cannot be
/// loaded: every batch, check lane, and terminal in the project is refused
/// until the file loads, so the sentence says that before it says why.
pub(crate) fn project_config_refusal(error: &str) -> String {
    format!(
        "{PROJECT_CONFIG_REFUSAL} until it loads, because {error}. Fix or revert the file, \
         or run a Cairn build that understands it"
    )
}

/// A project's placement requirements as its checkout at `project_path`
/// declares them, for a caller asking the eligibility question before a
/// request exists. A policy that cannot be read requires nothing here: the
/// request itself is refused at preparation with that error.
pub(super) fn declared_project_requirements_at(
    project_path: &Path,
) -> ProjectPlacementRequirements {
    crate::config::project_settings::load_execution_project_policy(project_path)
        .map(|policy| {
            project_placement_requirements(&policy.setup_commands, &policy.requires, project_path)
        })
        .unwrap_or_default()
}

/// Recast a residency-pinned request as the hermetic request that will replace it.
///
/// All three machine-binding gates move together. Clearing only the pin would
/// leave the shadow request immobile and its checkout non-transferable, making
/// the counterfactual silently choose the colocated executor every time.
pub(super) fn hermetic_shadow_request(request: &CellRequest) -> Option<CellRequest> {
    request.pinned_executor_id.as_ref()?;
    let identity = request.repository.identity();
    let mut shadow = request.clone();
    shadow.pinned_executor_id = None;
    shadow.placement_mobility = PlacementMobility::SpillEligible;
    shadow.repository = RepositoryLocator::ManagedObjects {
        project_id: identity.project_id,
        repository_id: identity.repository_id,
        object_format: identity.object_format,
    };
    Some(shadow)
}

impl<'a> PlacementScope<'a> {
    pub(super) fn of(request: &'a CellRequest) -> Self {
        Self {
            project_id: &request.project_id,
            repository: &request.repository,
            selector: request
                .executor
                .as_ref()
                .filter(|selector| !selector.is_empty()),
            pinned_executor_id: request.pinned_executor_id.as_deref(),
            mobility: request.placement_mobility,
            verdict_platforms: &request.verdict_platforms,
            avoided_executor_ids: &request.avoided_executor_ids,
            project_requirements: &request.project_requirements,
            work_class: request.placement_work_class,
            compiles_rust: request.command_class.compiles_rust()
                || matches!(
                    acquired_residency_holder(request),
                    Some(ResidencyHolder::Job { .. })
                ),
            pipe_floor: pipe_floor_work(request),
            publication: request.publication,
        }
    }

    /// Whether the work named where it must run, by machine or by platform.
    /// Targeted work is not held to the colocated default, because naming a
    /// machine IS the permission to leave home.
    pub(super) fn targeted(&self) -> bool {
        self.selector.is_some() || self.pinned_executor_id.is_some()
    }
}

/// Why one machine cannot structurally take this work, or `None` if it could.
///
/// The filters that are facts rather than judgement: a closed link, a pin that
/// points elsewhere, a project this machine does not serve, a selector it does
/// not satisfy, a platform whose answer this request does not count,
/// conservative work on a machine that is not the home, or a repository that
/// cannot be recreated from objects. Ranking never reaches a machine this
/// rejects.
///
/// Verdict-platform trust is a filter here rather than a ranking key, and that
/// is the point: it decides eligibility, so no amount of idleness, cache warmth,
/// or predicted time-to-verdict can outweigh it downstream.
///
/// This is THE eligibility relation. Anything that needs to know where work
/// could go asks here rather than approximating with a subset of these clauses
/// — a forecast that checked only the selector would happily read a machine that
/// does not serve the project.
///
/// The room a model resident holds is not judged here but where the work's
/// resolved demand is known ([`model_room_rejection`]), after every clause
/// below, so a machine refused for it is otherwise able to take the work.
/// The role clause of [`candidate_rejection`]: whether this machine's role
/// lets it be offered this work at all, and, for a publication leg that
/// carries the release secret, whether it is a measured publisher.
///
/// A machine that advertises publisher mode under any other role is refused
/// everything, a pin included: its account was set up to hold the secret, and
/// a `general` role would run agent batches there. A residency pin is exempt
/// from the plain role refusal only, as before; the publisher requirement is
/// never exempt, like a missing GitHub credential, since it is a fact about
/// the machine and not about where the work already lives.
pub(super) fn role_rejection(
    entry: &ExecutorConnectionState,
    scope: PlacementScope<'_>,
    pinned_here: bool,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    publication_role_rejection(
        entry.machine_role,
        entry.advertisement.capabilities.publish_isolation.as_ref(),
        scope.publication,
        scope.work_class,
        pinned_here,
        now_unix_ms,
    )
}

/// [`role_rejection`] over the facts it reads, so a caller asking before a
/// request exists (the publication actor choosing a target's builders) gets
/// placement's own answer and its own words.
pub(crate) fn publication_role_rejection(
    role: MachineRole,
    publish_isolation: Option<&PublishIsolationEvidence>,
    publication: Option<PublicationCustody>,
    work_class: PlacementWorkClass,
    pinned_here: bool,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    if publish_isolation.is_some() && role != MachineRole::PublishOnly {
        return Some(PlacementRejectionReason::NotPublishEligible {
            why: format!(
                "started as a publisher but enrolled as {}; re-enrol it with --role publishOnly",
                role.as_str()
            ),
        });
    }
    match publication {
        None => (!role.accepts(work_class) && !pinned_here).then_some(
            PlacementRejectionReason::RoleExcluded {
                role,
                work_class,
                publication_leg: false,
            },
        ),
        Some(custody) => {
            if !role.takes_publication_legs() && !pinned_here {
                Some(PlacementRejectionReason::RoleExcluded {
                    role,
                    work_class,
                    publication_leg: true,
                })
            } else if custody.publisher_required {
                publisher_ineligibility(role, publish_isolation, now_unix_ms)
                    .map(|why| PlacementRejectionReason::NotPublishEligible { why })
            } else {
                None
            }
        }
    }
}

/// Why a machine may not hold the release secret, or `None` when it may: it
/// is a `publishOnly` executor whose isolation probe measured PASS no longer
/// than [`PUBLISH_ISOLATION_STALE_AFTER_MS`] ago. Unmeasured is a refusal,
/// never a pass (tenet 1441), and that includes a publisher whose first probe
/// has not returned.
pub(crate) fn publisher_ineligibility(
    role: MachineRole,
    evidence: Option<&PublishIsolationEvidence>,
    now_unix_ms: u64,
) -> Option<String> {
    if role != MachineRole::PublishOnly {
        return Some(format!("role {}", role.as_str()));
    }
    let Some(evidence) = evidence else {
        return Some("isolation unmeasured: executor not started as a publisher".into());
    };
    match &evidence.verdict {
        PublishIsolationVerdict::Unmeasured { reason } => {
            Some(format!("isolation unmeasured: {reason}"))
        }
        PublishIsolationVerdict::Fail { reason } => Some(format!("isolation FAIL: {reason}")),
        PublishIsolationVerdict::Pass => {
            let age = now_unix_ms.saturating_sub(evidence.measured_at_unix_ms);
            (age > PUBLISH_ISOLATION_STALE_AFTER_MS)
                .then(|| format!("isolation measured {}m ago; stale", age / 60_000))
        }
    }
}

pub(super) fn candidate_rejection(
    entry: &ExecutorConnectionState,
    scope: PlacementScope<'_>,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    let pinned_here = scope.pinned_executor_id == Some(entry.identity.executor_id.as_str());
    if let Some(reason) = role_rejection(entry, scope, pinned_here, now_unix_ms) {
        // First, so a machine that is wrong for this class says so rather than
        // reporting a drain or a stale link that could clear on its own. It
        // binds the colocated machine and an explicit selector alike; only a
        // residency pin is exempt, as for a drain, so work already resident
        // there when the role changed may finish.
        Some(reason)
    } else if scope
        .selector
        .is_some_and(|selector| !matches_selector(entry, selector))
    {
        // Ahead of every hold for the role's reason: a machine the work named
        // itself out of can never run it, and reporting its drain or update
        // hold instead makes it a machine to wait for. A macOS-only pass then
        // waited on a Linux machine's executor update rather than taking the
        // only Mac at the clamp (cairn/5731).
        Some(PlacementRejectionReason::SelectorMismatch {
            requested: scope
                .selector
                .map(|selector| selector.describe())
                .unwrap_or_default(),
        })
    } else if !scope.verdict_platforms.is_empty()
        && !scope
            .verdict_platforms
            .iter()
            .any(|platform| platform.eq_ignore_ascii_case(&entry.advertisement.capabilities.os))
    {
        // The same: a platform this verdict does not count from is not one
        // the work will ever wait for.
        Some(PlacementRejectionReason::UntrustedVerdictPlatform {
            os: entry.advertisement.capabilities.os.clone(),
            trusted: scope.verdict_platforms.to_vec(),
        })
    } else if scope
        .pinned_executor_id
        .is_some_and(|id| id != entry.identity.executor_id)
    {
        // Ahead of every hold for the selector's reason: a machine the pin
        // excludes can never run the work, and reporting its drain or update
        // hold instead makes it a machine to wait for. A batch waiting for room
        // on its own home then read as waiting on another machine's executor
        // update (cairn/6382).
        Some(PlacementRejectionReason::PinMismatch {
            pinned_executor_id: scope.pinned_executor_id.unwrap_or_default().to_string(),
        })
    } else if !serves_project(entry, scope.project_id) {
        Some(PlacementRejectionReason::ProjectUnavailable {
            project_id: scope.project_id.to_string(),
        })
    } else if let Some(reason) = setup_toolchain_deficiency(
        &entry.advertisement.capabilities,
        scope.project_id,
        scope.project_requirements,
    ) {
        // Every cell for the project runs its setup before the first command,
        // so this holds whatever the work itself asked for — and for a pin as
        // well: pinned work would fail at preparation, and a refusal that
        // names the cause is the better answer.
        Some(reason)
    } else if let Some(reason) = github_credential_deficiency(
        &entry.advertisement.capabilities,
        scope.project_id,
        scope.project_requirements,
    ) {
        // Not exempt for a pin either, for the same reason: pinned work would
        // fail at its first private fetch.
        Some(reason)
    } else if let Some(refusal) = applicable_refusal(
        entry,
        scope.project_id,
        scope.repository,
        scope.project_requirements,
    ) {
        // Only for work that will run this setup: a live checkout never
        // prepares, and a refusal recorded under another setup says nothing
        // about this one.
        Some(refused(refusal))
    } else if let Some(reason) = build_prerequisite_deficiency(
        &entry.advertisement.capabilities,
        scope.project_id,
        scope.project_requirements,
        scope.selector,
        scope.compiles_rust,
    ) {
        Some(reason)
    } else if !scope.targeted() && !entry.colocated && !scope.mobility.may_spill() {
        // Untargeted work that has not been declared mobile stays on the
        // machine holding the runner's own checkout. Absence of a selector is
        // not permission to move.
        Some(PlacementRejectionReason::NotColocated)
    } else if !entry.colocated && !repository_is_transferable(scope.repository) {
        // A checkout that already exists on one machine cannot be recreated
        // from managed objects on another.
        Some(PlacementRejectionReason::RepositoryNotTransferable {
            locator: repository_locator_name(scope.repository).into(),
        })
    } else if drain_projection::entry_refuses(
        entry,
        drain_projection::resident_scope(pinned_here, scope.work_class),
    ) {
        // Operator, thread, legacy, fence and startup holds refuse all new
        // admission, a residency pin included. The self-update rollout's hold
        // refuses only new environments: a job resident here runs its next
        // batch or terminal on the outgoing build and the update waits for it,
        // as cairn/6382 promised. Running commands, control and cleanup never
        // pass through placement.
        Some(drain_projection::rejection(entry))
    } else if let Some(volume) = entry
        .health
        .build_volume
        .as_ref()
        .filter(|volume| volume.is_absent())
        .filter(|_| needs_build_volume(scope.repository))
    {
        // The machine says the volume it keeps cells on is not mounted. Like
        // a drain this is filtered, not offered and failed: another machine
        // taking the work records nothing against this one. Unlike a drain it
        // binds a residency pin too, because the pinned cell is on that very
        // volume; the work is refused at once with the reason rather than
        // offered to a machine that can only decline it.
        Some(PlacementRejectionReason::BuildVolumeAbsent {
            path: volume.root.clone(),
        })
    } else if let Some(reason) = pipe_health_rejection(entry, scope.pipe_floor, now_unix_ms) {
        // Binds every pin, a residency's included, and an explicit name: a
        // process started there may block forever on its first pipe write, so
        // a pin is not a safety override. Movable work goes to a machine whose
        // pipes are healthy; work that cannot leave this one waits the floor
        // out (see `survey_candidates`).
        Some(reason)
    } else if scope
        .avoided_executor_ids
        .iter()
        .any(|id| id == &entry.identity.executor_id)
    {
        // A check this work carries has spent its retry budget on this machine's
        // current environment. No fallback and no override: suppression that can
        // be overridden by a later placement decision is advisory, and a kill
        // switch may not be advisory. The machine remains eligible for every
        // other check.
        Some(PlacementRejectionReason::AvoidedInfrastructure)
    } else if entry.sender.is_closed() {
        Some(PlacementRejectionReason::ConnectionClosed)
    } else {
        match judge_link(entry, now_unix_ms, EXECUTOR_PROGRESS_FRESHNESS_MS) {
            link_clock::LinkJudgement::Fresh => None,
            link_clock::LinkJudgement::PeerSilent { silence_ms } => {
                Some(PlacementRejectionReason::ConnectionStale {
                    silence_ms,
                    stale_after_ms: EXECUTOR_PROGRESS_FRESHNESS_MS,
                })
            }
            // Not a statement about the machine: the runner's reader for this
            // link is not running, so it cannot know. Placement still passes
            // the machine over for now, because nothing sent there would be
            // heard either, and says whose fault the gap is.
            link_clock::LinkJudgement::JudgeAbsent {
                reader_silence_ms,
                ingest_phase,
                ingest_age_ms,
            } => Some(PlacementRejectionReason::RunnerNotListening {
                reader_silence_ms,
                ingest_phase: ingest_phase.as_str().to_string(),
                ingest_age_ms,
            }),
        }
    }
}

/// This machine's fresh-pipe floor, assessed against the runner's clock now.
///
/// A machine that posted no evidence (an executor that predates the probe, or
/// one that has not sampled yet) is a named gap and admits; so does every
/// platform the floor does not apply to. Neither is ever read as healthy.
pub(super) fn pipe_health_assessment(
    entry: &ExecutorConnectionState,
    now_unix_ms: u64,
) -> cairn_common::pipe_health::PipeHealthAssessment {
    match entry.health.host.pipe_health.as_ref() {
        Some(evidence) => evidence.assess(now_unix_ms),
        None => cairn_common::pipe_health::PipeHealthAssessment::not_reported(
            "this executor posted no fresh-pipe evidence",
        ),
    }
}

pub(super) fn pipe_health_rejection(
    entry: &ExecutorConnectionState,
    work: PipeFloorWork,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    let assessment = pipe_health_assessment(entry, now_unix_ms);
    (!assessment.admits_work(work)).then_some(PlacementRejectionReason::PipeDegraded { assessment })
}

/// Every machine that could structurally take this work, and every one that
/// could not with the reason.
///
/// Separate from ranking so that estimating what the work costs only ever
/// happens for machines that could actually take it. A request waiting for an
/// executor to attach surveys an empty fleet and queries nothing.
///
/// `declined` is this request's queue entry's declines. A machine that passes
/// every structural filter but has declined this entry on the connection it
/// still holds is rejected as [`PlacementRejectionReason::Declined`]: it is
/// not offered again until it posts new state, and it is something to wait
/// for rather than a reason to refuse. A structural reason always wins over a
/// decline, so a machine that is wrong for the work says so.
///
/// `view` is how the executor will judge this entry. The survey carries it to
/// ranking, which reads each usable machine's posted hold once the demand the
/// offer will name has been resolved for that machine (see [`rank_survey`]).
pub(super) fn survey_candidates<'a>(
    connections: &'a HashMap<String, ExecutorConnectionState>,
    unattached: &[EnrolledRemote],
    request: &CellRequest,
    now_unix_ms: u64,
    declined: &HashMap<String, queue::EntryDecline>,
    view: &AdmissionView,
) -> Result<CandidateSurvey<'a>, PlacementFailure> {
    let scope = PlacementScope::of(request);
    let targeted = scope.targeted();
    let immobile = request.pinned_executor_id.is_some()
        || (!targeted && !request.placement_mobility.may_spill());

    // Deterministic order in, deterministic rejections out: an operator reading
    // two decisions about the same fleet must not see the machines reshuffle.
    let mut ordered: Vec<_> = connections.values().collect();
    ordered.sort_by(|a, b| a.identity.executor_id.cmp(&b.identity.executor_id));
    let nests_elsewhere = nested_sandbox_preference(&ordered, scope, now_unix_ms);

    let mut usable = Vec::new();
    let mut rejected = Vec::new();
    for entry in ordered {
        let waived_needlessly = !nests_elsewhere.is_empty()
            && scope.selector.is_some_and(|selector| {
                nested_sandbox_fit(&entry.advertisement.capabilities, selector)
                    == NestedSandboxFit::Waivable
            });
        let reason = candidate_rejection(entry, scope, now_unix_ms)
            .or_else(|| {
                waived_needlessly.then(|| PlacementRejectionReason::NestedSandboxCapableElsewhere {
                    capable: nests_elsewhere.clone(),
                })
            })
            .or_else(|| {
                declined
                    .get(&entry.identity.executor_id)
                    .filter(|decline| decline.stands_on(entry.generation, now_unix_ms))
                    .map(declined_rejection)
            })
            .map(|reason| match reason {
                // The floor recovers by itself, and work tied to this machine
                // has nowhere else to go: refusing it protects nothing and
                // loses the verdict, so it is a hold to wait out.
                PlacementRejectionReason::PipeDegraded { assessment }
                    if immobile
                        && is_immobile_home(request, connections, &entry.identity.executor_id) =>
                {
                    PlacementRejectionReason::Held {
                        evidence: AdmissionHoldEvidence::FreshPipeFloor { assessment },
                    }
                }
                other => other,
            });
        match reason {
            Some(reason) => rejected.push(PlacementRejection {
                unreadable_memory: Vec::new(),
                drain_blockers: entry.drain_blockers.clone(),
                prediction: None,
                executor_name: executor_public_name(entry),
                executor_id: entry.identity.executor_id.clone(),
                reason,
            }),
            None => usable.push(entry),
        }
    }
    rejected.extend(
        unattached
            .iter()
            .filter(|machine| !connections.contains_key(&machine.executor_id))
            .map(|machine| PlacementRejection {
                unreadable_memory: Vec::new(),
                drain_blockers: machine.drain_state.blockers.clone(),
                prediction: None,
                executor_name: machine.name.clone(),
                executor_id: machine.executor_id.clone(),
                reason: detached_candidate_rejection(machine, request, now_unix_ms),
            }),
    );
    let declined_here = declined
        .iter()
        .map(|(executor_id, decline)| {
            (
                executor_id.clone(),
                PriorDecline {
                    reason: decline.reason,
                    declined_at_unix_ms: decline.at_unix_ms,
                },
            )
        })
        .collect();
    let mut journal: Vec<PlacementRejection> = declined
        .iter()
        .map(|(executor_id, decline)| PlacementRejection {
            unreadable_memory: Vec::new(),
            drain_blockers: Vec::new(),
            prediction: None,
            executor_name: connections
                .get(executor_id)
                .map(executor_public_name)
                .unwrap_or_else(|| executor_id.clone()),
            executor_id: executor_id.clone(),
            reason: declined_rejection(decline),
        })
        .collect();
    journal.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
    // A capable machine that has said "not yet" is something to wait for, so
    // neither immediate refusal below applies while one stands. Work tied to
    // one machine waits only on that machine's answer, and a home that said
    // it could never take the work is not a "not yet".
    let home_row = immobile
        .then(|| {
            rejected
                .iter()
                .find(|rejection| is_immobile_home(request, connections, &rejection.executor_id))
        })
        .flatten();
    let awaiting_decliner = match home_row {
        Some(home) => waits_on_machine(&home.reason) && !declined_for_good(&home.reason),
        None => rejected
            .iter()
            .any(|rejection| waits_on_machine(&rejection.reason)),
    };

    // A caller that named a machine or a platform is owed an immediate answer
    // naming what exists. An untargeted request with nothing usable is instead
    // waiting for its own executor to attach, which its horizon already bounds.
    if usable.is_empty() && targeted && !awaiting_decliner {
        let rejected = account_from_journal(rejected, &journal);
        return Err(PlacementFailure {
            diagnostic: no_matching_executor_diagnostic(
                connections,
                unattached,
                request,
                &rejected,
            ),
            rejected,
        });
    }
    // Waiting is the right answer for a mobile request that trust left with
    // nothing: a machine on a platform this verdict counts from may still
    // attach, and the horizon bounds how long that hope lasts. It is the wrong
    // answer for a request that cannot move at all, because its one possible
    // machine is attached right now and its platform will not change while the
    // horizon runs down. That combination is a contradiction between what the
    // check declared and the cadence it runs at, and saying so immediately is
    // the difference between a legible config error and a suite that silently
    // stalls out its horizon on every commit.
    if usable.is_empty() && !request.placement_mobility.may_spill() && !awaiting_decliner {
        // Only the machine the work is tied to can answer for it. Every other
        // row is a machine it was never going to use, and quoting the first of
        // those by id told a lane held on the runner's Mac that it could not
        // leave bbs (cairn/7220). A home with no row (not attached) is waited
        // for like any untargeted request's executor.
        if let Some(diagnostic) = rejected
            .iter()
            .find(|rejection| is_immobile_home(request, connections, &rejection.executor_id))
            .and_then(|home| immobile_home_refusal(request, home))
        {
            return Err(PlacementFailure {
                diagnostic,
                rejected,
            });
        }
    }
    let memory_budgets = connections
        .iter()
        .map(|(executor_id, entry)| {
            (
                executor_id.clone(),
                entry.advertisement.capabilities.memory_budget_bytes,
            )
        })
        .collect();
    Ok(CandidateSurvey {
        usable,
        rejected,
        view: view.clone(),
        declined_here,
        journal,
        memory_budgets,
    })
}

/// Whether `executor_id` is the one machine work that cannot move may run on:
/// its pin, or for untargeted work the machine holding the runner's checkout.
fn is_immobile_home(
    request: &CellRequest,
    connections: &HashMap<String, ExecutorConnectionState>,
    executor_id: &str,
) -> bool {
    match request.pinned_executor_id.as_deref() {
        Some(pinned) => pinned == executor_id,
        None => connections
            .get(executor_id)
            .is_some_and(|entry| entry.colocated),
    }
}

/// The immediate refusal owed to work that cannot leave `home`, when the
/// home's own reason is one time does not change: waiting would only run out
/// the horizon on it. `None` leaves the request to wait as it otherwise would.
fn immobile_home_refusal(request: &CellRequest, home: &PlacementRejection) -> Option<String> {
    let name = &home.executor_name;
    let reason = home.reason.describe();
    match &home.reason {
        PlacementRejectionReason::RoleExcluded { .. } => Some(format!(
            "this request cannot leave {name}, and {name} {reason}. Nothing was run. Its role is set by an operator on cairn://executors/{name}."
        )),
        PlacementRejectionReason::UntrustedVerdictPlatform { .. } => {
            Some(untrusted_verdict_platform_diagnostic(request, home))
        }
        other if is_preparation_rejection(other) => Some(format!(
            "this request cannot leave {name}, and {name} {reason}. Nothing was run. Read cairn://executors/{name} for what it advertises."
        )),
        other if declined_for_good(other) => Some(format!(
            "this request cannot leave {name}, and {name} said it could never take it: {reason}. Nothing was run."
        )),
        PlacementRejectionReason::GithubCredentialMissing { .. } => Some(format!(
            "this request cannot leave {name}, and {name} {reason}. Nothing was run. Read cairn://executors/{name} for its credentials."
        )),
        // Only an operator ends this drain, and only a new link a closed or
        // expired one; neither is a wait with an end.
        PlacementRejectionReason::Drained
        | PlacementRejectionReason::ConnectionClosed
        | PlacementRejectionReason::AbsenceExpired { .. } => Some(format!(
            "this request cannot leave {name}, and {name} {reason}. Nothing was run. Read cairn://executors/{name} for its state."
        )),
        _ => None,
    }
}

/// A machine's own decline that says it could never take the work, whatever
/// its load: not a "not yet" to wait on for work that cannot go elsewhere.
fn declined_for_good(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::Declined {
            reason: DeclineReason::Constraint,
            ..
        }
    )
}

/// Whether a rejection is something to wait for rather than a reason to
/// refuse: a capable machine's "not yet" (its own decline, or a posted
/// condition its admission would decline on), a drain the runner holds that
/// ends without anyone acting on the work (the self-update rollout's hold ends
/// when the machine returns on the runner's build), or room a model resident
/// holds, which placement evicts for the work once the resident is idle.
///
/// An operator's drain is not one. Only an operator ends it, so work it refuses
/// is told so at once rather than parked on a decision nobody is making.
pub(super) fn waits_on_machine(reason: &PlacementRejectionReason) -> bool {
    if let PlacementRejectionReason::TelemetryStale {
        age_ms,
        stale_after_ms,
        ..
    } = reason
    {
        // A stopped sampler may recover, but an indefinitely blind fleet is not
        // a wait with an end. Use the same recovery window as link absence.
        return age_ms.saturating_sub(*stale_after_ms) < ENVIRONMENT_LEASE_GRACE_MS;
    }
    matches!(
        reason,
        PlacementRejectionReason::Declined { .. }
            | PlacementRejectionReason::Held { .. }
            | PlacementRejectionReason::UpdatePending { .. }
            | PlacementRejectionReason::Reconnecting { .. }
            | PlacementRejectionReason::ModelResident { .. }
            | PlacementRejectionReason::ProvisioningTurn { .. }
            | PlacementRejectionReason::CadenceTurn { .. }
    )
}

/// Whether a machine is only away for now, so that the work may yet go there:
/// [`waits_on_machine`], or a drain. The set a refusal for size waits on.
fn away_for_now(reason: &PlacementRejectionReason) -> bool {
    waits_on_machine(reason) || matches!(reason, PlacementRejectionReason::Drained)
}

/// The refusal a machine's size makes whatever it is doing now: a single
/// process of the work larger than it can give ([`ProcessFit::Refused`]).
/// A waiver or a deferral is not one, and nothing resolved for the machine,
/// or an unread total, is unknown rather than a misfit.
fn hard_process_refusal(
    entry: &ExecutorConnectionState,
    reservation: Option<&resource_profiles::ResolvedResourceProfile>,
) -> Option<PlacementRejectionReason> {
    match process_fit(&reservation?.process_evidence, entry)? {
        ProcessFit::Refused(reason) => Some(reason),
        ProcessFit::Waived(_) | ProcessFit::Deferred(_) => None,
    }
}

/// Replace the reason of every machine the survey set aside as only away for
/// now with its hard single-process refusal, where its size makes one. A
/// machine with no attached connection (reconnecting, or never attached) has
/// nothing to judge and keeps its wait, as cairn/5602 bounds it.
fn judge_machines_away_for_now(
    rejected: &mut [PlacementRejection],
    connections: &HashMap<String, ExecutorConnectionState>,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
) {
    for rejection in rejected
        .iter_mut()
        .filter(|rejection| away_for_now(&rejection.reason))
    {
        let Some(entry) = connections.get(&rejection.executor_id) else {
            continue;
        };
        if let Some(resolved) = reservations.get(&rejection.executor_id) {
            rejection.unreadable_memory = resolved.rationale.unreadable_memory.clone();
        }
        if let Some(reason) = hard_process_refusal(entry, reservations.get(&rejection.executor_id))
        {
            rejection.reason = reason;
        }
    }
}

/// Why `entry` cannot take work needing `demand` of memory because of the
/// room its model residents hold, or `None`. A measured demand is weighed
/// against the machine's measured room. Work whose memory was never measured
/// (no reservation, an `Unmeasured` guess, or zero bytes) fails closed
/// against a loading model's reserved room (tenet 302) and runs beside a
/// healthy one; its refusal is marked `demand_unmeasured`, which is never
/// grounds for eviction, so a guess never displaces a model (tenet 1337). A
/// residency pin is exempt, as it is from a drain: work whose environment
/// lives here finishes here.
pub(super) fn model_room_rejection(
    entry: &ExecutorConnectionState,
    demand: Option<&ResourceReservation>,
    pinned_here: bool,
) -> Option<PlacementRejectionReason> {
    if pinned_here || entry.model_residents.is_empty() {
        return None;
    }
    let gpu_allocated = models::room::gpu_allocated_bytes(&entry.health.host);
    let Some(demand) = demand
        .filter(|demand| demand.source != ResourceReservationSource::Unmeasured)
        .map(|demand| demand.memory_bytes)
        .filter(|bytes| *bytes > 0)
    else {
        return models::room::unmeasured_misfit(&entry.model_residents, gpu_allocated);
    };
    let available = entry.health.machine.memory.value()?.available_bytes;
    models::room::resident_misfit(available, demand, &entry.model_residents, gpu_allocated)
}

/// Whether a rejection is the kernel's own verdict that this machine cannot
/// run the command. Nothing the machine posts will clear it, so the machine is
/// never offered the work; the request waits for the rest of the fleet instead
/// of being refused for want of a machine it was never going to get.
pub(super) fn excluded_by_memory_kill(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::MemoryKillExcluded { .. }
            | PlacementRejectionReason::ExceedsExecutorMemoryBound { .. }
            | PlacementRejectionReason::ProcessTooLarge(_)
    )
}

/// Whether this machine's memory brake exists: its memory-stall instrument
/// has measured, so admission can hold new work there. An executor that
/// predates the brake, or a platform without the kernel's pressure stall
/// information, never has.
pub(super) fn memory_brake_exists(entry: &ExecutorConnectionState) -> bool {
    entry
        .health
        .host
        .memory_admission
        .measured_at_unix_ms
        .is_some()
}

/// What the single-process fit concluded on one machine, when it concluded
/// anything but "fits".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ProcessFit {
    /// Not offered the work: a filter, never a clamp.
    Refused(PlacementRejectionReason),
    /// The fit would have refused, but one of its premises does not hold
    /// here; offered at its reservation, with the refusal on the record.
    Waived(cairn_common::executor_protocol::ProcessFitWaiver),
    /// The demand is a class prior, not a measurement of the work, and this
    /// machine cannot give one process that much: offered only when no
    /// machine that can is selectable.
    Deferred(cairn_common::executor_protocol::ProcessMisfit),
}

/// Whether this platform's kernel kills a process the machine cannot hold.
/// Linux's OOM killer does, and Windows fails the allocation, which ends the
/// compiler as surely. macOS has none: it compresses and swaps, so an
/// oversized process is slowed, never killed, and its size is a price
/// (tenets 1309, 1139), not a reason to withhold the machine.
pub(super) fn kernel_kills_for_memory(os: &str) -> bool {
    os != "macos"
}

/// Whether one process of this work is larger than this machine can give a
/// single process: its total memory less its standing use, or, from an
/// executor that posts no standing use, its reservation cap. Never `available`
/// at this instant, which says what is free now rather than what the machine
/// can hold. `None` when it fits, or when the machine's total is unread.
pub(super) fn process_fit(
    evidence: &resource_profiles::ProcessEvidence,
    entry: &ExecutorConnectionState,
) -> Option<ProcessFit> {
    judge_process_evidence(
        evidence,
        entry.health.machine.memory.value()?.total_bytes,
        entry.health.memory_record.standing_bytes,
        entry.advertisement.capabilities.memory_budget_bytes,
        executor_memory_bound(entry),
        &format!(
            "{}:{}",
            entry.identity.device_id, entry.identity.executor_id
        ),
        kernel_kills_for_memory(&entry.advertisement.capabilities.os),
    )
}

/// [`process_fit`] over the machine's numbers: its total, its standing use,
/// its memory budget, its profile context (`device:executor`), and whether
/// its kernel kills for memory.
///
/// The fit refuses only where both its premises hold: a single process was
/// measured (a named peak, or a kill's victim), and the kernel here would kill
/// it. A named demand on a platform that cannot kill, or an unnamed peak that
/// would have misfit anywhere, is waived with the refusal kept for the record.
/// A measurement borrowed from another platform refuses like one taken here.
/// A class prior defers instead: it is the project's largest rustc, not this
/// work's, so a machine too small for it is offered the work last rather than
/// never.
pub(super) fn judge_process_evidence(
    evidence: &resource_profiles::ProcessEvidence,
    total_bytes: u64,
    standing_bytes: Option<u64>,
    memory_budget_bytes: Option<u64>,
    bound_bytes: Option<u64>,
    here: &str,
    kernel_kills: bool,
) -> Option<ProcessFit> {
    use cairn_common::executor_protocol::{ProcessFitWaiver, ProcessFitWaiverReason};
    let judge = |demand: &resource_profiles::ProcessDemand| {
        judge_process_fit(
            demand,
            total_bytes,
            standing_bytes,
            memory_budget_bytes,
            bound_bytes,
            here,
        )
    };
    if let Some(misfit) = evidence.demand.as_ref().and_then(judge) {
        let prior = misfit.basis.is_class_prior();
        return Some(match (kernel_kills, prior) {
            (false, _) => ProcessFit::Waived(ProcessFitWaiver {
                misfit,
                reason: ProcessFitWaiverReason::NoMemoryKiller,
            }),
            (true, true) => ProcessFit::Deferred(misfit),
            (true, false) => ProcessFit::Refused(PlacementRejectionReason::ProcessTooLarge(misfit)),
        });
    }
    let misfit = evidence.unattributed.as_ref().and_then(judge)?;
    Some(ProcessFit::Waived(ProcessFitWaiver {
        misfit,
        reason: ProcessFitWaiverReason::UnattributedPeak,
    }))
}

/// One measured demand against the machine's numbers, whatever the platform:
/// the misfit the fit would name, or `None` when the process fits.
pub(super) fn judge_process_fit(
    demand: &resource_profiles::ProcessDemand,
    total_bytes: u64,
    standing_bytes: Option<u64>,
    memory_budget_bytes: Option<u64>,
    bound_bytes: Option<u64>,
    here: &str,
) -> Option<cairn_common::executor_protocol::ProcessMisfit> {
    let unbounded = single_process_capacity(total_bytes, standing_bytes, memory_budget_bytes, None);
    let capacity_bytes = single_process_capacity(
        total_bytes,
        standing_bytes,
        memory_budget_bytes,
        bound_bytes,
    );
    let too_large = demand.peak_bytes > 0 && demand.demand_bytes() > capacity_bytes;
    if !too_large {
        return None;
    }
    Some(cairn_common::executor_protocol::ProcessMisfit {
        process_bytes: demand.demand_bytes(),
        measured_bytes: (demand.peak_bytes > 0).then_some(demand.peak_bytes),
        process_name: demand.name.clone(),
        suite: demand.suite.clone(),
        measured_on: demand.measured_on.as_deref().map(|context| {
            context
                .rsplit_once(':')
                .map_or(context, |(_, executor)| executor)
                .to_string()
        }),
        measured_here: demand.measured_on.as_deref() == Some(here),
        capacity_bytes,
        total_bytes,
        standing_bytes,
        bound_bytes: bound_bytes.filter(|bound| *bound < unbounded),
        crowded_kills: demand
            .kills_here
            .as_ref()
            .and_then(|kills| kills.crowded.clone()),
        basis: demand.basis.clone(),
    })
}

/// What a machine can give one process: its total less its standing use,
/// or, from an executor that posts no standing use, its reservation cap; and
/// never more than the memory bound its executor's service unit enforces,
/// past which the kernel kills a cell's process whatever the machine has
/// free (cairn/6994). A 15.4 GiB machine whose executor is bound at 11.25 GiB
/// cannot finish a 21.6 GiB rustc, and saying 13 GiB would admit one that
/// cannot fit there and is killed.
pub(super) fn single_process_capacity(
    total_bytes: u64,
    standing_bytes: Option<u64>,
    memory_budget_bytes: Option<u64>,
    bound_bytes: Option<u64>,
) -> u64 {
    let room = match (standing_bytes, memory_budget_bytes) {
        (Some(standing), _) => total_bytes.saturating_sub(standing),
        (None, Some(budget)) => resource_profiles::memory_reservation_cap(budget),
        (None, None) => total_bytes,
    };
    bound_bytes.map_or(room, |bound| room.min(bound))
}

/// The memory bound a connected machine's executor unit enforces, when it
/// reported one.
pub(super) fn executor_memory_bound(entry: &ExecutorConnectionState) -> Option<u64> {
    entry
        .advertisement
        .capabilities
        .service_bounds
        .and_then(|bounds| bounds.memory_max_bytes)
}

/// A planning ceiling is a preference; the unit's enforced bound is not.
/// Ask the shared solver for the minimum admissible charge, not the wide modeled charge.
fn executor_bound_rejection(
    entry: &ExecutorConnectionState,
    demand: &ResourceReservation,
) -> Option<PlacementRejectionReason> {
    let bound_bytes = executor_memory_bound(entry)?;
    let shortfall = demand.solve_memory_grant(1, bound_bytes, 0).err()?;
    Some(PlacementRejectionReason::ExceedsExecutorMemoryBound {
        charge_bytes: shortfall.minimum_charge_bytes,
        bound_bytes,
        width_measured: shortfall.width_measured,
    })
}

/// [`single_process_capacity`] for a connected machine, `None` while its
/// total is unread.
pub(super) fn entry_process_capacity(entry: &ExecutorConnectionState) -> Option<u64> {
    Some(single_process_capacity(
        entry.health.machine.memory.value()?.total_bytes,
        entry.health.memory_record.standing_bytes,
        entry.advertisement.capabilities.memory_budget_bytes,
        executor_memory_bound(entry),
    ))
}

/// The single process `entry` was judged on, for the placement record: the
/// named demand and where it came from, or, for a Rust compile nothing has
/// measured, that it was placed unmeasured. `None` for work that compiles
/// nothing and measured no process.
pub(super) fn process_demand_record(
    evidence: &resource_profiles::ProcessEvidence,
    entry: &ExecutorConnectionState,
) -> Option<cairn_common::executor_protocol::ProcessDemandRecord> {
    use cairn_common::executor_protocol::{ProcessDemandEstimate, ProcessDemandRecord};
    let capacity_bytes = entry_process_capacity(entry);
    match evidence
        .demand
        .as_ref()
        .filter(|demand| demand.peak_bytes > 0)
    {
        Some(demand) => Some(ProcessDemandRecord::Estimated(ProcessDemandEstimate {
            process_bytes: demand.demand_bytes(),
            measured_bytes: demand.peak_bytes,
            process_name: demand.name.clone(),
            suite: demand.suite.clone(),
            measured_on: demand.measured_on.as_deref().map(|context| {
                context
                    .rsplit_once(':')
                    .map_or(context, |(_, executor)| executor)
                    .to_string()
            }),
            measured_here: demand.measured_on.as_deref()
                == Some(
                    format!(
                        "{}:{}",
                        entry.identity.device_id, entry.identity.executor_id
                    )
                    .as_str(),
                ),
            basis: demand.basis.clone(),
            capacity_bytes,
        })),
        None => evidence
            .unmeasured_compile
            .then_some(ProcessDemandRecord::Unmeasured { capacity_bytes }),
    }
}

/// The machines a pass passed over because the work cannot run there, each
/// with its reason, or `None` when memory kept the work off no machine.
/// What main's cadence lane for an unobserved tip is told about the machines
/// whose freed room it takes first, by public name.
pub(crate) fn describe_room_taken_first(machines: &[String]) -> String {
    format!(
        "main cadence, tip unobserved: takes the next freed room on {}",
        machines.join(", ")
    )
}

/// The machines this pass passed over behind main's cadence lane, one
/// sentence each, or `None` when it passed none over.
pub(super) fn describe_cadence_turns(rejected: &[PlacementRejection]) -> Option<String> {
    let rows: Vec<String> = rejected
        .iter()
        .filter_map(|rejection| match &rejection.reason {
            PlacementRejectionReason::CadenceTurn {
                check, platform, ..
            } => Some(format!(
                "{}: behind main's cadence lane for {check} on {platform}",
                rejection.executor_name
            )),
            _ => None,
        })
        .collect();
    (!rows.is_empty()).then(|| rows.join("; "))
}

pub(super) fn describe_memory_rejections(rejected: &[PlacementRejection]) -> Option<String> {
    let rows: Vec<String> = rejected
        .iter()
        .filter(|rejection| {
            matches!(
                rejection.reason,
                PlacementRejectionReason::MemoryKillExcluded { .. }
                    | PlacementRejectionReason::ProcessTooLarge(_)
                    | PlacementRejectionReason::ExceedsMemoryCeiling { .. }
            )
        })
        .map(|rejection| {
            format!(
                "{}: {}",
                rejection.executor_name,
                rejection.reason.describe()
            )
        })
        .collect();
    (!rows.is_empty()).then(|| rows.join("; "))
}

/// Each drained machine's owned refusals, by public name, with every blocker's
/// owner, generation, reason and estimate, or `None` when no row is drained.
pub(super) fn describe_drain_blockers(rejected: &[PlacementRejection]) -> Option<String> {
    let rows: Vec<String> = rejected
        .iter()
        .filter(|row| {
            !row.drain_blockers.is_empty()
                || matches!(row.reason, PlacementRejectionReason::Drained)
        })
        .map(|row| {
            let blockers = row
                .drain_blockers
                .iter()
                .map(|blocker| {
                    format!(
                        "{} generation {}: {}{}",
                        blocker.owner,
                        blocker.generation,
                        blocker.reason,
                        blocker
                            .estimate
                            .as_ref()
                            .map(|estimate| format!("; estimate {estimate}"))
                            .unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}: {}", row.executor_name, blockers)
        })
        .collect();
    (!rows.is_empty()).then(|| rows.join("; "))
}

/// The record of a pass that placed work nowhere for a reason that ends on its
/// own (machines it cannot run on for memory, a machine being emptied for its
/// executor update, or machines whose freed room main's cadence lane takes
/// first), so the queue view and each named machine's page show why the
/// request is pending. `updates` describes each update it waits on;
/// `takes_room_on` names the machines whose next freed room this request
/// takes first, because it is that cadence lane.
pub(super) fn wait_decision(
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    rejected: &[PlacementRejection],
    updates: &[String],
    takes_room_on: &[String],
) -> Option<PlacementDecision> {
    let precedence = (!takes_room_on.is_empty())
        .then(|| format!("{}.", describe_room_taken_first(takes_room_on)));
    let turns = describe_cadence_turns(rejected).map(|turns| format!("{turns}."));
    let memory = describe_memory_rejections(rejected)
        .map(|memory| format!("not placed where it cannot run: {memory}."));
    let waiting_on_update =
        (!updates.is_empty()).then(|| format!("waiting on {}.", updates.join("; ")));
    // A self-update drain's record and its decline account say less than the
    // update sentence already does, so a row it covers is told once.
    let told_by_update = |rejection: &&PlacementRejection| {
        waiting_on_update.is_some()
            && matches!(
                rejection.reason,
                PlacementRejectionReason::UpdatePending { .. }
            )
    };
    let others: Vec<PlacementRejection> = rejected
        .iter()
        .filter(|rejection| !told_by_update(rejection))
        .cloned()
        .collect();
    let drains = describe_drain_blockers(&others)
        .map(|drains| format!("held from new admission: {drains}."));
    let reasons: Vec<String> = [precedence, turns, memory, drains, waiting_on_update]
        .into_iter()
        .flatten()
        .collect();
    let declines = describe_declines(&others);
    if reasons.is_empty() && declines.is_empty() {
        return None;
    }
    Some(placement_decision(
        request,
        unix_time_ms(),
        Some(policy_evidence(request, policy, false)),
        None,
        PlacementOutcome::Waiting {
            diagnostic: format!("{}{}", reasons.join(" "), declines)
                .trim_start()
                .to_string(),
        },
        rejected.to_vec(),
    ))
}

/// Logged placement rejections, keyed by request and machine.
pub(super) type PlacementRejectionLog = crate::repeat_report::KeyedRepeatReporter<(String, String)>;

/// How long an unchanged rejection goes between log lines.
pub(super) const PLACEMENT_REJECTION_REPORT_INTERVAL: Duration = Duration::from_secs(60);

impl Default for crate::repeat_report::KeyedRepeatReporter<(String, String)> {
    fn default() -> Self {
        Self::new(PLACEMENT_REJECTION_REPORT_INTERVAL)
    }
}

/// What makes two rejections of one machine for one request "the same" for
/// the log: the reason's kind, and for a decline the machine's own reason.
/// The figures inside (silence, free bytes, ages) move on every pass and are
/// not part of it.
pub(super) fn placement_rejection_signature(reason: &PlacementRejectionReason) -> String {
    let kind = match reason {
        PlacementRejectionReason::Reconnecting { returns_by_unix_ms } => {
            return format!("reconnecting:{}", returns_by_unix_ms / 60_000)
        }
        PlacementRejectionReason::AbsenceExpired { .. } => "absenceExpired",
        PlacementRejectionReason::ConnectionClosed => "connectionClosed",
        PlacementRejectionReason::ConnectionStale { .. } => "connectionStale",
        PlacementRejectionReason::RunnerNotListening { .. } => "runnerNotListening",
        PlacementRejectionReason::SelectorMismatch { .. } => "selectorMismatch",
        PlacementRejectionReason::PinMismatch { .. } => "pinMismatch",
        PlacementRejectionReason::ProjectUnavailable { .. } => "projectUnavailable",
        PlacementRejectionReason::NotColocated => "notColocated",
        PlacementRejectionReason::RepositoryNotTransferable { .. } => "repositoryNotTransferable",
        PlacementRejectionReason::TelemetryGap { measurement, .. } => {
            return format!("telemetryGap:{}", measurement.as_str())
        }
        PlacementRejectionReason::TelemetryStale { measurement, .. } => {
            return format!("telemetryStale:{}", measurement.as_str())
        }
        PlacementRejectionReason::InsufficientMemory { .. } => "insufficientMemory",
        PlacementRejectionReason::InsufficientVolume { .. } => "insufficientVolume",
        PlacementRejectionReason::ExceedsMemoryCeiling { .. } => "exceedsMemoryCeiling",
        PlacementRejectionReason::ExceedsExecutorMemoryBound { .. } => "exceedsExecutorMemoryBound",
        PlacementRejectionReason::MemoryKillExcluded { .. } => "memoryKillExcluded",
        PlacementRejectionReason::ProcessTooLarge(_) => "processTooLarge",
        PlacementRejectionReason::ProcessDeferred(_) => "processDeferred",
        PlacementRejectionReason::OutrankedBy { .. } => "outrankedBy",
        PlacementRejectionReason::NestedSandboxCapableElsewhere { .. } => {
            "nestedSandboxCapableElsewhere"
        }
        PlacementRejectionReason::UntrustedVerdictPlatform { .. } => "untrustedVerdictPlatform",
        PlacementRejectionReason::Drained => "drained",
        // The bound's minute is part of the verdict: a bound that moves is
        // news to a parked request, so its wait is recorded afresh.
        PlacementRejectionReason::UpdatePending { returns_by_unix_ms } => {
            return match returns_by_unix_ms {
                Some(at) => format!("updatePending:{}", at / 60_000),
                None => "updatePending".to_string(),
            }
        }
        PlacementRejectionReason::ModelResident { .. } => "modelResident",
        PlacementRejectionReason::BuildVolumeAbsent { .. } => "buildVolumeAbsent",
        PlacementRejectionReason::PipeDegraded { .. } => "pipeDegraded",
        PlacementRejectionReason::RoleExcluded { .. } => "roleExcluded",
        PlacementRejectionReason::AvoidedInfrastructure => "avoidedInfrastructure",
        PlacementRejectionReason::Declined { reason, .. } => return format!("declined:{reason:?}"),
        PlacementRejectionReason::Held { evidence } => {
            return format!("held:{}", evidence.as_str())
        }
        PlacementRejectionReason::ProvisioningTurn { .. } => "provisioningTurn",
        PlacementRejectionReason::CadenceTurn { .. } => "cadenceTurn",
        PlacementRejectionReason::PreparationToolchainMissing { .. } => {
            "preparationToolchainMissing"
        }
        PlacementRejectionReason::PreparationRefused { .. } => "preparationRefused",
        PlacementRejectionReason::BuildPrerequisitesMissing { .. } => "buildPrerequisitesMissing",
        PlacementRejectionReason::GithubCredentialMissing { .. } => "githubCredentialMissing",
        PlacementRejectionReason::NotPublishEligible { .. } => "notPublishEligible",
    };
    kind.to_string()
}

/// How loudly the placement sink logs one rejection.
///
/// A `Declined` row restates a machine's answer that the decline handler has
/// already logged, at INFO the first time and DEBUG after. A parked request
/// is re-placed on every posted state, and each placement records every
/// standing decline again, so logging those rows at WARN would repeat the
/// same sentence every few seconds for as long as a hold lasts. The
/// placement record and the queue journal keep them either way. A `Held` row
/// is the same answer read from posted state before it was asked, restated on
/// every pass for as long as the condition lasts.
pub(super) fn placement_rejection_log_level(rejection: &PlacementRejection) -> log::Level {
    match rejection.reason {
        PlacementRejectionReason::Declined { .. }
        | PlacementRejectionReason::Held { .. }
        | PlacementRejectionReason::ProvisioningTurn { .. }
        | PlacementRejectionReason::CadenceTurn { .. } => log::Level::Debug,
        // A standing fact about the machine and the project, restated on every
        // placement for as long as it holds; the refusal itself was logged
        // at WARN when it was recorded, and the placement record keeps it.
        PlacementRejectionReason::PreparationToolchainMissing { .. }
        | PlacementRejectionReason::PreparationRefused { .. }
        | PlacementRejectionReason::BuildPrerequisitesMissing { .. }
        | PlacementRejectionReason::GithubCredentialMissing { .. }
        | PlacementRejectionReason::NotPublishEligible { .. } => log::Level::Info,
        _ => log::Level::Warn,
    }
}

/// A recorded decline as the placement rejection every surface renders.
pub(super) fn declined_rejection(decline: &queue::EntryDecline) -> PlacementRejectionReason {
    PlacementRejectionReason::Declined {
        reason: decline.reason,
        detail: decline.detail.clone(),
        declined_at_unix_ms: decline.at_unix_ms,
    }
}

/// Whether a rejection is the runner's own reading of a machine at the moment
/// a refusal is composed — its link's silence, its telemetry's age or absence,
/// a fit judged from those readings, a hold read off posted state — or an
/// earlier decline. Any of these yields to the last decline the machine
/// actually answered for this request. A structural reason (role, drain,
/// selector, project, preparation) is a fact about the machine and the work
/// that no answer changes, and it stands.
pub(super) fn answered_by_journal(reason: &PlacementRejectionReason) -> bool {
    matches!(
        reason,
        PlacementRejectionReason::Declined { .. }
            | PlacementRejectionReason::Held { .. }
            | PlacementRejectionReason::ConnectionStale { .. }
            | PlacementRejectionReason::RunnerNotListening { .. }
            | PlacementRejectionReason::TelemetryGap { .. }
            | PlacementRejectionReason::TelemetryStale { .. }
            | PlacementRejectionReason::InsufficientMemory { .. }
            | PlacementRejectionReason::InsufficientVolume { .. }
    )
}

/// A refusal's per-machine rows with each machine's own last answer leading.
///
/// `journal` is the request's declines, one per machine, superseded ones
/// included: a machine that declined, posted state, and then went quiet is
/// still the machine that declined. Without this a refusal names whatever the
/// admission view reads when it is composed (a link silent for two minutes, a
/// CPU sample past its bound) while the placement journal for the same request
/// says the machine declined on disk, and a reader chases a runner restart
/// that never happened. A machine the journal names that the rows do not is
/// appended, so a decliner that has since detached is still accounted for.
pub(super) fn account_from_journal(
    mut rejected: Vec<PlacementRejection>,
    journal: &[PlacementRejection],
) -> Vec<PlacementRejection> {
    for decline in journal {
        match rejected
            .iter_mut()
            .find(|row| row.executor_id == decline.executor_id)
        {
            Some(row) if answered_by_journal(&row.reason) => row.reason = decline.reason.clone(),
            Some(_) => {}
            None => rejected.push(decline.clone()),
        }
    }
    rejected
}

/// The decline rows of a refusal as one trailing sentence, or nothing.
///
/// Leads with a space so a caller can splice it between two sentences.
pub(super) fn describe_declines(rejected: &[PlacementRejection]) -> String {
    let rows: Vec<String> = rejected
        .iter()
        .filter(|rejection| waits_on_machine(&rejection.reason))
        .map(|rejection| {
            format!(
                "{} {}",
                rejection.executor_name,
                match &rejection.reason {
                    PlacementRejectionReason::TelemetryStale {
                        age_ms,
                        stale_after_ms,
                        ..
                    } => {
                        let returns_by = unix_time_ms()
                            .saturating_sub(*age_ms)
                            .saturating_add(*stale_after_ms)
                            .saturating_add(ENVIRONMENT_LEASE_GRACE_MS);
                        format!(
                            "{}; waiting for fresh telemetry by {}",
                            rejection.reason.describe(),
                            cairn_common::executor_protocol::describe_clock(returns_by),
                        )
                    }
                    _ => rejection.reason.describe(),
                }
            )
        })
        .collect();
    if rows.is_empty() {
        String::new()
    } else {
        format!(" Waited on: {}.", rows.join("; "))
    }
}

/// The refusal for work that can only run on the machine holding the runner's
/// checkout, whose platform is not one this verdict counts from.
pub(super) fn untrusted_verdict_platform_diagnostic(
    request: &CellRequest,
    rejection: &PlacementRejection,
) -> String {
    format!(
        "this request cannot leave {} ({}), and its verdict counts only from {}. Nothing was run. Either widen the platforms this verdict counts from or move the work to a cadence that can be placed elsewhere.",
        rejection.executor_name,
        rejection.reason.describe(),
        request.verdict_platforms.join(", ")
    )
}

#[derive(Clone, Copy)]
pub(super) struct SurveyRankingContext<'a> {
    pub(super) reservations: &'a HashMap<String, resource_profiles::ResolvedResourceProfile>,
    pub(super) predictions: &'a HashMap<String, PlacementPrediction>,
    pub(super) sync_costs: &'a HashMap<String, SyncCost>,
    pub(super) cache_distances: &'a HashMap<String, cairn_common::cache_distance::CacheDistance>,
    pub(super) policy: &'a ActivePlacementPolicy,
    pub(super) now_unix_ms: u64,
    pub(super) exploration: &'a ExplorationState,
    /// Set when the request is the live answer to a main-cadence question.
    pub(super) cadence_lane: Option<&'a CadenceLaneRestart>,
}

/// The main-cadence lane a request answers, and the restart bound it is
/// weighed against: the request-level half of [`LaneRestartExposure`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CadenceLaneRestart {
    pub(super) check: String,
    pub(super) platform: String,
    pub(super) bound_ms: u64,
}

/// A main-cadence lane expected to run longer than the runner stays up.
///
/// A runner restart ends every lane on the colocated executor (tenet 316) and
/// a remote lane is adopted across it (cairn/6581), so for such a lane the
/// colocated executor ranks behind every adoptable host that fits the work
/// ([`rank_candidates`]). Computed once per survey from the request and the
/// candidates' learned durations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LaneRestartExposure {
    pub(super) check: String,
    pub(super) platform: String,
    pub(super) expected_run_ms: u64,
    pub(super) bound_ms: u64,
}

/// The lane's exposure, when its learned run exceeds the restart bound. The
/// run is the colocated executor's when it has learned one, since that is the
/// run a restart would end; otherwise the longest any candidate learned. With
/// nothing learned there is no evidence the lane is long, and nothing changes.
pub(super) fn lane_restart_exposure(
    scored: &[ScoredCandidate<'_>],
    lane: &CadenceLaneRestart,
) -> Option<LaneRestartExposure> {
    let learned = |candidate: &ScoredCandidate<'_>| {
        candidate
            .prediction
            .run
            .is_learned()
            .then_some(candidate.prediction.run.predicted_ms)
    };
    let expected_run_ms = scored
        .iter()
        .filter(|candidate| candidate.entry.colocated)
        .find_map(learned)
        .or_else(|| scored.iter().filter_map(learned).max())?;
    (expected_run_ms > lane.bound_ms).then(|| LaneRestartExposure {
        check: lane.check.clone(),
        platform: lane.platform.clone(),
        expected_run_ms,
        bound_ms: lane.bound_ms,
    })
}

/// Whether a rejection says something about this machine taking this lane.
/// A machine of another platform, outside the pin, or not serving the project
/// was never a host for it, and naming it would only bury the ones that were.
pub(super) fn bears_on_the_lane(reason: &PlacementRejectionReason) -> bool {
    !matches!(
        reason,
        PlacementRejectionReason::SelectorMismatch { .. }
            | PlacementRejectionReason::PinMismatch { .. }
            | PlacementRejectionReason::ProjectUnavailable { .. }
            | PlacementRejectionReason::UntrustedVerdictPlatform { .. }
    )
}

/// Rank what survived the survey, on the estimates and readings each machine
/// carries.
pub(super) fn rank_survey(
    request: &CellRequest,
    survey: CandidateSurvey<'_>,
    context: SurveyRankingContext<'_>,
) -> Result<PlacementDraft, PlacementFailure> {
    let SurveyRankingContext {
        reservations,
        predictions,
        sync_costs,
        cache_distances,
        policy,
        now_unix_ms,
        exploration,
        cadence_lane,
    } = context;
    let CandidateSurvey {
        usable,
        mut rejected,
        view,
        declined_here,
        journal,
        memory_budgets,
    } = survey;
    // Each machine's posted state is read against the demand the offer to it
    // will name: the reservation resolved for that machine, which an ordinary
    // submission's declared zero growth is not. A hold nothing running will
    // clear (a latched cleanup, a disk under its floor with no sweep in
    // flight) keeps work that is free to go elsewhere off the machine, and is
    // waited for like a decline; work that cannot leave it is still offered
    // and gets the executor's own decline. A hold that relieves itself leaves
    // the machine ranked behind every machine that is not held.
    let free_to_move =
        request.placement_mobility.may_spill() && !placement_settled_by_caller(request);
    let mut holds = HashMap::new();
    let usable: Vec<_> = usable
        .into_iter()
        .filter(|entry| {
            let executor_id = &entry.identity.executor_id;
            let resolved = reservations.get(executor_id).map(|resolved| CellRequest {
                resource_reservation: resolved.reservation.clone(),
                ..request.clone()
            });
            let Some(hold) = decline_condition::posted_hold(
                &view,
                resolved.as_ref().unwrap_or(request),
                decline_condition::PostedState {
                    snapshot: &entry.snapshot,
                    health: &entry.health,
                    colocated: entry.colocated,
                },
                now_unix_ms,
            ) else {
                return true;
            };
            if free_to_move && !hold.relieves_itself {
                // A machine too small for one process of the work says that
                // rather than its hold: no wait on the hold makes it fit.
                let reason = hard_process_refusal(entry, reservations.get(executor_id)).unwrap_or(
                    PlacementRejectionReason::Held {
                        evidence: hold.evidence,
                    },
                );
                rejected.push(PlacementRejection {
                    unreadable_memory: Vec::new(),
                    drain_blockers: entry.drain_blockers.clone(),
                    prediction: None,
                    executor_name: executor_public_name(entry),
                    executor_id: executor_id.clone(),
                    reason,
                });
                return false;
            }
            holds.insert(executor_id.clone(), hold);
            true
        })
        .collect();
    if usable.is_empty() {
        // Nothing is left to rank, but the machines set aside may already
        // have answered: each one judged too small for one process, with
        // none only away for now, is the same refusal ranking would give.
        let too_small_somewhere = rejected.iter().any(|rejection| {
            matches!(
                rejection.reason,
                PlacementRejectionReason::ProcessTooLarge(_)
            )
        });
        if too_small_somewhere
            && !rejected
                .iter()
                .any(|rejection| away_for_now(&rejection.reason))
        {
            return Err(PlacementFailure {
                diagnostic: format!(
                    "no machine can run this work: {}. Nothing was run; this needs a machine with more memory for one process.",
                    describe_memory_rejections(&rejected).unwrap_or_default()
                ),
                rejected,
            });
        }
        return Ok(PlacementDraft {
            selected: None,
            rejected,
            policy: policy_evidence(request, policy, false),
        });
    }
    let targeted = request.pinned_executor_id.is_some()
        || request
            .executor
            .as_ref()
            .is_some_and(|selector| !selector.is_empty());
    let pinned = request.pinned_executor_id.as_deref();
    let settled = placement_settled_by_caller(request);
    let exercising_spill = request.placement_mobility.may_spill() && !settled;

    let only_candidate = usable.len() == 1;
    let scored: Vec<_> = usable
        .into_iter()
        .map(|entry| {
            let sync_cost = sync_costs
                .get(&entry.identity.executor_id)
                .copied()
                .unwrap_or(SyncCost::Unknown);
            let mut candidate = ScoredCandidate::new(
                entry,
                sync_cost,
                reservations.get(&entry.identity.executor_id),
                &request.resource_reservation,
                predictions
                    .get(&entry.identity.executor_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        // Every usable candidate is predicted for above; this is
                        // the shape of an entry that was surveyed and somehow not
                        // predicted, and it says so rather than scoring zero.
                        unpredicted_candidate(entry, request, sync_cost)
                    }),
                holds.get(&entry.identity.executor_id),
                declined_here.get(&entry.identity.executor_id).copied(),
                now_unix_ms,
            );
            candidate.cache_distance = cache_distances.get(&entry.identity.executor_id).copied();
            if candidate.blindness.is_none() {
                // The same demand the memory fit weighs: the reservation
                // resolved for this machine, else the one the request states.
                candidate.model_room = model_room_rejection(
                    entry,
                    Some(
                        candidate
                            .reservation
                            .map_or(&request.resource_reservation, |resolved| {
                                &resolved.reservation
                            }),
                    ),
                    pinned == Some(entry.identity.executor_id.as_str()),
                );
            }
            candidate.scratch_sync_ms = request
                .scratch_sync
                .as_ref()
                .map_or(0, |estimate| estimate.ms_for(&entry.identity.executor_id));
            candidate.home_affinity_ms = request.home_affinity.as_ref().map_or(0, |affinity| {
                affinity.price_ms(
                    &entry.identity.executor_id,
                    &entry.advertisement.capabilities.os,
                )
            });
            candidate
        })
        .collect();

    let restart_exposure = cadence_lane.and_then(|lane| lane_restart_exposure(&scored, lane));
    let (winner, ranked, changed_earliest_winner, bootstrap_exploration, tie_break) =
        rank_candidates(
            &scored,
            settled,
            exercising_spill,
            request,
            policy,
            exploration,
            restart_exposure.as_ref(),
        );
    let Some(winner) = winner else {
        // Every machine placement could score is too small for one process of
        // this work: a fact about their size that no wait changes.
        let all_too_small = !ranked.is_empty()
            && ranked.iter().all(|candidate| {
                matches!(
                    candidate.excluded,
                    Some(PlacementRejectionReason::ProcessTooLarge(_))
                )
            });
        rejected.extend(ranked.into_iter().map(|candidate| candidate.rejection()));
        // Refused at once, unless some machine is only away for now (held,
        // declining, updating, or drained) and may yet be the one that fits.
        let something_to_wait_for = rejected
            .iter()
            .any(|rejection| away_for_now(&rejection.reason));
        if all_too_small && !something_to_wait_for {
            return Err(PlacementFailure {
                diagnostic: format!(
                    "no machine can run this work: {}. Nothing was run; this needs a machine with more memory for one process.",
                    describe_memory_rejections(&rejected).unwrap_or_default()
                ),
                rejected,
            });
        }
        // A machine whose executor bound cannot hold the work's minimum charge,
        // or that cannot give one of its processes enough, is never chosen, so
        // when it is all there is the work waits for another machine, which is
        // the whole of the remedy: placing it there repeats the death. Work the
        // caller settled on that machine cannot wait for another, so it is
        // refused at once with the kill named.
        if let Some(excluded) = rejected
            .iter()
            .find(|rejection| excluded_by_memory_kill(&rejection.reason))
        {
            if settled {
                return Err(PlacementFailure {
                    diagnostic: format!(
                        "{} cannot run this work: {}. Nothing was run; run it without the pin or selector that holds it there.",
                        excluded.executor_name,
                        excluded.reason.describe()
                    ),
                    rejected,
                });
            }
            return Ok(PlacementDraft {
                selected: None,
                rejected,
                policy: policy_evidence(request, policy, false),
            });
        }
        // A machine that will take work once its condition or its hold ends
        // is something to wait for, so the blind rest are not a reason to
        // refuse.
        if rejected
            .iter()
            .any(|rejection| waits_on_machine(&rejection.reason))
        {
            return Ok(PlacementDraft {
                selected: None,
                rejected,
                policy: policy_evidence(request, policy, false),
            });
        }
        // Every usable machine was measured-blind and none of them was a
        // machine policy is allowed to spill onto. Say so with the evidence,
        // and where a machine answered this request, with its answer.
        let rejected = account_from_journal(rejected, &journal);
        return Err(PlacementFailure {
            diagnostic: no_measurable_executor_diagnostic(request, &rejected),
            rejected,
        });
    };

    // The clamp is for a fleet with nowhere bigger to go. A winner over its
    // memory ceiling means every selectable machine is too small, and while a
    // capable machine has only said "not yet" (a decline, a posted hold) the
    // work can still go somewhere that holds it: placing it at the clamp now
    // trades a wait for an out-of-memory kill hours in. So work free to move
    // waits on the decliner instead, as it would if nothing else were usable.
    // Work the caller settled stays at the clamp, as it always has.
    //
    // Only a decliner that could hold it is worth the wait. One whose budget
    // is under the demand too ends at the clamp as surely as the winner does,
    // so waiting on it trades the run for nothing, and while the learned peak
    // stays above every eligible ceiling it is never re-measured: the probe
    // at the clamp, run at the width the machine's memory grants, is what
    // brings the peak back to what this fleet can actually run (cairn/5731).
    // The demand is the winner's measurement, the only one this pass holds;
    // a decliner with no stated budget may be larger and is still waited for.
    let winner_ceiling = winner
        .reservation
        .and_then(|resolved| resolved.memory_fit.ceiling());
    let could_hold = |rejection: &PlacementRejection| {
        let Some(ceiling) = winner_ceiling else {
            return true;
        };
        let budget = memory_budgets
            .get(&rejection.executor_id)
            .copied()
            .flatten();
        !matches!(
            resource_profiles::MemoryFit::of(Some(ceiling.estimate_bytes), budget),
            resource_profiles::MemoryFit::Exceeds(_)
        )
    };
    if winner.over_ceiling.is_some()
        && exercising_spill
        && rejected
            .iter()
            .any(|rejection| waits_on_machine(&rejection.reason) && could_hold(rejection))
    {
        rejected.extend(ranked.into_iter().map(|candidate| candidate.rejection()));
        return Ok(PlacementDraft {
            selected: None,
            rejected,
            policy: policy_evidence(request, policy, false),
        });
    }
    // The same trade for a machine too small for the class prior: while a
    // machine whose cap holds the prior has only said "not yet", the work
    // waits for it rather than being the small machine's first measurement.
    if let Some(PlacementRejectionReason::ProcessDeferred(misfit)) = &winner.deferred {
        let holds_prior = |rejection: &PlacementRejection| {
            memory_budgets
                .get(&rejection.executor_id)
                .copied()
                .flatten()
                .is_none_or(|budget| {
                    resource_profiles::memory_reservation_cap(budget) >= misfit.process_bytes
                })
        };
        if exercising_spill
            && rejected
                .iter()
                .any(|rejection| waits_on_machine(&rejection.reason) && holds_prior(rejection))
        {
            rejected.extend(ranked.into_iter().map(|candidate| candidate.rejection()));
            return Ok(PlacementDraft {
                selected: None,
                rejected,
                policy: policy_evidence(request, policy, false),
            });
        }
    }

    // Blindness is named ahead of `OnlyCandidate` deliberately. A lone
    // measured-blind home winning by default is precisely the local-degradation
    // case this record exists to expose, and "it was the only one" would describe
    // it as a choice that was made rather than one that was impossible. When the
    // caller settled placement there was nothing to measure for, so blindness is
    // not the story and the ordinary reasons apply.
    let reason = if pinned.is_some() {
        PlacementReason::Pinned
    } else if winner.blindness.is_some() && !settled {
        PlacementReason::MeasuredBlindFleet
    } else if bootstrap_exploration {
        PlacementReason::BootstrapExploration
    } else if only_candidate {
        PlacementReason::OnlyCandidate
    } else if !targeted && !request.placement_mobility.may_spill() {
        PlacementReason::ColocatedHome
    } else {
        PlacementReason::PredictedEarliestVerdict
    };
    let winner_name = executor_public_name(winner.entry);
    let restart_exposure = restart_exposure.and_then(|exposure| {
        restart_exposure_record(exposure, winner, &winner_name, &ranked, &rejected)
    });
    rejected.extend(
        ranked
            .into_iter()
            .filter(|candidate| {
                candidate.entry.identity.executor_id != winner.entry.identity.executor_id
            })
            .map(|candidate| candidate.outranked_rejection(&winner_name)),
    );

    let selection = PlacementSelection {
        executor_name: winner_name,
        executor_id: winner.entry.identity.executor_id.clone(),
        colocated: winner.entry.colocated,
        reason,
        tie_break,
        readings: placement_readings(&winner.entry.health.machine),
        reservation: winner
            .reservation
            .map(|resolved| resolved.reservation.clone())
            .unwrap_or_else(|| request.resource_reservation.clone()),
        reservation_rationale: winner
            .reservation
            .map(|resolved| resolved.rationale.clone())
            .unwrap_or_else(|| unresolved_rationale(&request.resource_reservation)),
        sync_cost: match winner.sync_cost {
            SyncCost::Known(bytes) => PlacementSyncCost::Known { bytes },
            SyncCost::Unknown => PlacementSyncCost::Unknown,
        },
        object_transfer: None,
        observation_reuse: if winner.entry.colocated {
            ObservationReuse::Colocated
        } else {
            ObservationReuse::UntrustedRemoteEnvironment
        },
        prediction: Some(winner.prediction.clone()),
        memory_ceiling: winner
            .reservation
            .and_then(|resolved| resolved.memory_fit.ceiling()),
        // A deferred machine wins only when no machine that can hold the
        // prior was selectable, and its record says that is why.
        process_fit_waived: winner.process_fit_waived.clone().or_else(|| {
            match &winner.deferred {
                Some(PlacementRejectionReason::ProcessDeferred(misfit)) => {
                    Some(cairn_common::executor_protocol::ProcessFitWaiver {
                        misfit: misfit.clone(),
                        reason:
                            cairn_common::executor_protocol::ProcessFitWaiverReason::NoLargerMachine,
                    })
                }
                _ => None,
            }
        }),
        process_demand: winner.reservation.and_then(|resolved| {
            process_demand_record(&resolved.process_evidence, winner.entry)
        }),
        // The survey kept a waivable Mac only when nothing that satisfies the
        // requirement could take the work, so its winning is the fallback.
        nested_sandbox_waived: nested_sandbox_waiver(request, winner.entry),
        restart_exposure,
        cache_distance: winner.cache_distance.map(|distance| {
            cairn_common::cache_distance::MeasuredDistance {
                target_commit: request.base_commit.clone(),
                distance,
                measured_at_unix_ms: now_unix_ms,
            }
        }),
    };
    Ok(PlacementDraft {
        selected: Some((selected_executor(winner.entry), selection)),
        rejected,
        policy: policy_evidence(request, policy, changed_earliest_winner),
    })
}

/// What the selection records about a restart-exposed lane: that an adoptable
/// host won while the colocated executor was a candidate, or that the
/// colocated executor won and why each remote that could have served the lane
/// did not. Nothing when the colocated executor was never a candidate.
pub(super) fn restart_exposure_record(
    exposure: LaneRestartExposure,
    winner: &ScoredCandidate<'_>,
    winner_name: &str,
    ranked: &[&ScoredCandidate<'_>],
    rejected: &[PlacementRejection],
) -> Option<cairn_common::executor_protocol::RestartExposure> {
    use cairn_common::executor_protocol::{RestartExposure, RestartExposureOutcome};
    let outcome = if winner.entry.colocated {
        let mut remotes: Vec<String> = ranked
            .iter()
            .filter(|candidate| !candidate.entry.colocated)
            .map(|candidate| {
                let why = match candidate.prediction.queue.predicted_ms() {
                    Some(wait) if wait > exposure.expected_run_ms => format!(
                        "its queue is forecast at {} s, longer than the lane's run",
                        wait / 1_000
                    ),
                    _ => candidate.outranked_rejection(winner_name).reason.describe(),
                };
                format!("{}: {why}", executor_public_name(candidate.entry))
            })
            .collect();
        remotes.extend(
            rejected
                .iter()
                .filter(|rejection| {
                    rejection.executor_id != COLOCATED_EXECUTOR_ID
                        && bears_on_the_lane(&rejection.reason)
                })
                .map(|rejection| {
                    format!(
                        "{}: {}",
                        rejection.executor_name,
                        rejection.reason.describe()
                    )
                }),
        );
        RestartExposureOutcome::NoAdoptableHost { remotes }
    } else {
        let colocated = ranked.iter().find(|candidate| candidate.entry.colocated)?;
        RestartExposureOutcome::ColocatedPassedOver {
            colocated: executor_public_name(colocated.entry),
        }
    };
    Some(RestartExposure {
        check: exposure.check,
        platform: exposure.platform,
        expected_run_ms: exposure.expected_run_ms,
        restart_bound_ms: exposure.bound_ms,
        outcome,
    })
}

pub(super) fn policy_evidence(
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    changed_earliest_winner: bool,
) -> PlacementPolicyEvidence {
    let stance = policy.profile.stance(request.placement_work_class);
    PlacementPolicyEvidence {
        profile_name: policy.name.clone(),
        work_class: request.placement_work_class,
        stance: match stance {
            PlacementStance::LocalFirst => "localFirst",
            PlacementStance::RemoteFirst => "remoteFirst",
            PlacementStance::RemoteOnly => "remoteOnly",
            PlacementStance::Any => "any",
        }
        .to_string(),
        max_preference_delay_seconds: policy.profile.max_preference_delay_seconds,
        changed_earliest_winner,
        constrained_by_mobility: !request.placement_mobility.may_spill()
            && !matches!(stance, PlacementStance::Any | PlacementStance::LocalFirst),
    }
}

/// One usable machine, with everything the ranking decides on.
pub(super) struct ScoredCandidate<'a> {
    pub(super) entry: &'a ExecutorConnectionState,
    pub(super) sync_cost: SyncCost,
    pub(super) reservation: Option<&'a resource_profiles::ResolvedResourceProfile>,
    /// Why this machine's readings cannot be decided on, when they cannot.
    pub(super) blindness: Option<PlacementRejectionReason>,
    /// Set when that blindness is only readings this machine has not taken
    /// yet ([`blind_only_for_want_of_a_first_sample`]). Never-measured
    /// telemetry fails open (tenet 302), so such a machine is offered work
    /// that nothing seeing can take; a reading that failed or went stale is
    /// evidence, and still refuses.
    pub(super) never_sampled: bool,
    /// Set when the machine is measured and the resolved demand does not fit.
    pub(super) misfit: Option<PlacementRejectionReason>,
    /// Set when the resolved demand does not fit beside the model residents
    /// here: a loading model's unmet footprint, or memory evicting an idle
    /// one would return. A filter, unlike `misfit`: offering the work would
    /// put it on memory a model is about to take, which the machine's own
    /// admission cannot see, so the work waits and placement may evict.
    pub(super) model_room: Option<PlacementRejectionReason>,
    /// Set when the work's measured memory demand is larger than this machine
    /// will ever reserve. A fact about the machine's size rather than its
    /// readings, so it holds whether or not the machine is blind.
    pub(super) over_ceiling: Option<PlacementRejectionReason>,
    /// An executor bound below the work's minimum charge, or a measured
    /// single-process excess. Unlike `over_ceiling` it is a filter, not a
    /// ranking key. No count of kills sets it: no executor yet reports a
    /// kill's cause or whether its victim was alone when it died.
    pub(super) excluded: Option<PlacementRejectionReason>,
    /// The single-process refusal this machine would have met and did not,
    /// and why; carried onto the record if it wins.
    pub(super) process_fit_waived: Option<cairn_common::executor_protocol::ProcessFitWaiver>,
    /// Set when the work's class prior is larger than this machine can give
    /// one process: it ranks behind every machine that is not, and is
    /// selectable only when none of those is.
    pub(super) deferred: Option<PlacementRejectionReason>,
    /// What this machine can give one process, set only for a Rust compile
    /// nothing has measured on any platform, which is offered the machines
    /// that can give the most first.
    pub(super) unmeasured_capacity: Option<Option<u64>>,
    /// What this machine has posted that its admission would decline this
    /// request on now. A current fact, so it outranks everything below it.
    pub(super) held: Option<AdmissionHoldEvidence>,
    /// Whether executor admission can retain and fit this request right now.
    pub(super) admission_accepts: bool,
    /// This machine already declined this entry. History rather than current
    /// state, so it ranks below admission; it covers the refusals posted
    /// state cannot explain.
    pub(super) declined_here: Option<PriorDecline>,
    /// The machine's volume is at or below its pressure line. A price: a tight
    /// machine ranks below an otherwise-equal one that is not, and stays
    /// selectable — it is chosen whenever it is the only candidate.
    pub(super) disk_tight: bool,
    /// Normalized proximity to the entry bar, used only after a candidate can
    /// admit and has an otherwise-comparable verdict prediction.
    pub(super) cpu_headroom_risk: Option<f64>,
    /// When this machine is predicted to answer, and on what evidence. The
    /// ranking key, and the explanation, are the same object.
    pub(super) prediction: PlacementPrediction,
    pub(super) cache_distance: Option<cairn_common::cache_distance::CacheDistance>,
    /// What bringing the batch's node scratch current here is predicted to
    /// take. Added to the predicted verdict, never a filter: a machine where
    /// the scratch is warm ranks ahead of an otherwise-equal cold one.
    pub(super) scratch_sync_ms: u64,
    /// What a thread's home leaves behind by landing here: the runner's
    /// platform, the machine that last held it. Added to the predicted verdict
    /// and never a filter, so it decides among machines that can all take the
    /// home and loses to a machine that cannot.
    pub(super) home_affinity_ms: u64,
}

impl<'a> ScoredCandidate<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        entry: &'a ExecutorConnectionState,
        sync_cost: SyncCost,
        reservation: Option<&'a resource_profiles::ResolvedResourceProfile>,
        declared_reservation: &ResourceReservation,
        mut prediction: PlacementPrediction,
        hold: Option<&PostedHold>,
        declined_here: Option<PriorDecline>,
        now_unix_ms: u64,
    ) -> Self {
        let machine = &entry.health.machine;
        let blindness = placement_blindness(machine, now_unix_ms);
        let never_sampled =
            blindness.is_some() && blind_only_for_want_of_a_first_sample(machine, now_unix_ms);
        // Memory and volume remain eligibility evidence: a machine that cannot
        // hold the work cannot be the fastest at it. They are no longer ranking
        // keys, because how much a machine has left says nothing about how long
        // it takes.
        let available_memory_bytes = machine
            .memory
            .value()
            .map_or(0, |memory| memory.available_bytes);
        let free_volume_bytes = machine.volume.value().map_or(0, |volume| volume.free_bytes);
        // The machine's own admission floor counts toward fit: admission
        // refuses below the watermark plus declared growth, and placement
        // testing growth alone sent work to machines that would only decline
        // it. Zero from an executor that does not report its watermark.
        let watermark_bytes = entry.health.disk.watermark_bytes;
        let misfit = blindness
            .is_none()
            .then_some(reservation)
            .flatten()
            .and_then(|resolved| {
                let demand = &resolved.reservation;
                if machine.memory.value().is_some() && available_memory_bytes < demand.memory_bytes
                {
                    Some(PlacementRejectionReason::InsufficientMemory {
                        required_bytes: demand.memory_bytes,
                        available_bytes: available_memory_bytes,
                    })
                } else if machine.volume.value().is_some()
                    && free_volume_bytes < watermark_bytes.saturating_add(demand.disk_growth_bytes)
                {
                    Some(PlacementRejectionReason::InsufficientVolume {
                        required_bytes: watermark_bytes.saturating_add(demand.disk_growth_bytes),
                        free_bytes: free_volume_bytes,
                    })
                } else {
                    None
                }
            });
        let demand = reservation
            .map(|resolved| &resolved.reservation)
            .unwrap_or(declared_reservation);
        let admission = &entry.health.admission;
        let active = &admission.active_reservation;
        let queue_depth = entry
            .health
            .queues
            .iter()
            .map(|queue| queue.depth)
            .sum::<usize>();
        let cpu_admission = &entry.health.host.cpu_admission;
        // A verdict is trusted for exactly as long as it is not blind. Two
        // different bounds here — one interval to trust it, three to call it
        // blind — left a ten-second gap where a machine ranked as neither
        // pressured nor blind, which is to say as idle.
        //
        // A machine whose sampler measured and stopped is not a machine with
        // headroom, and ranking it as one is how work keeps arriving at a
        // host nobody can see into. Derived from each reading's age rather
        // than from any state the executor sent, because the executor could
        // only set such a state from the sampler whose silence is the
        // condition itself. An instrument that never measured is not blind:
        // that is a platform without the probe, and it fails open. Nor is one
        // that stopped while the sampler's CPU reading stayed current: that is
        // a broken probe on a visible machine, and it fails open the same way.
        let stall_blind = entry
            .health
            .host
            .sampler_blind_instrument(now_unix_ms)
            .is_some();
        let cpu_headroom_risk = (cpu_admission.fresh(now_unix_ms)
            && cpu_admission.state == StallInstrumentState::Accepting)
            .then(|| {
                let utilization = cpu_admission.reading?;
                let policy = entry.health.applied_policy.cpu_admission;
                let span = policy.entry - policy.clear;
                (span > 0.0).then(|| ((utilization - policy.clear) / span).clamp(0.0, 1.0))
            })
            .flatten();
        // A held machine's queue has no forecastable relief: nothing Cairn
        // owns completes on a schedule that ends sustained pressure or another
        // request's provisioning. Its wait is unknown rather than the zero its
        // free units would say, and the hold is named.
        if let Some(hold) = hold {
            prediction.queue = QueueForecast::Unknown {
                reason: QueueUnknownReason::Held {
                    evidence: hold.evidence.clone(),
                },
            };
            prediction.predicted_verdict_ms = prediction
                .adjusted_run_ms
                .saturating_add(prediction.reliability_penalty_ms())
                .saturating_add(prediction.host_stall_penalty_ms());
        }
        prediction.declined_here = declined_here;
        let admission_accepts = !stall_blind
            && queue_depth < entry.health.applied_policy.maximum_queue_depth
            && admission.memory_capacity_bytes.is_none_or(|capacity| {
                active.memory_bytes.saturating_add(demand.memory_bytes) <= capacity
            })
            && admission.disk_growth_capacity_bytes.is_none_or(|capacity| {
                active
                    .disk_growth_bytes
                    .saturating_add(demand.disk_growth_bytes)
                    <= capacity
            });
        let ceiling = reservation.and_then(|resolved| resolved.memory_fit.ceiling());
        let (too_large, process_fit_waived, deferred) =
            match reservation.and_then(|resolved| process_fit(&resolved.process_evidence, entry)) {
                Some(ProcessFit::Refused(reason)) => (Some(reason), None, None),
                Some(ProcessFit::Waived(waiver)) => (None, Some(waiver), None),
                Some(ProcessFit::Deferred(misfit)) => (
                    None,
                    None,
                    Some(PlacementRejectionReason::ProcessDeferred(misfit)),
                ),
                None => (None, None, None),
            };
        let unmeasured_capacity = reservation
            .filter(|resolved| {
                resolved.process_evidence.unmeasured_compile
                    && resolved.process_evidence.demand.is_none()
            })
            .map(|_| entry_process_capacity(entry));
        let excluded = executor_bound_rejection(entry, demand).or(too_large);
        let over_ceiling = ceiling.map(|ceiling| PlacementRejectionReason::ExceedsMemoryCeiling {
            estimate_bytes: ceiling.estimate_bytes,
            ceiling_bytes: ceiling.ceiling_bytes,
            killed_here: ceiling.killed_here,
        });
        Self {
            entry,
            sync_cost,
            reservation,
            blindness,
            never_sampled,
            misfit,
            model_room: None,
            over_ceiling,
            excluded,
            process_fit_waived,
            deferred,
            unmeasured_capacity,
            held: hold.map(|hold| hold.evidence.clone()),
            admission_accepts,
            declined_here,
            disk_tight: entry.health.disk.status == DiskHealthStatus::Tight,
            cpu_headroom_risk,
            prediction,
            cache_distance: None,
            scratch_sync_ms: 0,
            home_affinity_ms: 0,
        }
    }

    pub(super) fn rejection(&self) -> PlacementRejection {
        PlacementRejection {
            unreadable_memory: self
                .reservation
                .map(|resolved| resolved.rationale.unreadable_memory.clone())
                .unwrap_or_default(),
            drain_blockers: Vec::new(),
            prediction: Some(self.prediction.clone()),
            executor_name: executor_public_name(self.entry),
            executor_id: self.entry.identity.executor_id.clone(),
            reason: self
                .excluded
                .clone()
                .or_else(|| self.model_room.clone())
                .or_else(|| self.blindness.clone())
                .or_else(|| self.deferred.clone())
                .or_else(|| self.over_ceiling.clone())
                .or_else(|| self.misfit.clone())
                .or_else(|| self.held_rejection())
                .unwrap_or(PlacementRejectionReason::NotColocated),
        }
    }

    pub(super) fn outranked_rejection(&self, winner_name: &str) -> PlacementRejection {
        // A machine that could not be measured, or that measurably could not fit,
        // has a more useful thing to say than "it lost".
        let reason = self
            .excluded
            .clone()
            .or_else(|| self.model_room.clone())
            .or_else(|| self.blindness.clone())
            .or_else(|| self.deferred.clone())
            .or_else(|| self.over_ceiling.clone())
            .or_else(|| self.misfit.clone())
            .or_else(|| self.held_rejection())
            .unwrap_or_else(|| PlacementRejectionReason::OutrankedBy {
                executor_name: winner_name.to_string(),
            });
        PlacementRejection {
            unreadable_memory: self
                .reservation
                .map(|resolved| resolved.rationale.unreadable_memory.clone())
                .unwrap_or_default(),
            drain_blockers: Vec::new(),
            // A machine that was ranked and lost owes the operator its numbers,
            // not just the name of whoever beat it.
            prediction: Some(self.prediction.clone()),
            executor_name: executor_public_name(self.entry),
            executor_id: self.entry.identity.executor_id.clone(),
            reason,
        }
    }

    pub(super) fn held_rejection(&self) -> Option<PlacementRejectionReason> {
        self.held
            .clone()
            .map(|evidence| PlacementRejectionReason::Held { evidence })
    }
}

/// Order the usable machines and name the winner.
///
/// A measured machine always beats a measured-blind one, a machine that can ever
/// hold the work's measured memory always beats one that cannot, and a machine
/// that fits the resolved demand now always beats one that does not. The
/// ceiling comes before fit on purpose: a big machine that is busy right now
/// queues the work in its own admission until it has room, while a small one
/// that is idle right now runs it into its memory limit. Neither is a filter,
/// so a fleet where nothing can hold the work still places it, at the clamp
/// (unless a capable machine has only declined for now; see [`rank_survey`]).
/// An executor bound below the work's minimum charge, or a measured
/// single-process excess, is a filter (`excluded`), and no ranking may choose
/// it; a kill raises only its victim's floor. Fit is a ranking key
/// rather than a hard filter on purpose: reserving capacity and queueing for it
/// belong to the executor's admission, and placement refusing work the executor
/// would have queued would be a second, quieter queue.
///
/// **Placement will not choose a machine it cannot see.** A blind candidate is
/// selectable only when nothing is being chosen: the caller settled placement
/// itself (a pin, or a selector naming one machine), or the candidate is the
/// colocated home, where refusing to run at all is a worse failure than running
/// where the work already is. Otherwise a blind machine is excluded and, if that
/// leaves nothing, the request is refused with the evidence rather than shipped
/// somewhere unexamined.
///
/// Constraining the fleet is not settling placement. `executor: {os: "linux"}`
/// narrows the candidate set and leaves policy to pick among what is left, so
/// absent placement evidence is exactly as disqualifying there as it is for an
/// unconstrained request. A gap is never read as no load.
///
/// Below fit come two answers about this request on this machine. A machine
/// whose posted condition its own admission would decline the request on
/// (`held`) ranks behind every machine that is not, ahead of whether admission
/// has room, because it is current evidence of a decline. A machine that has
/// already declined this entry (`declined_here`) ranks behind every machine
/// that has not, after admission, because it is history: it covers the
/// refusals posted state cannot explain.
///
/// Among the machines that survive those gates, a known queue forecast always
/// precedes an unknown one: there is no numeric total to compare when one summand
/// is absent. Within that comparable set, the order is predicted time to a
/// verdict, ascending. Evidence quality breaks ties but never leads: a machine
/// confidently predicted to be slow is still slow, and preferring it because the
/// prediction was well-evidenced would rank on the measurement rather than on
/// what was measured.
///
/// A main-cadence lane expected to outlast the runner
/// ([`LaneRestartExposure`]) ranks the colocated executor behind every
/// adoptable host, right after fit: a remote that is busy still outranks an
/// idle colocated executor, because a hold costs the lane minutes while a
/// restart costs it the whole pass (cairn/6969). Only a remote whose own queue
/// is forecast to outlast the lane's run earns no such preference, since the
/// colocated run would likely finish first. The key orders the colocated
/// executor against remotes and nothing else: warmth still orders the remotes
/// among themselves (tenet 1395), and the colocated executor stays selectable,
/// so it takes the lane whenever no remote can.
#[allow(clippy::too_many_arguments)]
pub(super) fn rank_candidates<'a, 'b>(
    scored: &'b [ScoredCandidate<'a>],
    settled_by_caller: bool,
    exercising_spill: bool,
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    exploration: &ExplorationState,
    restart_exposure: Option<&LaneRestartExposure>,
) -> (
    Option<&'b ScoredCandidate<'a>>,
    Vec<&'b ScoredCandidate<'a>>,
    bool,
    bool,
    Option<PlacementTieBreak>,
) {
    // The key applies only where it has something to choose between: a
    // colocated candidate and at least one adoptable one.
    let restart_exposure = restart_exposure.filter(|_| {
        scored.iter().any(|candidate| candidate.entry.colocated)
            && scored.iter().any(|candidate| !candidate.entry.colocated)
    });
    let restart_demoted = |candidate: &&ScoredCandidate<'a>| {
        restart_exposure.is_some_and(|exposure| {
            candidate.entry.colocated
                || candidate
                    .prediction
                    .queue
                    .predicted_ms()
                    .is_some_and(|wait| wait > exposure.expected_run_ms)
        })
    };
    let hard_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        a.excluded
            .is_some()
            .cmp(&b.excluded.is_some())
            .then_with(|| a.model_room.is_some().cmp(&b.model_room.is_some()))
            .then_with(|| a.blindness.is_some().cmp(&b.blindness.is_some()))
            // Like the ceiling below, a fact about the machine's size: a
            // large machine that is busy queues the work, a small idle one
            // runs it into the kernel.
            .then_with(|| a.deferred.is_some().cmp(&b.deferred.is_some()))
            .then_with(|| b.unmeasured_capacity.cmp(&a.unmeasured_capacity))
            .then_with(|| a.over_ceiling.is_some().cmp(&b.over_ceiling.is_some()))
            .then_with(|| a.misfit.is_some().cmp(&b.misfit.is_some()))
            .then_with(|| restart_demoted(a).cmp(&restart_demoted(b)))
            .then_with(|| a.held.is_some().cmp(&b.held.is_some()))
            .then_with(|| b.admission_accepts.cmp(&a.admission_accepts))
            .then_with(|| a.declined_here.is_some().cmp(&b.declined_here.is_some()))
            // A tight volume is reclaiming toward its pressure line, and new
            // growth there costs a warm cache. Ranked, never refused: a tight
            // machine still wins when nothing else can take the work.
            .then_with(|| a.disk_tight.cmp(&b.disk_tight))
            .then_with(|| {
                a.prediction
                    .queue
                    .predicted_ms()
                    .is_none()
                    .cmp(&b.prediction.queue.predicted_ms().is_none())
            })
    };
    let forecast_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        a.prediction
            .predicted_verdict_ms
            .saturating_add(a.scratch_sync_ms)
            .saturating_add(a.home_affinity_ms)
            .cmp(
                &b.prediction
                    .predicted_verdict_ms
                    .saturating_add(b.scratch_sync_ms)
                    .saturating_add(b.home_affinity_ms),
            )
            .then_with(|| {
                a.cpu_headroom_risk
                    .partial_cmp(&b.cpu_headroom_risk)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                a.prediction
                    .evidence_rank()
                    .cmp(&b.prediction.evidence_rank())
            })
            .then_with(|| sync_cost_key(a.sync_cost).cmp(&sync_cost_key(b.sync_cost)))
            // Source distance breaks otherwise-equivalent cost predictions; it
            // never fabricates milliseconds or overrides admission and policy.
            .then_with(|| {
                a.cache_distance
                    .as_ref()
                    .map_or(u64::MAX, |distance| distance.rank())
                    .cmp(
                        &b.cache_distance
                            .as_ref()
                            .map_or(u64::MAX, |distance| distance.rank()),
                    )
            })
    };
    let recent_selection_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        let age = |candidate: &&ScoredCandidate<'a>| {
            exploration
                .selected_executors
                .iter()
                .position(|executor_id| executor_id == &candidate.entry.identity.executor_id)
        };
        match (age(a), age(b)) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a_age), Some(b_age)) => a_age.cmp(&b_age),
        }
    };
    let stable_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        recent_selection_order(a, b).then_with(|| {
            a.entry
                .identity
                .executor_id
                .cmp(&b.entry.identity.executor_id)
        })
    };
    // A blind remote is no spill target while anything placement can see, or
    // the work's own home, can take the work. When nothing can, a machine that
    // is blind only because it has not sampled yet is offered it: absence of
    // telemetry must not strand a machine (tenet 302), and a freshly attached
    // executor is in exactly that state until its first sample lands.
    let usable_with = |candidate: &&ScoredCandidate<'a>, admit_never_sampled: bool| {
        candidate.excluded.is_none()
            && candidate.model_room.is_none()
            && (candidate.blindness.is_none()
                || settled_by_caller
                || candidate.entry.colocated
                || !exercising_spill
                || (admit_never_sampled && candidate.never_sampled))
            && !(exercising_spill
                && matches!(
                    policy.profile.stance(request.placement_work_class),
                    PlacementStance::RemoteOnly
                )
                && candidate.entry.colocated)
    };
    let admit_never_sampled = !scored
        .iter()
        .any(|candidate| usable_with(&candidate, false));
    let usable = |candidate: &&ScoredCandidate<'a>| usable_with(candidate, admit_never_sampled);
    // A machine too small for the class prior is taken only when nothing
    // that can hold it is usable; otherwise ranking would still choose it
    // whenever every larger machine was outranked on some later key.
    let undeferred_usable = scored
        .iter()
        .any(|candidate| candidate.deferred.is_none() && usable(&candidate));
    let selectable = |candidate: &&ScoredCandidate<'a>| {
        usable(candidate) && (candidate.deferred.is_none() || !undeferred_usable)
    };

    let mut baseline: Vec<&ScoredCandidate<'a>> = scored.iter().collect();
    baseline.sort_by(|a, b| {
        hard_order(a, b)
            .then_with(|| forecast_order(a, b))
            .then_with(|| stable_order(a, b))
    });
    let earliest = baseline.iter().copied().find(selectable);
    let Some(earliest) = earliest else {
        return (None, baseline, false, false, None);
    };
    if settled_by_caller || !exercising_spill {
        return (Some(earliest), baseline, false, false, None);
    }

    // A prior is not evidence about a machine, and pure exploitation can never
    // turn it into evidence. One in every four spill decisions for this work
    // class may spend otherwise-idle, admission-ready capacity acquiring it. The
    // other three decisions remain pure earliest-verdict placements, so a machine
    // that repeatedly fails before completion or reports unusable measurements
    // cannot monopolize work. Candidates rotate by their most recent exploration,
    // which gives every fresh machine a bounded turn instead of retrying the first
    // executor forever. Failed object transfer never reaches `observe_completed`,
    // so those attempts consume budget but never become duration samples.
    //
    // Otherwise-idle is the learner's rule, asked of the forecast
    // ([`resource_profiles::teaches_baseline`]): a run beside other work, or one
    // whose stall reading is stale, never becomes a baseline sample, so
    // exploring there spends the lane, at whatever its stall and contention
    // cost, for nothing. A busy machine stays thin and stays eligible, and it
    // took one lane in four of a work class forever (cairn/4999: bbs at 47%
    // full I/O stall beside fifteen cargo lanes, chosen over a 1.6 s forecast,
    // each run dying at vitest's 60 s start bound). It still competes on its
    // forecast, where its stall is priced.
    // A service resident never completes, so it never teaches a duration, and
    // exploring with it would land it on whatever idle machine was due rather
    // than on the service box or home its stance and role affinity prefer.
    let exploration_due = exploration.decisions_since_exploration >= EXPLORATION_INTERVAL - 1
        && request.placement_work_class != PlacementWorkClass::Services;
    let last_explored = |candidate: &&ScoredCandidate<'a>| {
        exploration
            .explored_executors
            .iter()
            .position(|executor_id| executor_id == &candidate.entry.identity.executor_id)
    };
    let bootstrap_eligible = |candidate: &&ScoredCandidate<'a>| {
        selectable(candidate)
            && candidate.misfit.is_none()
            // Exploring a machine that can never hold the work earns an
            // out-of-memory kill, not a duration sample.
            && candidate.over_ceiling.is_none()
            // Nor is a Rust compile whose single process nothing measured
            // explored onto whatever machine is due: its order is by size.
            && candidate.deferred.is_none()
            && candidate.unmeasured_capacity.is_none()
            // Nor is a lane a restart would end sent where it would end.
            && !restart_demoted(candidate)
            && candidate.admission_accepts
            && candidate.prediction.queue.predicted_ms() == Some(0)
            && resource_profiles::teaches_baseline(&forecast_start(&candidate.prediction))
            && candidate.prediction.run.profile_key.is_some()
            && matches!(
                candidate.prediction.run.fallback,
                Some(
                    DurationFallback::NoProfileRecorded
                        | DurationFallback::BelowConfidenceFloor
                        | DurationFallback::ProfileTooOld
                )
            )
    };
    let exploration = exploration_due
        .then(|| {
            baseline
                .iter()
                .copied()
                .filter(bootstrap_eligible)
                .min_by(|a, b| match (last_explored(a), last_explored(b)) {
                    (None, None) => forecast_order(a, b).then_with(|| stable_order(a, b)),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (Some(a_age), Some(b_age)) => a_age
                        .cmp(&b_age)
                        .then_with(|| forecast_order(a, b))
                        .then_with(|| stable_order(a, b)),
                })
        })
        .flatten();
    if let Some(exploration) = exploration {
        let changed = exploration.entry.identity.executor_id != earliest.entry.identity.executor_id;
        let bootstrap_candidates = scored
            .iter()
            .filter(bootstrap_eligible)
            .map(|candidate| executor_public_name(candidate.entry))
            .collect::<Vec<_>>();
        let exploration_age_decided = scored
            .iter()
            .filter(bootstrap_eligible)
            .any(|candidate| last_explored(&exploration) != last_explored(&candidate));
        let tied_forecasts = scored
            .iter()
            .filter(bootstrap_eligible)
            .filter(|candidate| forecast_order(&exploration, candidate).is_eq())
            .count();
        let deciding_reason =
            exploration_age_decided
                .then_some("least recently explored bootstrap candidate")
                .or_else(|| {
                    (tied_forecasts > 1).then(|| {
                        if scored.iter().filter(bootstrap_eligible).any(|candidate| {
                            recent_selection_order(&exploration, &candidate).is_ne()
                        }) {
                            "least recently selected among equal bootstrap candidates"
                        } else {
                            "stable executor identity seeded recent-selection history"
                        }
                    })
                });
        return (
            Some(exploration),
            baseline,
            changed,
            true,
            deciding_reason.map(|reason| PlacementTieBreak {
                candidates: bootstrap_candidates,
                deciding_reason: reason.into(),
            }),
        );
    }

    let deadline = earliest.prediction.predicted_verdict_ms.saturating_add(
        policy
            .profile
            .max_preference_delay_seconds
            .saturating_mul(1_000),
    );
    let same_hard_group = |candidate: &&ScoredCandidate<'a>| {
        hard_order(&earliest, candidate).is_eq()
            && candidate.prediction.predicted_verdict_ms <= deadline
            && selectable(candidate)
    };
    let stance = policy.profile.stance(request.placement_work_class);
    let preference = |candidate: &&ScoredCandidate<'a>| match stance {
        PlacementStance::LocalFirst => !candidate.entry.colocated,
        PlacementStance::RemoteFirst | PlacementStance::RemoteOnly => candidate.entry.colocated,
        PlacementStance::Any => false,
    };
    // A service resident is what a services-only machine is for, so one ranks
    // first for it. Like the stance it is a preference inside the hard group
    // and the preference-delay deadline: priced, never a pin (tenet 314), so a
    // service box that is away or clearly slower yields to the next machine.
    // A publication leg likewise ranks a publisher first, which matters only
    // for a secret-free leg: a secret-bearing one is offered to nothing else.
    let role_affinity = |candidate: &&ScoredCandidate<'a>| {
        let suited = match request.publication {
            Some(_) => candidate.entry.machine_role == MachineRole::PublishOnly,
            None => {
                request.placement_work_class == PlacementWorkClass::Services
                    && candidate.entry.machine_role == MachineRole::ServicesOnly
            }
        };
        !suited
    };
    let priority = |candidate: &&ScoredCandidate<'a>| {
        policy
            .profile
            .machine_priority
            .iter()
            .position(|id| id == &candidate.entry.identity.executor_id)
            .unwrap_or(usize::MAX)
    };
    let mut ranked = baseline;
    ranked.sort_by(|a, b| {
        hard_order(a, b)
            .then_with(|| same_hard_group(b).cmp(&same_hard_group(a)))
            .then_with(|| {
                if same_hard_group(a) && same_hard_group(b) {
                    role_affinity(a)
                        .cmp(&role_affinity(b))
                        .then_with(|| preference(a).cmp(&preference(b)))
                        .then_with(|| priority(a).cmp(&priority(b)))
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| forecast_order(a, b))
            .then_with(|| stable_order(a, b))
    });
    let winner = ranked.iter().copied().find(selectable);
    let changed = winner.is_some_and(|winner| {
        winner.entry.identity.executor_id != earliest.entry.identity.executor_id
    });
    let tie_break = winner.and_then(|winner| {
        let tied = ranked
            .iter()
            .copied()
            .filter(|candidate| {
                hard_order(&winner, candidate).is_eq()
                    && forecast_order(&winner, candidate).is_eq()
                    && same_hard_group(candidate)
                    && role_affinity(&winner) == role_affinity(candidate)
                    && preference(&winner) == preference(candidate)
                    && priority(&winner) == priority(candidate)
            })
            .map(|candidate| executor_public_name(candidate.entry))
            .collect::<Vec<_>>();
        let recent_selection_decided = ranked.iter().copied().any(|candidate| {
            hard_order(&winner, &candidate).is_eq()
                && forecast_order(&winner, &candidate).is_eq()
                && same_hard_group(&candidate)
                && role_affinity(&winner) == role_affinity(&candidate)
                && preference(&winner) == preference(&candidate)
                && priority(&winner) == priority(&candidate)
                && recent_selection_order(&winner, &candidate).is_ne()
        });
        (tied.len() > 1).then(|| PlacementTieBreak {
            candidates: tied,
            deciding_reason: if recent_selection_decided {
                "least recently selected among candidates equal on every earlier placement key"
            } else {
                "stable executor identity seeded recent-selection history"
            }
            .into(),
        })
    });
    (winner, ranked, changed, false, tie_break)
}

/// What this machine already holds for this request, read off the facts it
/// published about its own cells and verified objects.
///
/// The runner classifies; the executor states facts. Nothing here is a score and
/// nothing here is a path: where a cell lives is that machine's business, and
/// comparing paths across machines would be meaningless anyway. What is
/// comparable is the project a cell serves and the work classes that have
/// completed in it.
///
/// An externally owned checkout never counts as warm. The executor supplies
/// process authority over such a tree and does not prepare, fingerprint, or
/// normalize it, so it cannot honestly claim anything about what it contains.
/// Measure only the surveyed machines' idle slots for this project. The
/// caller owns a metadata snapshot, not the connections or queue lock. A
/// target-labelled last_distance is history and deliberately never read here.
pub(super) fn candidate_cache_distances_with(
    entries: &[&ExecutorConnectionState],
    request: &CellRequest,
    now_unix_ms: u64,
    mut measure: impl FnMut(
        &std::path::Path,
        &str,
        &str,
    ) -> Option<cairn_common::cache_distance::CacheDistance>,
) -> HashMap<String, cairn_common::cache_distance::CacheDistance> {
    let mut result = HashMap::new();
    let Some(repository) = request
        .repository
        .colocated_path()
        .map(std::path::Path::new)
        .filter(|path| path.is_dir())
    else {
        return result;
    };
    let mut measured = HashMap::new();
    for entry in entries {
        let observed_at = entry
            .advertisement
            .liveness_observed_at_unix_ms
            .unwrap_or(entry.advertisement.observed_at_unix_ms);
        if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS
            || entry.health.inventory.authority != InventoryAuthorityState::Authoritative
        {
            continue;
        }
        let best = entry
            .health
            .slots
            .iter()
            .filter(|slot| {
                slot.project_id == request.project_id
                    && slot.lifecycle
                        == cairn_common::executor_protocol::PersistentCellLifecycle::Idle
            })
            .take(cairn_common::executor_protocol::IDLE_RETENTION_CEILING_PER_PROJECT)
            .filter_map(|slot| slot.head_commit.as_ref())
            .filter_map(|head| {
                // The target is fixed for this survey, so head identifies the
                // (head, target) pair and includes failed measurements too.
                *measured
                    .entry(head.clone())
                    .or_insert_with(|| measure(repository, head, &request.base_commit))
            })
            .min_by_key(|distance| distance.rank());
        if let Some(best) = best {
            result.insert(entry.identity.executor_id.clone(), best);
        }
    }
    result
}

pub(super) fn candidate_warmth(
    entry: &ExecutorConnectionState,
    request: &CellRequest,
    now_unix_ms: u64,
) -> CacheWarmthEvidence {
    let observed_at = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .unwrap_or(entry.advertisement.observed_at_unix_ms);
    if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS {
        return CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::FactsStale,
        };
    }
    // A machine that does not claim authority over its own inventory cannot have
    // the absence of a matching cell read as evidence that none exists. Cold and
    // "we cannot tell" are different answers and must not collapse into one.
    if entry.health.inventory.authority != InventoryAuthorityState::Authoritative {
        return CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::InventoryNotAuthoritative,
        };
    }
    let prepared = entry.snapshot.cells.iter().any(|cell| {
        cell.project_id == request.project_id
            && cell.checkout_kind != CellCheckoutKind::ExistingCheckout
            && cell.is_warm_for(request.command_class)
    });
    let warmth = if prepared {
        ExecutionWarmth::PreparedWarmSlot
    } else if warm_root_holds_commit(entry, request) {
        // Objects present is not a build cache. A machine that merely holds the
        // commit still compiles from scratch, and saying otherwise is how a cold
        // remote came to look as ready as one that had just built the tree.
        ExecutionWarmth::RepositoryOnly
    } else {
        ExecutionWarmth::Cold
    };
    CacheWarmthEvidence::Observed {
        warmth,
        observed_at_unix_ms: observed_at,
    }
}

/// Resolves "how long would this take" for any command identity, on any
/// machine, with or without a profile store behind it.
///
/// A residency placement carries no resource plan and therefore no store. That
/// is a different fact from having looked and found nothing, and this is what
/// keeps the two apart on the record instead of blaming a machine for a lookup
/// nobody attempted.
pub(super) struct DurationOracle {
    pub(super) db: Option<Arc<cairn_db::storage::LocalDb>>,
}

impl DurationOracle {
    pub(super) async fn predict(
        &self,
        identity: Option<&CommandResourceIdentity>,
        context: &resource_profiles::ProfileContext,
        warmth: ExecutionWarmth,
        class: cairn_common::executor_protocol::CellCommandClass,
        now_unix_ms: u64,
    ) -> DurationEstimate {
        match &self.db {
            Some(db) => {
                resource_profiles::resolve_duration(
                    db.clone(),
                    identity,
                    context,
                    warmth,
                    class,
                    now_unix_ms,
                )
                .await
            }
            None => resource_profiles::unmeasured_duration(
                class,
                context,
                identity,
                warmth,
                DurationFallback::NoProfileStore,
            ),
        }
    }
}

/// How long this request would wait before a machine could start it.
///
/// Advisory and read-only, in the strongest sense: it enqueues nothing, reserves
/// nothing, refuses nothing, and binds nobody. The executor's waiting room stays
/// the only authority over what actually runs when, and after placement picks a
/// machine that machine may still queue or refuse the request on its own live
/// state. This exists so a caller waiting on a verdict is not sent to a host that
/// will start its work in ten minutes just because that host looked idle.
///
/// The wait is a fluid approximation: the work ahead is summed as unit
/// milliseconds — each item's predicted duration times the concurrency it holds
/// — and the machine is assumed to drain it at its advertised capacity. It does
/// not simulate a scheduler, because the fidelity of such a simulation could not
/// be verified against a queue the runner does not own.
/// Every distinct thing a forecast must price on one machine, so each is
/// resolved once however much work shares an identity.
pub(super) fn queue_price_keys(
    entry: &ExecutorConnectionState,
) -> Vec<(Option<CommandResourceIdentity>, CellCommandClass)> {
    let mut keys: Vec<(Option<CommandResourceIdentity>, CellCommandClass)> = Vec::new();
    let mut push = |identity: Option<CommandResourceIdentity>, class| {
        if !keys
            .iter()
            .any(|(known, known_class)| *known == identity && *known_class == class)
        {
            keys.push((identity, class));
        }
    };
    for queued in &entry.snapshot.queued_requests {
        push(
            queued.command_resource_identity.clone(),
            queued.command_class,
        );
    }
    for running in &entry.snapshot.executing_requests {
        push(
            running.command_resource_identity.clone(),
            running.command_class,
        );
    }
    keys
}

/// What each piece of work on a machine is predicted to take.
///
/// Resolving these is the only asynchronous part of a forecast, which is why it
/// is separated from the arithmetic: the ordering, the capacity model, and the
/// honesty rules are then a pure function that can be reasoned about and tested
/// without a database behind it.
#[derive(Default)]
pub(super) struct QueuePrices {
    pub(super) by_key: Vec<(
        (Option<CommandResourceIdentity>, CellCommandClass),
        DurationEstimate,
    )>,
}

impl QueuePrices {
    /// Queued work has not run, so nothing is known about the state it will
    /// find. Everything ahead is priced cold, which predicts the longer wait and
    /// therefore biases placement away from busy machines rather than toward
    /// them.
    pub(super) async fn resolve(
        oracle: &DurationOracle,
        entry: &ExecutorConnectionState,
        context: &resource_profiles::ProfileContext,
        now_unix_ms: u64,
    ) -> Self {
        let mut by_key = Vec::new();
        for (identity, class) in queue_price_keys(entry) {
            let estimate = oracle
                .predict(
                    identity.as_ref(),
                    context,
                    ExecutionWarmth::Cold,
                    class,
                    now_unix_ms,
                )
                .await;
            by_key.push(((identity, class), estimate));
        }
        Self { by_key }
    }

    /// The labeled class priors alone, synchronously.
    ///
    /// Production reaches the same answer through [`Self::resolve`] against a
    /// [`DurationOracle`] with no store behind it; this exists only so the
    /// synchronous test entry points exercise the real forecast arithmetic
    /// rather than a stand-in for it.
    #[cfg(test)]
    pub(super) fn from_priors(
        entry: &ExecutorConnectionState,
        context: &resource_profiles::ProfileContext,
    ) -> Self {
        Self {
            by_key: queue_price_keys(entry)
                .into_iter()
                .map(|(identity, class)| {
                    let estimate = resource_profiles::unmeasured_duration(
                        class,
                        context,
                        identity.as_ref(),
                        ExecutionWarmth::Cold,
                        DurationFallback::NoProfileStore,
                    );
                    ((identity, class), estimate)
                })
                .collect(),
        }
    }

    pub(super) fn get(
        &self,
        identity: Option<&CommandResourceIdentity>,
        class: CellCommandClass,
    ) -> Option<&DurationEstimate> {
        self.by_key
            .iter()
            .find(|((known, known_class), _)| known.as_ref() == identity && *known_class == class)
            .map(|(_, estimate)| estimate)
    }
}

/// How long this request would wait on this machine before it starts.
///
/// `run_ms` is this request's own predicted run. A queue whose oldest entry
/// ahead has already waited longer than that has measurably not moved, and is
/// priced at least at that wait whatever its free units say.
pub(super) fn forecast_queue_wait(
    entry: &ExecutorConnectionState,
    prices: &QueuePrices,
    request: &CellRequest,
    run_ms: u64,
    now_unix_ms: u64,
) -> QueueForecast {
    let observed_at = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .unwrap_or(entry.advertisement.observed_at_unix_ms);
    if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::FactsStale,
        };
    }
    let capacity = entry
        .health
        .admission
        .concurrency_capacity
        .filter(|capacity| *capacity > 0);
    let Some(capacity) = capacity else {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::NoAdmissionCapacity,
        };
    };

    // Seniority is a property of the wait, not of the enqueue, so the
    // hypothetical request ranks by when its requester began waiting -- exactly
    // the key the executor's own admission uses.
    let waiting_since = if request.waiting_since_unix_ms == 0 {
        now_unix_ms
    } else {
        request.waiting_since_unix_ms
    };
    let ours = (
        aged_priority(request.priority, waiting_since, now_unix_ms),
        std::cmp::Reverse(waiting_since),
    );

    let mut demand_ahead = request.resource_reservation.concurrency_units;
    let mut requests_ahead = 0_usize;
    let mut oldest_ahead_ms = 0_u64;
    for queued in &entry.snapshot.queued_requests {
        let theirs = (
            aged_priority(queued.priority, queued.queued_at_unix_ms, now_unix_ms),
            std::cmp::Reverse(queued.queued_at_unix_ms),
        );
        if theirs <= ours {
            continue;
        }
        requests_ahead += 1;
        oldest_ahead_ms = oldest_ahead_ms.max(now_unix_ms.saturating_sub(queued.queued_at_unix_ms));
        let Some(estimate) = prices.get(
            queued.command_resource_identity.as_ref(),
            queued.command_class,
        ) else {
            // Something ahead could not be priced at all. Summing it as zero
            // would report an empty queue, so the whole forecast declines
            // instead.
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        let _ = estimate;
        demand_ahead = demand_ahead.saturating_add(queued.resource_reservation.concurrency_units);
    }

    // Whatever the units say, entries ahead that have waited longer than this
    // request would take to run are a queue that is not moving, and that wait
    // is the measurement that prices it. Free units cannot read it as zero:
    // a machine holding work back for a reason it has not posted looks exactly
    // like one with room.
    let stalled_head_ms =
        (requests_ahead > 0 && oldest_ahead_ms > run_ms).then_some(oldest_ahead_ms);
    // The advertised capacity is already net of the interactive reserve (the
    // executor removes it when it sizes its budget, and admits against that
    // budget with nothing subtracted), so the reserve is not taken again here.
    let free = capacity.saturating_sub(entry.health.admission.active_reservation.concurrency_units);
    let running_ahead = entry.snapshot.executing_requests.len();

    // If measured capacity can admit this request and everything ranked ahead,
    // running durations do not enter the answer at all. This remains a measured
    // zero even when those durations only have priors.
    if demand_ahead <= free {
        return QueueForecast::Forecast {
            predicted_ms: stalled_head_ms.unwrap_or(0),
            requests_ahead,
            running_ahead,
            fully_measured: stalled_head_ms.is_none(),
            observed_at_unix_ms: observed_at,
            stalled_head_ms,
        };
    }

    let mut releases = Vec::with_capacity(running_ahead);
    for running in &entry.snapshot.executing_requests {
        let Some(estimate) = prices.get(
            running.command_resource_identity.as_ref(),
            running.command_class,
        ) else {
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        let elapsed = now_unix_ms.saturating_sub(running.started_at_unix_ms);
        releases.push((
            estimate.predicted_ms.saturating_sub(elapsed),
            running.resource_reservation.concurrency_units,
            estimate.is_learned(),
        ));
    }
    releases.sort_by_key(|(remaining, _, _)| *remaining);

    let mut available = free;
    let mut predicted_ms = 0;
    let mut fully_measured = true;
    for (remaining, units, measured) in releases {
        available = available.saturating_add(units);
        predicted_ms = remaining;
        fully_measured &= measured && remaining > 0;
        if available >= demand_ahead {
            break;
        }
    }

    QueueForecast::Forecast {
        predicted_ms: stalled_head_ms.map_or(predicted_ms, |waited| waited.max(predicted_ms)),
        requests_ahead,
        running_ahead,
        fully_measured: fully_measured && stalled_head_ms.is_none(),
        observed_at_unix_ms: observed_at,
        stalled_head_ms,
    }
}

/// Assemble one machine's predicted time to a verdict from its legs.
///
/// The total is queue wait plus run duration and nothing else. Preparation rides
/// along as evidence because no transfer history exists to turn missing object
/// bytes into milliseconds, and a fabricated bytes-per-second constant would be
/// exactly the kind of fiction this ranking replaced. An unknown queue wait is
/// not summed as zero either: it contributes nothing to the number and says so
/// on the record, so a machine whose queue could not be read never wins by
/// looking empty.
pub(super) fn placement_prediction_with_contention(
    entry: &ExecutorConnectionState,
    warmth: CacheWarmthEvidence,
    queue: QueueForecast,
    run: DurationEstimate,
    contention: ContentionEstimate,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    let preparation = match sync_cost {
        SyncCost::Known(0) => PreparationForecast::ObjectsPresent,
        SyncCost::Known(bytes) => PreparationForecast::TransferPending { bytes },
        SyncCost::Unknown => PreparationForecast::Unknown,
    };
    let base_run_ms = run.predicted_ms;
    let adjusted_run_ms = contention.adjusted_ms(base_run_ms);
    PlacementPrediction {
        executor_name: executor_public_name(entry),
        executor_id: entry.identity.executor_id.clone(),
        predicted_verdict_ms: queue
            .predicted_ms()
            .unwrap_or(0)
            .saturating_add(adjusted_run_ms),
        queue,
        preparation,
        run,
        base_run_ms,
        adjusted_run_ms,
        contention,
        warmth,
        // Set by ranking, which is what knows this entry's declines.
        declined_here: None,
        reliability: None,
        memory_pressure: None,
        io_pressure: None,
    }
}

/// The stall share one reading prices, or the record of why it prices
/// nothing: unmeasured is never zero, and a stale stall is not this machine
/// now.
pub(super) fn priced_stall_share<T>(
    reading: &cairn_common::wire_vocabulary::Measurement<T>,
    now_unix_ms: u64,
    share: impl Fn(&T) -> u32,
) -> Result<u32, cairn_common::executor_protocol::HostStallPrice> {
    use cairn_common::executor_protocol::HostStallPrice;
    use cairn_common::wire_vocabulary::MeasurementGap;
    match reading.value() {
        None => Err(HostStallPrice::Unmeasured {
            gap: reading.gap().unwrap_or(MeasurementGap::NotSampled),
        }),
        Some(_) if reading.age_ms(now_unix_ms) > EXECUTOR_TELEMETRY_STALE_AFTER_MS => {
            Err(HostStallPrice::Stale {
                age_ms: reading.age_ms(now_unix_ms),
            })
        }
        Some(value) => Ok(share(value).min(resource_profiles::HOST_STALL_PRICED_CEILING_MILLIS)),
    }
}

/// Add a machine's memory and storage stalls to its time to a verdict: a run
/// that would take `t` on a machine fully stalled a fraction `f` of the time
/// takes `t/(1-f)`, so the price is `t * f/(1-f)` on the contention-adjusted
/// run ([`resource_profiles::stall_penalty_ms`]). The run and its contention
/// were learned with the stall each sample began under divided out, so this
/// is the only place a stall is charged. A summand like the queue and the run,
/// never an exclusion and never an admission gate (tenet 1139): an unmeasured
/// or stale reading adds nothing and the record says why.
///
/// The two stalls overlap: a task swapping a page in is stalled on memory and
/// waiting on I/O at once, and PSI counts it in both. So the machine is priced
/// at the worse of the two, memory first and storage for only what it adds
/// beyond memory, and a thrashing host is never charged twice for one wait.
pub(super) fn price_host_stall(
    prediction: &mut PlacementPrediction,
    host: &cairn_common::executor_protocol::HostHealth,
    now_unix_ms: u64,
) {
    use cairn_common::executor_protocol::{HostStallPrice, IoPressure, MemoryPressure};
    use resource_profiles::stall_penalty_ms;
    let run_ms = prediction.adjusted_run_ms;
    let memory = priced_stall_share(
        &host.memory_pressure,
        now_unix_ms,
        MemoryPressure::priced_full_millis,
    );
    let memory_millis = *memory.as_ref().unwrap_or(&0);
    let memory_price = memory.map_or_else(
        |unpriced| unpriced,
        |full_millis| HostStallPrice::Priced {
            full_millis,
            penalty_ms: stall_penalty_ms(run_ms, full_millis),
        },
    );
    let io_price = priced_stall_share(
        &host.io_pressure,
        now_unix_ms,
        IoPressure::priced_full_millis,
    )
    .map_or_else(
        |unpriced| unpriced,
        |full_millis| HostStallPrice::Priced {
            full_millis,
            penalty_ms: stall_penalty_ms(run_ms, full_millis.max(memory_millis))
                .saturating_sub(stall_penalty_ms(run_ms, memory_millis)),
        },
    );
    prediction.predicted_verdict_ms = prediction
        .predicted_verdict_ms
        .saturating_add(memory_price.penalty_ms())
        .saturating_add(io_price.penalty_ms());
    prediction.memory_pressure = Some(memory_price);
    prediction.io_pressure = Some(io_price);
}

/// Add a machine's own same-signature failure history to its time to a
/// verdict. The penalty is a summand like the queue and the run, so the
/// earliest-verdict order and the preference deadline both see it, and it
/// never removes the machine from ranking.
pub(super) fn price_reliability(
    prediction: &mut PlacementPrediction,
    estimate: Option<ReliabilityEstimate>,
) {
    let Some(estimate) = estimate else {
        return;
    };
    prediction.predicted_verdict_ms = prediction
        .predicted_verdict_ms
        .saturating_add(estimate.penalty_ms);
    prediction.reliability = Some(estimate);
}

/// Record how one completion ended, when it says anything about the machine.
/// Best effort: a lost row costs one sample of history, never the work.
#[allow(clippy::too_many_arguments)]
pub(super) async fn record_command_outcome(
    db: Arc<crate::storage::LocalDb>,
    identity: &CommandResourceIdentity,
    os: &str,
    executor_id: &str,
    outcome: Option<reliability::CommandOutcome>,
    duration_ms: Option<u64>,
    finished_at_unix_ms: u64,
) {
    let Some(outcome) = outcome else {
        return;
    };
    if let Err(error) = reliability::record(
        db,
        identity,
        os,
        executor_id,
        &outcome,
        duration_ms,
        finished_at_unix_ms,
    )
    .await
    {
        tracing::warn!(%executor_id, %error, "could not record a command outcome");
    }
}

/// The prediction for a candidate that reached ranking without one.
///
/// Structurally unreachable today, and shaped so that if it ever does happen the
/// machine sorts last on an explicitly unknown queue and a labeled prior rather
/// than winning on an accidental zero.
pub(super) fn unpredicted_candidate(
    entry: &ExecutorConnectionState,
    request: &CellRequest,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    let context = ReservationPlan::context_for(
        &entry.identity.device_id,
        &entry.identity.executor_id,
        &entry.advertisement.capabilities,
    );
    placement_prediction_with_contention(
        entry,
        CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::FactsStale,
        },
        QueueForecast::Unknown {
            reason: QueueUnknownReason::FactsStale,
        },
        resource_profiles::unmeasured_duration(
            request.command_class,
            &context,
            request.command_resource_identity.as_ref(),
            ExecutionWarmth::Cold,
            DurationFallback::NoProfileStore,
        ),
        resource_profiles::contention_prior(&candidate_load(entry)),
        sync_cost,
    )
}

/// Whether this machine has already verified the exact commit the request names.
pub(super) fn warm_root_holds_commit(
    entry: &ExecutorConnectionState,
    request: &CellRequest,
) -> bool {
    entry
        .warm_roots
        .holds(&request.repository.identity(), &request.base_commit)
}

/// The three readings placement decides on, as this machine last reported them.
pub(super) fn placement_readings(
    machine: &cairn_common::executor_protocol::MachineTelemetry,
) -> PlacementReadings {
    PlacementReadings {
        cpu: machine.cpu.clone(),
        memory: machine.memory.clone(),
        volume: machine.volume.clone(),
    }
}

/// Why this machine's readings cannot be decided on, or `None` when they can.
///
/// A gap comes first because it is the stronger statement: a reading that does
/// not exist cannot also be judged fresh.
pub(super) fn placement_blindness(
    machine: &cairn_common::executor_protocol::MachineTelemetry,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    if let Some((measurement, gap)) = machine.placement_gaps().into_iter().next() {
        return Some(PlacementRejectionReason::TelemetryGap { measurement, gap });
    }
    [
        (MachineMeasurement::Cpu, machine.cpu.measured_at_unix_ms),
        (
            MachineMeasurement::Memory,
            machine.memory.measured_at_unix_ms,
        ),
        (
            MachineMeasurement::Volume,
            machine.volume.measured_at_unix_ms,
        ),
    ]
    .into_iter()
    .find_map(|(measurement, measured_at)| {
        let age_ms = now_unix_ms.saturating_sub(measured_at);
        (age_ms > EXECUTOR_TELEMETRY_STALE_AFTER_MS).then_some(
            PlacementRejectionReason::TelemetryStale {
                measurement,
                age_ms,
                stale_after_ms: EXECUTOR_TELEMETRY_STALE_AFTER_MS,
            },
        )
    })
}

/// Whether this machine is blind only for want of a first sample: some
/// placement reading is `NotSampled`, none carries any other gap, and every
/// reading it did take is within the staleness bound. A failed or stale
/// reading is a measurement that stopped, which is evidence (tenet 1441); a
/// reading never taken is not, and fails open (tenet 302).
pub(super) fn blind_only_for_want_of_a_first_sample(
    machine: &cairn_common::executor_protocol::MachineTelemetry,
    now_unix_ms: u64,
) -> bool {
    use cairn_common::wire_vocabulary::MeasurementGap;
    let gaps = machine.placement_gaps();
    !gaps.is_empty()
        && gaps
            .iter()
            .all(|(_, gap)| *gap == MeasurementGap::NotSampled)
        && [
            machine.cpu.value().map(|_| machine.cpu.age_ms(now_unix_ms)),
            machine
                .memory
                .value()
                .map(|_| machine.memory.age_ms(now_unix_ms)),
            machine
                .volume
                .value()
                .map(|_| machine.volume.age_ms(now_unix_ms)),
        ]
        .into_iter()
        .flatten()
        .all(|age_ms| age_ms <= EXECUTOR_TELEMETRY_STALE_AFTER_MS)
}

/// Whether this repository can be recreated on a machine that does not already
/// hold it. A colocated path is addressable through the object plane; a checkout
/// that already exists somewhere is that machine's, and nowhere else's.
pub(super) fn repository_is_transferable(repository: &RepositoryLocator) -> bool {
    !matches!(repository, RepositoryLocator::ExistingCheckout { .. })
}

pub(super) fn repository_locator_name(repository: &RepositoryLocator) -> &'static str {
    match repository {
        RepositoryLocator::ColocatedPath { .. } => "a colocated checkout",
        RepositoryLocator::ExistingCheckout { .. } => "an existing checkout",
        RepositoryLocator::ManagedObjects { .. } => "managed objects",
        RepositoryLocator::ScratchOnly { .. } => "scratch-only storage",
    }
}

/// The refusal for work that could be placed nowhere policy is allowed to see.
///
/// Named rather than degraded: running this locally without saying so is exactly
/// how a fleet of broken remotes stays invisible.
pub(super) fn no_measurable_executor_diagnostic(
    request: &CellRequest,
    rejected: &[PlacementRejection],
) -> String {
    let evaluated = rejected
        .iter()
        .map(|rejection| {
            format!(
                "{} ({})",
                rejection.executor_name,
                rejection.reason.describe()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "no executor could take this {} request: {evaluated}. Read cairn://executors for live state.",
        request.placement_mobility.as_str()
    )
}

/// The rationale for a reservation placement did not resolve, because the caller
/// stated one and nothing overruled it.
pub(super) fn unresolved_rationale(stated: &ResourceReservation) -> ReservationRationale {
    ReservationRationale {
        declared_concurrency_units: Some(stated.concurrency_units),
        profile_key: None,
        profile_context: String::new(),
        sample_count: 0,
        upper_peak_rss_bytes: None,
        upper_disk_growth_bytes: None,
        upper_duration_ms: None,
        prior: stated.clone(),
        headroom_percent: 0,
        fallback: Some(ReservationFallback::CallerDeclared),
        memory_kill: None,
        build_jobs_price: None,
        unreadable_memory: Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SyncCost {
    Known(u64),
    Unknown,
}

pub(super) fn sync_cost_key(cost: SyncCost) -> (bool, u64) {
    match cost {
        SyncCost::Known(bytes) => (false, bytes),
        SyncCost::Unknown => (true, 0),
    }
}

/// What moving this request's repository onto `entry` costs. The answers that
/// need no git are given here; the rest go to `price`, called with the local
/// repository path, the machine's warm roots for it, and the target commit.
pub(super) fn repository_sync_cost(
    request: &CellRequest,
    entry: &ExecutorConnectionState,
    price: impl FnOnce(&str, &[String], &str) -> SyncCost,
) -> SyncCost {
    if entry.colocated {
        return SyncCost::Known(0);
    }

    let repository = request.repository.identity();
    if entry.warm_roots.holds(&repository, &request.base_commit) {
        return SyncCost::Known(0);
    }

    let Some(repository_path) = request.repository.colocated_path() else {
        return SyncCost::Unknown;
    };
    price(
        repository_path,
        entry.warm_roots.commits(&repository),
        &request.base_commit,
    )
}

pub(super) fn aggregate_batch_learned_estimates(
    items: &[Option<cairn_common::executor_protocol::LearnedResourceEstimate>],
) -> Option<cairn_common::executor_protocol::LearnedResourceEstimate> {
    let mut items = items.iter();
    let mut aggregate = items.next()?.clone()?;
    for item in items {
        let item = item.as_ref()?;
        aggregate.sample_count = aggregate.sample_count.min(item.sample_count);
        aggregate.upper_duration_ms = match (aggregate.upper_duration_ms, item.upper_duration_ms) {
            (Some(total), Some(value)) => Some(total.saturating_add(value)),
            _ => None,
        };
        aggregate.upper_peak_rss_bytes =
            match (aggregate.upper_peak_rss_bytes, item.upper_peak_rss_bytes) {
                (Some(current), Some(value)) => Some(current.max(value)),
                _ => None,
            };
        aggregate.upper_disk_growth_bytes = match (
            aggregate.upper_disk_growth_bytes,
            item.upper_disk_growth_bytes,
        ) {
            (Some(current), Some(value)) => Some(current.max(value)),
            _ => None,
        };
    }
    Some(aggregate)
}

/// Resolve a declared concurrency demand against the executor that will run it.
///
/// Whole-machine demand is declared as saturation because a submitter cannot
/// know which executor will be chosen, so the clamp is what makes such a
/// declaration schedulable at all. It resolves to the executor's entire
/// admission budget — the same number that executor admits against, via
/// [`ExecutorCapabilities::admission_concurrency_budget`] — so the lane both
/// always fits and leaves no headroom beside it. Deriving the budget here
/// independently is what previously let an exclusive lane share a small host.
pub(super) fn clamp_declared_concurrency(
    declared: u32,
    capabilities: &ExecutorCapabilities,
) -> u32 {
    declared.min(capabilities.admission_concurrency_budget())
}

/// What a request would cost, resolved per candidate machine before any machine
/// is chosen.
///
/// Resource profiles are keyed by executor context (class, OS, architecture,
/// toolchains), so "what does this cost" has a different answer on every
/// candidate, and placement cannot rank on fit without asking each of them. This
/// splits that into two pure stages: derive the estimates, then rank. Nothing
/// here touches admission state; reserving capacity remains the executor's.
pub(super) struct ReservationPlan {
    pub(super) db: Arc<cairn_db::storage::LocalDb>,
    pub(super) command_class: cairn_common::executor_protocol::CellCommandClass,
    /// One identity per batch item, empty for a single-command request.
    pub(super) batch_identities: Vec<CommandResourceIdentity>,
    pub(super) request_identity: Option<CommandResourceIdentity>,
    /// Present when the caller declared its own concurrency demand, which covers
    /// concurrency and nothing else.
    pub(super) declared_concurrency: Option<u32>,
    /// False when the caller stated a complete reservation placement must not
    /// overrule.
    pub(super) resolves: bool,
    pub(super) stated: ResourceReservation,
    /// How many items run at once: a parallel batch's item count, otherwise 1.
    pub(super) parallel_width: u32,
    /// The suite a single-command request's evidence is learned under.
    pub(super) suite: Option<String>,
    /// Every batch item's identity, suite and command class, in item order,
    /// including the items no identity was minted for.
    pub(super) batch_suites: Vec<(
        Option<CommandResourceIdentity>,
        Option<String>,
        cairn_common::executor_protocol::CellCommandClass,
    )>,
}

impl ReservationPlan {
    pub(super) fn new(
        db: Arc<cairn_db::storage::LocalDb>,
        request: &CellRequest,
        batch: Option<&ProcessBatch>,
    ) -> Self {
        // A submitter can honestly declare its CONCURRENCY demand -- it knows
        // whether its command drives its own machine-wide job server -- while
        // still knowing nothing about memory or disk, which are learned per
        // command identity from observed runs. Treat a declaration as covering
        // only concurrency: resolve the learned profile exactly as for an
        // undeclared request, then re-apply the declaration over the result.
        // Without this, declaring demand would silently opt a command out of
        // every memory and disk estimate the fleet had learned about it.
        let declared_concurrency = (request.resource_reservation.source
            == ResourceReservationSource::Declared)
            .then_some(request.resource_reservation.concurrency_units);
        Self {
            db,
            command_class: request.command_class,
            batch_identities: batch
                .map(|batch| {
                    batch
                        .items
                        .iter()
                        .filter_map(|item| item.command_resource_identity.clone())
                        .collect()
                })
                .unwrap_or_default(),
            request_identity: request.command_resource_identity.clone(),
            declared_concurrency,
            resolves: (declared_concurrency.is_some()
                && request.resource_reservation.memory_bytes == 0
                && request.resource_reservation.disk_growth_bytes == 0)
                || request.resource_reservation == ResourceReservation::default(),
            stated: request.resource_reservation.clone(),
            parallel_width: batch.filter(|batch| !batch.sequential).map_or(1, |batch| {
                u32::try_from(batch.items.len()).unwrap_or(u32::MAX).max(1)
            }),
            suite: resource_profiles::suite_key(
                &request.project_id,
                request.command_resource_identity.as_ref(),
                request.command_class,
            ),
            batch_suites: batch
                .map(|batch| {
                    batch
                        .items
                        .iter()
                        .map(|item| {
                            let command = std::iter::once(item.program.as_str())
                                .chain(item.args.iter().map(String::as_str))
                                .collect::<Vec<_>>()
                                .join(" ");
                            let class = cairn_common::executor_protocol::CellCommandClass::classify(
                                &command,
                            );
                            (
                                item.command_resource_identity.clone(),
                                resource_profiles::suite_key(
                                    &request.project_id,
                                    item.command_resource_identity.as_ref(),
                                    class,
                                ),
                                class,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    /// The largest single process this work is known to hold on `context`'s
    /// platform, and its largest unattributed peak: a batch's heaviest item.
    pub(super) async fn process_evidence(
        &self,
        context: &resource_profiles::ProfileContext,
    ) -> resource_profiles::ProcessEvidence {
        if self.batch_suites.is_empty() {
            return resource_profiles::resolve_process_demand(
                self.db.clone(),
                self.request_identity.as_ref(),
                self.suite.as_deref(),
                self.command_class,
                context,
            )
            .await;
        }
        // Folded from the first item rather than from empty evidence, which
        // has no test phase and would take every batch's away.
        let mut evidence: Option<resource_profiles::ProcessEvidence> = None;
        for (identity, suite, class) in &self.batch_suites {
            let item = resource_profiles::resolve_process_demand(
                self.db.clone(),
                identity.as_ref(),
                suite.as_deref(),
                *class,
                context,
            )
            .await;
            evidence = Some(match evidence {
                Some(held) => resource_profiles::ProcessEvidence::max(held, item),
                None => item,
            });
        }
        evidence.unwrap_or_default()
    }

    pub(super) fn profile_context(
        selected: &SelectedExecutor,
    ) -> resource_profiles::ProfileContext {
        Self::context_for(
            &selected.device_id,
            &selected.executor_id,
            &selected.capabilities,
        )
    }

    pub(super) fn context_for(
        device_id: &str,
        executor_id: &str,
        capabilities: &ExecutorCapabilities,
    ) -> resource_profiles::ProfileContext {
        let mut toolchains = capabilities.toolchains.clone();
        toolchains.sort();
        resource_profiles::ProfileContext {
            executor_class: format!("{device_id}:{executor_id}"),
            os: capabilities.os.clone(),
            arch: capabilities.arch.clone(),
            toolchain_fingerprint: toolchains.join("\u{1f}"),
        }
    }

    /// What this work would be charged on one machine, and how long it would
    /// take there.
    ///
    /// Warmth is a per-candidate fact, so the duration half of this answer can
    /// differ between machines even where the reservation half does not: the
    /// same suite against a populated target directory and against an empty one
    /// is exactly the difference this ranking turns on.
    pub(super) async fn resolve_for(
        &self,
        request: &CellRequest,
        device_id: &str,
        executor_id: &str,
        capabilities: &ExecutorCapabilities,
        duration_context: resource_profiles::DurationContext,
    ) -> resource_profiles::ResolvedResourceProfile {
        let context = Self::context_for(device_id, executor_id, capabilities);
        if !self.resolves {
            // A caller-stated reservation is not a caller-stated duration. The
            // prediction still comes from this machine's own history, because a
            // submitter that knows its own memory demand still knows nothing
            // about how fast any particular host is.
            //
            // Whether one process fits is the machine's size, which no
            // caller's statement changes; nor is what this work's build jobs
            // measured here, which sizes the jobs the executor grants.
            let process_evidence = self.process_evidence(&context).await;
            let mut reservation = self.stated.clone();
            if let Some(units) = self.declared_concurrency {
                reservation.concurrency_units = clamp_declared_concurrency(units, capabilities);
            }
            reservation.build_job_memory = process_evidence.build_job_memory;
            return resource_profiles::ResolvedResourceProfile {
                reservation,
                learned_estimate: request.learned_estimate.clone(),
                rationale: resource_profiles::declared_rationale(&context, self.stated.clone()),
                duration: self.predict_duration(&context, duration_context).await,
                item_durations: Vec::new(),
                // A stated reservation is the caller's own number, not a
                // measurement this machine's cap could be hiding.
                memory_fit: resource_profiles::MemoryFit::Unknown,
                process_evidence,
            };
        }
        // Whether the caller declared concurrency is decided here, before the
        // learned lookup, because the two answers must both survive onto the
        // record. Replacing one explanation with the other is how a
        // caller-declared whole-machine charge came to read as "learned from 1
        // observation, fell back because belowConfidenceFloor" (CAIRN-3345):
        // the learning it named cannot produce a concurrency number at all.
        let prior = resource_profiles::cold_start_prior(self.command_class, capabilities);
        let class_prior = prior.clone();
        let mut resolved = if self.batch_suites.is_empty()
            || (self.parallel_width > 1 && self.batch_identities.is_empty())
        {
            resource_profiles::resolve_reservation(
                self.db.clone(),
                self.request_identity.as_ref(),
                &context,
                prior,
                capabilities.memory_budget_bytes,
                duration_context,
            )
            .await
        } else {
            self.resolve_batch(
                &context,
                prior,
                capabilities.memory_budget_bytes,
                duration_context,
                capabilities,
            )
            .await
        };
        // The executor runs a parallel batch as many items at a time as it was
        // charged units for, so a parallel batch is charged one item's units for
        // every item running beside the others. Charging its heaviest item alone,
        // which is right for a sequential batch, runs a parallel one item by item.
        // The machine's admission budget caps it, past which the items run in
        // waves rather than the batch never fitting at all.
        if self.parallel_width > 1 {
            resolved.reservation.concurrency_units = clamp_declared_concurrency(
                resolved
                    .reservation
                    .concurrency_units
                    .max(1)
                    .saturating_mul(self.parallel_width),
                capabilities,
            );
        }
        // Whole-machine demand is declared as saturation because the submitter
        // cannot know which executor would be chosen. Clamp it to this executor's
        // capacity: a reservation larger than the host's budget can never fit, so
        // leaving it unclamped would queue the request until its deadline rather
        // than running it.
        if let Some(units) = self.declared_concurrency {
            let clamped = clamp_declared_concurrency(units, capabilities);
            resolved.reservation.concurrency_units = clamped;
            resolved.rationale.declared_concurrency_units = Some(clamped);
        }
        if self.batch_suites.is_empty() || self.parallel_width > 1 {
            resolved.process_evidence = self.process_evidence(&context).await;
            if self.batch_suites.is_empty() && self.request_identity.is_none() {
                resolved.process_evidence.build_job_memory = None;
                resolved.process_evidence.build_jobs_elsewhere = None;
                resolved.process_evidence.test_phase = None;
            }
            resolved.charge_measured_peak();
            let cpu_width = capabilities
                .concurrency_capacity
                .unwrap_or_else(|| u32::try_from(capabilities.logical_cores).unwrap_or(u32::MAX));
            if self.parallel_width == 1 {
                resolved.price_at_width(cpu_width, &class_prior, capabilities.memory_budget_bytes);
            } else if resolved.reservation.source == ResourceReservationSource::Unmeasured {
                let fit = resolved.memory_fit;
                resolved.price_at_width(cpu_width, &class_prior, capabilities.memory_budget_bytes);
                resolved.memory_fit = fit;
            }
        }
        resolved
    }

    /// The duration half alone, for the paths that do not resolve a reservation.
    pub(super) async fn predict_duration(
        &self,
        context: &resource_profiles::ProfileContext,
        duration_context: resource_profiles::DurationContext,
    ) -> cairn_common::executor_protocol::DurationEstimate {
        if self.batch_identities.is_empty() {
            return resource_profiles::resolve_duration(
                self.db.clone(),
                self.request_identity.as_ref(),
                context,
                duration_context.warmth,
                duration_context.class,
                duration_context.now_unix_ms,
            )
            .await;
        }
        self.predict_batch_duration(context, duration_context).await
    }

    /// A batch's predicted run time is the SUM of its items, not the maximum.
    ///
    /// The items run one after another in the same cell, so the caller waits for
    /// all of them. This is the one place where duration and reservation compose
    /// in opposite directions: a batch is CHARGED for its heaviest item, because
    /// that is the peak it must fit under, and TAKES as long as all of them.
    pub(super) async fn predict_batch_duration(
        &self,
        context: &resource_profiles::ProfileContext,
        duration_context: resource_profiles::DurationContext,
    ) -> cairn_common::executor_protocol::DurationEstimate {
        let mut total: Option<cairn_common::executor_protocol::DurationEstimate> = None;
        for identity in &self.batch_identities {
            let item = resource_profiles::resolve_duration(
                self.db.clone(),
                Some(identity),
                context,
                duration_context.warmth,
                duration_context.class,
                duration_context.now_unix_ms,
            )
            .await;
            total = Some(match total {
                None => item,
                Some(mut running) => {
                    running.predicted_ms = running.predicted_ms.saturating_add(item.predicted_ms);
                    // A sum is only as measured as its least-measured leg, and
                    // only as confident as its thinnest one. Anything else would
                    // let one well-observed item dress up a batch nothing else
                    // in it has been seen doing.
                    if !item.is_learned() {
                        running.source = item.source;
                        running.fallback = item.fallback;
                    }
                    running.sample_count = running.sample_count.min(item.sample_count);
                    running.updated_at_unix_ms =
                        match (running.updated_at_unix_ms, item.updated_at_unix_ms) {
                            (Some(left), Some(right)) => Some(left.min(right)),
                            _ => None,
                        };
                    running
                }
            });
        }
        total.unwrap_or_else(|| {
            resource_profiles::unmeasured_duration(
                duration_context.class,
                context,
                None,
                duration_context.warmth,
                cairn_common::executor_protocol::DurationFallback::NoCommandIdentity,
            )
        })
    }

    pub(super) async fn resolve_batch(
        &self,
        context: &resource_profiles::ProfileContext,
        prior: ResourceReservation,
        memory_budget_bytes: Option<u64>,
        duration_context: resource_profiles::DurationContext,
        capabilities: &ExecutorCapabilities,
    ) -> resource_profiles::ResolvedResourceProfile {
        let mut reservation = ResourceReservation::default();
        let mut learned_estimates = Vec::with_capacity(self.batch_identities.len());
        let mut item_durations = Vec::with_capacity(self.batch_identities.len());
        // No item has spoken yet, so there is no rationale to seed: an unconsulted
        // batch must not carry a stand-in explanation that could survive as one.
        let mut rationale: Option<ReservationRationale> = None;
        // The batch must fit its heaviest item, so the item measured furthest
        // past the cap is the one that says this machine cannot hold it.
        let mut unreadable_memory = Vec::new();
        let mut memory_fits = Vec::with_capacity(self.batch_suites.len());
        let mut members = Vec::with_capacity(self.batch_suites.len());
        let mut process_evidence = None;
        let cpu_width = capabilities
            .concurrency_capacity
            .unwrap_or_else(|| u32::try_from(capabilities.logical_cores).unwrap_or(u32::MAX))
            .max(1);
        for (identity, suite, class) in &self.batch_suites {
            if self.parallel_width > 1 && identity.is_none() {
                continue;
            }
            let member_prior = if self.parallel_width == 1 {
                resource_profiles::cold_start_prior(*class, capabilities)
            } else {
                prior.clone()
            };
            let mut item = resource_profiles::resolve_reservation(
                self.db.clone(),
                identity.as_ref(),
                context,
                member_prior.clone(),
                memory_budget_bytes,
                if self.parallel_width == 1 {
                    resource_profiles::DurationContext {
                        class: *class,
                        ..duration_context
                    }
                } else {
                    duration_context
                },
            )
            .await;
            if self.parallel_width == 1 {
                item.process_evidence = resource_profiles::resolve_process_demand(
                    self.db.clone(),
                    identity.as_ref(),
                    suite.as_deref(),
                    *class,
                    context,
                )
                .await;
                if identity.is_none() {
                    item.process_evidence.build_job_memory = None;
                    item.process_evidence.build_jobs_elsewhere = None;
                    item.process_evidence.test_phase = None;
                }
                item.charge_measured_peak();
                item.price_at_width(cpu_width, &member_prior, memory_budget_bytes);
                process_evidence = Some(match process_evidence {
                    Some(held) => {
                        resource_profiles::ProcessEvidence::max(held, item.process_evidence.clone())
                    }
                    None => item.process_evidence.clone(),
                });
                members.push(item.reservation.clone());
            }
            reservation.memory_bytes = reservation.memory_bytes.max(item.reservation.memory_bytes);
            reservation.disk_growth_bytes = reservation
                .disk_growth_bytes
                .max(item.reservation.disk_growth_bytes);
            reservation.concurrency_units = reservation
                .concurrency_units
                .max(item.reservation.concurrency_units);
            reservation.source = match (reservation.source, item.reservation.source) {
                (ResourceReservationSource::Learned, _)
                | (_, ResourceReservationSource::Learned) => ResourceReservationSource::Learned,
                _ => ResourceReservationSource::Unmeasured,
            };
            // The batch is charged for its heaviest item, so that is the item
            // whose rationale explains the number.
            if rationale.is_none() || item.reservation.memory_bytes >= reservation.memory_bytes {
                rationale = Some(item.rationale.clone());
            }
            for failure in &item.rationale.unreadable_memory {
                if !unreadable_memory.contains(failure) {
                    unreadable_memory.push(failure.clone());
                }
            }
            memory_fits.push(item.memory_fit);
            learned_estimates.push(item.learned_estimate);
            item_durations.push(item.duration);
        }
        let mut memory_fit = resource_profiles::MemoryFit::of_batch(memory_fits);
        let mut evidence = process_evidence.unwrap_or_default();
        if self.parallel_width == 1 {
            // Policy may raise a reduced live advertisement back to startup
            // capacity after placement. Prove the envelope through the immutable
            // hardware budget that bounds startup and every policy reapply.
            let envelope_width = cairn_common::executor_protocol::admission_concurrency_budget(
                capabilities.logical_cores,
            );
            reservation = sequential_memory_envelope(&members, envelope_width);
            evidence.build_job_memory = reservation.build_job_memory;
            evidence.build_jobs_elsewhere = None;
            // A later member may compile autonomously. Without re-admission a
            // current member's test phase cannot release the future envelope.
            if members.len() > 1 {
                evidence.test_phase = None;
            }
            if reservation.build_job_memory.is_none()
                && memory_budget_bytes.is_some_and(|budget| {
                    reservation.memory_charge_bytes()
                        > resource_profiles::memory_reservation_cap(budget)
                })
                && !matches!(memory_fit, resource_profiles::MemoryFit::Exceeds(_))
            {
                memory_fit = resource_profiles::MemoryFit::of(
                    Some(reservation.memory_charge_bytes()),
                    memory_budget_bytes,
                );
            }
        }
        resource_profiles::ResolvedResourceProfile {
            reservation,
            learned_estimate: aggregate_batch_learned_estimates(&learned_estimates),
            rationale: {
                let mut rationale = rationale
                    .unwrap_or_else(|| resource_profiles::declared_rationale(context, prior));
                rationale.unreadable_memory = unreadable_memory;
                rationale
            },
            duration: self.predict_batch_duration(context, duration_context).await,
            item_durations,
            memory_fit,
            process_evidence: evidence,
        }
    }
}

/// One existing wire model may describe a sequential group only when an
/// actual member dominates every other member over the entire grant range.
/// Crossing curves retain their full-width envelope instead of synthesizing
/// an unmeasured max-largest/max-typical process model.
pub(super) fn sequential_memory_envelope(
    members: &[ResourceReservation],
    max_jobs: u32,
) -> ResourceReservation {
    if let [member] = members {
        return member.clone();
    }
    let max_jobs = max_jobs.max(1);
    let whole = members
        .iter()
        .map(ResourceReservation::memory_charge_bytes)
        .max()
        .unwrap_or(0);
    let mut boundaries = vec![1, max_jobs];
    for member in members {
        if let Some(model) = member.build_job_memory {
            let plateau = model.jobs_within(member.memory_charge_bytes());
            boundaries.push(plateau.clamp(1, max_jobs));
            boundaries.push(plateau.saturating_add(1).clamp(1, max_jobs));
        }
    }
    let dominant = members.iter().find(|candidate| {
        candidate.build_job_memory.is_some()
            && candidate.source == ResourceReservationSource::Learned
            && candidate.memory_charge_bytes() == whole
            && boundaries.iter().all(|width| {
                members.iter().all(|member| {
                    candidate.memory_charge_at(Some(*width))
                        >= member.memory_charge_at(Some(*width))
                })
            })
    });
    let mut reservation = dominant.cloned().unwrap_or_else(|| ResourceReservation {
        memory_bytes: members
            .iter()
            .map(|member| member.memory_charge_at(Some(max_jobs)))
            .max()
            .unwrap_or(0),
        ..ResourceReservation::default()
    });
    reservation.test_phase = None;
    reservation.disk_growth_bytes = members
        .iter()
        .map(|member| member.disk_growth_bytes)
        .max()
        .unwrap_or(0);
    reservation.concurrency_units = members
        .iter()
        .map(|member| member.concurrency_units)
        .max()
        .unwrap_or(1);
    reservation
}

pub(super) fn selected_executor(entry: &ExecutorConnectionState) -> SelectedExecutor {
    SelectedExecutor {
        executor_id: entry.identity.executor_id.clone(),
        device_id: entry.identity.device_id.clone(),
        generation: entry.generation,
        sender: entry.sender.clone(),
        colocated: entry.colocated,
        capabilities: entry.advertisement.capabilities.clone(),
    }
}

/// The fleet as a refusal has to describe it: every live machine by the name a
/// caller could have asked for, with what it runs and what it can build.
///
/// A refusal that names only what was wanted leaves the caller to guess what
/// exists, and guessing is what opaque identities forced. Naming both closes the
/// loop against `cairn://executors`, which is the same list from the same cache.
/// One machine's current repair identity: who it is, what its attach-time probes
/// measured, and any repair its operator has declared since.
///
/// The point of this value is that it MOVES when the machine is repaired and
/// stays put when it is not. A digest over advertised identity alone would not:
/// a machine reattaching with a missing toolchain still missing advertises the
/// same thing, and re-arming on that is how a broken host keeps consuming
/// budgets. So the identity is built from the EVIDENCE behind the advertisement
/// — the probe that ran, where the program resolved, and the version line or the
/// OS error that decided it.
///
/// Those facts are measured once, at attach, and that known seam is the design's
/// asset rather than its problem: attach is an already-existing, operator-driven
/// repair edge. Fix the machine, restart its executor, the probes re-run, this
/// value moves, and every check suppressed on the old state re-arms. An executor
/// that reattaches with IDENTICAL evidence does not re-arm, which is correct —
/// unchanged evidence is not a repair — and safe only because the check is not
/// trapped: it runs on a healthy machine instead.
///
/// [`repair::mark_repaired`] covers what no probe can see.
pub(super) fn environment_identity(entry: &ExecutorConnectionState, repair_epoch: i64) -> String {
    use sha2::Digest as _;
    let capabilities = &entry.advertisement.capabilities;
    let mut facts = vec![
        format!("device={}", entry.identity.device_id),
        format!("executor={}", entry.identity.executor_id),
        format!("repair={repair_epoch}"),
    ];
    match capabilities.toolchain_detection.as_ref() {
        // `account` and `home` are load-bearing rather than decoration: a
        // per-user install the executor's account cannot reach is a real,
        // invisible defect, and it cost CAIRN-3407 a live investigation.
        Some(detection) => {
            facts.push(format!(
                "account={}|home={}",
                detection.account, detection.home
            ));
            let mut probes: Vec<String> = detection
                .probes
                .iter()
                .map(|probe| {
                    format!(
                        "probe={}|detected={}|path={}|detail={}",
                        probe.toolchain,
                        probe.detected,
                        probe.resolved_path.as_deref().unwrap_or(""),
                        probe.detail
                    )
                })
                .collect();
            probes.sort();
            facts.extend(probes);
        }
        // "This peer cannot explain itself" and "this peer probed nothing" are
        // different facts, and the identity keeps them different.
        None => facts.push("probes=unreported".to_string()),
    }
    match capabilities.build_prerequisites.as_ref() {
        Some(check) => facts.push(format!(
            "prerequisites={}|{:?}|{}",
            check.script, check.status, check.detail
        )),
        None => facts.push("prerequisites=unreported".to_string()),
    }
    match capabilities.sandbox.as_ref() {
        // The context's fingerprint rather than its timestamp: a re-probe
        // under the same inputs is not a repair, a moved outer profile is.
        Some(evidence) => facts.push(format!(
            "sandbox={}|{}|{}|{}",
            evidence.cairn_sandboxed_marker,
            evidence.nested_sandbox_write_available,
            evidence.nested_sandbox_write_detail,
            evidence
                .nested_sandbox_write_context
                .as_ref()
                .map_or("no-context", |context| context
                    .fingerprint
                    .as_deref()
                    .unwrap_or("unfingerprinted"))
        )),
        None => facts.push("sandbox=unreported".to_string()),
    }
    // Whether recovery works, not its wording or time: installing the sudo
    // rule is a repair, a re-measurement that finds the same refusal is not.
    if let Some(recovery) = capabilities
        .sandbox
        .as_ref()
        .and_then(|evidence| evidence.violation_recovery.as_ref())
    {
        facts.push(format!(
            "violation-recovery={:?}|{}",
            recovery.access,
            match recovery.outcome {
                cairn_common::executor_protocol::ViolationRecoveryOutcome::Measured { .. } =>
                    "measured",
                cairn_common::executor_protocol::ViolationRecoveryOutcome::Unmeasured {
                    ..
                } => "unmeasured",
            }
        ));
    }
    format!("{:x}", sha2::Sha256::digest(facts.join("\n").as_bytes()))
}

pub(super) fn known_executor_inventory(
    connections: &HashMap<String, ExecutorConnectionState>,
) -> String {
    let mut rows: Vec<String> = connections
        .values()
        .filter(|entry| !entry.sender.is_closed())
        .map(|entry| {
            let capabilities = &entry.advertisement.capabilities;
            let toolchains = if capabilities.toolchains.is_empty() {
                "no advertised toolchains".to_string()
            } else {
                format!("toolchains {}", capabilities.toolchains.join(", "))
            };
            let sandbox = capabilities.sandbox.as_ref().map_or_else(
                || "nested Seatbelt write capability not reported".to_string(),
                nested_sandbox_summary,
            );
            // A role other than general is part of what the machine is, so a
            // reader of the inventory sees why it took nothing.
            let role = match entry.machine_role {
                MachineRole::General => String::new(),
                role => format!("{}, ", role.label()),
            };
            format!(
                "{} ({}, {role}{toolchains}, {sandbox})",
                executor_public_name(entry),
                capabilities.os
            )
        })
        .collect();
    rows.sort();
    if rows.is_empty() {
        "no executor is currently attached".to_string()
    } else {
        rows.join("; ")
    }
}

pub(super) fn presence_cause(link: &RemoteLinkState) -> String {
    match link {
        RemoteLinkState::Connected { .. } => "connected".into(),
        RemoteLinkState::NeverConnected { .. } => "installed; waiting for first connection".into(),
        RemoteLinkState::Absent { last_reason, .. } if !last_reason.is_empty() => {
            last_reason.clone()
        }
        RemoteLinkState::Absent { .. } => "absent".into(),
        RemoteLinkState::Bootstrapping { phase } => format!("bootstrap is in progress ({phase:?})"),
        RemoteLinkState::BootstrapFailed { cause } => cause.clone(),
        RemoteLinkState::BootstrapHeld { precondition } => {
            format!("bootstrap held by the runner — {}", precondition.describe())
        }
    }
}

/// Enrollment proves identity, platform and owned holds even without a link.
/// Advertisement-only requirements are unknown until reattach, not mismatches.
pub(super) fn detached_candidate_rejection(
    machine: &EnrolledRemote,
    request: &CellRequest,
    now_unix_ms: u64,
) -> PlacementRejectionReason {
    let scope = PlacementScope::of(request);
    let pinned_here = scope.pinned_executor_id == Some(machine.executor_id.as_str());
    if let Some(pinned) = scope.pinned_executor_id.filter(|_| !pinned_here) {
        return PlacementRejectionReason::PinMismatch {
            pinned_executor_id: pinned.to_string(),
        };
    }
    if scope.selector.is_some_and(|selector| {
        selector
            .name
            .as_deref()
            .is_some_and(|name| !executor_names_match(name, &machine.name))
            || selector
                .os
                .as_deref()
                .is_some_and(|os| !os.eq_ignore_ascii_case(&machine.os))
    }) {
        return PlacementRejectionReason::SelectorMismatch {
            requested: scope
                .selector
                .map(|selector| selector.describe())
                .unwrap_or_default(),
        };
    }
    if !scope.verdict_platforms.is_empty()
        && !scope
            .verdict_platforms
            .iter()
            .any(|os| os.eq_ignore_ascii_case(&machine.os))
    {
        return PlacementRejectionReason::UntrustedVerdictPlatform {
            os: machine.os.clone(),
            trusted: scope.verdict_platforms.to_vec(),
        };
    }
    if !scope.mobility.may_spill() && !scope.targeted() {
        return PlacementRejectionReason::NotColocated;
    }
    if !repository_is_transferable(scope.repository) {
        return PlacementRejectionReason::RepositoryNotTransferable {
            locator: repository_locator_name(scope.repository).into(),
        };
    }
    if scope
        .avoided_executor_ids
        .iter()
        .any(|id| id == &machine.executor_id)
    {
        return PlacementRejectionReason::AvoidedInfrastructure;
    }
    let role_accepts = match scope.publication {
        Some(_) => machine.role.takes_publication_legs(),
        None => machine.role.accepts(scope.work_class),
    };
    if !role_accepts && !pinned_here {
        return PlacementRejectionReason::RoleExcluded {
            role: machine.role,
            work_class: scope.work_class,
            publication_leg: scope.publication.is_some(),
        };
    }
    let blockers = drain_projection::refusing(
        &machine.drain_state.blockers,
        drain_projection::resident_work(request, &machine.executor_id),
    );
    use cairn_common::executor_protocol::{ExecutorDrainOwner, ExecutorRuntimeDrainKind};
    let update_only = !machine.drain_state.records.is_empty()
        && machine.drain_state.records.len() == machine.drain_state.blockers.len()
        && machine.drain_state.records.iter().all(|record| {
            matches!(
                &record.owner,
                ExecutorDrainOwner::Runtime { context }
                    if context.kind == ExecutorRuntimeDrainKind::SelfUpdate
            )
        });
    let updating = machine.update_returns_by_unix_ms.is_some() || update_only;
    if (!blockers.is_empty() && !update_only)
        || (machine.drained && machine.drain_state.blockers.is_empty() && !updating)
    {
        return PlacementRejectionReason::Drained;
    }
    let since = machine
        .absence_since_unix_ms
        .or(machine.last_seen_unix_ms)
        .unwrap_or(0);
    let returns_by = machine
        .update_returns_by_unix_ms
        .unwrap_or_else(|| since.saturating_add(ENVIRONMENT_LEASE_GRACE_MS));
    if now_unix_ms >= returns_by || machine.unreturned_update.is_some() {
        PlacementRejectionReason::AbsenceExpired {
            absent_since_unix_ms: since,
        }
    } else if updating {
        PlacementRejectionReason::UpdatePending {
            returns_by_unix_ms: Some(returns_by),
        }
    } else {
        PlacementRejectionReason::Reconnecting {
            returns_by_unix_ms: returns_by,
        }
    }
}

pub(super) fn absent_enrolled_inventory(
    connections: &HashMap<String, ExecutorConnectionState>,
    unattached: &[EnrolledRemote],
    request: &CellRequest,
) -> String {
    let selector = request
        .executor
        .as_ref()
        .filter(|selector| !selector.is_empty());
    let pinned = request.pinned_executor_id.as_deref();
    // A pin is answered on its own terms below, so it is deliberately not one of
    // the constraints that make this inventory speak about the fleet at large.
    let targeted = selector.is_some() || !request.verdict_platforms.is_empty();
    let mut rows: Vec<_> = unattached
        .iter()
        .filter(|machine| {
            // A pin names one machine by identity, and `candidate_rejection`
            // refuses every other machine outright for a pinned request. Listing
            // the rest would send an operator to fix a host this work was never
            // going to land on.
            if let Some(pinned) = pinned {
                return machine.executor_id == pinned;
            }
            if let Some(name) = selector.and_then(|selector| selector.name.as_deref()) {
                return machine.name == name;
            }
            let selector_os = selector.and_then(|selector| selector.os.as_deref());
            selector_os.is_none_or(|os| os.eq_ignore_ascii_case(&machine.os))
                && (request.verdict_platforms.is_empty()
                    || request
                        .verdict_platforms
                        .iter()
                        .any(|os| os.eq_ignore_ascii_case(&machine.os)))
        })
        .map(|machine| {
            let reason = detached_candidate_rejection(machine, request, unix_time_ms());
            let cause = match reason {
                PlacementRejectionReason::Reconnecting { .. }
                | PlacementRejectionReason::UpdatePending { .. }
                | PlacementRejectionReason::AbsenceExpired { .. } => {
                    format!("{}; {}", reason.describe(), presence_cause(&machine.link))
                }
                _ if machine.drained => "drained, refusing new work".to_string(),
                _ => presence_cause(&machine.link),
            };
            format!(
                "{} ({}/{}) — {cause}",
                machine.name, machine.os, machine.arch
            )
        })
        .collect();
    rows.sort();
    if !rows.is_empty() {
        return format!("Absent enrolled machines: {}.", rows.join("; "));
    }
    // The pinned machine is not an absent enrollment, so this inventory has
    // nothing to add about it. A platform negative here would describe a
    // fleet-wide gap the pin never asked about and that may not exist.
    if pinned.is_some() {
        return String::new();
    }
    if targeted {
        if let Some(os) = selector
            .and_then(|selector| selector.os.as_deref())
            // Matched as a slice rather than counted and indexed. An index has
            // to be guarded by a length check, and the eager spelling of that
            // guard reads the element before the check runs; a slice pattern
            // cannot be evaluated too early because it tests the length itself.
            .or(match request.verdict_platforms.as_slice() {
                [only] => Some(only.as_str()),
                _ => None,
            })
        {
            // Only a fleet-wide negative is worth saying here, and an attached
            // machine on that platform makes it false: it is listed under
            // known executors with whatever kept it from this request.
            let attached_on_os = connections.values().any(|entry| {
                !entry.sender.is_closed()
                    && entry.advertisement.capabilities.os.eq_ignore_ascii_case(os)
            });
            if !attached_on_os {
                return format!("No enrolled machine on {os} exists.");
            }
        }
    }
    String::new()
}

/// Per machine, the containment edge that blocked it.
///
/// "No machine qualifies" sends a reader nowhere. Naming the deficient edge and
/// its reason -- unmeasured, stale build, prior boot, changed or uncomputable
/// fingerprint, failed positive control -- is the difference between a refusal
/// that describes a gap in Cairn and one that describes a machine an operator
/// can fix.
pub(super) fn containment_refusal_inventory(
    connections: &HashMap<String, ExecutorConnectionState>,
    selector: &ExecutorSelector,
) -> Option<String> {
    selector.containment.as_ref()?;
    let mut rows: Vec<String> = connections
        .values()
        .filter(|entry| !entry.sender.is_closed())
        .filter_map(|entry| {
            containment_deficiency(&entry.advertisement.capabilities, selector)
                .map(|reason| format!("{}: {reason}", executor_public_name(entry)))
        })
        .collect();
    rows.sort();
    (!rows.is_empty()).then(|| rows.join("; "))
}

/// Every machine a refusal passed over, each with its own reason, as one
/// `name: reason; ...` list, or nothing.
///
/// A refusal that names only the machine nearest to qualifying leaves the
/// reader to ask why every other one was excluded, and role, capability and
/// capacity send an operator to entirely different places. A selector
/// mismatch is stated as the part of the selector that failed rather than the
/// whole selector restated. Machines whose only shortfall is running another
/// platform are counted rather than listed: the platform is the whole story
/// for each of them.
pub(super) fn describe_exclusions(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    rejected: &[PlacementRejection],
) -> String {
    let selector = request
        .executor
        .as_ref()
        .filter(|selector| !selector.is_empty());
    let mut rows = Vec::new();
    let mut elsewhere: Vec<&str> = Vec::new();
    let mut wanted_platform = String::new();
    for rejection in rejected {
        let shortfall = match (&rejection.reason, selector) {
            (PlacementRejectionReason::SelectorMismatch { .. }, Some(selector)) => connections
                .get(&rejection.executor_id)
                .and_then(|entry| selector_shortfall(entry, selector)),
            _ => None,
        };
        match shortfall {
            Some(SelectorShortfall::Platform { wanted }) => {
                wanted_platform = wanted;
                elsewhere.push(&rejection.executor_name);
            }
            Some(shortfall) => rows.push(format!(
                "{}: {}",
                rejection.executor_name,
                shortfall.describe()
            )),
            None => rows.push(format!(
                "{}: {}",
                rejection.executor_name,
                rejection.reason.describe()
            )),
        }
    }
    match elsewhere.as_slice() {
        [] => {}
        [only] => rows.push(format!("{only}: not {wanted_platform}")),
        many => rows.push(format!(
            "{} {}machines: not {wanted_platform}",
            many.len(),
            if rows.is_empty() { "" } else { "other " }
        )),
    }
    rows.join("; ")
}

/// Why nothing in the fleet can take this request, in the terms the caller used
/// plus the terms it could have used instead.
pub(super) fn no_matching_executor_diagnostic(
    connections: &HashMap<String, ExecutorConnectionState>,
    unattached: &[EnrolledRemote],
    request: &CellRequest,
    rejected: &[PlacementRejection],
) -> String {
    if let Some(pinned) = request.pinned_executor_id.as_deref() {
        if let Some(rejection) = rejected
            .iter()
            .find(|rejection| rejection.executor_id == pinned)
        {
            return format!(
                "the machine holding this job's working tree ({}) cannot take this batch: {}. Read cairn://executors for live state.",
                rejection.executor_name,
                rejection.reason.describe()
            );
        }
    }

    // A caller that named the machine is owed the machine's role, the class
    // it asked for, and who can change the one so it accepts the other.
    let named = request
        .executor
        .as_ref()
        .and_then(|selector| selector.name.as_deref());
    let role_target = named.and_then(|named| {
        rejected.iter().find(|rejection| {
            matches!(
                rejection.reason,
                PlacementRejectionReason::RoleExcluded { .. }
            ) && executor_names_match(named, &rejection.executor_name)
        })
    });
    if let Some(rejection) = role_target {
        return format!(
            "{name} {}. Its role is set by an operator on cairn://executors/{name}.",
            rejection.reason.describe(),
            name = rejection.executor_name,
        );
    }

    // Whichever machine the refusal leads with, every other one is named with
    // what excluded it.
    let account = match describe_exclusions(connections, request, rejected) {
        rows if rows.is_empty() => String::new(),
        rows => format!(" Per machine: {rows}."),
    };
    let would_serve = |rejection: &&PlacementRejection| {
        connections
            .get(&rejection.executor_id)
            .is_some_and(|entry| {
                request
                    .executor
                    .as_ref()
                    .filter(|selector| !selector.is_empty())
                    .is_none_or(|selector| matches_selector(entry, selector))
                    && serves_project(entry, &request.project_id)
            })
    };
    let drained_target = rejected.iter().find(|rejection| {
        matches!(rejection.reason, PlacementRejectionReason::Drained) && would_serve(rejection)
    });
    if let Some(rejection) = drained_target {
        let blockers = rejection
            .drain_blockers
            .iter()
            .map(|blocker| {
                format!(
                    "{} generation {}: {}{}",
                    blocker.owner,
                    blocker.generation,
                    blocker.reason,
                    blocker
                        .estimate
                        .as_ref()
                        .map(|estimate| format!(" (estimate {estimate})"))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return format!(
            "{} is drained, so the runner is refusing new work there. Blocking records/fences: {blockers}. Read cairn://executors for live state.{account}",
            rejection.executor_name
        );
    }
    let updating_target = rejected.iter().find(|rejection| {
        matches!(
            rejection.reason,
            PlacementRejectionReason::UpdatePending { .. }
        ) && would_serve(rejection)
    });
    if let Some(rejection) = updating_target {
        let back = match &rejection.reason {
            PlacementRejectionReason::UpdatePending {
                returns_by_unix_ms: Some(at),
            } => format!(
                ", expected by {}",
                cairn_common::executor_protocol::describe_clock(*at)
            ),
            _ => String::new(),
        };
        return format!(
            "{name} is being emptied for an executor update, so the runner is refusing new work there until it returns on the runner's build{back}. This clears on its own; cairn://executors/{name} shows how far along it is.{account}",
            name = rejection.executor_name
        );
    }
    let model_target = rejected.iter().find(|rejection| {
        matches!(
            rejection.reason,
            PlacementRejectionReason::ModelResident { .. }
        ) && would_serve(rejection)
    });
    if let Some(rejection) = model_target {
        return format!(
            "on {name}, {}. cairn://inference/pools shows the models placement holds and when each may go.{account}",
            rejection.reason.describe(),
            name = rejection.executor_name
        );
    }

    // A pin and a selector are different halves of the same failure, and a
    // refusal reporting only the pin tells an agent its batch was misplaced when
    // what actually failed is the request it wrote. The caller's own words
    // appear whenever the caller supplied any.
    let pinned = request.pinned_executor_id.as_deref().map(|pinned| {
        // The caller never chose the pin, so it is owed the public name rather
        // than the identity placement happened to use.
        match connections
            .values()
            .find(|entry| entry.identity.executor_id == pinned)
            .map(executor_public_name)
        {
            Some(name) => format!("the machine holding this job's working tree ({name})"),
            None => "the machine holding this job's working tree".to_string(),
        }
    });
    let asked = request
        .executor
        .as_ref()
        .filter(|selector| !selector.is_empty())
        .map(|selector| selector.describe());
    let wanted = match (pinned, asked) {
        (Some(pinned), Some(asked)) => format!("{pinned}, which must also satisfy {asked}"),
        (Some(pinned), None) => pinned,
        (None, Some(asked)) => asked,
        (None, None) => "any executor".to_string(),
    };
    // Trust narrows the same fleet the selector does, and a refusal that reports
    // only half of what was applied sends the reader looking for a machine that
    // was passed over for the other half.
    let wanted = if request.verdict_platforms.is_empty() {
        wanted
    } else {
        format!(
            "{wanted} on {} (the platform(s) this verdict counts from)",
            request.verdict_platforms.join(", ")
        )
    };
    // A containment refusal is not a capacity problem and must not read like
    // one. When the envelope is what excluded the fleet, the edge that failed on
    // each machine is the only actionable part of the message.
    let containment = request
        .executor
        .as_ref()
        .and_then(|selector| containment_refusal_inventory(connections, selector))
        .map(|rows| format!(" Containment evidence: {rows}."))
        .unwrap_or_default();
    let other_name = request
        .executor
        .as_ref()
        .and_then(|selector| selector.name.as_deref())
        .and_then(|wanted| own_label_refusal(connections, wanted))
        .unwrap_or_default();
    format!(
        "no live enrolled executor satisfies {wanted} for project {}.{account}{other_name} Known executors: {}. {}{containment} Read cairn://executors for live state.",
        request.project_id,
        known_executor_inventory(connections),
        absent_enrolled_inventory(connections, unattached, request),
    )
}

/// The sentence a refusal owes a request that named a machine by the label its
/// own executor.json carries rather than the name this runner addresses it by,
/// naming both. `None` when no attached machine carries `wanted` as its label.
pub(super) fn own_label_refusal(
    connections: &HashMap<String, ExecutorConnectionState>,
    wanted: &str,
) -> Option<String> {
    connections.values().find_map(|entry| {
        let public = executor_public_name(entry);
        (!entry.colocated
            && executor_names_match(wanted, &entry.advertisement.identity.display_name)
            && !executor_names_match(wanted, &public))
        .then(|| {
            format!(
                " {wanted} is the name {public}'s own executor.json carries, not its address: this runner has it enrolled as {public}, and the fleet addresses it as {public}. Name {public}, or rename it with `cairn executor rename {public} {wanted}`."
            )
        })
    })
}
