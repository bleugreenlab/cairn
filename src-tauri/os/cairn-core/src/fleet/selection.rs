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
    /// Set when the dispatcher assigned this to hold room for aged work that
    /// fits no announced room (tenet 1444): the offer carries it.
    pub(super) headroom: Option<cairn_common::executor_protocol::HeadroomAuthorization>,
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
        // to fix. So is the state of a required toolchain's probe: a machine
        // whose probe could not finish lacks it in a different way from one
        // that measured it absent.
        let measured_requirement = selector.filter(|selector| {
            selector.containment.is_some() || !selector.page_call_programs.is_empty()
        });
        let mut known: Vec<String> = connections
            .values()
            .map(|entry| {
                let capabilities = &entry.advertisement.capabilities;
                let mut toolchains = capabilities.toolchains.clone();
                toolchains.sort();
                let shortfall = selector
                    .and_then(|selector| selector_shortfall(entry, selector))
                    .filter(|shortfall| {
                        measured_requirement.is_some()
                            || matches!(
                                shortfall,
                                SelectorShortfall::Toolchains { states, .. } if !states.is_empty()
                            )
                    })
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
        headroom: Option<cairn_common::executor_protocol::HeadroomAuthorization>,
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
                self.dispatcher_requeued(entry_id);
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
            summary.headroom = headroom;
            summary
        });
        let reserving = summary
            .as_ref()
            .is_some_and(|summary| summary.headroom.is_some());
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
        if reached {
            self.counters.offer(reserving);
        }
        if !reached {
            if let PlacementOutcome::Refused { diagnostic } = &decision.outcome {
                self.queue.retract_unsent_offer(
                    entry_id,
                    &selected.executor_id,
                    selected.generation,
                    diagnostic,
                    unix_time_ms(),
                );
                // A drain hold refused it here, before the machine heard of
                // it. Admission reads the same holds, so it is not handed
                // back to this machine while they stand.
                self.dispatcher_requeued(entry_id);
                self.record_binding_chain(decision, entry_id);
                self.record_placement_decision(decision.clone());
                return OfferAnswer::Requeued;
            }
            let recorded = self.queue.decline(
                entry_id,
                &selected.executor_id,
                selected.generation,
                DeclineReason::Unknown,
                Some("the link closed before the offer reached the machine".into()),
                None,
                unix_time_ms(),
            );
            if matches!(recorded, queue::DeclineRecord::Recorded { .. }) {
                self.dispatcher_declined(entry_id, &selected.executor_id);
            }
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
    /// ending, and a single place has to observe all of it. The entry's own
    /// wake ends each wait, so nothing else's claim or refusal does, and the
    /// only clocks are this entry's: the link-freshness budget after the
    /// offer, and the requester's horizon.
    pub(super) async fn await_offer_answer(
        &self,
        entry_id: &str,
        horizon_unix_ms: u64,
    ) -> OfferAnswer {
        let Some(wake) = self.queue.wake_of(entry_id) else {
            return OfferAnswer::Gone;
        };
        loop {
            let notified = wake.notified();
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
                    let unanswered_at = at_unix_ms.saturating_add(EXECUTOR_PROGRESS_FRESHNESS_MS);
                    if now >= unanswered_at {
                        // The machine's own refusal may have ended the offer
                        // since it was read: the queue then calls this
                        // decline stale, and the dispatcher, which heard that
                        // refusal, is not told twice. A claim landing in the
                        // same window is not told apart: the queue matches a
                        // claimed entry by machine and link as well.
                        let recorded = self.queue.decline(
                            entry_id,
                            &executor_id,
                            generation,
                            DeclineReason::Unknown,
                            Some("the machine did not answer the offer".into()),
                            None,
                            now,
                        );
                        if matches!(recorded, queue::DeclineRecord::Recorded { .. }) {
                            self.dispatcher_declined(entry_id, &executor_id);
                        }
                        return OfferAnswer::Requeued;
                    }
                    let until = unanswered_at.min(horizon_unix_ms);
                    let _ = tokio::time::timeout(
                        Duration::from_millis(until.saturating_sub(now).max(1)),
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
        let surveyed = match survey_candidates(&connections, &unattached, request, unix_time_ms()) {
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

    /// The main-cadence lanes that hold precedence for a machine's freed room
    /// now. The purpose registry's lock is released before the queue's is
    /// taken, so the two are never held together.
    pub(super) fn cadence_precedence(&self) -> queue::CadencePrecedence {
        self.purposes.lock().unwrap().main_cadence_holders()
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

    /// What a request no machine has claimed is waiting on, as one phrase:
    /// since when, then the account of it the check row and the placement
    /// view read ([`Self::queued_request_account`]). `None` once a machine
    /// has claimed it, and when the queue does not hold it.
    pub(crate) fn unplaced_wait(&self, request_id: &str) -> Option<String> {
        let CheckDispatchPhase::Placing {
            since_unix_ms,
            declines,
            placement,
            turn,
        } = self.queued_request_account(request_id)?
        else {
            return None;
        };
        let since = chrono::DateTime::from_timestamp_millis(since_unix_ms as i64)
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_else(|| "an unknown time".to_string());
        let waited = unix_time_ms().saturating_sub(since_unix_ms) / 1000;
        let mut phrase = format!("waiting to be placed since {since} ({waited}s)");
        match crate::execution::checks::describe_queued_hold(
            &declines,
            placement.as_ref(),
            turn.as_ref(),
        ) {
            Some(held) => phrase.push_str(&format!("; {held}")),
            None => phrase.push_str(", and no machine has refused it yet"),
        }
        Some(phrase)
    }
}

/// The machines that survived the structural filters, and the ones that did not
/// with the reason each was passed over.
#[derive(Clone)]
pub(super) struct CandidateSurvey<'a> {
    pub(super) usable: Vec<&'a ExecutorConnectionState>,
    pub(super) rejected: Vec<PlacementRejection>,
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
/// It answers what placement owes at once rather than at a horizon: whether
/// targeted work has anything usable, and whether work that cannot move has a
/// home that could ever take it. A request waiting for an executor to attach
/// surveys an empty fleet.
pub(super) fn survey_candidates<'a>(
    connections: &'a HashMap<String, ExecutorConnectionState>,
    unattached: &[EnrolledRemote],
    request: &CellRequest,
    now_unix_ms: u64,
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
    Ok(CandidateSurvey { usable, rejected })
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

/// The refusal a machine's size makes whatever it is doing now: a single
/// process of the work larger than it can give ([`ProcessFit::Refused`]).
/// A waiver or a deferral is not one, and nothing resolved for the machine,
/// or an unread total, is unknown rather than a misfit.
pub(super) fn hard_process_refusal(
    entry: &ExecutorConnectionState,
    reservation: Option<&resource_profiles::ResolvedResourceProfile>,
) -> Option<PlacementRejectionReason> {
    match process_fit(&reservation?.process_evidence, entry)? {
        ProcessFit::Refused(reason) => Some(reason),
        ProcessFit::Waived(_) | ProcessFit::Deferred(_) => None,
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
/// A measurement of this work borrowed from another platform refuses like one
/// taken here. A class prior defers instead: it is a number measured of other
/// work (its command class's peak, or the project's largest rustc), so a
/// machine too small for it is offered the work last rather than never, and
/// the first run there measures the work itself.
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
/// Work whose figure, its learned memory at this machine's compile slot width
/// (cairn/7303), is above the bound can never run under it. There is no
/// narrower width to try: a compile runs at its slot's width or not at all.
pub(super) fn executor_bound_rejection(
    entry: &ExecutorConnectionState,
    demand: &ResourceReservation,
) -> Option<PlacementRejectionReason> {
    let bound_bytes = executor_memory_bound(entry)?;
    let charge_bytes = demand.memory_charge_bytes();
    (charge_bytes > bound_bytes).then_some(PlacementRejectionReason::ExceedsExecutorMemoryBound {
        charge_bytes,
        bound_bytes,
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
        slot_price: None,
        unreadable_memory: Vec::new(),
    }
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
    /// Each batch item's identity, index-aligned with the items (`None` for
    /// an item that has none), empty for a single-command request. Learning
    /// zips it against the items' outcomes by position, so it must never drop
    /// an item.
    pub(super) batch_identities: Vec<Option<CommandResourceIdentity>>,
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
    /// Every profile scope this request's needs are read from: its identities,
    /// its suites, and their projects.
    needs_scopes: Vec<String>,
    /// What this request needs on each machine context, worked out when the
    /// context was first asked about and again only when a profile write
    /// names one of `needs_scopes`.
    needs: std::sync::Mutex<HashMap<String, WorkedOutNeeds>>,
    /// Needs worked out since the last [`Self::take_needs_worked_out`].
    needs_worked_out: std::sync::atomic::AtomicU64,
}

/// One machine context's needs for a request, and what they were derived from.
struct WorkedOutNeeds {
    capabilities: ExecutorCapabilities,
    stamp: profile_reads::NeedsStamp,
    resolved: resource_profiles::ResolvedResourceProfile,
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
        // The one place a batch item's identity is decided: the submitter's,
        // else one minted for an ad-hoc Rust compile, so an agent's `cargo
        // test -p small` is learned as itself rather than as its class.
        let batch_suites: Vec<_> = batch
            .map(|batch| {
                batch
                    .items
                    .iter()
                    .map(|item| {
                        let command = std::iter::once(item.program.as_str())
                            .chain(item.args.iter().map(String::as_str))
                            .collect::<Vec<_>>()
                            .join(" ");
                        let class =
                            cairn_common::executor_protocol::CellCommandClass::classify(&command);
                        let identity = item.command_resource_identity.clone().or_else(|| {
                            resource_profiles::ad_hoc_identity(&request.project_id, &command, class)
                        });
                        let suite = resource_profiles::suite_key(
                            &request.project_id,
                            identity.as_ref(),
                            class,
                        );
                        (identity, suite, class)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let suite = resource_profiles::suite_key(
            &request.project_id,
            request.command_resource_identity.as_ref(),
            request.command_class,
        );
        let mut needs_scopes: Vec<String> = request
            .command_resource_identity
            .iter()
            .chain(
                batch_suites
                    .iter()
                    .filter_map(|(identity, _, _)| identity.as_ref()),
            )
            .map(profile_reads::identity_scope)
            .collect();
        for key in suite.iter().chain(
            batch_suites
                .iter()
                .filter_map(|(_, suite, _)| suite.as_ref()),
        ) {
            needs_scopes.extend(profile_reads::suite_scopes(key));
        }
        needs_scopes.sort();
        needs_scopes.dedup();
        Self {
            db,
            command_class: request.command_class,
            batch_identities: batch_suites
                .iter()
                .map(|(identity, _, _)| identity.clone())
                .collect(),
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
            suite,
            batch_suites,
            needs_scopes,
            needs: Default::default(),
            needs_worked_out: Default::default(),
        }
    }

    /// How many (context, request) needs were worked out since this was last
    /// asked, which is the number of times a parked request cost the profile
    /// store anything.
    pub(super) fn take_needs_worked_out(&self) -> u64 {
        self.needs_worked_out
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// What this request's needs on every machine are derived from now; a
    /// write that moves it means they may have changed.
    pub(super) fn needs_stamp(&self) -> profile_reads::NeedsStamp {
        profile_reads::needs_stamp(&self.db, &self.needs_scopes)
    }

    /// The batch items' identities, skipping items that have none.
    pub(super) fn known_batch_identities(&self) -> impl Iterator<Item = &CommandResourceIdentity> {
        self.batch_identities.iter().flatten()
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
        // The needs are a function of the request, the machine's context and
        // size, and what the profile store has learned about the request's
        // identities and suites. None of those changes between two passes
        // unless a writer says so, so a pass that finds the stamp unchanged
        // reads nothing. Only the duration depends on the pass (it is
        // warmth-keyed and ranking alone reads it), so it is read fresh.
        //
        // Rows nothing here wrote (retention's pruning) are not noticed: a
        // request parked past a prune keeps the larger evidence it was queued
        // under, which is the safe side of the error.
        let stamp = profile_reads::needs_stamp(&self.db, &self.needs_scopes);
        let held = self
            .needs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&context.executor_class)
            .filter(|held| held.stamp == stamp && held.capabilities == *capabilities)
            .map(|held| held.resolved.clone());
        let mut resolved = match held {
            Some(resolved) => resolved,
            None => {
                let resolved = self
                    .work_out_needs(request, &context, capabilities, duration_context)
                    .await;
                self.needs_worked_out
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.needs
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(
                        context.executor_class.clone(),
                        WorkedOutNeeds {
                            capabilities: capabilities.clone(),
                            stamp,
                            resolved: resolved.clone(),
                        },
                    );
                resolved
            }
        };
        resolved.duration = self.predict_duration(&context, duration_context).await;
        resolved
    }

    #[cfg(test)]
    pub(super) fn needs_held(&self) -> usize {
        self.needs.lock().unwrap().len()
    }

    /// [`Self::resolve_for`]'s figures, read from the profile store.
    ///
    /// A compile's figure is its learned memory at this machine's compile
    /// slot width ([`ResolvedResourceProfile::charge_measured_peak`]), priced
    /// once, and it asks one slot's width of cores (cairn/7303). A batch's
    /// figure is derived here and nowhere else: a sequential group runs one
    /// member at a time at that one width, so it is its heaviest member's
    /// figure; a parallel one is charged for its items at once. A member's
    /// figure is its own context's, so a wave's figure is the same whichever
    /// pass or placement path asks.
    async fn work_out_needs(
        &self,
        request: &CellRequest,
        context: &resource_profiles::ProfileContext,
        capabilities: &ExecutorCapabilities,
        duration_context: resource_profiles::DurationContext,
    ) -> resource_profiles::ResolvedResourceProfile {
        let context = context.clone();
        let slot_width = cairn_common::executor_protocol::CompileSlots::of(
            capabilities.admission_concurrency_budget(),
        )
        .width;
        // A compile asks at least one compile slot's width of cores, as the
        // machine judges it (`Need::of`), so the reservation it is charged and
        // releases is the one it was judged by. A compile that declared the
        // whole machine (an exclusive check) keeps it.
        let compile = self.command_class.is_compile_class();
        if !self.resolves {
            // A caller-stated reservation is not a caller-stated duration. The
            // prediction still comes from this machine's own history, because a
            // submitter that knows its own memory demand still knows nothing
            // about how fast any particular host is.
            //
            // Whether one process fits is the machine's size, which no
            // caller's statement changes.
            let process_evidence = self.process_evidence(&context).await;
            let mut reservation = self.stated.clone();
            if let Some(units) = self.declared_concurrency {
                reservation.concurrency_units = clamp_declared_concurrency(units, capabilities);
            }
            if compile {
                reservation.concurrency_units = reservation.concurrency_units.max(slot_width);
            }
            return resource_profiles::ResolvedResourceProfile {
                reservation,
                learned_estimate: request.learned_estimate.clone(),
                rationale: resource_profiles::declared_rationale(&context, self.stated.clone()),
                duration: self.predict_duration(&context, duration_context).await,
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
        let mut resolved = if self.batch_suites.is_empty()
            || (self.parallel_width > 1 && self.known_batch_identities().next().is_none())
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
        if compile {
            resolved.reservation.concurrency_units =
                resolved.reservation.concurrency_units.max(slot_width);
        }
        if self.batch_suites.is_empty() || self.parallel_width > 1 {
            resolved.process_evidence = self.process_evidence(&context).await;
            resolved.charge_measured_peak(capabilities.memory_budget_bytes, slot_width);
        }
        resolved
    }

    /// The duration half alone, for the paths that do not resolve a reservation.
    pub(super) async fn predict_duration(
        &self,
        context: &resource_profiles::ProfileContext,
        duration_context: resource_profiles::DurationContext,
    ) -> cairn_common::executor_protocol::DurationEstimate {
        if self.known_batch_identities().next().is_none() {
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
        for identity in self.known_batch_identities() {
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
        // No item has spoken yet, so there is no rationale to seed: an unconsulted
        // batch must not carry a stand-in explanation that could survive as one.
        let mut rationale: Option<ReservationRationale> = None;
        // The batch must fit its heaviest item, so the item measured furthest
        // past the cap is the one that says this machine cannot hold it.
        let mut unreadable_memory = Vec::new();
        let mut memory_fits = Vec::with_capacity(self.batch_suites.len());
        let mut members = Vec::with_capacity(self.batch_suites.len());
        let mut process_evidence = None;
        let slot_width = cairn_common::executor_protocol::CompileSlots::of(
            capabilities.admission_concurrency_budget(),
        )
        .width;
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
                item.charge_measured_peak(memory_budget_bytes, slot_width);
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
        }
        let mut memory_fit = resource_profiles::MemoryFit::of_batch(memory_fits);
        let mut evidence = process_evidence.unwrap_or_default();
        if self.parallel_width == 1 {
            // One member runs at a time, at this machine's one slot width, so
            // the group needs what its heaviest member needs: that member's
            // figure is the group's, with the largest disk and units of any.
            if let Some(heaviest) = members
                .iter()
                .max_by_key(|member| member.memory_charge_bytes())
            {
                reservation = ResourceReservation {
                    disk_growth_bytes: reservation.disk_growth_bytes,
                    concurrency_units: reservation.concurrency_units,
                    ..heaviest.clone()
                };
            }
            // A later member may compile autonomously. Without re-admission a
            // current member's test phase cannot release a later member's
            // charge.
            if members.len() > 1 {
                reservation.test_phase = None;
                evidence.test_phase = None;
            }
            if memory_budget_bytes.is_some_and(|budget| {
                reservation.memory_charge_bytes()
                    > resource_profiles::memory_reservation_cap(budget)
            }) && !matches!(memory_fit, resource_profiles::MemoryFit::Exceeds(_))
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
            memory_fit,
            process_evidence: evidence,
        }
    }
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
