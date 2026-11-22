use super::*;

fn linked_check_machine(name: &str) -> String {
    format!("[{name}](cairn://executors/{name})")
}

pub(super) fn describe_ms(value_ms: u64) -> String {
    let seconds = value_ms / 1_000;
    match (seconds / 60, seconds % 60) {
        (0, seconds) => format!("{seconds}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, seconds) => format!("{minutes}m{seconds}s"),
    }
}

/// A wait as an operator reads it in a status line, in whole minutes so a line
/// that quotes it changes at most once a minute.
pub(super) fn describe_waited(value_ms: u64) -> String {
    let minutes = value_ms / 60_000;
    match (minutes / 60, minutes % 60) {
        (0, 0) => "under a minute".into(),
        (0, minutes) => format!("{minutes}m"),
        (hours, 0) => format!("{hours}h"),
        (hours, minutes) => format!("{hours}h{minutes}m"),
    }
}

/// A wall-clock instant as a status line names it: `09:48:09Z`.
pub(super) fn clock_utc(unix_ms: u64) -> String {
    i64::try_from(unix_ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            || format!("{unix_ms}ms"),
            |at| at.format("%H:%M:%SZ").to_string(),
        )
}

pub(super) fn decline_reason_words(
    reason: cairn_common::executor_protocol::DeclineReason,
) -> &'static str {
    use cairn_common::executor_protocol::DeclineReason;
    match reason {
        DeclineReason::Capacity => "full",
        DeclineReason::Pressure => "host pressure",
        DeclineReason::Disk => "low disk",
        DeclineReason::Constraint => "cannot run it",
        DeclineReason::Unknown => "declined",
        DeclineReason::ProjectProvisioning => "provisioning another cell for this project",
        DeclineReason::BuildVolumeAbsent => "its build volume is not mounted",
    }
}

/// One machine's decline in its own words, and the room it was given under.
pub(crate) fn describe_decline(decline: &crate::fleet::CheckDispatchDecline) -> String {
    let reason = decline_reason_words(decline.reason);
    let mut words = match decline
        .detail
        .as_deref()
        .filter(|detail| !detail.is_empty())
    {
        Some(detail) => format!("{reason}: {detail}"),
        None => reason.to_string(),
    };
    if let Some(seq) = decline.room_seq {
        words.push_str(&format!("; under its room #{seq}"));
    }
    format!("{} ({words})", decline.executor_name)
}

/// Every machine that declined a parked request, latest first, so a wait that
/// is really a disk or pressure hold says so instead of reading as a queue.
pub(super) fn describe_declines(declines: &[crate::fleet::CheckDispatchDecline]) -> Option<String> {
    (!declines.is_empty()).then(|| {
        let named: Vec<String> = declines.iter().map(describe_decline).collect();
        format!("declined by {}", named.join(", "))
    })
}

/// What holds a queued request, as one sentence: where it stands with each
/// machine (or every machine's latest refusal, when it is not waiting in
/// the dispatcher), then the turn it takes. The one account of a queued
/// request (cairn/6499): the check board writes it on a check request's
/// row, an executor's placement view renders it for any queued request, and
/// a run's wait text quotes it.
pub(crate) fn describe_queued_hold(
    declines: &[crate::fleet::CheckDispatchDecline],
    placement: Option<&crate::fleet::CheckPlacementAccount>,
    turn: Option<&String>,
) -> Option<String> {
    let candidates = match placement {
        Some(account) => Some(describe_placement_account(account, declines)),
        None => describe_declines(declines),
    };
    let held: Vec<String> = [candidates, turn.cloned()].into_iter().flatten().collect();
    (!held.is_empty()).then(|| held.join("; "))
}

/// Where a queued request stands with every machine, so a wait whose
/// machines have no room, have said nothing yet, or are away says which,
/// and silence never reads as a lack of room. A refusal under a room the
/// machine has since replaced is told as superseded, never as the reason
/// now. A decline from a machine the account does not name (one since
/// detached) is still told, as earlier.
pub(super) fn describe_placement_account(
    account: &crate::fleet::CheckPlacementAccount,
    declines: &[crate::fleet::CheckDispatchDecline],
) -> String {
    use crate::fleet::CheckCandidateStatus;
    use cairn_common::executor_protocol::PlacementRejectionReason;
    let mut offered = Vec::new();
    let mut declined = Vec::new();
    let mut each = Vec::new();
    let mut superseded = Vec::new();
    // One clause per distinct reason, its machines in the account's order.
    let mut not_offered: Vec<(String, Vec<&str>)> = Vec::new();
    for machine in &account.machines {
        let name = machine.executor_name.as_str();
        let placed_name = linked_check_machine(name);
        if let Some(old) = &machine.superseded {
            superseded.push(match old.current_seq {
                Some(seq) => format!(
                    "{}, room #{seq} announced since",
                    describe_decline(&old.decline)
                ),
                None => format!("{}, from an earlier link", describe_decline(&old.decline)),
            });
        }
        match &machine.status {
            CheckCandidateStatus::Offered => offered.push(format!("offered to {placed_name}, unanswered")),
            CheckCandidateStatus::Declined(decline) => declined.push(decline.clone()),
            CheckCandidateStatus::OfferUndelivered { room_seq } => each.push(format!(
                "{name}: the offer under its room #{room_seq} did not reach it"
            )),
            CheckCandidateStatus::NoRoomFits {
                needs,
                room_seq,
                since_unix_ms,
            } => each.push(format!(
                "{name}: no announced room fits: needs {needs} (room #{room_seq}, since {})",
                clock_utc(*since_unix_ms)
            )),
            CheckCandidateStatus::RefusedTwice { room_seq } => each.push(format!(
                "{name} refused twice under its current room #{room_seq}; waiting for it to announce"
            )),
            CheckCandidateStatus::HoldingRoom { room_seq, hold } => each.push(match hold {
                Some(hold) => format!("{placed_name} holding room for it ({hold}; room #{room_seq})"),
                None => format!(
                    "{placed_name} asked to hold room for it, not yet in its announced room #{room_seq}"
                ),
            }),
            CheckCandidateStatus::RoomNotAnnounced {
                attached_at_unix_ms,
            } => each.push(match attached_at_unix_ms {
                Some(at) => format!(
                    "{name} has not announced room (attached {})",
                    clock_utc(*at)
                ),
                None => format!("{name} has not announced room"),
            }),
            CheckCandidateStatus::LinkStale { since_unix_ms } => each.push(format!(
                "{name}: link stale since {}",
                clock_utc(*since_unix_ms)
            )),
            CheckCandidateStatus::Away { sentence, .. } => {
                each.push(format!("{placed_name}: away for now: {sentence}"))
            }
            CheckCandidateStatus::Fits { room_seq } => each.push(format!(
                "{name}: its room #{room_seq} fits; offered on its next announcement"
            )),
            CheckCandidateStatus::WaitsFor {
                executor_name,
                room_seq,
                first_room,
            } => each.push(match first_room {
                Some((remaining_ms, reason)) => format!(
                    "{name}: its room #{room_seq} fits; waiting up to {} s for {executor_name} to announce room, preferred for {reason}",
                    remaining_ms.div_ceil(1_000),
                ),
                None => format!(
                    "{name}: its room #{room_seq} fits, but the work waits for {executor_name}, which could hold it and ranks ahead on memory"
                ),
            }),
            CheckCandidateStatus::NotOffered(PlacementRejectionReason::CadenceTurn { .. }) => {}
            CheckCandidateStatus::NotOffered(reason) => {
                let why = reason.describe();
                match not_offered.iter_mut().find(|(told, _)| *told == why) {
                    Some((_, names)) => names.push(name),
                    None => not_offered.push((why, vec![name])),
                }
            }
        }
    }
    let earlier: Vec<String> = declines
        .iter()
        .filter(|decline| {
            !account
                .machines
                .iter()
                .any(|machine| machine.executor_name == decline.executor_name)
        })
        .map(describe_decline)
        .collect();
    let mut clauses = offered;
    clauses.extend(account.machines.iter().flat_map(|machine| {
        machine
            .unreadable_memory
            .iter()
            .map(|failure| format!("{}: {}", machine.executor_name, failure.describe()))
    }));
    clauses.extend(describe_declines(&declined));
    clauses.extend(each);
    clauses.extend(
        not_offered
            .into_iter()
            .map(|(why, names)| format!("not offered to {} ({why})", names.join(", "))),
    );
    if !superseded.is_empty() {
        clauses.push(format!("superseded refusals: {}", superseded.join(", ")));
    }
    if !earlier.is_empty() {
        clauses.push(format!("earlier declined by {}", earlier.join(", ")));
    }
    let header = format!(
        "placement of request {} (attempt {})",
        account.request_id, account.attempt_id
    );
    if clauses.is_empty() {
        header
    } else {
        format!("{header}: {}", clauses.join("; "))
    }
}

/// How often an outstanding pure-verdict batch re-reads where its execution
/// stands.
pub(super) const DISPATCH_PROGRESS_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(2);

/// How stale a parked batch's occupancy forecast may grow before it is read
/// again.
pub(super) const PARKED_FORECAST_REFRESH_MS: u64 = 30_000;

/// The status a pure-verdict batch shows while the fleet holds it, rendered
/// from where its execution stands ([`crate::fleet::CheckDispatch`]).
///
/// A forecast retires itself. The first finite relief forecast `F` for a wait
/// is remembered with when it was made; once the wait has outlasted `2F`, or
/// the machines' occupancy reads as unforecastable, the wait is reported as
/// unpredictable and no finite forecast is shown for it again. A forecast that
/// kept being quoted while it went unmet is the line that told a requester
/// "predicted to finish in 4m52s" for three hours.
pub(super) struct DispatchNarrator {
    pub(super) wait: GroupWait,
    pub(super) wait_read_at_unix_ms: u64,
    pub(super) first_forecast: Option<FirstForecast>,
    pub(super) unpredictable: bool,
    /// A machine claimed the execution. Back in the queue after that (its
    /// machine was lost), it is a new wait with its own forecast.
    pub(super) claimed: bool,
    pub(super) shown: Option<CheckPhaseStatus>,
}

#[derive(Clone, Copy)]
pub(super) struct FirstForecast {
    pub(super) relief_ms: u64,
    pub(super) at_unix_ms: u64,
}

impl DispatchNarrator {
    pub(super) fn new(wait: GroupWait, now_unix_ms: u64) -> Self {
        let mut narrator = Self {
            wait: wait.clone(),
            wait_read_at_unix_ms: now_unix_ms,
            first_forecast: None,
            unpredictable: false,
            claimed: false,
            shown: None,
        };
        narrator.absorb_wait(wait, now_unix_ms);
        narrator
    }

    /// Track the claim, and start a fresh wait when a claimed execution is
    /// queued again: the earlier wait's forecast and its verdict on that
    /// forecast belong to that wait alone, and its occupancy reading is stale.
    pub(super) fn note_phase(&mut self, phase: &crate::fleet::CheckDispatchPhase) {
        match phase {
            crate::fleet::CheckDispatchPhase::Running { .. } => self.claimed = true,
            crate::fleet::CheckDispatchPhase::Placing { .. } if self.claimed => {
                self.claimed = false;
                self.first_forecast = None;
                self.unpredictable = false;
                self.wait_read_at_unix_ms = 0;
            }
            _ => {}
        }
    }

    pub(super) fn wait_is_stale(&self, now_unix_ms: u64) -> bool {
        now_unix_ms.saturating_sub(self.wait_read_at_unix_ms) >= PARKED_FORECAST_REFRESH_MS
    }

    pub(super) fn absorb_wait(&mut self, wait: GroupWait, now_unix_ms: u64) {
        self.unpredictable |= wait.unforecastable;
        if self.first_forecast.is_none() && !self.unpredictable {
            self.first_forecast = wait.relief_ms.map(|relief_ms| FirstForecast {
                relief_ms,
                at_unix_ms: now_unix_ms,
            });
        }
        self.wait = wait;
        self.wait_read_at_unix_ms = now_unix_ms;
    }

    /// The status to show for `dispatch`, or `None` when it is what was last
    /// shown.
    pub(super) fn next(
        &mut self,
        dispatch: &crate::fleet::CheckDispatch,
        now_unix_ms: u64,
    ) -> Option<CheckPhaseStatus> {
        let status = self.render(dispatch, now_unix_ms);
        if self.shown.as_ref() == Some(&status) {
            return None;
        }
        self.shown = Some(status.clone());
        Some(status)
    }

    pub(super) fn render(
        &mut self,
        dispatch: &crate::fleet::CheckDispatch,
        now_unix_ms: u64,
    ) -> CheckPhaseStatus {
        use crate::fleet::CheckDispatchPhase;
        let joined = dispatch.joined.as_deref();
        let joined_prefix = joined
            .map(|owner| format!("joined {owner}'s run; "))
            .unwrap_or_default();
        match &dispatch.phase {
            CheckDispatchPhase::Running {
                executor_name,
                slot,
                since_unix_ms,
            } => {
                let at = clock_utc(*since_unix_ms);
                let machine = linked_check_machine(executor_name);
                let location = match slot {
                    Some(slot) => format!("{machine}, slot {slot}"),
                    None => format!("{machine}, slot pending"),
                };
                CheckPhaseStatus {
                    executor: Some(executor_name.clone()),
                    since_unix_ms: Some(*since_unix_ms),
                    ..check_phase_status(
                        Some("running"),
                        Some("running checks"),
                        Some(match joined {
                            Some(owner) => {
                                format!("joined {owner}'s run on {location} since {at}")
                            }
                            None => format!("running on {location} since {at}"),
                        }),
                    )
                }
            }
            CheckDispatchPhase::Preparing { since_unix_ms } => CheckPhaseStatus {
                since_unix_ms: Some(*since_unix_ms),
                ..check_phase_status(
                    Some("provisioning"),
                    Some("preparing"),
                    Some(format!(
                        "{joined_prefix}preparing the check's inputs since {}",
                        clock_utc(*since_unix_ms)
                    )),
                )
            },
            CheckDispatchPhase::Placing {
                since_unix_ms,
                declines,
                placement,
                turn,
            } => {
                if let Some(first) = self.first_forecast {
                    if now_unix_ms.saturating_sub(first.at_unix_ms)
                        > first.relief_ms.saturating_mul(2)
                    {
                        self.unpredictable = true;
                    }
                }
                let since = clock_utc(*since_unix_ms);
                let mut diagnostic = match (self.unpredictable, self.first_forecast) {
                    (true, Some(first)) => format!(
                        "parked since {since}; wait unpredictable (forecast {} at {}, waited {})",
                        describe_ms(first.relief_ms),
                        clock_utc(first.at_unix_ms),
                        describe_waited(now_unix_ms.saturating_sub(*since_unix_ms)),
                    ),
                    (true, None) => format!(
                        "parked since {since}; wait unpredictable: what holds the eligible machines has no measured duration"
                    ),
                    (false, _) => format!("{}; parked since {since}", self.wait.description),
                };
                // Main's cadence turn is what holds a lane passed over behind
                // it as surely as a decline is, and it is what the cadence
                // lane itself waits for, so it is named beside the declines.
                let held_by = describe_queued_hold(declines, placement.as_ref(), turn.as_ref());
                if let Some(declined) = &held_by {
                    diagnostic.push_str("; ");
                    diagnostic.push_str(declined);
                }
                CheckPhaseStatus {
                    since_unix_ms: Some(*since_unix_ms),
                    held_by,
                    ..check_phase_status(
                        Some("queued"),
                        Some("queued"),
                        Some(format!("{joined_prefix}{diagnostic}")),
                    )
                }
            }
        }
    }
}

/// Where a verdict request's execution stands, read by its subscriber
/// identity: its own run, or the one it coalesced onto.
pub(super) fn check_dispatch(
    fleet: &crate::fleet::Fleet,
    subscriber: &(String, String),
) -> Option<crate::fleet::CheckDispatch> {
    fleet.check_dispatch_phase(subscriber)
}

/// Await `submission`, showing where its execution stands every
/// [`DISPATCH_PROGRESS_INTERVAL`] and only when that changes.
///
/// The board said `queued` from submission until settlement before this
/// existed: the fleet's pure-verdict path reports no item start to a board, so
/// a check that a machine claimed in two seconds and ran for three hours read
/// as parked behind the forecast it was submitted with for all three.
pub(super) async fn narrate_dispatch<T>(
    submission: impl std::future::Future<Output = T>,
    wait: &GroupWait,
    read_dispatch: impl Fn() -> Option<crate::fleet::CheckDispatch>,
    refresh_wait: impl Fn() -> GroupWait,
    show: impl Fn(CheckPhaseStatus),
) -> T {
    let mut narrator = DispatchNarrator::new(wait.clone(), unix_time_ms_for_checks());
    tokio::pin!(submission);
    let mut ticks = tokio::time::interval(DISPATCH_PROGRESS_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            delivered = &mut submission => return delivered,
            _ = ticks.tick() => {
                let Some(dispatch) = read_dispatch() else {
                    continue;
                };
                let now = unix_time_ms_for_checks();
                narrator.note_phase(&dispatch.phase);
                if matches!(dispatch.phase, crate::fleet::CheckDispatchPhase::Placing { .. })
                    && narrator.wait_is_stale(now)
                {
                    narrator.absorb_wait(refresh_wait(), now);
                }
                if let Some(status) = narrator.next(&dispatch, now) {
                    show(status);
                }
            }
        }
    }
}

/// How much of one machine's stored diagnostic an aggregate suppression message
/// spends on it. A single-domain message quotes its one row whole; an
/// every-machine message quotes several, and an unbounded share each would let
/// the list crowd out the repair instruction that is the point of the message.
pub(super) const SUPPRESSION_MACHINE_DIAGNOSTIC_CHARS: usize = 800;

/// The agent-facing text for a check Cairn declined to run.
///
/// The behavior is one sentence, and it must name the repair that would actually
/// re-arm this check. "Re-runs automatically on the next commit" is true only of
/// the head domain; saying it about a standing machine or fleet defect is what
/// trains an agent to keep committing at a broken machine. The stored diagnostic
/// follows inline so the reader sees the cause without chasing an operator-only
/// log.
pub(crate) fn suppressed_check_message(suppressed: &SuppressedCheck) -> String {
    let streak = suppressed.streak();
    match suppressed.domain {
        InfraRepairDomain::Head => {
            let head = format!(
                "Stopped after {streak} infrastructure failures on this commit; re-runs automatically on the next commit."
            );
            let last = suppressed.evidence().output_tail.trim();
            if last.is_empty() {
                head
            } else {
                format!("{head}\n\nLast diagnostic:\n{last}")
            }
        }
        InfraRepairDomain::HeadOnPlatform => {
            let platforms = suppressed
                .scopes
                .iter()
                .map(|scope| scope.platform.as_deref().unwrap_or("an unnamed platform"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut message = format!(
                "Stopped after {streak} infrastructure failures on this commit on every platform this check counts ({platforms}); re-runs automatically on the next commit."
            );
            for scope in &suppressed.scopes {
                let last = scope.entry.output_tail.trim();
                if last.is_empty() {
                    continue;
                }
                let platform = scope.platform.as_deref().unwrap_or("an unnamed platform");
                message.push_str(&format!(
                    "\n\nLast diagnostic on {platform}:\n{}",
                    tail_lines(last, SUPPRESSION_MACHINE_DIAGNOSTIC_CHARS)
                ));
            }
            message
        }
        InfraRepairDomain::Environment => {
            let machines = suppressed
                .scopes
                .iter()
                .map(|scope| scope.executor_id.as_deref().unwrap_or("an unnamed machine"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut message = format!(
                "Stopped after {streak} infrastructure failures on every machine that can run this check ({machines}); a new commit will not re-run it. \
                 Repair a machine and restart its executor so its measured facts change, or declare the repair with `write cairn://executors/<name> {{repaired: true}}`."
            );
            for scope in &suppressed.scopes {
                let last = scope.entry.output_tail.trim();
                if last.is_empty() {
                    continue;
                }
                let machine = scope.executor_id.as_deref().unwrap_or("an unnamed machine");
                message.push_str(&format!(
                    "\n\nLast diagnostic on {machine}:\n{}",
                    tail_lines(last, SUPPRESSION_MACHINE_DIAGNOSTIC_CHARS)
                ));
            }
            message
        }
        InfraRepairDomain::Fleet => {
            // Two populations, two sentences: a fleet suppression keyed on
            // blind machines lifts when they can be measured again, and one
            // keyed on the capability set alone lifts only when it changes.
            let lifts = if suppressed.fleet_blind.is_empty() {
                "It runs again when the set of machines matching its requirements changes."
                    .to_string()
            } else {
                format!(
                    "Cairn could not read the current load of {}, so it runs again as soon as their telemetry is fresh, or when the set of machines matching its requirements changes.",
                    suppressed.fleet_blind.join(", ")
                )
            };
            let head = format!(
                "Stopped after this check died on {streak} separate commits against the fleet it can reach (retries of one commit count once); a new commit will not re-run it. {lifts}"
            );
            let last = suppressed.evidence().output_tail.trim();
            if last.is_empty() {
                head
            } else {
                format!("{head}\n\nLast diagnostic:\n{last}")
            }
        }
        InfraRepairDomain::Lane => {
            let entry = suppressed.evidence();
            let head = format!(
                "Not run: {}. Re-running it cannot produce a verdict, so review waves hold it; run it by hand with `cairn check run {}` to re-test it now.",
                lane_breaker_sentence(suppressed),
                entry.check_name
            );
            let last = entry.output_tail.trim();
            if last.is_empty() {
                head
            } else {
                format!("{head}\n\nLast diagnostic:\n{last}")
            }
        }
    }
}

/// The one sentence a held review lane is named by, on every surface: how
/// many identical failures tripped it, the persisted signature they share and
/// the line it was computed from, and the facts that release it.
pub(crate) fn lane_breaker_sentence(suppressed: &SuppressedCheck) -> String {
    let signature = suppressed.lane_signature().unwrap_or_default();
    format!(
        "tripped after {} identical infrastructure failures (sig {}: \"{}\"); waits for a new input, a change in the machines that can run it, or a genuine verdict for this input",
        suppressed.streak(),
        crate::execution::lane_breaker::short_signature(signature),
        crate::execution::lane_breaker::evidence_excerpt(&suppressed.evidence().output_tail),
    )
}

/// Render the inline pass/fail summary appended to the originating tool result.
/// The first line is the compact per-check status
/// (`\u{2713} frontend \u{b7} \u{2717} typecheck (exit 1)`); each failing check
/// then gets a detail block naming the failing tests and a bounded output
/// excerpt, so the agent learns WHAT broke without re-running the suite. A red
/// test lane in `attributions` splits its tally by what main at the branch's
/// base already fails (`3 of 900 failed; 1 is this branch's`) and lists its
/// failures by bucket, as the turn-checks wake does. Pure, so it is unit-tested
/// directly.
pub(crate) fn format_check_summary(
    results: &[CheckOutcome],
    attributions: &HashMap<String, crate::execution::checks_base::BaseAttribution>,
) -> String {
    let header = results
        .iter()
        .map(|o| {
            let mark = if holds_no_verdict(o) {
                // No verdict, so there is no red to mark.
                '\u{26a0}'
            } else if !o.passed {
                '\u{2717}'
            } else if o
                .parsed
                .as_ref()
                .is_some_and(|p| p.is_incomplete_lint() || p.unverified.is_some())
            {
                // An exit zero that measured nothing, or that its own run
                // disowned, is not a green.
                '\u{26a0}'
            } else {
                '\u{2713}'
            };
            match summary_annotation(o) {
                Some(ann) => format!("{mark} {} ({ann})", o.name),
                None => format!("{mark} {}", o.name),
            }
        })
        .collect::<Vec<_>>()
        .join(" \u{b7} ");

    let mut out = header;
    for o in results.iter().filter(|o| !o.passed && !holds_no_verdict(o)) {
        if let Some(detail) = format_check_detail(o, attributions.get(&o.name)) {
            out.push_str("\n\n");
            out.push_str(&detail);
        }
    }
    out
}

/// The parenthetical annotation for one check's status line, or `None` when there
/// is nothing worth adding beyond the bare `\u{2713}`/`\u{2717} <name>`. This is
/// the trust-carrying part of the summary: it turns four indistinguishable greens
/// (a real N-test pass, a zero-selection vacuous pass, a reused cache hit, and a
/// pass whose suite skipped part of itself) into four visibly different lines.
///
/// - Passing TEST-RUNNER check: `12 tests`, or `no tests matched the change`
///   when the selector executed zero tests (a `related` run that matched nothing).
/// - Any skipped test is named: `12 tests, 3 skipped`, and `no tests ran, 12
///   skipped` for a suite that skipped ITSELF entirely — the shape that let a
///   whole cross-surface suite read green while validating nothing (CAIRN-3164).
///   A skip is not a pass, so it can never be silent in the verdict.
/// - Passing non-test check (tsc/api/dead-code): `4.1s` on a fresh run (duration
///   is the only meaningful signal; a test count would be a lie).
/// - Failing TEST-RUNNER check: `2 of 40 failed, exit 101`, or `1 suite failed
///   to load, exit 1` when a file threw during collection and so ran no test,
///   or `3 undeclared skips, exit 1` when the skip ledger failed a run nothing
///   else failed.
/// - Failing non-test check: `exit 101`, or `failed to run` on a spawn error.
/// - A cache hit appends `cached` so a reused verdict never masquerades as fresh.
///
/// Pure, so it is unit-tested directly.
pub(super) fn summary_annotation(o: &CheckOutcome) -> Option<String> {
    // A check Cairn declined to run has no verdict to annotate, and every other
    // arm below would describe one. Say what actually happened instead.
    if let Some(streak) = o.suppressed_after {
        return Some(format!(
            "not run \u{2014} suppressed after {streak} infrastructure failures"
        ));
    }
    let test_parse = o.parsed.as_ref().filter(|p| p.is_test_runner());
    let mut parts: Vec<String> = Vec::new();
    let declared = o.parsed.as_ref().and_then(|p| p.unverified.as_deref());
    if let (true, Some(reason)) = (o.passed, declared) {
        // Same wording as `/checks`: the run's own reason, never a count that
        // would read as the green it disowned.
        parts.push(format!("unverified \u{2014} {reason}"));
    } else if o.passed {
        match test_parse {
            // A fast-tier green names its population instead of a bare count.
            Some(p) if p.is_fast_tier() && p.tests_run() > 0 => parts.extend(p.fast_tier_label()),
            Some(p) if p.tests_run() == 0 && p.skipped > 0 => {
                parts.push(format!("no tests ran, {} skipped", p.skipped))
            }
            Some(p) if p.tests_run() == 0 => parts.push("no tests matched the change".to_string()),
            Some(p) if p.skipped > 0 => {
                parts.push(format!("{} tests, {} skipped", p.tests_run(), p.skipped))
            }
            Some(p) => parts.push(format!("{} tests", p.tests_run())),
            // Same wording as `/checks`, so the inline line, the live board, and
            // the listing agree that this exit zero vouches for nothing.
            None if o.parsed.as_ref().is_some_and(|p| p.is_incomplete_lint()) => {
                parts.push("unverified \u{2014} the lint did not run to completion".to_string())
            }
            // Non-test check: duration is the only honest signal, and only on a
            // fresh run (a cache hit's stored duration would be misleading).
            None if !o.cached && o.duration_ms > 0 => {
                parts.push(format_check_duration(o.duration_ms))
            }
            None => {}
        }
    } else if let Some(kind) = o.failure_kind {
        // A classified death renders as itself, never a zero-failure assertion
        // count the agent would chase into tests that never failed.
        if kind == CheckFailureKind::Cancelled {
            parts.push(
                o.not_recorded
                    .clone()
                    .unwrap_or_else(|| kind.describe(o.duration_ms)),
            );
        } else if kind == CheckFailureKind::RunnerError {
            let passed = o.parsed.as_ref().map(|parsed| parsed.passed).unwrap_or(0);
            if passed == 0 {
                parts.push("test runner failed before reporting tests".to_string());
            } else {
                parts.push(format!(
                    "test runner failed after {passed} tests passed with no assertion failures"
                ));
            }
        } else {
            parts.push(kind.describe(o.duration_ms));
        }
    } else {
        match test_parse {
            Some(p) => {
                let exit = o
                    .exit_code
                    .map(|c| format!(", exit {c}"))
                    .unwrap_or_default();
                // A file that failed to collect ran no test, and a run the skip
                // ledger failed for an undeclared skip may have no failing test
                // at all, so folding either into the assertion tally would read
                // as "0 of 882 failed" — a red check pointing at nothing. Count
                // each separately.
                let mut segments = Vec::new();
                // The audit leads a red it is the whole of, ahead of a test
                // tally that passed.
                let audit = p.plan_audit_gating();
                if audit > 0 {
                    segments.push(format!("plan audit: {audit} gating"));
                    if p.failed == 0 {
                        segments.push(format!("all {} passed", p.tests_run()));
                    }
                }
                if p.failed > 0 || (p.suite_failures == 0 && p.undeclared_skips == 0 && audit == 0)
                {
                    segments.push(format!("{} of {} failed", p.failed, p.tests_run()));
                }
                if p.undeclared_skips > 0 {
                    let noun = if p.undeclared_skips == 1 {
                        "skip"
                    } else {
                        "skips"
                    };
                    segments.push(format!("{} undeclared {noun}", p.undeclared_skips));
                }
                if p.suite_failures > 0 {
                    let noun = if p.suite_failures == 1 {
                        "suite"
                    } else {
                        "suites"
                    };
                    segments.push(format!("{} {noun} failed to load", p.suite_failures));
                }
                parts.push(format!("{}{exit}", segments.join(", ")));
                parts.extend(p.fast_tier_label());
            }
            None => match o.exit_code {
                Some(code) => parts.push(format!("exit {code}")),
                None => parts.push("failed to run".to_string()),
            },
        }
    }
    if o.cached {
        parts.push("cached".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

/// Render a check duration compactly: `4.1s` at or above a second, `850ms` below.
pub(super) fn format_check_duration(ms: i64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// One failing check's detail block: a `\u{2717} <name> \u{2014} N failed: ...`
/// line (when structured names are available) over a fenced, bounded excerpt.
/// `None` when there is nothing to add beyond the header status (no structured
/// names and no output to excerpt). Pure.
/// For a timed-out check, the `N still running at kill: a, b, c +M more` line
/// naming the nextest tests that were mid-flight when the budget expired. `None`
/// when the check did not time out or no running tests could be parsed — the
/// agent's first question is "what was it doing when it died?".
pub(super) fn timeout_running_names(o: &CheckOutcome) -> Option<String> {
    if o.failure_kind != Some(CheckFailureKind::TimedOut) {
        return None;
    }
    let running = extract_running_tests(&o.output_tail);
    if running.is_empty() {
        return None;
    }
    let shown: Vec<&str> = running
        .iter()
        .take(MAX_FAILURE_NAMES)
        .map(String::as_str)
        .collect();
    let more = running.len().saturating_sub(shown.len());
    let listed = if more > 0 {
        format!("{}, +{more} more", shown.join(", "))
    } else {
        shown.join(", ")
    };
    Some(format!("{} still running at kill: {listed}", running.len()))
}

pub(super) fn format_check_detail(
    o: &CheckOutcome,
    attribution: Option<&crate::execution::checks_base::BaseAttribution>,
) -> Option<String> {
    // A timeout has no failing tests to name, but its still-running tests are
    // exactly the detail the agent needs; fall back to parsed failures otherwise.
    let names = timeout_running_names(o)
        .or_else(|| attribution.and_then(|attribution| attributed_failures(o, attribution)))
        .or_else(|| o.parsed.as_ref().and_then(format_failure_names));
    let excerpt = format_failure_excerpt(o.parsed.as_ref(), &o.output_tail);
    let head = match names {
        Some(n) => format!("\u{2717} {} \u{2014} {n}", o.name),
        None if excerpt.trim().is_empty() => return None,
        None => format!("\u{2717} {}:", o.name),
    };
    let mut block = head;
    if !excerpt.trim().is_empty() {
        block.push_str("\n```\n");
        block.push_str(excerpt.trim_end());
        block.push_str("\n```");
    }
    Some(block)
}

/// A red test lane's failures by what main at the base observed: the lane's
/// tally and the attribution's headline, then one line per bucket.
pub(super) fn attributed_failures(
    o: &CheckOutcome,
    attribution: &crate::execution::checks_base::BaseAttribution,
) -> Option<String> {
    let parsed = o.parsed.as_ref()?;
    let names: Vec<String> = parsed.failures.iter().map(|f| f.name.clone()).collect();
    let tally = crate::execution::checks_status::failed_annotation(
        None,
        Some(o.duration_ms),
        Some(parsed.passed),
        Some(parsed.failed),
        Some(parsed.suite_failures),
        &names,
    )?;
    let mut out = format!("{tally}; {}", attribution.headline());
    for line in attribution.line_texts() {
        out.push_str("\n    ");
        out.push_str(&line);
    }
    Some(out)
}

/// Last `max_chars` characters of `s`, on a char boundary.
pub(super) fn tail(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    s.chars().skip(count - max_chars).collect()
}

/// The fork point a node's write-check impact gate diffs from, resolved live.
///
/// `None` when the job has no branch or an endpoint does not resolve; the
/// caller falls back to the project default tip, which is what this path did
/// for a job with no recorded coordinate at all. What it must never do is start
/// from `jobs.base_commit`: that row is the coordinate the branch was cut at
/// and does not follow a base advance, so a stale one hands the impact gate
/// every file the target merged in the meantime.
pub(super) async fn live_node_base(orch: &Orchestrator, job_id: &str) -> Option<String> {
    // A team job lives wholly in its team replica; the private database has no
    // row for it and would answer "no range" for every team node.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| {
            log::debug!("node {job_id} write checks: no owning database ({error})");
        })
        .ok()?;
    crate::diff::live_job_branch_range(&db, job_id, &orch.jj_binary_path, &orch.config_dir)
        .await
        .map_err(|error| {
            log::debug!("node {job_id} write checks: no live base coordinate ({error})");
        })
        .ok()
        .flatten()
        .map(|range| range.base)
}

/// The base attribution of each red test lane a write wave reports, against
/// the node's live fork point. Empty, at the cost of one pass over the
/// outcomes, when none is a test runner's complete red.
pub(super) async fn write_check_attributions(
    orch: &Orchestrator,
    job_id: &str,
    results: &[CheckOutcome],
) -> HashMap<String, crate::execution::checks_base::BaseAttribution> {
    use crate::execution::checks_base::{attribute_outcomes, failing_outcome_tests};
    if !results
        .iter()
        .any(|outcome| failing_outcome_tests(outcome).is_some())
    {
        return HashMap::new();
    }
    let Some(base) = live_node_base(orch, job_id).await else {
        return HashMap::new();
    };
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return HashMap::new();
    };
    let coords = match crate::execution::checks_turn_end::resolve_job_coords(&db, job_id).await {
        Ok(Some(coords)) => coords,
        Ok(None) => return HashMap::new(),
        Err(error) => {
            log::debug!("node {job_id} write checks: coordinates unreadable ({error})");
            return HashMap::new();
        }
    };
    attribute_outcomes(orch, &db, &coords, job_id, &base, results).await
}
