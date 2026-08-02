//! What Cairn's own placed work is doing on a machine, and when it lets go.
//!
//! A caller refused for capacity knows only that there was no room. That is the
//! right amount of information when the room was taken by something outside
//! Cairn -- an operator's own build, a dev harness, a browser -- because nothing
//! in this process can say when such a thing will end.
//!
//! It is far less than Cairn knows when the room was taken by Cairn. The runner
//! placed those cells: it knows their work class, who owns them, when they
//! started, and -- through the learned resource profile resolved at placement --
//! how long that class of work has been taking on that machine. A patience
//! policy that spends a constant against that occupancy is guessing next to a
//! fact it already holds.
//!
//! So this module answers one question -- when is the work currently holding
//! this machine predicted to let go? -- and answers it only from measurement. An
//! occupant with no learned duration, or one that has already outlived the
//! duration learned for it, makes the whole reading unforecastable. Refusing to
//! guess is the point: a wait bounded by knowledge beats a wait bounded by a
//! constant only for as long as the knowledge is real.

use cairn_common::executor_protocol::{CellOwnerRef, ExecutingCellRequest};

/// What one machine's current occupancy says about when it will have room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MachineOccupancy {
    /// Nothing Cairn placed is running here. A machine running none of Cairn's
    /// own work is not one whose refusal occupancy can explain, so a caller
    /// keeps whatever bound it would have used without this module.
    Idle,
    /// Every occupant is Cairn's own work with a learned duration it has not yet
    /// outlived, so the moment the last of them finishes is predictable.
    Predicted(OccupancyForecast),
    /// Occupied by work whose end this process cannot predict: an occupant with
    /// no learned duration, or one already running longer than its learned
    /// duration says it should. The second case matters as much as the first --
    /// an overdue occupant has falsified its own estimate for this run, and a
    /// prediction built on a falsified estimate is a guess wearing a number.
    Unforecastable,
}

/// When the work holding a machine is predicted to release it, and which
/// occupant that prediction is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OccupancyForecast {
    /// Milliseconds from the moment of the forecast until the LAST occupant is
    /// predicted to finish. The last rather than the first, because a caller
    /// whose reservation the machine cannot currently fit does not know which
    /// departure would make room -- and admission, not this module, is the only
    /// party entitled to answer that. Predicting the full drain is the bound
    /// that is true regardless.
    pub(crate) relief_ms: u64,
    /// The occupant whose finish is that latest one: what the wait is actually
    /// on, in the terms an operator recognises.
    pub(crate) blocking: String,
    pub(crate) occupant_count: usize,
}

impl MachineOccupancy {
    /// Read one machine's occupancy from the cells it reports executing.
    ///
    /// The evidence is the executor's own snapshot of what is running, which
    /// carries the learned estimate this runner resolved for each request when
    /// it placed it. So the prediction is made from the same number admission
    /// was sized against, not from a second source that could disagree with it.
    pub(crate) fn read(executing: &[ExecutingCellRequest], now_unix_ms: u64) -> Self {
        if executing.is_empty() {
            return Self::Idle;
        }
        let mut latest: Option<(u64, &ExecutingCellRequest)> = None;
        for occupant in executing {
            let Some(remaining) = predicted_remaining_ms(occupant, now_unix_ms) else {
                return Self::Unforecastable;
            };
            if latest.is_none() || latest.is_some_and(|(known, _)| remaining > known) {
                latest = Some((remaining, occupant));
            }
        }
        let (relief_ms, blocking) = latest.expect("a non-empty occupancy has a latest occupant");
        Self::Predicted(OccupancyForecast {
            relief_ms,
            blocking: describe_occupant(blocking),
            occupant_count: executing.len(),
        })
    }

    /// The two machines' readings combined for a caller that could use either.
    ///
    /// A mobile batch needs ONE machine to have room, so the earlier relief
    /// wins. An unforecastable machine cannot lower that: it might free sooner,
    /// but nothing here knows it will, and a bound must not be shortened by a
    /// possibility. It does not raise it either -- the other machine's
    /// prediction stands on its own evidence.
    ///
    /// Idle dominates both. A batch refused while some machine it could use was
    /// running none of Cairn's work was not refused BY that work, whatever the
    /// busy machine beside it is predicted to do, so the caller keeps the bound
    /// it would have used with no forecast at all.
    pub(crate) fn or_earlier(self, other: Self) -> Self {
        match (self, other) {
            (Self::Idle, _) | (_, Self::Idle) => Self::Idle,
            (Self::Predicted(mine), Self::Predicted(theirs)) => {
                Self::Predicted(if theirs.relief_ms < mine.relief_ms {
                    theirs
                } else {
                    mine
                })
            }
            (Self::Predicted(only), _) | (_, Self::Predicted(only)) => Self::Predicted(only),
            _ => Self::Unforecastable,
        }
    }
}

/// How much longer one occupant is predicted to run, or `None` when nothing
/// measured says.
///
/// Two absences, one answer. A request with no learned duration was placed
/// against a cold-start prior, which sizes a reservation honestly but says
/// nothing about time. A request already past its learned duration has
/// outlived the measurement -- the profile's upper bound is a high-water mark
/// with slow decay, so exceeding it is not noise, it is this run being unlike
/// every run that taught the number. Both are "no prediction", and saying so is
/// what keeps the caller's fallback honest.
fn predicted_remaining_ms(occupant: &ExecutingCellRequest, now_unix_ms: u64) -> Option<u64> {
    let upper_ms = occupant.learned_estimate.as_ref()?.upper_duration_ms?;
    let elapsed_ms = now_unix_ms.saturating_sub(occupant.started_at_unix_ms);
    (elapsed_ms < upper_ms).then(|| upper_ms - elapsed_ms)
}

/// One occupant in the terms the operator who is waiting on it recognises:
/// whose work it is, then what it is running.
///
/// The owner comes first because that is the coordinate a person navigates by --
/// "CAIRN-3414's rust-tests" locates the work in the fleet, where a bare command
/// line only describes it. Work with no owner (a service lease, a bare batch)
/// falls back to the command, and work with neither is named as the anonymous
/// thing it is rather than given a fabricated label.
fn describe_occupant(occupant: &ExecutingCellRequest) -> String {
    let command = occupant.command.trim();
    match (occupant.owner.as_ref().and_then(describe_owner), command) {
        (Some(owner), "") => owner,
        (Some(owner), command) => format!("{owner}'s {command}"),
        (None, "") => "an unattributed cell".to_string(),
        (None, command) => command.to_string(),
    }
}

fn describe_owner(owner: &CellOwnerRef) -> Option<String> {
    match (owner.project_key.as_deref(), owner.issue_number) {
        (Some(key), Some(number)) => Some(format!("{key}-{number}")),
        (Some(key), None) => Some(key.to_string()),
        _ => owner.node_kind.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{CellCommandClass, LearnedResourceEstimate};

    /// Far enough into the epoch that a fixture can place an occupant hours
    /// into the past without the subtraction wrapping.
    const NOW: u64 = 1_785_000_000_000;

    fn occupant(command: &str, started_ago_ms: u64, upper_ms: Option<u64>) -> ExecutingCellRequest {
        ExecutingCellRequest {
            executor_id: String::new(),
            cell_id: "cell".into(),
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            owner: Some(CellOwnerRef {
                project_id: "project".into(),
                project_key: Some("CAIRN".into()),
                issue_number: Some(3414),
                job_id: None,
                execution_seq: None,
                node_kind: Some("builder".into()),
            }),
            command_class: CellCommandClass::CargoTest,
            command: command.into(),
            started_at_unix_ms: NOW - started_ago_ms,
            process_ids: Vec::new(),
            priority: None,
            subscriber_count: 1,
            resource_reservation: Default::default(),
            command_resource_identity: None,
            learned_estimate: upper_ms.map(|upper_duration_ms| LearnedResourceEstimate {
                sample_count: 12,
                upper_duration_ms: Some(upper_duration_ms),
                upper_peak_rss_bytes: None,
                upper_disk_growth_bytes: None,
            }),
        }
    }

    /// The reading this whole module exists to produce: measured occupants, a
    /// relief time taken from the LAST of them to finish, and the name of that
    /// occupant so the wait can be described rather than merely endured.
    #[test]
    fn measured_occupants_predict_the_moment_the_machine_drains() {
        let occupancy = MachineOccupancy::read(
            &[
                occupant("rust-tests", 60_000, Some(300_000)),
                occupant("rust-fmt", 1_000, Some(5_000)),
            ],
            NOW,
        );
        let MachineOccupancy::Predicted(forecast) = occupancy else {
            panic!("every occupant is measured and none is overdue");
        };
        assert_eq!(
            forecast.relief_ms, 240_000,
            "relief is when the last occupant finishes, not the first"
        );
        assert_eq!(forecast.blocking, "CAIRN-3414's rust-tests");
        assert_eq!(forecast.occupant_count, 2);
    }

    /// The two ways knowledge is absent, which must not be told apart by the
    /// caller: one occupant nobody has measured, and one that has outlived the
    /// measurement it was placed against. Either poisons the whole reading,
    /// because the machine is not free until BOTH of them are done.
    #[test]
    fn one_unmeasured_or_overdue_occupant_makes_the_whole_reading_unforecastable() {
        assert_eq!(
            MachineOccupancy::read(
                &[
                    occupant("rust-tests", 60_000, Some(300_000)),
                    occupant("an agent's terminal", 1_000, None),
                ],
                NOW,
            ),
            MachineOccupancy::Unforecastable,
            "a cell placed against a cold-start prior says nothing about time"
        );
        assert_eq!(
            MachineOccupancy::read(
                &[
                    occupant("rust-fmt", 1_000, Some(5_000)),
                    occupant("rust-tests", 1_800_000, Some(300_000)),
                ],
                NOW,
            ),
            MachineOccupancy::Unforecastable,
            "an occupant thirty minutes into a five-minute estimate has falsified it"
        );
    }

    #[test]
    fn a_machine_running_none_of_cairns_work_is_idle_rather_than_unforecastable() {
        assert_eq!(MachineOccupancy::read(&[], NOW), MachineOccupancy::Idle);
    }

    /// Combining machines for a batch that could use either. The earlier
    /// prediction wins because one machine having room is enough; an
    /// unforecastable machine can neither shorten a bound (nothing knows it
    /// will free sooner) nor lengthen one (the other machine's evidence stands).
    #[test]
    fn combining_machines_takes_the_earliest_thing_actually_known() {
        let soon = MachineOccupancy::read(&[occupant("rust-fmt", 1_000, Some(5_000))], NOW);
        let later = MachineOccupancy::read(&[occupant("rust-tests", 0, Some(300_000))], NOW);
        let MachineOccupancy::Predicted(forecast) = soon.clone().or_earlier(later.clone()) else {
            panic!("two predictions combine into a prediction");
        };
        assert_eq!(forecast.relief_ms, 4_000);

        let MachineOccupancy::Predicted(forecast) =
            later.clone().or_earlier(MachineOccupancy::Unforecastable)
        else {
            panic!("an unforecastable peer does not erase a real prediction");
        };
        assert_eq!(forecast.relief_ms, 300_000);

        assert_eq!(
            MachineOccupancy::Unforecastable.or_earlier(MachineOccupancy::Idle),
            MachineOccupancy::Idle,
            "a machine with room is the strongest statement either side can make"
        );
        assert_eq!(
            soon.or_earlier(MachineOccupancy::Idle),
            MachineOccupancy::Idle,
            "a refusal taken while some usable machine was empty was not caused by occupancy"
        );
        assert_eq!(
            MachineOccupancy::Unforecastable.or_earlier(MachineOccupancy::Unforecastable),
            MachineOccupancy::Unforecastable
        );
    }

    /// The label is for a person reading a check that went red. Owner first
    /// because that is what locates the work; no fabricated name when there is
    /// nothing to name it with.
    #[test]
    fn an_occupant_is_named_by_its_owner_then_its_command() {
        let mut anonymous = occupant("bun run test", 0, Some(1));
        anonymous.owner = None;
        assert_eq!(describe_occupant(&anonymous), "bun run test");

        anonymous.command = String::new();
        assert_eq!(describe_occupant(&anonymous), "an unattributed cell");

        let mut unnumbered = occupant("bun run test", 0, Some(1));
        unnumbered.owner.as_mut().unwrap().issue_number = None;
        assert_eq!(describe_occupant(&unnumbered), "CAIRN's bun run test");
    }
}
