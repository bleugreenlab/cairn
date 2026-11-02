//! What a batch's run result says about its node scratch, and what the runner
//! learns from the report it came in.
//!
//! One line always, when the batch had a scratch:
//!
//! > Scratch: brought current on fedora (+61.2 MB in 3.4 s from local); wrote
//! > back 1.2 MB in 0.3 s; 62.4 MB of 512 MiB.
//!
//! then whichever of these apply: conflicts, paths that do not travel, a
//! refusal, a deferred bring-current, writes another machine holds that this
//! batch could not see, and a warning once the scratch passes 80% of its
//! ceiling. Every sentence names the machine, so an agent never has to guess
//! where its data is.

use cairn_common::executor_protocol::{
    ScratchEntrySize, ScratchRefusal, ScratchSyncReport, ScratchWriteBackOutcome,
    SCRATCH_STAGE_CREDENTIAL_REFUSED, SCRATCH_STAGE_NO_CREDENTIAL,
};

use super::owner::{self, NodeScratch, Replica};
use crate::orchestrator::Orchestrator;

/// The share of the ceiling past which the run result warns.
const WARN_AT_PERCENT: u64 = 80;

/// Bytes the way the run result says them: decimal units, one decimal place.
pub fn bytes(count: u64) -> String {
    const UNITS: [&str; 4] = ["KB", "MB", "GB", "TB"];
    if count < 1000 {
        return format!("{count} B");
    }
    let mut value = count as f64;
    let mut unit = "B";
    for next in UNITS {
        if value < 1000.0 {
            break;
        }
        value /= 1000.0;
        unit = next;
    }
    format!("{value:.1} {unit}")
}

/// A ceiling the way a project writes it: whole MiB when it is one.
pub fn ceiling(count: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    if count >= MIB && count.is_multiple_of(MIB) {
        format!("{} MiB", count / MIB)
    } else {
        bytes(count)
    }
}

fn seconds(ms: u64) -> String {
    format!("{:.1} s", ms as f64 / 1000.0)
}

pub(super) fn named(entries: &[ScratchEntrySize]) -> String {
    entries
        .iter()
        .map(|entry| format!("`{}` ({})", entry.path, bytes(entry.bytes)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn refusal_text(refusal: &ScratchRefusal, machine: &str) -> String {
    let mut text = if refusal.oversized.is_empty() {
        format!(
            "Scratch write-back refused: {} would hold {}, past its ceiling of {}.",
            refusal.owner_uri,
            bytes(refusal.merged_bytes),
            ceiling(refusal.ceiling_bytes)
        )
    } else {
        format!(
            "Scratch write-back refused: {} names files larger than one transfer can carry ({}), which no ceiling admits.",
            refusal.owner_uri,
            named(&refusal.oversized)
        )
    };
    if !refusal.largest.is_empty() {
        text.push_str(&format!(" Largest: {}.", named(&refusal.largest)));
    }
    if !refusal.largest_directories.is_empty() {
        text.push_str(&format!(
            " Largest directories: {}.",
            named(&refusal.largest_directories)
        ));
    }
    text.push_str(&format!(
        " Nothing was synced: your files stay in `$TMPDIR` on {machine}, and every later batch there is refused the same way until you delete enough to fit."
    ));
    text
}

/// A failed write-back, said by what decides when it lands: a re-attach, when
/// the machine has no credential or the runner refused the newest it holds;
/// otherwise the executor's own retry.
fn failed_text(stage: &str, detail: &str, machine: &str) -> String {
    let stranded = format!("The changes exist only on {machine} until they reach the runner");
    match stage {
        SCRATCH_STAGE_NO_CREDENTIAL => format!(
            "Scratch write-back failed: {machine} holds no object-channel credential, so it is not attached to the runner. {stranded}; {machine} sends them as soon as it attaches."
        ),
        SCRATCH_STAGE_CREDENTIAL_REFUSED => format!(
            "Scratch write-back failed: the runner refused the newest object-channel credential {machine} holds ({detail}), because the runner restarted or that credential lapsed since {machine} last attached. {stranded}; {machine} sends them as soon as its re-attach brings a new credential."
        ),
        _ => format!(
            "Scratch write-back failed ({stage}: {detail}). {stranded}; {machine} retries every two minutes, and at once when its link returns."
        ),
    }
}

/// The paths that do not travel, the sized ones first and largest first, so
/// the one worth deleting leads.
fn not_synced_text(report: &ScratchSyncReport) -> String {
    let mut paths: Vec<(&str, Option<u64>)> = report
        .not_synced
        .iter()
        .map(|path| {
            let size = report
                .not_synced_bytes
                .iter()
                .find(|sized| sized.path == *path)
                .map(|sized| sized.bytes);
            (path.as_str(), size)
        })
        .collect();
    paths.sort_by_key(|(_, size)| (size.is_none(), std::cmp::Reverse(*size)));
    paths
        .into_iter()
        .map(|(path, size)| match size {
            Some(size) => format!("`{path}` ({})", bytes(size)),
            None => format!("`{path}`"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Everything the run result says about the scratch, given what the executor
/// reported and what the authority holds now. `None` when there is nothing to
/// say: the scratch was already current, the batch changed nothing in it, and
/// no warning applies.
pub fn compose(
    report: &ScratchSyncReport,
    machine: &str,
    from_machine: Option<&str>,
    ceiling_bytes: u64,
    held_elsewhere: &[(String, Replica)],
) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(unavailable) = &report.unavailable {
        lines.push(format!(
            "Scratch: unavailable on {machine} ({unavailable}); this batch ran with a private `$TMPDIR` that does not travel."
        ));
        return Some(lines.join("\n"));
    }
    let near_ceiling = ceiling_bytes > 0
        && report.bytes.saturating_mul(100) > ceiling_bytes.saturating_mul(WARN_AT_PERCENT);
    if report.materialize.is_none()
        && report.write_back.is_none()
        && report.not_synced.is_empty()
        && held_elsewhere.is_empty()
        && !near_ceiling
    {
        return None;
    }
    let mut parts = Vec::new();
    match &report.materialize {
        Some(materialized) if materialized.deferred_reason.is_none() => {
            let from = from_machine
                .map(|from| format!(" from {from}"))
                .unwrap_or_default();
            parts.push(format!(
                "brought current on {machine} (+{} in {}{from})",
                bytes(materialized.bytes),
                seconds(materialized.ms)
            ));
        }
        Some(_) => parts.push(format!("not brought current on {machine}")),
        None => parts.push(format!("already current on {machine}")),
    }
    match report
        .write_back
        .as_ref()
        .map(|write| (write, &write.outcome))
    {
        Some((write, ScratchWriteBackOutcome::Applied { .. })) => parts.push(format!(
            "wrote back {} in {}",
            bytes(write.bytes),
            seconds(write.ms)
        )),
        Some((_, ScratchWriteBackOutcome::Refused(_))) => parts.push("write-back refused".into()),
        Some((_, ScratchWriteBackOutcome::Failed { .. })) => parts.push("write-back failed".into()),
        None => parts.push("nothing to write back".into()),
    }
    parts.push(format!(
        "{} of {}",
        bytes(report.bytes),
        ceiling(ceiling_bytes)
    ));
    lines.push(format!("Scratch: {}.", parts.join("; ")));

    if let Some(write) = &report.write_back {
        match &write.outcome {
            ScratchWriteBackOutcome::Applied { conflicts, .. } => {
                for conflict in conflicts {
                    lines.push(format!(
                        "Scratch conflict: `{}` was also changed elsewhere; that version stays at `{}` and this batch's is beside it at `{}`.",
                        conflict.path.display(),
                        conflict.path.display(),
                        conflict.written_as.display()
                    ));
                }
            }
            ScratchWriteBackOutcome::Refused(refusal) => lines.push(refusal_text(refusal, machine)),
            ScratchWriteBackOutcome::Failed { stage, detail } => {
                lines.push(failed_text(stage, detail, machine))
            }
        }
    }
    if let Some(reason) = report
        .materialize
        .as_ref()
        .and_then(|materialized| materialized.deferred_reason.as_ref())
    {
        lines.push(format!(
            "Scratch not brought current: {reason}. This batch ran against {machine}'s copy as it stood."
        ));
    }
    if !report.not_synced.is_empty() {
        lines.push(format!(
            "Not synced, so present only on {machine}: {}. Dependency trees, test temp roots, cache-tagged directories and nested repositories never travel; recreate them where they are needed, and delete them when done, since they stay on {machine}'s disk until you do.",
            not_synced_text(report)
        ));
    }
    for (other, replica) in held_elsewhere {
        match replica.refused_bytes {
            Some(refused) => lines.push(format!(
                "Not included: {other}'s writes refused by the ceiling ({} of {}); they are still only on {other}.",
                bytes(refused),
                ceiling(ceiling_bytes)
            )),
            None => lines.push(format!(
                "Not included: writes on {other} not yet written back."
            )),
        }
    }
    if near_ceiling
        && !matches!(
            report.write_back.as_ref().map(|write| &write.outcome),
            Some(ScratchWriteBackOutcome::Refused(_))
        )
    {
        lines.push(format!(
            "Scratch is at {}% of its ceiling. Largest: {}. Trim it before a write-back is refused.",
            report.bytes.saturating_mul(100) / ceiling_bytes,
            named(&report.largest)
        ));
    }
    Some(lines.join("\n"))
}

/// Record what a batch's report says about its machine's replica, learn that
/// machine's sync throughput, and say it all in the run result.
pub async fn settle(
    orch: &Orchestrator,
    scratch: &NodeScratch,
    report: &ScratchSyncReport,
    executor_id: &str,
) -> Option<String> {
    let local = &orch.db.local;
    let current = owner::load(local, &scratch.id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| scratch.clone());
    let known = owner::replicas(local, &scratch.id)
        .await
        .unwrap_or_default();
    if report.unavailable.is_none() {
        let mine = known
            .iter()
            .find(|replica| replica.executor_id == executor_id);
        let synced_written_bytes = match (&report.synced_commit, mine) {
            (Some(synced), _) if current.head_commit.as_ref() == Some(synced) => {
                current.written_bytes
            }
            (Some(synced), Some(mine)) if mine.synced_commit.as_ref() == Some(synced) => {
                mine.synced_written_bytes
            }
            // A head that is not the current one, synced at grant time.
            (Some(synced), _) if scratch.head_commit.as_ref() == Some(synced) => {
                scratch.written_bytes
            }
            (_, Some(mine)) => mine.synced_written_bytes,
            _ => 0,
        };
        let refused_bytes = match report.write_back.as_ref().map(|write| &write.outcome) {
            Some(ScratchWriteBackOutcome::Refused(refusal)) => Some(refusal.merged_bytes),
            _ if report.dirty => mine.and_then(|mine| mine.refused_bytes),
            _ => None,
        };
        let _ = owner::record_replica(
            local,
            &Replica {
                scratch_id: scratch.id.clone(),
                executor_id: executor_id.to_owned(),
                synced_commit: report.synced_commit.clone(),
                synced_written_bytes,
                dirty: report.dirty,
                refused_bytes,
            },
        )
        .await;
        let moved: Vec<(u64, u64)> = report
            .materialize
            .iter()
            .map(|materialized| (materialized.bytes, materialized.ms))
            .chain(
                report
                    .write_back
                    .iter()
                    .map(|write| (write.bytes, write.ms)),
            )
            .collect();
        for (bytes, ms) in moved {
            orch.object_plane
                .scratch
                .observe_throughput(executor_id, bytes, ms);
        }
    }
    let machine = orch
        .fleet
        .executor_display_name(executor_id)
        .unwrap_or_else(|| executor_id.to_owned());
    let from_machine = scratch
        .head_executor_id
        .as_deref()
        .filter(|from| *from != executor_id)
        .map(|from| {
            orch.fleet
                .executor_display_name(from)
                .unwrap_or_else(|| from.to_owned())
        });
    // Writes another machine holds that this batch could not see: a replica
    // the authority knows is dirty, or one a live terminal or REPL holds
    // there, which writes back only when it exits.
    let mut elsewhere: Vec<Replica> = known
        .into_iter()
        .filter(|replica| replica.executor_id != executor_id && replica.dirty)
        .collect();
    for advertised in orch.fleet.node_scratch_replicas(&scratch.id) {
        if advertised.executor_id != executor_id
            && advertised.replica.in_use
            && !elsewhere
                .iter()
                .any(|replica| replica.executor_id == advertised.executor_id)
        {
            elsewhere.push(Replica {
                scratch_id: scratch.id.clone(),
                executor_id: advertised.executor_id,
                synced_commit: advertised.replica.synced_commit,
                synced_written_bytes: 0,
                dirty: true,
                refused_bytes: None,
            });
        }
    }
    let held_elsewhere: Vec<(String, Replica)> = elsewhere
        .into_iter()
        .map(|replica| {
            let name = orch
                .fleet
                .executor_display_name(&replica.executor_id)
                .unwrap_or_else(|| replica.executor_id.clone());
            (name, replica)
        })
        .collect();
    let ceiling_bytes = owner::ceiling_bytes(orch, &scratch.project_id).await;
    compose(
        report,
        &machine,
        from_machine.as_deref(),
        ceiling_bytes,
        &held_elsewhere,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        ScratchConflict, ScratchMaterializeReport, ScratchPath, ScratchWriteBackReport,
    };

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn the_line_says_bytes_seconds_and_the_ceiling() {
        let report = ScratchSyncReport {
            materialize: Some(ScratchMaterializeReport {
                bytes: 61_200_000,
                ms: 3_400,
                ..ScratchMaterializeReport::default()
            }),
            write_back: Some(ScratchWriteBackReport {
                bytes: 1_200_000,
                ms: 300,
                outcome: ScratchWriteBackOutcome::Applied {
                    head: "a".repeat(40),
                    conflicts: vec![ScratchConflict {
                        path: ScratchPath::from("report.md"),
                        written_as: ScratchPath::from("report.md~conflict-ab12cd34"),
                    }],
                },
            }),
            bytes: 62_400_000,
            not_synced: vec!["out/node_modules/".into()],
            ..ScratchSyncReport::default()
        };
        let text = compose(&report, "fedora", Some("local"), 512 * MIB, &[]).unwrap();
        assert!(
            text.starts_with(
                "Scratch: brought current on fedora (+61.2 MB in 3.4 s from local); wrote back 1.2 MB in 0.3 s; 62.4 MB of 512 MiB."
            ),
            "{text}"
        );
        assert!(text.contains("`report.md~conflict-ab12cd34`"), "{text}");
        assert!(text.contains("`out/node_modules/`"), "{text}");
        assert!(!text.contains("% of its ceiling"), "{text}");
    }

    #[test]
    fn a_refusal_names_the_owner_size_ceiling_and_largest_files() {
        let report = ScratchSyncReport {
            write_back: Some(ScratchWriteBackReport {
                bytes: 0,
                ms: 12,
                outcome: ScratchWriteBackOutcome::Refused(ScratchRefusal {
                    owner_uri: "cairn://p/rs/document".into(),
                    merged_bytes: 3_700_000_000,
                    ceiling_bytes: 512 * MIB,
                    largest: vec![ScratchEntrySize {
                        path: "export.tar".into(),
                        bytes: 3_690_000_000,
                    }],
                    largest_directories: vec![ScratchEntrySize {
                        path: "cairn-rust-tests-nextest-Urs14M/".into(),
                        bytes: 13_100_000_000,
                    }],
                    oversized: Vec::new(),
                }),
            }),
            bytes: 3_700_000_000,
            dirty: true,
            ..ScratchSyncReport::default()
        };
        let text = compose(&report, "local", None, 512 * MIB, &[]).unwrap();
        for named in [
            "cairn://p/rs/document",
            "3.7 GB",
            "512 MiB",
            "`export.tar` (3.7 GB)",
            "Largest directories: `cairn-rust-tests-nextest-Urs14M/` (13.1 GB)",
            "stay in `$TMPDIR` on local",
        ] {
            assert!(text.contains(named), "missing {named}: {text}");
        }
        assert!(
            !text.contains("% of its ceiling"),
            "no warning on top of a refusal"
        );
    }

    #[test]
    fn what_does_not_travel_is_named_largest_first_with_its_size() {
        let report = ScratchSyncReport {
            bytes: 1_000,
            not_synced: vec![
                ".venv/".into(),
                "cairn-rust-tests-nextest-Urs14M/".into(),
                "clone/".into(),
            ],
            not_synced_bytes: vec![
                ScratchEntrySize {
                    path: "cairn-rust-tests-nextest-Urs14M/".into(),
                    bytes: 13_100_000_000,
                },
                ScratchEntrySize {
                    path: ".venv/".into(),
                    bytes: 200_000_000,
                },
            ],
            ..ScratchSyncReport::default()
        };
        let text = compose(&report, "bgsrv-ub", None, 512 * MIB, &[]).unwrap();
        assert!(
            text.contains(
                "present only on bgsrv-ub: `cairn-rust-tests-nextest-Urs14M/` (13.1 GB), `.venv/` (200.0 MB), `clone/`."
            ),
            "{text}"
        );
        assert!(text.contains("test temp roots"), "{text}");
    }

    #[test]
    fn writes_held_elsewhere_and_a_near_ceiling_scratch_are_named() {
        let report = ScratchSyncReport {
            bytes: 450 * MIB,
            largest: vec![ScratchEntrySize {
                path: "rs.duckdb".into(),
                bytes: 400 * MIB,
            }],
            ..ScratchSyncReport::default()
        };
        let held = vec![(
            "bbs".to_owned(),
            Replica {
                scratch_id: "s".into(),
                executor_id: "bbs".into(),
                synced_commit: None,
                synced_written_bytes: 0,
                dirty: true,
                refused_bytes: Some(600 * MIB),
            },
        )];
        let text = compose(&report, "fedora", None, 512 * MIB, &held).unwrap();
        assert!(text.contains("already current on fedora"), "{text}");
        assert!(
            text.contains("Not included: bbs's writes refused by the ceiling"),
            "{text}"
        );
        assert!(text.contains("87% of its ceiling"), "{text}");
        assert!(text.contains("`rs.duckdb`"), "{text}");
    }

    #[test]
    fn a_failed_write_back_says_what_it_waits_for() {
        let failed = |stage: &str, detail: &str| ScratchSyncReport {
            write_back: Some(ScratchWriteBackReport {
                bytes: 0,
                ms: 40,
                outcome: ScratchWriteBackOutcome::Failed {
                    stage: stage.into(),
                    detail: detail.into(),
                },
            }),
            bytes: 31_100_000,
            dirty: true,
            ..ScratchSyncReport::default()
        };
        let refused = compose(
            &failed(
                SCRATCH_STAGE_CREDENTIAL_REFUSED,
                "scratch write-back answered 401 Unauthorized: invalid object-channel credential",
            ),
            "fedora",
            None,
            512 * MIB,
            &[],
        )
        .unwrap();
        assert!(
            refused.contains("write-back failed; 31.1 MB of 512 MiB."),
            "{refused}"
        );
        assert!(
            refused.contains("the runner refused the newest object-channel credential fedora holds (scratch write-back answered 401"),
            "{refused}"
        );
        assert!(
            refused.contains("as soon as its re-attach brings a new credential"),
            "{refused}"
        );
        assert!(!refused.contains("every two minutes"), "{refused}");

        let unattached = compose(
            &failed(SCRATCH_STAGE_NO_CREDENTIAL, "no credential"),
            "bbs",
            None,
            512 * MIB,
            &[],
        )
        .unwrap();
        assert!(
            unattached.contains("bbs holds no object-channel credential, so it is not attached"),
            "{unattached}"
        );

        let unreachable = compose(
            &failed("upload", "scratch upload: connection reset"),
            "bgsrv-ub",
            None,
            512 * MIB,
            &[],
        )
        .unwrap();
        assert!(
            unreachable.contains("(upload: scratch upload: connection reset)"),
            "{unreachable}"
        );
        assert!(
            unreachable.contains("retries every two minutes"),
            "{unreachable}"
        );
    }

    #[test]
    fn a_batch_that_never_touched_its_scratch_says_nothing() {
        let report = ScratchSyncReport {
            bytes: 1_000,
            ..ScratchSyncReport::default()
        };
        assert_eq!(compose(&report, "local", None, 512 * MIB, &[]), None);
    }
}
