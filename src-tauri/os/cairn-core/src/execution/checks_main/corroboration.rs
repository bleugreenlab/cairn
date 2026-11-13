//! Which per-test reds wait for a second host before they become incidents.
//!
//! A red whose verdict is a duration, a time or liveness bound that ended the
//! test ([`BoundEnding::Timeout`], the reading the cadence dates stall windows
//! by), says as much about the host it ran on as about the tree. So does a red
//! the cadence's own diff holds under a host condition ([`row_diff`]: an OS
//! error many tests share, or a stall window). One host's row alone files
//! neither. Every other red is an assertion, evidence about the code on any
//! host, and files at once: holding it would hide a real break whenever no
//! second host runs that tree.
//!
//! A hold is not a stored verdict. The observation's incident source stays
//! unacknowledged with a `held_until`, which keeps the observation under the
//! source's retention guard, and every decision is re-read from the lane's rows
//! when a new row lands or the wait ends: [`decide`] is a pure function of them.

use super::*;
use crate::storage::DbResult;

/// How long a held red waits for a second host before it is decided without
/// one.
pub(super) const CORROBORATION_BOUND_MS: i64 = 6 * 60 * 60 * 1000;

/// Why a red waits for a second host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HoldReason {
    /// A time or liveness bound ended the test.
    Duration,
    /// The cadence's diff holds the test under a host condition. It is never
    /// filed as a test failure on this host's word.
    HostCondition,
}

/// Whether a red waits for a second host, read from the cadence's structured
/// reading of it, never from its prose: `on_host` is membership in a
/// [`row_diff`] host-condition group, and a duration is [`Failure::ending`].
pub(super) fn hold_reason(failure: Option<&Failure>, on_host: bool) -> Option<HoldReason> {
    if on_host {
        Some(HoldReason::HostCondition)
    } else if failure.is_some_and(|failure| failure.ending == Some(BoundEnding::Timeout)) {
        Some(HoldReason::Duration)
    } else {
        None
    }
}

/// Another whole-suite row's outcome for a held test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Sighting {
    pub(super) reference: String,
    pub(super) host: String,
    pub(super) executor_id: Option<String>,
    pub(super) commit: String,
    pub(super) ran_at_ms: i64,
    pub(super) passed: bool,
}

/// How a held red's wait ended without a second host at its tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum WaitEnded {
    /// The same host's next row, at a newer tree, failed the test again.
    NextSameHostRow(Sighting),
    /// [`CORROBORATION_BOUND_MS`] passed.
    Bound,
}

/// A held red that is decided without filing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Release {
    /// Another host passed it at the same tree: a difference between hosts.
    GreenElsewhere(Sighting),
    /// A later row passed it.
    GreenLater(Sighting),
    /// Another host's row at a newer tree failed it. That row's red is decided
    /// on its own (filed if an assertion, held if not), so this one is not.
    Superseded(Sighting),
    /// A host-condition red whose wait ended without a second host: a fact
    /// about the host, never a test failure.
    HostCondition(WaitEnded),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Decision {
    /// Still waiting, until this instant at the latest.
    Held {
        until_ms: i64,
    },
    /// Another host failed it at the same tree: file.
    Corroborated(Sighting),
    /// A duration red whose wait ended with no second host at its tree: file,
    /// marked single-host and uncorroborated, rather than drop it.
    Uncorroborated(WaitEnded),
    Released(Release),
}

/// Decide a held red of `row` from the other rows that observed its test.
///
/// The same tree on another host decides first: its red corroborates, else its
/// green releases. Otherwise the next row after this one decides: a green
/// releases; another host's red at a newer tree supersedes; this host's red
/// at a newer tree ends the wait, as [`CORROBORATION_BOUND_MS`] does. A rerun
/// of the same tree on the same host never corroborates.
pub(super) fn decide(
    reason: HoldReason,
    row: &CadenceRow,
    sightings: &[Sighting],
    now_ms: i64,
) -> Decision {
    let same_host =
        |sighting: &Sighting| row.executor_id.is_some() && sighting.executor_id == row.executor_id;
    let same_tree_elsewhere: Vec<&Sighting> = sightings
        .iter()
        .filter(|s| s.commit == row.commit && s.executor_id.is_some() && !same_host(s))
        .collect();
    if let Some(red) = same_tree_elsewhere.iter().find(|s| !s.passed) {
        return Decision::Corroborated((*red).clone());
    }
    if let Some(green) = same_tree_elsewhere.first() {
        return Decision::Released(Release::GreenElsewhere((*green).clone()));
    }
    let ended = |ended: WaitEnded| match reason {
        HoldReason::Duration => Decision::Uncorroborated(ended),
        HoldReason::HostCondition => Decision::Released(Release::HostCondition(ended)),
    };
    let next = sightings
        .iter()
        .filter(|s| s.ran_at_ms > row.ran_at_ms && (s.passed || s.commit != row.commit))
        .min_by(|a, b| (a.ran_at_ms, &a.reference).cmp(&(b.ran_at_ms, &b.reference)));
    if let Some(next) = next {
        return if next.passed {
            Decision::Released(Release::GreenLater(next.clone()))
        } else if same_host(next) {
            ended(WaitEnded::NextSameHostRow(next.clone()))
        } else {
            Decision::Released(Release::Superseded(next.clone()))
        };
    }
    let until_ms = row.ran_at_ms.saturating_add(CORROBORATION_BOUND_MS);
    if now_ms >= until_ms {
        ended(WaitEnded::Bound)
    } else {
        Decision::Held { until_ms }
    }
}

/// Every other whole-suite row of the lane that observed one of `tests`: any
/// cadence's row at `row`'s commit, and the cadence's own rows that ran after
/// it. Keyed by test.
#[allow(clippy::too_many_arguments)]
pub(super) async fn sightings(
    db: &LocalDb,
    project: &str,
    key: &str,
    check: &str,
    platform: &str,
    row: &CadenceRow,
    tests: &[&str],
    names: ExecutorNames<'_>,
) -> DbResult<HashMap<String, Vec<Sighting>>> {
    use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
    use cairn_db::turso::Value as SqlValue;

    let fixed = vec![
        SqlValue::Text(project.to_string()),
        SqlValue::Text(check.to_string()),
        SqlValue::Text(platform.to_string()),
        SqlValue::Text(row.id.clone()),
        SqlValue::Text(row.commit.clone()),
        SqlValue::Integer(row.ran_at_ms),
    ];
    let mut found: HashMap<String, Vec<Sighting>> = HashMap::new();
    for chunk in tests.chunks(IN_LIST_CHUNK - fixed.len()) {
        let mut bound = fixed.clone();
        bound.extend(chunk.iter().map(|test| SqlValue::Text((*test).to_string())));
        let rows = db
            .query_all(
                format!(
                    "SELECT r.test_id, COALESCE(o.public_handle, o.id), o.executor_id, o.commit_sha,
                            o.ran_at, r.status
                       FROM check_result_observations o
                       JOIN check_test_results r ON r.observation_id = o.id
                      WHERE o.project_id = ?1 AND o.check_name = ?2 AND o.verdict_platform = ?3
                        AND o.id != ?4 AND o.complete = 1 AND o.failure_kind IS NULL
                        AND {PER_TEST_WHOLE_SUITE}
                        AND (o.commit_sha = ?5 OR (o.cadence = 'main' AND o.ran_at > ?6))
                        AND r.status IN ('passed', 'failed')
                        AND r.test_id IN ({})",
                    placeholders(chunk.len())
                ),
                bound,
                |r| {
                    Ok((
                        r.text(0)?,
                        r.text(1)?,
                        r.opt_text(2)?,
                        r.text(3)?,
                        r.i64(4)?,
                        r.text(5)?,
                    ))
                },
            )
            .await?;
        for (test, handle, executor_id, commit, ran_at_ms, status) in rows {
            found.entry(test).or_default().push(Sighting {
                reference: format!("cairn://p/{key}/check-observations/{handle}"),
                host: executor_id
                    .as_deref()
                    .map(names)
                    .unwrap_or_else(|| "unknown".into()),
                executor_id,
                commit,
                ran_at_ms,
                passed: status == "passed",
            });
        }
    }
    Ok(found)
}

/// The line a filed held red's evidence carries about how it was decided.
pub(super) fn filing_line(reason: HoldReason, decision: &Decision, row: &CadenceRow) -> String {
    let why = match reason {
        HoldReason::Duration => "This red's verdict is a duration (a time or liveness bound ended the test), not an assertion",
        HoldReason::HostCondition => "The cadence held this red under a host condition on this row",
    };
    let how = match decision {
        Decision::Corroborated(red) => format!(
            "corroborated: `{}` failed it at the same commit ({}).",
            red.host, red.reference
        ),
        Decision::Uncorroborated(ended) => format!(
            "**single-host, uncorroborated**: {}, and no other host's row at `{}` observed it. Filed so it is not dropped; the host, not the tree, may be the cause.",
            wait_ended(ended, row),
            short_commit(&row.commit)
        ),
        _ => String::new(),
    };
    format!("\nCorroboration: {why}, so it waited for a second host before filing; {how}\n")
}

fn wait_ended(ended: &WaitEnded, row: &CadenceRow) -> String {
    match ended {
        WaitEnded::NextSameHostRow(next) => format!(
            "`{}`'s next row ({}, at `{}`) failed it again",
            host(row),
            next.reference,
            short_commit(&next.commit)
        ),
        WaitEnded::Bound => format!(
            "its {}h wait ended",
            CORROBORATION_BOUND_MS / (60 * 60 * 1000)
        ),
    }
}

/// What one row's held reds came to, for its incident note.
#[derive(Debug, Default)]
pub(super) struct Ledger {
    held: Vec<(String, HoldReason)>,
    /// Held reds decided by this pass, each with how.
    decided: Vec<(String, String)>,
    until_ms: Option<i64>,
}

impl Ledger {
    pub(super) fn hold(&mut self, test: &str, reason: HoldReason, until_ms: i64) {
        self.held.push((test.to_string(), reason));
        self.until_ms = Some(self.until_ms.map_or(until_ms, |until| until.min(until_ms)));
    }

    pub(super) fn release(&mut self, test: &str, release: &Release, row: &CadenceRow) {
        let how = match release {
            Release::GreenElsewhere(green) => format!(
                "released: passed on `{}` at the same commit ({}), a difference between hosts, not the tree",
                green.host, green.reference
            ),
            Release::GreenLater(green) => format!(
                "released: passed on `{}` at `{}` ({}), a later row",
                green.host,
                short_commit(&green.commit),
                green.reference
            ),
            Release::Superseded(red) => format!(
                "superseded: `{}`'s newer row at `{}` ({}) failed it, and that row's red is decided on its own",
                red.host,
                short_commit(&red.commit),
                red.reference
            ),
            Release::HostCondition(ended) => format!(
                "recorded as a host condition on `{}`, not filed as a test failure: {}",
                host(row),
                wait_ended(ended, row)
            ),
        };
        self.decided.push((test.to_string(), how));
    }

    pub(super) fn filed(&mut self, test: &str, decision: &Decision, row: &CadenceRow) {
        let how = match decision {
            Decision::Corroborated(red) => format!(
                "filed, corroborated: `{}` failed it at the same commit ({})",
                red.host, red.reference
            ),
            Decision::Uncorroborated(ended) => format!(
                "filed as single-host, uncorroborated: {}",
                wait_ended(ended, row)
            ),
            _ => return,
        };
        self.decided.push((test.to_string(), how));
    }

    /// When the earliest held red's wait ends, in Unix milliseconds.
    pub(super) fn until_ms(&self) -> Option<i64> {
        self.until_ms
    }

    /// The held and decided reds as the cadence notice lists them, so nothing a
    /// hold keeps from the issue tracker disappears from view.
    pub(super) fn render(&self, row: &CadenceRow) -> String {
        const LISTED: usize = 10;
        let list = |out: &mut String, tests: &[&str]| {
            for test in tests.iter().take(LISTED) {
                out.push_str(&format!("- `{test}`\n"));
            }
            if tests.len() > LISTED {
                out.push_str(&format!("- and {} more\n", tests.len() - LISTED));
            }
        };
        let mut out = String::new();
        let timing: Vec<&str> = self
            .held
            .iter()
            .filter(|(_, reason)| *reason == HoldReason::Duration)
            .map(|(test, _)| test.as_str())
            .collect();
        let on_host: Vec<&str> = self
            .held
            .iter()
            .filter(|(_, reason)| *reason == HoldReason::HostCondition)
            .map(|(test, _)| test.as_str())
            .collect();
        let until = self
            .until_ms
            .map(|until| format!(" (until {} at the latest)", stamp(until)))
            .unwrap_or_default();
        if !timing.is_empty() {
            out.push_str(&format!(
                "\n**{} timing red{} held for corroboration** on `{}` at `{}`: no incident is filed yet. Another host's red at this commit files {}, its green releases {}, and without one {} filed as single-host, uncorroborated at `{}`'s next red row or when the wait ends{until}.\n",
                timing.len(),
                plural(timing.len()),
                host(row),
                short_commit(&row.commit),
                if timing.len() == 1 { "it" } else { "them" },
                if timing.len() == 1 { "it" } else { "them" },
                if timing.len() == 1 { "it is" } else { "they are" },
                host(row),
            ));
            list(&mut out, &timing);
        }
        if !on_host.is_empty() {
            out.push_str(&format!(
                "\n**{} red{} inside a host condition held for corroboration** on `{}`: never filed as test failures on this host's word. Another host's red at this commit files {}; a green releases {}; otherwise {} recorded as the host's condition{until}.\n",
                on_host.len(),
                plural(on_host.len()),
                host(row),
                if on_host.len() == 1 { "it" } else { "them" },
                if on_host.len() == 1 { "it" } else { "them" },
                if on_host.len() == 1 { "it is" } else { "they are" },
            ));
            list(&mut out, &on_host);
        }
        if !self.decided.is_empty() {
            out.push_str(&format!(
                "\nCorroboration decided {} held red{} of {}:\n",
                self.decided.len(),
                plural(self.decided.len()),
                row.reference
            ));
            for (test, how) in self.decided.iter().take(LISTED) {
                out.push_str(&format!("- `{test}` — {how}\n"));
            }
            if self.decided.len() > LISTED {
                out.push_str(&format!("- and {} more\n", self.decided.len() - LISTED));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 60 * 60 * 1000;

    fn held_row() -> CadenceRow {
        CadenceRow {
            id: "held".into(),
            reference: "cairn://p/cairn/check-observations/held".into(),
            commit: "c1".into(),
            cadence: "main".into(),
            platform: "linux".into(),
            executor_id: Some("fedora-id".into()),
            executor: Some("fedora".into()),
            ran_at_ms: 10 * HOUR,
            verdict_passed: false,
            exit_code: 100,
            complete: true,
            selection: Some("full".into()),
            tests: BTreeMap::new(),
            output_tail: String::new(),
            lane_started_at_ms: None,
            plan_audit: None,
        }
    }

    fn seen(id: &str, executor: &str, commit: &str, ran_at_ms: i64, passed: bool) -> Sighting {
        Sighting {
            reference: format!("cairn://p/cairn/check-observations/{id}"),
            host: executor.trim_end_matches("-id").into(),
            executor_id: Some(executor.into()),
            commit: commit.into(),
            ran_at_ms,
            passed,
        }
    }

    #[test]
    fn only_a_duration_or_a_host_condition_waits() {
        let timeout = Failure::from_excerpt(
            "TIMEOUT: nextest terminated this test at its slow-timeout bound (src-tauri/.config/nextest.toml)\nran 663s before termination",
        );
        let vitest = Failure::from_excerpt(
            "TIMEOUT: vitest terminated this test at its timeout bound (testTimeout/hookTimeout in vitest.config.ts, or the test's own)\nran 14991 ms before termination",
        );
        let liveness = Failure::from_excerpt(
            "admission: no progress within 30s on 2 runtime worker(s): the admission gate never answered",
        );
        let assertion =
            Failure::from_excerpt("assertion `left == right` failed\n  left: 1\n right: 2");
        let refused = Failure::from_excerpt(
            "no colocated executor is configured, enrolled, or being supervised",
        );
        for failure in [&timeout, &vitest, &liveness] {
            assert_eq!(
                hold_reason(Some(failure), false),
                Some(HoldReason::Duration),
                "{failure:?}"
            );
        }
        assert_eq!(hold_reason(Some(&assertion), false), None);
        // A refusal is the executor's absence, not a duration: alone it files.
        assert_eq!(hold_reason(Some(&refused), false), None);
        // Inside a host-condition group, any red waits, assertions included.
        assert_eq!(
            hold_reason(Some(&assertion), true),
            Some(HoldReason::HostCondition)
        );
        assert_eq!(hold_reason(None, false), None);
    }

    #[test]
    fn the_same_tree_on_another_host_decides_first() {
        let row = held_row();
        let red = seen("sp2", "sp2-id", "c1", 9 * HOUR, false);
        let green = seen("sp1", "sp1-id", "c1", 11 * HOUR, true);
        // A red elsewhere corroborates even beside a green, and even when it
        // ran first.
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                &[green.clone(), red.clone()],
                10 * HOUR
            ),
            Decision::Corroborated(red.clone())
        );
        assert_eq!(
            decide(
                HoldReason::HostCondition,
                &row,
                std::slice::from_ref(&red),
                10 * HOUR
            ),
            Decision::Corroborated(red)
        );
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                std::slice::from_ref(&green),
                10 * HOUR
            ),
            Decision::Released(Release::GreenElsewhere(green))
        );
    }

    #[test]
    fn the_same_host_never_corroborates_and_its_next_red_ends_the_wait() {
        let row = held_row();
        // A rerun of the same tree on the same host is not a second host.
        let rerun = seen("rerun", "fedora-id", "c1", 11 * HOUR, false);
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                std::slice::from_ref(&rerun),
                11 * HOUR
            ),
            Decision::Held {
                until_ms: 16 * HOUR
            }
        );
        let again = seen("again", "fedora-id", "c2", 12 * HOUR, false);
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                &[rerun.clone(), again.clone()],
                12 * HOUR
            ),
            Decision::Uncorroborated(WaitEnded::NextSameHostRow(again.clone()))
        );
        assert_eq!(
            decide(
                HoldReason::HostCondition,
                &row,
                std::slice::from_ref(&again),
                12 * HOUR
            ),
            Decision::Released(Release::HostCondition(WaitEnded::NextSameHostRow(again)))
        );
    }

    #[test]
    fn the_next_newer_row_decides_and_older_rows_do_not() {
        let row = held_row();
        let older_red = seen("older", "sp2-id", "c0", 9 * HOUR, false);
        let newer_green = seen("green", "sp2-id", "c2", 11 * HOUR, true);
        let newer_red = seen("red", "sp2-id", "c3", 12 * HOUR, false);
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                std::slice::from_ref(&older_red),
                11 * HOUR
            ),
            Decision::Held {
                until_ms: 16 * HOUR
            }
        );
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                &[newer_red.clone(), newer_green.clone()],
                12 * HOUR
            ),
            Decision::Released(Release::GreenLater(newer_green))
        );
        assert_eq!(
            decide(
                HoldReason::Duration,
                &row,
                std::slice::from_ref(&newer_red),
                12 * HOUR
            ),
            Decision::Released(Release::Superseded(newer_red))
        );
    }

    #[test]
    fn without_a_second_host_the_bound_files_a_duration_and_records_a_host_condition() {
        let row = held_row();
        assert_eq!(
            decide(HoldReason::Duration, &row, &[], 16 * HOUR - 1),
            Decision::Held {
                until_ms: 16 * HOUR
            }
        );
        assert_eq!(
            decide(HoldReason::Duration, &row, &[], 16 * HOUR),
            Decision::Uncorroborated(WaitEnded::Bound)
        );
        assert_eq!(
            decide(HoldReason::HostCondition, &row, &[], 16 * HOUR),
            Decision::Released(Release::HostCondition(WaitEnded::Bound))
        );
    }
}
