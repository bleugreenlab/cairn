//! Incident ingestion consumes cadence evidence; it never schedules a test or
//! invents a verdict. Fingerprints name identities, not similar-looking prose.

use super::*;
use crate::execution::main_incidents_store::{self, IncidentDraft};
use crate::issues::crud::IssueAuthorship;
use crate::storage::{DbError, DbResult};
use sha2::{Digest, Sha256};

fn digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("v1:{:x}", hash.finalize())
}

/// A test failure's signature is its assertion as the test states it, so every
/// run of one failure mechanism must produce the same text. Two kinds of text
/// are not the assertion and leave it:
///
/// - Harness lines, wherever they appear: libtest's slow-test notice, the
///   `thread '<name>' (<os tid>) panicked at <path>:<line>:<col>:` header (the
///   thread id is per run and the location drifts with unrelated edits; text
///   after it on the same line stays), the `RUST_BACKTRACE` notes, and
///   nextest's status and output-section lines.
/// - Run nonces in what remains, which become placeholders: temporary-directory
///   paths, UUIDs and hex runs of 12 or more characters (SHAs minted by a test's
///   own temporary repository), Unix-time readings, OS thread and process ids,
///   and loopback ports.
///
/// Every other number stays, because expected/actual counts distinguish
/// mechanisms. Lines are trimmed and blank lines dropped, since which harness
/// lines surrounded them decides where blanks fall.
fn assertion_signature(excerpt: &str) -> String {
    static HARNESS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(concat!(
            r"^(?:test \S+ has been running for over \d+ seconds$",
            r"|note: run with `RUST_BACKTRACE=",
            r"|note: Some details are omitted, run with `RUST_BACKTRACE=",
            r"|(?:TRY \d+ )?(?:START|SLOW|TERMINATING|SIGTERM|SIGKILL|SIGSEGV|SIGABRT|LEAK|PASS|FAIL|TIMEOUT|RETRY)\s+\[",
            r"|-{3} (?:TRY \d+ )?STD(?:OUT|ERR):)",
        ))
        .expect("valid test harness line regex")
    });
    static PANIC_HEADER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^thread '[^']*'(?: \(\d+\))? panicked at (?:\S+?(?::\d+){1,2}:)?\s*")
            .expect("valid panic header regex")
    });
    static NONCES: LazyLock<[(Regex, &str); 7]> = LazyLock::new(|| {
        let nonce = |pattern: &str| Regex::new(pattern).expect("valid run nonce regex");
        [
            (
                nonce(concat!(
                    r#"(?:/private)?/(?:tmp|var/folders)/[^\s"'`,;()\[\]{}<>]*"#,
                    r#"|[A-Za-z]:\\[^\s"'`,;()\[\]{}<>]*\\Temp\\[^\s"'`,;()\[\]{}<>]*"#,
                    r"|\.tmp[0-9A-Za-z]{6}",
                )),
                "<tmp>",
            ),
            (
                nonce(
                    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
                ),
                "<id>",
            ),
            (nonce(r"\b[0-9a-fA-F]{12,}\b"), "<id>"),
            // Seconds, milliseconds, microseconds or nanoseconds since 2001-09-09;
            // a ten-digit Windows status code (2147500037) is not one.
            (nonce(r"\b1\d{9}(?:\d{3}|\d{6}|\d{9})?\b"), "<unix-time>"),
            (
                nonce(r"\b(pid|tid|PID|TID)([\s:=#]*)\d+\b"),
                "${1}${2}<pid>",
            ),
            (nonce(r"\bThreadId\(\d+\)"), "ThreadId(<tid>)"),
            (
                nonce(r"(127\.0\.0\.1|0\.0\.0\.0|localhost|\[::1\]):\d+\b"),
                "${1}:<port>",
            ),
        ]
    });
    let mut signature = excerpt
        .lines()
        .map(str::trim)
        .filter(|line| !HARNESS.is_match(line))
        .map(|line| PANIC_HEADER.replace(line, ""))
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.trim().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    for (pattern, placeholder) in NONCES.iter() {
        signature = pattern.replace_all(&signature, *placeholder).into_owned();
    }
    signature
}

fn disk_full(excerpt: &str) -> bool {
    let lower = excerpt.to_lowercase();
    lower.contains("no space left on device")
        || lower.contains("(os error 28)")
        || lower.contains("storagefull")
}

async fn observation_context(
    db: &LocalDb,
    project: &str,
    row: &CadenceRow,
) -> DbResult<(String, String)> {
    let observation_id = row.id.clone();
    let row = row.clone();
    db.query_opt(
        "SELECT tree_hash, environment_fingerprint, toolchain_fingerprint,
                verdict_arch, runner_build_id, duration_ms
           FROM check_result_observations WHERE project_id=?1 AND id=?2",
        (project.to_string(), row.id.clone()),
        move |r| Ok((format!(
            "- Observation: {}\n- Actual commit: `{}`\n- Actual tree: `{}`\n- Platform/architecture: `{}` / `{}`\n- Environment class: `{}`\n- Runtime/toolchain: `{}`\n- Runner build: `{}`\n- Executor: `{}`\n- Ran at (Unix ms): {}\n- Duration (ms): {}\n",
            row.reference, row.commit, r.text(0)?, row.platform, r.opt_text(3)?.unwrap_or_else(|| "unavailable".into()),
            r.text(1)?, r.opt_text(2)?.unwrap_or_else(|| "unavailable".into()),
            r.opt_text(4)?.unwrap_or_else(|| "unavailable".into()),
            row.executor_id.as_deref().unwrap_or("unavailable"), row.ran_at_ms, r.i64(5)?
        ), r.text(1)?)),
    ).await?.ok_or_else(|| DbError::Row(format!("incident observation {observation_id} is unavailable")))
}

/// Match complete identity tokens, not substrings (foo is not foo_bar).
fn names_identity(text: &str, identity: &str) -> bool {
    fn identity_char(c: char) -> bool {
        c.is_alphanumeric() || matches!(c, '_' | ':' | '$' | '/' | '.' | '-')
    }
    text.match_indices(identity).any(|(start, _)| {
        let end = start + identity.len();
        !text[..start].chars().next_back().is_some_and(identity_char)
            && !text[end..].chars().next().is_some_and(identity_char)
    })
}

/// Only issue records are consulted. Messages and free-text thread claims are
/// not an ownership registry. Ambiguous matches remain unavailable.
fn meaningful_diagnostic(signature: &str) -> bool {
    signature.len() >= 16
        && [
            "assert",
            "error[",
            "error:",
            "error ts",
            "timeout",
            "no space left",
            "permission denied",
        ]
        .iter()
        .any(|word| signature.to_lowercase().contains(word))
}

/// A placement refusal ([`crate::fleet::Fleet::unplaceable_reason`]) ends by
/// listing every machine the fleet knows of. That inventory moves whenever a
/// machine attaches, detaches, re-advertises or re-enrolls under another name,
/// while the cause (the requirement no machine satisfies) stays the same, so
/// it is evidence and never identity. The list runs to the first full stop at
/// parenthesis depth zero that ends a sentence: a machine's shortfall is a
/// parenthesised sentence of its own, and a public name may be dotted.
fn without_fleet_inventory(diagnostic: &str) -> String {
    const MARKER: &str = "Known machines: ";
    let mut out = String::with_capacity(diagnostic.len());
    let mut rest = diagnostic;
    while let Some(start) = rest.find(MARKER) {
        let list = &rest[start + MARKER.len()..];
        let mut depth = 0usize;
        let mut end = None;
        for (index, c) in list.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                '\n' if depth == 0 => end = Some(index),
                '.' if depth == 0
                    && list[index + 1..]
                        .chars()
                        .next()
                        .is_none_or(char::is_whitespace) =>
                {
                    end = Some(index)
                }
                _ => {}
            }
            if end.is_some() {
                break;
            }
        }
        let Some(end) = end else { break };
        out.push_str(&rest[..start + MARKER.len()]);
        out.push_str("<fleet inventory>");
        rest = &list[end..];
    }
    out.push_str(rest);
    out
}

fn infrastructure_signature(diagnostic: &str) -> String {
    let diagnostic = without_fleet_inventory(diagnostic);
    let diagnostic = diagnostic.as_str();
    // Delivery ids and attested commits vary between attempts. Keep numeric
    // error codes: unlike elapsed time they can distinguish failure mechanisms.
    static IDS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
        r"\b(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{7,64})\b"
    ).expect("valid infrastructure identity regex")
    });
    IDS.replace_all(
        &ELAPSED.replace_all(diagnostic, "<elapsed>"),
        |captures: &regex::Captures<'_>| {
            let token = &captures[0];
            if token.contains('-')
                || token
                    .chars()
                    .any(|c| c.is_ascii_hexdigit() && !c.is_ascii_digit())
            {
                "<id>".to_string()
            } else {
                token.to_string()
            }
        },
    )
    .into_owned()
}

fn lane_signature(diagnostic: &str) -> String {
    let lines: Vec<_> = diagnostic.lines().map(str::trim).collect();
    let mut named = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if (line.starts_with("error[") || line.starts_with("error:") || line.contains(": error TS"))
            && !line.contains("could not compile")
            && !line.contains("aborting due to")
        {
            named.push(*line);
            if let Some(location) = lines
                .iter()
                .skip(index + 1)
                .take(4)
                .find(|line| line.starts_with("-->"))
            {
                named.push(*location);
            }
        }
    }
    // Lane output is not a test assertion: without a named compiler error it
    // keeps its text, so the test signature rule never moves a lane fingerprint.
    if named.is_empty() {
        diagnostic.trim().to_string()
    } else {
        named.join("\n")
    }
}

async fn existing_issue(
    db: &LocalDb,
    project: &str,
    identity: &str,
    signature: &str,
    fingerprint: &str,
    class: &str,
    observed_at: Option<i64>,
) -> DbResult<Option<String>> {
    let key = db
        .query_one(
            "SELECT key FROM projects WHERE id=?1",
            (project.to_string(),),
            |r| r.text(0),
        )
        .await?;
    // The fingerprint's durable link (historical generation covering the
    // observation, else the latest open one) is canonical: the store attaches
    // there first, so it beats labels and prose and bypasses label ambiguity.
    if let Some(uri) =
        main_incidents_store::linked_issue_uri(db, project, fingerprint, observed_at).await?
    {
        return Ok(Some(uri));
    }
    // Next, an open incident for this test owns its identity whatever the
    // signature (the store applies the same rule again when it files).
    if let Some(uri) =
        main_incidents_store::open_identity_issue_uri(db, project, class, identity).await?
    {
        return Ok(Some(uri));
    }
    let label = format!("incident:{class}:{identity}");
    // Test classes are one family; every other class stands alone. A prose
    // candidate that already owns persisted incidents of other families
    // (e.g. an infrastructure issue titled "Fix rust-tests:macos placement
    // failure") never absorbs this class by name.
    let (family_a, family_b) = if matches!(class, "test-failure" | "evidence-gap") {
        ("test-failure", "evidence-gap")
    } else {
        (class, class)
    };
    let candidates = db.query_all(
        "SELECT i.number,i.title,COALESCE(i.description,''),
                EXISTS(SELECT 1 FROM main_incidents m WHERE m.issue_id=i.id)
                AND NOT EXISTS(SELECT 1 FROM main_incidents m WHERE m.issue_id=i.id AND m.class IN (?2,?3))
         FROM issues i WHERE i.project_id=?1 AND i.status NOT IN ('closed','merged')
           AND i.closed_at IS NULL AND i.merged_at IS NULL",
        (project.to_string(), family_a.to_string(), family_b.to_string()),
        |r|Ok((r.i64(0)?,r.text(1)?,r.text(2)?,r.i64(3)?!=0)),
    ).await?;
    // An explicit `incident:<class>:<identity>` label is the owner registry: it wins
    // over every prose heuristic, and two unresolved owners refuse rather than
    // letting either one (or a new issue) be chosen arbitrarily.
    let labelled = db
        .query_all(
            "SELECT i.number FROM issues i
           JOIN projects p ON p.id=i.project_id
           JOIN issue_labels il ON il.issue_id=i.id
           JOIN labels l ON l.id=il.label_id AND l.workspace_id=p.workspace_id
          WHERE i.project_id=?1 AND l.name=?2
            AND i.status NOT IN ('closed','merged')
            AND i.closed_at IS NULL AND i.merged_at IS NULL
          ORDER BY i.number",
            (project.to_string(), label.clone()),
            |r| r.i64(0),
        )
        .await?;
    match labelled.as_slice() {
        [] => {}
        [number] => return Ok(Some(format!("cairn://p/{key}/{number}"))),
        many => {
            let owners: Vec<_> = many
                .iter()
                .map(|n| format!("cairn://p/{key}/{n}"))
                .collect();
            return Err(DbError::Row(format!(
                "ambiguous incident ownership: label `{label}` is on {} unresolved issues ({}); no incident was filed. Remove the label from all but one.",
                many.len(),
                owners.join(", ")
            )));
        }
    }
    let marker = format!("Main incident fingerprint: {fingerprint}");
    let matches: Vec<_> = candidates
        .into_iter()
        .filter(|(_, title, body, other_class_owner)| {
            let explicit = body.lines().any(|line| line.trim() == marker);
            let lower = title.to_lowercase();
            let failure_title = [
                "fail",
                "fix ",
                "red ",
                "panic",
                "hang",
                "flak",
                "broken",
                "no-verdict",
                "do not compile",
                "does not compile",
            ]
            .iter()
            .any(|word| lower.contains(word));
            let title_identity = names_identity(title, identity);
            let diagnostic = meaningful_diagnostic(signature) && body.contains(signature);
            explicit
                || (failure_title
                    && !*other_class_owner
                    && (title_identity || diagnostic && names_identity(body, identity)))
        })
        .collect();
    Ok(if matches.len() == 1 {
        Some(format!("cairn://p/{key}/{}", matches[0].0))
    } else {
        None
    })
}

async fn bracket_owner(
    db: &LocalDb,
    project: &str,
    key: &str,
    report: &CadenceReport<'_>,
    test: &str,
    environment: &str,
) -> DbResult<Option<String>> {
    let Comparison::SameExecutor { previous, .. } = report.comparison else {
        return Ok(None);
    };
    if !matches!(previous.tests.get(test), Some(TestOutcome::Passed))
        || report.bracket.truncated
        || report.bracket.unavailable.is_some()
        || report.bracket.merges.len() != 1
    {
        return Ok(None);
    }
    let prior_environment = db.query_opt_text(
        "SELECT environment_fingerprint FROM check_result_observations WHERE project_id=?1 AND id=?2",
        (project.to_string(), previous.id.clone()),
    ).await?;
    if environment.is_empty() || prior_environment.as_deref() != Some(environment) {
        return Ok(None);
    }
    let Some(issue) = report.bracket.merges[0].issue.as_deref() else {
        return Ok(None);
    };
    let prefix = format!("cairn://p/{key}/");
    let Some(number) = issue
        .strip_prefix(&prefix)
        .and_then(|n| n.parse::<i64>().ok())
    else {
        return Ok(None);
    };
    let name = db
        .query_opt_text(
            "SELECT t.name FROM issues i JOIN threads t ON t.id=i.parent_thread_id
          WHERE i.project_id=?1 AND i.number=?2 AND t.project_id=?1 AND t.status='active'",
            (project.to_string(), number),
        )
        .await?;
    Ok(name.map(|name| format!("cairn://p/{key}/{name}")))
}

fn comparable_note(report: &CadenceReport<'_>, test: Option<&str>) -> String {
    match report.comparison {
        Comparison::Unavailable { cause, .. } => format!("Comparison unavailable: {cause}."),
        Comparison::Baseline => {
            "Coverage: never observed on a complete comparable lane row (not green).".into()
        }
        comparison => {
            let previous = comparison
                .previous()
                .expect("comparison has a previous row");
            let status = test
                .map(|t| match previous.tests.get(t) {
                    Some(TestOutcome::Passed) => "passed",
                    Some(TestOutcome::Failed(_)) => "already red",
                    Some(TestOutcome::Skipped) => "skipped, not observed",
                    None => "not observed on that row",
                })
                .unwrap_or("complete lane verdict");
            format!(
                "Last comparison: {} at commit `{}`, ran at {}: {status}. {} {}",
                previous.reference,
                previous.commit,
                previous.ran_at_ms,
                if report.now_ms - previous.ran_at_ms > SILENCE_BOUND_MS {
                    "Coverage was formerly observed and is now stale, not never observed."
                } else {
                    "Coverage is formerly observed."
                },
                if matches!(comparison, Comparison::SameExecutor { .. }) {
                    "Same executor; environment comparability is checked separately for introduction attribution."
                } else {
                    "Different or unknown executor; no introduction or host-cause claim is made."
                }
            )
        }
    }
}

fn evidence_body(
    report: &CadenceReport<'_>,
    context: &str,
    identity: &str,
    excerpt: &str,
    attribution: &str,
) -> String {
    let mut body = format!(
        "Main-cadence incident evidence (fingerprint schema v1).\n\nIdentity: `{identity}`\n\n{context}\n{attribution}\n\n{}\n\n",
        comparable_note(report, Some(identity))
    );
    if let Some(row) = report.verdict.row() {
        body.push_str(&format!(
            "Recorded population: {} terminal rows ({} passed, {} failed, {} skipped); complete: {}; selection: {}.\n",
            row.tests.len(), row.passed(), row.failed(), row.skipped(), row.complete,
            row.selection.as_deref().unwrap_or("unknown"),
        ));
        if excerpt.trim().is_empty() && !row.output_tail.trim().is_empty() {
            body.push_str(&format!(
                "\nLane output (not an attributed assertion):\n~~~text\n{}\n~~~\n",
                row.output_tail
            ));
        }
    }
    render_bracket(
        &mut body,
        "Candidate merge bracket (not proof of cause)",
        report.bracket,
    );
    if excerpt.trim().is_empty() {
        body.push_str(
            "\nFailure assertion unavailable. Diagnose the evidence gap; no cause is inferred.\n",
        );
    } else {
        body.push_str(&format!(
            "\nRetained diagnostic:\n~~~text\n{excerpt}\n~~~\n"
        ));
    }
    body.push_str("\nPost-fix acceptance: the owner must state the repair commit, required comparable platform/runtime and repeat count, then cite that evidence before resolving. A green sample never automatically closes this issue. Issue resolution and latest test status are independent. No repair execution or merge was launched.\n");
    body
}

pub(super) async fn record(
    db: &LocalDb,
    project: &str,
    key: &str,
    notify: &str,
    report: &CadenceReport<'_>,
    authorship: IssueAuthorship,
    delivery: Option<(&str, i64)>,
) -> DbResult<String> {
    if matches!(report.verdict, CadenceVerdict::Cancelled { .. }) {
        return Ok(String::new());
    }
    let row = report.verdict.row();
    let (context, environment) = match row {
        Some(row) => observation_context(db, project, row).await?,
        None => (format!(
            "- Requested commit: `{}`\n- Requested platform: `{}`\n- Actual tree/platform/runtime/executor: unavailable; no observation was produced.\n",
            report.commit.unwrap_or("unresolved"), report.platform), String::new()),
    };
    let fallback = format!("cairn://p/{key}/{notify}");
    let execution = db
        .query_opt_text(
            "SELECT name FROM threads WHERE project_id=?1 AND name='execution' AND status='active'",
            (project.to_string(),),
        )
        .await?
        .map(|name| format!("cairn://p/{key}/{name}"));
    let event = match row {
        Some(row) => row.id.clone(),
        None => delivery.map(|(key, _)| key.to_string()).ok_or_else(|| {
            DbError::Row("row-less incident requires a durable attempt identity".into())
        })?,
    };
    let observed_at = row
        .map(|r| r.ran_at_ms)
        .or(delivery.map(|(_, at)| at))
        .unwrap_or(report.now_ms)
        / 1000;
    let mut drafts = Vec::new();
    // Do not turn a no-verdict into failures of every selected test. Only
    // terminal failed rows can name an affected test.
    let failed = if let Some(row) = row {
        db.query_all(
            "SELECT test_id, COALESCE(failure_excerpt,'') FROM check_test_results
              WHERE observation_id=?1 AND status='failed' ORDER BY test_id",
            (row.id.clone(),),
            |r| Ok((r.text(0)?, r.text(1)?)),
        )
        .await?
    } else {
        Vec::new()
    };
    let mut disk_tests = Vec::new();
    for (test, excerpt) in &failed {
        if disk_full(excerpt) {
            disk_tests.push((test, excerpt));
            continue;
        }
        let signature = assertion_signature(excerpt);
        let diagnostic_known = [
            "assert",
            "panic",
            "error",
            "timeout",
            "timed out",
            "expected",
            "failed",
        ]
        .iter()
        .any(|word| excerpt.to_lowercase().contains(word));
        let class = if signature.is_empty() || !diagnostic_known {
            "evidence-gap"
        } else {
            "test-failure"
        };
        let fingerprint = digest(&[class, report.check, test, &signature]);
        let known = existing_issue(
            db,
            project,
            test,
            &signature,
            &fingerprint,
            class,
            Some(observed_at),
        )
        .await?;
        let suspect = if known.is_none() {
            bracket_owner(db, project, key, report, test, &environment).await?
        } else {
            None
        };
        let attribution = if let Some(issue) = &known {
            format!("Established incident: {issue}; existing issue ownership is retained.")
        } else if suspect.is_some() {
            format!("Bracket-derived suspected introduction: {}. Exactly one merge follows a comparable passing row; this is not proven causation.",
                report.bracket.merges[0].issue.as_deref().unwrap_or("unknown"))
        } else {
            "Attribution unavailable. No unique existing issue or single comparable introduction bracket establishes ownership.".into()
        };
        drafts.push(IncidentDraft {
            fingerprint,
            identity: test.clone(),
            signature,
            class: class.into(),
            title: format!("Main {class}: {}", truncate(test, 180)),
            body: evidence_body(report, &context, test, excerpt, &attribution),
            owner_uri: Some(suspect.unwrap_or_else(|| fallback.clone())),
            existing_issue_uri: known,
            event_key: event.clone(),
            observation_id: row.map(|r| r.id.clone()),
            observed_at: Some(observed_at),
        });
    }
    // A positive operating-system disk-full diagnostic establishes the shared
    // resource failure. A timeout or timing overlap never does.
    if !disk_tests.is_empty() {
        let host = row
            .and_then(|r| r.executor_id.as_deref())
            .unwrap_or("unknown");
        let host_identity = row.and_then(|r| r.executor_id.as_deref()).unwrap_or(&event);
        let identity = format!("disk-full:{host_identity}:{}", report.platform);
        let diagnostic = disk_tests
            .iter()
            .map(|(test, excerpt)| format!("{test}\n{excerpt}"))
            .collect::<Vec<_>>()
            .join("\n\n");
        let fingerprint = digest(&["infrastructure", &identity]);
        let known = existing_issue(
            db,
            project,
            &identity,
            &diagnostic,
            &fingerprint,
            "infrastructure",
            Some(observed_at),
        )
        .await?;
        drafts.push(IncidentDraft {
            fingerprint,
            identity: identity.clone(), signature: "os-error-28".into(), class: "infrastructure".into(),
            title: format!("Main infrastructure: disk full on {host}"),
            body: evidence_body(report, &context, &identity, &diagnostic,
                &format!("Positive disk-full evidence; affected population: {} terminal failures. Not a claim that all selected tests failed. Infrastructure owner: {}.",
                    disk_tests.len(), execution.as_deref().unwrap_or("unavailable; notify-thread triage"))),
            owner_uri: Some(execution.clone().unwrap_or_else(|| fallback.clone())),
            existing_issue_uri: known, event_key: event.clone(), observation_id: row.map(|r| r.id.clone()),
            observed_at: Some(observed_at),
        });
    }
    let lane_failure = match report.verdict {
        CadenceVerdict::Death { cause, .. } => Some(("infrastructure", cause.as_str())),
        CadenceVerdict::NoVerdict { cause, .. } => Some(("no-verdict", cause.as_str())),
        CadenceVerdict::Complete(r) if report.kind == LaneVerdict::Exit && !r.verdict_passed => {
            Some(("lane-failure", r.output_tail.as_str()))
        }
        _ => None,
    };
    if let Some((class, cause)) = lane_failure {
        let identity = format!("{}:{}", report.check, report.platform);
        let diagnostic = format!(
            "{cause}\n{}",
            row.map(|r| r.output_tail.as_str()).unwrap_or("")
        );
        let signature = if class == "infrastructure" {
            match row {
                Some(row) => db.query_opt_text(
                    "SELECT failure_signature FROM check_result_observations WHERE project_id=?1 AND id=?2",
                    (project.to_string(), row.id.clone()),
                ).await?.unwrap_or_else(|| infrastructure_signature(&diagnostic)),
                None => infrastructure_signature(&diagnostic),
            }
        } else {
            lane_signature(&diagnostic)
        };
        let fingerprint = digest(&[class, &identity, &signature]);
        let known = existing_issue(
            db,
            project,
            &identity,
            &signature,
            &fingerprint,
            class,
            Some(observed_at),
        )
        .await?;
        drafts.push(IncidentDraft {
            fingerprint,
            identity: identity.clone(),
            signature,
            class: class.into(),
            title: format!("Main {class}: {identity}"),
            body: evidence_body(
                report,
                &context,
                &identity,
                &diagnostic,
                &format!(
                    "Lane incident; no unreported test is charged. {} Attribution is {}.",
                    comparable_note(report, None),
                    if class == "infrastructure" {
                        "infrastructure classification, routed to execution when available"
                    } else {
                        "unavailable; notify-thread diagnosis"
                    }
                ),
            ),
            owner_uri: Some(if class == "infrastructure" {
                execution.unwrap_or(fallback)
            } else {
                fallback
            }),
            existing_issue_uri: known,
            event_key: event,
            observation_id: row.map(|r| r.id.clone()),
            observed_at: Some(observed_at),
        });
    }
    for draft in &mut drafts {
        draft.body.push_str(&format!(
            "\nMain incident fingerprint: {}\n",
            draft.fingerprint
        ));
    }
    let links =
        main_incidents_store::ingest(db, project, drafts, authorship, report.now_ms / 1000).await?;
    let pending = main_incidents_store::pending_status(db, project).await?;
    let created: Vec<_> = links.iter().filter(|link| link.created).collect();
    let mut note = String::new();
    if !created.is_empty() {
        note.push_str("\nDurable main incidents (no executions started):\n");
        for link in created.iter().take(10) {
            note.push_str(&format!("- {}\n", link.issue_uri));
        }
    }
    if pending.count > 0 {
        note.push_str(&format!("\nIncident filing pending: {} retained evidence intents; the existing cadence retries them. Last filing error: {}.\n",
            pending.count, pending.last_error.as_deref().unwrap_or("creation budget reached")));
    }
    Ok(note)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::turso::params;

    async fn fixture() -> (tempfile::TempDir, LocalDb) {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("ingestion.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','Project','proj','/tmp/repo',1,1);
             INSERT INTO threads(id,project_id,name,created_at,updated_at)
               VALUES('testing-thread','p','testing',1,1),('execution-thread','p','execution',1,1),
                     ('feature-thread','p','feature',1,1);",
        )
        .await
        .unwrap();
        (temp, db)
    }

    async fn observation(
        db: &LocalDb,
        id: &str,
        host: &str,
        tests: &[(&str, &str, &str)],
    ) -> CadenceRow {
        let failed = tests.iter().any(|(_, status, _)| *status == "failed");
        let observed_at = (count(db, "check_result_observations").await + 1) * 1000;
        db.execute(
            "INSERT INTO check_result_observations
             (id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,
              exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,
              cadence,output_tail,executor_id,verdict_platform,verdict_arch,toolchain_fingerprint,
              runner_build_id,selection)
             VALUES(?1,'p',?2,?3,'rust-tests','input','env',?4,?5,1,0,1,1,?7,17,
                    'main','retained lane log',?6,'linux','x86_64','rust-runtime','runner','full')",
            params![id,format!("commit-{id}"),format!("tree-{id}"),if failed {100} else {0},
                if failed {"failed"} else {"passed"},host,observed_at],
        ).await.unwrap();
        for (test, status, excerpt) in tests {
            db.execute(
                "INSERT INTO check_test_results(observation_id,test_id,status,failure_excerpt)
                 VALUES(?1,?2,?3,?4)",
                params![id, *test, *status, *excerpt],
            )
            .await
            .unwrap();
        }
        load_row(db, "proj", id, &|host| host.into()).await.unwrap()
    }

    async fn record_verdict(
        db: &LocalDb,
        verdict: &CadenceVerdict,
        comparison: &Comparison,
        bracket: &Bracket,
    ) -> DbResult<String> {
        let report = CadenceReport {
            check: "rust-tests",
            platform: "linux",
            kind: LaneVerdict::PerTest,
            commit: Some("requested-tip"),
            verdict,
            comparison,
            bracket,
            now_ms: 10_000,
            rearmed: None,
        };
        record(
            db,
            "p",
            "proj",
            "testing",
            &report,
            crate::issues::crud::installation_machine_authorship("fixture-device", 1).unwrap(),
            None,
        )
        .await
    }

    async fn count(db: &LocalDb, table: &str) -> i64 {
        db.query_one(format!("SELECT COUNT(*) FROM {table}"), (), |r| r.i64(0))
            .await
            .unwrap()
    }

    async fn body(db: &LocalDb) -> String {
        db.query_one(
            "SELECT body FROM main_incident_evidence ORDER BY created_at LIMIT 1",
            (),
            |r| r.text(0),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn ingestion_to_issue_boundary_deduplicates_concurrent_host_rows_and_preserves_actual_evidence(
    ) {
        let (_temp, db) = fixture().await;
        let a = CadenceVerdict::Complete(
            observation(
                &db,
                "a",
                "host-a",
                &[(
                    "binary$module::test",
                    "failed",
                    "assertion failed: expected 1, got 2",
                )],
            )
            .await,
        );
        let b = CadenceVerdict::Complete(
            observation(
                &db,
                "b",
                "host-b",
                &[(
                    "binary$module::test",
                    "failed",
                    "assertion failed: expected 1, got 2",
                )],
            )
            .await,
        );
        let comparison = Comparison::Baseline;
        let bracket = Bracket::default();
        let (first, second) = tokio::join!(
            record_verdict(&db, &a, &comparison, &bracket),
            record_verdict(&db, &b, &comparison, &bracket)
        );
        first.unwrap();
        second.unwrap();
        record_verdict(&db, &a, &comparison, &bracket)
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "main_incidents").await, 1);
        assert_eq!(count(&db, "main_incident_evidence").await, 2);
        assert_eq!(count(&db, "comments").await, 2);
        assert_eq!(count(&db, "executions").await, 0);
        let body = body(&db).await;
        for expected in [
            "binary$module::test",
            "tree-",
            "commit-",
            "rust-runtime",
            "x86_64",
            "runner",
            "assertion failed",
            "Attribution unavailable",
            "never observed",
            "cairn://p/proj/check-observations/",
        ] {
            assert!(body.contains(expected), "missing {expected}: {body}");
        }
        assert!(!body.contains("Actual commit: `requested-tip`"));
        // The issue is ordinary backlog work, under fallback ownership.
        let (status, parent) = db
            .query_one("SELECT status,parent_thread_id FROM issues", (), |r| {
                Ok((r.text(0)?, r.text(1)?))
            })
            .await
            .unwrap();
        assert_eq!(
            (status.as_str(), parent.as_str()),
            ("backlog", "testing-thread")
        );
    }

    #[tokio::test]
    async fn changed_assertion_values_are_preserved_without_grouping_unrelated_tests() {
        let (_temp, db) = fixture().await;
        let a = CadenceVerdict::Complete(
            observation(
                &db,
                "a",
                "host",
                &[
                    (
                        "binary$module::test",
                        "failed",
                        "assertion failed: expected 1, got 2",
                    ),
                    (
                        "binary$unrelated::test",
                        "failed",
                        "assertion failed: expected 1, got 2",
                    ),
                ],
            )
            .await,
        );
        record_verdict(&db, &a, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        let b = CadenceVerdict::Complete(
            observation(
                &db,
                "b",
                "host",
                &[(
                    "binary$module::test",
                    "failed",
                    "assertion failed: expected 1, got 3",
                )],
            )
            .await,
        );
        record_verdict(&db, &b, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        // A changed signature is a new incident identity on the known open issue.
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 3);
        assert_eq!(count(&db, "main_incident_evidence").await, 3);
    }

    #[tokio::test]
    async fn disk_full_has_one_infrastructure_incident_with_explicit_population_but_timeouts_do_not_group(
    ) {
        let (_temp, db) = fixture().await;
        let disk = CadenceVerdict::Complete(
            observation(
                &db,
                "disk",
                "host",
                &[
                    (
                        "binary$one",
                        "failed",
                        "No space left on device (os error 28)",
                    ),
                    ("binary$two", "failed", "write: No space left on device"),
                    ("binary$pass", "passed", ""),
                ],
            )
            .await,
        );
        record_verdict(&db, &disk, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        let (class,parent) = db.query_one(
            "SELECT m.class,i.parent_thread_id FROM main_incidents m JOIN issues i ON i.id=m.issue_id",
            (),|r|Ok((r.text(0)?,r.text(1)?))).await.unwrap();
        assert_eq!(
            (class.as_str(), parent.as_str()),
            ("infrastructure", "execution-thread")
        );
        let body = body(&db).await;
        assert!(body.contains("affected population: 2"));
        assert!(body.contains("binary$one") && body.contains("binary$two"));
        let timed = CadenceVerdict::Complete(
            observation(
                &db,
                "timed",
                "host",
                &[
                    ("binary$one", "failed", "TIMEOUT: test timed out in 5000ms"),
                    ("binary$two", "failed", "TIMEOUT: test timed out in 5000ms"),
                ],
            )
            .await,
        );
        record_verdict(&db, &timed, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "main_incidents").await, 3);
    }

    #[tokio::test]
    async fn unknown_assertion_and_compile_no_verdict_create_diagnosis_not_fabricated_test_failures(
    ) {
        let (_temp, db) = fixture().await;
        let unknown = CadenceVerdict::Complete(
            observation(
                &db,
                "unknown",
                "host",
                &[("binary$unknown", "failed", "unrecognized runner output")],
            )
            .await,
        );
        record_verdict(&db, &unknown, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        let class = db
            .query_one("SELECT class FROM main_incidents", (), |r| r.text(0))
            .await
            .unwrap();
        assert_eq!(class, "evidence-gap");
        let compile = CadenceVerdict::NoVerdict {
            row: Some(observation(&db, "compile", "host", &[]).await),
            cause: "test targets did not compile; assertion unavailable".into(),
        };
        record_verdict(&db, &compile, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "main_incidents").await, 2);
        assert_eq!(count(&db, "check_test_results").await, 1);
        let lane_body = db.query_one(
            "SELECT e.body FROM main_incident_evidence e JOIN main_incidents m
             ON m.project_id=e.project_id AND m.fingerprint=e.fingerprint WHERE m.class='no-verdict'",
            (),|r|r.text(0)).await.unwrap();
        assert!(lane_body.contains("no unreported test is charged"));
        assert!(lane_body.contains("retained lane log"));
    }

    #[tokio::test]
    async fn single_merge_owner_requires_same_host_environment_and_a_passing_test() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at)
             VALUES('intro','p',1,'Feature','merged','feature-thread',1,1);
             UPDATE projects SET next_issue_number=2 WHERE id='p';"
        ).await.unwrap();
        let previous = observation(&db, "prior", "host", &[("binary$test", "passed", "")]).await;
        let comparison = Comparison::SameExecutor {
            previous: previous.clone(),
            neighbour: None,
            peer: None,
            unread: Unread::default(),
        };
        let bracket = bracket_from_log(
            &db,
            "p",
            "proj",
            "merge\tbase\tfeature proj/1 (#42)\n",
            "base",
            "merge",
        )
        .await;
        db.execute_script(
            "INSERT INTO merge_requests
             (id,job_id,project_id,issue_id,title,source_branch,target_branch,status,opened_at,updated_at,github_pr_number)
             VALUES('mr','pr-node','p','intro','Feature','feature','main','merged',1,1,42);"
        ).await.unwrap();
        let resolved = bracket_from_log(
            &db,
            "p",
            "proj",
            "merge\tbase\tfeature (#42)\n",
            "base",
            "merge",
        )
        .await;
        assert_eq!(bracket.merges[0].issue, resolved.merges[0].issue);
        let current = CadenceVerdict::Complete(
            observation(
                &db,
                "red",
                "host",
                &[("binary$test", "failed", "assertion failed")],
            )
            .await,
        );
        let report = CadenceReport {
            check: "rust-tests",
            platform: "linux",
            kind: LaneVerdict::PerTest,
            commit: Some("tip"),
            verdict: &current,
            comparison: &comparison,
            bracket: &bracket,
            now_ms: 10_000,
            rearmed: None,
        };
        assert_eq!(
            bracket_owner(&db, "p", "proj", &report, "binary$test", "env")
                .await
                .unwrap()
                .as_deref(),
            Some("cairn://p/proj/feature")
        );
        assert!(
            bracket_owner(&db, "p", "proj", &report, "binary$test", "different-env")
                .await
                .unwrap()
                .is_none()
        );
        let resolved_report = CadenceReport {
            bracket: &resolved,
            ..report
        };
        assert_eq!(
            bracket_owner(&db, "p", "proj", &resolved_report, "binary$test", "env")
                .await
                .unwrap()
                .as_deref(),
            Some("cairn://p/proj/feature")
        );
        let cross = Comparison::OtherExecutor { previous };
        let cross_report = CadenceReport {
            comparison: &cross,
            ..report
        };
        assert!(
            bracket_owner(&db, "p", "proj", &cross_report, "binary$test", "env")
                .await
                .unwrap()
                .is_none()
        );
        record_verdict(&db, &current, &comparison, &bracket)
            .await
            .unwrap();
        let parent = db
            .query_one(
                "SELECT parent_thread_id FROM issues WHERE number=2",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert_eq!(parent, "feature-thread");
    }

    #[tokio::test]
    async fn known_main_red_is_attached_and_green_does_not_close_repair_then_new_red_recurs() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at)
             VALUES('known','p',1,'Fix `binary$known`','active','feature-thread',1,1);
             UPDATE projects SET next_issue_number=2 WHERE id='p';"
        ).await.unwrap();
        let red = CadenceVerdict::Complete(
            observation(
                &db,
                "red",
                "host",
                &[("binary$known", "failed", "assertion failed")],
            )
            .await,
        );
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        let green = CadenceVerdict::Complete(
            observation(&db, "green", "host", &[("binary$known", "passed", "")]).await,
        );
        record_verdict(&db, &green, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        let status = db
            .query_one("SELECT status FROM issues WHERE id='known'", (), |r| {
                r.text(0)
            })
            .await
            .unwrap();
        assert_eq!(status, "active");
        db.execute(
            "UPDATE issues SET status='closed',closed_at=2 WHERE id='known'",
            (),
        )
        .await
        .unwrap();
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        let recurrence = CadenceVerdict::Complete(
            observation(
                &db,
                "recurrence",
                "host",
                &[("binary$known", "failed", "assertion failed")],
            )
            .await,
        );
        record_verdict(&db, &recurrence, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 2);
    }

    #[tokio::test]
    async fn durable_ingestion_recovers_after_issue_comment_write_interruption() {
        let (temp, db) = fixture().await;
        let red = CadenceVerdict::Complete(
            observation(
                &db,
                "red",
                "host",
                &[("binary$test", "failed", "assertion failed")],
            )
            .await,
        );
        db.execute_script(
            "CREATE TRIGGER reject_comment BEFORE INSERT ON comments
            BEGIN SELECT RAISE(ABORT,'fixture interruption'); END;",
        )
        .await
        .unwrap();
        assert!(
            record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
                .await
                .is_err()
        );
        assert_eq!(count(&db, "issues").await, 0);
        assert_eq!(
            main_incidents_store::pending_status(&db, "p")
                .await
                .unwrap()
                .count,
            1
        );
        db.execute_script("DROP TRIGGER reject_comment;")
            .await
            .unwrap();
        drop(db);
        let db = LocalDb::open_migrated_for_test(temp.path().join("ingestion.db"))
            .await
            .unwrap();
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "comments").await, 1);
        assert_eq!(
            main_incidents_store::pending_status(&db, "p")
                .await
                .unwrap()
                .count,
            0
        );
    }

    #[tokio::test]
    async fn passing_mentions_and_unnamed_compile_diagnostics_do_not_claim_failures() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,description,status,created_at,updated_at)
             VALUES('feature','p',1,'Add feature','Acceptance: `binary$test` passes','backlog',1,1),
                   ('compile','p',2,'Main test targets do not compile','error[E0061]: function expects 3 arguments but 2 supplied','active',1,1),
                   ('marker','p',3,'Existing infrastructure','Main incident fingerprint: explicit-fp','active',1,1);
             UPDATE projects SET next_issue_number=4 WHERE id='p';"
        ).await.unwrap();
        assert!(existing_issue(
            &db,
            "p",
            "binary$test",
            "assertion failed",
            "fp",
            "test-failure",
            None
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            existing_issue(
                &db,
                "p",
                "rust-tests:linux",
                "error[E0061]: function expects 3 arguments but 2 supplied",
                "compile-fp",
                "lane-failure",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            None
        );
        assert_eq!(
            existing_issue(
                &db,
                "p",
                "rust-tests:linux",
                "unknown",
                "explicit-fp",
                "lane-failure",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/proj/3")
        );
        db.execute(
            "UPDATE issues SET description=?1 WHERE id='compile'",
            (
                "rust-tests:linux\nerror[E0061]: function expects 3 arguments but 2 supplied"
                    .to_string(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            existing_issue(
                &db,
                "p",
                "rust-tests:linux",
                "error[E0061]: function expects 3 arguments but 2 supplied",
                "compile-fp",
                "lane-failure",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/proj/2")
        );
        assert!(existing_issue(
            &db,
            "p",
            "rust-tests:linux-other",
            "error[E0061]: function expects 3 arguments but 2 supplied",
            "compile-fp",
            "lane-failure",
            None
        )
        .await
        .unwrap()
        .is_none());
        let row = observation(
            &db,
            "red",
            "host",
            &[("binary$test", "failed", "assertion failed")],
        )
        .await;
        record_verdict(
            &db,
            &CadenceVerdict::Complete(row),
            &Comparison::Baseline,
            &Bracket::default(),
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "issues").await, 4);
        let signature=lane_signature("Compiling crate\nerror[E0061]: function expects 3 arguments but 2 supplied\nother build noise");
        assert_eq!(
            signature,
            "error[E0061]: function expects 3 arguments but 2 supplied"
        );
    }

    #[tokio::test]
    async fn generic_lane_prose_does_not_capture_execution_owned_death() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,description,status,parent_thread_id,created_at,updated_at)
             VALUES('other-lane','p',1,'Fix compile failure','error[E0061]: function expects 3 arguments but 2 supplied','active','feature-thread',1,1);
             UPDATE projects SET next_issue_number=2 WHERE id='p';"
        ).await.unwrap();
        let death = CadenceVerdict::Death {
            row: Some(observation(&db, "death", "host", &[]).await),
            cause: "error[E0061]: function expects 3 arguments but 2 supplied".into(),
        };
        record_verdict(&db, &death, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        let (parent, class, number) = db
            .query_one(
                "SELECT i.parent_thread_id,m.class,i.number FROM main_incidents m
             JOIN issues i ON i.id=m.issue_id",
                (),
                |r| Ok((r.text(0)?, r.text(1)?, r.i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            (parent.as_str(), class.as_str(), number),
            ("execution-thread", "infrastructure", 2)
        );
    }

    #[test]
    fn signature_keeps_assertion_values_and_identity_lookup_is_exact() {
        assert_eq!(
            infrastructure_signature(
                "request aabbccdd-1111-2222-3333-444444444444 at abcdef012345 waited 5s: HTTP 503"
            ),
            infrastructure_signature(
                "request 11223344-5555-6666-7777-888888888888 at 123456abcdef waited 8s: HTTP 503"
            ),
        );
        assert_ne!(
            infrastructure_signature("HTTP 503"),
            infrastructure_signature("HTTP 401")
        );
        assert_ne!(
            infrastructure_signature("error 2147500037"),
            infrastructure_signature("error 2147942405")
        );
        assert_ne!(
            assertion_signature("assertion failed: 1 != 2"),
            assertion_signature("assertion failed: 1 != 3")
        );
        assert_eq!(
            assertion_signature("thread 'test' (123) panicked at a.rs:2:\nassertion failed"),
            assertion_signature("thread 'test' (456) panicked at a.rs:3:\nassertion failed")
        );
        assert!(names_identity("Fix `binary$test`", "binary$test"));
        assert!(!names_identity("Fix `binary$test_extra`", "binary$test"));
    }

    /// A placement refusal exactly as `Fleet::unplaceable_reason` and the
    /// not-placeable recorder word it, as the main cadence's death cause.
    fn unplaceable(wanted: &str, known: &str) -> String {
        format!(
            "infrastructure: No machine enrolled in this fleet, attached or not, satisfies {wanted}. Known machines: {known}. Not attempted: it runs on the first wave after a machine that satisfies it attaches."
        )
    }

    const SEATBELT: &str = "os macos with toolchains bun, rust with measured macOS nested Seatbelt write capability on macos";

    /// The machine lists of the live cairn/6461, cairn/6462 and cairn/6465
    /// specimens: one unchanged refusal while bgsrv-win attached and
    /// bginf-halo re-enrolled as fedora.
    fn live_inventories() -> [String; 3] {
        let list = |halo: &str, win: &str| {
            format!(
                "bbs (linux; bun, cairn-cli, node, npm, rust), {halo}, bginf-sp1 (linux; bun, cairn-cli, node, npm, rust), bgsrv-mac (macos; bun, cairn-cli, node, npm, rust), bgsrv-win ({win}), local (macos; bun, cairn-cli, node, npm, rust)"
            )
        };
        [
            list("bginf-halo (linux; not attached)", "windows; not attached"),
            list(
                "bginf-halo (linux; not attached)",
                "windows; bun, cairn-cli, node, npm, rust",
            ),
            list(
                "fedora (linux; not attached)",
                "windows; bun, cairn-cli, node, npm, rust",
            ),
        ]
    }

    #[test]
    fn placement_signature_excludes_fleet_inventory_but_keeps_requirement_and_cause() {
        let [a, b, c] = live_inventories().map(|known| unplaceable(SEATBELT, &known));
        assert_eq!(infrastructure_signature(&a), infrastructure_signature(&b));
        assert_eq!(infrastructure_signature(&a), infrastructure_signature(&c));
        assert_eq!(
            infrastructure_signature(&a),
            infrastructure_signature(&unplaceable(SEATBELT, "none"))
        );
        // A machine's shortfall is a parenthesised sentence of its own, and a
        // public name may be dotted; neither ends the inventory early.
        assert_eq!(
            infrastructure_signature(&a),
            infrastructure_signature(&unplaceable(
                SEATBELT,
                "mac.lan (macos; bun, rust; nested sandbox writes denied (EPERM). Re-measure.), win (windows; not attached)"
            ))
        );
        let signature = infrastructure_signature(&a);
        assert!(signature.contains(SEATBELT), "{signature}");
        assert!(signature.contains("Not attempted:"), "{signature}");
        assert!(!signature.contains("bgsrv-win"), "{signature}");
        assert_ne!(
            signature,
            infrastructure_signature(&unplaceable(
                "os windows with toolchains bun, rust on windows",
                &live_inventories()[0]
            )),
            "another unmet requirement is another cause"
        );
        assert_ne!(
            signature,
            infrastructure_signature(&format!(
                "infrastructure: ExecutorUnavailable: nothing answered. Known machines: {}.",
                live_inventories()[0]
            )),
            "another failure mechanism is another cause"
        );
    }

    async fn record_placement_death(
        db: &LocalDb,
        cause: String,
        attempt: &str,
        at_ms: i64,
    ) -> DbResult<String> {
        let verdict = CadenceVerdict::Death { row: None, cause };
        let report = CadenceReport {
            check: "rust-tests",
            platform: "macos",
            kind: LaneVerdict::PerTest,
            commit: Some("requested-tip"),
            verdict: &verdict,
            comparison: &Comparison::Baseline,
            bracket: &Bracket::default(),
            now_ms: at_ms,
            rearmed: None,
        };
        record(
            db,
            "p",
            "proj",
            "testing",
            &report,
            crate::issues::crud::installation_machine_authorship("fixture-device", 1).unwrap(),
            Some((attempt, at_ms)),
        )
        .await
    }

    /// cairn/6461, cairn/6462 and cairn/6465: one unchanged row-less placement
    /// refusal filed three issues because each pass's fleet inventory differed.
    #[tokio::test]
    async fn unchanged_placement_cause_with_moved_inventory_attaches_one_incident_across_restart() {
        let (temp, db) = fixture().await;
        let [first, attached, renamed] = live_inventories();
        record_placement_death(&db, unplaceable(SEATBELT, &first), "attempt-1", 10_000)
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        drop(db);
        let db = LocalDb::open_migrated_for_test(temp.path().join("ingestion.db"))
            .await
            .unwrap();
        record_placement_death(&db, unplaceable(SEATBELT, &attached), "attempt-2", 20_000)
            .await
            .unwrap();
        record_placement_death(&db, unplaceable(SEATBELT, &renamed), "attempt-3", 30_000)
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "main_incidents").await, 1);
        assert_eq!(count(&db, "main_incident_evidence").await, 3);
        let (identity, class, parent) = db
            .query_one(
                "SELECT m.identity,m.class,i.parent_thread_id FROM main_incidents m
                 JOIN issues i ON i.id=m.issue_id",
                (),
                |r| Ok((r.text(0)?, r.text(1)?, r.text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            (identity.as_str(), class.as_str(), parent.as_str()),
            ("rust-tests:macos", "infrastructure", "execution-thread")
        );
        // Each attempt's evidence keeps the inventory it was refused under.
        let bodies = db
            .query_all("SELECT body FROM main_incident_evidence", (), |r| r.text(0))
            .await
            .unwrap()
            .join("\n");
        for raw in [
            "bgsrv-win (windows; not attached)",
            "bgsrv-win (windows; bun, cairn-cli, node, npm, rust)",
            "fedora (linux; not attached)",
        ] {
            assert!(bodies.contains(raw), "missing {raw}: {bodies}");
        }

        // Same lane identity, another requirement or mechanism: distinct
        // incidents, whichever issue ends up owning them.
        record_placement_death(
            &db,
            unplaceable("os macos with toolchains bun, rust on macos", &attached),
            "attempt-4",
            40_000,
        )
        .await
        .unwrap();
        record_placement_death(
            &db,
            "infrastructure: ExecutorUnavailable: nothing answered within 120s".into(),
            "attempt-5",
            50_000,
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "main_incidents").await, 3);
    }

    async fn labelled_issue(db: &LocalDb, number: i64, status: &str, label: &str) {
        db.execute(
            "INSERT INTO issues(id,project_id,number,title,description,status,parent_thread_id,created_at,updated_at)
             VALUES(?1,'p',?2,'Owner without identity prose','nothing here',?3,'feature-thread',1,1)",
            params![format!("lab-{number}"), number, status],
        ).await.unwrap();
        db.execute(
            "INSERT OR IGNORE INTO labels(id,workspace_id,name,color,created_at,updated_at)
             VALUES(?1,'w',?1,'#000',1,1)",
            params![label],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issue_labels(issue_id,label_id,created_at) VALUES(?1,?2,1)",
            params![format!("lab-{number}"), label],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE projects SET next_issue_number=?1 WHERE id='p'",
            params![number + 1],
        )
        .await
        .unwrap();
    }

    async fn red_on(db: &LocalDb, id: &str, test: &str) -> CadenceVerdict {
        CadenceVerdict::Complete(
            observation(
                db,
                id,
                "host",
                &[(test, "failed", "assertion failed: boom")],
            )
            .await,
        )
    }

    #[tokio::test]
    async fn labelled_owner_takes_attachment_even_when_failed_and_unrelated_prose() {
        let (_temp, db) = fixture().await;
        // `failed` is an unresolved status; the issue never names the test.
        labelled_issue(&db, 1, "failed", "incident:test-failure:binary$labelled").await;
        let red = red_on(&db, "red", "binary$labelled").await;
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        let number = db
            .query_one(
                "SELECT i.number FROM main_incidents m JOIN issues i ON i.id=m.issue_id",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(number, 1);
        assert_eq!(count(&db, "comments").await, 1);
    }

    #[tokio::test]
    async fn unlabelled_identity_files_a_new_issue_and_other_labels_do_not_own() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "active", "incident:test-failure:binary$other").await;
        let red = red_on(&db, "red", "binary$labelled").await;
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 1);
    }

    #[tokio::test]
    async fn two_unresolved_labelled_owners_refuse_with_a_replayable_diagnostic() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "active", "incident:test-failure:binary$labelled").await;
        labelled_issue(&db, 2, "failed", "incident:test-failure:binary$labelled").await;
        let red = red_on(&db, "red", "binary$labelled").await;
        let error = record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("ambiguous"), "{error}");
        assert!(
            error.contains("incident:test-failure:binary$labelled"),
            "{error}"
        );
        assert!(
            error.contains("proj/1") && error.contains("proj/2"),
            "{error}"
        );
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 0);
        assert_eq!(count(&db, "comments").await, 0);
        // Refusal precedes the journal: source replay retains the observation.
        db.execute("DELETE FROM issue_labels WHERE issue_id='lab-2'", ())
            .await
            .unwrap();
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 1);
    }

    async fn rowless_macos_death(db: &LocalDb, cause: &str, attempt: &str) -> DbResult<String> {
        let verdict = CadenceVerdict::Death {
            row: None,
            cause: cause.into(),
        };
        let report = CadenceReport {
            check: "rust-tests",
            platform: "macos",
            kind: LaneVerdict::PerTest,
            commit: Some("tip"),
            verdict: &verdict,
            comparison: &Comparison::Baseline,
            bracket: &Bracket::default(),
            now_ms: 10_000,
            rearmed: None,
        };
        record(
            db,
            "p",
            "proj",
            "testing",
            &report,
            crate::issues::crud::installation_machine_authorship("fixture-device", 1).unwrap(),
            Some((attempt, 5)),
        )
        .await
    }

    #[tokio::test]
    async fn rowless_macos_death_attaches_to_its_labelled_owner_and_refuses_two() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "failed", "incident:infrastructure:rust-tests:macos").await;
        rowless_macos_death(&db, "No machine satisfies os macos", "attempt-1")
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        let (number, class) = db
            .query_one(
                "SELECT i.number,m.class FROM main_incidents m JOIN issues i ON i.id=m.issue_id",
                (),
                |r| Ok((r.i64(0)?, r.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!((number, class.as_str()), (1, "infrastructure"));
        labelled_issue(&db, 2, "active", "incident:infrastructure:rust-tests:macos").await;
        let error = rowless_macos_death(&db, "a different cause", "attempt-2")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("ambiguous")
                && error.contains("incident:infrastructure:rust-tests:macos"),
            "{error}"
        );
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 1);
    }

    #[tokio::test]
    async fn durable_link_beats_later_labels_and_ambiguity_for_the_same_fingerprint() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "active", "incident:test-failure:binary$labelled").await;
        let first = red_on(&db, "first", "binary$labelled").await;
        record_verdict(&db, &first, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        // Labels arrive on other issues afterwards, even making the label ambiguous.
        labelled_issue(&db, 2, "active", "incident:test-failure:binary$labelled").await;
        labelled_issue(&db, 3, "active", "incident:test-failure:binary$labelled").await;
        let second = red_on(&db, "second", "binary$labelled").await;
        record_verdict(&db, &second, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 3);
        let bodies_on_one = db
            .query_one(
                "SELECT COUNT(*) FROM comments c JOIN issues i ON i.id=c.issue_id WHERE i.number=1",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(bodies_on_one, 2);
        let attribution = db
            .query_one(
                "SELECT body FROM main_incident_evidence WHERE event_key='second'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert!(
            attribution.contains("Established incident: cairn://p/proj/1"),
            "{attribution}"
        );
    }

    #[tokio::test]
    async fn infrastructure_label_does_not_capture_other_lane_classes_on_the_same_identity() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "failed", "incident:infrastructure:rust-tests:linux").await;
        let no_verdict = CadenceVerdict::NoVerdict {
            row: Some(observation(&db, "nv", "host", &[]).await),
            cause: "test targets did not compile; assertion unavailable".into(),
        };
        record_verdict(&db, &no_verdict, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        let class = db
            .query_one(
                "SELECT m.class FROM main_incidents m JOIN issues i ON i.id=m.issue_id WHERE i.number=2",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert_eq!(class, "no-verdict");
    }

    #[tokio::test]
    async fn titled_lane_failure_owner_is_not_captured_by_infrastructure_prose() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,description,status,parent_thread_id,created_at,updated_at)
             VALUES('lane','p',1,'Fix rust-tests:macos compile failure','compile','active','feature-thread',1,1);
             UPDATE projects SET next_issue_number=2 WHERE id='p';
             INSERT INTO main_incidents(project_id,fingerprint,generation,identity,signature,class,issue_id,created_at)
             VALUES('p','older-lane-fp',1,'rust-tests:macos','error','lane-failure','lane',1);",
        )
        .await
        .unwrap();
        rowless_macos_death(&db, "No machine satisfies os macos", "attempt-1")
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
    }

    #[tokio::test]
    async fn titled_infrastructure_owner_with_persisted_rows_is_not_captured_by_prose() {
        let (_temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,description,status,parent_thread_id,created_at,updated_at)
             VALUES('infra','p',1,'Fix rust-tests:linux placement failure','placement','active','execution-thread',1,1);
             UPDATE projects SET next_issue_number=2 WHERE id='p';
             INSERT INTO main_incidents(project_id,fingerprint,generation,identity,signature,class,issue_id,created_at)
             VALUES('p','older-infra-fp',1,'rust-tests:linux','placement','infrastructure','infra',1);",
        )
        .await
        .unwrap();
        let no_verdict = CadenceVerdict::NoVerdict {
            row: Some(observation(&db, "nv", "host", &[]).await),
            cause: "test targets did not compile; assertion unavailable".into(),
        };
        record_verdict(&db, &no_verdict, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
    }

    async fn historical_link_case(with_open_generation: bool) {
        let (_temp, db) = fixture().await;
        let fingerprint = digest(&[
            "test-failure",
            "rust-tests",
            "binary$hist",
            "assertion failed: boom",
        ]);
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,closed_at,created_at,updated_at)
             VALUES('a','p',1,'Old owner A','closed',100,1,1),
                   ('b','p',2,'New owner B','active',NULL,1,1);
             UPDATE projects SET next_issue_number=3 WHERE id='p';",
        )
        .await
        .unwrap();
        for (generation, issue) in [(1, "a"), (2, "b")] {
            if generation == 2 && !with_open_generation {
                continue;
            }
            db.execute(
                "INSERT INTO main_incidents(project_id,fingerprint,generation,identity,signature,class,issue_id,created_at)
                 VALUES('p',?1,?2,'binary$hist','s','test-failure',?3,1)",
                params![fingerprint.clone(), generation, issue],
            )
            .await
            .unwrap();
        }
        // Two unresolved owners carry the class label; the durable link must
        // still bypass the ambiguity.
        labelled_issue(&db, 3, "active", "incident:test-failure:binary$hist").await;
        labelled_issue(&db, 4, "active", "incident:test-failure:binary$hist").await;
        // The delayed observation (1s) predates generation 1's resolution (100s).
        let late = red_on(&db, "late", "binary$hist").await;
        record_verdict(&db, &late, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 4);
        let on_a = db
            .query_one(
                "SELECT COUNT(*) FROM comments c JOIN issues i ON i.id=c.issue_id WHERE i.number=1",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(on_a, 1);
        let body = db
            .query_one(
                "SELECT body FROM main_incident_evidence WHERE event_key='late'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert!(
            body.contains("Established incident: cairn://p/proj/1"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn delayed_evidence_follows_the_historical_generation_despite_ambiguous_labels() {
        historical_link_case(true).await;
        historical_link_case(false).await;
    }

    #[tokio::test]
    async fn resolved_labelled_owner_is_ignored_and_new_event_after_resolution_files_anew() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "active", "incident:test-failure:binary$labelled").await;
        let red = red_on(&db, "red", "binary$labelled").await;
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        // Observations are stamped (n+1) seconds; resolve at the first, strictly
        // before the recurrence, so the historical boundary is untouched.
        db.execute(
            "UPDATE issues SET status='merged',merged_at=1 WHERE id='lab-1'",
            (),
        )
        .await
        .unwrap();
        let recurrence = red_on(&db, "recurrence", "binary$labelled").await;
        record_verdict(&db, &recurrence, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 2);
        let new_owner = db
            .query_one(
                "SELECT i.number FROM main_incidents m JOIN issues i ON i.id=m.issue_id
                 WHERE m.generation=2",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(new_owner, 2);
    }

    // Retained diagnostics of two carried main reds that filed one issue per
    // row (11-07..11-09), exactly as their check observations stored them.
    const PUBLICATION_REPLAY: [(i64, &str); 9] = [
        (6505, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (4158988) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at 72d7d213c4e4d6138b0868a8041735a06a91f533, origin at fed7cc02181a1c0678ab6980ad1802e6e3697dcc); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6560, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (2645238) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at 589093f7443343aaba7e50f1fdb706d01e049fd3, origin at c253ed2fef2a224c6919f2b13dd187a05d9dfee6); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6590, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (1779193) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at 00f3a5aedb0ed9d9092489e9c444981a55b55e0d, origin at e5a347bbbd4e8b8018f47e968e64ab17863d7237); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6602, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (3271138) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at c7a4a5c43b7ec8b294409f2002e1762775361956, origin at cd46ad1133d103761c3b3049cbbe068b26d63687); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6617, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (3744700) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at f87532637f74a0eb84771110277bd891ca6972be, origin at 2e8e6332cde296f0ffb414260ee3760a45f06648); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6626, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (1481723) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at 3dc00e3452a0df694007044acf27680fe4ab46ab, origin at 96d3c2aba068ba28a46cfb75842671444d322c6d); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6644, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (1342811) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at fdd36b9c6e590a3cb2293bf082cded85240b4fc4, origin at 990264328d693dfab5120a37ab4bd66df411e6aa); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6653, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (3861649) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at 756b592a899d18c23719b071792a693efedb7195, origin at 2eb29281c67a0c1b00a563379615739c009ef284); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6661, "test orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip has been running for over 60 seconds\n\nthread 'orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip' (3182373) panicked at os/cairn-core/src/orchestrator/base_branch_sync_tests.rs:1229:13:\nthe replayed parent's publication was never recorded (store at abdad88beec4fdac7a74646f8d3c0a6f018f54f2, origin at 085142238b56637dbe420db14f9e65c2ff414594); publishing now says: Ok(())\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
    ];
    const STALE_BOUNDARY: [(i64, &str); 9] = [
        (6524, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (3938272) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794139560)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6576, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (2411162) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794132975)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6587, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (1620598) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794180836)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6603, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (3087501) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794187119)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6620, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (3560732) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794193613)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6628, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (1286476) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794199948)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6642, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (1082753) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794205616)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6651, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (3641400) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794214509)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
        (6662, "thread 'backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts' (3010261) panicked at os/cairn-core/src/backends/claude.rs:1055:9:\nassertion `left == right` failed\n  left: Some(1794225841)\n right: None\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"),
    ];

    fn test_fingerprint(test: &str, excerpt: &str) -> String {
        digest(&[
            "test-failure",
            "rust-tests",
            test,
            &assertion_signature(excerpt),
        ])
    }

    #[test]
    fn carried_red_diagnostics_keep_one_fingerprint_per_test_and_mechanism() {
        for (test, excerpts, signature) in [
            (
                "cairn-core::cairn_core$orchestrator::base_branch_sync::tests::a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip",
                PUBLICATION_REPLAY,
                "the replayed parent's publication was never recorded (store at <id>, origin at <id>); publishing now says: Ok(())",
            ),
            (
                "cairn-core::cairn_core$backends::claude::account_reach_tests::a_stale_boundary_rearms_instead_of_burning_launch_attempts",
                STALE_BOUNDARY,
                "assertion `left == right` failed\nleft: Some(<unix-time>)\nright: None",
            ),
        ] {
            for (issue, excerpt) in excerpts {
                assert_eq!(assertion_signature(excerpt), signature, "cairn/{issue}");
            }
            let fingerprints: std::collections::BTreeSet<_> = excerpts
                .iter()
                .map(|(_, excerpt)| test_fingerprint(test, excerpt))
                .collect();
            assert_eq!(fingerprints.len(), 1, "{test}: {fingerprints:?}");
        }
        // A different assertion is a different mechanism.
        let (_, replay) = PUBLICATION_REPLAY[0];
        assert_ne!(
            test_fingerprint("t", replay),
            test_fingerprint(
                "t",
                &replay.replace(
                    "publishing now says: Ok(())",
                    "publishing now says: Err(Drift)"
                )
            ),
        );
        let (_, stale) = STALE_BOUNDARY[0];
        assert_ne!(
            test_fingerprint("t", stale),
            test_fingerprint("t", &stale.replace("right: None", "right: Some(3)")),
        );
        // Counts stay, and so do long numbers that are not clock readings.
        assert_ne!(
            assertion_signature("expected 3 rows, got 2"),
            assertion_signature("expected 3 rows, got 4")
        );
        assert_ne!(
            assertion_signature("os error 2147500037"),
            assertion_signature("os error 2147942405")
        );
    }

    #[test]
    fn run_nonces_and_harness_lines_leave_the_signature_wherever_they_appear() {
        let run = |dir: &str, sha: &str, uuid: &str, pid: u32, port: u16, tid: u32| {
            format!(
                "        SLOW [> 60.000s] crate::bin some::test\n\
                 --- STDERR:              crate::bin some::test ---\n\
                 serving on 127.0.0.1:{port} for pid {pid}\n\
                 thread 'some::test' ({tid}) panicked at src/lib.rs:{tid}:9:\n\
                 clone of {dir}/repo at {sha} for job {uuid} failed: 3 of 4 refs\n\
                 note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace."
            )
        };
        let a = run(
            "/tmp/.tmpAb12Cd",
            "0123456789abcdef0123",
            "aabbccdd-1111-2222-3333-444444444444",
            41,
            40001,
            7,
        );
        let b = run(
            "/private/var/folders/x1/T/.tmpZz99Yy",
            "fedcba98765432100000",
            "11223344-5555-6666-7777-888888888888",
            52,
            52002,
            8,
        );
        assert_eq!(
            assertion_signature(&a),
            "serving on 127.0.0.1:<port> for pid <pid>\nclone of <tmp> at <id> for job <id> failed: 3 of 4 refs"
        );
        assert_eq!(assertion_signature(&a), assertion_signature(&b));
        // A header with nothing after it leaves nothing; an excerpt of only
        // harness lines is an evidence gap, not an assertion.
        assert_eq!(
            assertion_signature(
                "test some::test has been running for over 60 seconds\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"
            ),
            ""
        );
    }

    #[tokio::test]
    async fn an_open_incident_owns_its_test_identity_when_the_signature_changes() {
        let (_temp, db) = fixture().await;
        let test = "binary$carried::red";
        let first = CadenceVerdict::Complete(
            observation(
                &db,
                "first",
                "host",
                &[(test, "failed", "assertion failed: replay never recorded")],
            )
            .await,
        );
        record_verdict(&db, &first, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        // A second open issue naming the test leaves no unique prose owner, as
        // the duplicates themselves did on main.
        db.execute_script(&format!(
            "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at)
             VALUES('human','p',2,'Fix failing `{test}`','backlog','feature-thread',1,1);
             UPDATE projects SET next_issue_number=3 WHERE id='p';"
        ))
        .await
        .unwrap();
        let changed = CadenceVerdict::Complete(
            observation(
                &db,
                "changed",
                "host",
                &[(
                    test,
                    "failed",
                    "TIMEOUT: nextest terminated this test at its slow-timeout bound\nran 600s before termination",
                )],
            )
            .await,
        );
        let note = record_verdict(&db, &changed, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        assert!(!note.contains("Durable main incidents"), "{note}");
        let owners = db
            .query_all(
                "SELECT DISTINCT i.number FROM main_incidents m JOIN issues i ON i.id=m.issue_id",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(owners, vec![1]);
        assert_eq!(count(&db, "main_incidents").await, 2);
        let comment = db
            .query_one(
                "SELECT c.content FROM comments c JOIN issues i ON i.id=c.issue_id
                 WHERE i.number=1 AND c.content LIKE '%Signature changed from%'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert!(
            comment.contains("assertion failed: replay never recorded")
                && comment.contains("TIMEOUT: nextest terminated"),
            "{comment}"
        );
        let evidence = db
            .query_one(
                "SELECT body FROM main_incident_evidence WHERE event_key='changed'",
                (),
                |r| r.text(0),
            )
            .await
            .unwrap();
        assert!(
            evidence.contains("Established incident: cairn://p/proj/1"),
            "{evidence}"
        );
    }

    #[tokio::test]
    async fn resolved_second_labelled_owner_does_not_make_ownership_ambiguous() {
        let (_temp, db) = fixture().await;
        labelled_issue(&db, 1, "active", "incident:test-failure:binary$labelled").await;
        labelled_issue(&db, 2, "closed", "incident:test-failure:binary$labelled").await;
        db.execute("UPDATE issues SET closed_at=1 WHERE id='lab-2'", ())
            .await
            .unwrap();
        let red = red_on(&db, "red", "binary$labelled").await;
        record_verdict(&db, &red, &Comparison::Baseline, &Bracket::default())
            .await
            .unwrap();
        assert_eq!(count(&db, "issues").await, 2);
        let number = db
            .query_one(
                "SELECT i.number FROM main_incidents m JOIN issues i ON i.id=m.issue_id",
                (),
                |r| r.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(number, 1);
    }
}
