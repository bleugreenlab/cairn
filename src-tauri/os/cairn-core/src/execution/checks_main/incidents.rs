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

/// Only the harness header varies without changing an assertion. In particular,
/// numbers, expected/actual values, and the rest of the diagnostic are retained.
fn assertion_signature(excerpt: &str) -> String {
    let mut lines = excerpt.trim().lines();
    let first = lines.next().unwrap_or("");
    if first.starts_with("thread '") && first.contains("panicked at") {
        lines.collect::<Vec<_>>().join("\n")
    } else {
        excerpt.trim().to_string()
    }
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

fn infrastructure_signature(diagnostic: &str) -> String {
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
    if named.is_empty() {
        assertion_signature(diagnostic)
    } else {
        named.join("\n")
    }
}

async fn existing_issue(
    db: &LocalDb,
    project: &str,
    key: &str,
    identity: &str,
    signature: &str,
    fingerprint: &str,
    test_identity: bool,
) -> DbResult<Option<String>> {
    let candidates = db.query_all(
        "SELECT i.number,i.title,COALESCE(i.description,''),
                EXISTS(SELECT 1 FROM main_incidents m WHERE m.issue_id=i.id AND m.class='infrastructure')
         FROM issues i WHERE i.project_id=?1 AND i.status NOT IN ('closed','merged')
           AND i.closed_at IS NULL AND i.merged_at IS NULL",
        (project.to_string(),),|r|Ok((r.i64(0)?,r.text(1)?,r.text(2)?,r.i64(3)?!=0)),
    ).await?;
    let marker = format!("Main incident fingerprint: {fingerprint}");
    let matches: Vec<_> = candidates
        .into_iter()
        .filter(|(_, title, body, infrastructure)| {
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
            let title_identity = test_identity && names_identity(title, identity);
            let diagnostic = meaningful_diagnostic(signature) && body.contains(signature);
            explicit
                || (failure_title
                    && !(test_identity && *infrastructure)
                    && (title_identity
                        || diagnostic && (!test_identity || names_identity(body, identity))))
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
        let known = existing_issue(db, project, key, test, &signature, &fingerprint, true).await?;
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
            key,
            &identity,
            &diagnostic,
            &fingerprint,
            false,
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
        let known =
            existing_issue(db, project, key, &identity, &signature, &fingerprint, false).await?;
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
        let bracket = Bracket {
            merges: vec![BracketMerge {
                commit: "merge".into(),
                subject: "feature".into(),
                pr: Some(42),
                issue: Some("cairn://p/proj/1".into()),
            }],
            truncated: false,
            unavailable: None,
        };
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
    async fn passing_mentions_do_not_claim_failures_but_exact_compile_diagnostics_and_markers_attach(
    ) {
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
            "proj",
            "binary$test",
            "assertion failed",
            "fp",
            true
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            existing_issue(
                &db,
                "p",
                "proj",
                "rust-tests:linux",
                "error[E0061]: function expects 3 arguments but 2 supplied",
                "compile-fp",
                false
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/proj/2")
        );
        assert_eq!(
            existing_issue(
                &db,
                "p",
                "proj",
                "rust-tests:linux",
                "unknown",
                "explicit-fp",
                false
            )
            .await
            .unwrap()
            .as_deref(),
            Some("cairn://p/proj/3")
        );
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
}
