use serde_json::{json, Value};

use super::*;
use crate::pages::tests::{attempt_count, operator, read, seeded_for_writes, ATTEMPTS};
use crate::storage::RowExt;

/// The read envelope's segment bodies, in target order.
fn bodies(response: &Value) -> Vec<String> {
    let envelope: Value = serde_json::from_str(response["result"].as_str().unwrap())
        .unwrap_or_else(|_| {
            panic!("not a read envelope: {response}");
        });
    envelope["bodies"]
        .as_array()
        .unwrap_or_else(|| panic!("no bodies: {envelope}"))
        .iter()
        .map(|body| body.as_str().unwrap_or_default().to_string())
        .collect()
}

fn change(target: &str, mode: &str, payload: Value) -> Value {
    json!({"target": target, "mode": mode, "payload": payload})
}

async fn write(orch: &Orchestrator, changes: Vec<Value>) -> Result<Value, DbBrowserError> {
    write_db_rows(orch, "p", &json!({ "changes": changes }), &operator()).await
}

fn report(response: &Value) -> Value {
    serde_json::from_str(response["result"].as_str().unwrap()).unwrap()
}

/// A change report leaves `failures` out when there are none.
fn failed(report: &Value) -> bool {
    report["failures"]
        .as_array()
        .is_some_and(|failures| !failures.is_empty())
}

#[tokio::test]
async fn a_browser_read_answers_database_uris_exactly_as_the_read_verb_does() {
    let (orch, _) = seeded_for_writes().await;
    write(
        &orch,
        vec![change(ATTEMPTS, "append", json!({"problem": "two-sum"}))],
    )
    .await
    .unwrap();

    let targets = [
        format!("{ATTEMPTS}?format=json"),
        "cairn://p/prj/roadmap/db?table=attempts&format=json".to_string(),
        "cairn://db/projects?format=json&limit=1".to_string(),
    ];
    let response = read_db(&orch, "p", &targets).await.unwrap();
    let bodies = bodies(&response);
    assert_eq!(bodies.len(), 3);
    assert!(bodies[0].starts_with("{"), "{}", bodies[0]);
    assert!(
        bodies[0].contains("\"problem\":\"two-sum\""),
        "{}",
        bodies[0]
    );
    assert!(
        bodies[1].contains("\"requiredForInsert\":true"),
        "{}",
        bodies[1]
    );
    assert!(
        bodies[2].contains("\"table\":\"projects\""),
        "{}",
        bodies[2]
    );
    // The same body a run-less read of the same URI produces.
    let direct = read(&orch, &targets[0]).await;
    assert!(
        direct.contains("\"problem\":\"two-sum\""),
        "the browser read and the read verb disagree: {direct}"
    );
}

#[tokio::test]
async fn a_browser_read_admits_only_database_uris_in_its_own_project() {
    let (orch, _) = seeded_for_writes().await;
    for target in [
        "cairn://p/prj/tenets/1",
        "cairn://p/prj/roadmap",
        "cairn://p/prj/roadmap/pages/board",
        "cairn:~/db/attempts",
        "file:README.md",
        "cairn://p/elsewhere/roadmap/db/attempts",
        "https://example.com",
    ] {
        let refused = read_db(&orch, "p", &[target.to_string()])
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, DbBrowserError::Refused(message) if message.starts_with(target)),
            "{target}: {refused:?}"
        );
    }
    // One refused URI refuses the whole read.
    let mixed = read_db(
        &orch,
        "p",
        &[ATTEMPTS.to_string(), "cairn://p/prj/tenets/1".to_string()],
    )
    .await;
    assert!(matches!(mixed, Err(DbBrowserError::Refused(_))));
    assert!(matches!(
        read_db(&orch, "p", &[]).await,
        Err(DbBrowserError::BadRequest(_))
    ));
}

/// A browser read crosses the same final-response guard a model-visible read
/// does, so a registered secret stored in a row comes back redacted.
#[tokio::test]
async fn a_browser_read_is_scrubbed_like_a_model_visible_read() {
    use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
    const SECRET: &str = "browser-Jx7Rk2Vq9Lm4Tz";
    let (orch, _) = seeded_for_writes().await;
    write(
        &orch,
        vec![change(
            ATTEMPTS,
            "append",
            json!({"problem": "p", "code": SECRET}),
        )],
    )
    .await
    .unwrap();
    let _guard = registry()
        .register(
            SecretId::new("db-browser-read"),
            SecretCategory::CallbackCredential,
            "db browser test",
            SecretMaterial::from_string(SECRET.to_string()),
        )
        .unwrap();
    let response = read_db(&orch, "p", &[format!("{ATTEMPTS}?format=json")])
        .await
        .unwrap();
    let body = &bodies(&response)[0];
    assert!(!body.contains(SECRET), "{body}");
    assert!(body.contains("\"problem\":\"p\""), "{body}");
}

#[tokio::test]
async fn a_browser_write_round_trips_rows_and_is_journaled_through_the_browser() {
    let (orch, emitter) = seeded_for_writes().await;
    let inserted = write(
        &orch,
        vec![change(
            ATTEMPTS,
            "append",
            json!({"problem": "two-sum", "result": "ok"}),
        )],
    )
    .await
    .unwrap();
    let row = format!("{ATTEMPTS}/1");
    assert_eq!(report(&inserted)["applied"][0]["data"]["uri"], json!(row));

    let patched = write(
        &orch,
        vec![change(&row, "patch", json!({"result": "failed"}))],
    )
    .await
    .unwrap();
    assert!(!failed(&report(&patched)), "{}", report(&patched));
    let rows = read(&orch, &format!("{row}?format=json")).await;
    assert!(rows.contains("\"result\":\"failed\""), "{rows}");

    // The resource validates, the browser does not: a NOT NULL column set to
    // null comes back as the resource's own refusal, and nothing changes.
    let refused = write(&orch, vec![change(&row, "patch", json!({"problem": null}))])
        .await
        .unwrap();
    let refused = report(&refused);
    assert_eq!(refused["applied"], json!([]), "{refused}");
    assert!(
        refused["failures"][0]["error"]
            .as_str()
            .is_some_and(|error| error.to_ascii_lowercase().contains("null")),
        "{refused}"
    );

    let deleted = write(&orch, vec![change(&row, "delete", Value::Null)])
        .await
        .unwrap();
    assert!(!failed(&report(&deleted)), "{}", report(&deleted));
    assert_eq!(attempt_count(&orch).await, 0);

    // Every write announced its table, as every other row write does.
    let changes: Vec<Value> = emitter
        .events_named("db-change")
        .into_iter()
        .filter(|event| event["table"] == "thread_db" && event["dbTable"] == "attempts")
        .collect();
    assert!(
        changes.len() >= 3,
        "expected a db-change per write: {changes:?}"
    );

    let principals: Vec<Value> = orch
        .db
        .local
        .query_all(
            "SELECT principal_json FROM authorization_events ORDER BY rowid",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|json| serde_json::from_str(&json).unwrap())
        .collect();
    assert_eq!(principals.len(), 3, "{principals:?}");
    for principal in &principals {
        assert_eq!(
            principal["instrument"],
            json!({"kind": "db_browser", "owner": "cairn://p/prj/roadmap"})
        );
        assert_eq!(
            principal["actor"],
            json!({"kind": "machine", "device_id": "device-1"})
        );
    }
}

#[tokio::test]
async fn a_browser_write_reaches_only_one_threads_rows_in_its_own_project() {
    let (orch, _) = seeded_for_writes().await;
    for (target, mode, payload, expected) in [
        (
            "cairn://db/projects",
            "append",
            json!({"id": "x"}),
            "read-only",
        ),
        (
            "cairn://db/projects/p",
            "patch",
            json!({"name": "x"}),
            "read-only",
        ),
        ("cairn://db/projects/p", "delete", Value::Null, "read-only"),
        (
            "cairn://p/prj/roadmap/db",
            "patch",
            json!({"sql": "DELETE FROM attempts"}),
            "writes only rows",
        ),
        (
            "cairn://p/prj/tenets/1",
            "patch",
            json!({"verdict": "approved"}),
            "writes only rows",
        ),
        (
            "cairn:~/db/attempts",
            "append",
            json!({"problem": "p"}),
            "canonical address",
        ),
        (
            "file:x",
            "create",
            json!({"content": ""}),
            "canonical address",
        ),
        (
            ATTEMPTS,
            "patch",
            json!({"result": "x"}),
            "mode `patch` is refused",
        ),
        (
            "cairn://p/elsewhere/roadmap/db/attempts",
            "append",
            json!({"problem": "p"}),
            "own project",
        ),
    ] {
        let refused = write(&orch, vec![change(target, mode, payload)])
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, DbBrowserError::Refused(message) if message.contains(expected)),
            "{target} {mode}: {refused:?}"
        );
    }

    // Two threads in one batch: refused whole, nothing written.
    let refused = write(
        &orch,
        vec![
            change(ATTEMPTS, "append", json!({"problem": "p"})),
            change(
                "cairn://p/prj/devops/db/attempts",
                "append",
                json!({"problem": "p"}),
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&refused, DbBrowserError::Refused(message) if message.contains("one thread's rows")),
        "{refused:?}"
    );
    assert_eq!(attempt_count(&orch).await, 0);
}

/// The instrument is the door's: input naming one is refused, and so is a
/// change carrying any key past target, mode and payload.
#[tokio::test]
async fn a_browser_write_cannot_name_its_own_instrument() {
    let (orch, _) = seeded_for_writes().await;
    for input in [
        json!({"changes": [change(ATTEMPTS, "append", json!({"problem": "p"}))], "instrument": "page"}),
        json!({"changes": [{"target": ATTEMPTS, "mode": "append", "payload": {"problem": "p"},
                             "instrument": {"Page": {"page": "cairn://p/prj/roadmap/pages/board"}}}]}),
        json!({"changes": [change(ATTEMPTS, "append", json!({"problem": "p"}))], "commit_msg": "x"}),
    ] {
        let refused = write_db_rows(&orch, "p", &input, &operator())
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, DbBrowserError::Refused(_)),
            "{input}: {refused:?}"
        );
    }
    assert_eq!(attempt_count(&orch).await, 0);
}
