//! CAIRN-3822: the typed secret crossings, exercised end to end.
//!
//! Each test registers its own unique credential with the process registry and
//! releases it when the guard drops, so tests sharing this binary cannot see
//! each other's values.

use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::RunHandle;
use cairn_core::internal::agent_process::stream::TranscriptEvent;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::security::{
    registry, Crossing, ObservedSafe, SecretCategory, SecretGuard, SecretId, SecretMaterial,
};

fn register(id: &str, value: &str) -> SecretGuard<'static> {
    registry()
        .register(
            SecretId::new(id),
            SecretCategory::CallbackCredential,
            "crossing test",
            SecretMaterial::from_string(value.to_string()),
        )
        .expect("test credential is registerable")
}

fn register_run(orch: &Orchestrator, run_id: &str) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let child = Arc::new(Mutex::new(None));
    let stdin = Arc::new(Mutex::new(None));
    let handle = RunHandle::new(child, stdin, Some(format!("sess-{run_id}")), None);
    processes.register(run_id.to_string(), handle);
}

async fn fixture() -> (
    tempfile::TempDir,
    Orchestrator,
    Mutex<HashMap<String, usize>>,
) {
    let (temp, orch) = common::test_orchestrator().await;
    register_run(&orch, "run-1");
    orch.process_state.begin_turn("run-1", "turn-1");
    (temp, orch, Mutex::new(HashMap::new()))
}

fn write_request(target: &str, content: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": target,
                "mode": "create",
                "payload": { "content": content },
            }],
        }),
        tool_use_id: None,
    }
}

fn read_request(path: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "read".to_string(),
        payload: serde_json::json!({ "path": path }),
        tool_use_id: None,
    }
}

// ── Inbound invocation crossing ───────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn a_model_write_carrying_a_registered_secret_is_refused_before_any_side_effect() {
    const SECRET: &str = "inbound-JqR7t2Vm9Xa4Zc";
    let _guard = register("crossing-inbound", SECRET);
    let (_temp, orch, cursors) = fixture().await;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("leak.txt");
    let request = write_request(
        &format!("file:{}", target.display()),
        &format!("export TOKEN={SECRET}\n"),
    );

    let output = dispatch_tool(&orch, &request, &cursors).await.into_inner();

    assert!(
        output.content.contains("was refused"),
        "expected a refusal, got: {}",
        output.content
    );
    // The write handler always answers with a change report, successful or not.
    // Its absence is the evidence that no handler ran at all — which is what
    // "before any side effect" means, and is stronger than checking one file.
    assert!(
        !output.content.contains("\"applied\""),
        "the handler must never run, so no change report may exist: {}",
        output.content
    );
    assert!(
        !target.exists(),
        "the write must be rejected before the file is touched"
    );
    // The refusal must not become an oracle for probing the registry.
    assert!(
        !output.content.contains(SECRET),
        "the refusal must not echo the value"
    );
    assert!(
        !output.content.contains("crossing-inbound"),
        "the refusal must not name which secret matched"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_encoded_registered_secret_is_refused_too() {
    use base64::Engine;

    const SECRET: &str = "encoded-Wp3Kd8Nz5Yb1Qe";
    let _guard = register("crossing-encoded", SECRET);
    let (_temp, orch, cursors) = fixture().await;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("encoded.txt");
    let encoded = base64::engine::general_purpose::STANDARD.encode(SECRET);
    let request = write_request(&format!("file:{}", target.display()), &encoded);

    let output = dispatch_tool(&orch, &request, &cursors).await.into_inner();

    assert!(
        output.content.contains("was refused"),
        "expected a refusal, got: {}",
        output.content
    );
    assert!(!target.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn a_secret_nested_deep_in_tool_input_is_still_found() {
    const SECRET: &str = "nested-Hs6Lp4Rv8Tw2Kd";
    let _guard = register("crossing-nested", SECRET);
    let (_temp, orch, cursors) = fixture().await;

    let request = McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": "cairn:~/todos",
                "mode": "append",
                "payload": { "todos": [{ "content": format!("use {SECRET}"), "status": "pending" }] },
            }],
        }),
        tool_use_id: None,
    };

    let output = dispatch_tool(&orch, &request, &cursors).await.into_inner();
    assert!(
        output.content.contains("was refused"),
        "a secret nested in an array of objects must be found: {}",
        output.content
    );
}

/// The control for the rejection tests above.
///
/// Without this, `!target.exists()` would pass for the boring reason that this
/// fixture's writes never reach the filesystem at all, and the rejection tests
/// would prove nothing. Here the *same* payload shape, differing only in that it
/// carries no registered value, reaches the write handler and gets its change
/// report back — so the gate, not the fixture, is what stopped the other calls.
#[tokio::test(flavor = "current_thread")]
async fn credential_shaped_content_that_is_not_registered_reaches_the_handler() {
    const SECRET: &str = "unrelated-Bn9Fj3Xq7Mc5Ht";
    let _guard = register("crossing-control", SECRET);
    let (_temp, orch, cursors) = fixture().await;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("config.yaml");
    // Every structural heuristic fires on this, and none of them may block a
    // legitimate authored write.
    let content = "api_key: ${LINEAR_KEY}\nAuthorization: Bearer abcdefghijklmnop\n";
    let request = write_request(&format!("file:{}", target.display()), content);

    let output = dispatch_tool(&orch, &request, &cursors).await.into_inner();
    assert!(
        !output.content.contains("was refused"),
        "structural shapes must never reject an authored write: {}",
        output.content
    );
    assert!(
        output.content.contains("\"applied\""),
        "the write handler must have run and produced its change report: {}",
        output.content
    );
    // The report names the target this fixture could not resolve, which is fine:
    // what matters is that the handler produced one at all. Whether authored
    // content survives the final-response guard unredacted is covered directly by
    // `security::sanitize::tests::exact_mode_leaves_ordinary_content_alone`.
    assert!(output.content.contains(&target.display().to_string()));
}

// ── Final response crossing ───────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn reading_a_file_that_contains_a_registered_secret_returns_it_redacted() {
    const SECRET: &str = "onread-Cv4Gm8Pk2Zx6Ly";
    let _guard = register("crossing-read", SECRET);
    let (_temp, orch, cursors) = fixture().await;

    // The file predates the registration and was authored outside the model, so
    // it is left alone on disk; only what the model *observes* is sanitized.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.env");
    let on_disk = format!("TOKEN={SECRET}\nSHA=f7d6a7a84a958f847a91f491cdb9908192b9a338\n");
    std::fs::write(&path, &on_disk).unwrap();

    let output = dispatch_tool(
        &orch,
        &read_request(&format!("file:{}", path.display())),
        &cursors,
    )
    .await
    .into_inner();

    assert!(
        !output.content.contains(SECRET),
        "the model must not observe the plaintext: {}",
        output.content
    );
    assert!(
        output.content.contains("[REDACTED]"),
        "the redaction must be visible, not silent: {}",
        output.content
    );
    assert!(
        output
            .content
            .contains("f7d6a7a84a958f847a91f491cdb9908192b9a338"),
        "an unrelated content hash must survive untouched: {}",
        output.content
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        on_disk,
        "reading must never rewrite the file on disk"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reminders_cross_the_final_response_guard_too() {
    const SECRET: &str = "reminder-Dq5Ns9Tj3Wb7Rf";
    let guard = register("crossing-reminder", SECRET);

    let output = ObservedSafe::observe(
        cairn_core::internal::dispatch::DispatchOutput {
            content: "ok".to_string(),
            reminders: vec![format!("a queued message mentioning {SECRET}")],
        },
        Crossing::FinalResponse,
    );

    assert!(!output.reminders[0].contains(SECRET));
    assert!(output.reminders[0].contains("[REDACTED]"));
    drop(guard);
}

// ── Transcript crossing ───────────────────────────────────────────────────────

#[test]
fn every_transcript_field_that_can_carry_backend_text_is_sanitized() {
    const SECRET: &str = "transcript-Ek7Uy1Io4Pa8Sd";
    let _guard = register("crossing-transcript", SECRET);

    // Built through the wire shape rather than the struct literal, so this test
    // also pins the field names the frontend and the transcript reader depend on.
    let event: TranscriptEvent = serde_json::from_value(serde_json::json!({
        "eventType": "assistant",
        "sessionId": "sess-1",
        "parentToolUseId": null,
        "content": format!("here it is: {SECRET}"),
        "thinking": format!("thinking about {SECRET}"),
        "toolName": "run",
        "toolInput": {"commands": [{"command": format!("echo {SECRET}")}]},
        "toolUses": [{"id": "toolu_1", "name": "run", "input": {"cmd": SECRET}}],
        "toolUseId": "toolu_1",
        "toolResult": format!("stdout: {SECRET}"),
        "isError": false,
        "raw": {"message": {"residue": SECRET}},
    }))
    .expect("transcript event deserializes from its wire shape");

    let json = event.observed().to_event_json();
    assert!(
        !json.contains(SECRET),
        "no transcript field may carry the plaintext: {json}"
    );
    assert!(json.contains("[REDACTED]"));
    // Identity fields must survive: redacting one would break reconstruction.
    assert!(json.contains("sess-1"));
    assert!(json.contains("toolu_1"));
}
