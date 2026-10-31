//! Acceptance for the resident-process relay (cairn/5562): a terminal placed on
//! a machine other than the runner's reaches the runner through its executor's
//! relay, as the process it is, and loses that reach when it ends.
//!
//! The machines are the node scratch suite's: real, non-colocated
//! `ExecutorRuntime`s whose resident relay is authorized by the runner's own
//! `authorize_resident_relay`. Like that suite these cannot run nested inside a
//! Cairn worktree fence, and each records a declared skip there
//! (skip-manifest.toml).

use crate::common;
use crate::node_scratch_acceptance::{eventually, node, MACHINE_B};

use std::process::Command;
use std::time::Duration;

/// One `curl` POST of `body` to `url` with `secret` as its bearer: the status
/// code and the body.
fn post(url: &str, secret: &str, body: &std::path::Path) -> (String, String) {
    let output = Command::new("curl")
        .args(["-s", "-w", "\n%{http_code}", "-H"])
        .arg(format!("Authorization: Bearer {secret}"))
        .args(["-H", "content-type: application/json", "--data"])
        .arg(format!("@{}", body.display()))
        .arg(url)
        .output()
        .expect("run curl");
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", text.as_str()));
    (code.to_owned(), body.to_owned())
}

/// Acceptance 1 and 5: a terminal on a remote machine reads its own home
/// through the relay its executor serves, never through the runner's loopback
/// or its machine secret, and once it has exited the URL and credential it
/// held reach nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_remote_terminal_reads_its_home_through_its_executors_relay() {
    if common::skip_if_fenced("a_remote_terminal_reads_its_home_through_its_executors_relay") {
        return;
    }
    let node = node().await;
    // The builder's home lands on a, where the terminal then lives.
    node.drain(MACHINE_B, true).await;
    node.shell("r-1", None, "true").await;
    let probe = node.machines.temp.path().join("relay-probe");
    std::fs::create_dir_all(&probe).unwrap();
    let body = probe.join("body.json");
    std::fs::write(
        &body,
        r#"{"cwd":"","tool":"read","payload":{"paths":["cairn:~"]}}"#,
    )
    .unwrap();
    let (env, read, code) = (probe.join("env"), probe.join("read"), probe.join("code"));
    node.start_terminal(
        "probe",
        &format!(
            "printf \"%s\\n%s\\n\" \"$CAIRN_CALLBACK_URL\" \"$CAIRN_MCP_SECRET\" > {env}; \
             curl -s -o {read} -w \"%{{http_code}}\" -H \"Authorization: Bearer $CAIRN_MCP_SECRET\" \
             -H \"content-type: application/json\" --data @{body} \"$CAIRN_CALLBACK_URL\" > {code}.part; \
             mv {code}.part {code}",
            env = env.display(),
            read = read.display(),
            body = body.display(),
            code = code.display(),
        ),
    )
    .await;
    eventually("the terminal's relayed read", || code.is_file()).await;

    let env = std::fs::read_to_string(&env).unwrap();
    let mut lines = env.lines();
    let callback_url = lines.next().unwrap().to_owned();
    let secret = lines.next().unwrap().to_owned();
    assert!(
        callback_url.starts_with("http://127.0.0.1:") && callback_url.contains("/resident/"),
        "a remote terminal calls back through its executor's relay: {callback_url}"
    );
    assert!(
        !secret.is_empty(),
        "a remote terminal holds a credential of its own"
    );
    assert_ne!(
        secret,
        node.machines.orch.mcp_auth.get_secret_for_mcp().unwrap(),
        "a remote terminal never holds the runner's machine secret"
    );
    assert_eq!(
        std::fs::read_to_string(&code).unwrap(),
        "200",
        "the relayed read was answered: {}",
        std::fs::read_to_string(&read).unwrap_or_default()
    );
    let answer = std::fs::read_to_string(&read).unwrap();
    assert!(
        answer.contains("builder"),
        "the read resolved the terminal's own home: {answer}"
    );

    // The script has ended, so the terminal exits; its relay goes with it.
    let mut last = (String::new(), String::new());
    for _ in 0..200 {
        last = post(&callback_url, &secret, &body);
        if last.0 == "410" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        last.0, "410",
        "an exited terminal's relay reaches nothing: {}",
        last.1
    );
}
