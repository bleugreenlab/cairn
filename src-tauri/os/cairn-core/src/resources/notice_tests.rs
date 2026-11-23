use super::produce_cairn_resource;
use crate::execution::gates::sweep::tests::{env, resolve_issue};
use crate::mcp::types::McpCallbackRequest;
use serde_json::json;

fn request() -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read".into(),
        payload: json!({}),
        tool_use_id: None,
        origin: None,
    }
}

#[tokio::test]
async fn live_notices_cross_resource_read_boundaries_and_expire() {
    let env = env("notice-resource-boundaries.db").await;
    env.db
        .execute("UPDATE jobs SET branch='agent-notice' WHERE id='coord'", ())
        .await
        .unwrap();
    let orch = &env.orch;
    let expiry =
        json!([{"fact":"issue_resolution","issue":"cairn://p/prj/1","resolution":"merged"}]);
    for (resource, text) in [
        ("cairn://executors/local", "executor-boundary-token"),
        ("cairn://p/prj/*/*/*/rebase", "rebase-boundary-token"),
    ] {
        crate::resource_notices::set(
            orch,
            "prj",
            resource,
            "operator",
            &json!({"text":text,"until":expiry}),
            false,
        )
        .await
        .unwrap();
    }
    crate::resource_notices::set(
        orch,
        "prj",
        "cairn://executors/local",
        "other",
        &json!({"text":"persistent-boundary-token"}),
        false,
    )
    .await
    .unwrap();
    let request = request();
    for (uri, present, absent) in [
        (
            "cairn://executors/local",
            "executor-boundary-token",
            "rebase-boundary-token",
        ),
        (
            "cairn://p/prj/9/1/coordinator/rebase",
            "rebase-boundary-token",
            "executor-boundary-token",
        ),
    ] {
        let page = produce_cairn_resource(orch, &request, uri).await;
        assert!(!page.failed, "{}", page.content);
        assert!(page.content.contains(present), "{}", page.content);
        assert!(!page.content.contains(absent), "{}", page.content);
    }
    let listing = produce_cairn_resource(orch, &request, "cairn://p/prj/notices").await;
    assert!(!listing.failed, "{}", listing.content);
    for token in [
        "executor-boundary-token",
        "rebase-boundary-token",
        "persistent-boundary-token",
    ] {
        assert!(listing.content.contains(token), "{}", listing.content);
    }
    resolve_issue(&env, 1, "merged").await;
    for uri in [
        "cairn://executors/local",
        "cairn://p/prj/9/1/coordinator/rebase",
        "cairn://p/prj/notices",
    ] {
        let page = produce_cairn_resource(orch, &request, uri).await;
        assert!(!page.failed, "{}", page.content);
        assert!(
            !page.content.contains("executor-boundary-token"),
            "{}",
            page.content
        );
        assert!(
            !page.content.contains("rebase-boundary-token"),
            "{}",
            page.content
        );
        if uri != "cairn://p/prj/9/1/coordinator/rebase" {
            assert!(
                page.content.contains("persistent-boundary-token"),
                "{}",
                page.content
            );
        }
    }
}

#[tokio::test]
async fn notice_mutation_attributes_operator_and_thread_and_rejects_unverified() {
    use super::mutations::dispatch_resource_change;
    use crate::execution::gates::capacity::tests::add_thread;
    use crate::execution::gates::reorder::tests::thread_run;
    use crate::mcp::types::{ChangeItem, ChangeMode};
    use cairn_common::protocol::DispatchOrigin;

    let env = env("notice-mutation-boundary.db").await;
    add_thread(&env, "notice-thread", "maintenance").await;
    thread_run(&env, "run-notice-thread", Some("notice-thread"), None).await;
    let item = ChangeItem {
        target: "cairn://p/prj/notices".into(),
        mode: ChangeMode::Patch,
        payload: Some(
            json!({"resource":"cairn://p/prj/*/*/*/rebase","text":"mutation-boundary-token","until":[{"fact":"issue_resolution","issue":"cairn://p/prj/1","resolution":"merged"}]}),
        ),
    };
    let mut req = request();
    req.tool = "write".into();
    assert!(dispatch_resource_change(&env.orch, &req, 0, &item, false)
        .await
        .is_err());
    req = super::channels::operator_request("write");
    dispatch_resource_change(&env.orch, &req, 0, &item, false)
        .await
        .unwrap();
    req.origin = None;
    req.run_id = Some("run-notice-thread".into());
    req.origin = Some(DispatchOrigin::UnverifiedLocal);
    assert!(dispatch_resource_change(&env.orch, &req, 0, &item, false)
        .await
        .is_err());
    req.origin = None;
    dispatch_resource_change(&env.orch, &req, 0, &item, false)
        .await
        .unwrap();
    let authors = env
        .orch
        .db
        .local
        .query_all(
            "SELECT author FROM resource_notices ORDER BY author",
            (),
            |row| {
                use crate::storage::RowExt;
                row.text(0)
            },
        )
        .await
        .unwrap();
    assert_eq!(authors, vec!["cairn://p/prj/maintenance", "operator"]);
    let clear = ChangeItem {
        target: item.target.clone(),
        mode: ChangeMode::Patch,
        payload: Some(json!({"resource":"cairn://p/prj/*/*/*/rebase","text":null})),
    };
    dispatch_resource_change(&env.orch, &req, 0, &clear, false)
        .await
        .unwrap();
    let remaining = env
        .orch
        .db
        .local
        .query_opt_text("SELECT author FROM resource_notices", ())
        .await
        .unwrap();
    assert_eq!(remaining.as_deref(), Some("operator"));
    let mut forged = item.clone();
    forged.payload.as_mut().unwrap()["author"] = json!("operator");
    assert!(dispatch_resource_change(&env.orch, &req, 0, &forged, false)
        .await
        .is_err());

    crate::fleet::management::mutate_fleet(&env.orch, |fleet| {
        fleet.remote_executors.insert(
            "remote-stable".into(),
            crate::fleet::RemoteExecutorConfig {
                host: "notice-host.invalid".into(),
                ssh_user: "test".into(),
                platform: crate::fleet::RemotePlatform::LinuxX86_64,
                binary_path: "/tmp/cairn-exec".into(),
                cairn_home: "/tmp/cairn-exec-home".into(),
                executor_id: "remote-stable".into(),
                device_id: "notice-device".into(),
                display_name: "notice-public".into(),
                project_ids: Vec::new(),
                bootstrapped_at_unix_ms: None,
                legacy_retired_at_unix_ms: None,
                legacy_tunnel_port: None,
                extra_ssh_args: Vec::new(),
            },
        );
        Ok(())
    })
    .unwrap();
    req = super::channels::operator_request("write");
    req.run_id = None;
    for alias in ["notice-public", "remote-stable", "notice-host.invalid"] {
        let shortcut = ChangeItem {
            target: format!("cairn://executors/{alias}"),
            mode: ChangeMode::Patch,
            payload: Some(json!({"notice":{"text":"alias-boundary-token"}})),
        };
        dispatch_resource_change(&env.orch, &req, 0, &shortcut, false)
            .await
            .unwrap();
        let page = produce_cairn_resource(&env.orch, &req, "cairn://executors/notice-public").await;
        assert!(!page.failed, "{}", page.content);
        assert!(
            page.content.contains("alias-boundary-token"),
            "{}",
            page.content
        );
        let clear = ChangeItem {
            target: "cairn://executors/notice-public".into(),
            mode: ChangeMode::Patch,
            payload: Some(json!({"notice":null})),
        };
        dispatch_resource_change(&env.orch, &req, 0, &clear, false)
            .await
            .unwrap();
        let page = produce_cairn_resource(&env.orch, &req, "cairn://executors/notice-public").await;
        assert!(
            !page.content.contains("alias-boundary-token"),
            "{}",
            page.content
        );
    }
}

#[tokio::test]
async fn placement_refusal_notice_is_live_and_preserves_typed_reason() {
    use cairn_common::executor_protocol::{
        CellOutcome, CellUnavailableReason, PlacementRefusal, PlacementRejectionReason,
    };
    let env = env("notice-refusal-boundary.db").await;
    crate::resource_notices::set(&env.orch, "prj", "cairn://executors/local", "operator",
        &json!({"text":"refusal-boundary-token","until":[{"fact":"issue_resolution","issue":"cairn://p/prj/1","resolution":"merged"}]}),
        false).await.unwrap();
    let reason = CellUnavailableReason::NoMatchingExecutor {
        rejected: vec![PlacementRefusal {
            executor_name: "local".into(),
            executor_id: "local-id".into(),
            reason: PlacementRejectionReason::Drained,
            drain_blockers: Vec::new(),
        }],
    };
    for expired in [false, true] {
        if expired {
            resolve_issue(&env, 1, "merged").await;
        }
        let mut outcome = CellOutcome::Unavailable {
            reason: reason.clone(),
            diagnostic: "placement-diagnostic-token".into(),
        };
        crate::resource_notices::annotate_refusal(&env.orch, &mut outcome).await;
        let CellOutcome::Unavailable {
            reason: actual,
            diagnostic,
        } = outcome
        else {
            panic!("annotation must preserve unavailable outcome");
        };
        assert_eq!(actual, reason);
        assert!(diagnostic.contains("placement-diagnostic-token"));
        assert_eq!(
            diagnostic.contains("refusal-boundary-token"),
            !expired,
            "{diagnostic}"
        );
    }
}
