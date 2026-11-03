//! `cairn://grants/<id> {revoke:true}` revokes only a grant the caller holds.
//!
//! The holder rule itself is unit-tested beside it
//! (`authorization::revoke`); these drive the real write path, so the caller is
//! whoever the runner resolves the request to, exactly as an agent's write is.

use crate::common;

use std::sync::Arc;

use cairn_common::authorization::{
    AuthorityAudience, AuthorityGrant, AuthorityLifetimeKind, AuthorityPrincipal,
    AuthorityProvenance,
};
use cairn_core::internal::authorization::{self, normalize, GrantIssue, WORKSPACE_ID};
use cairn_core::internal::orchestrator::served_api::clients;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;
use serde_json::json;

/// Two live runs in one project, each in its own job and session.
async fn two_runs(db: &LocalDb) {
    let project_id = common::create_project(db, "tgr").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Grant revocation', 'active', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, '{}', 'manual')",
                params![project_id.as_str()],
            )
            .await?;
            for n in ["1", "2"] {
                conn.execute(
                    &format!(
                        "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                         VALUES ('job-{n}', 'exec-1', 'issue-1', ?1, 'node{n}', 'running', 'session-{n}', 'node{n}', 1, 1)"
                    ),
                    params![project_id.as_str()],
                )
                .await?;
                conn.execute(
                    &format!(
                        "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                         VALUES ('session-{n}', 'job-{n}', 'active', 1, 1)"
                    ),
                    (),
                )
                .await?;
                conn.execute(
                    &format!(
                        "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                         VALUES ('run-{n}', ?1, 'issue-1', 'job-{n}', 'live', 'session-{n}', 1, 1, 'resume')"
                    ),
                    params![project_id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A grant minted the way an operator's answer mints one, to `run`.
async fn grant_to(db: &LocalDb, run: &str, lifetime: AuthorityLifetimeKind) -> AuthorityGrant {
    authorization::issue_grant(
        db,
        GrantIssue {
            request: normalize::workspace_settings_write(WORKSPACE_ID, "backends").unwrap(),
            principal: AuthorityPrincipal {
                node_uri: Some(format!("cairn://p/tgr/1/1/node-of-{run}")),
                run_id: Some(run.to_string()),
                ..Default::default()
            },
            audience: AuthorityAudience::workspace(WORKSPACE_ID),
            lifetime,
            request_id: Some("perm-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            session_id: Some("session-1".to_string()),
            expires_at: None,
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            constraints: Default::default(),
        },
    )
    .await
    .unwrap()
}

async fn revoke_as(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    grant: &AuthorityGrant,
    run: Option<&str>,
) -> String {
    patch_as(orch, grant, run, json!({"revoke": true})).await
}

async fn patch_as(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    grant: &AuthorityGrant,
    run: Option<&str>,
    payload: serde_json::Value,
) -> String {
    common::change_resource_as_run(
        orch,
        json!([{
            "target": format!("cairn://grants/{}", grant.id),
            "mode": "patch",
            "payload": payload
        }]),
        run,
    )
    .await
}

async fn revoked(db: &LocalDb, grant: &AuthorityGrant) -> bool {
    authorization::get_grant(db, &grant.id)
        .await
        .unwrap()
        .unwrap()
        .revoked_at
        .is_some()
}

async fn revoked_by(db: &LocalDb, grant: &AuthorityGrant) -> Option<String> {
    db.read(|conn| {
        let id = grant.id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT revoked_by FROM authority_grants WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("grant row");
            Ok(row.get::<Option<String>>(0)?)
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_run_revokes_its_own_once_turn_and_session_grants() {
    for lifetime in [
        AuthorityLifetimeKind::Once,
        AuthorityLifetimeKind::Turn,
        AuthorityLifetimeKind::Session,
    ] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        two_runs(&db).await;
        let orch = common::orchestrator(&temp, db.clone());
        let grant = grant_to(&db, "run-1", lifetime).await;

        let result = revoke_as(&orch, &grant, Some("run-1")).await;
        assert!(
            result.contains("Revoked authority grant"),
            "{lifetime:?}: {result}"
        );
        assert!(revoked(&db, &grant).await, "{lifetime:?}");
        let by = revoked_by(&db, &grant)
            .await
            .expect("a revoker is recorded");
        assert!(by.contains("run-1"), "{lifetime:?}: recorded {by}");
    }
}

/// Who revoked a grant is the caller the runner resolved, so a payload that
/// tries to name one is refused rather than recorded.
#[tokio::test]
async fn a_payload_naming_the_revoker_is_refused() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    two_runs(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let grant = grant_to(&db, "run-1", AuthorityLifetimeKind::Session).await;

    let result = patch_as(
        &orch,
        &grant,
        Some("run-1"),
        json!({"revoke": true, "revokedBy": "the operator"}),
    )
    .await;
    assert!(result.contains("Unknown payload key"), "{result}");
    assert!(result.contains("revokedBy"), "{result}");
    assert!(!revoked(&db, &grant).await, "nothing was revoked");
}

#[tokio::test]
async fn another_runs_grant_is_refused_naming_whose_it_is() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    two_runs(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let grant = grant_to(&db, "run-1", AuthorityLifetimeKind::Session).await;

    let result = revoke_as(&orch, &grant, Some("run-2")).await;
    assert!(result.contains("Refused"), "{result}");
    assert!(
        result.contains("cairn://p/tgr/1/1/node-of-run-1 (run run-1)"),
        "the refusal names the holder: {result}"
    );
    assert!(result.contains("Settings → Grants"), "{result}");
    assert!(!revoked(&db, &grant).await, "nothing was revoked");
}

/// A standing grant with no principal is workspace-wide: the run that asked
/// for it does not hold it, and neither does a caller with no run.
#[tokio::test]
async fn an_unbound_standing_grant_is_refused_to_a_run_and_to_a_runless_caller() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    two_runs(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let grant = grant_to(&db, "run-1", AuthorityLifetimeKind::Standing).await;

    for caller in [Some("run-1"), None] {
        let result = revoke_as(&orch, &grant, caller).await;
        assert!(
            result.contains("only the operator may revoke it"),
            "{caller:?}: {result}"
        );
        assert!(result.contains("Settings → Grants"), "{caller:?}: {result}");
    }
    assert!(!revoked(&db, &grant).await, "nothing was revoked");
}

/// The operator's door revokes anything, re-reads the grant first, and records
/// the label it was handed rather than one the request carried.
#[tokio::test]
async fn the_operator_revokes_any_grant_once() {
    let (_temp, db) = common::migrated_db().await;
    two_runs(&db).await;
    let standing = grant_to(&db, "run-1", AuthorityLifetimeKind::Standing).await;
    let others = grant_to(&db, "run-2", AuthorityLifetimeKind::Session).await;

    for grant in [&standing, &others] {
        let after =
            authorization::revoke::revoke_as_operator(&db, &grant.id, "the desktop operator")
                .await
                .unwrap();
        assert!(after.revoked_at.is_some());
        assert_eq!(
            revoked_by(&db, grant).await.as_deref(),
            Some("the desktop operator")
        );
        assert_eq!(
            authorization::revoke::revoke_as_operator(&db, &grant.id, "the desktop operator")
                .await
                .unwrap_err(),
            authorization::revoke::OperatorRevokeRefusal::NotActive("revoked"),
            "a grant already revoked is reported, not revoked again"
        );
    }
    assert_eq!(
        authorization::revoke::revoke_as_operator(&db, "no-such-grant", "the desktop operator")
            .await
            .unwrap_err(),
        authorization::revoke::OperatorRevokeRefusal::NotFound
    );
}

/// A served-API client with a standing remote grant bound to it.
async fn client_with_remote_use(
    db: &LocalDb,
) -> (
    cairn_db::storage::served_api::ServedApiClient,
    AuthorityGrant,
) {
    let (client, _key) = clients::mint(db, "dig").await.unwrap();
    let grant = clients::grant_remote(
        db,
        &client,
        "openrouter",
        "acc_or",
        AuthorityProvenance {
            issuer: "served_api_client".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (client, grant)
}

async fn patch_client(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    client_id: &str,
    run: Option<&str>,
    payload: serde_json::Value,
) -> String {
    common::change_resource_as_run(
        orch,
        json!([{
            "target": format!("cairn://models/clients/{client_id}"),
            "mode": "patch",
            "payload": payload
        }]),
        run,
    )
    .await
}

/// Revoking a served-API client strips the grants it holds, so it is the
/// operator's alone, and no write to `cairn://models/clients` can prove it
/// comes from the operator: an agent run and a run-less caller are both
/// refused and sent to the operator's page, and nothing is revoked.
#[tokio::test]
async fn no_caller_revokes_a_served_api_client_through_the_resource() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    two_runs(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let (client, grant) = client_with_remote_use(&db).await;

    for caller in [Some("run-1"), None] {
        let result = patch_client(&orch, &client.id, caller, json!({"revoke": true})).await;
        assert!(result.contains("Refused"), "{caller:?}: {result}");
        assert!(
            result.contains("Settings → Served API"),
            "{caller:?}: {result}"
        );
    }
    let after = clients::find(&db, &client.id).await.unwrap().unwrap();
    assert_eq!(after.revoked_at, None, "the client is still live");
    assert!(!revoked(&db, &grant).await, "its grant is still active");
}

/// Withdrawing a client's remote use revokes grants the client holds, so it
/// follows the holder rule: neither an agent run nor a run-less caller holds
/// them, and both are told whose they are and where the operator revokes them.
#[tokio::test]
async fn withdrawing_a_clients_remote_use_follows_the_holder_rule() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    two_runs(&db).await;
    let orch = common::orchestrator(&temp, db.clone());
    let (client, grant) = client_with_remote_use(&db).await;

    for caller in [Some("run-1"), None] {
        let result = patch_client(&orch, &client.id, caller, json!({"remote": false})).await;
        assert!(
            result.contains("Withdrawing client dig's remote use"),
            "{caller:?}: {result}"
        );
        assert!(result.contains(&client.id), "names the holder: {result}");
        assert!(result.contains("Settings → Grants"), "{caller:?}: {result}");
    }
    assert!(!revoked(&db, &grant).await, "nothing was revoked");
}
