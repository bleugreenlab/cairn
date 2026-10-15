//! `cairn://models/accounts` writes, end to end through dispatch: the sign-in
//! boundary and its pre-persist check, the journal a start and its end leave,
//! the instructive rejections, and that no result or read ever carries a
//! secret.

use super::issue_mutation_tests::{change_item, seed_running_node};
use super::*;
use crate::authorization::{issue_grant, normalize, resolve_actor, GrantIssue, WORKSPACE_ID};
use crate::orchestrator::models::tests::{
    seeded_orchestrator, seeded_orchestrator_with, API_KEY_SECRET, CUSTOM_KEY_SECRET,
    HEADER_SECRET, OAUTH_SECRET,
};
use cairn_common::authorization::{
    AuthorityConstraint, AuthorityConstraintSet, AuthorityLifetimeKind, AuthorityMutation,
    AuthorityProvenance, AuthorityRequest,
};
use cairn_common::identity::PrincipalRef;
use cairn_models::accounts::RoutedProvider;
use cairn_models::subscription::testing::{TempHomes, SIGNED_IN_EMAIL, SIGN_IN_VERIFIER};
use serde_json::{json, Value};
use std::sync::Arc;

const SCOPE: &str = "workspace/default/settings/accounts:write";
/// A pasted code that completes a scripted Claude sign-in, and one that the
/// scripted CLI refuses in place. Neither may ever be echoed.
const CODE_SECRET: &str = "pasted-code-secret#scripted-state";
const BAD_CODE_SECRET: &str = "pasted-code-secret-without-state";

struct Fixture {
    orch: Orchestrator,
    run_id: String,
    node: String,
}

async fn fixture() -> Fixture {
    let homes = TempHomes::new(tempfile::tempdir().unwrap().keep());
    let orch = seeded_orchestrator_with(Arc::new(homes.clone())).await;
    fixture_on(orch).await
}

async fn fixture_on(orch: Orchestrator) -> Fixture {
    let (number, _job_id, run_id) = seed_running_node(&orch).await;
    Fixture {
        orch,
        run_id,
        node: format!("cairn://p/cairn/{number}/1/builder"),
    }
}

fn as_run(run_id: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "change".to_string(),
        payload: json!({}),
        tool_use_id: None,
    }
}

async fn act(
    f: &Fixture,
    target: &str,
    mode: ChangeMode,
    payload: Value,
) -> Result<String, String> {
    act_with(f, target, mode, payload, false).await
}

async fn act_with(
    f: &Fixture,
    target: &str,
    mode: ChangeMode,
    payload: Value,
    dry_run: bool,
) -> Result<String, String> {
    dispatch_resource_change(
        &f.orch,
        &as_run(&f.run_id),
        0,
        &change_item(target, mode, Some(payload)),
        dry_run,
    )
    .await
    .map(|change| change.summary)
    .map_err(|failure| failure.error)
}

async fn read(f: &Fixture, uri: &str) -> String {
    crate::resources::read::read_cairn_resource(&f.orch, &as_run(&f.run_id), uri).await
}

/// Mint a grant for this run as an operator's approval would.
async fn grant(
    f: &Fixture,
    request: &AuthorityRequest,
    lifetime: AuthorityLifetimeKind,
    constraints: Vec<AuthorityConstraint>,
) {
    let actor = resolve_actor(&f.orch, &as_run(&f.run_id))
        .await
        .expect("the seeded run resolves");
    issue_grant(
        &f.orch.db.local,
        GrantIssue {
            request: request.clone(),
            principal: actor.principal.clone(),
            audience: actor.audience.clone(),
            lifetime,
            request_id: Some("perm-1".to_string()),
            turn_id: actor.context.turn_id.clone(),
            session_id: actor.context.session_id.clone(),
            expires_at: None,
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            constraints: AuthorityConstraintSet::new(constraints),
        },
    )
    .await
    .expect("grant");
}

async fn grant_new_sign_ins(f: &Fixture, lifetime: AuthorityLifetimeKind) {
    let request = normalize::provider_sign_in(WORKSPACE_ID, RoutedProvider::Claude, None);
    grant(
        f,
        &request,
        lifetime,
        vec![AuthorityConstraint::MutationModes {
            modes: vec![AuthorityMutation::Create],
        }],
    )
    .await;
}

fn account_id(f: &Fixture, label: &str) -> String {
    f.orch
        .list_accounts(None)
        .into_iter()
        .find(|account| account.label == label)
        .unwrap_or_else(|| panic!("no account {label}"))
        .id
}

fn pending_account(f: &Fixture) -> String {
    f.orch
        .subscriptions()
        .pending_sign_ins()
        .into_iter()
        .find_map(|pending| pending.account_id)
        .expect("a pending sign-in with a row")
}

/// `(kind, sign_in_ref, initiator, authorization_event_id, failure_kind)` for
/// every journal row, in order.
async fn journal(
    f: &Fixture,
) -> Vec<(
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    f.orch
        .db
        .local
        .query_all(
            "SELECT kind, sign_in_ref, initiator_json, authorization_event_id, failure_kind \
             FROM account_credential_events ORDER BY rowid",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                    row.opt_text(4)?,
                ))
            },
        )
        .await
        .unwrap()
}

use crate::storage::RowExt;

#[tokio::test(flavor = "multi_thread")]
async fn a_sign_in_start_without_a_grant_is_refused_naming_the_scope() {
    let f = fixture().await;
    let before = f.orch.list_accounts(None).len();
    let refused = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap_err();
    assert!(refused.contains(SCOPE), "{refused}");
    assert!(f.orch.subscriptions().pending_sign_ins().is_empty());
    assert_eq!(f.orch.list_accounts(None).len(), before, "nothing started");
    assert!(journal(&f).await.is_empty(), "no start was recorded");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_granted_claude_sign_in_is_journaled_and_finished_with_a_pasted_code() {
    let f = fixture().await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Standing).await;

    let started = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap();
    let id = pending_account(&f);
    assert!(
        started.contains(&format!("cairn://models/accounts/{id}")),
        "{started}"
    );
    assert!(
        started.contains(&TempHomes::open_url(RoutedProvider::Claude)),
        "{started}"
    );
    assert!(started.contains("submit_code"), "{started}");

    // The start names this run as initiator and cites the decision.
    let rows = journal(&f).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "sign_in_started");
    let initiator: PrincipalRef = serde_json::from_str(rows[0].2.as_deref().unwrap()).unwrap();
    assert_eq!(
        initiator,
        PrincipalRef::Agent {
            node: f.node.clone(),
            run_id: Some(f.run_id.clone())
        }
    );
    let decision = rows[0].3.clone().expect("cites the decision row");
    let outcome = f
        .orch
        .db
        .local
        .query_opt_text(
            "SELECT outcome FROM authorization_events WHERE id = ?1",
            (decision,),
        )
        .await
        .unwrap();
    assert_eq!(outcome.as_deref(), Some("allowed_by_grant"));

    let page = read(&f, &format!("cairn://models/accounts/{id}")).await;
    assert!(page.contains("A sign-in is in progress"), "{page}");

    let account_uri = format!("cairn://models/accounts/{id}");
    let retry = act(
        &f,
        &account_uri,
        ChangeMode::Patch,
        json!({"action": "submit_code", "code": BAD_CODE_SECRET}),
    )
    .await
    .unwrap();
    assert!(retry.contains("still waiting"), "{retry}");
    let done = act(
        &f,
        &account_uri,
        ChangeMode::Patch,
        json!({"action": "submit_code", "code": CODE_SECRET}),
    )
    .await
    .unwrap();
    assert!(done.contains("Signed in"), "{done}");
    assert!(f.orch.subscriptions().pending_sign_ins().is_empty());

    let page = read(&f, &account_uri).await;
    assert!(page.contains(&format!("started by {}", f.node)), "{page}");
    assert!(
        page.contains(&format!("completed as {SIGNED_IN_EMAIL}")),
        "{page}"
    );
    let rows = journal(&f).await;
    assert_eq!(
        rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        ["sign_in_started", "sign_in_completed"]
    );
    assert!(rows[1].2.is_none(), "an end names no principal");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_once_grant_starts_exactly_one_sign_in() {
    let f = fixture().await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Once).await;
    act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap();
    let second = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap_err();
    assert!(second.contains(SCOPE), "{second}");
}

/// Authority is keyed to the place reached: an unconstrained grant minted for
/// a `cairn://settings` `accounts` patch covers a sign-in.
#[tokio::test(flavor = "multi_thread")]
async fn a_grant_minted_at_the_settings_door_authorizes_a_sign_in() {
    let f = fixture().await;
    let settings = normalize::workspace_settings_write(WORKSPACE_ID, "accounts").unwrap();
    grant(&f, &settings, AuthorityLifetimeKind::Standing, Vec::new()).await;
    let started = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap();
    assert!(started.contains("Started a Claude sign-in"), "{started}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_drops_a_new_row_and_keeps_a_re_authenticated_one() {
    let f = fixture().await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Standing).await;
    act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap();
    let provisional = pending_account(&f);
    let cancelled = act(
        &f,
        &format!("cairn://models/accounts/{provisional}"),
        ChangeMode::Patch,
        json!({"action": "cancel_sign_in"}),
    )
    .await
    .unwrap();
    assert!(cancelled.contains("Cancelled"), "{cancelled}");
    assert!(f
        .orch
        .list_accounts(None)
        .iter()
        .all(|account| account.id != provisional));

    let existing = account_id(&f, "Work Max");
    let reauth = normalize::provider_sign_in(WORKSPACE_ID, RoutedProvider::Claude, Some(&existing));
    grant(
        &f,
        &reauth,
        AuthorityLifetimeKind::Standing,
        vec![AuthorityConstraint::MutationModes {
            modes: vec![AuthorityMutation::Update],
        }],
    )
    .await;
    let account_uri = format!("cairn://models/accounts/{existing}");
    let started = act(
        &f,
        &account_uri,
        ChangeMode::Patch,
        json!({"action": "sign_in"}),
    )
    .await
    .unwrap();
    assert!(started.contains(&account_uri), "{started}");
    act(
        &f,
        &account_uri,
        ChangeMode::Patch,
        json!({"action": "cancel_sign_in"}),
    )
    .await
    .unwrap();
    assert!(f
        .orch
        .list_accounts(None)
        .iter()
        .any(|account| account.id == existing));

    let kinds: Vec<String> = journal(&f).await.into_iter().map(|row| row.0).collect();
    assert_eq!(
        kinds,
        [
            "sign_in_started",
            "sign_in_cancelled",
            "sign_in_started",
            "sign_in_cancelled"
        ]
    );
    let page = read(&f, &account_uri).await;
    assert!(page.contains("cancelled"), "{page}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_codex_sign_in_is_watched_on_the_provider_list() {
    let f = fixture().await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Standing).await;
    let started = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "codex"}),
    )
    .await
    .unwrap();
    assert!(
        started.contains("cairn://models/accounts?provider=codex"),
        "{started}"
    );
    assert!(!started.contains("submit_code"), "{started}");
    let list = read(&f, "cairn://models/accounts?provider=codex").await;
    assert!(list.contains("Sign-in in progress"), "{list}");
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_writes_are_refused_with_the_door_that_fits() {
    let f = fixture().await;
    let key = account_id(&f, "Anthropic key");
    let on_key = act(
        &f,
        &format!("cairn://models/accounts/{key}"),
        ChangeMode::Patch,
        json!({"action": "sign_in"}),
    )
    .await
    .unwrap_err();
    assert!(on_key.contains("cairn://settings"), "{on_key}");

    let profile = account_id(&f, "Work Max");
    let nothing_pending = act(
        &f,
        &format!("cairn://models/accounts/{profile}"),
        ChangeMode::Patch,
        json!({"action": "submit_code", "code": CODE_SECRET}),
    )
    .await
    .unwrap_err();
    assert!(
        nothing_pending.contains("No sign-in is pending"),
        "{nothing_pending}"
    );
    assert!(nothing_pending.contains("append"), "{nothing_pending}");
    assert!(!nothing_pending.contains(CODE_SECRET), "{nothing_pending}");

    let unknown = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "openrouter"}),
    )
    .await
    .unwrap_err();
    assert!(unknown.contains("claude | codex"), "{unknown}");

    let local = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Patch,
        json!({"action": "refresh_usage", "provider": "ollama"}),
    )
    .await
    .unwrap();
    assert!(
        local.contains("Local models have no provider-side usage"),
        "{local}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_build_without_subscription_support_says_so_and_journals_the_end() {
    let f = fixture_on(seeded_orchestrator().await).await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Standing).await;
    let refused = act(
        &f,
        "cairn://models/accounts",
        ChangeMode::Append,
        json!({"provider": "claude"}),
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains(cairn_models::subscription::NO_SUBSCRIPTION_SUPPORT),
        "{refused}"
    );
    let rows = journal(&f).await;
    assert_eq!(
        rows.iter()
            .map(|row| (row.0.as_str(), row.4.as_deref()))
            .collect::<Vec<_>>(),
        [("sign_in_started", None), ("sign_in_failed", Some("other"))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn only_a_sign_in_start_crosses_the_accounts_boundary() {
    use crate::authorization::prepare::{prepare_batch, Prepared};
    let f = fixture().await;
    let profile = account_id(&f, "Work Max");
    let account_uri = format!("cairn://models/accounts/{profile}");
    let crossings = |payload: Value, target: &str, mode: ChangeMode| {
        let item = change_item(target, mode, Some(payload));
        let f = &f;
        async move { prepare_batch(&f.orch, &as_run(&f.run_id), &[item]).await }
    };

    for (target, mode, payload) in [
        (
            "cairn://models/accounts",
            ChangeMode::Append,
            json!({"provider": "claude"}),
        ),
        (
            account_uri.as_str(),
            ChangeMode::Patch,
            json!({"action": "sign_in"}),
        ),
    ] {
        let prepared = crossings(payload, target, mode).await;
        assert_eq!(prepared.len(), 1, "{prepared:?}");
        match &prepared[0].1 {
            Prepared::Authority(request) => assert_eq!(request.scope.shorthand(), SCOPE),
            other => panic!("expected the boundary, got {other:?}"),
        }
    }
    for (target, mode, payload) in [
        (
            "cairn://models/accounts",
            ChangeMode::Patch,
            json!({"action": "refresh_usage", "provider": "claude"}),
        ),
        (
            account_uri.as_str(),
            ChangeMode::Patch,
            json!({"action": "refresh_usage"}),
        ),
        (
            account_uri.as_str(),
            ChangeMode::Patch,
            json!({"action": "submit_code", "code": CODE_SECRET}),
        ),
        (
            account_uri.as_str(),
            ChangeMode::Patch,
            json!({"action": "cancel_sign_in"}),
        ),
    ] {
        let prepared = crossings(payload.clone(), target, mode).await;
        assert!(prepared.is_empty(), "{payload} crossed: {prepared:?}");
    }
    let malformed = crossings(
        json!({"provider": "gemini"}),
        "cairn://models/accounts",
        ChangeMode::Append,
    )
    .await;
    assert!(
        matches!(&malformed[..], [(_, Prepared::Invalid(error))] if error.contains("claude | codex")),
        "{malformed:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deny_run_is_refused_with_the_scope_named() {
    use crate::mcp::handlers::authority::{raise_authority, AuthorityGate};
    let f = fixture().await;
    let actor = resolve_actor(&f.orch, &as_run(&f.run_id)).await.unwrap();
    let request = normalize::provider_sign_in(WORKSPACE_ID, RoutedProvider::Claude, None);
    match raise_authority(
        &f.orch,
        &actor,
        crate::models::Fence::Deny,
        &as_run(&f.run_id),
        "write",
        &request,
    )
    .await
    {
        AuthorityGate::Deny(message) => assert!(message.contains(SCOPE), "{message}"),
        _ => panic!("a Deny run is refused"),
    }
    assert!(f.orch.subscriptions().pending_sign_ins().is_empty());
}

/// Every action, dry and live, and every read after them: none carries a
/// stored credential, the verifier the flow held, a pasted code, or a
/// sign-in's reference.
#[tokio::test(flavor = "multi_thread")]
async fn no_account_action_or_read_reveals_a_secret() {
    let f = fixture().await;
    grant_new_sign_ins(&f, AuthorityLifetimeKind::Standing).await;
    let profile = account_id(&f, "Work Max");
    let reauth = normalize::provider_sign_in(WORKSPACE_ID, RoutedProvider::Claude, Some(&profile));
    grant(&f, &reauth, AuthorityLifetimeKind::Standing, Vec::new()).await;
    let profile_uri = format!("cairn://models/accounts/{profile}");

    let mut outputs = Vec::new();
    let mut record = |result: Result<String, String>| match result {
        Ok(text) | Err(text) => outputs.push(text),
    };
    for dry_run in [true, false] {
        for (target, mode, payload) in [
            (
                "cairn://models/accounts",
                ChangeMode::Append,
                json!({"provider": "claude"}),
            ),
            (
                "cairn://models/accounts",
                ChangeMode::Append,
                json!({"provider": "codex"}),
            ),
            (
                "cairn://models/accounts",
                ChangeMode::Patch,
                json!({"action": "refresh_usage", "provider": "claude"}),
            ),
            (
                profile_uri.as_str(),
                ChangeMode::Patch,
                json!({"action": "sign_in"}),
            ),
            (
                profile_uri.as_str(),
                ChangeMode::Patch,
                json!({"action": "submit_code", "code": BAD_CODE_SECRET}),
            ),
            (
                profile_uri.as_str(),
                ChangeMode::Patch,
                json!({"action": "submit_code", "code": CODE_SECRET}),
            ),
            (
                profile_uri.as_str(),
                ChangeMode::Patch,
                json!({"action": "refresh_usage"}),
            ),
            (
                profile_uri.as_str(),
                ChangeMode::Patch,
                json!({"action": "cancel_sign_in"}),
            ),
        ] {
            record(act_with(&f, target, mode, payload, dry_run).await);
        }
    }
    let mut uris = vec![
        "cairn://models/accounts".to_string(),
        "cairn://models/accounts?provider=codex".to_string(),
        "cairn://models/usage".to_string(),
    ];
    uris.extend(
        f.orch
            .list_accounts(None)
            .into_iter()
            .map(|account| format!("cairn://models/accounts/{}", account.id)),
    );
    for uri in uris {
        outputs.push(read(&f, &uri).await);
    }

    let refs: Vec<String> = journal(&f).await.into_iter().map(|row| row.1).collect();
    assert!(!refs.is_empty(), "sign-ins ran");
    let mut secrets = vec![
        API_KEY_SECRET.to_string(),
        OAUTH_SECRET.to_string(),
        HEADER_SECRET.to_string(),
        CUSTOM_KEY_SECRET.to_string(),
        SIGN_IN_VERIFIER.to_string(),
        CODE_SECRET.to_string(),
        BAD_CODE_SECRET.to_string(),
    ];
    secrets.extend(refs);
    for output in &outputs {
        for secret in &secrets {
            assert!(
                !output.contains(secret.as_str()),
                "reveals {secret}:\n{output}"
            );
        }
    }
}
