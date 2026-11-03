//! Turning a resolved mutation target into a canonical [`AuthorityScope`].
//!
//! Normalization always runs on the target the mutation actually resolved to —
//! the settings key being written, the registry key of the MCP server being
//! edited — never on display text, a summary, or anything an agent authored.
//! That is what makes the scope name mean the same thing to the prompt, to the
//! grant, and to the re-check immediately before persistence.
//!
//! Each request that can raise a card also names its act ([`ActDigest`]), the
//! resolved content a `Once` grant binds to. The digest is taken over the
//! inputs both check points derive the request from, the change item and the
//! target it resolved to, and never over live state that could move between
//! them, so the gate and the pre-persist re-check name the same act.

use cairn_common::authorization::{
    ActDigest, AuthorityAction, AuthorityMutation, AuthorityPlace, AuthorityRequest,
    AuthorityScope, McpConfigFingerprint, ToolKind,
};
use serde_json::Value;

use crate::mcp::types::ChangeItem;

/// The one local workspace. Multi-workspace resolution, when it exists, belongs
/// here rather than at each call site.
pub use crate::labels::crud::DEFAULT_WORKSPACE_ID as WORKSPACE_ID;

/// A target that cannot be named as a place at all.
///
/// This is distinct from "not allowed": there is no scope to approve, so no
/// grant could ever apply. The caller turns it into
/// [`cairn_common::authorization::AuthorityPolicy::Forbidden`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnnameableTarget(pub String);

/// Canonicalize a settings section key.
fn canonical_section(section: &str) -> Result<String, UnnameableTarget> {
    let trimmed = section.trim();
    if trimmed.is_empty() {
        return Err(UnnameableTarget(
            "a settings section with no name cannot be authorized".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

/// Canonicalize an MCP server name to its registry key.
///
/// The registry is keyed by this exact string, so the canonical form is the
/// trimmed name and nothing more: lowercasing or slugifying it here would name
/// a place that does not correspond to any real entry.
fn canonical_server(server: &str) -> Result<String, UnnameableTarget> {
    let trimmed = server.trim();
    if trimmed.is_empty() {
        return Err(UnnameableTarget(
            "an MCP server with no name cannot be authorized".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

/// `cairn://settings` patch on one section → `WorkspaceSettings(…) + Write`.
pub fn workspace_settings_write(
    workspace_id: &str,
    section: &str,
) -> Result<AuthorityRequest, UnnameableTarget> {
    let section = canonical_section(section)?;
    let scope = AuthorityScope::new(
        AuthorityPlace::WorkspaceSettings {
            workspace_id: workspace_id.to_string(),
            section: section.clone(),
        },
        AuthorityAction::Write,
    );
    let summary = format!("change workspace settings section '{section}'");
    // A settings patch always edits an existing document section in place.
    Ok(AuthorityRequest::new(
        scope,
        AuthorityMutation::Update,
        summary,
    ))
}

/// One section of a `cairn://settings` patch, naming the act: the value the
/// patch writes to that section. Each section is its own request, so a Once
/// approval of one section's value covers neither another value for it nor
/// another section.
///
/// The value is the section's payload as written, an edit list for the
/// list-shaped sections, rather than the section as it would stand afterwards:
/// the resultant state reads the live store, which can move between the gate
/// and the re-check, while the payload is the same at both.
pub fn workspace_settings_section_write(
    workspace_id: &str,
    section: &str,
    value: &Value,
) -> Result<AuthorityRequest, UnnameableTarget> {
    let request = workspace_settings_write(workspace_id, section)?;
    let act = ActDigest::of("settings.section", |encoder| {
        encoder.field("workspace", workspace_id);
        encoder.field("section", section.trim());
        encoder.json("value", value);
    });
    Ok(request.with_act(act))
}

/// The act a change item performs, for a carrier whose authority is the item
/// as a whole: its target, its mode, and its payload. `kind` separates the
/// carriers that use it.
pub fn change_item_act(kind: &str, item: &ChangeItem) -> ActDigest {
    ActDigest::of(kind, |encoder| {
        encoder.field("target", item.target.trim());
        encoder.json(
            "mode",
            &serde_json::to_value(item.mode).unwrap_or(Value::Null),
        );
        encoder.json("payload", item.payload.as_ref().unwrap_or(&Value::Null));
    })
}

/// Writing Cairn's inference records (`cairn://inference` replace, or a patch
/// that upserts or removes) → the same `WorkspaceSettings(inference) + Write`
/// place a `cairn://settings` `inference` patch reaches.
///
/// Both say what this workspace's inference machines run, and a profile's
/// start and stop commands are shell its head machine runs as its account.
/// Authority is keyed to the place reached rather than the carrier, so a grant
/// for either door covers the other.
///
/// The act is the change item that writes the records.
pub fn inference_records_write(workspace_id: &str, item: &ChangeItem) -> AuthorityRequest {
    let scope = AuthorityScope::new(
        AuthorityPlace::WorkspaceSettings {
            workspace_id: workspace_id.to_string(),
            section: "inference".to_string(),
        },
        AuthorityAction::Write,
    );
    AuthorityRequest::new(
        scope,
        AuthorityMutation::Update,
        "write the inference records, whose start and stop commands run on their head machines"
            .to_string(),
    )
    .with_act(change_item_act("inference.records", item))
}

/// Workspace `cairn://mcp` create/patch/delete → `Tool(…, McpServer, …) + Write`.
///
/// Enabling or disabling a server is a reconfiguration of an existing entry, so
/// it normalizes to `Update` like any other patch: the operator approving
/// "reconfigure linear" is approving exactly that.
///
/// `fingerprint` is the identity of the configuration that would result, and it
/// is required rather than optional. The scope names the registry entry, which
/// is the same place whatever is configured there; without the fingerprint an
/// approval would cover any future command registered under that name. Callers
/// compute it from the validated resultant config — not from the request
/// payload — so the gate and the pre-persist re-check name the same thing.
pub fn workspace_mcp_write(
    workspace_id: &str,
    server: &str,
    mutation: AuthorityMutation,
    fingerprint: McpConfigFingerprint,
) -> Result<AuthorityRequest, UnnameableTarget> {
    let server = canonical_server(server)?;
    let scope = AuthorityScope::new(
        AuthorityPlace::Tool {
            workspace_id: workspace_id.to_string(),
            kind: ToolKind::McpServer,
            canonical_name: server.clone(),
        },
        AuthorityAction::Write,
    );
    let summary = match mutation {
        AuthorityMutation::Create => format!("install workspace MCP server '{server}'"),
        AuthorityMutation::Update => format!("reconfigure workspace MCP server '{server}'"),
        AuthorityMutation::Delete => format!("remove workspace MCP server '{server}'"),
    };
    // The resultant configuration already names the act, mutation and server
    // included; the act digest carries it so a Once grant binds the same way
    // every other kind's does.
    let act = ActDigest::of("mcp.write", |encoder| {
        encoder.field("workspace", workspace_id);
        encoder.field("server", &server);
        encoder.field("mutation", mutation.as_str());
        encoder.field("config.algorithm", &fingerprint.algorithm);
        encoder.field(
            "config.encoding_version",
            &fingerprint.encoding_version.to_string(),
        );
        encoder.field("config.digest", &fingerprint.digest);
    });
    Ok(AuthorityRequest::new(scope, mutation, summary)
        .with_mcp_config(fingerprint)
        .with_act(act))
}

/// Starting a subscription sign-in through `cairn://models/accounts` → the same
/// `WorkspaceSettings(accounts) + Write` place a `cairn://settings` `accounts`
/// patch reaches.
///
/// A sign-in writes the credential store that section writes, and authority is
/// keyed to the place reached rather than the carrier, so one store has one
/// scope: a grant for either door covers the other, subject to its mutation
/// constraint. A new account is a `Create`; re-authenticating one is an
/// `Update`.
pub fn provider_sign_in(
    workspace_id: &str,
    provider: cairn_models::accounts::RoutedProvider,
    account_id: Option<&str>,
) -> AuthorityRequest {
    let scope = AuthorityScope::new(
        AuthorityPlace::WorkspaceSettings {
            workspace_id: workspace_id.to_string(),
            section: "accounts".to_string(),
        },
        AuthorityAction::Write,
    );
    let provider_name = provider_name(provider);
    let act = ActDigest::of("accounts.sign_in", |encoder| {
        encoder.field("workspace", workspace_id);
        encoder.field("provider", provider.backend());
        match account_id {
            None => encoder.field("account", "new"),
            Some(account_id) => {
                encoder.field("account", "existing");
                encoder.field("account.id", account_id);
            }
        }
    });
    match account_id {
        None => AuthorityRequest::new(
            scope,
            AuthorityMutation::Create,
            format!("sign in to a {provider_name} subscription as a new workspace account"),
        ),
        Some(account_id) => AuthorityRequest::new(
            scope,
            AuthorityMutation::Update,
            format!("re-authenticate workspace account {account_id} ({provider_name})"),
        ),
    }
    .with_act(act)
}

fn provider_name(provider: cairn_models::accounts::RoutedProvider) -> &'static str {
    match provider {
        cairn_models::accounts::RoutedProvider::Claude => "Claude",
        cairn_models::accounts::RoutedProvider::Codex => "Codex (ChatGPT)",
    }
}

/// Spending one of a subscription account's usage-reset credits through
/// `cairn://models/accounts` → `ExternalAccount(provider, account) + Write`.
///
/// The place is the account at its provider, not the `accounts` settings
/// section: a reset writes nothing in Cairn's credential store, and keying it
/// there would let a grant to sign an account in also spend its resets. Each
/// account is its own place, so approving a reset on one never covers another.
/// `account_id` is the workspace account id the write resolved to.
pub fn usage_reset_spend(
    provider: cairn_models::accounts::RoutedProvider,
    account_id: &str,
) -> AuthorityRequest {
    let scope = AuthorityScope::new(
        AuthorityPlace::ExternalAccount {
            provider: provider.backend().to_string(),
            account_id: account_id.to_string(),
        },
        AuthorityAction::Write,
    );
    AuthorityRequest::new(
        scope,
        AuthorityMutation::Update,
        format!(
            "spend one of {} account {account_id}'s usage-reset credits; a spent credit cannot be refunded",
            provider_name(provider)
        ),
    )
    .with_act(ActDigest::of("accounts.use_reset", |encoder| {
        encoder.field("provider", provider.backend());
        encoder.field("account", account_id);
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in configuration identity for the normalization tests, which are
    /// about naming a place rather than about what a digest covers.
    fn fingerprint(digest: &str) -> McpConfigFingerprint {
        McpConfigFingerprint {
            algorithm: "sha256".to_string(),
            encoding_version: 1,
            digest: digest.to_string(),
        }
    }

    fn mcp(
        server: &str,
        mutation: AuthorityMutation,
    ) -> Result<AuthorityRequest, UnnameableTarget> {
        workspace_mcp_write("default", server, mutation, fingerprint("digest"))
    }

    #[test]
    fn normalization_is_deterministic_for_the_same_target() {
        let a = mcp("linear", AuthorityMutation::Update).unwrap();
        let b = mcp("  linear  ", AuthorityMutation::Delete).unwrap();
        // Same place and action regardless of surrounding whitespace or which
        // mutation is being performed — the mutation narrows a grant, it does
        // not change what is being written to.
        assert_eq!(a.scope, b.scope);
        assert_eq!(
            a.scope.shorthand(),
            "workspace/default/tool/mcp/linear:write"
        );

        let s1 = workspace_settings_write("default", "backends").unwrap();
        let s2 = workspace_settings_write("default", " backends ").unwrap();
        assert_eq!(s1.scope, s2.scope);
    }

    #[test]
    fn different_targets_never_share_a_scope() {
        let linear = mcp("linear", AuthorityMutation::Update).unwrap();
        let github = mcp("github", AuthorityMutation::Update).unwrap();
        assert_ne!(linear.scope, github.scope);

        let backends = workspace_settings_write("default", "backends").unwrap();
        let accounts = workspace_settings_write("default", "accounts").unwrap();
        assert_ne!(backends.scope, accounts.scope);
    }

    #[test]
    fn a_nameless_target_cannot_be_normalized() {
        assert!(mcp("   ", AuthorityMutation::Create).is_err());
        assert!(workspace_settings_write("default", "").is_err());
    }

    #[test]
    fn summaries_describe_the_concrete_mutation() {
        let created = mcp("linear", AuthorityMutation::Create).unwrap();
        assert!(created.summary.contains("install"), "{}", created.summary);
        let removed = mcp("linear", AuthorityMutation::Delete).unwrap();
        assert!(removed.summary.contains("remove"), "{}", removed.summary);
    }

    /// Two doors onto one credential store name one place.
    #[test]
    fn a_sign_in_reaches_the_settings_accounts_place() {
        use cairn_models::accounts::RoutedProvider;
        let settings = workspace_settings_write("default", "accounts").unwrap();
        let new = provider_sign_in("default", RoutedProvider::Claude, None);
        let again = provider_sign_in("default", RoutedProvider::Codex, Some("acc_1"));
        assert_eq!(new.scope, settings.scope);
        assert_eq!(again.scope, settings.scope);
        assert_eq!(
            new.scope.shorthand(),
            "workspace/default/settings/accounts:write"
        );
        assert_eq!(new.mutation, AuthorityMutation::Create);
        assert_eq!(again.mutation, AuthorityMutation::Update);
        assert!(
            new.summary.contains("new workspace account"),
            "{}",
            new.summary
        );
        assert!(again.summary.contains("acc_1"), "{}", again.summary);
    }

    /// Every request that can raise a card names its act, and two different
    /// acts at one place never share one.
    #[test]
    fn every_carded_request_names_its_act() {
        use cairn_models::accounts::RoutedProvider;
        let section = |value: Value| {
            workspace_settings_section_write("default", "models", &value)
                .unwrap()
                .facts
                .act
                .expect("a settings section names its act")
        };
        assert_eq!(
            section(serde_json::json!({"a": 1, "b": 2})),
            section(serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap())
        );
        assert_ne!(
            section(serde_json::json!({"a": 1})),
            section(serde_json::json!({"a": 2}))
        );

        let item = |payload: Value| ChangeItem {
            target: "cairn://inference".to_string(),
            mode: crate::mcp::types::ChangeMode::Patch,
            payload: Some(payload),
        };
        let records = |payload: Value| inference_records_write("default", &item(payload)).facts.act;
        assert!(records(serde_json::json!({"upsert": "a"})).is_some());
        assert_ne!(
            records(serde_json::json!({"upsert": "a"})),
            records(serde_json::json!({"upsert": "b"}))
        );

        let mcp_act = |digest: &str| {
            workspace_mcp_write(
                "default",
                "linear",
                AuthorityMutation::Update,
                fingerprint(digest),
            )
            .unwrap()
            .facts
            .act
        };
        assert!(mcp_act("a").is_some());
        assert_ne!(mcp_act("a"), mcp_act("b"));

        let new = provider_sign_in("default", RoutedProvider::Claude, None)
            .facts
            .act;
        let codex = provider_sign_in("default", RoutedProvider::Codex, None)
            .facts
            .act;
        let again = provider_sign_in("default", RoutedProvider::Claude, Some("acc_1"))
            .facts
            .act;
        assert!(new.is_some());
        assert_ne!(new, codex);
        assert_ne!(new, again);

        let first = usage_reset_spend(RoutedProvider::Codex, "acc_1").facts.act;
        let second = usage_reset_spend(RoutedProvider::Codex, "acc_2").facts.act;
        assert!(first.is_some());
        assert_ne!(first, second);
    }

    /// A reset is spent at its account, never at the credential store a
    /// sign-in writes, and one account's place is not another's.
    #[test]
    fn a_usage_reset_reaches_its_own_account_and_says_it_is_final() {
        use cairn_models::accounts::RoutedProvider;
        let spend = usage_reset_spend(RoutedProvider::Codex, "acc_1");
        let other = usage_reset_spend(RoutedProvider::Codex, "acc_2");
        let sign_in = provider_sign_in("default", RoutedProvider::Codex, Some("acc_1"));
        assert_eq!(spend.scope.shorthand(), "account/codex/acc_1:write");
        assert_ne!(spend.scope, other.scope);
        assert_ne!(spend.scope, sign_in.scope);
        assert!(spend.summary.contains("acc_1"), "{}", spend.summary);
        assert!(
            spend.summary.contains("cannot be refunded"),
            "{}",
            spend.summary
        );
    }
}
