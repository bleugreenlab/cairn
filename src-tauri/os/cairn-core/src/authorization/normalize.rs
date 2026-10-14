//! Turning a resolved mutation target into a canonical [`AuthorityScope`].
//!
//! Normalization always runs on the target the mutation actually resolved to —
//! the settings key being written, the registry key of the MCP server being
//! edited — never on display text, a summary, or anything an agent authored.
//! That is what makes the scope name mean the same thing to the prompt, to the
//! grant, and to the re-check immediately before persistence.

use cairn_common::authorization::{
    AuthorityAction, AuthorityMutation, AuthorityPlace, AuthorityRequest, AuthorityScope,
    McpConfigFingerprint, ToolKind,
};

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
    Ok(AuthorityRequest::new(scope, mutation, summary).with_mcp_config(fingerprint))
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
}
