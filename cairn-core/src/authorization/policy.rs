//! Which normalized scopes are ordinary work, which are authority boundaries,
//! and which are refused outright.
//!
//! Policy is deliberately a pure function of the scope plus ownership context.
//! It consults no grants and performs no I/O, so "is this a boundary?" and "has
//! it been approved?" stay separate questions with separate answers — which is
//! what lets the journal record a prompt and an allow as different facts about
//! the same scope.
//!
//! The governing rule is that a prompt marks **blast radius expanding**, not
//! authority being exercised. Every classification below is justified against
//! that rule; a section that merely tunes how this install behaves for its own
//! operator is not a boundary, no matter how global it is.

use cairn_common::authorization::{
    AuthorityAction, AuthorityPlace, AuthorityPolicy, AuthorityReason, AuthorityScope,
};

/// Workspace settings sections that hand every future agent capability,
/// credentials, executable reach, or an outward-facing identity.
///
/// A write here is not the operator changing their own view of the app; it is a
/// local mutation that silently changes what every subsequent agent in this
/// workspace can do. That is the failure mode the prompt exists to catch.
const CAPABILITY_BEARING_SETTINGS: &[&str] = &[
    // Provider credentials and the identities agents authenticate as.
    "accounts",
    // Which models and providers agent work is routed to — where prompts,
    // code, and credentials actually go.
    "activeBackend",
    "backends",
    "tiers",
    "openrouterRouting",
    "routeCallsViaOpenRouter",
    // Commands this workspace will execute on the host.
    "buildServices",
    // Outward-facing message delivery and the identity commits are attributed
    // to: both let a local change speak as the operator to the outside world.
    "channels",
    "externalReplies",
    "gitIdentities",
    "bugReports",
];

/// Sections that are cosmetic or account-local: they change how this install
/// presents itself or how much housekeeping it does, and grant nothing.
///
/// Listed explicitly rather than inferred, so that adding a settings key
/// without thinking about its authority lands in
/// [`AuthorityReason::UnclassifiedWorkspaceSettingsSection`] instead of
/// defaulting to direct.
const LOCAL_PREFERENCE_SETTINGS: &[&str] = &[
    "keybinds",
    "logLevel",
    "logRetentionDays",
    "maxThinkingTokens",
    "maxOpenTriageIssuesPerScope",
    "memoryReviewEnabled",
    "memoryTriageEnabled",
    "mergeType",
    "orphanCleanupDays",
    "pendingMemoryThreshold",
    "repoTargetSweepDays",
    "subscriptionFees",
    "thinkingDisplayMode",
    "threadCompactThreshold",
    "transcriptDensity",
    "transcriptTextSize",
];

/// Whether a settings section is classified at all. Exposed so the settings
/// mutation surface can prove at test time that every key it accepts has been
/// deliberately classified here.
pub fn settings_section_is_classified(section: &str) -> bool {
    CAPABILITY_BEARING_SETTINGS.contains(&section) || LOCAL_PREFERENCE_SETTINGS.contains(&section)
}

/// Classify a normalized scope.
///
/// `owned_by_actor` says whether the place is already inside the authority the
/// actor is operating under — a project's own settings written by a run in that
/// project. V1 only ever passes `true` for project-scoped places, which are all
/// direct; it is threaded through so the executor and resource adapters have
/// the seam they need without re-deriving ownership here.
pub fn classify(scope: &AuthorityScope, owned_by_actor: bool) -> AuthorityPolicy {
    match (&scope.place, scope.action) {
        // ── Workspace settings ────────────────────────────────────────────
        (AuthorityPlace::WorkspaceSettings { section, .. }, AuthorityAction::Write) => {
            if CAPABILITY_BEARING_SETTINGS.contains(&section.as_str()) {
                AuthorityPolicy::RequiresApproval(AuthorityReason::WorkspaceSettingsCapability)
            } else if LOCAL_PREFERENCE_SETTINGS.contains(&section.as_str()) {
                AuthorityPolicy::Direct
            } else {
                // Fail closed. A section this build has never heard of is more
                // likely to be a new capability than a new colour preference,
                // and the cost of asking once is far below the cost of silently
                // granting something nobody classified.
                AuthorityPolicy::RequiresApproval(
                    AuthorityReason::UnclassifiedWorkspaceSettingsSection,
                )
            }
        }

        // ── Workspace tools ──────────────────────────────────────────────
        // Installing, removing, enabling, or reconfiguring a workspace MCP
        // server wires executable or network capability, and often credentials,
        // into every future agent in the workspace.
        (AuthorityPlace::Tool { .. }, AuthorityAction::Write) if !owned_by_actor => {
            AuthorityPolicy::RequiresApproval(AuthorityReason::WorkspaceToolCapability)
        }

        // ── Everything else ─────────────────────────────────────────────
        // Ordinary work stays direct: project reads, writes, and runs; project
        // settings and project MCP configuration under the project's own
        // authority; invoking an already-configured server. Executor places are
        // deliberately unclassified in v1 — placement is not authorization, and
        // enrollment has no adapter yet, so nothing here should start prompting
        // for a routine remote command.
        _ => AuthorityPolicy::Direct,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::ToolKind;

    fn settings(section: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::WorkspaceSettings {
                workspace_id: "default".to_string(),
                section: section.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    fn tool(name: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::Tool {
                workspace_id: "default".to_string(),
                kind: ToolKind::McpServer,
                canonical_name: name.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    #[test]
    fn capability_bearing_settings_require_approval() {
        for section in ["accounts", "backends", "buildServices", "channels"] {
            assert_eq!(
                classify(&settings(section), false),
                AuthorityPolicy::RequiresApproval(AuthorityReason::WorkspaceSettingsCapability),
                "{section}"
            );
        }
    }

    #[test]
    fn local_preferences_stay_direct() {
        for section in ["keybinds", "transcriptTextSize", "mergeType", "logLevel"] {
            assert_eq!(
                classify(&settings(section), false),
                AuthorityPolicy::Direct,
                "{section}"
            );
        }
    }

    #[test]
    fn unclassified_settings_section_fails_closed() {
        assert_eq!(
            classify(&settings("someFutureCapability"), false),
            AuthorityPolicy::RequiresApproval(
                AuthorityReason::UnclassifiedWorkspaceSettingsSection
            )
        );
    }

    #[test]
    fn workspace_tool_writes_require_approval_but_project_owned_ones_do_not() {
        assert_eq!(
            classify(&tool("linear"), false),
            AuthorityPolicy::RequiresApproval(AuthorityReason::WorkspaceToolCapability)
        );
        assert_eq!(classify(&tool("linear"), true), AuthorityPolicy::Direct);
    }

    #[test]
    fn running_a_configured_tool_and_reading_settings_stay_direct() {
        // Invoking an already-configured server is exercising existing
        // authority, not expanding it — the single most important
        // false-positive to keep out.
        let run_tool = AuthorityScope::new(tool("linear").place, AuthorityAction::Run);
        assert_eq!(classify(&run_tool, false), AuthorityPolicy::Direct);
        let read_settings = AuthorityScope::new(settings("backends").place, AuthorityAction::Read);
        assert_eq!(classify(&read_settings, false), AuthorityPolicy::Direct);
    }

    #[test]
    fn executor_and_project_places_are_not_classified_in_v1() {
        // Placing a command on an enrolled executor must not invoke an
        // authority boundary; enrollment gets its own adapter later.
        let executor = AuthorityScope::new(
            AuthorityPlace::Executor {
                runner_device_id: "runner".to_string(),
                executor_id: "exec".to_string(),
                device_id: "device".to_string(),
            },
            AuthorityAction::Run,
        );
        assert_eq!(classify(&executor, false), AuthorityPolicy::Direct);
        let project = AuthorityScope::new(
            AuthorityPlace::Project {
                project_id: "proj".to_string(),
            },
            AuthorityAction::Write,
        );
        assert_eq!(classify(&project, false), AuthorityPolicy::Direct);
    }

    #[test]
    fn classification_lists_do_not_overlap() {
        for section in CAPABILITY_BEARING_SETTINGS {
            assert!(
                !LOCAL_PREFERENCE_SETTINGS.contains(section),
                "'{section}' is classified twice; a section must have exactly one policy"
            );
        }
    }
}
