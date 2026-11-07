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
    // code, and credentials actually go. Installing a provider adds a
    // destination every future agent can be routed to; removing one takes a
    // destination away from work already configured to use it.
    "enabledProviders",
    "models",
    "decisionModel",
    "imageModel",
    "speechModel",
    // Which provider every transcript, issue and memory is sent to for
    // embedding, history included once it is set explicitly.
    "embeddingModel",
    "openrouterRouting",
    "routeCallsViaOpenRouter",
    // The recipe every unnamed start runs decides which agents, tools and
    // fences other agents' work gets, so an agent setting it would steer them.
    "defaultRecipe",
    // Commands this workspace will execute on the host.
    "buildServices",
    // Which verbs each inference profile's model is offered for, and so which
    // work is routed to it, as the model bindings above route work.
    "inference",
    // Opens a network listener that spends this workspace's models for
    // outside clients, and decides which names reach which model.
    "servedApi",
    // Outward-facing message delivery and the identity commits are attributed
    // to: both let a local change speak as the operator to the outside world.
    "channels",
    "externalReplies",
    "gitIdentities",
    "bugReports",
    // The execution queue's caps (tenets 1288, 1350) are rules written to
    // constrain what agents start. An agent that could raise them would
    // release its own way past them, which tenet 1319 forbids, so a write
    // here is the operator's to approve.
    "maxConcurrentExecutions",
    "maxOpenChildrenPerThread",
];

/// Sections that are cosmetic or account-local: they change how this install
/// presents itself, how much housekeeping it does, or how much work it admits
/// at once, and grant nothing.
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
    // A ceiling on how many agent sessions this host keeps resident. Raising or
    // clearing it changes how much of the operator's own machine Cairn uses;
    // every admitted session still runs with exactly the authority it had.
    "privateDedicatedProcessCapacity",
    "repoTargetSweepDays",
    "seedBudgetTokens",
    "seedEras",
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

        // ── External accounts ───────────────────────────────────────────
        // Writing to an account the operator holds with an outside provider
        // changes something there that Cairn cannot take back: a spent Codex
        // usage-reset credit is not refunded. Not blast radius, but the one
        // place a scarce, irreversible spend crosses, so the operator decides.
        // Using the account's credential is `Run` at the same place and stays
        // direct below: the broker resolves it for every launch.
        (AuthorityPlace::ExternalAccount { .. }, AuthorityAction::Write) => {
            AuthorityPolicy::RequiresApproval(AuthorityReason::ExternalAccountWrite)
        }

        // ── The execution queue ─────────────────────────────────────────────────────
        // A thread ordering its own children works in places its work already
        // holds and never reaches here. Moving held work across the fleet's
        // order changes when OTHER threads' work starts, even when the row
        // moved is the thread's own: the blast radius is everyone it passes.
        (AuthorityPlace::ExecutionQueue { .. }, AuthorityAction::Write) => {
            AuthorityPolicy::RequiresApproval(AuthorityReason::QueueReorderAcrossThreads)
        }
        // Starting held work past every cap. The caps constrain what a thread
        // starts itself (tenet 1319), so a thread skipping them is exactly
        // the expansion they exist to prevent, and only the operator's
        // approval may allow it.
        (AuthorityPlace::ExecutionQueue { .. }, AuthorityAction::Run) => {
            AuthorityPolicy::RequiresApproval(AuthorityReason::QueueStartNow)
        }

        // ── A branch's catch-up mode ─────────────────────────────────────
        // Exempting a branch from replay, and gating its landing on declared
        // review paths, is an exception to how every branch is kept current.
        // Security's condition (cairn/6080): it must not become a general way
        // out of replay, so it is never the asking agent's own to make.
        (AuthorityPlace::BranchCatchUp { .. }, AuthorityAction::Write) => {
            AuthorityPolicy::RequiresApproval(AuthorityReason::BranchCatchUpMode)
        }

        // ── Stored content ───────────────────────────────────────────
        // Removing a stored object deletes its bytes for every reader, which
        // no one can undo, and decides over whatever private matter it shows.
        // Owning the issue it was attached in confers nothing here: an agent
        // removes only through a grant the operator issued.
        (AuthorityPlace::Resource { canonical_uri }, AuthorityAction::Write)
            if names_stored_object(canonical_uri) =>
        {
            AuthorityPolicy::RequiresApproval(AuthorityReason::StoredContentRemoval)
        }

        // ── Everything else ─────────────────────────────────────────────
        // Ordinary work stays direct: project reads, writes, and runs; project
        // settings and project MCP configuration under the project's own
        // authority; invoking an already-configured server; using an external
        // account's credential. Executor places are
        // deliberately unclassified in v1 — placement is not authorization, and
        // enrollment has no adapter yet, so nothing here should start prompting
        // for a routine remote command.
        _ => AuthorityPolicy::Direct,
    }
}

/// Whether a resource place is one stored object, by either spelling.
fn names_stored_object(canonical_uri: &str) -> bool {
    matches!(
        cairn_uri::parse_uri(canonical_uri),
        Some(
            cairn_uri::CairnResource::ProjectAttachment { .. }
                | cairn_uri::CairnResource::ProjectImage { .. }
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::ToolKind;

    #[test]
    fn removing_a_stored_object_requires_approval_whoever_owns_it() {
        for uri in [
            "cairn://p/cairn/6110/attachments/1",
            "cairn://p/cairn/6110/images/1",
            "cairn://p/cairn/ui/attachments/2",
        ] {
            let scope = AuthorityScope::new(
                AuthorityPlace::Resource {
                    canonical_uri: uri.to_string(),
                },
                AuthorityAction::Write,
            );
            for owned in [false, true] {
                assert_eq!(
                    classify(&scope, owned),
                    AuthorityPolicy::RequiresApproval(AuthorityReason::StoredContentRemoval),
                    "{uri}, owned: {owned}"
                );
            }
        }
        // Reading one, and writing a resource that is not a stored object,
        // stay ordinary work.
        let read = AuthorityScope::new(
            AuthorityPlace::Resource {
                canonical_uri: "cairn://p/cairn/6110/images/1".to_string(),
            },
            AuthorityAction::Read,
        );
        assert_eq!(classify(&read, false), AuthorityPolicy::Direct);
        let other = AuthorityScope::new(
            AuthorityPlace::Resource {
                canonical_uri: "cairn://p/cairn/db/issues".to_string(),
            },
            AuthorityAction::Write,
        );
        assert_eq!(classify(&other, false), AuthorityPolicy::Direct);
    }

    #[test]
    fn a_branch_catch_up_mode_write_always_requires_approval() {
        let scope = AuthorityScope::new(
            AuthorityPlace::BranchCatchUp {
                project_id: "proj".to_string(),
                bookmark: "agent/cairn-4113-builder-0".to_string(),
            },
            AuthorityAction::Write,
        );
        for owned in [false, true] {
            assert_eq!(
                classify(&scope, owned),
                AuthorityPolicy::RequiresApproval(AuthorityReason::BranchCatchUpMode)
            );
        }
    }

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
        for section in [
            "accounts",
            "models",
            "buildServices",
            "channels",
            "maxConcurrentExecutions",
            "maxOpenChildrenPerThread",
        ] {
            assert_eq!(
                classify(&settings(section), false),
                AuthorityPolicy::RequiresApproval(AuthorityReason::WorkspaceSettingsCapability),
                "{section}"
            );
        }
    }

    #[test]
    fn local_preferences_stay_direct() {
        for section in [
            "keybinds",
            "transcriptTextSize",
            "mergeType",
            "logLevel",
            "seedBudgetTokens",
            "seedEras",
        ] {
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
    fn writing_an_external_account_requires_approval_but_using_its_credential_does_not() {
        let place = AuthorityPlace::ExternalAccount {
            provider: "codex".to_string(),
            account_id: "acc_1".to_string(),
        };
        assert_eq!(
            classify(
                &AuthorityScope::new(place.clone(), AuthorityAction::Write),
                false
            ),
            AuthorityPolicy::RequiresApproval(AuthorityReason::ExternalAccountWrite)
        );
        assert_eq!(
            classify(&AuthorityScope::new(place, AuthorityAction::Run), false),
            AuthorityPolicy::Direct
        );
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
    fn moving_or_starting_queued_work_requires_approval() {
        let queue = AuthorityPlace::ExecutionQueue {
            workspace_id: "default".to_string(),
        };
        assert_eq!(
            classify(
                &AuthorityScope::new(queue.clone(), AuthorityAction::Write),
                false
            ),
            AuthorityPolicy::RequiresApproval(AuthorityReason::QueueReorderAcrossThreads)
        );
        assert_eq!(
            classify(
                &AuthorityScope::new(queue.clone(), AuthorityAction::Run),
                false
            ),
            AuthorityPolicy::RequiresApproval(AuthorityReason::QueueStartNow)
        );
        // Ownership does not make either direct: a thread's own row still
        // passes other threads' work.
        assert_eq!(
            classify(
                &AuthorityScope::new(queue.clone(), AuthorityAction::Write),
                true
            ),
            AuthorityPolicy::RequiresApproval(AuthorityReason::QueueReorderAcrossThreads)
        );
        // Reading the queue exercises no authority.
        assert_eq!(
            classify(&AuthorityScope::new(queue, AuthorityAction::Read), false),
            AuthorityPolicy::Direct
        );
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
