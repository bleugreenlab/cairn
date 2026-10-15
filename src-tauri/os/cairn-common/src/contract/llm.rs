//! The cairn-llm facet's resources: `cairn://llm/*`.
//!
//! Reads render what Settings → Providers shows for the workspace; nothing here
//! reveals a credential value. The writes are a session's stop and the
//! accounts' sign-in and usage-refresh actions. Starting a sign-in crosses the
//! `workspace/default/settings/accounts:write` boundary, the place
//! `cairn://settings` `accounts` writes reach.
//!
//! Sessions are started and messaged through the doors that already exist for
//! them (a delegated task, a home's messages), which the sessions affordances
//! name as cross-actions rather than duplicate.

use super::specs::*;
use super::types::*;

const LLM_SESSION_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "stop — interrupts the session's active turn and parks it warm (resumable, not a kill; cascades to child runs), exactly as stopping its node does",
);

const LLM_SIGN_IN_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "claude | codex — the subscription to sign a new account in to",
);

const LLM_ACCOUNTS_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "refresh_usage — re-measure every account of one installed provider now (an account measured in the last minute answers from that measurement)",
);

const LLM_USAGE_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "KEY — the installed provider to re-measure, e.g. claude, codex, openrouter",
);

const LLM_ACCOUNT_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "sign_in (re-authenticate a Claude or Codex subscription account; needs the workspace/default/settings/accounts:write grant) | submit_code (finish its pending Claude sign-in with the pasted code#state) | cancel_sign_in (end its pending sign-in) | refresh_usage (re-measure it now)",
);

const LLM_SIGN_IN_CODE: KeySpec = KeySpec::new(
    "code",
    KeyType::Str,
    "with submit_code: the code#state string Claude's sign-in page shows on its manual path",
);

pub(crate) const LLM_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Llm,
    uri_template: "cairn://llm",
    name: "LLM facet",
    description: "The cairn-llm facet: how tokens are produced, routed, and accounted. An index of its resources: the catalog of providers, models, and tier bindings; the provider accounts and their usage windows; each provider's pooled capacity; and the live model sessions. Every read shows what Settings → Providers shows for this workspace, and none reveals a credential value.",
    read_projections: NO_PROJECTIONS,
    related: &[
        RelatedSpec { label: "providers, models, and tier bindings", kind: ResourceKind::LlmCatalog, actions: false },
        RelatedSpec { label: "accounts and their usage windows; sign in and re-measure", kind: ResourceKind::LlmAccounts, actions: false },
        RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::LlmUsage, actions: false },
        RelatedSpec { label: "live model sessions", kind: ResourceKind::LlmSessions, actions: false },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const LLM_CATALOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmCatalog,
    uri_template: "cairn://llm/catalog",
    name: "LLM catalog",
    description: "Every provider this workspace installed, in the order Settings → Providers lists them: its category, whether an account or host is configured, what model discovery last found (id, name, context window, and pricing where the provider reports it, or the discovery error), and the tiers bound to it. A tier marked default is where an unqualified tier reference such as `lg` resolves; `claude/lg` names the provider outright. The same catalog is what tier resolution and context-window lookup read, so a binding shown here is the binding a launch gets.",
    read_projections: &[
        ProjectionSpec { key: "view", values: "providers (default) | models — each model once under its catalog id (the provider's canonical slug, else provider/model), with every provider offering it and the tier references bound to it" },
        ProjectionSpec { key: "provider", values: "KEY — one provider, e.g. claude, codex, openrouter, custom:<account id>" },
        ProjectionSpec { key: "installed", values: "all — also list the supported providers this workspace has not installed (the Add-provider list)" },
    ],
    related: &[RelatedSpec { label: "accounts behind each provider", kind: ResourceKind::LlmAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const LLM_ACCOUNTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmAccounts,
    uri_template: "cairn://llm/accounts",
    name: "LLM accounts",
    description: "Every provider account visible to the workspace, grouped by provider in priority order: label, the kind of credential it holds (a Cairn-managed Claude sign-in, a ChatGPT sign-in, an API key, a host, a custom endpoint) but never the credential itself, email and plan where the provider reports them, and its usage windows (remaining percent and reset time) with when they were measured and any block still in force. A window whose reset has passed since it was measured is shown as expired history, not as remaining capacity. A sign-in still in the user's hands shows under its provider. Append {provider: claude|codex} to start a sign-in for a new subscription account: a browser opens on this machine, the result names the page to open if it did not, and the account appears here when the user finishes. Starting a sign-in needs the `workspace/default/settings/accounts:write` grant, the same place `cairn://settings` `accounts` writes reach: an Ask run is prompted for it and a Deny run is refused. Patch {action:\"refresh_usage\", provider} to re-measure one provider's accounts now. People sign in through Settings → Providers or `cairn auth login claude`; all three doors run the same sign-in.",
    read_projections: &[ProjectionSpec { key: "provider", values: "KEY — one provider's accounts, e.g. claude, codex, ollama" }],
    related: &[RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::LlmUsage, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[LLM_SIGN_IN_PROVIDER],
            optional: &[],
            label: "start a sign-in for a new subscription account (needs the workspace/default/settings/accounts:write grant)",
            example: r#"write({changes:[{target:"cairn://llm/accounts",mode:"append",payload:{provider:"claude"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[LLM_ACCOUNTS_ACTION, LLM_USAGE_PROVIDER],
            optional: &[],
            label: "re-measure one provider's accounts now",
            example: r#"write({changes:[{target:"cairn://llm/accounts",mode:"patch",payload:{action:"refresh_usage",provider:"claude"}}]})"#,
        },
    ],
};

pub(crate) const LLM_ACCOUNT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmAccount,
    uri_template: "cairn://llm/accounts/{id}",
    name: "LLM account",
    description: "One provider account: its credential kind (never the credential), the providers it serves, email, plan, scope, priority, when it was last used, and every usage window it has reported, a sign-in still pending on it, and its sign-in history (who started each sign-in and how it ended). Patch it with action sign_in to re-authenticate a Claude or Codex subscription account (needs the `workspace/default/settings/accounts:write` grant; an Ask run is prompted for it and a Deny run is refused; a browser opens on this machine), submit_code with the pasted code#state to finish a pending Claude sign-in on its manual path, cancel_sign_in to end a pending one, or refresh_usage to re-measure it now. An API key, host, or custom endpoint changes through `cairn://settings` `accounts`. People re-authenticate in Settings → Providers or with `cairn auth login claude --account <id>`; every door runs the same sign-in.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[LLM_ACCOUNT_ACTION],
        optional: &[LLM_SIGN_IN_CODE],
        label: "re-authenticate, finish or cancel a pending sign-in, or re-measure usage",
        example: r#"write({changes:[{target:"cairn://llm/accounts/ACCOUNT_ID",mode:"patch",payload:{action:"sign_in"}}]})"#,
    }],
};

pub(crate) const LLM_USAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmUsage,
    uri_template: "cairn://llm/usage",
    name: "LLM usage",
    description: "Each installed provider's capacity as its Settings → Providers card shows it: the pooled usage windows across every account (remaining out of total, next reset), each account's share, what the pool is currently exhausted on, credits or recorded cost where the provider has them, when it was measured, and the last probe error. A pure read of what was already measured; it never probes a provider.",
    read_projections: &[
        ProjectionSpec { key: "provider", values: "KEY — one installed provider" },
        ProjectionSpec { key: "view", values: "card (default) | requests — per-request usage rows (not recorded yet; says what exists instead)" },
    ],
    related: &[RelatedSpec { label: "each account's windows", kind: ResourceKind::LlmAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const LLM_SESSIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmSessions,
    uri_template: "cairn://llm/sessions",
    name: "LLM sessions",
    description: "Every live model session on this host: its session id, the node it belongs to, provider, model, the account its launch chose, the provider's own id for the conversation, the machine it runs on (`local` today), whether it is busy (starting, serving a turn, or holding one open) or warm (parked between turns, resumable without a new process), and when it started. Read cairn://llm/sessions/<id> for one session and to stop it. To start a session, append a task to `cairn:~/tasks` (choose the provider and model with `backend`/`tier`); to send a live session a message, append to its home's `/messages` (the home is shown on each session). This resource reads and stops sessions; it does not duplicate those doors.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: &[CrossActionSpec {
        kind: ResourceKind::NodeTasks,
        mode: ChangeMode::Append,
        label: "start a session: delegate a task (choose its provider and model with backend/tier)",
    }],
    mutations: NO_MUTATIONS,
};

pub(crate) const LLM_SESSION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::LlmSession,
    uri_template: "cairn://llm/sessions/{id}",
    name: "LLM session",
    description: "One live model session, addressed by its session id (or its run id before it has one). Patch with action:\"stop\" to stop it: this is the same stop as a node's, so the active turn is interrupted, the session parks warm and resumable, and child runs stop with it. To send it a message, append to its home's `/messages` (the home is shown on the session).",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: &[CrossActionSpec {
        kind: ResourceKind::NodeMessages,
        mode: ChangeMode::Append,
        label: "send this session a message: append to its home's messages (the home is shown on the session)",
    }],
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[LLM_SESSION_ACTION],
        optional: &[],
        label: "stop a session",
        example: r#"write({changes:[{target:"cairn://llm/sessions/SESSION_ID",mode:"patch",payload:{action:"stop"}}]})"#,
    }],
};
