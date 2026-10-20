//! The cairn-models facet's resources: `cairn://models/*`.
//!
//! Reads render what Settings → Providers and Analytics show for the
//! workspace; nothing here reveals a credential value. The writes are a
//! session's stop and the accounts' sign-in, usage-refresh, plan and reset
//! actions. Starting a sign-in crosses the
//! `workspace/default/settings/accounts:write` boundary, the place
//! `cairn://settings` `accounts` writes reach.
//!
//! Sessions are started and messaged through the doors that already exist for
//! them (a delegated task, a home's messages), which the sessions affordances
//! name as cross-actions rather than duplicate.

use super::specs::*;
use super::types::*;

const MODELS_SESSION_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "stop — interrupts the session's active turn and parks it warm (resumable, not a kill; cascades to child runs), exactly as stopping its node does",
);

const MODELS_SIGN_IN_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "claude | codex — the subscription to sign a new account in to",
);

const MODELS_ACCOUNTS_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "refresh_usage — re-measure every account of one installed provider now (an account measured in the last minute answers from that measurement)",
);

const MODELS_SET_PLAN_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "set_plan — name which of its candidate plans an account is on, where its provider's signal cannot tell (ChatGPT pro, a Team or Business seat, OpenCode Go)",
);

const MODELS_SET_PLAN_ACCOUNT: KeySpec = KeySpec::new(
    "account",
    KeyType::Str,
    "ID — the account whose plan to set",
);

const MODELS_SET_PLAN_PLAN: KeySpec = KeySpec::new(
    "plan",
    KeyType::Str,
    "KEY — one of the account's candidate plans as listed on it (e.g. pro_200, pro_500), or null to clear the choice",
);

const MODELS_USE_RESET_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "use_reset — spend one of an account's usage resets now, restoring its 5-hour and weekly limits; it cannot be undone, so it needs that account's own `account/codex/<id>:write` grant (an Ask run is prompted for it, a Deny run is refused). Codex only: Claude has no reset API, and its resets and usage credits are redeemed at claude.ai/settings/usage",
);

const MODELS_USE_RESET_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "codex — the provider the account belongs to",
);

const MODELS_USE_RESET_ACCOUNT: KeySpec = KeySpec::new(
    "account",
    KeyType::Str,
    "ID — the account to spend a reset from; its usage resets are listed under it",
);

const MODELS_USAGE_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "KEY — the installed provider to re-measure, e.g. claude, codex, openrouter",
);

const MODELS_ACCOUNT_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "sign_in (re-authenticate a Claude or Codex subscription account; needs the workspace/default/settings/accounts:write grant) | submit_code (finish its pending Claude sign-in with the pasted code#state) | cancel_sign_in (end its pending sign-in) | refresh_usage (re-measure it now)",
);

const MODELS_SIGN_IN_CODE: KeySpec = KeySpec::new(
    "code",
    KeyType::Str,
    "with submit_code: the code#state string Claude's sign-in page shows on its manual path",
);

pub(crate) const MODELS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Models,
    uri_template: "cairn://models",
    name: "Models facet",
    description: "The cairn-models facet: how tokens are produced, routed, and accounted. An index of its resources: the catalog of providers, models, and tier bindings; the provider accounts and their usage windows; each provider's pooled capacity; and the live model sessions. Every read shows what Settings → Providers shows for this workspace, and none reveals a credential value.",
    read_projections: NO_PROJECTIONS,
    related: &[
        RelatedSpec { label: "providers, models, and tier bindings", kind: ResourceKind::ModelsCatalog, actions: false },
        RelatedSpec { label: "accounts and their usage windows; sign in and re-measure", kind: ResourceKind::ModelsAccounts, actions: false },
        RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::ModelsUsage, actions: false },
        RelatedSpec { label: "live model sessions", kind: ResourceKind::ModelsSessions, actions: false },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_CATALOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsCatalog,
    uri_template: "cairn://models/catalog",
    name: "Model catalog",
    description: "Every provider this workspace installed, in the order Settings → Providers lists them: its category, whether an account or host is configured, what model discovery last found (id, name, context window, pricing where the provider reports it with each price in its own unit, what the model takes in and gives out, and the verbs it serves with the wire each rides; or the discovery error), the tiers bound to it, what the provider serves for any model it routes, and any model observed unable to serve a verb it declares, with why. A tier marked default is where an unqualified tier reference such as `lg` resolves; `claude/lg` names the provider outright. The same catalog is what tier resolution and context-window lookup read, so a binding shown here is the binding a launch gets.",
    read_projections: &[
        ProjectionSpec { key: "view", values: "providers (default) | models — each model once under its catalog id (the provider's canonical slug, else provider/model), with every provider offering it and the tier references bound to it" },
        ProjectionSpec { key: "provider", values: "KEY — one provider, e.g. claude, codex, openrouter, custom:<account id>" },
        ProjectionSpec { key: "installed", values: "all — also list the supported providers this workspace has not installed (the Add-provider list)" },
        ProjectionSpec { key: "serves", values: "complete | session | decide | image | embed — only the models (in either view) that serve that verb; an image offer also says what it can draw (count, references, aspect ratios or sizes, mask), and an embed offer its width, which widths it can return, how it tells documents from queries, and its batch limits" },
    ],
    related: &[RelatedSpec { label: "accounts behind each provider", kind: ResourceKind::ModelsAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_ACCOUNTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsAccounts,
    uri_template: "cairn://models/accounts",
    name: "Model accounts",
    description: "Every provider account visible to the workspace, grouped by provider in priority order: label, the kind of credential it holds (a Cairn-managed Claude sign-in, a ChatGPT sign-in, an API key, a host, a custom endpoint) but never the credential itself, email, the subscription plan and its monthly list price (or no price) where the account is a subscription, with the candidate plans when the provider cannot say which it is, and its usage windows (remaining percent and reset time) with when they were measured and any block still in force. A window whose reset has passed since it was measured is shown as expired history, not as remaining capacity. A sign-in still in the user's hands shows under its provider. Append {provider: claude|codex} to start a sign-in for a new subscription account: a browser opens on this machine, the result names the page to open if it did not, and the account appears here when the user finishes. Starting a sign-in needs the `workspace/default/settings/accounts:write` grant, the same place `cairn://settings` `accounts` writes reach: an Ask run is prompted for it and a Deny run is refused. Patch {action:\"refresh_usage\", provider} to re-measure one provider's accounts now, {action:\"set_plan\", account, plan} to name which candidate plan an account is on, as Settings → Providers' plan toggle does, or {action:\"use_reset\", provider:\"codex\", account} to spend one of a Codex account's usage resets, as its row's Use does; a reset cannot be refunded, so spending one needs that account's `account/codex/<id>:write` grant, and an Ask run is prompted for it. Each account lists the usage resets it holds. People sign in through Settings → Providers or `cairn auth login claude`; all three doors run the same sign-in.",
    read_projections: &[ProjectionSpec { key: "provider", values: "KEY — one provider's accounts, e.g. claude, codex, ollama" }],
    related: &[RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::ModelsUsage, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[MODELS_SIGN_IN_PROVIDER],
            optional: &[],
            label: "start a sign-in for a new subscription account (needs the workspace/default/settings/accounts:write grant)",
            example: r#"write({changes:[{target:"cairn://models/accounts",mode:"append",payload:{provider:"claude"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[MODELS_ACCOUNTS_ACTION, MODELS_USAGE_PROVIDER],
            optional: &[],
            label: "re-measure one provider's accounts now",
            example: r#"write({changes:[{target:"cairn://models/accounts",mode:"patch",payload:{action:"refresh_usage",provider:"claude"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[MODELS_SET_PLAN_ACTION, MODELS_SET_PLAN_ACCOUNT, MODELS_SET_PLAN_PLAN],
            optional: &[],
            label: "name which candidate plan an account is on",
            example: r#"write({changes:[{target:"cairn://models/accounts",mode:"patch",payload:{action:"set_plan",account:"ACCOUNT_ID",plan:"pro_200"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[MODELS_USE_RESET_ACTION, MODELS_USE_RESET_PROVIDER, MODELS_USE_RESET_ACCOUNT],
            optional: &[],
            label: "spend one of a Codex account's usage resets (cannot be undone; needs the account's grant)",
            example: r#"write({changes:[{target:"cairn://models/accounts",mode:"patch",payload:{action:"use_reset",provider:"codex",account:"ACCOUNT_ID"}}]})"#,
        },
    ],
};

pub(crate) const MODELS_ACCOUNT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsAccount,
    uri_template: "cairn://models/accounts/{id}",
    name: "Model account",
    description: "One provider account: its credential kind (never the credential), the providers it serves, email, plan, scope, priority, when it was last used, and every usage window it has reported, a sign-in still pending on it, and its sign-in history (who started each sign-in and how it ended). Patch it with action sign_in to re-authenticate a Claude or Codex subscription account (needs the `workspace/default/settings/accounts:write` grant; an Ask run is prompted for it and a Deny run is refused; a browser opens on this machine), submit_code with the pasted code#state to finish a pending Claude sign-in on its manual path, cancel_sign_in to end a pending one, or refresh_usage to re-measure it now. An API key, host, or custom endpoint changes through `cairn://settings` `accounts`. People re-authenticate in Settings → Providers or with `cairn auth login claude --account <id>`; every door runs the same sign-in.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[MODELS_ACCOUNT_ACTION],
        optional: &[MODELS_SIGN_IN_CODE],
        label: "re-authenticate, finish or cancel a pending sign-in, or re-measure usage",
        example: r#"write({changes:[{target:"cairn://models/accounts/ACCOUNT_ID",mode:"patch",payload:{action:"sign_in"}}]})"#,
    }],
};

pub(crate) const MODELS_USAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsUsage,
    uri_template: "cairn://models/usage",
    name: "Model usage",
    description: "Each installed provider's capacity, as Settings → Analytics shows it: every usage window the provider measures, each account's remaining share of it, and the pooled total (remaining out of total, next reset); what the pool is currently exhausted on; each account's usage resets; credits or recorded cost where the provider has them; when it was measured; and the last probe error. A pure read of what was already measured; it never probes a provider.",
    read_projections: &[
        ProjectionSpec { key: "provider", values: "KEY — one installed provider" },
        ProjectionSpec { key: "view", values: "card (default) | requests — per-request usage rows (not recorded yet; says what exists instead)" },
    ],
    related: &[RelatedSpec { label: "each account's windows", kind: ResourceKind::ModelsAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SESSIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSessions,
    uri_template: "cairn://models/sessions",
    name: "Model sessions",
    description: "Every live model session on this host: its session id, the node it belongs to, provider, model, the account its launch chose, the provider's own id for the conversation, the machine it runs on (`local` today), whether it is busy (starting, serving a turn, or holding one open) or warm (parked between turns, resumable without a new process), and when it started. Read cairn://models/sessions/<id> for one session and to stop it. To start a session, append a task to `cairn:~/tasks` (choose the provider and model with `backend`/`tier`); to send a live session a message, append to its home's `/messages` (the home is shown on each session). This resource reads and stops sessions; it does not duplicate those doors.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: &[CrossActionSpec {
        kind: ResourceKind::NodeTasks,
        mode: ChangeMode::Append,
        label: "start a session: delegate a task (choose its provider and model with backend/tier)",
    }],
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SESSION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSession,
    uri_template: "cairn://models/sessions/{id}",
    name: "Model session",
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
        required: &[MODELS_SESSION_ACTION],
        optional: &[],
        label: "stop a session",
        example: r#"write({changes:[{target:"cairn://models/sessions/SESSION_ID",mode:"patch",payload:{action:"stop"}}]})"#,
    }],
};
