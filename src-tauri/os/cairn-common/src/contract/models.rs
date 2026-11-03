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

const MODELS_DECIDE_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model to decide with: a catalog id, native id, alias, or provider/model, as cairn://models/catalog?serves=decide lists them; never chosen for you",
);

const MODELS_DECIDE_INPUT: KeySpec =
    KeySpec::new("input", KeyType::Str, "the state the questions judge");

const MODELS_DECIDE_QUESTIONS: KeySpec = KeySpec::new(
    "questions",
    KeyType::Array,
    "[{name, instructions, type}] — type choice (with options:[{value, description?}], 1-255) | predicate (yes or no) | score (with levels:[{label, description?}], 2-10, lowest first); names unique, letters, digits, _ and -",
);

const MODELS_DECIDE_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "KEY — only this provider's offers of `model`, e.g. custom:<account id>",
);

pub(crate) const MODELS_DECIDE_RUN_TARGET: RunTargetSpec = RunTargetSpec {
    kind: ResourceKind::ModelsDecide,
    label: "answer typed questions with a named model",
    required: &[
        MODELS_DECIDE_MODEL,
        MODELS_DECIDE_INPUT,
        MODELS_DECIDE_QUESTIONS,
    ],
    optional: &[MODELS_DECIDE_PROVIDER],
    example: r#"run({commands:[{target:"cairn://models/decide",payload:{args_json:{model:"MODEL",input:"...",questions:[{name:"q",type:"predicate",instructions:"..."}]}}}]})"#,
};

const MODELS_EMBED_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model to embed with: a catalog id or provider/model, as cairn://models/embed lists them; never chosen for you, because vectors are only comparable within one model",
);

const MODELS_EMBED_INPUT: KeySpec = KeySpec::new(
    "input",
    KeyType::Array,
    "[string] — the texts to embed, none empty; one vector comes back per text, in order",
);

const MODELS_EMBED_PURPOSE: KeySpec = KeySpec::new(
    "purpose",
    KeyType::Str,
    "document (default) | query — whether the texts are what is searched or what searches; the facet applies it as the offer declares",
);

const MODELS_EMBED_WIDTH: KeySpec = KeySpec::new(
    "width",
    KeyType::Int,
    "N — exactly this many dimensions, where the offer can return it (refused naming the widths it can); absent means the model's native width",
);

pub(crate) const MODELS_EMBED_RUN_TARGET: RunTargetSpec = RunTargetSpec {
    kind: ResourceKind::ModelsEmbed,
    label: "embed texts with a named model",
    required: &[MODELS_EMBED_MODEL, MODELS_EMBED_INPUT],
    optional: &[MODELS_EMBED_PURPOSE, MODELS_EMBED_WIDTH],
    example: r#"run({commands:[{target:"cairn://models/embed",payload:{args_json:{model:"openai/text-embedding-3-small",input:["first text","second text"]}}}]})"#,
};

const MODELS_COMPLETE_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model to complete with: a catalog id, native id, alias, or provider/model, as cairn://models/complete lists them; never chosen for you",
);

const MODELS_COMPLETE_PROMPT: KeySpec = KeySpec::new(
    "prompt",
    KeyType::Str,
    "one user message; give this or `messages`, not both",
);

const MODELS_COMPLETE_MESSAGES: KeySpec = KeySpec::new(
    "messages",
    KeyType::Array,
    "[{role: user|assistant, content}] — the conversation so far, ending with a user message; give this or `prompt`, not both",
);

const MODELS_COMPLETE_SYSTEM: KeySpec = KeySpec::new("system", KeyType::Str, "the system prompt");

const MODELS_COMPLETE_OPTIONS: KeySpec = KeySpec::new(
    "options",
    KeyType::Object,
    "{reasoningEffort?: low|medium|high|..., serviceTier?: STR, fastMode?: bool} — the facet's per-call completion options, applied as the offer allows; the same keys a response definition's `options` takes",
);

const MODELS_COMPLETE_OUTPUT: KeySpec = KeySpec::new(
    "output",
    KeyType::Object,
    "a JSON Schema the answer must meet; the parsed answer comes back as `structured`, and a violating answer is retried once naming the violation",
);

pub(crate) const MODELS_COMPLETE_RUN_TARGET: RunTargetSpec = RunTargetSpec {
    kind: ResourceKind::ModelsComplete,
    label: "send one completion to a named model",
    required: &[MODELS_COMPLETE_MODEL],
    optional: &[
        MODELS_COMPLETE_PROMPT,
        MODELS_COMPLETE_MESSAGES,
        MODELS_COMPLETE_SYSTEM,
        MODELS_COMPLETE_OPTIONS,
        MODELS_COMPLETE_OUTPUT,
    ],
    example: r#"run({commands:[{target:"cairn://models/complete",payload:{args_json:{model:"MODEL",prompt:"..."}}}]})"#,
};

const MODELS_MEDIA_PROVIDER: KeySpec = KeySpec::new(
    "provider",
    KeyType::Str,
    "KEY — only this provider's offers of the model, e.g. openrouter, codex, custom:<account id>",
);

const MODELS_IMAGE_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model to draw with: a catalog id, native id or provider/model, as cairn://models/image lists them; absent takes the workspace's `imageModel`, and with none set the call is refused",
);

const MODELS_IMAGE_PROMPT: KeySpec = KeySpec::new("prompt", KeyType::Str, "what to draw");

const MODELS_IMAGE_COUNT: KeySpec = KeySpec::new(
    "count",
    KeyType::Int,
    "N — how many images (default 1), up to what the offer draws per call",
);

const MODELS_IMAGE_SIZE: KeySpec = KeySpec::new(
    "size",
    KeyType::Str,
    "WIDTHxHEIGHT, e.g. 1024x1536; give at most one of size, aspect and orientation",
);

const MODELS_IMAGE_ASPECT: KeySpec = KeySpec::new("aspect", KeyType::Str, "W:H, e.g. 2:3");

const MODELS_IMAGE_ORIENTATION: KeySpec =
    KeySpec::new("orientation", KeyType::Str, "portrait | landscape | square");

const MODELS_IMAGE_REFERENCES: KeySpec = KeySpec::new(
    "references",
    KeyType::Array,
    "[{image: cairn://p/PROJECT/ISSUE/images/N, label?}] — stored images to draw from, in this run's project",
);

const MODELS_IMAGE_EDIT: KeySpec = KeySpec::new(
    "edit",
    KeyType::Object,
    "{image: cairn://p/.../images/N, mask?: cairn://p/.../images/N} — a stored image to change rather than draw from scratch; a mask's transparent pixels mark where",
);

const MODELS_IMAGE_EXTRAS: KeySpec = KeySpec::new(
    "extras",
    KeyType::Object,
    "{WIRE: {field: value}} — provider-only knobs (quality, seed, ...), applied only when that wire carries the call, e.g. {\"openai-images\": {\"quality\": \"high\"}}",
);

pub(crate) const MODELS_IMAGE_RUN_TARGET: RunTargetSpec = RunTargetSpec {
    kind: ResourceKind::ModelsImage,
    label: "draw images with a named model or the workspace's image default",
    required: &[MODELS_IMAGE_PROMPT],
    optional: &[
        MODELS_IMAGE_MODEL,
        MODELS_MEDIA_PROVIDER,
        MODELS_IMAGE_COUNT,
        MODELS_IMAGE_SIZE,
        MODELS_IMAGE_ASPECT,
        MODELS_IMAGE_ORIENTATION,
        MODELS_IMAGE_REFERENCES,
        MODELS_IMAGE_EDIT,
        MODELS_IMAGE_EXTRAS,
    ],
    example: r#"run({commands:[{target:"cairn://models/image",payload:{args_json:{prompt:"a lighthouse at dusk",orientation:"landscape"}}}]})"#,
};

const MODELS_SPEAK_MODEL: KeySpec = KeySpec::new(
    "model",
    KeyType::Str,
    "the model to speak with: a catalog id, native id or provider/model, as cairn://models/speak lists them; absent takes the workspace's `speechModel`, and with none set the call is refused",
);

const MODELS_SPEAK_TEXT: KeySpec = KeySpec::new("text", KeyType::Str, "what to say");

const MODELS_SPEAK_VOICE: KeySpec = KeySpec::new(
    "voice",
    KeyType::Str,
    "who speaks: an OpenAI voice such as alloy, a compatible server's voice name, or an ElevenLabs voice_id",
);

const MODELS_SPEAK_FORMAT: KeySpec = KeySpec::new(
    "format",
    KeyType::Str,
    "codec[_rate[_kbps]]: mp3, opus, pcm_24000, mp3_44100_128; absent takes the offer's default. PCM comes back as a WAV file",
);

const MODELS_SPEAK_SPEED: KeySpec = KeySpec::new(
    "speed",
    KeyType::Float,
    "1.0 is normal, within the offer's range",
);

const MODELS_SPEAK_INSTRUCTIONS: KeySpec = KeySpec::new(
    "instructions",
    KeyType::Str,
    "free-text delivery direction, for offers that take it (OpenAI's gpt-4o-mini-tts)",
);

pub(crate) const MODELS_SPEAK_RUN_TARGET: RunTargetSpec = RunTargetSpec {
    kind: ResourceKind::ModelsSpeak,
    label: "speak text as one audio clip with a named model or the workspace's speech default",
    required: &[MODELS_SPEAK_TEXT, MODELS_SPEAK_VOICE],
    optional: &[
        MODELS_SPEAK_MODEL,
        MODELS_MEDIA_PROVIDER,
        MODELS_SPEAK_FORMAT,
        MODELS_SPEAK_SPEED,
        MODELS_SPEAK_INSTRUCTIONS,
    ],
    example: r#"run({commands:[{target:"cairn://models/speak",payload:{args_json:{text:"Hello there.",voice:"alloy",format:"mp3"}}}]})"#,
};

pub(crate) const MODELS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Models,
    uri_template: "cairn://models",
    name: "Models facet",
    description: "The cairn-models facet: how tokens are produced, routed, and accounted. An index of its resources: the catalog of providers and models; the provider accounts and their usage windows; each provider's pooled capacity; and the live model sessions. Every read shows what Settings → Providers shows for this workspace, and none reveals a credential value.",
    read_projections: NO_PROJECTIONS,
    related: &[
        RelatedSpec { label: "providers and models", kind: ResourceKind::ModelsCatalog, actions: false },
        RelatedSpec { label: "answer typed questions with a named model (a run target)", kind: ResourceKind::ModelsDecide, actions: false },
        RelatedSpec { label: "embed texts with a named model (a run target)", kind: ResourceKind::ModelsEmbed, actions: false },
        RelatedSpec { label: "send one completion to a named model (a run target)", kind: ResourceKind::ModelsComplete, actions: false },
        RelatedSpec { label: "draw images (a run target)", kind: ResourceKind::ModelsImage, actions: false },
        RelatedSpec { label: "speak text as an audio clip (a run target)", kind: ResourceKind::ModelsSpeak, actions: false },
        RelatedSpec { label: "accounts and their usage windows; sign in and re-measure", kind: ResourceKind::ModelsAccounts, actions: false },
        RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::ModelsUsage, actions: false },
        RelatedSpec { label: "spend by thread for a window, reconciled with the Analytics Cost card", kind: ResourceKind::ModelsSpend, actions: false },
        RelatedSpec { label: "pools, bands, and the capability ladder", kind: ResourceKind::ModelsLadder, actions: false },
        RelatedSpec { label: "what an allotment would select now", kind: ResourceKind::ModelsSelection, actions: false },
        RelatedSpec { label: "the embedding model and how far history is embedded in it", kind: ResourceKind::ModelsEmbeddings, actions: false },
        RelatedSpec { label: "live model sessions", kind: ResourceKind::ModelsSessions, actions: false },
        RelatedSpec { label: "outside clients of the served OpenAI-compatible API and their keys", kind: ResourceKind::ModelsClients, actions: false },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_CATALOG_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsCatalog,
    uri_template: "cairn://models/catalog",
    name: "Model catalog",
    description: "Every provider this workspace installed, in the order Settings → Providers lists them: its category, whether an account or host is configured, what model discovery last found (id, name, context window, pricing where the provider reports it with each price in its own unit, what the model takes in and gives out, and the verbs it serves with the wire each rides; or the discovery error), what the provider serves for any model it routes, and any model observed unable to serve a verb it declares, with why. The same catalog is what selection and context-window lookup read, so a model listed here is one an allotment can select.",
    read_projections: &[
        ProjectionSpec { key: "view", values: "providers (default) | models — each model once under its catalog id (the provider's canonical slug, else provider/model), with every provider offering it" },
        ProjectionSpec { key: "provider", values: "KEY — one provider, e.g. claude, codex, openrouter, custom:<account id>" },
        ProjectionSpec { key: "installed", values: "all — also list the supported providers this workspace has not installed (the Add-provider list)" },
        ProjectionSpec { key: "serves", values: "complete | session | decide | image | embed — only the models (in either view) that serve that verb; an image offer also says what it can draw (count, references, aspect ratios or sizes, mask), and an embed offer its width, which widths it can return, how it tells documents from queries, and its batch limits" },
    ],
    related: &[RelatedSpec { label: "accounts behind each provider", kind: ResourceKind::ModelsAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_DECIDE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsDecide,
    uri_template: "cairn://models/decide",
    name: "Decide",
    description: "The decide verb's door, invoked with `run`, not written. Give it a model, the input to judge, and typed questions (choice, predicate, score); it answers each question in order with the model's probability over every answer, how that was measured (`native` from a Decisions model, `logprobs` from a chat model's next-token distribution), and a refusal where the model declined. It returns the decision as the verb produced it, with no thresholds applied, plus the offer that answered and the call's latency. It never chooses a model: an absent one, one that serves no decisions, or one that cannot measure these questions is refused, naming the offers that do serve decide, and nothing else is asked instead. An unconfirmed logprobs offer is probed once first, exactly as a recipe's decision node is. Each provider request is bounded at 30 seconds and a probe at 15; the item's own `timeout` releases the caller sooner, and the call it abandons ends within those bounds. Reading it lists the offers that serve decide now.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "every offer that serves decide", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_EMBED_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsEmbed,
    uri_template: "cairn://models/embed",
    name: "Embed",
    description: "The embed verb's door, invoked with `run`, not written. Give it a model and the texts; it returns one vector per text in input order, the exact model and width that produced them, the provider, input tokens, cost where the provider reports it, and the call's latency. `purpose` says whether the texts are documents or queries, and the facet applies it as the offer declares (a request field, an instruction prefix, or nothing). `width` asks for exactly that many dimensions and is refused, naming the widths the offer can return, when the offer cannot; absent means the native width. A request over the offer's batch limits is split into consecutive requests and joined in order, and a single text longer than one request may carry is refused by its index. It never chooses a model: an absent one, or one that serves no embeddings, is refused naming the offers that do, and nothing else is asked instead. Each provider request is bounded at 60 seconds and the item at the 120-second host-item cap, so embed a large corpus in calls of a few hundred texts; a call its caller abandons sends no further request. An agent reads the reply as text, which omits the vectors when they would not fit in a run result; a script receives them whole through `embed()` in @cairn/sdk or cairn-sdk. Reading it lists the offers that serve embed now, with their widths, purpose handling and batch limits.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "every offer that serves embed", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_COMPLETE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsComplete,
    uri_template: "cairn://models/complete",
    name: "Complete",
    description: "The complete verb's door, invoked with `run`, not written: one tool-free completion with a model the caller names, without writing a response definition first. Give it the model and either a `prompt` or `messages` (ending with a user message), and optionally a `system` prompt, per-call `options` (reasoningEffort, serviceTier, fastMode, the keys a response definition takes) and an `output` JSON Schema. It returns the answer's `text`, the parsed answer as `structured` when a schema was given (an answer that violates it is retried once, naming the violation), the model and provider that answered, input and output tokens, cost where the provider reports it, latency, and the `history` entry the call was journaled as. It goes through the same execution and journal a named response does, so through the launch door's verb gate, the provider's account selection and its retry. It never chooses a model: an absent one, or one that serves no completions, is refused naming offers that do. Each provider request is bounded at 10 minutes, and the item runs to its own `timeout` up to the run ceiling, suspending its batch past the 120-second window as a response item does. Response definitions stay the named, reusable layer over the same verb. Reading it lists the providers that serve complete and how many models each offers.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "every offer that serves complete", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_IMAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsImage,
    uri_template: "cairn://models/image",
    name: "Image",
    description: "The image verb's door, invoked with `run`, not written. Give it a prompt and, optionally, the model, a shape (one of size, aspect or orientation), a count, stored reference images, a stored image to edit, and per-wire extras. The model is the one named, else the workspace's `imageModel` setting; with neither, the call is refused naming that setting, so leaving it unset keeps agents from drawing unasked. A named model that cannot take the request (a mask it does not support, a count past its limit) is refused naming the offers that could, and nothing else draws instead. Each image comes back as an image the caller sees and is stored in this run's issue as `cairn://p/PROJECT/ISSUE/images/N`, which renders wherever it is cited; a script gets those addresses, and the attachment `materialize` action puts one in its working cell as a file. The reply also carries the provenance (the provider, the model, the prompt as sent after any lowering) and the call's metered usage with its cost where the provider reports it. No per-request journal records it. One call is bounded at 180 seconds, and the item runs to its own `timeout`, suspending its batch past the 120-second window as a response item does. Reading it lists the offers that draw now, with what each can do, and the workspace default.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "every offer that serves image", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SPEAK_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSpeak,
    uri_template: "cairn://models/speak",
    name: "Speak",
    description: "The speech verb's door, invoked with `run`, not written. The verb streams; this door speaks one whole text and returns the finished clip. Give it the text and a voice and, optionally, the model, a format, a speed and delivery instructions. The model is the one named, else the workspace's `speechModel` setting; with neither, the call is refused naming that setting. A format, speed or instruction the offer cannot serve is refused naming the offers that could. The clip is stored in this run's issue as `cairn://p/PROJECT/ISSUE/attachments/N` (MP3, Ogg Opus, or WAV for PCM), at most 25 MiB; the attachment `materialize` action puts it in the caller's working cell as a file. The reply carries the address, the format, the offer that spoke, and the call's metered usage. No per-request journal records it. The item runs to its own `timeout`, suspending its batch past the 120-second window as a response item does, and a clip still speaking when it ends is cancelled upstream. Reading it lists the offers that speak now and the workspace default.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "providers and the models they offer", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_ACCOUNTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsAccounts,
    uri_template: "cairn://models/accounts",
    name: "Model accounts",
    description: "Every provider account visible to the workspace, grouped by provider in priority order: label, the kind of credential it holds (a Cairn-managed Claude sign-in, a ChatGPT sign-in, an API key, a host, a custom endpoint) but never the credential itself, email, the subscription plan and its monthly list price (or no price) where the account is a subscription, with the candidate plans when the provider cannot say which it is, and its usage windows (remaining percent and reset time) with when they were measured and any block still in force, including a hold no clock lifts (usage credits exhausted, or provider terms the account's owner must accept) with what lifts it. A window whose reset has passed since it was measured is shown as expired history, not as remaining capacity. A sign-in still in the user's hands shows under its provider. Append {provider: claude|codex} to start a sign-in for a new subscription account: a browser opens on this machine, the result names the page to open if it did not, and the account appears here when the user finishes. Starting a sign-in needs the `workspace/default/settings/accounts:write` grant, the same place `cairn://settings` `accounts` writes reach: an Ask run is prompted for it and a Deny run is refused. Patch {action:\"refresh_usage\", provider} to re-measure one provider's accounts now, {action:\"set_plan\", account, plan} to name which candidate plan an account is on, as Settings → Providers' plan toggle does, or {action:\"use_reset\", provider:\"codex\", account} to spend one of a Codex account's usage resets, as its row's Use does; a reset cannot be refunded, so spending one needs that account's `account/codex/<id>:write` grant, and an Ask run is prompted for it. Each account lists the usage resets it holds. People sign in through Settings → Providers or `cairn auth login claude`; all three doors run the same sign-in.",
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
        ProjectionSpec { key: "view", values: "card (default) | requests — the served API's per-request rows (outside clients), with per-client totals" },
        ProjectionSpec { key: "client", values: "NAME|ID — with view=requests: one served-API client's rows" },
        ProjectionSpec { key: "hours", values: "N — with view=requests: how far back (default 24)" },
    ],
    related: &[RelatedSpec { label: "each account's windows", kind: ResourceKind::ModelsAccounts, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SPEND_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSpend,
    uri_template: "cairn://models/spend",
    name: "Model spend",
    description: "What each line of work cost over a window, as Settings → Analytics → Spend by thread shows it: every thread with its child issues nested beneath it, then unparented issues and other spend by kind (workflow, unplaced job, no job), each with its list-price cost, billable tokens, and per-model split. A run's cost belongs to the thread reached by following parent edges up from its issue (an issue parent first, then a thread parent), a sub-agent task's to the node that spawned it, and a thread session's to the thread itself; the edges are read at query time, so reparenting an issue moves its history with it. The total equals the Analytics Cost card for the same window and project.",
    read_projections: &[
        ProjectionSpec { key: "by", values: "thread (default) — the only grouping so far" },
        ProjectionSpec { key: "window", values: "24h (default) | 7d | 30d | 90d | all — the Analytics page's ranges" },
        ProjectionSpec { key: "thread", values: "NAME | KEY/NAME | unparented | other — one root's nested tree; a bare name several projects share is refused with the candidates" },
        ProjectionSpec { key: "project", values: "KEY — only this project's spend, as the page's project scope" },
        ProjectionSpec { key: "format", values: "markdown (default) | json — the serialized tree, or the one root with thread=" },
    ],
    related: &[RelatedSpec { label: "pooled capacity per provider", kind: ResourceKind::ModelsUsage, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_LADDER_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsLadder,
    uri_template: "cairn://models/ladder",
    name: "Model ladder",
    description: "The pools an allotment names and the capability ladder they slice, as Settings → Models shows them. Each pool with its description and kind: a band of the ladder (from its min to the next band's), or a hand-picked set and its members. The bands in order with their bounds. Every ladder entry, highest level first: its level on 0..1 and where that came from (set by a person, seeded by Cairn, measured), its band, its maker, its effort position with the level it launches at and its model's effort scale, its other launch options, the set pools it belongs to, and where licensed public benchmarks place the model (level, interval, sources, anchor) as a hint that never replaces a stored level. Then the served session models on installed providers that are not on the ladder (unranked), each with its benchmark placement and band when the benchmarks cover it (an allotment naming one selects it there), the default allotment, any notes from the one-time conversion from tiers, and the benchmark data's license and credit lines. Change it with a `cairn://settings` patch of `models`.",
    read_projections: NO_PROJECTIONS,
    related: &[
        RelatedSpec { label: "what an allotment would select now", kind: ResourceKind::ModelsSelection, actions: false },
        RelatedSpec { label: "providers and their discovered models", kind: ResourceKind::ModelsCatalog, actions: false },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SELECTION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSelection,
    uri_template: "cairn://models/selection",
    name: "Model selection preview",
    description: "What an allotment would run on if it launched now, with the same selection a launch makes: the allotment expanded against the pools and ladder, the candidates the catalog serves for the verb, the band its top falls in, the band's candidates in supply order (expiring subscription allotment first, then headroom, then metered price) with what each costs now, the candidates set aside and why, the chosen model, account and effort, and the one-line provenance. A preview: nothing is stored and no provider is probed, so it reads the usage last measured. A model named directly is never replaced; when it cannot run the preview says why instead.",
    read_projections: &[
        ProjectionSpec { key: "models", values: "ENTRY,ENTRY — the allotment: pool ids and provider/model pairs, e.g. lg,claude/fable (default: the workspace's defaultModels)" },
        ProjectionSpec { key: "verb", values: "session (default) | complete | decide — what the launch would run" },
    ],
    related: &[RelatedSpec { label: "the pools and ladder an allotment names", kind: ResourceKind::ModelsLadder, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_EMBEDDINGS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsEmbeddings,
    uri_template: "cairn://models/embeddings",
    name: "Embeddings",
    description: "The workspace's embedding model and how far its stored vectors are in it, as Settings → Models → Embeddings shows: the model (the `embeddingModel` setting, or the Cairn gateway's Cohere Embed v4 when it is unset), the width each corpus is stored at, the state (running, paused with when it began, the last error and the next attempt, complete, or unavailable and why), turns embedded out of those eligible (and how many are deferred because their text could not be read yet), resources embedded out of those eligible once the resource sweep has counted them, and whether history before this install is in scope (it is when the model was chosen explicitly or an account is connected). Changing the model is a `cairn://settings` patch of `embeddingModel`; it deletes the old model's vectors and re-embeds history.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "providers and the embedding models they offer", kind: ResourceKind::ModelsCatalog, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const MODELS_SESSIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsSessions,
    uri_template: "cairn://models/sessions",
    name: "Model sessions",
    description: "Every live model session on this host: its session id, the node it belongs to, provider, model, the account its launch chose, the provider's own id for the conversation, the machine it runs on (`local` today), whether it is busy (starting, serving a turn, or holding one open) or warm (parked between turns, resumable without a new process), and when it started. Read cairn://models/sessions/<id> for one session and to stop it. To start a session, append a task to `cairn:~/tasks` (choose its model with `models`, an allotment of pools and provider/model pairs); to send a live session a message, append to its home's `/messages` (the home is shown on each session). This resource reads and stops sessions; it does not duplicate those doors.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: &[CrossActionSpec {
        kind: ResourceKind::NodeTasks,
        mode: ChangeMode::Append,
        label: "start a session: delegate a task (choose its model with models)",
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

const MODELS_CLIENT_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "the client's name (letters, digits, - _ .), unique among live keys; an alias, never its identity",
);
const MODELS_CLIENT_REMOTE: KeySpec = KeySpec::new(
    "remote",
    KeyType::Bool,
    "true grants the client use of the workspace's OpenRouter account (a standing grant bound to the client); false, the default, keeps it to Cairn's own inference deployments. On a client, false revokes the grants the client holds, so only the client itself may write it; the operator revokes them at Settings → Grants",
);
const MODELS_CLIENT_IMPORT: KeySpec = KeySpec::new(
    "import",
    KeyType::Array,
    "[{name, keyHash, remote?}]: clients whose existing keys are kept by hash (lowercase hex SHA-256 of the key, bgllm's scheme), all or none",
);
const MODELS_CLIENT_ORIGIN: KeySpec = KeySpec::new(
    "origin",
    KeyType::Str,
    "where imported keys were issued; default bgllm",
);
const MODELS_CLIENT_REVOKE: KeySpec = KeySpec::new(
    "revoke",
    KeyType::Bool,
    "the operator's alone, and refused here for every caller: revoking cuts off the key and every grant bound to it, and a write here cannot prove it comes from the operator, who revokes a client at Settings → Served API in the Cairn desktop app",
);

pub(crate) const MODELS_CLIENTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsClients,
    uri_template: "cairn://models/clients",
    name: "Served API clients",
    description: "Who may call Cairn's served OpenAI-compatible API (/v1/models, chat, completions, responses, embeddings) from outside: each client's name, where its key came from, the last four characters of a key Cairn minted, whether it is live, whether it may use remote providers (the grant that allows it, or local-only), and when it last made a request. A key hash is never shown. Append {name} to mint a key, shown once; append {import:[...]} to keep existing keys by hash. Minting, importing and granting remote use ask the operator when an agent writes them. Requests are at cairn://models/usage?view=requests; the listener is the servedApi section of cairn://settings.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "each client's requests", kind: ResourceKind::ModelsUsage, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[],
            optional: &[MODELS_CLIENT_NAME, MODELS_CLIENT_REMOTE, MODELS_CLIENT_IMPORT, MODELS_CLIENT_ORIGIN],
            label: "mint a client key (name), or import existing keys by hash (import)",
            example: r#"write({changes:[{target:"cairn://models/clients",mode:"append",payload:{name:"dig"}}]})"#,
        },
    ],
};

pub(crate) const MODELS_CLIENT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ModelsClient,
    uri_template: "cairn://models/clients/{id}",
    name: "Served API client",
    description: "One outside client of the served API, by its immutable id: its name, key provenance and hint, whether it is live, its remote grants, and its recent requests. Patch {remote:true} to grant use of the workspace's OpenRouter account (it asks the operator when an agent writes it). Withdrawing remote use revokes grants the client holds, and revoking the client cuts it off, so they are not an agent's to do: the operator revokes a client at Settings → Served API and a client's grants at Settings → Grants in the Cairn desktop app.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "every served-API client", kind: ResourceKind::ModelsClients, actions: false }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[],
        optional: &[MODELS_CLIENT_REMOTE, MODELS_CLIENT_REVOKE],
        label: "grant remote use (withdrawing it, and revoking the key, are refused to all but the client and the operator)",
        example: r#"write({changes:[{target:"cairn://models/clients/CLIENT_ID",mode:"patch",payload:{remote:true}}]})"#,
    }],
};
