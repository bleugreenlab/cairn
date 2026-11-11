//! Reusable `KeySpec`/projection/related/cross-action constants shared by the
//! `RESOURCE_CONTRACTS` table entries.

use super::types::*;
use cairn_uri::ResourceKind;

// Reusable key specs. Notes carry value guidance (enumerations/defaults) only
// where the key name + type + example don't already make the value obvious; an
// empty note renders as just `key(type)`.
pub(crate) const CONTENT: KeySpec = KeySpec::new("content", KeyType::Str, "");
pub(crate) const CHANNEL_CONVERSATION: KeySpec = KeySpec::new(
    "conversation",
    KeyType::Str,
    "canonical address of a row that carries send",
);
pub(crate) const AMBIENT_FEEDBACK_EVENT: KeySpec = KeySpec::new(
    "event",
    KeyType::Any,
    "the event URI the delivered message cites (cairn://p/{project}/events/{id}), or its id",
);
pub(crate) const AMBIENT_FEEDBACK_VERDICT: KeySpec =
    KeySpec::new("verdict", KeyType::Str, "not_mine | reroute");
pub(crate) const AMBIENT_FEEDBACK_TO: KeySpec = KeySpec::new(
    "to",
    KeyType::Str,
    "the chartered thread URI that owns it; required for reroute, refused for not_mine",
);
pub(crate) const AMBIENT_FEEDBACK_NOTE: KeySpec = KeySpec::new("note", KeyType::Str, "");
pub(crate) const CHANNEL_MESSAGE_CONTENT: KeySpec = KeySpec::new(
    "content",
    KeyType::Str,
    "plain text, at most the provider's message limit",
);
/// Node/task direct messages only. A project or issue channel append ignores
/// this key, so declaring it there would advertise a promise that surface does
/// not keep.
pub(crate) const ESCALATE: KeySpec = KeySpec::new(
    "escalate",
    KeyType::Bool,
    "interrupt the recipient's active turn instead of queueing for its next one",
);
pub(crate) const PROGRESS_KIND: KeySpec = KeySpec::new("kind", KeyType::Str, "phase | log");
pub(crate) const PROGRESS_TEXT: KeySpec = KeySpec::new(
    "text",
    KeyType::Str,
    "phase name (kind=phase) or log message (kind=log)",
);
pub(crate) const EXECUTOR_MACHINE_SERVICES: KeySpec = KeySpec::with_aliases(
    "machineServices", &["machine_services"], KeyType::Object,
    "{\"<pack>/<service>\": {} | {override:{command:\"/abs/path\"}} | null}: select a pack-declared machine service on this machine (running the pack's artifact, or the operator's own absolute binary) or unselect it. Stored fleet configuration, not an authorization gate; the service's state is read from cairn://executors/<name>",
);
pub(crate) const EXECUTOR_REPAIRED: KeySpec = KeySpec::new(
    "repaired",
    KeyType::Bool,
    "declare this machine repaired, re-arming every check infrastructure-suppressed on its environment and re-admitting it for projects it failed to prepare (a setup command not found); for a repair the executor's own attach probes cannot see. Does not lift a MISSING build-prerequisite verdict, which only a re-attach refreshes",
);
pub(crate) const REBASE_RESOLUTION: KeySpec = KeySpec::new(
    "resolution",
    KeyType::Str,
    "take-committed-tip — explicitly use the branch's committed tip content for the session's conflicting paths",
);
pub(crate) const REBASE_FAST_FORWARD_BRANCH: KeySpec = KeySpec::new(
    "branch",
    KeyType::Str,
    "fast-forward-from-origin only: `base` takes origin's copy of the branch this node is cut from instead of the node's own",
);
pub(crate) const REBASE_DROP_INCOMING_REASON: KeySpec = KeySpec::new(
    "drop_incoming_reason",
    KeyType::Str,
    "why discarding the incoming hunks a take-committed-tip would drop is correct; only needed when the replay is refused for dropping them; for a file your branch deletes that the base edited, states where the base's change went (or why it is moot) and is recorded as that deletion's resolution",
);
pub(crate) const OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
pub(crate) const NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "text replacement operation key; not stored as artifact metadata",
);
pub(crate) const REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace all old_string matches; default false errors if old_string is non-unique",
);
pub(crate) const SUBMIT: KeySpec = KeySpec::new(
    "submit",
    KeyType::Bool,
    "send as a command line (append newline if missing); set false to send bytes verbatim. default true",
);
pub(crate) const FIELD: KeySpec = KeySpec::new(
    "field",
    KeyType::Str,
    "top-level string artifact field to edit; defaults to content then body",
);
pub(crate) const COMMAND: KeySpec = KeySpec::new("command", KeyType::Str, "");
pub(crate) const REPL_INTERPRETER: KeySpec = KeySpec::new(
    "interpreter",
    KeyType::Str,
    "python (py) | typescript (ts) — the language this REPL evaluates",
);
pub(crate) const REPL_DEPS: KeySpec = KeySpec::new(
    "deps",
    KeyType::Array,
    "python-only: package names preloaded via uv (uv run --with)",
);
/// The selector a terminal or REPL create takes: `run`'s own, under the same
/// honour-or-refuse rule.
pub(crate) const RESIDENT_EXECUTOR: KeySpec = KeySpec::new(
    "executor",
    KeyType::Object,
    "{name} for one machine as cairn://executors lists it, or {os} for any machine on that platform, optionally with requiredToolchains (the selector run takes). Started there or refused naming what prevents it, such as a live terminal or REPL holding this job's environment on another machine; an idle environment on another machine moves, losing its uncommitted files. Omit to join the job's environment wherever it is. The reply and the page name the machine",
);
/// A home's placement default (cairn/5920), on the node, task and thread
/// patches alike, so one sentence says what it is wherever it is advertised.
pub(crate) const PLACEMENT_DEFAULT_EXECUTOR: KeySpec = KeySpec::new(
    "executor",
    KeyType::Object,
    "this home's placement default: {name} for one machine as cairn://executors lists it, or {os} for any machine on that platform, optionally with requiredToolchains (the selector run takes); null clears it. Its own run batches, terminals and REPLs created without an `executor` go there, the result says `(node default)`, and a machine that cannot serve refuses the work by name. A call-level `executor` overrides it; sub-agent tasks and check lanes never inherit it",
);
pub(crate) const WAKE: KeySpec = KeySpec::new(
    "wake",
    KeyType::Str,
    "\"exit\" to resume when the command finishes, or a literal output phrase to resume when it prints (also fires on exit)",
);
pub(crate) const BROWSER_URL: KeySpec = KeySpec::with_aliases(
    "url",
    &["navigate"],
    KeyType::Str,
    "navigate the browser to this URL",
);
/// The full browser patch action vocabulary. Advertised verbatim in
/// [`BROWSER_ACTION`] and pinned against the set `apply_browser_action` actually
/// handles by a cairn-core test, so the structured affordance can't silently
/// under-advertise an action the dispatch really accepts.
pub const BROWSER_ACTIONS: &[&str] = &[
    "back",
    "forward",
    "reload",
    "click",
    "type",
    "select",
    "drag",
    "scroll",
    "waitFor",
    "waitForNavigation",
    "waitForLoad",
    "clearData",
];
pub(crate) const BROWSER_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "back|forward|reload (history); click (needs selector|text|handle); type (needs value + selector|text|handle); select (picks a <select> option: needs value + selector|text|handle); drag (needs a source selector|text|handle + a destination toSelector|toText|toHandle); scroll (needs selector|text|handle|to|by); waitFor (needs selector); waitForNavigation|waitForLoad (await the next navigation/page-load, optional timeoutMs); clearData (clears website data — default cookies+cache, or kinds). Interaction args below.",
);
pub(crate) const BROWSER_SELECTOR: KeySpec = KeySpec::new(
    "selector",
    KeyType::Str,
    "CSS selector target for click/type/scroll/waitFor",
);
pub(crate) const BROWSER_TEXT: KeySpec = KeySpec::new(
    "text",
    KeyType::Str,
    "visible-text target (alternative to selector) for click/type/scroll",
);
pub(crate) const BROWSER_VALUE: KeySpec = KeySpec::new(
    "value",
    KeyType::Str,
    "text to type (type; may be empty to clear the field), or the option label/value to pick (select); required by both",
);
pub(crate) const BROWSER_TO_SELECTOR: KeySpec = KeySpec::new(
    "toSelector",
    KeyType::Str,
    "CSS selector for a drag destination",
);
pub(crate) const BROWSER_TO_TEXT: KeySpec = KeySpec::new(
    "toText",
    KeyType::Str,
    "visible-text drag destination (alternative to toSelector)",
);
pub(crate) const BROWSER_TO_HANDLE: KeySpec = KeySpec::with_aliases(
    "toHandle",
    &["toRef"],
    KeyType::Str,
    "element handle (ref e1..eN) for a drag destination",
);
pub(crate) const BROWSER_MODE: KeySpec = KeySpec::new(
    "mode",
    KeyType::Str,
    "drag event family: auto (default; html5 when the source is draggable, else pointer) | pointer | html5",
);
pub(crate) const BROWSER_STEPS: KeySpec = KeySpec::new(
    "steps",
    KeyType::Int,
    "interpolated drag moves (default 10, max 60)",
);
pub(crate) const BROWSER_DELAY_MS: KeySpec = KeySpec::with_aliases(
    "delayMs",
    &["delay_ms"],
    KeyType::Int,
    "pause between drag moves in ms (default 16, max 200)",
);
pub(crate) const BROWSER_SUBMIT: KeySpec =
    KeySpec::new("submit", KeyType::Bool, "press Enter after typing (type)");
pub(crate) const BROWSER_TO: KeySpec =
    KeySpec::new("to", KeyType::Str, "scroll target top|bottom (scroll)");
pub(crate) const BROWSER_BY: KeySpec =
    KeySpec::new("by", KeyType::Int, "scroll delta in pixels (scroll)");
pub(crate) const BROWSER_TIMEOUT_MS: KeySpec = KeySpec::with_aliases(
    "timeoutMs",
    &["timeout_ms"],
    KeyType::Int,
    "poll/await budget in ms (waitFor, waitForNavigation, waitForLoad)",
);
pub(crate) const BROWSER_HANDLE: KeySpec = KeySpec::with_aliases(
    "handle",
    &["ref"],
    KeyType::Str,
    "element handle (ref e1..eN) from the last ?interactive read; a click/type/scroll locator resolved via the durable element anchor",
);
pub(crate) const BROWSER_KINDS: KeySpec = KeySpec::new(
    "kinds",
    KeyType::Array,
    "data buckets for clearData: cookies|cache|storage (default cookies+cache); clears the live webview's persistent website data",
);
pub(crate) const DESCRIPTION: KeySpec = KeySpec::new("description", KeyType::Str, "");
pub(crate) const REFERENCE_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "reference identifier used in the URI and project config",
);
pub(crate) const REFERENCE_GIT: KeySpec = KeySpec::new(
    "git",
    KeyType::Str,
    "git remote URL; use exactly one of git or path",
);
pub(crate) const REFERENCE_PATH: KeySpec = KeySpec::new(
    "path",
    KeyType::Str,
    "local directory path; use exactly one of git or path",
);
pub(crate) const REFERENCE_BRANCH: KeySpec = KeySpec::new(
    "branch",
    KeyType::Str,
    "optional git branch; send null in patch to clear",
);
pub(crate) const REFERENCE_REFRESH: KeySpec = KeySpec::new(
    "refresh",
    KeyType::Bool,
    "when true, refresh the git reference after patching",
);
pub(crate) const TITLE: KeySpec = KeySpec::new("title", KeyType::Str, "");
pub(crate) const EXECUTION: KeySpec = KeySpec::new(
    "execution",
    KeyType::Object,
    "true | {recipe?, overrides?, after?, place?} to also start an execution once the issue is created; omit to create only. With no recipe Cairn chooses: the workspace setting defaultRecipe until a measured chooser exists, refused (and no issue created) when it is unset; a named recipe is run exactly. The reply carries the resolution: the recipe and what decided it, one line per agent step with its allotment and what it would pick `now:` (each step selects at its own launch, so this is a prediction, not a lock; a single-model allotment is exact), and the URIs to change a step live or re-run on another recipe. `overrides` takes the same launch deltas the executions collection does, so a child can be created with review already off in one call. `after` takes the same gate clauses the executions collection does: the execution is filed HELD and starts only once every gate is satisfied. `place` takes the same value the executions collection does",
);
/// `place` on a filing, shared verbatim by the executions-collection append and
/// the issue-create `execution` block.
pub(crate) const QUEUE_PLACE: KeySpec = KeySpec::new(
    "place",
    KeyType::Str,
    "head — if the execution queue's caps hold this execution, it takes the earliest places its parent thread's cap-held set already occupies (fleet and thread order), never ahead of another thread's work and skipping no cap. Only for a child of a thread, filed by that thread or the operator, and never with `after`. Omit to queue behind what is already held; reorder later with cairn://p/PROJECT/THREAD/queue {order:[N,...]}",
);
/// `after:` gates, shared verbatim by the executions-collection append, the
/// issue-create `execution` block, and the execution patch — one grammar,
/// described once.
pub(crate) const AFTER_GATES: KeySpec = KeySpec::new(
    "after",
    KeyType::Array,
    "gate clause OBJECTS (never prose); the execution is filed HELD — no jobs, no slot — and starts by itself, exactly once, when every clause is satisfied. Facts: {fact:\"issue_resolution\",issue:URI,resolution?:\"merged\"|\"closed\"} · {fact:\"pr_state\",issue:URI,state?:\"open\"|\"merged\"|\"closed\"} · {fact:\"artifact_confirmed\",issue:URI,artifact?:TYPE} · {fact:\"check_verdict\",check:NAME,issue:URI,verdict?:\"passed\"|\"failed\"} · {fact:\"clock\",notBefore:RFC3339} · {fact:\"tenet_verdict\",tenet:\"cairn://p/PROJECT/tenets/N\",outcome:\"answered\"|\"ratified\"} (the operator's verdict on a tenet proposed through cairn://p/PROJECT/tenets; the only way to hold a filing on one). Request gates use {fact:\"request\",request:\"cairn://requests/ID\",outcome:\"accepted\"|\"done\"}: both fields mandatory and scalar, always read from the private DB of this install even for team work. Accepted means historical acceptance membership; done means successful fulfillment, never merely a finished failed attempt. Other optional fields mean any when omitted, and an array means any of. A clause that can never come true leaves the execution held and wakes its filer; a wait cycle across issues is refused",
);
/// Ad-hoc launch deltas, shared verbatim by the executions-collection append and
/// the issue-create `execution` block — one grammar, described once.
pub(crate) const LAUNCH_OVERRIDES: KeySpec = KeySpec::new(
    "overrides",
    KeyType::Object,
    "ad-hoc adjustments to THIS launch only, never to the recipe others run. `without:[node,...]` removes nodes and splices their edges through, e.g. {without:[\"review\"]} to skip review on a tiny fix; `nodes:{node:{agent}}` rebinds one node to a different agent and re-resolves its snapshot from that agent's config, e.g. {nodes:{\"builder\":{\"agent\":\"coordinator\"}}}; `agents:{agentId:{...}}` merges agent-snapshot fields (prompt, models, selection, tools, disallowedTools, skills, extras, description), e.g. {agents:{\"builder\":{\"models\":[\"lg\"]}}} to give this launch's builder the lg pool, using the same grammar as the post-create snapshot patch. Address a node by its name or by the agent id it runs — recipe-file node ids are not stable and a token matching no node is refused, never skipped. Removing the trigger, the last agent node, or anything that would strand a node from the trigger is refused; a fence is not settable here",
);
pub(crate) const PARENT: KeySpec = KeySpec::new(
    "parent",
    KeyType::Str,
    "canonical issue URI (cairn://p/PROJECT/N) or thread URI (cairn://p/PROJECT/thread-name) of the parent: an issue parent also confers branch ancestry (the child branches from / PRs into it), a thread parent routes attention only; defaults to the thread the creating agent is acting for, and to unparented outside a thread",
);
pub(crate) const ISSUE_CAMP: KeySpec = KeySpec::new(
    "camp",
    KeyType::Str,
    "canonical camp URI (cairn://p/PROJECT/camps/SLUG) to join the issue to, separately from its parent; your thread must take part in the camp. An issue is in at most one camp; null takes it out",
);
pub(crate) const TODOS: KeySpec = KeySpec::new("todos", KeyType::Array, "");
pub(crate) const CONFIRMED: KeySpec = KeySpec::new(
    "confirmed",
    KeyType::Bool,
    "set true to confirm a gated artifact and advance the DAG; omit to edit data",
);
pub(crate) const DEFER: KeySpec = KeySpec::new(
    "defer",
    KeyType::Object,
    "{until:[gate clause, ...]} — defer a gated artifact pending on you until every clause holds, in the `after:` vocabulary (issue_resolution, pr_state, artifact_confirmed, check_verdict, clock, tenet_verdict, request); e.g. {until:[{fact:\"request\",request:\"cairn://requests/ID\",outcome:\"done\"}]}; you are woken once when it comes true or can no longer come true. Deferring again replaces the last deferral (mutually exclusive with confirmed and action)",
);
pub(crate) const PR_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "merge|close|refresh|publish — operate on the PR a PR artifact produced; publish pushes the branch's current head when the PR holds an older one (mutually exclusive with confirmed)",
);
pub(crate) const PR_METHOD: KeySpec = KeySpec::new(
    "method",
    KeyType::Str,
    "merge method for action:merge (default squash)",
);
pub(crate) const PR_PRECONDITION: KeySpec = KeySpec::new(
    "precondition",
    KeyType::Str,
    "pinBump — action:merge only: land only if the change is exactly an upstream tool-pin bump the base revision's `pinBumps` config describes, verified by its required checks on every required platform at this head (see docs/checks.md); any other value is refused",
);
pub(crate) const NODE_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "stop|retry|merge|close|refresh|publish — retry starts a fresh publication attempt cycle on a failed `pr` action node only (published nodes use refresh); stop interrupts the node's active turn and parks the session warm (resumable, not a kill; cascades to child runs); merge|close|refresh|publish operate on the PR a `pr` action node produced (mutually exclusive with confirmed)",
);
/// Dedicated to the attachment member, and deliberately not shared: no sibling
/// contract can copy a durable object into a cell.
pub(crate) const ATTACHMENT_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "materialize — copy this object into the calling run's own execution cell and return the cell-local path",
);

pub(crate) const REMOVAL_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "why it is being removed; recorded on the tombstone every address shows afterwards, and read by the operator before approving",
);
pub(crate) const REMOVAL_INCLUDE_SHARED: KeySpec = KeySpec::new(
    "includeShared",
    KeyType::Bool,
    "also remove every other live reference to the same bytes; without it, shared bytes are refused and the other references listed",
);

pub(crate) const REBASE_SOURCE_COMMIT: KeySpec = KeySpec::new(
    "source_commit", KeyType::Str,
    "rebuild only: full immutable ID of the branch's last clean source commit; its conflicted rewrite and all later deltas must be provable",
);
pub(crate) const REBASE_SOURCE_BASE: KeySpec = KeySpec::new(
    "source_base", KeyType::Str,
    "rebuild only: owner-attested full immutable original branch cut; source_base..source_commit is deliberately declared to be this branch's own delta, excluding inherited parent changes (never inferred from today's merge base)",
);
pub(crate) const REBASE_SOURCE_BASE_REASON: KeySpec = KeySpec::new(
    "source_base_reason", KeyType::Str,
    "rebuild only: required provenance explaining why source_base is the original own-delta boundary rather than inherited parent work; recorded with the recovery",
);
pub(crate) const REBASE_EXPECTED_TIP: KeySpec = KeySpec::new(
    "expected_tip", KeyType::Str,
    "rebuild only: full immutable flagged tip ID observed by the owner; a moved or ambiguous bookmark is refused",
);
pub(crate) const REBASE_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "replay — replay onto the base (a branch in merge catch-up mode merges the base in instead); fast-forward-from-origin — take origin's copy of the branch (or of its base, with branch:\"base\") now, only when it is a pure fast-forward; rebuild — rebuild a flagged branch onto its current base using explicit source_commit, owner-attested source_base, source_base_reason and expected_tip, pinning the flagged tip in a recovery ref and refusing unprovable later edits; refused while a run batch can still publish to the branch or while its parent's conflict session is open; repair — retry now the targeted repair of a recovery fence on this branch (only the recorded bookmarks and Git heads move); catch-up-mode — set (mode:\"merge\") or end (mode:\"replay\") this branch's merge catch-up mode, on the operator's approval; mark-reviewed — record that the owner reviewed the base's incoming changes through `through`; dismiss-acceptance — the owning node's repair for an `unknown` acceptance this page lists (needs acceptance_id, expected_head and reason, and nothing else): retires the row as dismissed and moves, exports and publishes nothing",
);
pub(crate) const REBASE_ACCEPTANCE_ID: KeySpec = KeySpec::new(
    "acceptance_id",
    KeyType::Str,
    "dismiss-acceptance only: the id of an `unknown` acceptance of this node's branch, as this page lists it; a pending row (it settles at the next write), an accepted row, or another branch's row is refused",
);
pub(crate) const REBASE_EXPECTED_HEAD: KeySpec = KeySpec::new(
    "expected_head",
    KeyType::Str,
    "dismiss-acceptance only: the branch's full current commit id as this page shows it; refused unless it is exactly the head read under the branch's reservation and the store lock (a prefix or a name is refused)",
);
pub(crate) const REBASE_DISMISS_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "dismiss-acceptance only: why retiring the row is correct, recorded with it. The row's drop claims were never proven and are discarded, not accepted; a later publication must still account for every commit only origin holds, and may still refuse",
);
pub(crate) const REBASE_CANCEL_HOLDING_BATCH: KeySpec = KeySpec::new(
    "cancel_holding_batch",
    KeyType::Bool,
    "replay only: first cancel the run batch holding this branch at the mutation boundary (it is validating a tip the replay supersedes), then request the replay; a batch already publishing is left to finish and its commit stands",
);
pub(crate) const BATCH_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "cancel — stop this one run batch: its machine work ends, a call waiting on it resumes once with a result saying who cancelled it, and it releases its branch; nothing else on the node is touched. A batch already publishing its result is left to finish, and its commit stands. Its owning node, a node sharing its branch, or the operator may cancel it",
);
pub(crate) const REBASE_CATCH_UP_MODE: KeySpec = KeySpec::new(
    "mode",
    KeyType::Str,
    "catch-up-mode only: merge — catch up with the base by one merge commit, never a replay; replay — end the mode",
);
pub(crate) const REBASE_REVIEW_PATHS: KeySpec = KeySpec::new(
    "review_paths",
    KeyType::Array,
    "catch-up-mode merge only: path globs (`*` within a directory, `**` across, a trailing `/` for a whole directory) whose base-side changes the owner must review before the branch lands",
);
pub(crate) const REBASE_REVIEWED_THROUGH: KeySpec = KeySpec::new(
    "reviewed_through",
    KeyType::Str,
    "catch-up-mode merge only: the base commit the owner has reviewed through; defaults to the current merge base",
);
pub(crate) const REBASE_THROUGH: KeySpec = KeySpec::new(
    "through",
    KeyType::Str,
    "mark-reviewed only: the base commit reviewed through; it must be in the branch and the default branch, at or after the current mark",
);
pub(crate) const REBASE_FINGERPRINT: KeySpec = KeySpec::new(
    "fingerprint",
    KeyType::Str,
    "the session fingerprint you read; a request naming a stale one is refused with a fresh diagnostic",
);
pub(crate) const UPDATES: KeySpec = KeySpec::new("updates", KeyType::Array, "");
pub(crate) const SKILL_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");

/// Dedicated to the pack member: `install` materializes an available pack,
/// `update` re-syncs an installed one. Deliberately not shared with
/// `NODE_ACTION`/`PR_ACTION`, which no sibling contract can perform.
pub(crate) const PACK_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "install | update | restore | export (requires path). install takes a bundled pack or an \
     entry the Cairn library offers; an update that changes what runs on this machine is \
     refused here and applied by the operator from the Library page",
);
pub(crate) const PACK_PATH: KeySpec =
    KeySpec::new("path", KeyType::Str, "local Agent Plugin directory");
pub(crate) const PACK_ITEM_KIND: KeySpec = KeySpec::new(
    "kind",
    KeyType::Str,
    "required for reset-item: which kind of packed item to restore",
);
pub(crate) const PACK_ITEM_ID: KeySpec = KeySpec::new(
    "itemId",
    KeyType::Str,
    "required for reset-item: the packed item to restore to its shipped form",
);
pub(crate) const SKILL_PROMPT: KeySpec = KeySpec::new("prompt", KeyType::Str, "SKILL.md body");
pub(crate) const SKILL_ALLOWED_TOOLS: KeySpec = KeySpec::new(
    "allowedTools",
    KeyType::Array,
    "tool names this skill is allowed to use; empty clears the restriction",
);
pub(crate) const SKILL_SOURCE_ISSUE: KeySpec = KeySpec::new(
    "sourceIssue",
    KeyType::Str,
    "issue this skill came from; defaults to the writing run's issue",
);
pub(crate) const SKILL_APPEND_TO_PROMPT: KeySpec = KeySpec::with_aliases(
    "appendToPrompt",
    &["append_to_prompt"],
    KeyType::Str,
    "append to SKILL.md instead of replacing it",
);
pub(crate) const SKILL_REPLACE_SECTION: KeySpec = KeySpec::with_aliases(
    "replaceSection",
    &["replace_section"],
    KeyType::Object,
    "{heading, content} — replace one SKILL.md section in place",
);
pub(crate) const MEMORY_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "short display handle; not used for identity",
);
pub(crate) const MEMORY_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "project | role | workspace; backend resolves scope_value",
);
pub(crate) const MEMORY_STATUS: KeySpec = KeySpec::new(
    "status",
    KeyType::Str,
    "draft | pending | claimed | promoted | discarded | deferred",
);
pub(crate) const MEMORY_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "promote | discard | defer — reasoned triage decision for claimed memories",
);
pub(crate) const MEMORY_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "why this triage decision is correct",
);
pub(crate) const MEMORY_NEW_SCOPE: KeySpec = KeySpec::with_aliases(
    "newScope",
    &["new_scope"],
    KeyType::Object,
    "optional for defer: {scope,value} (project key or role name); re-pools as pending",
);
pub(crate) const LABEL_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the label id",
);
pub(crate) const LABEL_COLOR: KeySpec = KeySpec::new(
    "color",
    KeyType::Str,
    "#RRGGBB; deterministic palette color when omitted",
);
pub(crate) const LABELS: KeySpec = KeySpec::new(
    "labels",
    KeyType::Array,
    "full replacement label refs by name or slug; a name the workspace vocabulary lacks is created",
);
pub(crate) const SUBAGENT_TYPE: KeySpec = KeySpec::with_aliases(
    "subagentType",
    &["subagent_type"],
    KeyType::Str,
    "one of the Available Agents listed above",
);
pub(crate) const TASK_DESCRIPTION: KeySpec = KeySpec::new(
    "description",
    KeyType::Str,
    "short title for what this task is",
);
pub(crate) const QUESTIONS: KeySpec = KeySpec::new("questions", KeyType::Array, "");
pub(crate) const ANSWER: KeySpec =
    KeySpec::new("answer", KeyType::Str, "single-question shorthand answer");
pub(crate) const PERMISSION_DECISION: KeySpec =
    KeySpec::new("decision", KeyType::Str, "allow|deny");
pub(crate) const PERMISSION_SCOPE: KeySpec =
    KeySpec::new("scope", KeyType::Str, "once|session (default once)");
pub(crate) const PERMISSION_LIFETIME: KeySpec = KeySpec::new(
    "lifetime",
    KeyType::Str,
    "how long an allow decision grants authority for, beyond this one answer",
);
pub(crate) const PERMISSION_EXPIRES_IN_SECONDS: KeySpec = KeySpec::new(
    "expiresInSeconds",
    KeyType::Int,
    "explicit expiry for an allow decision, in seconds from now",
);
pub(crate) const PERMISSION_MESSAGE: KeySpec = KeySpec::new(
    "message",
    KeyType::Str,
    "a deny's reason or workaround, delivered to the requesting agent with the refusal (deny only)",
);
pub(crate) const ANSWERS: KeySpec = KeySpec::new(
    "answers",
    KeyType::Array,
    "indexed answers for one or more questions; each item is {index(int), and exactly one of selection(str option label) | selections(array of labels, for multiSelect) | text(str, free-form/'Other')}; a bare string item is shorthand for {index:<position>, text:<string>}",
);
pub(crate) const BUG_CATEGORY: KeySpec = KeySpec::new(
    "category",
    KeyType::Str,
    "tool_bug|prompt_issue|harness_friction|suggestion",
);
pub(crate) const RECIPE_CONTENT: KeySpec = KeySpec::new(
    "content",
    KeyType::Str,
    "recipe YAML body (cairnVersion, name, trigger, nodes, edges); validated like the file loader",
);
pub(crate) const RECIPE_ID: KeySpec = KeySpec::new(
    "id",
    KeyType::Str,
    "filename id; defaults to slugify(name from the YAML)",
);
pub(crate) const RECIPE_OLD_STRING: KeySpec = KeySpec::new(
    "old_string",
    KeyType::Str,
    "exact text in the recipe YAML source to replace (targeted edit; pair with new_string)",
);
pub(crate) const RECIPE_NEW_STRING: KeySpec = KeySpec::new(
    "new_string",
    KeyType::Str,
    "replacement for old_string; the resulting YAML is re-validated like the file loader",
);
pub(crate) const RECIPE_REPLACE_ALL: KeySpec = KeySpec::new(
    "replace_all",
    KeyType::Bool,
    "replace every occurrence of old_string instead of requiring a unique match",
);
pub(crate) const DELETE_REASON: KeySpec =
    KeySpec::new("reason", KeyType::Str, "why it was removed");
pub(crate) const AGENT_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "display name; slugified into the agent id",
);
pub(crate) const AGENT_PROMPT: KeySpec = KeySpec::new(
    "prompt",
    KeyType::Str,
    "agent system prompt (markdown body)",
);
pub(crate) const AGENT_TOOLS: KeySpec =
    KeySpec::new("tools", KeyType::Array, "tool names; at least one required");
pub(crate) const AGENT_MODELS: KeySpec = KeySpec::new(
    "models",
    KeyType::Array,
    "the allotment a node running this agent selects its model within: pool ids (sm, md, lg as cairn://models/ladder defines them) and provider/model pairs, e.g. [lg] or [claude/opus]; omitted means the workspace default",
);
pub(crate) const AGENT_FENCE: KeySpec =
    KeySpec::new("fence", KeyType::Str, "deny | ask (default) | allow");
pub(crate) const AGENT_DISALLOWED: KeySpec = KeySpec::with_aliases(
    "disallowedTools",
    &["disallowed_tools"],
    KeyType::Array,
    "tools to block",
);
pub(crate) const AGENT_SKILLS: KeySpec =
    KeySpec::new("skills", KeyType::Array, "skill ids to inject");
pub(crate) const AGENT_ICON: KeySpec =
    KeySpec::new("icon", KeyType::Str, "icon name shown beside the agent");
pub(crate) const AGENT_HOOKS: KeySpec = KeySpec::new(
    "hooks",
    KeyType::Any,
    "lifecycle hook configuration, stored verbatim",
);
/// Superseded by `fence`, still read so existing agent definitions keep working.
pub(crate) const AGENT_SANDBOX: KeySpec = KeySpec::new(
    "sandbox",
    KeyType::Str,
    "legacy, use fence: workspace-write | read-only | danger-full-access",
);
/// Superseded by `fence`, still read so existing agent definitions keep working.
pub(crate) const AGENT_ON_ESCAPE: KeySpec = KeySpec::with_aliases(
    "onEscape",
    &["on_escape"],
    KeyType::Str,
    "legacy, use fence: deny | ask | allow",
);
pub(crate) const ACTION_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "");
pub(crate) const ACTION_COMMAND: KeySpec = KeySpec::with_aliases(
    "commandTemplate",
    &["command_template"],
    KeyType::Str,
    "shell template with {{var:type}} placeholders",
);
pub(crate) const ACTION_INPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "inputSchema",
    &["input_schema"],
    KeyType::Object,
    "JSON Schema; derived from the template when omitted",
);
pub(crate) const ACTION_OUTPUT_SCHEMA: KeySpec = KeySpec::with_aliases(
    "outputSchema",
    &["output_schema"],
    KeyType::Object,
    "JSON Schema for the action output",
);

// --- external MCP server registry (cairn://mcp write CRUD) ---
pub(crate) const MCP_NAME: KeySpec = KeySpec::new(
    "name",
    KeyType::Str,
    "server key under mcpServers (the <server> segment in cairn://mcp/<server>)",
);
pub(crate) const MCP_TYPE: KeySpec =
    KeySpec::new("type", KeyType::Str, "stdio (default) | http | sse");
pub(crate) const MCP_COMMAND: KeySpec =
    KeySpec::new("command", KeyType::Str, "stdio: program to spawn");
pub(crate) const MCP_ARGS: KeySpec =
    KeySpec::new("args", KeyType::Array, "stdio: command arguments");
pub(crate) const MCP_ENV: KeySpec = KeySpec::new(
    "env",
    KeyType::Object,
    "stdio: environment variables; values may use ${VAR} references (no plaintext secrets)",
);
pub(crate) const MCP_URL: KeySpec = KeySpec::new("url", KeyType::Str, "http/sse: server URL");
pub(crate) const MCP_HEADERS: KeySpec = KeySpec::new(
    "headers",
    KeyType::Object,
    "http/sse: per-request headers; values may use ${VAR} references (no plaintext secrets)",
);
pub(crate) const MCP_ENABLED: KeySpec =
    KeySpec::new("enabled", KeyType::Bool, "expose to agents (default true)");
pub(crate) const MCP_OAUTH: KeySpec = KeySpec::new(
    "oauth",
    KeyType::Object,
    "non-secret OAuth block only: {clientId?, scopes?}; tokens and client secrets live in the keychain and are never set here",
);
/// Dedicated to the MCP server member: an operation on the calling run's
/// connection, never a registry edit, so it shares nothing with the config keys.
pub(crate) const MCP_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "restart — drop the calling run's pooled connection to this server (stopping its stdio process) so the next call reconnects; for a server that is alive but not answering. Edits no configuration",
);
pub(crate) const MCP_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "workspace (default; ~/.cairn/settings.yaml, gated by the worktree fence) | project (the run's .cairn/config.yaml)",
);

// --- fleet management (cairn://executors write) ---
pub(crate) const EXECUTOR_HOST: KeySpec = KeySpec::new(
    "host",
    KeyType::Str,
    "hostname or address ssh reaches the machine at",
);
pub(crate) const EXECUTOR_SSH_USER: KeySpec = KeySpec::with_aliases(
    "sshUser",
    &["ssh_user"],
    KeyType::Str,
    "the user ssh authenticates as",
);
pub(crate) const EXECUTOR_PROJECT_KEYS: KeySpec = KeySpec::with_aliases(
    "projectKeys",
    &["project_keys"],
    KeyType::Array,
    "project keys this machine serves; omitted or empty means every project",
);
pub(crate) const EXECUTOR_BINARY_PATH: KeySpec = KeySpec::with_aliases(
    "binaryPath",
    &["binary_path"],
    KeyType::Str,
    "absolute remote path for the executor binary; derived per platform when omitted",
);
pub(crate) const EXECUTOR_CAIRN_HOME: KeySpec = KeySpec::with_aliases(
    "cairnHome",
    &["cairn_home"],
    KeyType::Str,
    "absolute remote CAIRN_HOME; derived per platform when omitted",
);
pub(crate) const EXECUTOR_ID: KeySpec = KeySpec::with_aliases(
    "executorId",
    &["executor_id"],
    KeyType::Str,
    "stable internal identity; derived from the host when omitted",
);
pub(crate) const EXECUTOR_DEVICE_ID: KeySpec = KeySpec::with_aliases(
    "deviceId",
    &["device_id"],
    KeyType::Str,
    "enrollment device identity; derived from the executor identity when omitted",
);
pub(crate) const EXECUTOR_DISPLAY_NAME: KeySpec = KeySpec::with_aliases(
    "displayName",
    &["display_name"],
    KeyType::Str,
    "the public name placement requests address this machine by; derived from the host when omitted",
);
pub(crate) const EXECUTOR_EXTRA_SSH_ARGS: KeySpec = KeySpec::with_aliases(
    "extraSshArgs",
    &["extra_ssh_args"],
    KeyType::Array,
    "transport-only ssh arguments; -4 or -6",
);
pub(crate) const EXECUTOR_NEW_NAME: KeySpec = KeySpec::with_aliases(
    "newName",
    &["new_name"],
    KeyType::Str,
    "new public name; configuration, enrollment claim, and supervision move together",
);
pub(crate) const EXECUTOR_RUNTIME_POLICY: KeySpec = KeySpec::with_aliases(
    "runtimePolicy",
    &["runtime_policy"],
    KeyType::Object,
    "{memoryBudgetBytes, diskGrowthBudgetBytes, freeDiskWatermarkBytes, concurrencyUnits, maximumQueueDepth}",
);
pub(crate) const EXECUTOR_DRAINING: KeySpec = KeySpec::new(
    "draining",
    KeyType::Bool,
    "true imposes the authenticated owning thread\'s record; false clears only that thread\'s record and imported legacy state. Other owners and fences remain; operator lifting requires accepted lift_drain. No live connection generation required",
);
pub(crate) const EXECUTOR_DRAIN_REASON: KeySpec = KeySpec::new(
    "reason",
    KeyType::Str,
    "optional drain reason; omission stores exactly no reason supplied",
);
pub(crate) const EXECUTOR_ROLE: KeySpec = KeySpec::new(
    "role",
    KeyType::Str,
    "buildsOnly | general | inferenceOnly | servicesOnly: what this machine is for; omitted means general (or, re-enrolling, the role it holds). Durable runner-side state like draining: unfenced, works on an unreachable machine, and the machine is never told. inferenceOnly is never offered any batch class. servicesOnly takes service residents only (no run batch, check lane, REPL, terminal, dev instance or inference deployment), is ranked first for services, and is read as always on",
);
pub(crate) const EXECUTOR_BUILD_ROOT: KeySpec = KeySpec::new(
    "buildRoot",
    KeyType::Str,
    "local only: an existing directory on another volume where the runner's own executor keeps its cells, managed objects, dependency catalog and build caches; null restores the home. Written while the volume is mounted (it gets a marker), and takes effect at the next runner start. While the volume is unplugged the machine stays attached and declines work that needs a cell, and it adopts its cells again when the volume returns",
);
pub(crate) const EXECUTOR_MEMORY_DIAGNOSTIC: KeySpec = KeySpec::new(
    "memoryDiagnostic", KeyType::Object,
    "development local executor only: {runId,durationMs} attaches one measurement-only comparison to a currently registered runner process for 1..=60000 ms; null detaches. Requires expectedGeneration. Cannot be combined with other configuration. Reads never attach or sample",
);
pub(crate) const EXECUTOR_EXPECTED_GENERATION: KeySpec = KeySpec::with_aliases(
    "expectedGeneration",
    &["expected_generation"],
    KeyType::Int,
    "the connection generation you read; required with runtimePolicy or memoryDiagnostic",
);
pub(crate) const EXECUTOR_REMOVAL_OPERATION_ID: KeySpec = KeySpec::new(
    "operationId", KeyType::Str,
    "caller-retained unique removal ID; reuse this and the frozen stable executor ID after an ambiguous failure to reconcile its durable receipt without repeating removal",
);
pub(crate) const EXECUTOR_REMOVAL_CONFIRM: KeySpec = KeySpec::new(
    "confirm",
    KeyType::Bool,
    "remove even though the machine still counts work: what the counts describe is stranded and its lanes re-arm elsewhere",
);

// Empty mutation set, named for readability.
pub(crate) const NO_MUTATIONS: &[MutationSpec] = &[];
pub(crate) const NO_PROJECTIONS: &[ProjectionSpec] = &[];

/// Browser reads accept a content format, a native screenshot, the page's
/// captured runtime buffers (console/network), or its actionable elements
/// (interactive). The screenshot/console/network/interactive facets are
/// mutually exclusive; the screenshot is a host-native capture returned as an
/// image block (works even on about:blank). Content reads page like any
/// resource via ?offset/?limit.
pub(crate) const BROWSER_READ_PROJECTIONS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "format",
        values: "markdown (default) | text — live page content",
    },
    ProjectionSpec {
        key: "screenshot",
        values: "(no value) — a PNG screenshot of the rendered page, returned as an image",
    },
    ProjectionSpec {
        key: "console",
        values: "(no value) — the page's captured console output + uncaught errors; optional &limit=N",
    },
    ProjectionSpec {
        key: "network",
        values: "(no value) — the page's captured fetch/XHR request summaries; optional &limit=N",
    },
    ProjectionSpec {
        key: "interactive",
        values: "(no value) — actionable elements as durable handles (e1..eN) with descriptor + selector, for click/type/scroll by handle; optional &limit=N",
    },
    ProjectionSpec {
        key: "offset",
        values: "N — line window into a long content read (follows the continue: footer)",
    },
    ProjectionSpec {
        key: "limit",
        values: "N — line-window size for content (a buffer/element cap for ?console/?network/?interactive)",
    },
];
/// Terminal reads (node, task, and project scoped) share one parser,
/// `parse_terminal_uri` in cairn-core; the universal offset/limit/grep view
/// grammar rides alongside.
pub(crate) const TERMINAL_PROJECTIONS: &[ProjectionSpec] = &[ProjectionSpec {
    key: "new",
    values: "true — only the bytes appended since your last new=true read, advancing a cursor (default: the whole buffer, idempotent)",
}];
pub(crate) const NO_RELATED: &[RelatedSpec] = &[];
pub(crate) const NO_CROSS_ACTIONS: &[CrossActionSpec] = &[];
// Shared read-query projections for the symbol resources (node- and project-scoped).
pub(crate) const SYMBOLS_PROJECTIONS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "op",
        values: "definition|references|callers|implementations (absent = overview: definition site + signature + reference count)",
    },
    ProjectionSpec {
        key: "in",
        values: "GLOB — scope navigation to a path subtree",
    },
];
// A recipe is the `{recipe}` input to starting an execution; that `append`
// mutation lives on the executions resource, so surface it as a cross-action.
pub(crate) const RECIPE_CROSS_ACTIONS: &[CrossActionSpec] = &[CrossActionSpec {
    kind: ResourceKind::IssueExecutions,
    mode: ChangeMode::Append,
    label: "start an execution with this recipe",
}];
pub(crate) const PROJECT_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "issues",
        kind: ResourceKind::ProjectIssues,
        actions: true,
    },
    RelatedSpec {
        label: "threads",
        kind: ResourceKind::ProjectThreads,
        actions: false,
    },
    RelatedSpec {
        label: "messages",
        kind: ResourceKind::ProjectMessages,
        actions: false,
    },
    RelatedSpec {
        label: "labels",
        kind: ResourceKind::Labels,
        actions: true,
    },
];
pub(crate) const PROJECT_CHILD_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Project,
    actions: false,
}];
pub(crate) const ISSUE_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "messages",
        kind: ResourceKind::IssueMessages,
        actions: true,
    },
    RelatedSpec {
        label: "comments",
        kind: ResourceKind::IssueComments,
        actions: true,
    },
    RelatedSpec {
        label: "changed",
        kind: ResourceKind::Changed,
        actions: false,
    },
];
pub(crate) const ISSUE_COMMENTS_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "up",
        kind: ResourceKind::Issue,
        actions: false,
    },
    // Surface the member's edit/delete in the collection's affordance block so a
    // reader of /comments discovers how to act on a specific comment.
    RelatedSpec {
        label: "comment",
        kind: ResourceKind::IssueComment,
        actions: true,
    },
];
pub(crate) const ISSUE_COMMENT_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::IssueComments,
    actions: false,
}];
pub(crate) const ISSUE_MESSAGES_RELATED: &[RelatedSpec] = &[
    RelatedSpec {
        label: "up",
        kind: ResourceKind::Issue,
        actions: false,
    },
    RelatedSpec {
        label: "changed",
        kind: ResourceKind::Changed,
        actions: false,
    },
];
pub(crate) const NODE_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::NodeMessages,
    actions: true,
}];
pub(crate) const NODE_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Node,
    actions: true,
}];
pub(crate) const NODE_PROGRESS_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Node,
    actions: true,
}];
pub(crate) const TASK_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "messages",
    kind: ResourceKind::TaskMessages,
    actions: true,
}];
pub(crate) const TASK_MESSAGES_RELATED: &[RelatedSpec] = &[RelatedSpec {
    label: "up",
    kind: ResourceKind::Task,
    actions: true,
}];

// --- workspace settings (cairn://settings patch) ---
pub(crate) const SETTINGS_MODELS: KeySpec = KeySpec::new(
    "models",
    KeyType::Object,
    "pools, ladder and defaultModels, replaced whole: {pools: [{id, description, band: {min}} | {id, description, members: [provider/model]}], ladder: [{model: provider/model, level, effort, options}], defaultModels: [md]}; read the current value at cairn://models/ladder",
);
pub(crate) const SETTINGS_ENABLED_PROVIDERS: KeySpec = KeySpec::new(
    "enabledProviders",
    KeyType::Array,
    "providers installed in this workspace, e.g. [claude, openrouter]; refused when it would strand call routing",
);
pub(crate) const SETTINGS_DECISION_MODEL: KeySpec = KeySpec::new(
    "decisionModel",
    KeyType::Str,
    "the pool or provider/model a decision node naming no models is answered within; null returns it to the catalog's choice",
);
pub(crate) const SETTINGS_DEFAULT_RECIPE: KeySpec = KeySpec::new(
    "defaultRecipe",
    KeyType::Str,
    "the recipe id an execution naming no recipe starts (execution:true on issue create, an executions append without recipe); null clears it, and such a start is then refused",
);
pub(crate) const SETTINGS_IMAGE_MODEL: KeySpec = KeySpec::new(
    "imageModel",
    KeyType::Str,
    "image model a request naming none is drawn by; null clears it, and such a request is then refused",
);
pub(crate) const SETTINGS_SPEECH_MODEL: KeySpec = KeySpec::new(
    "speechModel",
    KeyType::Str,
    "speech model a request naming none is spoken by; null clears it, and such a request is then refused",
);
pub(crate) const SETTINGS_EMBEDDING_MODEL: KeySpec = KeySpec::new(
    "embeddingModel",
    KeyType::Str,
    "provider/model the workspace embeds with; changing it re-embeds history; null is cairn/cohere-embed-v4",
);
pub(crate) const SETTINGS_MERGE_TYPE: KeySpec =
    KeySpec::new("mergeType", KeyType::Str, "squash|merge|rebase");
pub(crate) const SETTINGS_MEMORY_REVIEW_ENABLED: KeySpec = KeySpec::new(
    "memoryReviewEnabled",
    KeyType::Bool,
    "enable memory review prompts and automatic triage",
);
pub(crate) const SETTINGS_GIT_IDENTITIES: KeySpec = KeySpec::new(
    "gitIdentities",
    KeyType::Object,
    "{add[{label,name,email}], update[{id,label?,name?,email?}], remove[ids], order[ids]}",
);
pub(crate) const SETTINGS_ACCOUNTS: KeySpec = KeySpec::new(
    "accounts",
    KeyType::Object,
    "{add[{provider,label,authType,authValue?}], update[{id,label}], remove[ids], order{provider,ids}} (api_key|oauth_token|base_url only; Claude sign-in stays UI-only)",
);
pub(crate) const SETTINGS_KEYBINDS: KeySpec = KeySpec::new(
    "keybinds",
    KeyType::Object,
    "{set[{action,key,modifiers}], reset[actions], resetAll?}",
);
pub(crate) const SETTINGS_BUILD_SERVICES: KeySpec = KeySpec::new(
    "buildServices",
    KeyType::Object,
    "{upsert[{name,config}], setEnabled[{name,enabled}], remove[names]}",
);
pub(crate) const SETTINGS_INFERENCE: KeySpec = KeySpec::new(
    "inference",
    KeyType::Object,
    "{profiles?{profileId:{verbs?[complete|session|decide|embed|image|speak], inputModalities?, outputModalities?}}} replaces the section; profiles declare what an inference profile's model serves and takes, over the record; the records themselves are written at cairn://inference",
);
pub(crate) const SETTINGS_SERVED_API: KeySpec = KeySpec::new(
    "servedApi",
    KeyType::Object,
    "{bind?:\"addr:port\", tls?:{certPath,keyPath}, loadWaitSecs?}|null replaces the section; the OpenAI-compatible listener for outside clients (off without bind; a restart applies a changed bind), and how long a request waits for a local model to load; clients name models by their real names; client keys live at cairn://models/clients",
);
pub(crate) const SETTINGS_MAX_THINKING_TOKENS: KeySpec = KeySpec::new(
    "maxThinkingTokens",
    KeyType::Int,
    "thinking budget per turn; null restores the backend default",
);
pub(crate) const SETTINGS_ORPHAN_CLEANUP_DAYS: KeySpec = KeySpec::new(
    "orphanCleanupDays",
    KeyType::Int,
    "days before orphaned worktrees are swept",
);
pub(crate) const SETTINGS_REPO_TARGET_SWEEP_DAYS: KeySpec = KeySpec::new(
    "repoTargetSweepDays",
    KeyType::Int,
    "days before stale build artifacts in project checkouts are swept; 0 disables",
);
pub(crate) const SETTINGS_BUG_REPORTS: KeySpec = KeySpec::new(
    "bugReports",
    KeyType::Bool,
    "whether agents may file bug reports",
);
pub(crate) const SETTINGS_THINKING_DISPLAY_MODE: KeySpec = KeySpec::new(
    "thinkingDisplayMode",
    KeyType::Str,
    "how thinking blocks render in transcripts",
);
pub(crate) const SETTINGS_TRANSCRIPT_TEXT_SIZE: KeySpec = KeySpec::new(
    "transcriptTextSize",
    KeyType::Str,
    "base text scale for transcript markdown",
);
pub(crate) const SETTINGS_TRANSCRIPT_DENSITY: KeySpec = KeySpec::new(
    "transcriptDensity",
    KeyType::Str,
    "vertical rhythm preset for transcript markdown",
);
pub(crate) const SETTINGS_LOG_LEVEL: KeySpec =
    KeySpec::new("logLevel", KeyType::Str, "file-log verbosity");
pub(crate) const SETTINGS_LOG_RETENTION_DAYS: KeySpec = KeySpec::new(
    "logRetentionDays",
    KeyType::Int,
    "days of daily JSONL logs to retain",
);
pub(crate) const SETTINGS_MEMORY_TRIAGE_ENABLED: KeySpec = KeySpec::new(
    "memoryTriageEnabled",
    KeyType::Bool,
    "whether memory-triage issues are created automatically",
);
pub(crate) const SETTINGS_MAX_OPEN_TRIAGE_ISSUES: KeySpec = KeySpec::new(
    "maxOpenTriageIssuesPerScope",
    KeyType::Int,
    "cap on simultaneously open memory-triage issues for one exact scope",
);
pub(crate) const SETTINGS_MAX_CONCURRENT_EXECUTIONS: KeySpec = KeySpec::new(
    "maxConcurrentExecutions",
    KeyType::Int,
    "executions in progress across this runner's fleet before new starts hold for a slot (minimum 1)",
);
pub(crate) const SETTINGS_MAX_OPEN_CHILDREN_PER_THREAD: KeySpec = KeySpec::new(
    "maxOpenChildrenPerThread",
    KeyType::Int,
    "open started children a thread may have before its new starts hold (minimum 1)",
);
pub(crate) const SETTINGS_PENDING_MEMORY_THRESHOLD: KeySpec = KeySpec::new(
    "pendingMemoryThreshold",
    KeyType::Int,
    "pending memories in one scope that trigger a triage issue",
);
pub(crate) const SETTINGS_THREAD_COMPACT_THRESHOLD: KeySpec = KeySpec::new(
    "threadCompactThreshold",
    KeyType::Float,
    "fraction of the context window at which a warm thread session rebuilds",
);
pub(crate) const SETTINGS_SEED_BUDGET_TOKENS: KeySpec = KeySpec::new(
    "seedBudgetTokens",
    KeyType::Int,
    "approximate token budget for thread seed context below its intent and journal",
);
pub(crate) const SETTINGS_SEED_ERAS: KeySpec = KeySpec::new(
    "seedEras",
    KeyType::Int,
    "number of recent era summaries rendered into a thread seed",
);
pub(crate) const SETTINGS_EXTERNAL_REPLIES: KeySpec = KeySpec::new(
    "externalReplies",
    KeyType::Str,
    "policy for replies addressed to an external correspondent",
);
pub(crate) const SETTINGS_OPENROUTER_ROUTING: KeySpec = KeySpec::new(
    "openrouterRouting",
    KeyType::Object,
    "OpenRouter provider-routing controls; replaces the whole object",
);
pub(crate) const SETTINGS_ROUTE_CALLS_VIA_OPENROUTER: KeySpec = KeySpec::new(
    "routeCallsViaOpenRouter",
    KeyType::Bool,
    "select ephemeral calls among the OpenRouter models their allotment holds, instead of a native backend; a call whose allotment holds none is refused",
);
pub(crate) const SETTINGS_PRIVATE_DEDICATED_PROCESS_CAPACITY: KeySpec = KeySpec::new(
    "privateDedicatedProcessCapacity",
    KeyType::Int,
    "operator ceiling on resident private-session backend processes; 0 clears it",
);
pub(crate) const SETTINGS_CHANNELS: KeySpec = KeySpec::new(
    "channels",
    KeyType::Object,
    "workspace external delivery policy; replaces the whole object",
);

// --- projects collection + project lifecycle ---
pub(crate) const PROJECT_KEY: KeySpec =
    KeySpec::new("key", KeyType::Str, "uppercase project key (issue prefix)");
pub(crate) const PROJECT_CREATE_NAME: KeySpec = KeySpec::new("name", KeyType::Str, "display name");
pub(crate) const PROJECT_REPO_PATH: KeySpec = KeySpec::with_aliases(
    "repoPath",
    &["repo_path"],
    KeyType::Str,
    "absolute path to the local git repo",
);
pub(crate) const PROJECT_DEFAULT_BRANCH: KeySpec = KeySpec::with_aliases(
    "defaultBranch",
    &["default_branch"],
    KeyType::Str,
    "default branch (default main)",
);
pub(crate) const PROJECT_TEAM_ID: KeySpec = KeySpec::with_aliases(
    "teamId",
    &["team_id"],
    KeyType::Str,
    "route this project to a team's shared database (default: local/private)",
);
pub(crate) const PROJECT_HIDDEN: KeySpec =
    KeySpec::new("hidden", KeyType::Bool, "hide/unhide the project");
pub(crate) const PROJECT_REMOTE_URL: KeySpec = KeySpec::with_aliases(
    "remoteUrl",
    &["remote_url"],
    KeyType::Str,
    "attach this git remote as origin",
);
pub(crate) const PROJECT_ACTION: KeySpec = KeySpec::new(
    "action",
    KeyType::Str,
    "export (requires path): write every row the project owns to a self-contained local archive, archived transcript events reconstructed to plain content; holds no transaction across the export (each table's rows are selected up front, then read in short pages); until a production-copy run confirms it, export a cairn-sized project from an APFS clone rather than the live instance",
);
pub(crate) const PROJECT_EXPORT_PATH: KeySpec = KeySpec::new(
    "path",
    KeyType::Str,
    "absolute local directory for action:export, new or empty; receives manifest.json (written last) and tables/<table>.jsonl",
);
pub(crate) const PROJECT_REF_STORE_PRUNE: KeySpec = KeySpec::with_aliases(
    "refStorePrune",
    &["ref_store_prune"],
    KeyType::Bool,
    "true confirms this machine's ref-store prune (the one-time confirmation a store over the size thresholds needs before its first prune, so only with the operator's say-so); false opts the store out: measured, never pruned",
);

// --- project settings ---
pub(crate) const PS_SETUP_COMMANDS: KeySpec =
    KeySpec::with_aliases("setupCommands", &["setup_commands"], KeyType::Array, "");
pub(crate) const PS_TERMINAL_COMMANDS: KeySpec = KeySpec::with_aliases(
    "terminalCommands",
    &["terminal_commands"],
    KeyType::Array,
    "[{name,command}]",
);
pub(crate) const PS_MATERIALIZATION_POPULATE: KeySpec = KeySpec::with_aliases(
    "materializationPopulate",
    &["materialization_populate"],
    KeyType::Object,
    "{copy[],symlink[]} gitignored-path populate rules",
);
pub(crate) const PS_ACCOUNT_OVERRIDES: KeySpec = KeySpec::with_aliases(
    "accountOverrides",
    &["account_overrides"],
    KeyType::Object,
    "per-project identity/account overrides; null clears",
);
pub(crate) const PS_REFERENCES: KeySpec = KeySpec::new(
    "references",
    KeyType::Object,
    "{add[{name, git|path, description?, branch?}], remove[names], refresh[names]}",
);
pub(crate) const PS_CHECKS: KeySpec = KeySpec::new(
    "checks",
    KeyType::Object,
    "{name: {full, select?{mode,command,targetsFrom?}, impact?[], parse?, policy?, when?, deterministic?}}; empty object clears all",
);

// --- posts ---
pub(crate) const POST_CONTENT: KeySpec =
    KeySpec::new("content", KeyType::Str, "non-empty Markdown");
pub(crate) const POST_TITLE: KeySpec =
    KeySpec::new("title", KeyType::Str, "optional non-empty title");
pub(crate) const POST_SCOPE: KeySpec = KeySpec::new(
    "scope",
    KeyType::Str,
    "project or the authenticated caller's own project key; camp (with `camp`) addresses one camp of your project, reaching its participants by role",
);
pub(crate) const POST_CAMP: KeySpec = KeySpec::new(
    "camp",
    KeyType::Str,
    "with scope:\"camp\" only: canonical camp URI of your own project (cairn://p/PROJECT/camps/SLUG); your thread must take part in it, and every thread takes part in base camp",
);
pub(crate) const FEED_ACK: KeySpec = KeySpec::new(
    "ack",
    KeyType::Str,
    "the acknowledgement token this home's last feed read returned; a position cannot be named",
);
