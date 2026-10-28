//! Input and data types for the cairn-cmd MCP tools: the lenient `ChangeInput`
//! carrier, the `read`/`run` inputs, and the agent-info descriptor. Pure data
//! plus `validate_run_input`; no I/O.
use serde::{Deserialize, Serialize};

/// One change item as received from the MCP client.
///
/// Every field is optional and `skip_serializing_if` so serde never hard-rejects
/// a malformed item before `write()` runs: control always reaches our own
/// validator, which owns the friendly error text. The advertised contract (the
/// manual `JsonSchema` on `ChangeInput`) still marks `target`/`mode` required —
/// the schema guides the model; the lenient struct is the runtime gate.
///
/// Scope: `#[serde(default)]` only supplies the default when a field is *absent*
/// or null, so this disambiguates the absent/null `changes` case (the reported
/// `-32602` symptom). A present-but-wrong-typed `changes` (e.g. a string, or an
/// item that isn't an object) still fails rmcp deserialization before `write()`;
/// cairn-core's `handle_write` runs the same validator on the raw `Value` and
/// catches those shapes authoritatively.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeItemInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

/// Input for canonical change tool.
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
///
/// Fields are lenient (see `ChangeItemInput`). A genuinely-absent `changes`
/// stays absent on re-serialization (`skip_serializing_if`), so the validator
/// can emit a precise "required and not present" message instead of the opaque
/// rmcp `-32602 missing field 'changes'`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes: Option<Vec<ChangeItemInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atomic: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_markers_reason: Option<String>,
}

impl schemars::JsonSchema for ChangeInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ChangeInput".into()
    }
    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["changes"],
            "properties": {
                "changes": {
                    "description": "Ordered mutations to apply. By default, matching items apply and failures are reported per item; set atomic:true to stop at the first apply failure.",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["target", "mode"],
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "File URI like file:src/lib.rs (worktree-relative) or file:/abs/path, or a canonical cairn://p/... resource URI"
                            },
                            "mode": {
                                "type": "string",
                                "enum": ["create", "append", "patch", "unified_patch", "replace", "delete", "rename", "apply", "revert"],
                                "description": "Mutation mode. Use unified_patch for native *** Begin Patch envelopes on file: targets, rename for an ast-grep-backed structural identifier rename on a file: target, apply only with a single transcript event URI from a pending preview, and revert only as a sole bare-file: item naming an immutable commit. Unsupported target/mode pairs fail explicitly."
                            },
                            "payload": {
                                "type": "object",
                                "description": "Structured payload carrying this item's keys for file and resource targets alike. File targets: create/replace/append take {content}; patch takes {diff} OR {old_string, new_string} (optional {replace_all}); unified_patch takes {patch} containing a native *** Begin Patch envelope; delete needs no payload; rename takes {new_name, and exactly one of old_name | symbol_at}; revert takes exactly {commit}, where commit is the full immutable commit object ID. Revert requires bare file:, must be the sole change, always creates a new child commit, and does not support preview or atomic:false. Resource targets carry keys like {title} or {content}; read the target URI for its exact payload keys.",
                                "additionalProperties": true
                            }
                        }
                    }
                },
                "branch": {
                    "type": "string",
                    "description": "Commit file changes onto an existing managed branch, named by thread-branch entity URI (canonical or cairn:~/branches/N) or raw ref. This mutates and advances the ref; unlike run branch:, it is not verdict-only."
                },
                "commit_msg": {
                    "type": "string",
                    "description": "Git commit message. REQUIRED when the batch contains any file-target change (the edits are committed so they survive worktree cleanup); omit it for resource-only batches. Use '^' to commit with the previous commit's description instead of a new one; on an agent branch this always creates a new commit, and the previous commit is never rewritten in place. Without a commit_msg, a batch that dirties the worktree is restored to HEAD."
                },
                "preview": {
                    "type": "boolean",
                    "description": "When true, validate and compute the change report without applying side effects. Apply later with a single change item using mode=apply and the preview event URI."
                },
                "atomic": {
                    "type": "boolean",
                    "description": "Apply-phase atomicity opt-in. Default false applies every item whose anchor matches, reports per-item failures, and commits only files that applied. true preserves fail-fast behavior."
                },
                "conflict_markers_reason": {
                    "type": "string",
                    "description": "A short written reason for committing content that contains literal Git conflict markers (lines starting `<<<<<<<`/`=======`/`>>>>>>>`); without it such a commit is REFUSED. Only for a deliberate literal example in documentation or a test fixture; the reason is recorded with the commit."
                }
            }
        }))
        .unwrap()
    }
}

/// Input for the always-array read tool.
///
/// `paths` is a non-empty list of self-contained target URIs. All per-target
/// scoping (`offset`, `limit`, `glob`, `grep`, `issue_history`, `branch`) rides in each
/// URI's query string (e.g. `file:x.rs?offset=10&limit=20`,
/// `cairn:~/diff?view=patch&file=src/lib.rs`). There is no top-level offset/limit:
/// they are meaningless across N targets, and query-string scoping is the one
/// canonical per-target mechanism.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ReadFileInput {
    /// One or more targets to read, applied in order. Each is a canonical file
    /// URI (`file:...`, bare `file:` for the worktree root), a Cairn resource URI (`cairn://...`), or a
    /// web/PDF URL. Append `?key=value&...` to a URI for per-target scoping.
    pub paths: Vec<String>,
}

/// Validate a run batch: every key is one the run grammar defines, the batch is
/// non-empty, and each item has exactly one of `command` / `target` / `code`.
/// Returns the problem as a user-facing message.
pub fn validate_run_input(input: &RunInput) -> Result<(), String> {
    // Re-serializing restores the call's own keys: the modeled fields under
    // their wire names and every captured unknown one where the caller put it.
    // The grammar refusal is cairn-core's text verbatim, from the one function
    // both sides call.
    let raw = serde_json::to_value(input).map_err(|error| error.to_string())?;
    if let Some(refusal) = cairn_common::run_grammar::run_grammar_refusal(&raw) {
        return Err(refusal);
    }
    if input.commands.is_empty() {
        return Err("`commands` must contain at least one item".to_string());
    }
    if input.commands.iter().any(|item| item.wait_for.is_some()) {
        if input.commands.len() != 1 {
            return Err("a waitFor item must be the only item in its run batch".to_string());
        }
        if input.branch.is_some() || input.commit_msg.is_some() {
            return Err("a waitFor run cannot use branch or commit_msg".to_string());
        }
        if input.sequential.is_some() || input.stop_on_error.is_some() {
            return Err("a waitFor run cannot use sequential or stop_on_error".to_string());
        }
        let item = &input.commands[0];
        if item.command.is_some()
            || item.target.is_some()
            || item.code.is_some()
            || item.repl.is_some()
            || item.payload.is_some()
            || item.interpreter.is_some()
            || item.timeout.is_some()
            || item.background.is_some()
        {
            return Err("a waitFor item cannot include command, target, code, repl, payload, interpreter, timeout, or background".to_string());
        }
        return Ok(());
    }
    for (i, item) in input.commands.iter().enumerate() {
        // A `repl` key routes inline `code` into a live REPL session; it requires
        // `code` + `interpreter` and rejects `command`/`target`. Kept in lockstep
        // with cairn-core's `resolve_repl_send`.
        if item.repl.is_some() {
            if item.command.is_some() || item.target.is_some() {
                return Err(format!(
                    "commands[{i}] has `repl` with `command` or `target`; a REPL send takes inline `code` only"
                ));
            }
            if item.code.is_none() {
                return Err(format!(
                    "commands[{i}] has `repl` but no `code`; a REPL send evaluates inline `code`"
                ));
            }
            if item.interpreter.is_none() {
                return Err(format!(
                    "commands[{i}] has `repl` but no `interpreter`; set it to the REPL's language (python)"
                ));
            }
        }
        // Exactly one of `command` / `target` / `code`. Kept in lockstep with
        // cairn-core's `resolve_run_item` so a headless caller that bypasses
        // cairn-cmd gets the same three-way exclusivity message.
        let present: Vec<&str> = [
            item.command.as_deref().map(|_| "command"),
            item.target.as_deref().map(|_| "target"),
            item.code.as_deref().map(|_| "code"),
        ]
        .into_iter()
        .flatten()
        .collect();
        match present.as_slice() {
            [] => {
                return Err(format!(
                    "commands[{i}] has none of `command`, `target`, or `code`; provide exactly one"
                ));
            }
            [first, second, ..] => {
                return Err(format!(
                    "commands[{i}] has both `{first}` and `{second}`; provide exactly one of `command`, `target`, or `code`"
                ));
            }
            _ => {}
        }
        // `code` requires an `interpreter`, and `interpreter` is only valid with
        // inline `code`.
        if item.code.is_some() && item.interpreter.is_none() {
            return Err(format!(
                "commands[{i}] has `code` but no `interpreter`; set `interpreter` to one of: typescript (ts), javascript (js), python (py)"
            ));
        }
        if item.interpreter.is_some() && item.code.is_none() {
            return Err(format!(
                "commands[{i}] has `interpreter` but no `code`; `interpreter` is only valid with inline `code`"
            ));
        }
        // `payload` is meaningless for inline code — reject it at the front door so
        // this edge matches cairn-core's `resolve_code_spec` (both refuse it).
        if item.code.is_some() && item.payload.is_some() {
            return Err(format!(
                "commands[{i}] has both `code` and `payload`; inline code takes no payload"
            ));
        }
    }
    let mut placements = std::collections::BTreeSet::new();
    for item in &input.commands {
        if item.repl.is_some() {
            placements.insert("REPL");
        } else if item
            .target
            .as_deref()
            .is_some_and(|target| target.starts_with("cairn://mcp/"))
        {
            placements.insert("MCP gateway");
        } else {
            placements.insert("tree-bound");
        }
    }
    if placements.len() > 1 {
        return Err(
            "a run batch may not mix tree-bound shell/inline/skill items with MCP-target or REPL items; split them into separate run calls"
                .to_string(),
        );
    }
    if let Some(executor) = &input.executor {
        validate_executor_selector(executor)?;
    }
    Ok(())
}

/// A selector that asks for nothing, or for two contradictory things, is a
/// caller error rather than a request to interpret.
fn validate_executor_selector(selector: &RunExecutorSelectorInput) -> Result<(), String> {
    if selector.name.is_some() && selector.os.is_some() {
        return Err(
            "`executor` states `name` or `os`, never both: naming a machine already settles its platform. Read cairn://executors for the names available"
                .to_string(),
        );
    }
    if selector.name.is_none() && selector.os.is_none() && selector.required_toolchains.is_empty() {
        return Err(
            "`executor` must state at least one of `name`, `os`, or `requiredToolchains`. Read cairn://executors for the names available"
                .to_string(),
        );
    }
    Ok(())
}

/// Input for run tool: an ordered batch of invocations.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunInput {
    /// Ordered list of invocations. Each item is exactly one of three shapes: a
    /// shell `command`, inline `code` (with an `interpreter`), or a `target`
    /// skill-script/MCP URI. Must contain at least one item. A batch may not mix
    /// tree-bound items (shell, inline code, skill scripts) with MCP gateway or
    /// REPL items; split different placement classes into separate run calls.
    pub commands: Vec<RunItemInput>,
    /// Run items in input order instead of concurrently (default: false = parallel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequential: Option<bool>,
    /// In sequential mode, abort remaining items after a failure (default: true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_on_error: Option<bool>,
    /// Commit message for successful worktree-bound batches that dirty the tree.
    /// Stages all changes and commits once after success. Use "^" to amend the
    /// previous commit. Without a commit_msg, a batch that dirties the worktree
    /// is restored to HEAD. Cannot be combined with branch.
    #[serde(
        rename = "commit_msg",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub commit_msg: Option<String>,
    /// Run the batch against another revision — a branch name, a commit, or a
    /// node URI — to tell a regression from a failure already on the base.
    /// Verdict-only: tracked writes are discarded, and commit_msg, MCP, and
    /// REPL items are rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Which machine runs this batch: `{name}` for one executor by its public
    /// name, or `{os}` for any machine on that platform, optionally refined by
    /// `requiredToolchains`. The batch runs on a machine that satisfies it or is
    /// refused naming what prevents it, never quietly elsewhere. It sits beside
    /// `commands` (one batch runs on one machine); one inside a command item is
    /// refused. Every result's first line names the machine the batch ran on.
    /// Read cairn://executors for the names, platforms, and toolchains
    /// available. Omit unless the batch must land somewhere specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<RunExecutorSelectorInput>,
    /// A short written reason for committing files that contain literal Git
    /// conflict markers; without it such a commit is REFUSED and the working
    /// tree left intact. Only for a deliberate literal example in documentation
    /// or a test fixture; the reason is recorded with the commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_markers_reason: Option<String>,
    /// Every key the run grammar does not define, captured for the sole purpose
    /// of refusing it.
    ///
    /// Dropping an unknown key is how an `executor` written inside an item ran
    /// a batch on a machine its caller never asked for. Capturing the leftovers
    /// (rather than denying them in the deserializer, which stops at the first)
    /// lets [`validate_run_input`] name every one with its location through
    /// `cairn_common::run_grammar`. Kept out of the published schema; a call
    /// carrying any is refused before it is forwarded.
    #[serde(flatten)]
    #[schemars(skip)]
    pub(crate) unknown: serde_json::Map<String, serde_json::Value>,
}

/// Which machine a batch is asking for: `name` for one executor, or `os` for
/// any machine on a platform (never both), optionally refined by
/// `requiredToolchains`. Unknown keys are refused rather than ignored.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunExecutorSelectorInput {
    /// The executor's public name, exactly as `cairn://executors` lists it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The operating system the batch needs (`linux`, `macos`, `windows`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    /// Toolchains the chosen machine must advertise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_toolchains: Vec<String>,
}

/// A single run item: exactly one of `command` (shell), inline `code` (with an
/// `interpreter`), or `target` (skill script / MCP tool URI).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunItemInput {
    /// Shell command to execute. Mutually exclusive with `code` and `target`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Short description of what this command does (5-10 words).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Kill bound in milliseconds for this item, never a bound on the call.
    /// Setting it terminates this item at the bound, its result block reporting
    /// the timeout with whatever output it produced. Omitting it lets a shell,
    /// code, or skill-script item run to completion, bounded only by the 6-hour
    /// ceiling on a single batch (a larger value is clamped to it). A response
    /// item runs to this bound, or to its definition's own timeout when it is
    /// omitted, up to the same ceiling, because its batch suspends. An MCP-tool
    /// item runs to this bound up to the same ceiling and suspends the same way;
    /// omitting it keeps the MCP gateway's 30-second default. A response or
    /// MCP-tool item called from @cairn/sdk inside a shell or script item cannot
    /// suspend and ends at 120 seconds. A REPL or decide item executes on the
    /// host and is capped at the 120-second synchronous window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    /// A `cairn://skills/<id>/scripts/<name>` target. Mutually exclusive with `command` and `code`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Structured args for a `target` skill script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<RunItemPayloadInput>,
    /// Inline source to execute — the default way to run code that isn't a CLI
    /// invocation (the interpreter execs it directly; no shell, no quoting).
    /// Mutually exclusive with `command`/`target`; requires `interpreter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Language for an inline `code` item: `typescript`/`ts` or `javascript`/`js`
    /// (both via bun), or `python`/`py` (via the bundled `uv`, with PEP 723 deps
    /// and project-env pickup), or `matlab` (via `matlab -batch`). Required iff
    /// `code` is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpreter: Option<String>,
    /// Route this item's inline `code` into the live stateful REPL session with
    /// this slug, so variables/imports/defs persist across `run` calls. Requires
    /// `code` + `interpreter` matching the REPL's language; create the REPL
    /// first with a `write` to `cairn:~/repl/<slug>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repl: Option<String>,
    /// For a `cairn://workflows/...` target only: `true` returns the workflow
    /// URI at once and wakes the caller when it completes, instead of
    /// suspending the call until the workflow finishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    /// Suspend this turn without polling until a duration elapses, a terminal
    /// exits/prints a phrase, or another node's project check lanes settle. Must
    /// be the sole item in the batch. Examples: `{waitFor:{duration:"3m"}}`,
    /// `{waitFor:{kind:"terminal",ref:"cairn:~/terminal/dev",on:"output",phrase:"ready"}}`,
    /// `{waitFor:{kind:"checks",ref:"cairn://p/CAIRN/123/1/builder/checks",on:"settled"}}`.
    #[serde(default, rename = "waitFor", skip_serializing_if = "Option::is_none")]
    pub wait_for: Option<WaitForInput>,
    /// Keys an item does not define, captured to be refused by name (see
    /// [`RunInput::unknown`]).
    #[serde(flatten)]
    #[schemars(skip)]
    pub(crate) unknown: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WaitForInput {
    Duration {
        duration: WaitDurationInput,
    },
    Terminal {
        kind: TerminalWaitKindInput,
        #[serde(rename = "ref")]
        reference: String,
        on: TerminalWaitEventInput,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        phrase: Option<String>,
    },
    Checks {
        kind: ChecksWaitKindInput,
        #[serde(rename = "ref")]
        reference: String,
        on: ChecksWaitEventInput,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        suite: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WaitDurationInput {
    Human(String),
    Milliseconds(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TerminalWaitKindInput {
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TerminalWaitEventInput {
    Exit,
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ChecksWaitKindInput {
    Checks,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ChecksWaitEventInput {
    Settled,
    Verdict,
}

/// Structured args for a `target`: positional `args` for a skill script, or a
/// named-argument `args_json` object for an MCP tool call.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RunItemPayloadInput {
    /// Positional arguments appended to the script's argv (skill-script targets).
    #[serde(default)]
    args: Vec<String>,
    /// Named-argument object for an MCP tool call
    /// (`cairn://mcp/<server>/<tool>`), forwarded to the server's `tools/call`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args_json: Option<serde_json::Value>,
    /// Keys a payload does not define, captured to be refused by name (see
    /// [`RunInput::unknown`]).
    #[serde(flatten)]
    #[schemars(skip)]
    unknown: serde_json::Map<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::run_input;

    /// The advertised JSON-schema `mode` enum must list exactly the canonical
    /// `ChangeMode` variants, so the schema the model sees never drifts from the
    /// modes the shared validator accepts.
    #[test]
    fn change_input_schema_mode_enum_matches_change_mode() {
        let mut generator = schemars::SchemaGenerator::default();
        let schema = <ChangeInput as schemars::JsonSchema>::json_schema(&mut generator);
        let value = serde_json::to_value(&schema).unwrap();
        let enum_values = value["properties"]["changes"]["items"]["properties"]["mode"]["enum"]
            .as_array()
            .expect("mode enum array");
        let mut from_schema: Vec<String> = enum_values
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        from_schema.sort();
        let mut from_enum: Vec<String> = cairn_common::contract::ChangeMode::ALL
            .iter()
            .map(|m| m.as_str().to_string())
            .collect();
        from_enum.sort();
        assert_eq!(from_schema, from_enum);
    }
    /// A genuinely-absent `changes` survives the lenient deserialize + the
    /// re-serialization the validator runs on, producing the precise "not
    /// present" message rather than an opaque rmcp parse error.
    #[test]
    fn absent_changes_round_trips_to_not_present() {
        let input: ChangeInput = serde_json::from_str("{}").unwrap();
        let raw = serde_json::to_value(&input).unwrap();
        let errors = cairn_common::change_validation::validate_change_value(&raw);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("was not present"));
    }

    /// File target without commit_msg + a bad mode + a missing target are all
    /// reported together in a single validation pass over the lenient input.
    #[test]
    fn multiple_problems_reported_in_one_pass_via_lenient_input() {
        let input: ChangeInput = serde_json::from_value(serde_json::json!({
            "changes": [
                { "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } },
                { "mode": "bogus" }
            ]
        }))
        .unwrap();
        let raw = serde_json::to_value(&input).unwrap();
        let errors = cairn_common::change_validation::validate_change_value(&raw);
        assert!(errors.iter().any(|e| e.field == "commit_msg"));
        assert!(errors
            .iter()
            .any(|e| e.field == "target" && e.index == Some(1)));
        assert!(errors
            .iter()
            .any(|e| e.field == "mode" && e.index == Some(1)));
    }
    #[test]
    fn run_input_parses_commands_array() {
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "npm test", "description": "tests", "timeout": 1000 },
                { "target": "cairn://skills/ui/scripts/check.sh", "payload": { "args": ["--fast"] } }
            ],
            "sequential": true,
            "stop_on_error": false,
            "commit_msg": "done"
        }));
        assert_eq!(input.commands.len(), 2);
        assert_eq!(input.sequential, Some(true));
        assert_eq!(input.stop_on_error, Some(false));
        assert_eq!(input.commit_msg.as_deref(), Some("done"));
        assert_eq!(input.commands[0].command.as_deref(), Some("npm test"));
        assert_eq!(
            input.commands[1].target.as_deref(),
            Some("cairn://skills/ui/scripts/check.sh")
        );
        assert_eq!(
            input.commands[1].payload.as_ref().unwrap().args,
            vec!["--fast".to_string()]
        );
    }

    #[test]
    fn run_input_preserves_mcp_args_json() {
        // An MCP tool call carries its named arguments in payload.args_json.
        // The field must survive deserialize so the re-serialized payload
        // forwarded to the backend still contains the args.
        let input = run_input(serde_json::json!({
            "commands": [
                { "target": "cairn://mcp/axon/look", "payload": { "args_json": { "app": "Finder" } } }
            ]
        }));
        let payload = input.commands[0].payload.as_ref().expect("payload present");
        assert_eq!(
            payload.args_json,
            Some(serde_json::json!({ "app": "Finder" }))
        );
        // Round-trip: re-serializing the input (as `run` does before forwarding)
        // keeps args_json intact.
        let reser = serde_json::to_value(&input).expect("serialize RunInput");
        assert_eq!(
            reser["commands"][0]["payload"]["args_json"],
            serde_json::json!({ "app": "Finder" })
        );
    }

    /// Both selector forms survive the forward to the backend byte for byte:
    /// the tool schema is the only place the agent's words are checked, so a
    /// key that silently changed shape here would place work elsewhere.
    #[test]
    fn run_input_forwards_each_executor_selector_form_verbatim() {
        let named = run_input(serde_json::json!({
            "commands": [{"command": "uname -m"}],
            "executor": {"name": "bglab-ub", "requiredToolchains": ["rust"]}
        }));
        assert!(validate_run_input(&named).is_ok());
        assert_eq!(
            serde_json::to_value(named).expect("serialize RunInput")["executor"],
            serde_json::json!({"name": "bglab-ub", "requiredToolchains": ["rust"]})
        );

        let platform = run_input(serde_json::json!({
            "commands": [{"command": "uname -m"}],
            "executor": {"os": "linux"}
        }));
        assert!(validate_run_input(&platform).is_ok());
        assert_eq!(
            serde_json::to_value(platform).expect("serialize RunInput")["executor"],
            serde_json::json!({"os": "linux"})
        );
    }

    /// The retired placement vocabulary is refused, not ignored. A silently
    /// dropped `constraints` block would run the batch wherever the fleet felt
    /// like while the caller believed it had pinned a machine.
    #[test]
    fn run_input_rejects_the_retired_placement_vocabulary() {
        for retired in [
            serde_json::json!({"executorId": "linux-builder"}),
            serde_json::json!({"deviceId": "linux-device"}),
            serde_json::json!({"arch": "x86_64"}),
        ] {
            let raw = serde_json::json!({
                "commands": [{"command": "uname -m"}],
                "executor": retired
            });
            assert!(
                serde_json::from_value::<RunInput>(raw.clone()).is_err(),
                "expected a rejection for {raw}"
            );
        }
        // The retired TOP-LEVEL block is the dangerous one: silently dropping it
        // runs the batch untargeted while its caller believes it pinned a
        // machine. It is refused by name, pointing at the key that replaced it.
        let legacy = run_input(serde_json::json!({
            "commands": [{"command": "uname -m"}],
            "constraints": {"os": "linux"}
        }));
        let error = validate_run_input(&legacy)
            .expect_err("a legacy `constraints` block must be refused, never ignored");
        assert!(
            error.contains("`constraints` was replaced by `executor`"),
            "{error}"
        );
        assert!(error.contains("cairn://executors"), "{error}");
    }

    /// The rs/778 specimen exactly: the selector rode inside the item, was
    /// dropped by the deserializer, and the batch ran on the job's residency
    /// machine. It is refused with the call-level shape, and nothing runs.
    #[test]
    fn an_item_level_executor_is_refused_with_the_move_it_text() {
        let input = run_input(serde_json::json!({
            "commands": [{"command": "hostname", "executor": {"name": "fedora"}}]
        }));
        let error = validate_run_input(&input).expect_err("a misplaced executor is refused");
        assert_eq!(
            Some(error.as_str()),
            cairn_common::run_grammar::run_grammar_refusal(&serde_json::json!({
                "commands": [{"command": "hostname", "executor": {"name": "fedora"}}]
            }))
            .as_deref(),
            "the MCP refusal is the core text verbatim"
        );
        assert!(
            error.contains("`commands[0].executor` is a call-level option: one batch runs on one machine. Move it beside `commands`"),
            "{error}"
        );
        assert!(error.ends_with("Nothing ran."), "{error}");
    }

    /// A camelCase `commitMsg` would drop the commit with it; an item with two
    /// unknown keys names both. Every offending key is reported in one refusal.
    #[test]
    fn unknown_keys_are_refused_together_by_location() {
        let input = run_input(serde_json::json!({
            "commands": [{"command": "true", "timout": 5, "zzqq": true}],
            "commitMsg": "m"
        }));
        let error = validate_run_input(&input).expect_err("unknown keys are refused");
        assert!(error.contains("did you mean `commit_msg`"), "{error}");
        assert!(error.contains("`commands[0].timout`"), "{error}");
        assert!(error.contains("`commands[0].zzqq`"), "{error}");
        assert!(error.ends_with("Nothing ran."), "{error}");
    }

    /// `background` is a documented workflow-target key: it reaches the
    /// backend instead of being dropped.
    #[test]
    fn background_is_forwarded() {
        let input = run_input(serde_json::json!({
            "commands": [{"target": "cairn://workflows/w", "background": true}]
        }));
        assert!(validate_run_input(&input).is_ok());
        assert_eq!(
            serde_json::to_value(&input).expect("serialize RunInput")["commands"][0]["background"],
            serde_json::json!(true)
        );
    }

    /// The capture of unknown keys stays out of the schema the model reads.
    #[test]
    fn the_published_run_schema_does_not_advertise_the_capture() {
        let schema = serde_json::to_string(&schemars::schema_for!(RunInput))
            .expect("the run tool schema serializes");
        assert!(!schema.contains("\"unknown\""), "{schema}");
        assert!(schema.contains("\"background\""), "{schema}");
    }

    /// The retired key is refused, not advertised. Publishing it in the tool
    /// schema would invite the very call the validator exists to reject.
    #[test]
    fn the_published_run_schema_does_not_advertise_the_retired_key() {
        let schema = serde_json::to_value(schemars::schema_for!(RunInput))
            .expect("the run tool schema serializes");
        let properties = schema
            .pointer("/properties")
            .and_then(serde_json::Value::as_object)
            .expect("RunInput publishes properties");
        assert!(
            properties.contains_key("executor"),
            "the placement key must be discoverable: {properties:?}"
        );
        assert!(
            !properties.contains_key("constraints"),
            "the retired placement key must not be advertised: {properties:?}"
        );
    }

    /// An empty selector and a self-contradicting one are caller errors.
    #[test]
    fn run_input_rejects_empty_and_contradictory_executor_selectors() {
        let empty =
            run_input(serde_json::json!({"commands": [{"command": "true"}], "executor": {}}));
        assert!(validate_run_input(&empty)
            .unwrap_err()
            .contains("at least one"));
        let both = run_input(serde_json::json!({
            "commands": [{"command": "true"}],
            "executor": {"name": "bglab-ub", "os": "linux"}
        }));
        assert!(validate_run_input(&both)
            .unwrap_err()
            .contains("never both"));
    }

    #[test]
    fn validate_run_input_accepts_each_wait_form_and_rejects_mixtures() {
        for wait_for in [
            serde_json::json!({"duration":"3m"}),
            serde_json::json!({"duration":25}),
            serde_json::json!({"kind":"terminal","ref":"cairn:~/terminal/tests","on":"exit"}),
            serde_json::json!({"kind":"terminal","ref":"cairn:~/terminal/dev","on":"output","phrase":"ready"}),
        ] {
            let input = run_input(serde_json::json!({"commands":[{"waitFor":wait_for}]}));
            assert!(validate_run_input(&input).is_ok());
        }
        let mixed = run_input(
            serde_json::json!({"commands":[{"waitFor":{"duration":"3m"}},{"command":"echo no"}]}),
        );
        assert!(validate_run_input(&mixed)
            .unwrap_err()
            .contains("only item"));
        let branch = run_input(
            serde_json::json!({"commands":[{"waitFor":{"duration":"3m"}}],"branch":"main"}),
        );
        assert!(validate_run_input(&branch).unwrap_err().contains("branch"));
        let commit = run_input(
            serde_json::json!({"commands":[{"waitFor":{"duration":"3m"}}],"commit_msg":"no"}),
        );
        assert!(validate_run_input(&commit)
            .unwrap_err()
            .contains("commit_msg"));
    }

    #[test]
    fn validate_run_input_rejects_empty_commands() {
        let input = run_input(serde_json::json!({ "commands": [] }));
        assert!(validate_run_input(&input).is_err());
    }

    #[test]
    fn validate_run_input_rejects_item_with_both_command_and_target() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "echo hi", "target": "cairn://skills/ui/scripts/x.sh" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("both"));
    }

    #[test]
    fn validate_run_input_rejects_item_with_none_of_the_three_kinds() {
        let input = run_input(serde_json::json!({
            "commands": [{ "description": "nothing" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("none of"), "got: {err}");
    }

    #[test]
    fn validate_run_input_accepts_well_formed_batch() {
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "echo a" },
                { "target": "cairn://skills/ui/scripts/x.sh" }
            ]
        }));
        assert!(validate_run_input(&input).is_ok());
    }

    #[test]
    fn validate_run_input_accepts_code_item_with_interpreter() {
        let input = run_input(serde_json::json!({
            "commands": [{ "code": "console.log(1)", "interpreter": "typescript" }]
        }));
        assert!(validate_run_input(&input).is_ok());
        assert_eq!(input.commands[0].code.as_deref(), Some("console.log(1)"));
        assert_eq!(input.commands[0].interpreter.as_deref(), Some("typescript"));
    }

    #[test]
    fn validate_run_input_rejects_code_with_command() {
        let input = run_input(serde_json::json!({
            "commands": [{ "code": "print(1)", "interpreter": "python", "command": "echo hi" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("both") && err.contains("code"), "got: {err}");
    }

    #[test]
    fn validate_run_input_rejects_code_with_target() {
        let input = run_input(serde_json::json!({
            "commands": [{ "code": "print(1)", "interpreter": "python", "target": "cairn://skills/ui/scripts/x.sh" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("both"), "got: {err}");
    }

    #[test]
    fn validate_run_input_rejects_code_without_interpreter() {
        let input = run_input(serde_json::json!({
            "commands": [{ "code": "print(1)" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("interpreter"), "got: {err}");
    }

    #[test]
    fn validate_run_input_rejects_interpreter_without_code() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "echo hi", "interpreter": "python" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(
            err.contains("interpreter") && err.contains("code"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_run_input_rejects_code_with_payload() {
        let input = run_input(serde_json::json!({
            "commands": [{ "code": "print(1)", "interpreter": "python", "payload": { "args": ["x"] } }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(
            err.contains("payload") && err.contains("code"),
            "got: {err}"
        );
    }
}
