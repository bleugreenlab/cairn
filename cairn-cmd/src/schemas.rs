//! Input and data types for the cairn-cmd MCP tools: the lenient `ChangeInput`
//! carrier, the `read`/`run` inputs, and the agent-info descriptor. Pure data
//! plus `validate_run_input`; no I/O.
use serde::{Deserialize, Serialize};

/// Agent info for tool description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AgentInfo {
    pub(crate) name: String,
    pub(crate) description: String,
}

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
pub(crate) struct ChangeItemInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<serde_json::Value>,
}

/// Input for canonical change tool.
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
///
/// Fields are lenient (see `ChangeItemInput`). A genuinely-absent `changes`
/// stays absent on re-serialization (`skip_serializing_if`), so the validator
/// can emit a precise "required and not present" message instead of the opaque
/// rmcp `-32602 missing field 'changes'`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChangeInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) changes: Option<Vec<ChangeItemInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) commit_msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) preview: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) atomic: Option<bool>,
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
                                "enum": ["create", "append", "patch", "unified_patch", "replace", "delete", "rename", "apply"],
                                "description": "Mutation mode. Use unified_patch for native *** Begin Patch envelopes on file: targets, rename for an ast-grep-backed structural identifier rename on a file: target, and apply only with a single transcript event URI from a pending preview. Unsupported target/mode pairs fail explicitly."
                            },
                            "payload": {
                                "type": "object",
                                "description": "Structured payload carrying this item's keys for file and resource targets alike. File targets: create/replace/append take {content}; patch takes {diff} OR {old_string, new_string} (optional {replace_all}); unified_patch takes {patch} containing a native *** Begin Patch envelope; delete needs no payload; rename takes {new_name, and exactly one of old_name | symbol_at} and resolves every edit site by parsing the worktree with the in-process ast-grep engine. The ~~*~~ wildcard marker applies inside old_string. Resource targets carry keys like {title} or {content}; read the target URI for its exact payload keys.",
                                "additionalProperties": true
                            }
                        }
                    }
                },
                "commit_msg": {
                    "type": "string",
                    "description": "Git commit message. REQUIRED when the batch contains any file-target change (the edits are committed so they survive worktree cleanup); omit it for resource-only batches. Use '^' to amend the previous commit. Without a commit_msg, a batch that dirties the worktree is restored to HEAD."
                },
                "preview": {
                    "type": "boolean",
                    "description": "When true, validate and compute the change report without applying side effects. Apply later with a single change item using mode=apply and the preview event URI."
                },
                "atomic": {
                    "type": "boolean",
                    "description": "Apply-phase atomicity opt-in. Default false applies every item whose anchor matches, reports per-item failures, and commits only files that applied. true preserves fail-fast behavior."
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
/// `cairn://p/CAIRN/123/changed?grep=foo`). There is no top-level offset/limit:
/// they are meaningless across N targets, and query-string scoping is the one
/// canonical per-target mechanism.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub(crate) struct ReadFileInput {
    /// One or more targets to read, applied in order. Each is a canonical file
    /// URI (`file:...`, bare `file:` for the worktree root), a Cairn resource URI (`cairn://...`), or a
    /// web/PDF URL. Append `?key=value&...` to a URI for per-target scoping.
    pub(crate) paths: Vec<String>,
}

/// Validate a run batch: non-empty, and each item has exactly one of
/// `command` / `target`. Returns the first problem as a user-facing message.
pub(crate) fn validate_run_input(input: &RunInput) -> Result<(), String> {
    if input.commands.is_empty() {
        return Err("`commands` must contain at least one item".to_string());
    }
    for (i, item) in input.commands.iter().enumerate() {
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
    Ok(())
}

/// Input for run tool: an ordered batch of invocations.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub(crate) struct RunInput {
    /// Ordered list of invocations. Each item is either a shell `command` or a
    /// `target` skill-script URI. Must contain at least one item.
    pub(crate) commands: Vec<RunItemInput>,
    /// Run items in input order instead of concurrently (default: false = parallel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) sequential: Option<bool>,
    /// In sequential mode, abort remaining items after a failure (default: true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_on_error: Option<bool>,
    /// Commit message for successful worktree-bound batches that dirty the tree.
    /// Stages all changes and commits once after success. Use "^" to amend the
    /// previous commit. Without a commit_msg, a batch that dirties the worktree
    /// is restored to HEAD. Cannot be combined with branch.
    #[serde(
        rename = "commit_msg",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) commit_msg: Option<String>,
    /// Branch/ref whose live checkout should be used as the batch cwd. Cannot be
    /// combined with commit_msg; refuses dirty checkouts and warns if tracked
    /// changes appear during the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
}

/// A single run item: exactly one of `command` (shell) or `target` (skill script).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub(crate) struct RunItemInput {
    /// Shell command to execute. Mutually exclusive with `target`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) command: Option<String>,
    /// Short description of what this command does (5-10 words).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Timeout in milliseconds (default: 120000, max: 600000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout: Option<u32>,
    /// A `cairn://skills/<id>/scripts/<name>` target. Mutually exclusive with `command`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<String>,
    /// Structured args for a `target` skill script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<RunItemPayloadInput>,
    /// Inline source to execute (light, synchronous compute). Mutually exclusive
    /// with `command`/`target`; requires `interpreter`. Runs as `bun -e <code>`
    /// (typescript/javascript) or `python3 -c <code>` (python) — direct argv, no
    /// shell. Inline TypeScript gets zero-config `@cairn/sdk` from the worktree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) code: Option<String>,
    /// Language for an inline `code` item: `typescript`/`ts` or `javascript`/`js`
    /// (both via bun), or `python`/`py` (via python3, currently stdlib-only).
    /// Required iff `code` is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) interpreter: Option<String>,
}

/// Structured args for a `target`: positional `args` for a skill script, or a
/// named-argument `args_json` object for an MCP tool call.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub(crate) struct RunItemPayloadInput {
    /// Positional arguments appended to the script's argv (skill-script targets).
    #[serde(default)]
    args: Vec<String>,
    /// Named-argument object for an MCP tool call
    /// (`cairn://mcp/<server>/<tool>`), forwarded to the server's `tools/call`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args_json: Option<serde_json::Value>,
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
