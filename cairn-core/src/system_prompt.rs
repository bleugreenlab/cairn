//! Shared Cairn system prompt content.
//!
//! The base system prompt is bundled into the binary and shared across
//! backends (Claude, Codex, future engines). Keep all global guardrails and
//! instructions in `system_prompt.md` so every backend runs with identical
//! containment.

/// Cairn's base system prompt (compiled into the binary).
///
/// Shared across backends. Appended on top of each backend's Cairn-owned base
/// prompt so every backend receives the same global harness contract.
pub const CAIRN_SYSTEM_PROMPT: &str = include_str!("agent_process/system_prompt.md");

/// Codex-only base prompt (compiled into the binary).
///
/// Replaces Codex CLI's default base instructions via `baseInstructions`.
/// `CAIRN_SYSTEM_PROMPT` is appended on top, so the effective base prompt is
/// `CODEX_SYSTEM_PROMPT` + Cairn additions.
pub const CODEX_SYSTEM_PROMPT: &str = include_str!("agent_process/codex_system_prompt.md");

/// Claude-only base system prompt (compiled into the binary).
///
/// Replaces Claude Code's default system prompt entirely via
/// `--system-prompt-file`. `CAIRN_SYSTEM_PROMPT` is still appended on top via
/// `--append-system-prompt-file`, so the effective prompt is
/// `CLAUDE_SYSTEM_PROMPT` + Cairn additions + agent-specific content.
pub const CLAUDE_SYSTEM_PROMPT: &str = include_str!("agent_process/claude_system_prompt.md");

use cairn_common::contract::{KeySpec, KeyType, RESOURCE_CONTRACTS};

fn format_keys(specs: &[KeySpec]) -> String {
    specs
        .iter()
        .map(KeySpec::display)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render the resource-mutation reference for the system prompt directly from
/// the `RESOURCE_CONTRACTS` table, so the agent-facing payload schema never
/// drifts from the dispatcher's gate. Each mutable resource lists its modes,
/// required (typed) and optional payload keys, and — where the payload nests
/// (arrays/objects) — a copy-paste example.
pub fn resource_mutation_reference() -> String {
    let mut out = String::from(
        "## Resource Mutations\n\n\
         `write` mutates a resource by URI. Keys are typed; [optional]. \
         Home-relative `cairn:~/...` targets resolve to your own node. An \
         unsupported mode or a missing required key is rejected with the \
         resource's valid mutations and an example.\n\n",
    );
    for contract in RESOURCE_CONTRACTS {
        if contract.mutations.is_empty() {
            continue;
        }
        out.push_str(&format!("- `{}`\n", contract.uri_template));
        for spec in contract.mutations {
            let mut line = format!("  - {}: ", spec.mode.as_str());
            if spec.required.is_empty() {
                line.push_str("(no required keys)");
            } else {
                line.push_str(&format_keys(spec.required));
            }
            if !spec.optional.is_empty() {
                line.push_str(&format!(" [{}]", format_keys(spec.optional)));
            }
            out.push_str(&line);
            out.push('\n');
            // Arrays/objects are where agents guess the shape wrong; show the
            // example for those (tasks, questions, todos, ...).
            let nests = spec
                .required
                .iter()
                .chain(spec.optional.iter())
                .any(|k| matches!(k.ty, KeyType::Array | KeyType::Object));
            if nests {
                out.push_str(&format!("    e.g. {}\n", spec.example));
            }
        }
    }
    out
}

/// Grammar and cross-cutting mechanics the per-resource table can't express:
/// the URI component glossary, the `cairn:~` vs canonical distinction, and the
/// `write` mechanics (commit_msg, preview/apply, terminals, blocking
/// tasks/questions). Migrated out of `system_prompt.md` so it lives once,
/// rendered identically into `cairn://help` and the session-start injection.
const HELP_GRAMMAR: &str = r#"# Cairn Resource Reference

Cairn resources are addressed by `cairn://` URIs and reached through three verbs:
`read` (fetch a resource or file), `write` (mutate files and resources), and
`run` (execute shell commands).

## URI grammar

Canonical project-scoped URIs use the explicit `p` namespace token:
`cairn://p/PROJECT/...`. Home-relative URIs (`cairn:~/...`) resolve against your
own node's job — use them to address your own todos, tasks, questions, and
terminals without spelling out the full path.

Components:
- `p` — explicit project-scope namespace token
- PROJECT — project key, uppercase (e.g. `CAIRN`)
- NUMBER — issue number (e.g. `123`)
- EXEC — execution sequence (1, 2, 3, ...); required for all node/task URIs
- NODE — node name (e.g. `Planner`, `builder-1`)
- SLUG — terminal identifier (e.g. `dev-server`)
- NAME — task name; duplicates get a `-N` suffix (`Explore`, `Explore-2`)
- RUN_SEQ / EVENT_SEQ — positive integers identifying a single event; never UUIDs

Legacy root-as-project forms such as `cairn://PROJECT/NUMBER` are invalid; always
use `cairn://p/PROJECT/...`.

## write mechanics

`write` applies an ordered list of file and resource mutations. Behavior that
spans resources rather than belonging to any single one:

- Every item is `{target, mode, payload}`. File-target keys (`content`, `diff`,
  `patch`, `old_string`/`new_string`, `replace_all`) ride under `payload`,
  exactly where resource-target keys live.
- `commit_msg: "Add X"` commits the batch's file-target changes as a new commit.
- `commit_msg: "^"` amends the previous commit (for multi-file atomic changes).
- Invariant: the worktree equals HEAD between tool calls. `write` requires a
  `commit_msg` for file-target edits; a successful worktree-bound `run` that
  dirties the tree without one is reverted to HEAD, entry dirt included. Commit
  work you want kept in the same call that creates it.
- `commit_msg: "NO_COMMIT"` is valid only while resolving an in-progress merge
  or rebase; anywhere else the batch's changes are restored to HEAD.
- While a worktree-bound agent tree is dirty, every tool result includes a
  `<system-reminder>` telling the agent to commit or discard the changes; it
  clears automatically once `git status --porcelain` is empty.
- `preview: true` validates and computes the change report without side effects,
  returning an `apply_uri`; re-submit a single item with `mode: "apply"` and that
  URI to commit it. Apply is same-run only and rejects stale targets.
- Terminals are long-lived resources: `create` starts one, `append` sends input,
  `delete` stops it (see the terminal entries in the mutation matrix).
- Appending to your node's `cairn:~/tasks` spawns sub-agents; appending to
  `cairn:~/questions` asks the user. Both block until results return and then
  resume your turn, and multiple task appends in one call run in parallel.
  `background: true` returns immediately (task URIs for tasks) without waiting.
"#;

/// Render the read-side catalog from the contract table: every resource's URI
/// template, name, description, and its read-query projections (`?key=values`).
/// This is the read surface the mutation reference never showed.
pub fn resource_read_catalog() -> String {
    let mut out = String::from(
        "## Read catalog\n\n\
         Every readable resource and its read-query projections (`?key=values`), \
         fetched with the `read` tool.\n\n",
    );
    for contract in RESOURCE_CONTRACTS {
        out.push_str(&format!(
            "- `{}` — {}: {}\n",
            contract.uri_template, contract.name, contract.description
        ));
        for proj in contract.read_projections {
            out.push_str(&format!("    - `?{}={}`\n", proj.key, proj.values));
        }
    }
    out
}

/// The full self-describing help page: grammar + mechanics, the read catalog,
/// and the mutation matrix. Served by `cairn://help` as the complete on-demand
/// resource reference.
pub fn cairn_help() -> String {
    format!(
        "{}\n{}\n{}",
        HELP_GRAMMAR,
        resource_read_catalog(),
        resource_mutation_reference()
    )
}

/// The Cairn system prompt: static guardrails, verb orientation, and compact
/// URI-shape guidance. The complete generated reference stays available via
/// `cairn://help`.
pub fn cairn_system_prompt() -> String {
    CAIRN_SYSTEM_PROMPT.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_lists_nested_task_and_question_payloads() {
        let reference = resource_mutation_reference();
        // Tasks and questions must surface their payload keys + nested example.
        assert!(reference.contains("subagentType(str"));
        assert!(reference.contains("questions(array)"));
        assert!(
            reference.contains("multiSelect"),
            "question example shape missing"
        );
        // Typed required keys for common mutations.
        assert!(reference.contains("title(str)"));
        assert!(reference.contains("todos(array)"));
    }

    #[test]
    fn reference_explains_constrained_value_spaces() {
        let reference = resource_mutation_reference();
        // The opaque fields the test agents flagged: enumerate their values.
        assert!(reference.contains("sm|md|lg"), "tier values missing");
        assert!(reference.contains("claude|codex"), "backend values missing");
        assert!(
            reference.contains("new (fresh context)"),
            "session values missing"
        );
        assert!(
            reference.contains("tool_bug|prompt_issue|harness_friction|suggestion"),
            "bug categories missing"
        );
    }

    #[test]
    fn full_prompt_includes_verb_orientation_and_uri_shapes() {
        let prompt = cairn_system_prompt();
        assert!(prompt.contains("Cairn is an agent orchestration system"));
        assert!(prompt.contains("## Verb Model"));
        assert!(prompt.contains("## URI Shapes"));
        assert!(prompt.contains("file:src?grep=native_tool_map&glob=**/*.rs"));
        assert!(prompt.contains("cairn://p/{project}/{number}/{exec}/{node}/task/{task}"));
        assert!(prompt.contains("Resource reads that include affordance blocks"));
    }

    #[test]
    fn full_prompt_teaches_unified_patch_change_batches() {
        let prompt = cairn_system_prompt();
        assert!(prompt.contains("mode:\"unified_patch\""));
        assert!(prompt.contains("write({changes:["));
        assert!(prompt.contains("*** Begin Patch"));
        assert!(!prompt.contains("apply_patch"));
    }

    #[test]
    fn full_prompt_teaches_single_file_patch_payload_shapes() {
        let prompt = cairn_system_prompt();
        // mode:"patch" accepts both structured old_string/new_string and a
        // single-file unified diff; both shapes must carry a concrete example.
        assert!(prompt.contains("old_string:"));
        assert!(prompt.contains("new_string:"));
        assert!(prompt.contains("payload:{diff:"));
    }

    #[test]
    fn full_prompt_keeps_catalog_and_mutation_matrix_on_demand() {
        let prompt = cairn_system_prompt();
        assert!(!prompt.contains("## Read catalog"));
        assert!(!prompt.contains("## Resource Mutations"));
        assert!(prompt.contains("cairn://help"));
    }

    #[test]
    fn static_prompt_uses_positive_framing() {
        assert!(!CAIRN_SYSTEM_PROMPT.contains("don't"));
        assert!(!CAIRN_SYSTEM_PROMPT.contains("Don't"));
    }

    #[test]
    fn help_lists_every_resource_uri_template() {
        let help = cairn_help();
        for contract in cairn_common::contract::RESOURCE_CONTRACTS {
            assert!(
                help.contains(contract.uri_template),
                "cairn://help dropped resource {}",
                contract.uri_template
            );
        }
    }

    #[test]
    fn help_contains_grammar_and_mechanics_markers() {
        let help = cairn_help();
        assert!(
            help.contains("cairn:~"),
            "home-relative URI guidance missing"
        );
        assert!(help.contains("commit_msg"), "commit_msg mechanics missing");
        assert!(help.contains("preview"), "preview/apply mechanics missing");
        assert!(help.contains("## Read catalog"));
        assert!(help.contains("## Resource Mutations"));
    }
}
