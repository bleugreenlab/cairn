//! Shared Cairn system prompt content.
//!
//! The base system prompt is bundled into the binary and shared across
//! backends (Claude, Codex, future engines). Keep all global guardrails and
//! instructions in `system_prompt.md` so every backend runs with identical
//! containment.

/// Cairn's base system prompt (compiled into the binary).
///
/// Shared across backends as the first system-prompt segment, so every backend
/// receives the same global harness contract.
pub const CAIRN_SYSTEM_PROMPT: &str = include_str!("agent_process/system_prompt.md");

/// The default provider-agnostic workspace character prompt (compiled into the
/// binary). Seeded once to `~/.cairn/AGENTS.md` on a fresh install; from there
/// it is assembled as the `workspace` segment for every backend, carrying the
/// motivating doctrine the old per-backend base prompts used to hold. It is
/// never assembled directly — only used as the seed bytes.
pub const DEFAULT_WORKSPACE_PROMPT: &str =
    include_str!("agent_process/default_workspace_prompt.md");

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
- When a base advance auto-rebases your workspace and records a conflict, the
  conflict markers materialize in your files; resolve them and re-seal on your
  next `write`/`run` with a normal `commit_msg`. No manual rebase or force-push.
- While a worktree-bound agent tree is dirty, every tool result includes a
  `<system-reminder>` telling the agent to commit or discard the changes; it
  clears automatically once the working copy is clean.
- `preview: true` validates and computes the change report without side effects
  and needs no `commit_msg`, returning an `apply_uri`; land it by re-submitting a
  single item with `mode: "apply"`, that URI, and the `commit_msg` that commits
  the edits (apply is the step that writes). A bare `mode: "rename"` is
  preview-shaped the same way. Apply is same-run only and rejects stale targets.
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
}
