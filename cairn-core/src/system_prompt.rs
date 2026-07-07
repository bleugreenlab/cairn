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

/// Sentinel line in `system_prompt.md` marking where the capability-tier Version
/// Control section is substituted in. The trailing newline is part of the marker
/// so the substituted snippet (which ends in exactly one newline) reproduces the
/// surrounding bytes precisely. The base file carries only this marker in place
/// of the section body.
const VERSION_CONTROL_MARKER: &str = "<!--TIER:VERSION_CONTROL-->\n";

/// Authoring-tier Version Control section: a worktree-backed job that authors
/// commits on its own branch. Byte-identical to the section that lived inline in
/// `system_prompt.md` before tiering, so substituting it for the marker
/// reproduces the pre-tiering prompt exactly and keeps its prompt-cache lineage.
const VERSION_CONTROL_AUTHORING: &str = include_str!("agent_process/version_control_authoring.md");

/// Ambient-tier Version Control section: a no-worktree job that runs on the
/// project's live checkout, observing and orchestrating rather than authoring.
/// Substituted in when the run is ambient; routes code changes through child
/// issues since it has no branch of its own to land on.
const VERSION_CONTROL_AMBIENT: &str = include_str!("agent_process/version_control_ambient.md");

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
///
/// Composed by capability tier: the `<!--TIER:VERSION_CONTROL-->` marker in
/// `system_prompt.md` is substituted with the authoring Version Control section
/// (worktree-backed job) or the ambient one (no worktree, runs on the project's
/// live checkout). `ambient == false` reproduces the pre-tiering bytes exactly,
/// so the authoring variant stays byte-identical for provider prompt-cache
/// reuse; the ambient variant is a second content-addressable variant that
/// dedups cleanly within its tier.
pub fn cairn_system_prompt(ambient: bool) -> String {
    let version_control = if ambient {
        VERSION_CONTROL_AMBIENT
    } else {
        VERSION_CONTROL_AUTHORING
    };
    CAIRN_SYSTEM_PROMPT.replace(VERSION_CONTROL_MARKER, version_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoring_tier_carries_worktree_doctrine() {
        let prompt = cairn_system_prompt(false);
        // The authoring doctrine is present.
        assert!(prompt.contains("the workspace always equals HEAD"));
        assert!(prompt.contains("must carry a `commit_msg`"));
        // No ambient phrasing leaks into the authoring variant.
        assert!(!prompt.contains("do not author"));
        assert!(!prompt.contains("rejected by design"));
        // No marker residue survives substitution.
        assert!(!prompt.contains("<!--"));
        assert!(!prompt.contains("TIER:VERSION_CONTROL"));
    }

    #[test]
    fn ambient_tier_carries_no_author_doctrine_exactly_once() {
        let prompt = cairn_system_prompt(true);
        assert!(prompt.contains("do not author"));
        assert!(prompt.contains("child issue"));
        assert!(prompt.contains("rejected by design"));
        // The Version Control section appears exactly once (replaced, not appended).
        assert_eq!(prompt.matches("## Version Control").count(), 1);
        // The authoring doctrine is fully gone, not stacked underneath.
        assert!(!prompt.contains("the workspace always equals HEAD"));
        assert!(!prompt.contains("<!--"));
    }

    #[test]
    fn tiers_are_two_deterministic_variants() {
        // Exactly two variants, each deterministic (content-addressable) per call.
        assert_ne!(cairn_system_prompt(false), cairn_system_prompt(true));
        assert_eq!(cairn_system_prompt(false), cairn_system_prompt(false));
        assert_eq!(cairn_system_prompt(true), cairn_system_prompt(true));
        // Authoring byte-identity guard: the marker is fully consumed and the
        // section heading appears exactly once.
        let authoring = cairn_system_prompt(false);
        assert!(!authoring.contains(VERSION_CONTROL_MARKER));
        assert_eq!(authoring.matches("## Version Control").count(), 1);
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
}
