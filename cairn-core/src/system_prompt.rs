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
const CAIRN_SYSTEM_PROMPT: &str = include_str!("agent_process/system_prompt.md");

/// Sentinel line in `system_prompt.md` marking where the capability-tier Version
/// Control section is substituted in. The trailing newline is part of the marker
/// so the substituted snippet (which ends in exactly one newline) reproduces the
/// surrounding bytes precisely. The base file carries only this marker in place
/// of the section body.
const VERSION_CONTROL_MARKER: &str = "<!--TIER:VERSION_CONTROL-->\n";

/// Version-control contract for virtual agent residence.
const VERSION_CONTROL_AUTHORING: &str = include_str!("agent_process/version_control_authoring.md");

/// The default provider-agnostic workspace character prompt (compiled into the
/// binary). Seeded once to `~/.cairn/AGENTS.md` on a fresh install; from there
/// it is assembled as the `workspace` segment for every backend, carrying the
/// motivating doctrine the old per-backend base prompts used to hold. It is
/// never assembled directly — only used as the seed bytes.
pub(crate) const DEFAULT_WORKSPACE_PROMPT: &str =
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
fn resource_mutation_reference() -> String {
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
- Your branch is the only durable record. `write` requires a `commit_msg` for
  file-target edits, and a mutating `run` commits to the same branch; without a
  `commit_msg`, a run's changes are undone.
- When the base branch advances, Cairn rebases your branch onto it. Resolve any
  reported conflict with ordinary file writes; do not rebase or force-push by
  hand.
- Relative `file:` targets address the project root, and repository commands
  execute through `run`. Your `run` batches, terminals, and REPLs share one
  working directory, so a command validated with `run` behaves identically in a
  terminal. Installed packages and `$TMPDIR` persist there as on any machine —
  convenient, never durable storage; only committed work keeps.
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
fn resource_read_catalog() -> String {
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
pub(crate) fn cairn_help() -> String {
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
/// `system_prompt.md` is substituted with the single virtual-residence version-
/// control contract. The retained `ambient` argument cannot select a physical
/// agent-checkout implementation,
/// so the authoring variant stays byte-identical for provider prompt-cache
/// reuse; the ambient variant is a second content-addressable variant that
/// dedups cleanly within its tier.
pub(crate) fn cairn_system_prompt(_ambient: bool) -> String {
    CAIRN_SYSTEM_PROMPT.replace(VERSION_CONTROL_MARKER, VERSION_CONTROL_AUTHORING)
}

/// Substrate vocabulary that must never reach agent-facing text.
///
/// The normalcy invariant (`docs/execution-fabric.md`) is that an agent behaving
/// as if its working directory is an ordinary repository checkout must never be
/// wrong. These words are correct internally and wrong in anything an agent
/// reads, because each one names a mechanism the agent cannot act on. Shared by
/// the prompt and orientation-block guards.
#[cfg(test)]
pub(crate) const SUBSTRATE_VOCABULARY: &[&str] = &[
    "cell",
    "cells",
    "lease",
    "leases",
    "coordinate",
    "coordinates",
    "materialization",
    "materializations",
    "occupancy",
    "occupant",
    "epoch",
    "incarnation",
    "residence",
    "residency",
    "residencies",
];

/// Whole-word, case-insensitive containment. Substring matching would flag
/// "please" for `lease` and "excellent" for `cell`.
#[cfg(test)]
pub(crate) fn contains_substrate_word(haystack: &str, word: &str) -> bool {
    let haystack = haystack.to_lowercase();
    let boundary = |c: Option<char>| c.is_none_or(|c| !c.is_alphanumeric());
    haystack.match_indices(word).any(|(at, _)| {
        boundary(haystack[..at].chars().next_back())
            && boundary(haystack[at + word.len()..].chars().next())
    })
}

/// Assert a piece of agent-facing text carries no substrate vocabulary.
#[cfg(test)]
pub(crate) fn assert_no_substrate_vocabulary(label: &str, text: &str) {
    for word in SUBSTRATE_VOCABULARY {
        assert!(
            !contains_substrate_word(text, word),
            "{label} leaks substrate vocabulary `{word}`: an agent cannot act on it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_carries_one_version_control_contract() {
        let prompt = cairn_system_prompt(false);
        assert!(prompt.contains("must carry a `commit_msg`"));
        assert!(prompt.contains("never rebase or force-push by hand"));
        // No marker residue survives substitution.
        assert!(!prompt.contains("<!--"));
        assert!(!prompt.contains("TIER:VERSION_CONTROL"));
    }

    #[test]
    fn legacy_tier_argument_does_not_change_virtual_contract() {
        let prompt = cairn_system_prompt(true);
        assert!(prompt.contains("must carry a `commit_msg`"));
        assert_eq!(prompt.matches("## Version Control").count(), 1);
        assert!(!prompt.contains("<!--"));
    }

    #[test]
    fn system_prompt_carries_no_substrate_vocabulary() {
        for ambient in [false, true] {
            assert_no_substrate_vocabulary("system prompt", &cairn_system_prompt(ambient));
        }
    }

    #[test]
    fn substrate_word_match_respects_word_boundaries() {
        assert!(contains_substrate_word("one lease, renewed", "lease"));
        assert!(contains_substrate_word("A Cell.", "cell"));
        assert!(!contains_substrate_word("please retry", "lease"));
        assert!(!contains_substrate_word("excellent work", "cell"));
        assert!(!contains_substrate_word("cancelled", "cell"));
    }

    #[test]
    fn every_tier_carries_visual_markdown_guidance() {
        for ambient in [false, true] {
            let prompt = cairn_system_prompt(ambient);
            assert!(prompt.contains("`mermaid` and `vega-lite` fenced code blocks"));
            assert!(prompt.contains("`$…$` as inline math"));
            assert!(prompt.contains("`$$…$$` as display math"));
            assert!(prompt.contains("literal dollar signs in code spans"));
            assert!(prompt.contains("inline data with `data.values`"));
            assert!(prompt.contains("not remote `data.url` sources"));
            assert!(prompt.contains("`inlinehtml` fences render as live sandboxed HTML previews"));
            assert!(prompt.contains("plain `html` fences stay code"));
        }
    }

    #[test]
    fn virtual_residence_prompt_is_one_deterministic_contract() {
        // Agent jobs have one version-control contract. The legacy ambient flag
        // cannot select a second agent-workspace implementation.
        assert_eq!(cairn_system_prompt(false), cairn_system_prompt(true));
        assert_eq!(cairn_system_prompt(false), cairn_system_prompt(false));
        // Authoring byte-identity guard: the marker is fully consumed and the
        // section heading appears exactly once.
        let authoring = cairn_system_prompt(false);
        assert!(!authoring.contains(VERSION_CONTROL_MARKER));
        assert_eq!(authoring.matches("## Version Control").count(), 1);
    }

    /// A capability only exists for an agent that can find it at the moment it
    /// needs it. "Is this failure mine, or already on the base?" has two answers
    /// — the canonical producer for a configured suite, `branch` for anything
    /// else — so the prompt carries both rather than leaving them to the tool
    /// schema. Asserted by shape, not by the example's wording, which is
    /// copy-edited freely.
    #[test]
    fn every_tier_shows_how_to_run_a_check_against_another_branch() {
        for ambient in [false, true] {
            let prompt = cairn_system_prompt(ambient);
            assert!(prompt.contains(r#"], branch:"main"})"#));
            assert!(prompt.contains("cairn check run <suite>... [--branch <revision>]"));
            assert!(prompt.contains(r#"read({paths:["file:src/lib.rs?branch=main"]})"#));
            assert!(prompt.contains("cannot be combined with `commit_msg`"));
            // `run` presents shell, inline code, MCP tools, and REPL sends as
            // peer item classes, so an unqualified description invites
            // attaching `branch` to a class the handler rejects.
            assert!(prompt.contains("an MCP-tool or REPL batch executes on the host"));
        }
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
