//! Comment-preserving, diff-scoped merge of serialized settings back into an
//! on-disk YAML document.
//!
//! Serializing a settings struct with `serde_yaml::to_string` and overwriting
//! the file is lossy: it deletes every comment and every top-level key the
//! schema does not model. [`merge_into_yaml`] instead rewrites only the subtrees
//! whose *semantic value* changed, leaving untouched bytes — comments,
//! formatting, and unknown user keys — exactly as they were on disk.
//!
//! The engine is the same in-process tree-sitter grammar the `symbols/` modules
//! use ([`SupportLang::Yaml`]); no new dependency. tree-sitter yields byte spans,
//! `serde_yaml` yields comparable values, and the merge marries the two: values
//! are compared with `serde_yaml`, spans are cut with tree-sitter.
//!
//! Correctness always wins over comment preservation: anything the merge cannot
//! reason about safely (parse error, non-mapping root, multiple documents,
//! anchors/aliases) returns `Err`, and the caller falls back to a full
//! re-serialization.

use std::collections::HashSet;
use std::ops::Range;

use crate::symbols::SupportLang;
use serde_yaml::{Mapping, Value};

use crate::symbols::engine::{self, SymbolNode};

/// One byte-range replacement in the original source. An insert has an empty
/// range (`start == end`); a delete has empty `text`.
struct Edit {
    range: Range<usize>,
    text: String,
}

/// Merge `target` (the freshly serialized settings mapping) into `original` (the
/// on-disk YAML text), rewriting only the subtrees whose semantic value changed.
///
/// `managed_top_level_keys` are the keys the schema owns: a top-level key present
/// on disk but absent from `target` is deleted only when it is managed, so
/// unknown user keys survive. Below the top level the target mapping is
/// authoritative (an absent key is deleted).
///
/// Returns `Err` when the document is something the merge refuses to reason about
/// (the caller should fall back to full re-serialization). Returns the original
/// bytes unchanged when nothing semantically differs, so a no-op save produces a
/// byte-identical file and stages no git change.
pub(crate) fn merge_into_yaml(
    original: &str,
    target: &Mapping,
    managed_top_level_keys: &[&str],
) -> Result<String, String> {
    let orig_value: Value =
        serde_yaml::from_str(original).map_err(|e| format!("parse original yaml: {e}"))?;
    let orig_map = match &orig_value {
        Value::Mapping(m) => m,
        _ => return Err("original yaml root is not a mapping".to_string()),
    };

    let ast = engine::parse(original, SupportLang::Yaml);
    let root = ast.root();

    // Anchors/aliases make span-scoped edits unsafe (a change to one subtree can
    // affect an aliased one). Bail wholesale — configs never use them.
    if root.dfs().any(|n| {
        let kind = n.kind();
        let kind = kind.as_ref();
        kind.contains("anchor") || kind.contains("alias")
    }) {
        return Err("anchors/aliases present".to_string());
    }

    let docs: Vec<_> = root
        .children()
        .filter(|c| c.kind().as_ref() == "document")
        .collect();
    if docs.len() != 1 {
        return Err(format!("expected exactly 1 document, found {}", docs.len()));
    }

    // Pre-order search finds the outermost mapping first.
    let mapping_node = docs[0]
        .dfs()
        .find(|n| n.kind().as_ref() == "block_mapping")
        .ok_or_else(|| "no top-level block mapping".to_string())?;

    let mut edits = Vec::new();
    merge_level(
        original,
        &mapping_node,
        orig_map,
        target,
        true,
        managed_top_level_keys,
        &mut edits,
    )?;

    if edits.is_empty() {
        return Ok(original.to_string());
    }
    apply_edits(original, edits)
}

/// Diff one mapping level. `node` is the tree-sitter `block_mapping`; `orig` and
/// `target` are the corresponding `serde_yaml` mappings (values for comparison).
/// `is_top` selects the top-level deletion policy (managed-keys-only).
fn merge_level(
    src: &str,
    node: &SymbolNode,
    orig: &Mapping,
    target: &Mapping,
    is_top: bool,
    managed: &[&str],
    edits: &mut Vec<Edit>,
) -> Result<(), String> {
    let pairs = mapping_pairs(node);
    if pairs.is_empty() {
        return Err("empty block mapping".to_string());
    }

    // The child indent is the column shared by this level's pairs; used when
    // appending new keys.
    let first_start = pairs[0].2.range().start;
    let child_col = first_start - line_start(src, first_start);

    let mut seen: HashSet<String> = HashSet::new();
    let mut last_pair_end = 0usize;

    for (key, value_node, pair_node) in &pairs {
        seen.insert(key.clone());
        let pstart = pair_node.range().start;
        let pend = pair_node.range().end;
        last_pair_end = last_pair_end.max(pend);
        let col = pstart - line_start(src, pstart);

        match target.get(key.as_str()) {
            None => {
                // Key on disk, absent from target. Top level: delete only managed
                // keys (unknown user keys are preserved). Nested: authoritative.
                let deletable = if is_top {
                    managed.contains(&key.as_str())
                } else {
                    true
                };
                if deletable {
                    let (ds, de) = deletion_span(src, pstart, pend, col);
                    edits.push(Edit {
                        range: ds..de,
                        text: String::new(),
                    });
                }
            }
            Some(tv) => {
                let ov = orig.get(key.as_str());
                if ov == Some(tv) {
                    // Semantically identical: leave the original bytes untouched.
                    // This is what preserves comments and formatting.
                    continue;
                }
                // Both sides mappings (target non-empty) → recurse so edits stay
                // scoped to the changed sub-subtree and sibling comments survive.
                if let (Some(Value::Mapping(om)), Value::Mapping(tm)) = (ov, tv) {
                    if !tm.is_empty() {
                        if let Some(child) = value_node.as_ref().and_then(child_block_mapping) {
                            merge_level(src, &child, om, tm, false, managed, edits)?;
                            continue;
                        }
                    }
                }
                // Scalar / sequence / type change: replace the whole pair span.
                let rendered = render_pair(key, tv)?;
                edits.push(Edit {
                    range: pstart..pend,
                    text: reindent(&rendered, col),
                });
            }
        }
    }

    // Keys in target but not on disk: append rendered blocks at the end of this
    // mapping, on their own lines at the child indent.
    let mut append_text = String::new();
    for (k, v) in target {
        let key = k
            .as_str()
            .ok_or_else(|| "non-string key in target".to_string())?;
        if seen.contains(key) {
            continue;
        }
        let rendered = render_pair(key, v)?;
        append_text.push_str(&indent_block(&rendered, child_col));
        append_text.push('\n');
    }
    if !append_text.is_empty() {
        let (pos, prefix) = match src[last_pair_end..].find('\n') {
            Some(i) => (last_pair_end + i + 1, ""),
            None => (src.len(), "\n"),
        };
        edits.push(Edit {
            range: pos..pos,
            text: format!("{prefix}{append_text}"),
        });
    }
    Ok(())
}

/// The `block_mapping_pair` children of a mapping node as
/// `(logical key, value node, pair node)`.
fn mapping_pairs<'r>(
    mapping: &SymbolNode<'r>,
) -> Vec<(String, Option<SymbolNode<'r>>, SymbolNode<'r>)> {
    let mut out = Vec::new();
    for child in mapping.children() {
        if child.kind().as_ref() != "block_mapping_pair" {
            continue;
        }
        let Some(key_node) = child.field("key") else {
            continue;
        };
        out.push((logical_key(&key_node), child.field("value"), child));
    }
    out
}

/// The logical string of a key node, unwrapping quotes via `serde_yaml`.
fn logical_key(key_node: &SymbolNode) -> String {
    let raw = key_node.text();
    let raw = raw.trim();
    serde_yaml::from_str::<Value>(raw)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| raw.to_string())
}

/// The `block_mapping` inside a pair's value node, if the value is a mapping.
/// Returns `None` for scalars, sequences, and flow mappings (which are replaced
/// wholesale rather than recursed into).
fn child_block_mapping<'r>(value_node: &SymbolNode<'r>) -> Option<SymbolNode<'r>> {
    if value_node.kind().as_ref() == "block_mapping" {
        return Some(value_node.clone());
    }
    value_node
        .dfs()
        .find(|n| n.kind().as_ref() == "block_mapping")
}

/// Render a single-entry mapping `{key: value}` to YAML text (with its trailing
/// newline), the snippet spliced in for a replaced or appended pair.
fn render_pair(key: &str, value: &Value) -> Result<String, String> {
    let mut m = Mapping::new();
    m.insert(Value::String(key.to_string()), value.clone());
    serde_yaml::to_string(&Value::Mapping(m)).map_err(|e| format!("render {key}: {e}"))
}

/// The deletion span for a pair: from the start of its first line through the
/// newline ending its last line, plus contiguous immediately-preceding
/// same-indent comment lines (nested pairs only — a top-level comment such as the
/// file header is document structure, not an entry annotation).
fn deletion_span(src: &str, pstart: usize, pend: usize, col: usize) -> (usize, usize) {
    let mut ds = line_start(src, pstart);
    let mut de = match src[pend..].find('\n') {
        Some(i) => pend + i + 1,
        None => src.len(),
    };
    // tree-sitter attaches a same-indent comment that follows a pair to that
    // pair's value node, so `pend` can reach past a comment line that visually
    // annotates the *next* entry. Trim trailing same-indent comment lines back
    // out of the deletion so they stay with the entry they describe.
    while de > ds {
        let content_end = de - 1; // the trailing newline
        let ls = line_start(src, content_end);
        if ls <= pstart {
            break; // never trim into the pair's own first line
        }
        let line = &src[ls..content_end];
        let indent = line.len() - line.trim_start().len();
        if indent == col && line.trim_start().starts_with('#') {
            de = ls;
        } else {
            break;
        }
    }
    if col > 0 {
        while ds > 0 {
            let prev_nl = ds - 1;
            let pls = line_start(src, prev_nl);
            let line = &src[pls..prev_nl];
            let indent = line.len() - line.trim_start().len();
            if indent == col && line.trim_start().starts_with('#') {
                ds = pls;
            } else {
                break;
            }
        }
    }
    (ds, de)
}

/// Re-indent a rendered snippet whose first line replaces a pair in place: the
/// first line keeps its position (the leading indent already precedes the pair
/// on disk), continuation lines are shifted to `indent`.
fn reindent(rendered: &str, indent: usize) -> String {
    let pad = " ".repeat(indent);
    rendered
        .trim_end_matches('\n')
        .split('\n')
        .enumerate()
        .map(|(i, line)| {
            if i == 0 || line.is_empty() {
                line.to_string()
            } else {
                format!("{pad}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Indent every non-empty line of a rendered snippet by `indent`, for a freshly
/// appended block that occupies its own lines.
fn indent_block(rendered: &str, indent: usize) -> String {
    let pad = " ".repeat(indent);
    rendered
        .trim_end_matches('\n')
        .split('\n')
        .map(|line| {
            if line.is_empty() {
                line.to_string()
            } else {
                format!("{pad}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The byte offset of the start of the line containing `byte`.
fn line_start(src: &str, byte: usize) -> usize {
    src[..byte].rfind('\n').map(|i| i + 1).unwrap_or(0)
}

/// Splice ordered, non-overlapping edits into `src`.
fn apply_edits(src: &str, mut edits: Vec<Edit>) -> Result<String, String> {
    edits.sort_by_key(|e| e.range.start);
    let mut last_end = 0usize;
    for e in &edits {
        if e.range.start < last_end {
            return Err("overlapping edits".to_string());
        }
        last_end = last_end.max(e.range.end);
    }
    let mut out = String::with_capacity(src.len());
    let mut cursor = 0usize;
    for e in edits {
        out.push_str(&src[cursor..e.range.start]);
        out.push_str(&e.text);
        cursor = e.range.end;
    }
    out.push_str(&src[cursor..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Debug helper: dump the tree-sitter-yaml CST so node kinds and field names
    /// are verified empirically rather than assumed.
    #[test]
    #[ignore]
    fn dump_tree() {
        let src = "# header\nchecks:\n  # note\n  frontend:\n    command: vitest\n    when: review\n  typecheck:\n    command: tsc\nsetupCommands:\n- npm install\n";
        let ast = engine::parse(src, SupportLang::Yaml);
        fn walk(n: &SymbolNode, depth: usize, src: &str) {
            let range = n.range();
            let text = &src[range.clone()];
            let snippet: String = text.chars().take(20).collect();
            eprintln!(
                "{}{} [{}..{}] {:?}",
                "  ".repeat(depth),
                n.kind().as_ref(),
                range.start,
                range.end,
                snippet
            );
            for child in n.children() {
                walk(&child, depth + 1, src);
            }
        }
        walk(&ast.root(), 0, src);
    }

    fn managed() -> &'static [&'static str] {
        &[
            "setupCommands",
            "terminalCommands",
            "checks",
            "defaultBranch",
            "references",
            "worktree",
            "activeBackend",
            "backends",
            "mcpServers",
            "ciCommands",
            "copyFiles",
        ]
    }

    fn map_of(yaml: &str) -> Mapping {
        match serde_yaml::from_str::<Value>(yaml).unwrap() {
            Value::Mapping(m) => m,
            _ => panic!("not a mapping"),
        }
    }

    #[test]
    fn noop_save_is_byte_identical() {
        let original = "# Cairn Project Configuration\nchecks:\n  # frontend check\n  frontend:\n    command: vitest related {changedFiles}\n    when: review\n  typecheck:\n    command: tsc --noEmit\nsetupCommands:\n- npm install\n";
        // Target equals the on-disk semantics.
        let target = map_of(
            "checks:\n  frontend:\n    command: vitest related {changedFiles}\n    when: review\n  typecheck:\n    command: tsc --noEmit\nsetupCommands:\n- npm install\n",
        );
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert_eq!(out, original, "no-op save must be byte-identical");
    }

    #[test]
    fn scoped_edit_preserves_sibling_comments() {
        let original = "# Cairn Project Configuration\nchecks:\n  # frontend runs vitest\n  frontend:\n    command: vitest related {changedFiles}\n    when: review\n  # typecheck runs tsc\n  typecheck:\n    command: tsc --noEmit\n";
        let target = map_of(
            "checks:\n  frontend:\n    command: NEWCMD\n    when: review\n  typecheck:\n    command: tsc --noEmit\n",
        );
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(out.contains("# frontend runs vitest"), "out:\n{out}");
        assert!(out.contains("# typecheck runs tsc"), "out:\n{out}");
        assert!(out.contains("command: NEWCMD"), "out:\n{out}");
        assert!(out.contains("command: tsc --noEmit"), "out:\n{out}");
        assert!(out.contains("when: review"), "out:\n{out}");
        assert!(out.contains("# Cairn Project Configuration"), "out:\n{out}");
        // The change is scoped: the typecheck subtree bytes are untouched.
        assert!(
            !out.contains("vitest related"),
            "old command should be gone"
        );
        // Result is still valid and semantically correct.
        let reparsed = map_of(&out);
        assert_eq!(reparsed, target);
    }

    #[test]
    fn adds_a_check() {
        let original =
            "checks:\n  frontend:\n    command: vitest\n    when: write\n    policy: advisory\n";
        let target = map_of(
            "checks:\n  frontend:\n    command: vitest\n    when: write\n    policy: advisory\n  lint:\n    command: eslint\n    when: write\n    policy: advisory\n",
        );
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(out.contains("frontend:"));
        assert!(out.contains("lint:"));
        assert_eq!(map_of(&out), target);
    }

    #[test]
    fn removes_a_check_with_attached_comment() {
        let original = "checks:\n  # frontend check\n  frontend:\n    command: vitest\n  # typecheck check\n  typecheck:\n    command: tsc\n";
        let target = map_of("checks:\n  typecheck:\n    command: tsc\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(!out.contains("frontend"), "out:\n{out}");
        assert!(
            !out.contains("# frontend check"),
            "orphaned comment should be removed with its entry:\n{out}"
        );
        assert!(out.contains("# typecheck check"), "out:\n{out}");
        assert!(out.contains("typecheck:"), "out:\n{out}");
        assert_eq!(map_of(&out), target);
    }

    #[test]
    fn clears_top_level_key_when_absent() {
        let original =
            "# header\nsetupCommands:\n- npm install\nchecks:\n  frontend:\n    command: vitest\n";
        // Target drops `checks` entirely (empty checks → None → key absent).
        let target = map_of("setupCommands:\n- npm install\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(!out.contains("checks"), "out:\n{out}");
        assert!(out.contains("setupCommands"), "out:\n{out}");
        assert!(out.contains("# header"), "out:\n{out}");
        assert_eq!(map_of(&out), target);
    }

    #[test]
    fn preserves_unknown_top_level_key() {
        let original = "customUserKey: keep-me\nsetupCommands:\n- npm install\n";
        // Target has no knowledge of customUserKey (not a schema field).
        let target = map_of("setupCommands:\n- npm ci\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(out.contains("customUserKey: keep-me"), "out:\n{out}");
        assert!(out.contains("npm ci"), "out:\n{out}");
    }

    #[test]
    fn migrates_legacy_top_level_key_keeping_comments() {
        let original =
            "# Cairn Project Configuration\nciCommands:\n- old ci\nsetupCommands:\n- npm install\n";
        // Migrated target drops the legacy ciCommands key.
        let target = map_of("setupCommands:\n- npm install\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(!out.contains("ciCommands"), "out:\n{out}");
        assert!(out.contains("# Cairn Project Configuration"), "out:\n{out}");
        assert!(out.contains("setupCommands"), "out:\n{out}");
        assert_eq!(map_of(&out), target);
    }

    #[test]
    fn removes_nested_legacy_key() {
        let original = "worktree:\n  seedIgnored: true\n  populate:\n    copy:\n    - .env\n";
        let target = map_of("worktree:\n  populate:\n    copy:\n    - .env\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert!(!out.contains("seedIgnored"), "out:\n{out}");
        assert!(out.contains("populate"), "out:\n{out}");
        assert_eq!(map_of(&out), target);
    }

    #[test]
    fn bails_on_multi_document() {
        let original = "a: 1\n---\nb: 2\n";
        let target = map_of("a: 2\n");
        assert!(merge_into_yaml(original, &target, managed()).is_err());
    }

    #[test]
    fn bails_on_anchors() {
        let original = "base: &base\n  x: 1\nchild:\n  <<: *base\n";
        let target = map_of("base:\n  x: 2\n");
        assert!(merge_into_yaml(original, &target, managed()).is_err());
    }

    #[test]
    fn changed_sequence_replaced_wholesale() {
        let original = "setupCommands:\n- npm install\n- npm test\n";
        let target = map_of("setupCommands:\n- bun install\n");
        let out = merge_into_yaml(original, &target, managed()).unwrap();
        assert_eq!(map_of(&out), target);
        assert!(out.contains("bun install"), "out:\n{out}");
        assert!(!out.contains("npm test"), "out:\n{out}");
    }
}
