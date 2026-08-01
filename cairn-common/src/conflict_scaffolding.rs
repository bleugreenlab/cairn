//! jj conflict scaffolding: the paths jj writes into a git tree when a
//! conflict-flagged commit is exported, and the predicate that refuses such a
//! tree at Cairn's boundaries.
//!
//! jj's conflict representation was designed to be consumed by a jj *workspace*,
//! which materializes a conflict as inline markers for a human to resolve. Cairn
//! gives agents plain detached git worktrees, so nothing ever materializes those
//! markers. What `jj git export` writes into git for a conflict-flagged commit is
//! something else entirely, and far more dangerous (measured on jj 0.42): three
//! root-level sidecar directories (`.jjconflict-base-0`, `.jjconflict-side-0`,
//! `.jjconflict-side-1`) plus a `JJ-CONFLICT-README`, and at the TOP LEVEL the
//! destination side of every conflicted file, verbatim, with no markers at all.
//!
//! So the top-level tree silently holds one side. A checkout of that ref looks
//! like an ordinary resolved merge, which means the absence of conflict markers
//! is not evidence that the tree is trustworthy — that false signal is what let a
//! residue cleanup delete the only surviving copy of a branch's real work and
//! land half a design on the default branch.
//!
//! The scaffolding always sits at the tree ROOT, so detection is one
//! `git ls-tree --name-only <commit>` (no recursive walk) fed to
//! [`conflict_scaffolding_in_root_listing`].
//!
//! # The other half: literal conflict markers
//!
//! The sidecar guard above covers scaffolding jj *exports*. It says nothing
//! about the ordinary case of a file whose text contains `<<<<<<<` — which is
//! what an interactive resolution session puts on disk on purpose, and what a
//! half-finished resolution would otherwise commit. Both carriers of a Cairn
//! commit (`write` with a `commit_msg`, and `run`'s post-batch commit barrier)
//! screen their complete resulting file content through
//! [`conflict_markers_in_content`] before anything is sealed, so conflict
//! scaffolding is EPHEMERAL: it may live in a working tree for the length of a
//! resolution, and it can never become durable history.
//!
//! The scan reads complete resulting content rather than a patch's added lines,
//! because the dangerous shape is a marker the batch did not itself write — a
//! materialized conflict the agent edited around, or a generated file that
//! inherited one. A patch-scoped scan would wave both through.

/// The marker file jj writes beside the sidecar directories.
pub const JJ_CONFLICT_README: &str = "JJ-CONFLICT-README";
/// Prefix of the per-conflict merge-base sidecar directories.
pub const JJ_CONFLICT_BASE_PREFIX: &str = ".jjconflict-base-";
/// Prefix of the per-conflict side sidecar directories.
pub const JJ_CONFLICT_SIDE_PREFIX: &str = ".jjconflict-side-";

/// Whether one root-level tree entry name is jj conflict scaffolding.
pub fn is_conflict_scaffolding_entry(name: &str) -> bool {
    let name = name.trim_end_matches('/');
    name == JJ_CONFLICT_README
        || name.starts_with(JJ_CONFLICT_BASE_PREFIX)
        || name.starts_with(JJ_CONFLICT_SIDE_PREFIX)
}

/// The scaffolding entries present in a `git ls-tree --name-only <commit>`
/// listing of a tree ROOT, in listing order. Empty means the tree is free of jj
/// conflict scaffolding — which is necessary for the tree to be materializable,
/// though never sufficient for it to be *correct* (see the module docs).
pub fn conflict_scaffolding_in_root_listing(listing: &str) -> Vec<String> {
    listing
        .lines()
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .filter(|entry| is_conflict_scaffolding_entry(entry))
        .map(ToOwned::to_owned)
        .collect()
}

/// The operator-facing refusal for a commit whose tree carries scaffolding.
///
/// This is a diagnostic, not an agent instruction: the repair is store-side
/// (the branch must be rebuilt on clean content), and nothing an agent can do
/// inside a materialized checkout addresses it.
pub fn conflict_scaffolding_refusal(what: &str, commit: &str, entries: &[String]) -> String {
    format!(
        "refusing to {what} commit {commit}: its tree carries jj conflict scaffolding ({}). \
         Exporting a conflict-flagged commit writes the destination side of every conflicted \
         file into the top-level tree with no markers, so this tree is not a resolved merge. \
         The branch needs repair in the store before it can be used.",
        entries.join(", ")
    )
}

// ── Literal conflict markers in file content ─────────────────────────────────

/// The canonical Git conflict-marker prefixes, at the default marker size.
///
/// `|||||||` is the diff3/zdiff3 base section, which jj materializes by default;
/// a scanner that knew only the classic three would wave a diff3 conflict's base
/// section through and call the file resolved.
pub const CONFLICT_MARKER_PREFIXES: [&str; 4] = ["<<<<<<<", "|||||||", "=======", ">>>>>>>"];

/// The shortest run of a marker character Git will ever emit.
///
/// Git's `conflict-marker-size` attribute makes the emitted length configurable
/// PER PATH, via a `.gitattributes` this scanner does not read and could not
/// read for every repository it guards. Seven is the default and the floor, so
/// matching "seven or more" is the only rule that holds for every repository:
/// pinning it to exactly seven would seal a genuine conflict in any tree that
/// raised the size, which is precisely the failure this boundary exists to
/// prevent.
///
/// The cost is that a long ASCII rule or a setext underline of seven or more
/// `=` now reads as a marker. That is the right side to err on — a spurious
/// refusal names its file and line and offers an audited escape, while a missed
/// marker becomes history.
pub const MIN_CONFLICT_MARKER_SIZE: usize = 7;

/// The canonical marker a line opens with, or `None`.
///
/// Git's own rule, generalized over [`MIN_CONFLICT_MARKER_SIZE`]: a run of at
/// least seven of one marker character, at the start of the line, followed by a
/// space or the end of the line. So `=======` and `============` are both
/// markers, `======` is not, and an indented or mid-line run is ordinary text.
/// Trailing `\r` is tolerated so a CRLF checkout scans the same as an LF one.
///
/// The returned name is always the canonical seven-character form, whatever
/// length was actually found — it labels which marker this is, not how long it
/// was.
pub fn conflict_marker_prefix(line: &str) -> Option<&'static str> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    let first = line.chars().next()?;
    let canonical = CONFLICT_MARKER_PREFIXES
        .into_iter()
        .find(|prefix| prefix.starts_with(first))?;
    let run = line
        .chars()
        .take_while(|character| *character == first)
        .count();
    if run < MIN_CONFLICT_MARKER_SIZE {
        return None;
    }
    let rest = &line[run..];
    (rest.is_empty() || rest.starts_with(' ')).then_some(canonical)
}

/// One conflict-marker line located in a file's complete resulting content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictMarkerHit {
    /// Repository-relative path of the offending file.
    pub path: String,
    /// 1-based line number, so the refusal addresses a place the agent can open.
    pub line: usize,
    /// The marker prefix found on that line.
    pub marker: &'static str,
}

/// Every conflict-marker line in one file's complete resulting content.
///
/// Content that is not valid UTF-8 is binary and yields nothing: a marker is a
/// text construct, and lossy-decoding a binary blob to hunt for one only invents
/// false positives in a default-deny guard.
pub fn conflict_markers_in_content(path: &str, content: &[u8]) -> Vec<ConflictMarkerHit> {
    let Ok(text) = std::str::from_utf8(content) else {
        return Vec::new();
    };
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            conflict_marker_prefix(line).map(|marker| ConflictMarkerHit {
                path: path.to_string(),
                line: index + 1,
                marker,
            })
        })
        .collect()
}

/// Render the hits as `path:line (marker)` entries grouped one line per file, so
/// a multi-file refusal reads as a work list rather than a wall.
fn render_marker_hits(hits: &[ConflictMarkerHit]) -> String {
    let mut by_path: Vec<(&str, Vec<&ConflictMarkerHit>)> = Vec::new();
    for hit in hits {
        match by_path.iter_mut().find(|(path, _)| *path == hit.path) {
            Some((_, entries)) => entries.push(hit),
            None => by_path.push((hit.path.as_str(), vec![hit])),
        }
    }
    by_path
        .into_iter()
        .map(|(path, entries)| {
            let lines = entries
                .iter()
                .map(|hit| format!("{}:{}", hit.line, hit.marker))
                .collect::<Vec<_>>()
                .join(", ");
            format!("  {path} — {lines}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Where the marker-bearing content the guard refused actually lives.
///
/// The two commit carriers differ, and the refusal must not blur them: telling
/// an agent to "resolve the markers in your tree" when the markers only ever
/// existed inside a rejected batch is an instruction to act on state the
/// machinery never made true.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerSource {
    /// The content came from the refused `write` batch itself. An in-worktree
    /// `write` publishes a logical tree and never lands on disk, so a refusal
    /// leaves nothing behind: the markers exist only in the call.
    ProposedContent,
    /// The files are on disk in the agent's checkout — a `run` batch wrote them,
    /// or a resolution session materialized them — and the refusal deliberately
    /// left them there.
    WorkingTree,
}

impl MarkerSource {
    /// The sentence naming what survived the refusal and what to do next.
    fn disposition(self) -> &'static str {
        match self {
            Self::ProposedContent => {
                "Nothing was committed, and because an in-worktree write publishes a tree rather \
                 than touching your checkout, nothing was written to disk either — the markers \
                 exist only in this call. Send the content again with every marker line removed."
            }
            Self::WorkingTree => {
                "Nothing was committed and your working tree was left exactly as it is, so the \
                 resolution in progress is intact. Resolve every marker above — keep the content \
                 you want and delete the marker lines — then commit again."
            }
        }
    }
}

/// The refusal for a commit whose resulting content still carries conflict
/// markers.
///
/// Unlike [`conflict_scaffolding_refusal`] — a diagnostic about a store-side
/// repair no agent can perform — this one is an agent instruction, so it states
/// exactly what the refusal left behind (see [`MarkerSource`]) before asking for
/// anything.
///
/// `escape_key` is the request field that carries a written reason for an
/// intentional literal marker (a doc or a fixture). It is named in the refusal
/// rather than hidden, because the alternative to a visible escape is an agent
/// working around an unexplained refusal by mangling the very text it meant to
/// write.
pub fn conflict_marker_refusal(
    what: &str,
    hits: &[ConflictMarkerHit],
    escape_key: &str,
    source: MarkerSource,
) -> String {
    let files = hits
        .iter()
        .map(|hit| hit.path.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    format!(
        "Refusing to {what}: {} file(s) contain Git conflict markers, and conflict scaffolding \
         must never become durable history.\n{}\n{} If a marker is a deliberate literal example in \
         documentation or a test fixture, re-send this call with `{escape_key}` set to a short \
         reason and it will be recorded with the commit.",
        files,
        render_marker_hits(hits),
        source.disposition()
    )
}

/// The audit line appended to a commit result when the marker guard was
/// deliberately bypassed. The bypass is never silent: whoever reads the result
/// sees that markers landed and why.
pub fn conflict_marker_bypass_note(hits: &[ConflictMarkerHit], reason: &str) -> String {
    let files = hits
        .iter()
        .map(|hit| hit.path.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    format!(
        "⚠️ Conflict-marker guard bypassed for {files} file(s) — reason: {reason}\n{}",
        render_marker_hits(hits)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact root listing jj 0.42 produces for an exported conflict-flagged
    /// commit, captured from a scratch store: three sidecar directories, the
    /// README, and the ordinary files whose top-level content is one silent side.
    const CONFLICTED_ROOT: &str = ".jjconflict-base-0\n.jjconflict-side-0\n.jjconflict-side-1\nJJ-CONFLICT-README\nother.txt\nshared.txt\n";

    #[test]
    fn every_scaffolding_entry_of_a_real_conflicted_export_is_detected() {
        assert_eq!(
            conflict_scaffolding_in_root_listing(CONFLICTED_ROOT),
            vec![
                ".jjconflict-base-0",
                ".jjconflict-side-0",
                ".jjconflict-side-1",
                "JJ-CONFLICT-README",
            ]
        );
    }

    #[test]
    fn a_clean_tree_carries_no_scaffolding() {
        assert!(conflict_scaffolding_in_root_listing("src\nCargo.toml\nother.txt\n").is_empty());
        assert!(conflict_scaffolding_in_root_listing("").is_empty());
    }

    /// A multi-conflict export numbers its sidecars past zero, and `ls-tree`
    /// variants may render a directory with a trailing slash. Both are the same
    /// scaffolding.
    #[test]
    fn higher_numbered_and_slash_suffixed_sidecars_are_scaffolding() {
        assert!(is_conflict_scaffolding_entry(".jjconflict-side-7"));
        assert!(is_conflict_scaffolding_entry(".jjconflict-base-12/"));
        assert!(is_conflict_scaffolding_entry("JJ-CONFLICT-README"));
    }

    /// Ordinary paths that merely resemble the markers are not scaffolding — the
    /// predicate gates materialization, so a false positive strands a cell.
    #[test]
    fn lookalike_paths_are_not_scaffolding() {
        assert!(!is_conflict_scaffolding_entry("jjconflict-side-0"));
        assert!(!is_conflict_scaffolding_entry(".jjconflict"));
        assert!(!is_conflict_scaffolding_entry("docs/JJ-CONFLICT-README"));
        assert!(!is_conflict_scaffolding_entry("JJ-CONFLICT-README.md"));
    }

    /// A real `git merge` conflict, diff3 style (jj's default materialization),
    /// captured verbatim. All four marker kinds appear, and the guard must see
    /// every one — a scanner blind to `|||||||` would call a diff3 conflict's
    /// base section ordinary text.
    const DIFF3_CONFLICT: &str = "fn main() {\n<<<<<<< HEAD\n    ours();\n||||||| base\n    original();\n=======\n    theirs();\n>>>>>>> main\n}\n";

    #[test]
    fn every_marker_of_a_real_diff3_conflict_is_located() {
        let hits = conflict_markers_in_content("src/main.rs", DIFF3_CONFLICT.as_bytes());
        assert_eq!(
            hits.iter()
                .map(|hit| (hit.line, hit.marker))
                .collect::<Vec<_>>(),
            vec![
                (2, "<<<<<<<"),
                (4, "|||||||"),
                (6, "======="),
                (8, ">>>>>>>")
            ],
        );
        assert!(hits.iter().all(|hit| hit.path == "src/main.rs"));
    }

    /// The half-resolved shape is the one that matters most: an agent deleted
    /// the opening marker and the separator but left the tail. Requiring a
    /// complete marker triple would wave exactly this through — a file carrying
    /// both sides' text with no sign of it.
    #[test]
    fn a_partially_resolved_file_is_still_refused() {
        let hits = conflict_markers_in_content("a.rs", b"ours();\ntheirs();\n>>>>>>> main\n");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].line, 3);
    }

    /// A real conflict from a repository whose `.gitattributes` sets
    /// `conflict-marker-size=12`, captured verbatim from `git merge`.
    ///
    /// Git makes marker length configurable per path, so a scanner pinned to
    /// exactly seven characters seals this file and publishes a live conflict as
    /// history — the one outcome this whole boundary exists to prevent. Cairn
    /// cannot know every guarded repository's setting, so it matches the floor
    /// and up.
    const LONG_MARKER_CONFLICT: &str =
        "line1\n<<<<<<<<<<<< HEAD\nOURS\n============\nTHEIRS\n>>>>>>>>>>>> other\n";

    #[test]
    fn a_conflict_using_a_configured_larger_marker_size_is_caught() {
        let hits = conflict_markers_in_content("f.txt", LONG_MARKER_CONFLICT.as_bytes());
        assert_eq!(
            hits.iter()
                .map(|hit| (hit.line, hit.marker))
                .collect::<Vec<_>>(),
            vec![(2, "<<<<<<<"), (4, "======="), (6, ">>>>>>>")],
            "a 12-character marker is the same marker, reported under its canonical name"
        );
    }

    /// The false-positive boundary, and the reason default-deny is safe. The
    /// rule is a run of at least seven of ONE marker character, at the start of
    /// the line, followed by a space or the line's end. Everything short,
    /// indented, mixed, or mid-line is ordinary text.
    #[test]
    fn lookalike_lines_are_not_markers() {
        assert!(conflict_marker_prefix("======").is_none(), "6 chars");
        assert!(
            conflict_marker_prefix("  <<<<<<< HEAD").is_none(),
            "indented"
        );
        assert!(conflict_marker_prefix("x >>>>>>> y").is_none(), "mid-line");
        assert!(
            conflict_marker_prefix("=======x").is_none(),
            "a run must end at a space or the line's end"
        );
        assert!(
            conflict_marker_prefix("<<<<<<=").is_none(),
            "a mixed run is not a marker of either kind"
        );
        assert!(conflict_marker_prefix("").is_none(), "empty line");
        assert!(conflict_marker_prefix("    ").is_none(), "blank line");
    }

    /// The deliberate cost of matching every configured marker size: a long rule
    /// of `=` now reads as a marker. Asserted rather than left implicit, because
    /// it is the trade this guard consciously makes — a spurious refusal names
    /// its file and offers an audited escape, while a missed marker becomes
    /// history. The census taken before enforcement found no such line anywhere
    /// in the tree.
    #[test]
    fn a_long_ascii_rule_is_treated_as_a_marker_by_design() {
        assert_eq!(conflict_marker_prefix("========"), Some("======="));
        assert_eq!(
            conflict_marker_prefix("========================="),
            Some("=======")
        );
    }

    #[test]
    fn a_bare_marker_and_a_labelled_marker_both_count() {
        assert_eq!(conflict_marker_prefix("======="), Some("======="));
        assert_eq!(conflict_marker_prefix("<<<<<<< HEAD"), Some("<<<<<<<"));
        // A CRLF checkout scans identically to an LF one.
        assert_eq!(conflict_marker_prefix("=======\r"), Some("======="));
    }

    /// Binary content yields nothing. Lossy-decoding a blob to hunt for a text
    /// construct can only invent false positives in a default-deny guard, and a
    /// spurious refusal would strand a legitimate commit.
    #[test]
    fn binary_content_is_not_scanned() {
        let mut blob = vec![0x00, 0xff, 0xfe];
        blob.extend_from_slice(b"\n<<<<<<< HEAD\n");
        assert!(conflict_markers_in_content("logo.png", &blob).is_empty());
    }

    #[test]
    fn clean_content_yields_no_hits() {
        assert!(conflict_markers_in_content("a.rs", b"fn main() {}\n").is_empty());
        assert!(conflict_markers_in_content("empty", b"").is_empty());
    }

    /// The refusal has to be actionable on its own: every file, every line, the
    /// fact that the tree was NOT rolled back, and the escape's name.
    #[test]
    fn the_marker_refusal_names_every_file_line_and_the_escape() {
        let hits = vec![
            ConflictMarkerHit {
                path: "a.rs".into(),
                line: 2,
                marker: "<<<<<<<",
            },
            ConflictMarkerHit {
                path: "a.rs".into(),
                line: 9,
                marker: ">>>>>>>",
            },
            ConflictMarkerHit {
                path: "b.rs".into(),
                line: 4,
                marker: "=======",
            },
        ];
        let message = conflict_marker_refusal(
            "commit",
            &hits,
            "conflict_markers_reason",
            MarkerSource::WorkingTree,
        );
        assert!(message.contains("a.rs — 2:<<<<<<<, 9:>>>>>>>"), "{message}");
        assert!(message.contains("b.rs — 4:======="), "{message}");
        assert!(message.contains("2 file(s)"), "{message}");
        assert!(
            message.contains("left exactly as it is"),
            "the refusal must say the resolution in progress survived: {message}"
        );
        assert!(message.contains("conflict_markers_reason"), "{message}");
    }

    /// The standing rule the two carriers differ on: a refused in-worktree
    /// `write` never touched disk, so the refusal must not send the agent
    /// looking for markers in a tree that does not have them.
    #[test]
    fn the_proposed_content_refusal_does_not_claim_markers_are_on_disk() {
        let hits = vec![ConflictMarkerHit {
            path: "a.rs".into(),
            line: 2,
            marker: "<<<<<<<",
        }];
        let message = conflict_marker_refusal(
            "commit",
            &hits,
            "conflict_markers_reason",
            MarkerSource::ProposedContent,
        );
        assert!(
            message.contains("exist only in this call"),
            "the refusal must place the markers in the call, not the tree: {message}"
        );
        assert!(
            !message.contains("resolution in progress"),
            "a logical write has no resolution session to preserve: {message}"
        );
    }

    #[test]
    fn the_bypass_note_records_the_reason_and_the_files() {
        let hits = vec![ConflictMarkerHit {
            path: "docs/conflicts.md".into(),
            line: 12,
            marker: "<<<<<<<",
        }];
        let note = conflict_marker_bypass_note(&hits, "documenting marker syntax");
        assert!(note.contains("documenting marker syntax"), "{note}");
        assert!(note.contains("docs/conflicts.md — 12:<<<<<<<"), "{note}");
    }

    #[test]
    fn the_refusal_names_the_commit_and_the_offending_entries() {
        let message = conflict_scaffolding_refusal(
            "materialize",
            "deadbeef",
            &[".jjconflict-side-0".to_string()],
        );
        assert!(message.contains("deadbeef"));
        assert!(message.contains(".jjconflict-side-0"));
        assert!(message.contains("materialize"));
    }
}
