//! Unified diff application for file changes submitted through the `write` verb.
//!
//! Parses and applies standard unified diffs (`@@ -start,count +start,count @@`)
//! and supported patch envelopes to file content.

const ACCEPTED_PATCH_FORMATS_ERROR: &str = "patch text must be a unified diff with `@@ -old,+new @@` hunks or a supported `*** Begin Patch` envelope";

fn is_hunk_header(line: &str) -> bool {
    line == "@@" || line.starts_with("@@ ")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchEnvelopeFileChange {
    Add { path: String, content: String },
    Update { path: String, diff: String },
    Delete { path: String },
}

/// Apply a unified diff to file content.
/// The diff must be for a single file. Multi-file diffs are rejected.
/// Returns the patched content on success.
pub(crate) fn apply_unified_diff(content: &str, diff: &str) -> Result<String, String> {
    let hunks = parse_hunks(diff)?;

    if hunks.is_empty() {
        return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
    }

    let mut lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();

    if hunks.iter().any(|hunk| hunk.contextual) {
        for hunk in &hunks {
            lines = apply_hunk(&lines, hunk)?;
        }
    } else {
        // Apply hunks in reverse order so line numbers stay valid.
        let mut sorted_hunks = hunks;
        sorted_hunks.sort_by(|a, b| b.old_start.cmp(&a.old_start));

        for hunk in &sorted_hunks {
            lines = apply_hunk(&lines, hunk)?;
        }
    }

    let mut result = lines.join("\n");

    // Preserve trailing newline if original had one
    if content.ends_with('\n') && !result.ends_with('\n') {
        result.push('\n');
    }

    Ok(result)
}

/// Normalize supported patch payloads into a canonical single-file unified diff body.
pub(crate) fn normalize_single_file_patch(
    diff: &str,
    expected_path: &str,
) -> Result<String, String> {
    if diff.trim().is_empty() {
        return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
    }

    if diff.trim_start().starts_with("*** Begin Patch") {
        normalize_patch_envelope(diff, expected_path)
    } else {
        validate_unified_diff(diff, expected_path)?;
        Ok(diff.to_string())
    }
}

/// Validate that a diff is for a single file and matches the expected path.
#[cfg_attr(not(test), allow(dead_code))]
fn validate_single_file_diff(diff: &str, expected_path: &str) -> Result<(), String> {
    normalize_single_file_patch(diff, expected_path).map(|_| ())
}

fn validate_unified_diff(diff: &str, expected_path: &str) -> Result<(), String> {
    let mut file_count = 0;
    let mut saw_hunk = false;

    for line in diff.lines() {
        if is_hunk_header(line) {
            saw_hunk = true;
        }

        if let Some(path) = line
            .strip_prefix("--- ")
            .or_else(|| line.strip_prefix("+++ "))
            .map(normalize_diff_path)
        {
            if !path_matches_expected(path, expected_path) {
                return Err(format!(
                    "Diff is for '{}' but expected '{}'",
                    path, expected_path
                ));
            }

            if line.starts_with("--- ") {
                file_count += 1;
                if file_count > 1 {
                    return Err(
                        "Multi-file diffs are not supported. Submit one change per file."
                            .to_string(),
                    );
                }
            }
        }

        if line.starts_with("diff --git") && file_count > 0 {
            return Err(
                "Multi-file diffs are not supported. Submit one change per file.".to_string(),
            );
        }
    }

    if !saw_hunk {
        return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
    }

    Ok(())
}

pub(crate) fn parse_patch_envelope(diff: &str) -> Result<Vec<PatchEnvelopeFileChange>, String> {
    if diff.trim().is_empty() {
        return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
    }

    let lines = diff.trim().lines().collect::<Vec<_>>();
    if !matches!(lines.first(), Some(&"*** Begin Patch")) {
        return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
    }

    let mut changes = Vec::new();
    let mut index = 1;
    let mut saw_end = false;

    while index < lines.len() {
        let line = lines[index];
        if line == "*** End Patch" {
            saw_end = true;
            index += 1;
            break;
        }
        if line.trim().is_empty() {
            index += 1;
            continue;
        }

        let (kind, path) = if let Some(path) = line.strip_prefix("*** Add File: ") {
            ("add", path.trim())
        } else if let Some(path) = line.strip_prefix("*** Update File: ") {
            ("update", path.trim())
        } else if let Some(path) = line.strip_prefix("*** Delete File: ") {
            ("delete", path.trim())
        } else if line.starts_with("*** ") {
            return Err(format!("unsupported patch envelope section: {line}"));
        } else {
            return Err("malformed patch envelope: expected a file section".to_string());
        };

        if path.is_empty() {
            return Err("malformed patch envelope: file section path is empty".to_string());
        }

        index += 1;
        let mut body = Vec::new();
        while index < lines.len() {
            let body_line = lines[index];
            if body_line == "*** End Patch"
                || body_line.starts_with("*** Add File: ")
                || body_line.starts_with("*** Update File: ")
                || body_line.starts_with("*** Delete File: ")
            {
                break;
            }
            if body_line.starts_with("*** ") {
                return Err(format!("unsupported patch envelope section: {body_line}"));
            }
            body.push(body_line);
            index += 1;
        }

        match kind {
            "add" => {
                let mut content_lines = Vec::with_capacity(body.len());
                for line in body {
                    let Some(content) = line.strip_prefix('+') else {
                        return Err(
                            "malformed add file section: content lines must start with `+`"
                                .to_string(),
                        );
                    };
                    content_lines.push(content);
                }
                changes.push(PatchEnvelopeFileChange::Add {
                    path: path.to_string(),
                    content: content_lines.join("\n"),
                });
            }
            "update" => {
                let patch = body.join("\n");
                if !body.iter().any(|line| is_hunk_header(line)) {
                    return Err(ACCEPTED_PATCH_FORMATS_ERROR.to_string());
                }
                changes.push(PatchEnvelopeFileChange::Update {
                    path: path.to_string(),
                    diff: patch,
                });
            }
            "delete" => {
                if body.iter().any(|line| !line.trim().is_empty()) {
                    return Err("malformed delete file section: unexpected body".to_string());
                }
                changes.push(PatchEnvelopeFileChange::Delete {
                    path: path.to_string(),
                });
            }
            _ => unreachable!(),
        }
    }

    if !saw_end {
        return Err("malformed patch envelope: missing `*** End Patch`".to_string());
    }
    if lines[index..].iter().any(|line| !line.trim().is_empty()) {
        return Err(
            "malformed patch envelope: unexpected content after `*** End Patch`".to_string(),
        );
    }
    if changes.is_empty() {
        return Err("malformed patch envelope: expected at least one file section".to_string());
    }

    Ok(changes)
}

fn normalize_patch_envelope(diff: &str, expected_path: &str) -> Result<String, String> {
    let changes = parse_patch_envelope(diff)?;
    if changes.len() != 1 {
        return Err("multi-file envelopes require mode=unified_patch".to_string());
    }
    match changes.into_iter().next().unwrap() {
        PatchEnvelopeFileChange::Update { path, diff } => {
            if !path_matches_expected(&path, expected_path) {
                return Err(format!(
                    "envelope target path does not match change.target ('{}' != '{}')",
                    path, expected_path
                ));
            }
            Ok(diff)
        }
        PatchEnvelopeFileChange::Add { .. } | PatchEnvelopeFileChange::Delete { .. } => {
            Err("add/delete envelopes require mode=unified_patch".to_string())
        }
    }
}

fn normalize_diff_path(path: &str) -> &str {
    path.trim()
        .strip_prefix("a/")
        .or_else(|| path.trim().strip_prefix("b/"))
        .unwrap_or(path.trim())
}

fn path_matches_expected(path: &str, expected_path: &str) -> bool {
    path == "/dev/null"
        || path.is_empty()
        || path == expected_path
        || path.ends_with(expected_path)
        || expected_path.ends_with(path)
}

#[derive(Debug)]
struct Hunk {
    old_start: usize, // 1-based line number in original file; 0 for native contextual hunks
    old_count: usize,
    contextual: bool,
    anchor: Option<String>,
    new_lines: Vec<HunkLine>,
}

#[derive(Debug, Clone)]
enum HunkLine {
    Context(String),
    Add(String),
    Remove(String),
}

/// Parse unified diff hunks from diff text.
fn parse_hunks(diff: &str) -> Result<Vec<Hunk>, String> {
    let mut hunks = Vec::new();
    let mut current_hunk: Option<Hunk> = None;
    let mut in_header = true;

    for line in diff.lines() {
        // Skip file headers
        if line.starts_with("diff --git")
            || line.starts_with("index ")
            || line.starts_with("--- ")
            || line.starts_with("+++ ")
        {
            in_header = true;
            continue;
        }

        // Parse hunk header: @@ -old_start,old_count +new_start,new_count @@
        if is_hunk_header(line) {
            // Save previous hunk
            if let Some(hunk) = current_hunk.take() {
                hunks.push(hunk);
            }
            in_header = false;

            let header = parse_hunk_header(line)?;
            current_hunk = Some(Hunk {
                old_start: header.old_start,
                old_count: header.old_count,
                contextual: header.contextual,
                anchor: header.anchor,
                new_lines: Vec::new(),
            });
            continue;
        }

        if in_header {
            continue;
        }

        // Parse hunk content
        if let Some(ref mut hunk) = current_hunk {
            if let Some(rest) = line.strip_prefix('+') {
                hunk.new_lines.push(HunkLine::Add(rest.to_string()));
            } else if let Some(rest) = line.strip_prefix('-') {
                hunk.new_lines.push(HunkLine::Remove(rest.to_string()));
            } else if let Some(rest) = line.strip_prefix(' ') {
                hunk.new_lines.push(HunkLine::Context(rest.to_string()));
            } else if line == "\\ No newline at end of file" {
                // Ignore this marker
            } else {
                // Treat unrecognized lines as context (some diffs omit the space prefix)
                hunk.new_lines.push(HunkLine::Context(line.to_string()));
            }
        }
    }

    // Save last hunk
    if let Some(hunk) = current_hunk {
        hunks.push(hunk);
    }

    Ok(hunks)
}

struct HunkHeader {
    old_start: usize,
    old_count: usize,
    contextual: bool,
    anchor: Option<String>,
}

/// Parse @@ -start,count +start,count @@ or native apply_patch contextual headers.
fn parse_hunk_header(line: &str) -> Result<HunkHeader, String> {
    // Find the old range: -start,count or -start. Native apply_patch envelopes
    // also allow contextual headers such as `@@ fn example() {`.
    let at_idx = line.find("@@").unwrap_or(0);
    let rest = &line[at_idx + 2..];
    let range_payload = rest.trim_start();

    if !range_payload
        .strip_prefix('-')
        .and_then(|value| value.chars().next())
        .is_some_and(|next| next.is_ascii_digit())
    {
        let anchor = rest.strip_suffix("@@").unwrap_or(rest).trim().to_string();
        return Ok(HunkHeader {
            old_start: 0,
            old_count: 0,
            contextual: true,
            anchor: (!anchor.is_empty()).then_some(anchor),
        });
    }

    let plus_idx = range_payload
        .find('+')
        .ok_or_else(|| format!("Invalid hunk header (no +): {}", line))?;

    let old_range = range_payload[1..plus_idx].trim().trim_end_matches(',');

    // Parse "start,count" or just "start"
    let (old_start, old_count) = if let Some(comma) = old_range.find(',') {
        let start = old_range[..comma]
            .trim()
            .parse::<usize>()
            .map_err(|e| format!("Invalid old start: {}", e))?;
        let count = old_range[comma + 1..]
            .trim()
            .parse::<usize>()
            .map_err(|e| format!("Invalid old count: {}", e))?;
        (start, count)
    } else {
        let start = old_range
            .trim()
            .parse::<usize>()
            .map_err(|e| format!("Invalid old start: {}", e))?;
        (start, 1)
    };

    Ok(HunkHeader {
        old_start,
        old_count,
        contextual: false,
        anchor: None,
    })
}

fn consumed_old_count(hunk: &Hunk) -> usize {
    hunk.new_lines
        .iter()
        .filter(|line| !matches!(line, HunkLine::Add(_)))
        .count()
}

fn find_contextual_hunk_start(lines: &[String], hunk: &Hunk) -> Result<usize, String> {
    let old_lines = hunk
        .new_lines
        .iter()
        .filter_map(|line| match line {
            HunkLine::Context(text) | HunkLine::Remove(text) => Some(text),
            HunkLine::Add(_) => None,
        })
        .collect::<Vec<_>>();
    let anchor_line = hunk.anchor.as_deref();

    let search_start = if let Some(anchor) = anchor_line {
        lines
            .iter()
            .position(|line| line.trim() == anchor || line.contains(anchor))
            .ok_or_else(|| format!("Contextual hunk anchor did not match: {anchor}"))?
    } else {
        0
    };

    if old_lines.is_empty() {
        return Ok(search_start + usize::from(anchor_line.is_some()));
    }

    let max_start = lines.len().saturating_sub(old_lines.len());
    for start in search_start..=max_start {
        if old_lines
            .iter()
            .enumerate()
            .all(|(offset, expected)| lines[start + offset] == **expected)
        {
            return Ok(start);
        }
    }

    Err(format!(
        "Contextual hunk did not match any location{}: {}",
        anchor_line
            .map(|anchor| format!(" after anchor '{anchor}'"))
            .unwrap_or_default(),
        old_lines
            .iter()
            .map(|line| line.as_str())
            .collect::<Vec<_>>()
            .join("\\n")
    ))
}

/// Apply a single hunk to the file lines.
fn apply_hunk(lines: &[String], hunk: &Hunk) -> Result<Vec<String>, String> {
    let start_idx = if hunk.contextual {
        find_contextual_hunk_start(lines, hunk)?
    } else if hunk.old_start == 0 {
        0
    } else {
        hunk.old_start - 1
    };

    // Verify context lines match before applying
    let mut old_idx = start_idx;
    for hunk_line in &hunk.new_lines {
        match hunk_line {
            HunkLine::Context(expected) => {
                if old_idx >= lines.len() {
                    return Err(format!(
                        "Context line at {} is past end of file ({} lines)",
                        old_idx + 1,
                        lines.len()
                    ));
                }
                if lines[old_idx] != *expected {
                    return Err(format!(
                        "Context mismatch at line {}: expected '{}', got '{}'",
                        old_idx + 1,
                        expected,
                        lines[old_idx]
                    ));
                }
                old_idx += 1;
            }
            HunkLine::Remove(expected) => {
                if old_idx >= lines.len() {
                    return Err(format!(
                        "Remove line at {} is past end of file ({} lines)",
                        old_idx + 1,
                        lines.len()
                    ));
                }
                if lines[old_idx] != *expected {
                    return Err(format!(
                        "Remove mismatch at line {}: expected '{}', got '{}'",
                        old_idx + 1,
                        expected,
                        lines[old_idx]
                    ));
                }
                old_idx += 1;
            }
            HunkLine::Add(_) => {
                // Additions don't consume old lines
            }
        }
    }

    // Build result: keep lines before hunk, apply hunk, keep lines after
    let mut result = Vec::new();

    // Lines before the hunk
    result.extend_from_slice(&lines[..start_idx]);

    // Apply hunk
    for hunk_line in &hunk.new_lines {
        match hunk_line {
            HunkLine::Context(text) | HunkLine::Add(text) => {
                result.push(text.clone());
            }
            HunkLine::Remove(_) => {
                // Skip removed lines
            }
        }
    }

    // Lines after the hunk
    let end_idx = start_idx
        + if hunk.contextual {
            consumed_old_count(hunk)
        } else {
            hunk.old_count
        };
    if end_idx < lines.len() {
        result.extend_from_slice(&lines[end_idx..]);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_single_hunk() {
        let content = "line 1\nline 2\nline 3\nline 4\n";
        let diff = "@@ -2,2 +2,2 @@\n-line 2\n-line 3\n+line 2 modified\n+line 3 modified\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "line 1\nline 2 modified\nline 3 modified\nline 4\n");
    }

    #[test]
    fn test_apply_with_context() {
        let content = "a\nb\nc\nd\ne\n";
        let diff = "@@ -2,3 +2,3 @@\n b\n-c\n+C\n d\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "a\nb\nC\nd\ne\n");
    }

    #[test]
    fn test_apply_add_lines() {
        let content = "a\nb\nc\n";
        let diff = "@@ -2,1 +2,3 @@\n b\n+b1\n+b2\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "a\nb\nb1\nb2\nc\n");
    }

    #[test]
    fn test_apply_remove_lines() {
        let content = "a\nb\nc\nd\n";
        let diff = "@@ -2,2 +2,0 @@\n-b\n-c\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "a\nd\n");
    }

    #[test]
    fn test_apply_multi_hunk() {
        let content = "1\n2\n3\n4\n5\n6\n7\n8\n";
        let diff = "@@ -2,1 +2,1 @@\n-2\n+TWO\n@@ -7,1 +7,1 @@\n-7\n+SEVEN\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "1\nTWO\n3\n4\n5\n6\nSEVEN\n8\n");
    }

    #[test]
    fn test_context_mismatch_error() {
        let content = "a\nb\nc\n";
        let diff = "@@ -1,2 +1,2 @@\n a\n-x\n+y\n";

        let result = apply_unified_diff(content, diff);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Context mismatch") || err.contains("Remove mismatch"));
    }

    #[test]
    fn test_empty_diff_error() {
        let content = "a\nb\nc\n";
        let diff = "";

        let result = apply_unified_diff(content, diff);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("unified diff") || error.contains("single-file envelope"));
    }

    #[test]
    fn test_validate_single_file_diff_ok() {
        let diff = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        assert!(validate_single_file_diff(diff, "src/lib.rs").is_ok());
    }

    #[test]
    fn test_normalize_single_file_patch_accepts_unified_diff() {
        let diff = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        let normalized = normalize_single_file_patch(diff, "src/lib.rs").unwrap();
        assert_eq!(normalized, diff);
    }

    #[test]
    fn test_normalize_single_file_patch_accepts_codex_envelope() {
        let diff = "*** Begin Patch\n*** Update File: src/lib.rs\n@@ -1,2 +1,2 @@\n old\n-old\n+new\n*** End Patch\n";
        let normalized = normalize_single_file_patch(diff, "src/lib.rs").unwrap();
        assert_eq!(normalized, "@@ -1,2 +1,2 @@\n old\n-old\n+new");

        let result = apply_unified_diff("old\nold\n", &normalized).unwrap();
        assert_eq!(result, "old\nnew\n");
    }

    #[test]
    fn test_normalize_single_file_patch_rejects_malformed_envelope() {
        let diff = "*** Begin Patch\n*** Update File: src/lib.rs\n-old\n+new\n*** End Patch\n";
        let error = normalize_single_file_patch(diff, "src/lib.rs").unwrap_err();
        assert!(error.contains("unified diff") || error.contains("single-file envelope"));
    }

    #[test]
    fn test_normalize_single_file_patch_rejects_multi_file_envelope() {
        let diff = "*** Begin Patch\n*** Update File: src/lib.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n*** Update File: src/main.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n*** End Patch\n";
        let error = normalize_single_file_patch(diff, "src/lib.rs").unwrap_err();
        assert!(error.contains("multi-file envelopes require mode=unified_patch"));
    }

    #[test]
    fn test_normalize_single_file_patch_rejects_mismatched_envelope_path() {
        let diff = "*** Begin Patch\n*** Update File: src/other.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n*** End Patch\n";
        let error = normalize_single_file_patch(diff, "src/lib.rs").unwrap_err();
        assert!(error.contains("envelope target path does not match change.target"));
    }

    #[test]
    fn test_parse_patch_envelope_add_update_delete() {
        let diff = "*** Begin Patch\n*** Add File: src/new.rs\n+one\n+two\n*** Update File: src/lib.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n*** Delete File: src/old.rs\n*** End Patch\n";
        let changes = parse_patch_envelope(diff).unwrap();
        assert_eq!(
            changes,
            vec![
                PatchEnvelopeFileChange::Add {
                    path: "src/new.rs".to_string(),
                    content: "one\ntwo".to_string(),
                },
                PatchEnvelopeFileChange::Update {
                    path: "src/lib.rs".to_string(),
                    diff: "@@ -1,1 +1,1 @@\n-old\n+new".to_string(),
                },
                PatchEnvelopeFileChange::Delete {
                    path: "src/old.rs".to_string(),
                },
            ]
        );
    }

    #[test]
    fn test_parse_patch_envelope_preserves_repeated_path_order() {
        let diff = "*** Begin Patch\n*** Update File: src/lib.rs\n@@ -1,1 +1,1 @@\n-a\n+b\n*** Update File: src/lib.rs\n@@ -1,1 +1,1 @@\n-b\n+c\n*** End Patch\n";
        let changes = parse_patch_envelope(diff).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(
            changes
                .iter()
                .map(|change| match change {
                    PatchEnvelopeFileChange::Update { path, .. } => path.as_str(),
                    _ => "",
                })
                .collect::<Vec<_>>(),
            vec!["src/lib.rs", "src/lib.rs"]
        );
    }

    #[test]
    fn test_parse_patch_envelope_rejects_malformed_bodies() {
        let add = "*** Begin Patch\n*** Add File: src/new.rs\nmissing-plus\n*** End Patch\n";
        assert!(parse_patch_envelope(add)
            .unwrap_err()
            .contains("content lines"));

        let delete = "*** Begin Patch\n*** Delete File: src/old.rs\n-body\n*** End Patch\n";
        assert!(parse_patch_envelope(delete)
            .unwrap_err()
            .contains("unexpected body"));

        let update = "*** Begin Patch\n*** Update File: src/lib.rs\n-old\n+new\n*** End Patch\n";
        assert!(parse_patch_envelope(update).is_err());
    }

    #[test]
    fn test_apply_native_contextual_hunk() {
        let content = "fn example() {\n    old();\n}\n\nfn other() {}\n";
        let diff = "@@ fn example() {\n fn example() {\n-    old();\n+    new();\n }\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "fn example() {\n    new();\n}\n\nfn other() {}\n");
    }

    #[test]
    fn test_apply_native_bare_header_hunk() {
        let content = "- [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n- [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n";
        let diff = "@@\n - [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n - [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n-- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+\n+## Unified Patch Test\n+\n+Temporary change to validate Cairn `change` with unified patch style in this throwaway worktree.\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(
            result,
            "- [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n- [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n\n## Unified Patch Test\n\nTemporary change to validate Cairn `change` with unified patch style in this throwaway worktree.\n"
        );
    }

    #[test]
    fn test_apply_native_contextual_hunk_uses_header_anchor() {
        let content = "fn first() {\n    old();\n}\n\nfn second() {\n    old();\n}\n";
        let diff = "@@ fn second() {\n-    old();\n+    new();\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(
            result,
            "fn first() {\n    old();\n}\n\nfn second() {\n    new();\n}\n"
        );
    }

    #[test]
    fn test_apply_native_add_only_hunk_uses_header_anchor() {
        let content = "fn first() {\n}\n\nfn second() {\n}\n";
        let diff = "@@ fn second() {\n+    inserted();\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(
            result,
            "fn first() {\n}\n\nfn second() {\n    inserted();\n}\n"
        );
    }

    #[test]
    fn test_apply_native_contextual_hunk_anchor_allows_arrow_signature() {
        let content = "fn first() -> Result<()> {\n    old();\n}\n\nfn build() -> Result<()> {\n    old();\n}\n";
        let diff = "@@ fn build() -> Result<()> {\n-    old();\n+    new();\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(
            result,
            "fn first() -> Result<()> {\n    old();\n}\n\nfn build() -> Result<()> {\n    new();\n}\n"
        );
    }

    #[test]
    fn test_apply_native_contextual_hunk_rejects_missing_context() {
        let content = "fn other() {}\n";
        let diff = "@@ fn example() {\n fn example() {\n-    old();\n+    new();\n }\n";

        let result = apply_unified_diff(content, diff).unwrap_err();
        assert!(result.contains("Contextual hunk anchor did not match"));
    }

    #[test]
    fn test_apply_with_git_headers() {
        let content = "fn main() {\n    println!(\"hello\");\n}\n";
        let diff = "diff --git a/main.rs b/main.rs\nindex abc..def 100644\n--- a/main.rs\n+++ b/main.rs\n@@ -1,3 +1,3 @@\n fn main() {\n-    println!(\"hello\");\n+    println!(\"world\");\n }\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "fn main() {\n    println!(\"world\");\n}\n");
    }

    #[test]
    fn test_apply_new_file() {
        let content = "";
        let diff = "@@ -0,0 +1,3 @@\n+line 1\n+line 2\n+line 3\n";

        let result = apply_unified_diff(content, diff).unwrap();
        assert_eq!(result, "line 1\nline 2\nline 3");
    }

    #[test]
    fn test_validate_rejects_multi_file_diff() {
        let diff = "diff --git a/src/a.rs b/src/a.rs\n--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,1 +1,1 @@\n-old\n+new\ndiff --git a/src/b.rs b/src/b.rs\n--- a/src/b.rs\n+++ b/src/b.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        let result = validate_single_file_diff(diff, "src/a.rs");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Multi-file"));
    }

    #[test]
    fn test_validate_rejects_wrong_path() {
        let diff = "--- a/src/wrong.rs\n+++ b/src/wrong.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        let result = validate_single_file_diff(diff, "src/right.rs");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 'src/right.rs'"));
    }

    #[test]
    fn test_validate_allows_dev_null_for_new_files() {
        let diff = "--- /dev/null\n+++ b/src/new.rs\n@@ -0,0 +1,1 @@\n+hello\n";
        assert!(validate_single_file_diff(diff, "src/new.rs").is_ok());
    }

    #[test]
    fn test_apply_preserves_trailing_newline() {
        let content = "a\nb\n";
        let diff = "@@ -1,1 +1,1 @@\n-a\n+A\n";
        let result = apply_unified_diff(content, diff).unwrap();
        assert!(result.ends_with('\n'), "should preserve trailing newline");
    }

    #[test]
    fn test_apply_no_trailing_newline_when_original_lacks_it() {
        let content = "a\nb";
        let diff = "@@ -1,1 +1,1 @@\n-a\n+A\n";
        let result = apply_unified_diff(content, diff).unwrap();
        assert!(
            !result.ends_with('\n'),
            "should not add trailing newline when original lacks it"
        );
    }
}
