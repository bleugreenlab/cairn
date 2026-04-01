//! Unified diff application for the filechange tool.
//!
//! Parses and applies standard unified diffs (`@@ -start,count +start,count @@`)
//! to file content. Used by the Codex filechange handler to apply `kind=update` changes.

/// Apply a unified diff to file content.
///
/// The diff must be for a single file. Multi-file diffs are rejected.
/// Returns the patched content on success.
pub fn apply_unified_diff(content: &str, diff: &str) -> Result<String, String> {
    let hunks = parse_hunks(diff)?;

    if hunks.is_empty() {
        return Err("No hunks found in diff".to_string());
    }

    let mut lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();

    // Apply hunks in reverse order so line numbers stay valid
    let mut sorted_hunks = hunks;
    sorted_hunks.sort_by(|a, b| b.old_start.cmp(&a.old_start));

    for hunk in &sorted_hunks {
        lines = apply_hunk(&lines, hunk)?;
    }

    let mut result = lines.join("\n");

    // Preserve trailing newline if original had one
    if content.ends_with('\n') && !result.ends_with('\n') {
        result.push('\n');
    }

    Ok(result)
}

/// Validate that a diff is for a single file and matches the expected path.
pub fn validate_single_file_diff(diff: &str, expected_path: &str) -> Result<(), String> {
    let mut file_count = 0;
    for line in diff.lines() {
        if line.starts_with("--- ") || line.starts_with("+++ ") {
            // Extract path (skip "--- a/" or "+++ b/" prefixes)
            let path = line[4..]
                .trim()
                .strip_prefix("a/")
                .or_else(|| line[4..].trim().strip_prefix("b/"))
                .unwrap_or(line[4..].trim());

            if path != "/dev/null"
                && !path.is_empty()
                && !path.ends_with(expected_path)
                && !expected_path.ends_with(path)
                && path != expected_path
            {
                return Err(format!(
                    "Diff is for '{}' but expected '{}'",
                    path, expected_path
                ));
            }

            if line.starts_with("--- ") {
                file_count += 1;
            }
        }

        // Check for diff --git header indicating multiple files
        if line.starts_with("diff --git") && file_count > 0 {
            return Err(
                "Multi-file diffs are not supported. Submit one change per file.".to_string(),
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Hunk {
    old_start: usize, // 1-based line number in original file
    old_count: usize,
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
        if line.starts_with("@@ ") {
            // Save previous hunk
            if let Some(hunk) = current_hunk.take() {
                hunks.push(hunk);
            }
            in_header = false;

            let (old_start, old_count) = parse_hunk_header(line)?;
            current_hunk = Some(Hunk {
                old_start,
                old_count,
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

/// Parse @@ -start,count +start,count @@ header
fn parse_hunk_header(line: &str) -> Result<(usize, usize), String> {
    // Find the old range: -start,count or -start
    let at_idx = line.find("@@").unwrap_or(0);
    let rest = &line[at_idx + 2..];

    let minus_idx = rest
        .find('-')
        .ok_or_else(|| format!("Invalid hunk header (no -): {}", line))?;
    let plus_idx = rest
        .find('+')
        .ok_or_else(|| format!("Invalid hunk header (no +): {}", line))?;

    let old_range = rest[minus_idx + 1..plus_idx].trim().trim_end_matches(',');

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

    Ok((old_start, old_count))
}

/// Apply a single hunk to the file lines.
fn apply_hunk(lines: &[String], hunk: &Hunk) -> Result<Vec<String>, String> {
    let start_idx = if hunk.old_start == 0 {
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
    let end_idx = start_idx + hunk.old_count;
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
        assert!(result.unwrap_err().contains("No hunks"));
    }

    #[test]
    fn test_validate_single_file_diff_ok() {
        let diff = "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        assert!(validate_single_file_diff(diff, "src/lib.rs").is_ok());
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
