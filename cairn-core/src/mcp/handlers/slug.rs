//! Slug utilities for terminal resource URIs
//!
//! Provides functions for generating human-readable, URL-safe slugs for terminals.

use cairn_common::uri::{build_node_terminal_uri, build_project_terminal_uri};

/// Convert a string to a URL-safe slug
pub fn slugify(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
        .chars()
        .take(30) // Limit slug length
        .collect()
}

/// Extract a short identifier from a command
pub fn slugify_command(command: &str) -> String {
    // Take first 2-3 words of the command
    let words: Vec<&str> = command.split_whitespace().take(3).collect();

    if words.is_empty() {
        return "term".to_string();
    }

    // Remove common prefixes
    let result = words
        .iter()
        .map(|w| {
            w.trim_start_matches("./")
                .trim_start_matches("npx")
                .trim_end_matches(".js")
                .trim_end_matches(".sh")
        })
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    if result.is_empty() {
        "term".to_string()
    } else {
        slugify(&result)
    }
}

/// Build a terminal resource URI using cairn:// scheme
///
/// Format:
/// - Node terminals: cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG
/// - Project terminals: cairn://PROJECT/terminal/SLUG
pub fn build_terminal_uri(
    project_key: &str,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    node_id: Option<&str>,
    slug: &str,
) -> String {
    match (issue_number, exec_seq, node_id) {
        (Some(num), Some(seq), Some(node)) => {
            build_node_terminal_uri(project_key, num, seq, node, slug)
        }
        _ => build_project_terminal_uri(project_key, slug),
    }
}
