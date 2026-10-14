//! Slug utilities for terminal resource URIs
//!
//! Provides functions for generating human-readable, URL-safe slugs for terminals.

/// Convert a string to a URL-safe slug
pub(crate) fn slugify(text: &str) -> String {
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
pub(crate) fn slugify_command(command: &str) -> String {
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
