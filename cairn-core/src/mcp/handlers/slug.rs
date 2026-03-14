//! Slug utilities for terminal resource URIs
//!
//! Provides functions for generating human-readable, URL-safe slugs for terminals.

use crate::schema::job_terminals;
use diesel::prelude::*;

/// Generate a terminal slug from available context.
/// Priority: terminal name > description > command
/// Ensures uniqueness by appending -2, -3, etc. if needed.
pub fn generate_terminal_slug(
    conn: &mut diesel::SqliteConnection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    terminal_name: Option<&str>,
    description: Option<&str>,
    command: &str,
) -> String {
    // Generate base slug
    let base_slug = if let Some(name) = terminal_name {
        slugify(name)
    } else if let Some(desc) = description {
        slugify(desc)
    } else {
        // Extract short command identifier
        slugify_command(command)
    };

    // Ensure uniqueness within scope
    ensure_unique_slug(conn, job_id, project_id, &base_slug)
}

/// Generate a slug from a title (for user-created terminals)
pub fn generate_slug_from_title(
    conn: &mut diesel::SqliteConnection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    title: &str,
) -> String {
    let base_slug = slugify(title);
    ensure_unique_slug(conn, job_id, project_id, &base_slug)
}

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

/// Ensure a slug is unique within the job/project scope
pub fn ensure_unique_slug(
    conn: &mut diesel::SqliteConnection,
    job_id: Option<&str>,
    project_id: Option<&str>,
    base_slug: &str,
) -> String {
    let mut candidate = base_slug.to_string();
    let mut counter = 1;

    loop {
        // Check if slug exists in the same scope
        let exists = if let Some(jid) = job_id {
            job_terminals::table
                .filter(job_terminals::job_id.eq(jid))
                .filter(job_terminals::slug.eq(&candidate))
                .count()
                .get_result::<i64>(conn)
                .unwrap_or(0)
                > 0
        } else if let Some(pid) = project_id {
            job_terminals::table
                .filter(job_terminals::project_id.eq(pid))
                .filter(job_terminals::job_id.is_null())
                .filter(job_terminals::slug.eq(&candidate))
                .count()
                .get_result::<i64>(conn)
                .unwrap_or(0)
                > 0
        } else {
            false // No scope, slug doesn't need to be unique
        };

        if !exists {
            return candidate;
        }

        counter += 1;
        candidate = format!("{}-{}", base_slug, counter);
    }
}

/// Build a terminal resource URI using cairn:// scheme
///
/// Format:
/// - Node terminals: cairn://PROJECT/NUMBER/NODE/terminal/SLUG
/// - Project terminals: cairn://PROJECT/terminal/SLUG
pub fn build_terminal_uri(
    project_key: &str,
    issue_number: Option<i32>,
    node_id: Option<&str>,
    slug: &str,
) -> String {
    match (issue_number, node_id) {
        (Some(num), Some(node)) => {
            format!("cairn://{}/{}/{}/terminal/{}", project_key, num, node, slug)
        }
        _ => {
            // Project-level terminal (no issue/node context)
            format!("cairn://{}/terminal/{}", project_key, slug)
        }
    }
}
