//! `cairn://p/{project}/headlines`: the merged live headlines of a project's
//! active threads, each marked new or seen against the operator's cursor.
//! The projection is [`crate::threads::headlines::merged`], which the desktop's
//! invoke command also returns, so the app and an agent's read agree.

use cairn_common::query::QueryParam;

use crate::storage::LocalDb;
use crate::threads::headlines::{self, HeadlineFilter, MergedHeadline, MergedHeadlines};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 500;

struct Query {
    filter: HeadlineFilter,
    json: bool,
}

fn parse_params(project: &str, params: &[QueryParam]) -> Result<Query, String> {
    let mut filter = HeadlineFilter {
        limit: Some(DEFAULT_LIMIT),
        ..HeadlineFilter::default()
    };
    let mut json = false;
    for param in params {
        match param.key.as_str() {
            "thread" => filter.thread = Some(thread_name(project, &param.value)?),
            "seen" if param.value == "false" => filter.unseen_only = true,
            "seen" => {
                return Err(format!(
                    "headlines seen= takes only false (the lines new to the operator), not {:?}",
                    param.value
                ))
            }
            "limit" => {
                let limit: usize = param
                    .value
                    .parse()
                    .map_err(|_| "headlines limit must be an integer".to_string())?;
                if !(1..=MAX_LIMIT).contains(&limit) {
                    return Err(format!("headlines limit must be between 1 and {MAX_LIMIT}"));
                }
                filter.limit = Some(limit);
            }
            "format" if param.value == "json" => json = true,
            "format" => return Err("headlines format must be json".to_string()),
            key => return Err(format!("Unsupported headlines query parameter: {key}")),
        }
    }
    Ok(Query { filter, json })
}

/// A thread named bare or by its canonical URI in `project`.
pub(crate) fn thread_name(project: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if !value.starts_with("cairn:") {
        return Ok(value.to_string());
    }
    match cairn_uri::parse_uri(value) {
        Some(cairn_uri::CairnResource::Thread {
            project: owner,
            name,
            path,
        }) if path.is_empty() && owner == cairn_uri::canonical_project(project) => Ok(name),
        _ => Err(format!(
            "thread must be a thread name or a thread URI of this project, like {}",
            cairn_uri::build_thread_uri(project, "NAME")
        )),
    }
}

pub(crate) async fn read_project_headlines(
    db: &LocalDb,
    project: &str,
    params: &[QueryParam],
) -> String {
    match render_project_headlines(db, project, params).await {
        Ok(text) | Err(text) => text,
    }
}

async fn render_project_headlines(
    db: &LocalDb,
    project: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let query = parse_params(project, params)?;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(db, project).await?;
    let project = cairn_uri::canonical_project(project);
    let merged = headlines::merged(db, &project_id, &query.filter).await?;
    if query.json {
        return serde_json::to_string_pretty(&merged)
            .map_err(|error| format!("Failed to render headlines: {error}"));
    }
    Ok(render(&project, &merged))
}

fn render(project: &str, merged: &MergedHeadlines) -> String {
    let mut out = format!("# Headlines — {project}\n\n");
    out.push_str(&format!("{} new · {} live\n", merged.unseen, merged.total));
    let new: Vec<&MergedHeadline> = merged.lines.iter().filter(|line| !line.seen).collect();
    let seen: Vec<&MergedHeadline> = merged.lines.iter().filter(|line| line.seen).collect();
    if merged.lines.is_empty() {
        out.push_str("\nNo live headlines.\n");
    }
    for (heading, lines) in [("New", new), ("Seen", seen)] {
        if lines.is_empty() {
            continue;
        }
        out.push_str(&format!("\n## {heading}\n\n"));
        for line in lines {
            out.push_str(&line_row(project, line));
        }
    }
    if merged.lines.len() < merged.total {
        out.push_str(&format!(
            "\n_{} more past the limit._\n",
            merged.total - merged.lines.len()
        ));
    }
    for error in &merged.errors {
        out.push_str(&format!("\n_Could not read {error}_\n"));
    }
    out
}

/// `- [alpha #3](row) — line (ref) · ts`, on one line.
fn line_row(project: &str, line: &MergedHeadline) -> String {
    let root = format!("{}/db", cairn_uri::build_thread_uri(project, &line.thread));
    let row = cairn_uri::build_db_row_uri(&root, "headlines", &line.id.to_string());
    let text = line.line.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(
        "- [{} #{}]({row}) — {text} ({}) · {}\n",
        line.thread, line.id, line.link, line.ts
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(thread: &str, id: i64, seen: bool) -> MergedHeadline {
        MergedHeadline {
            thread: thread.into(),
            thread_id: format!("t-{thread}"),
            id,
            line: format!("Decided {thread} {id}."),
            link: "cairn://p/hl/1".into(),
            ts: "2026-11-10T06:00:00Z".into(),
            until: "2099-01-01T00:00:00Z".into(),
            seen,
        }
    }

    #[test]
    fn new_and_seen_render_under_their_own_headings_and_an_empty_group_is_omitted() {
        let merged = MergedHeadlines {
            lines: vec![line("alpha", 3, false), line("bravo", 1, true)],
            total: 3,
            unseen: 1,
            errors: vec!["charlie: failed to read headlines: boom".into()],
        };
        let rendered = render("hl", &merged);
        let new = rendered.find("## New").unwrap();
        let seen = rendered.find("## Seen").unwrap();
        assert!(new < seen, "{rendered}");
        assert!(
            rendered.contains(
                "- [alpha #3](cairn://p/hl/alpha/db/headlines/3) — Decided alpha 3. (cairn://p/hl/1) · 2026-11-10T06:00:00Z\n"
            ),
            "{rendered}"
        );
        assert!(rendered.contains("1 new · 3 live"), "{rendered}");
        assert!(rendered.contains("_1 more past the limit._"), "{rendered}");
        assert!(rendered.contains("_Could not read charlie:"), "{rendered}");

        let only_seen = MergedHeadlines {
            lines: vec![line("bravo", 1, true)],
            total: 1,
            unseen: 0,
            errors: Vec::new(),
        };
        assert!(!render("hl", &only_seen).contains("## New"));
    }

    #[test]
    fn the_filters_parse_and_a_bad_one_is_named() {
        let params = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(key, value)| QueryParam {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect::<Vec<_>>()
        };
        let query = parse_params(
            "hl",
            &params(&[
                ("thread", "cairn://p/hl/alpha"),
                ("seen", "false"),
                ("limit", "5"),
            ]),
        )
        .unwrap();
        assert_eq!(query.filter.thread.as_deref(), Some("alpha"));
        assert!(query.filter.unseen_only);
        assert_eq!(query.filter.limit, Some(5));
        assert!(parse_params("hl", &params(&[("seen", "true")])).is_err());
        assert!(parse_params("hl", &params(&[("thread", "cairn://p/other/alpha")])).is_err());
        assert!(parse_params("hl", &params(&[("limit", "0")])).is_err());
    }
}
