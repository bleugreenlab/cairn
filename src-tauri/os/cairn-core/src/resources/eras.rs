//! Read-only rendering of persisted thread eras: `cairn:~/eras` and its
//! canonical node-family form.
//!
//! An era is minted and summarized at thread-compaction fold time
//! (`crate::threads::eras`), never here -- this module only renders what is
//! already persisted. Until fold-time minting lands, every thread renders
//! "no eras yet", which is the honest and correct state today.

use cairn_common::query::QueryParam;
use cairn_uri::{build_node_era_uri, build_node_eras_uri};

use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use crate::threads::eras::{self, EraChild, ThreadEra};

use super::common::read_resource;
use super::paging::RecordPage;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

fn parse_query(params: &[QueryParam]) -> Result<(i64, i64), String> {
    let mut limit = None;
    let mut offset = None;
    for param in params {
        match param.key.as_str() {
            "limit" if limit.is_none() => {
                let parsed = param.value.parse::<i64>().map_err(|_| {
                    "Query parameter 'limit' must be a positive integer".to_string()
                })?;
                if parsed <= 0 || parsed > MAX_LIMIT {
                    return Err(format!(
                        "Query parameter 'limit' must be between 1 and {MAX_LIMIT}"
                    ));
                }
                limit = Some(parsed);
            }
            "offset" if offset.is_none() => {
                let parsed = param.value.parse::<i64>().map_err(|_| {
                    "Query parameter 'offset' must be a non-negative integer".to_string()
                })?;
                if parsed < 0 {
                    return Err(
                        "Query parameter 'offset' must be a non-negative integer".to_string()
                    );
                }
                offset = Some(parsed);
            }
            "limit" | "offset" => {
                return Err(format!(
                    "Query parameter '{}' must be specified once",
                    param.key
                ))
            }
            other => {
                return Err(format!(
                    "Unsupported query parameter '{other}' for node eras"
                ))
            }
        }
    }
    Ok((limit.unwrap_or(DEFAULT_LIMIT), offset.unwrap_or(0)))
}

fn turn_range(era: &ThreadEra) -> String {
    if era.start_block == era.end_block {
        format!("turn {}", era.start_block + 1)
    } else {
        format!("turns {}\u{2013}{}", era.start_block + 1, era.end_block + 1)
    }
}

/// Derive the era's `.../chat?offset=&limit=` address from the thread's LIVE
/// coordinates. Never persisted (see the module doc): a thread renames, and a
/// baked URI would go stale.
fn range_uri(chat_uri: &str, era: &ThreadEra) -> String {
    format!(
        "{chat_uri}?offset={}&limit={}",
        era.start_block,
        era.end_block - era.start_block + 1
    )
}

fn one_line(summary: &str) -> String {
    const MAX: usize = 160;
    let flattened = summary.trim().replace('\n', " ");
    if flattened.chars().count() <= MAX {
        return flattened;
    }
    let truncated: String = flattened.chars().take(MAX).collect();
    format!("{truncated}\u{2026}")
}

fn render_era_line(era: &ThreadEra, member_uri: &str, chat_uri: &str) -> String {
    format!(
        "- [era {}]({}) \u{b7} {} \u{b7} {} \u{2014} {}\n",
        era.seq,
        member_uri,
        turn_range(era),
        one_line(&era.summary),
        range_uri(chat_uri, era)
    )
}

fn render_child_line(child: &EraChild) -> String {
    format!(
        "- [{}]({}) [{}]\n",
        child.child_title, child.child_issue_uri, child.final_status
    )
}

async fn resolve_chat_uri(db: &LocalDb, node_id: &str, job_id: &str) -> Result<String, String> {
    let job = job_id.to_owned();
    let home = read_resource(db, move |conn| {
        Box::pin(async move { Ok(crate::jobs::queries::home_uri_for_job_conn(conn, &job).await) })
    })
    .await?;
    match home {
        Ok(Some(base_uri)) => Ok(format!("{base_uri}/chat")),
        Ok(None) => Err(format!(
            "Cannot resolve eras: '{node_id}' has no addressable coordinates."
        )),
        Err(error) => Err(format!("Error resolving thread coordinates: {error}")),
    }
}

/// Render the eras collection from already-resolved coordinates. Split from
/// [`read_node_eras_collection`] so it is directly testable against a bare
/// `LocalDb`, the way `memories::render_job_memories` is.
pub(crate) async fn render_node_eras(
    db: &LocalDb,
    node_id: &str,
    collection_uri: &str,
    chat_uri: &str,
    job_id: &str,
    limit: i64,
    offset: i64,
) -> String {
    match render_node_eras_page(db, node_id, collection_uri, chat_uri, job_id, limit, offset).await
    {
        Ok(page) => {
            let mut out = page.body;
            let next_offset = page.offset + page.shown;
            if page.shown > 0 && page.total.is_some_and(|total| next_offset < total) {
                out.push_str(&format!(
                    "\n_Continue: {collection_uri}?offset={next_offset}&limit={limit}_\n"
                ));
            }
            out
        }
        Err(error) => error,
    }
}

/// One page of [`render_node_eras`], counted in eras: a fixed header, then one
/// line per era.
async fn render_node_eras_page(
    db: &LocalDb,
    node_id: &str,
    collection_uri: &str,
    chat_uri: &str,
    job_id: &str,
    limit: i64,
    offset: i64,
) -> Result<RecordPage, String> {
    let total = eras::count_eras(db, job_id)
        .await
        .map_err(|error| format!("Error listing eras: {error}"))?;
    let list = eras::list_eras(db, job_id, limit, offset)
        .await
        .map_err(|error| format!("Error listing eras: {error}"))?;

    let mut out = format!("# Eras \u{2014} {node_id}\n\n{total} era(s)\n\n");
    let prelude_lines = out.lines().count();
    if list.is_empty() {
        out.push_str(
            "_No eras have been minted for this thread yet. An era is written at \
             thread-compaction fold time, when the seed's oldest span exceeds its token \
             budget \u{2014} nothing is computed or authored by this read._\n\n",
        );
    }
    for era in &list {
        let member_uri = format!("{collection_uri}/{}", era.seq);
        out.push_str(&render_era_line(era, &member_uri, chat_uri));
    }
    Ok(RecordPage {
        body: out,
        noun: "eras",
        offset: offset as usize,
        limit: limit as usize,
        shown: list.len(),
        total: Some(total as usize),
        more: false,
        prelude_lines: Some(prelude_lines),
    })
}

/// Render one era from already-resolved coordinates. See
/// [`render_node_eras`] for why this is split from the URI-resolving caller.
pub(crate) async fn render_node_era(
    db: &LocalDb,
    node_id: &str,
    member_uri: &str,
    chat_uri: &str,
    job_id: &str,
    era_seq: i32,
) -> String {
    let era = match eras::get_era(db, job_id, era_seq).await {
        Ok(Some(era)) => era,
        Ok(None) => return format!("Era not found at {node_id}/eras/{era_seq}"),
        Err(error) => return format!("Error loading era: {error}"),
    };
    let children = match eras::era_children(db, job_id, era_seq).await {
        Ok(children) => children,
        Err(error) => return format!("Error loading era children: {error}"),
    };

    let mut out = format!(
        "# Era {} \u{2014} {node_id}\n\n`{member_uri}`\n\n{}\n\n",
        era.seq,
        turn_range(&era)
    );
    out.push_str(era.summary.trim());
    out.push_str("\n\n");
    if !children.is_empty() {
        out.push_str("## Children resolved in this span\n\n");
        for child in &children {
            out.push_str(&render_child_line(child));
        }
        out.push('\n');
    }
    out.push_str(&format!("- range: {}\n", range_uri(chat_uri, &era)));
    out.push_str(&format!("- status: {}\n", era.summary_status));
    if let Some(model) = &era.summary_model {
        out.push_str(&format!("- summarized by: {model}\n"));
    }
    out
}

pub(crate) async fn read_node_eras_collection(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    params: &[QueryParam],
) -> String {
    let (limit, offset) = match parse_query(params) {
        Ok(pair) => pair,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let (job_id, chat_uri) = match resolve_eras_owner(&db, project, number, exec_seq, node_id).await
    {
        Ok(owner) => owner,
        Err(error) => return error,
    };
    let collection_uri = build_node_eras_uri(project, number, exec_seq, node_id);
    render_node_eras(
        &db,
        node_id,
        &collection_uri,
        &chat_uri,
        &job_id,
        limit,
        offset,
    )
    .await
}

/// [`read_node_eras_collection`] as a page the read view continues in eras.
pub(crate) async fn read_node_eras_page(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    params: &[QueryParam],
) -> Result<RecordPage, String> {
    let (limit, offset) = parse_query(params)?;
    let db = orch.db.for_project(project).await;
    let (job_id, chat_uri) = resolve_eras_owner(&db, project, number, exec_seq, node_id).await?;
    let collection_uri = build_node_eras_uri(project, number, exec_seq, node_id);
    render_node_eras_page(
        &db,
        node_id,
        &collection_uri,
        &chat_uri,
        &job_id,
        limit,
        offset,
    )
    .await
}

/// The job whose eras a node address names, and that job's chat URI.
async fn resolve_eras_owner(
    db: &LocalDb,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> Result<(String, String), String> {
    let job_id = crate::resources::node::resolve_node_or_task_job_id_for_read(
        db, project, number, exec_seq, node_id, None,
    )
    .await?;
    let chat_uri = resolve_chat_uri(db, node_id, &job_id).await?;
    Ok((job_id, chat_uri))
}

pub(crate) async fn read_node_era(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    era_seq: i32,
) -> String {
    let db = orch.db.for_project(project).await;
    let job_id = match crate::resources::node::resolve_node_or_task_job_id_for_read(
        &db, project, number, exec_seq, node_id, None,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };
    let chat_uri = match resolve_chat_uri(&db, node_id, &job_id).await {
        Ok(chat_uri) => chat_uri,
        Err(error) => return error,
    };
    let member_uri = build_node_era_uri(project, number, exec_seq, node_id, era_seq);
    render_node_era(&db, node_id, &member_uri, &chat_uri, &job_id, era_seq).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn query_validates_pagination() {
        assert!(parse_query(&[param("limit", "0")]).is_err());
        assert!(parse_query(&[param("limit", "201")]).is_err());
        assert!(parse_query(&[param("offset", "-1")]).is_err());
        assert!(parse_query(&[param("other", "x")]).is_err());
        let (limit, offset) = parse_query(&[param("limit", "10"), param("offset", "5")]).unwrap();
        assert_eq!((limit, offset), (10, 5));
        let (limit, offset) = parse_query(&[]).unwrap();
        assert_eq!((limit, offset), (DEFAULT_LIMIT, 0));
    }

    #[test]
    fn summary_line_flattens_and_truncates() {
        let short = one_line("one\ntwo");
        assert_eq!(short, "one two");
        let long = one_line(&"x".repeat(200));
        assert_eq!(long.chars().count(), 161);
        assert!(long.ends_with('\u{2026}'));
    }
}
