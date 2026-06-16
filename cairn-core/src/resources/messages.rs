//! Project and issue message resource readers.

use super::common::{
    connect_for_read, find_query_value, lookup_project_by_key, parse_optional_i64_param,
};

use crate::storage::LocalDb;
use cairn_common::query::QueryParam;

type MessageQueryParams = (Option<String>, Option<String>, Option<i64>, Option<i64>);

// ============================================================================
// Message Resource Readers
// ============================================================================

/// Parse query params from a message resource target.
/// Returns (before, after, since, limit).
fn parse_message_query_params(params: &[QueryParam]) -> Result<MessageQueryParams, String> {
    if let Some(unsupported) = params
        .iter()
        .find(|param| !["before", "after", "since", "limit"].contains(&param.key.as_str()))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for message resource",
            unsupported.key
        ));
    }

    Ok((
        find_query_value(params, "before").map(|value| value.to_string()),
        find_query_value(params, "after").map(|value| value.to_string()),
        parse_optional_i64_param(params, "since")?,
        parse_optional_i64_param(params, "limit")?,
    ))
}

/// Node/task message resources advertise only `since` + `limit` (the direct
/// stream is small and chronological; cursor windows belong to channels).
fn parse_node_message_query_params(
    params: &[QueryParam],
) -> Result<(Option<i64>, Option<i64>), String> {
    if let Some(unsupported) = params
        .iter()
        .find(|param| !["since", "limit"].contains(&param.key.as_str()))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for node message resource",
            unsupported.key
        ));
    }

    Ok((
        parse_optional_i64_param(params, "since")?,
        parse_optional_i64_param(params, "limit")?,
    ))
}

fn format_messages(messages: &[crate::models::Message]) -> String {
    if messages.is_empty() {
        return "(no messages)".to_string();
    }

    let mut out = String::new();
    for msg in messages {
        let ts = chrono::DateTime::from_timestamp(msg.created_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| msg.created_at.to_string());
        out.push_str(&format!("[{}] {}: {}\n", ts, msg.sender_name, msg.content));
    }
    out
}

pub(super) async fn read_project_messages(
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    // Validate project exists
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project_ctx = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(_) => return format!("Project '{}' not found", project_key),
    };

    let (before, after, since, limit) = match parse_message_query_params(params) {
        Ok(parsed) => parsed,
        Err(error) => return error,
    };

    match crate::messages::db::query_channel(
        db,
        &crate::models::ChannelType::Project,
        Some(&project_ctx.project_key),
        before.as_deref(),
        after.as_deref(),
        since,
        limit,
    ) {
        Ok(messages) => {
            let mut out = format!("# Messages — {}\n\n", project_ctx.project_key);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}

/// Read the direct-message stream for a node or sub-agent task. Symmetric with
/// the `/messages` append target: `task_name = None` reads a node's direct messages,
/// `Some` reads a sub-task's. Returns every direct message where the resolved
/// job is sender or recipient, oldest first.
#[allow(clippy::too_many_arguments)]
pub(super) async fn read_node_messages(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: Option<&str>,
    params: &[QueryParam],
) -> String {
    let (since, limit) = match parse_node_message_query_params(params) {
        Ok(parsed) => parsed,
        Err(error) => return error,
    };

    let job_id = match super::node::resolve_todos_job_id(
        db,
        project_key,
        number,
        exec_seq,
        node_name,
        task_name,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };

    match crate::messages::db::query_directs_for_job(db, &job_id, since, limit) {
        Ok(messages) => {
            let label = match task_name {
                Some(task) => format!(
                    "{}-{}/{}/{}/task/{}",
                    project_key, number, exec_seq, node_name, task
                ),
                None => format!("{}-{}/{}/{}", project_key, number, exec_seq, node_name),
            };
            let mut out = format!("# Messages \u{2014} {}\n\n", label);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}

pub(super) async fn read_issue_messages(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    params: &[QueryParam],
) -> String {
    // Use KEY/NUMBER as channel_id (matches how messages are stored)
    let issue_key = format!("{}/{}", project_key, number);

    let (before, after, since, limit) = match parse_message_query_params(params) {
        Ok(parsed) => parsed,
        Err(error) => return error,
    };

    match crate::messages::db::query_channel(
        db,
        &crate::models::ChannelType::Issue,
        Some(&issue_key),
        before.as_deref(),
        after.as_deref(),
        since,
        limit,
    ) {
        Ok(messages) => {
            let mut out = format!("# Messages — {}-{}\n\n", project_key, number);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}
