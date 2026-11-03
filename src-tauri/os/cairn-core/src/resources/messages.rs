//! Project and issue message resource readers.

use super::common::{
    find_query_value, lookup_project_by_key, parse_optional_i64_param, read_resource,
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
        let ts = crate::clock::stamp_with_seconds(msg.created_at)
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
    // Validate project exists. The lookup's read releases before the channel
    // query, which runs on a blocking worker of its own.
    let key = project_key.to_owned();
    let project_ctx = match read_resource(db, move |conn| {
        Box::pin(async move {
            lookup_project_by_key(conn, &key)
                .await
                .map_err(|_| format!("Project '{}' not found", key))
        })
    })
    .await
    {
        Ok(ctx) => ctx,
        Err(error) => return error,
    };

    let (before, after, since, limit) = match parse_message_query_params(params) {
        Ok(parsed) => parsed,
        Err(error) => return error,
    };

    match crate::messages::db::query_channel(
        db,
        &crate::models::ChannelType::Project,
        &project_ctx.project_key,
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

/// A direct message's line carries its delivery urgency: whether it woke its
/// recipient or rode along is part of what was said. Rows written before
/// system directs recorded urgency carry none and show none.
fn format_direct_messages(messages: &[crate::models::Message]) -> String {
    if messages.is_empty() {
        return "(no messages)".to_string();
    }

    let mut out = String::new();
    for msg in messages {
        let ts = crate::clock::stamp_with_seconds(msg.created_at)
            .unwrap_or_else(|| msg.created_at.to_string());
        match msg.urgency {
            Some(urgency) => out.push_str(&format!(
                "[{}] {} ({}): {}\n",
                ts,
                msg.sender_name,
                urgency.as_str(),
                msg.content
            )),
            None => out.push_str(&format!("[{}] {}: {}\n", ts, msg.sender_name, msg.content)),
        }
    }
    out
}

/// Read the direct-message stream for a node, thread, or sub-agent task.
/// Symmetric with the `/messages` append target: `task_name = None` reads the
/// home's own direct messages, `Some` reads a sub-task's. Returns the direct
/// messages where the resolved job is sender or recipient across all of its
/// runs, oldest first, windowed as [`crate::messages::db::query_directs_for_job`]
/// describes.
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

    let job_id = match super::node::resolve_node_or_task_job_id_for_read(
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
            let home = cairn_common::uri::build_node_uri(project_key, number, exec_seq, node_name);
            let label = match task_name {
                Some(task) => format!("{home}/task/{task}"),
                None => home,
            };
            let mut out = format!("# Messages \u{2014} {}\n\n", label);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_direct_messages(&messages));
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
        &issue_key,
        before.as_deref(),
        after.as_deref(),
        since,
        limit,
    ) {
        Ok(messages) => {
            let mut out = format!("# Messages — {}/{}\n\n", project_key, number);
            out.push_str(&format!("{} message(s)\n\n", messages.len()));
            out.push_str(&format_messages(&messages));
            out
        }
        Err(e) => format!("Failed to query messages: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::read_node_messages;

    /// A thread's `/messages` is its session's direct stream across every run
    /// the session has had, newest window first, each line with its urgency,
    /// headed by the thread's own address.
    #[tokio::test(flavor = "current_thread")]
    async fn a_threads_messages_show_its_newest_directs_across_runs_with_urgency() {
        let db = crate::storage::migrated_test_db("thread-node-messages.db").await;
        let mut script = String::from(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('t','p','ux',1,1);
             INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES('j','p','t','running','thread',1,1);
             INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at) VALUES('r1','j','p','crashed',1,1);
             INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at) VALUES('r2','j','p','live',2,2);",
        );
        // Sixty older directs to the first run: more than one default page.
        for i in 0..60 {
            script.push_str(&format!(
                "INSERT INTO messages(id,channel_type,sender_name,recipient_run_id,content,created_at,urgency) \
                 VALUES('old-{i}','direct','cairn://p/p/peer','r1','history {i:02}',{},'steer');",
                100 + i
            ));
        }
        db.execute_script(&script).await.unwrap();
        db.write(|conn| {
            Box::pin(async move {
                cairn_attention::direct::insert_system_direct_push_conn(
                    conn,
                    "j",
                    "r1",
                    "a passive verdict",
                    crate::messages::queued::DeliveryUrgency::Passive,
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let read = read_node_messages(&db, "p", 0, 0, "ux", None, &[]).await;

        assert!(
            read.starts_with("# Messages \u{2014} cairn://p/p/ux\n"),
            "{read}"
        );
        assert!(read.contains("50 message(s)"), "{read}");
        assert!(
            read.contains("system (passive): a passive verdict"),
            "{read}"
        );
        assert!(read.contains("(steer): history 59"), "{read}");
        assert!(
            !read.contains("history 00"),
            "the oldest fall outside the window: {read}"
        );
        let newest = read.find("a passive verdict").unwrap();
        assert!(
            read.find("history 59").unwrap() < newest,
            "oldest first within the window"
        );

        let forward = read_node_messages(
            &db,
            "p",
            0,
            0,
            "ux",
            None,
            &[cairn_common::query::QueryParam {
                key: "since".into(),
                value: "100".into(),
            }],
        )
        .await;
        assert!(
            forward.contains("history 00"),
            "`since` reads forward: {forward}"
        );
    }
}
