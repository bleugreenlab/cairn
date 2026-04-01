//! Prompt injection for messaging context.
//!
//! Builds a section injected into agent system prompts that includes:
//! - List of peer agents working on the same issue (with cairn:// URIs)
//! - Recent message history from project + issue channels
//! - Brief instructions for the message tool

use crate::node_segments::visible_node_segment_or_name;
use crate::schema::{executions, issues, jobs, projects, runs};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Build the messaging context section for injection into agent system prompts.
///
/// Returns an empty string if there's nothing meaningful to inject
/// (no peers and no messages).
pub fn build_messaging_context(
    conn: &mut SqliteConnection,
    project_key: &str,
    issue_id: &str,
    current_run_id: &str,
) -> String {
    let peers = find_peer_agents(conn, issue_id, current_run_id);

    // Resolve issue UUID to KEY/NUMBER for channel query
    let issue_key: Option<String> = issues::table
        .find(issue_id)
        .select(issues::number)
        .first::<i32>(conn)
        .ok()
        .map(|n| format!("{}/{}", project_key, n));

    let recent = super::db::query_recent_for_run(
        conn,
        project_key,
        issue_key.as_deref(),
        Some(current_run_id),
        20,
    )
    .unwrap_or_default();

    if peers.is_empty() && recent.is_empty() {
        return String::new();
    }

    let mut out = String::from("## Agent Messaging\n\n");

    out.push_str(
        "You can communicate with other agents working on this issue using the `message` tool.\n",
    );
    out.push_str("Messages sent to the issue channel are visible to all agents on the issue.\n");
    out.push_str(
        "Direct messages go to a specific agent via their URI and auto-resume them if idle.\n\n",
    );

    if !peers.is_empty() {
        out.push_str("### Active Peers\n\n");
        for peer in &peers {
            out.push_str(&format!(
                "- **{}** ({}) — `{}`\n",
                peer.node_name, peer.status, peer.uri
            ));
        }
        out.push('\n');
    }

    if !recent.is_empty() {
        out.push_str("### Recent Messages\n\n");
        for msg in &recent {
            let ts = chrono::DateTime::from_timestamp(msg.created_at, 0)
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "??:??:??".to_string());
            out.push_str(&format!("[{}] {}: {}\n", ts, msg.sender_name, msg.content));
        }
    }

    out
}

struct PeerAgent {
    node_name: String,
    status: String,
    uri: String,
}

/// Find peer agents working on the same issue.
/// Returns structured peer info with cairn:// URIs for direct messaging.
fn find_peer_agents(
    conn: &mut SqliteConnection,
    issue_id: &str,
    current_run_id: &str,
) -> Vec<PeerAgent> {
    // Get current run's job_id to exclude self
    let current_job_id: Option<String> = runs::table
        .find(current_run_id)
        .select(runs::job_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    // Find all active/running/complete jobs on this issue
    let mut query = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .filter(jobs::status.ne("pending"))
        .select((
            jobs::id,
            jobs::node_name,
            jobs::recipe_node_id,
            jobs::status,
            jobs::execution_id,
        ))
        .into_boxed();

    if let Some(ref my_job_id) = current_job_id {
        query = query.filter(jobs::id.ne(my_job_id));
    }

    let peers: Vec<(
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    )> = query.load(conn).unwrap_or_default();

    // Look up project_key and issue_number for URI construction
    let project_key: Option<String> = issues::table
        .find(issue_id)
        .inner_join(projects::table)
        .select(projects::key)
        .first(conn)
        .ok();

    let issue_number: Option<i32> = issues::table
        .find(issue_id)
        .select(issues::number)
        .first(conn)
        .ok();

    let (project_key, issue_number) = match (project_key, issue_number) {
        (Some(k), Some(n)) => (k, n),
        _ => return vec![], // Can't build URIs without these
    };

    peers
        .into_iter()
        .map(|(_id, name, recipe_node_id, status, exec_id)| {
            let node_name = name.unwrap_or_else(|| "unknown".to_string());

            // Get execution sequence for URI
            let exec_seq = exec_id
                .and_then(|eid| {
                    executions::table
                        .find(eid.as_str())
                        .select(executions::seq)
                        .first::<Option<i32>>(conn)
                        .ok()
                        .flatten()
                })
                .unwrap_or(1);

            let uri = format!(
                "cairn://{}/{}/{}/{}",
                project_key,
                issue_number,
                exec_seq,
                visible_node_segment_or_name(recipe_node_id.as_deref(), &node_name)
            );

            PeerAgent {
                node_name,
                status,
                uri,
            }
        })
        .collect()
}
