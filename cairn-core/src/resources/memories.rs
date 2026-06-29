//! Node-scoped memory resource reads.

use crate::models::Memory;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::build_node_memory_uri;

fn memory_label<'a>(memory: &'a Memory, uri: &'a str) -> &'a str {
    memory
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(uri)
}

pub(crate) async fn read_node_memories_collection(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> String {
    let db = orch.db.for_project(project).await;
    let job_id = match crate::resources::node::resolve_todos_job_id(
        &db, project, number, exec_seq, node_id, None,
    )
    .await
    {
        Ok(job_id) => job_id,
        Err(error) => return error,
    };
    let memories = match crate::memories::db::load_memories_for_job(&db, &job_id).await {
        Ok(memories) => memories,
        Err(error) => return format!("Error listing node memories: {error}"),
    };

    let mut out = format!(
        "# Memories — {node_id}\n\n{} memory(ies)\n\n",
        memories.len()
    );
    if memories.is_empty() {
        out.push_str("No memories captured for this node.\n\n");
    } else {
        for memory in &memories {
            let seq = memory.node_seq.unwrap_or_default() as i32;
            let uri = build_node_memory_uri(project, number, exec_seq, node_id, seq);
            out.push_str(&format!(
                "- [{}]({}) [{}; {}]\n",
                memory_label(memory, &uri),
                uri,
                memory.scope,
                memory.status
            ));
        }
        out.push('\n');
    }

    out
}

pub(crate) async fn read_node_memory(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    memory_seq: i32,
) -> String {
    let db = orch.db.for_project(project).await;
    let memory_id = match crate::memories::db::resolve_node_memory_id(
        &db, project, number, exec_seq, node_id, memory_seq,
    )
    .await
    {
        Ok(Some(id)) => id,
        Ok(None) => return format!("Memory not found at {node_id}/memories/{memory_seq}"),
        Err(error) => return format!("Error resolving node memory: {error}"),
    };
    let memory = match crate::memories::db::load_memory(&db, &memory_id).await {
        Ok(memory) => memory,
        Err(_) => return format!("Memory not found at {node_id}/memories/{memory_seq}"),
    };
    let uri = build_node_memory_uri(project, number, exec_seq, node_id, memory_seq);
    let title = memory.name.as_deref().unwrap_or(&uri);
    let mut out = format!(
        "# Memory `{title}`\n\n`{uri}`\n\n[{}: {}]\n\n",
        memory.scope, memory.scope_value
    );
    out.push_str(&memory.content);
    out.push_str("\n\n## provenance\n");
    out.push_str(&format!("- status: {}\n", memory.status));
    if let Some(provenance) = &memory.provenance_uri {
        out.push_str(&format!("- provenance turn: {provenance}\n"));
    }
    out.push('\n');
    out
}
