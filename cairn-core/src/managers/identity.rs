//! Manager actor identity lookup helpers.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::managers::crud;
use crate::mcp::handlers;
use crate::models::Manager;
use crate::schema::{jobs, turns};

/// Resolve a manager actor directly from a job row.
pub fn lookup_manager_actor_by_job(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<Manager>, String> {
    let direct_manager_id: Option<Option<String>> = jobs::table
        .find(job_id)
        .select(jobs::manager_id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if let Some(Some(manager_id)) = direct_manager_id {
        return crud::get(conn, &manager_id);
    }

    crud::get_by_job_id(conn, job_id)
}

/// Resolve a manager actor from a run context.
pub fn lookup_manager_actor_by_run_context(
    conn: &mut SqliteConnection,
    run_ctx: &handlers::RunContext,
) -> Result<Option<Manager>, String> {
    lookup_manager_actor_by_job(conn, &run_ctx.job_id)
}

/// Resolve a manager actor from an MCP callback request.
pub fn lookup_manager_actor_by_request(
    conn: &mut SqliteConnection,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<Option<Manager>, String> {
    let run_ctx = handlers::lookup_run(conn, request)?;
    lookup_manager_actor_by_run_context(conn, &run_ctx)
}

/// Resolve a manager actor from a turn.
pub fn lookup_manager_actor_by_turn(
    conn: &mut SqliteConnection,
    turn_id: &str,
) -> Result<Option<Manager>, String> {
    let manager_id: Option<Option<String>> = turns::table
        .find(turn_id)
        .select(turns::manager_id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if let Some(Some(manager_id)) = manager_id {
        return crud::get(conn, &manager_id);
    }

    let job_id: Option<Option<String>> = turns::table
        .find(turn_id)
        .select(turns::job_id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if let Some(Some(job_id)) = job_id {
        let direct_manager_id: Option<Option<String>> = jobs::table
            .find(&job_id)
            .select(jobs::manager_id)
            .first(conn)
            .optional()
            .map_err(|e| e.to_string())?;
        if let Some(Some(manager_id)) = direct_manager_id {
            return crud::get(conn, &manager_id);
        }
        return crud::get_by_job_id(conn, &job_id);
    }

    Ok(None)
}
