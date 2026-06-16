use super::*;

pub(super) async fn load_execution_issue_id_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<Option<String>> {
    crate::storage::query_opt_text_conn(
        conn,
        "SELECT issue_id FROM executions WHERE id = ?1",
        (execution_id,),
    )
    .await
}
