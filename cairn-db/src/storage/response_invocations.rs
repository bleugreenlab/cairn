use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

pub const RESPONSE_INVOCATION_RETENTION: i64 = 200;

#[derive(Debug, Clone, PartialEq)]
pub struct NewResponseInvocation {
    pub response_id: String,
    pub scope_key: String,
    pub project_id: Option<String>,
    pub caller_kind: String,
    pub caller_label: Option<String>,
    pub caller_run_id: Option<String>,
    pub rendered_prompt: String,
    pub args_json: Option<String>,
    pub status: String,
    pub output_text: Option<String>,
    pub error: Option<String>,
    pub model: Option<String>,
    pub backend: Option<String>,
    pub latency_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cost: Option<f64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseInvocationRecord {
    pub id: String,
    pub response_id: String,
    pub scope_key: String,
    pub seq: i64,
    pub project_id: Option<String>,
    pub caller_kind: String,
    pub caller_label: Option<String>,
    pub caller_run_id: Option<String>,
    pub rendered_prompt: String,
    pub args_json: Option<String>,
    pub status: String,
    pub output_text: Option<String>,
    pub error: Option<String>,
    pub model: Option<String>,
    pub backend: Option<String>,
    pub latency_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cost: Option<f64>,
    pub created_at: i64,
}

const COLUMNS: &str = "id,response_id,scope_key,seq,project_id,caller_kind,caller_label,caller_run_id,rendered_prompt,args_json,status,output_text,error,model,backend,latency_ms,input_tokens,output_tokens,cost,created_at";

fn from_row(row: &crate::turso::Row) -> DbResult<ResponseInvocationRecord> {
    Ok(ResponseInvocationRecord {
        id: row.text(0)?,
        response_id: row.text(1)?,
        scope_key: row.text(2)?,
        seq: row.i64(3)?,
        project_id: row.opt_text(4)?,
        caller_kind: row.text(5)?,
        caller_label: row.opt_text(6)?,
        caller_run_id: row.opt_text(7)?,
        rendered_prompt: row.text(8)?,
        args_json: row.opt_text(9)?,
        status: row.text(10)?,
        output_text: row.opt_text(11)?,
        error: row.opt_text(12)?,
        model: row.opt_text(13)?,
        backend: row.opt_text(14)?,
        latency_ms: row.opt_i64(15)?,
        input_tokens: row.opt_i64(16)?,
        output_tokens: row.opt_i64(17)?,
        cost: row.opt_f64(18)?,
        created_at: row.i64(19)?,
    })
}

/// Insert one invocation, allocate its per-response sequence atomically, and prune
/// records older than the newest 200 for that response.
pub async fn insert_response_invocation(
    db: &LocalDb,
    new: NewResponseInvocation,
) -> DbResult<ResponseInvocationRecord> {
    let id = uuid::Uuid::new_v4().to_string();
    let response_id = new.response_id.clone();
    let scope_key = new.scope_key.clone();
    let result_id = id.clone();
    let response_id_for_write = response_id.clone();
    db.write(move |conn| {
        let new = new.clone();
        let id = id.clone();
        let response_id = response_id_for_write.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM response_invocations WHERE scope_key = ?1 AND response_id = ?2",
                params![new.scope_key.clone(), new.response_id.clone()],
            ).await?;
            let seq = rows.next().await?.expect("aggregate always returns a row").i64(0)?;
            conn.execute(
                "INSERT INTO response_invocations (id,response_id,scope_key,seq,project_id,caller_kind,caller_label,caller_run_id,rendered_prompt,args_json,status,output_text,error,model,backend,latency_ms,input_tokens,output_tokens,cost,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
                params![id, new.response_id, new.scope_key.clone(), seq, new.project_id, new.caller_kind, new.caller_label, new.caller_run_id, new.rendered_prompt, new.args_json, new.status, new.output_text, new.error, new.model, new.backend, new.latency_ms, new.input_tokens, new.output_tokens, new.cost, new.created_at],
            ).await?;
            conn.execute(
                "DELETE FROM response_invocations WHERE scope_key = ?1 AND response_id = ?2 AND seq <= ?3",
                params![new.scope_key, response_id.clone(), seq - RESPONSE_INVOCATION_RETENTION],
            ).await?;
            Ok(())
        })
    }).await?;
    get_response_invocation(db, &scope_key, &response_id, {
        db.query_opt_i64(
            "SELECT seq FROM response_invocations WHERE id = ?1",
            (result_id,),
        )
        .await?
        .expect("inserted response invocation exists")
    })
    .await?
    .ok_or_else(|| super::DbError::internal("inserted response invocation disappeared"))
}

pub async fn list_response_invocations(
    db: &LocalDb,
    scope_key: &str,
    response_id: &str,
    limit: i64,
) -> DbResult<Vec<ResponseInvocationRecord>> {
    let sql = format!("SELECT {COLUMNS} FROM response_invocations WHERE scope_key = ?1 AND response_id = ?2 ORDER BY seq DESC LIMIT ?3");
    db.query_all(
        sql,
        (
            scope_key.to_string(),
            response_id.to_string(),
            limit.clamp(1, RESPONSE_INVOCATION_RETENTION),
        ),
        from_row,
    )
    .await
}

pub async fn get_response_invocation(
    db: &LocalDb,
    scope_key: &str,
    response_id: &str,
    seq: i64,
) -> DbResult<Option<ResponseInvocationRecord>> {
    let sql =
        format!("SELECT {COLUMNS} FROM response_invocations WHERE scope_key = ?1 AND response_id = ?2 AND seq = ?3");
    db.query_opt(
        sql,
        (scope_key.to_string(), response_id.to_string(), seq),
        from_row,
    )
    .await
}

pub async fn count_response_invocations(
    db: &LocalDb,
    scope_key: &str,
    response_id: &str,
) -> DbResult<i64> {
    Ok(db
        .query_opt_i64(
            "SELECT COUNT(*) FROM response_invocations WHERE scope_key = ?1 AND response_id = ?2",
            (scope_key.to_string(), response_id.to_string()),
        )
        .await?
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn invocation(response_id: &str, n: i64) -> NewResponseInvocation {
        NewResponseInvocation {
            response_id: response_id.into(),
            scope_key: "workspace".into(),
            project_id: None,
            caller_kind: "internal".into(),
            caller_label: Some("test".into()),
            caller_run_id: None,
            rendered_prompt: format!("prompt {n}"),
            args_json: None,
            status: "ok".into(),
            output_text: Some(format!("output {n}")),
            error: None,
            model: Some("test".into()),
            backend: Some("test".into()),
            latency_ms: Some(1),
            input_tokens: None,
            output_tokens: None,
            cost: None,
            created_at: n,
        }
    }

    #[tokio::test]
    async fn insert_allocates_sequence_and_prunes_per_response() {
        let db = crate::storage::migrated_test_db("response-retention.db").await;
        for n in 1..=250 {
            insert_response_invocation(&db, invocation("conveyor", n))
                .await
                .unwrap();
        }
        assert_eq!(
            count_response_invocations(&db, "workspace", "conveyor")
                .await
                .unwrap(),
            200
        );
        let rows = list_response_invocations(&db, "workspace", "conveyor", 200)
            .await
            .unwrap();
        assert_eq!(
            (rows.first().unwrap().seq, rows.last().unwrap().seq),
            (250, 51)
        );
        assert_eq!(rows.first().unwrap().rendered_prompt, "prompt 250");
    }
}
