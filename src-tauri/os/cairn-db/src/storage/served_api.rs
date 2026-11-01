//! The served API's client records and its per-request usage journal
//! (`served_api_clients`, `model_requests`).

use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

/// How long a usage row is kept.
pub const MODEL_REQUEST_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedApiClient {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub key_hash: String,
    pub key_hint: Option<String>,
    pub source: String,
    pub created_at: i64,
    pub revoked_at: Option<i64>,
    pub revoked_by: Option<String>,
}

const CLIENT_COLUMNS: &str =
    "id,workspace_id,name,key_hash,key_hint,source,created_at,revoked_at,revoked_by";

fn client_from_row(row: &crate::turso::Row) -> DbResult<ServedApiClient> {
    Ok(ServedApiClient {
        id: row.text(0)?,
        workspace_id: row.text(1)?,
        name: row.text(2)?,
        key_hash: row.text(3)?,
        key_hint: row.opt_text(4)?,
        source: row.text(5)?,
        created_at: row.i64(6)?,
        revoked_at: row.opt_i64(7)?,
        revoked_by: row.opt_text(8)?,
    })
}

/// Insert client records in one transaction: all of them or none.
pub async fn insert_served_api_clients(
    db: &LocalDb,
    clients: Vec<ServedApiClient>,
) -> DbResult<()> {
    db.write(move |conn| {
        let clients = clients.clone();
        Box::pin(async move {
            for client in clients {
                conn.execute(
                    &format!(
                        "INSERT INTO served_api_clients ({CLIENT_COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
                    ),
                    params![
                        client.id,
                        client.workspace_id,
                        client.name,
                        client.key_hash,
                        client.key_hint,
                        client.source,
                        client.created_at,
                        client.revoked_at,
                        client.revoked_by
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

/// Every client of `workspace_id`, revoked ones included, by name then age.
pub async fn list_served_api_clients(
    db: &LocalDb,
    workspace_id: &str,
) -> DbResult<Vec<ServedApiClient>> {
    db.query_all(
        format!(
            "SELECT {CLIENT_COLUMNS} FROM served_api_clients WHERE workspace_id = ?1 ORDER BY name, created_at"
        ),
        (workspace_id.to_string(),),
        client_from_row,
    )
    .await
}

/// Stamp `id` revoked. False when it was not live.
pub async fn revoke_served_api_client(
    db: &LocalDb,
    id: &str,
    revoked_at: i64,
    revoked_by: &str,
) -> DbResult<bool> {
    let changed = db
        .execute(
            "UPDATE served_api_clients SET revoked_at = ?2, revoked_by = ?3 WHERE id = ?1 AND revoked_at IS NULL",
            params![id, revoked_at, revoked_by],
        )
        .await?;
    Ok(changed > 0)
}

/// One served-API request, as journaled.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ModelRequestRow {
    pub id: String,
    pub ts: i64,
    pub door: String,
    pub client_id: Option<String>,
    pub principal_json: Option<String>,
    pub appearance_json: Option<String>,
    pub model_asked: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub account_id: Option<String>,
    pub sent_model: Option<String>,
    pub status: i64,
    pub stream: bool,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub cost: Option<f64>,
    pub latency_ms: i64,
    pub wait_ms: Option<i64>,
    pub upstream_request_id: Option<String>,
    pub grant_id: Option<String>,
    pub outcome: String,
    pub detail: Option<String>,
}

const REQUEST_COLUMNS: &str = "id,ts,door,client_id,principal_json,appearance_json,model_asked,model,provider,account_id,sent_model,status,stream,input_tokens,output_tokens,cached_tokens,cost,latency_ms,wait_ms,upstream_request_id,grant_id,outcome,detail";

fn request_from_row(row: &crate::turso::Row) -> DbResult<ModelRequestRow> {
    Ok(ModelRequestRow {
        id: row.text(0)?,
        ts: row.i64(1)?,
        door: row.text(2)?,
        client_id: row.opt_text(3)?,
        principal_json: row.opt_text(4)?,
        appearance_json: row.opt_text(5)?,
        model_asked: row.opt_text(6)?,
        model: row.opt_text(7)?,
        provider: row.opt_text(8)?,
        account_id: row.opt_text(9)?,
        sent_model: row.opt_text(10)?,
        status: row.i64(11)?,
        stream: row.i64(12)? != 0,
        input_tokens: row.opt_i64(13)?,
        output_tokens: row.opt_i64(14)?,
        cached_tokens: row.opt_i64(15)?,
        cost: row.opt_f64(16)?,
        latency_ms: row.i64(17)?,
        wait_ms: row.opt_i64(18)?,
        upstream_request_id: row.opt_text(19)?,
        grant_id: row.opt_text(20)?,
        outcome: row.text(21)?,
        detail: row.opt_text(22)?,
    })
}

/// Journal one request, and drop rows past [`MODEL_REQUEST_RETENTION_MS`].
pub async fn insert_model_request(db: &LocalDb, row: ModelRequestRow) -> DbResult<()> {
    db.write(move |conn| {
        let row = row.clone();
        Box::pin(async move {
            conn.execute(
                &format!(
                    "INSERT INTO model_requests ({REQUEST_COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)"
                ),
                params![
                    row.id,
                    row.ts,
                    row.door,
                    row.client_id,
                    row.principal_json,
                    row.appearance_json,
                    row.model_asked,
                    row.model,
                    row.provider,
                    row.account_id,
                    row.sent_model,
                    row.status,
                    i64::from(row.stream),
                    row.input_tokens,
                    row.output_tokens,
                    row.cached_tokens,
                    row.cost,
                    row.latency_ms,
                    row.wait_ms,
                    row.upstream_request_id,
                    row.grant_id,
                    row.outcome,
                    row.detail
                ],
            )
            .await?;
            conn.execute(
                "DELETE FROM model_requests WHERE ts < ?1",
                params![row.ts - MODEL_REQUEST_RETENTION_MS],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// The newest rows since `since_ms`, of one client when `client_id` is set.
pub async fn list_model_requests(
    db: &LocalDb,
    since_ms: i64,
    client_id: Option<&str>,
    limit: i64,
) -> DbResult<Vec<ModelRequestRow>> {
    let limit = limit.clamp(1, 10_000);
    match client_id {
        Some(client) => {
            db.query_all(
                format!(
                    "SELECT {REQUEST_COLUMNS} FROM model_requests WHERE client_id = ?1 AND ts >= ?2 ORDER BY ts DESC LIMIT ?3"
                ),
                (client.to_string(), since_ms, limit),
                request_from_row,
            )
            .await
        }
        None => {
            db.query_all(
                format!(
                    "SELECT {REQUEST_COLUMNS} FROM model_requests WHERE ts >= ?1 ORDER BY ts DESC LIMIT ?2"
                ),
                (since_ms, limit),
                request_from_row,
            )
            .await
        }
    }
}

/// When each client last made a request.
pub async fn last_model_request_by_client(db: &LocalDb) -> DbResult<Vec<(String, i64)>> {
    db.query_all(
        "SELECT client_id, MAX(ts) FROM model_requests WHERE client_id IS NOT NULL GROUP BY client_id",
        (),
        |row| Ok((row.text(0)?, row.i64(1)?)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str, name: &str, hash_digit: char) -> ServedApiClient {
        ServedApiClient {
            id: id.into(),
            workspace_id: "default".into(),
            name: name.into(),
            key_hash: hash_digit.to_string().repeat(64),
            key_hint: None,
            source: "imported:bgllm".into(),
            created_at: 1,
            revoked_at: None,
            revoked_by: None,
        }
    }

    #[tokio::test]
    async fn clients_insert_whole_list_and_revoke() {
        let db = super::super::migrated_test_db("served-api-clients.db").await;
        insert_served_api_clients(
            &db,
            vec![client("cli_a", "dig", 'a'), client("cli_b", "sitrep", 'b')],
        )
        .await
        .unwrap();
        // A duplicate hash refuses the whole batch.
        assert!(insert_served_api_clients(
            &db,
            vec![client("cli_c", "tm-llm", 'c'), client("cli_d", "ha", 'a')]
        )
        .await
        .is_err());
        // A second live key under one name is refused.
        assert!(
            insert_served_api_clients(&db, vec![client("cli_e", "dig", 'e')])
                .await
                .is_err()
        );
        let listed = list_served_api_clients(&db, "default").await.unwrap();
        assert_eq!(
            listed.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["cli_a", "cli_b"]
        );
        assert!(revoke_served_api_client(&db, "cli_a", 5, "operator")
            .await
            .unwrap());
        assert!(!revoke_served_api_client(&db, "cli_a", 6, "operator")
            .await
            .unwrap());
        // A revoked name may be issued again.
        insert_served_api_clients(&db, vec![client("cli_f", "dig", 'f')])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn request_rows_list_by_client_and_age_out() {
        let db = super::super::migrated_test_db("model-requests.db").await;
        let row = |id: &str, ts: i64, client: Option<&str>| ModelRequestRow {
            id: id.into(),
            ts,
            door: "chat".into(),
            client_id: client.map(str::to_string),
            status: 200,
            outcome: "answered".into(),
            ..Default::default()
        };
        let now = MODEL_REQUEST_RETENTION_MS * 2;
        insert_model_request(&db, row("old", 1, Some("cli_a")))
            .await
            .unwrap();
        insert_model_request(&db, row("r1", now - 10, Some("cli_a")))
            .await
            .unwrap();
        insert_model_request(&db, row("r2", now, Some("cli_b")))
            .await
            .unwrap();
        let all = list_model_requests(&db, 0, None, 100).await.unwrap();
        assert_eq!(
            all.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r2", "r1"]
        );
        let a = list_model_requests(&db, 0, Some("cli_a"), 100)
            .await
            .unwrap();
        assert_eq!(a.len(), 1);
        let last = last_model_request_by_client(&db).await.unwrap();
        assert!(last.contains(&("cli_b".to_string(), now)));
    }
}
