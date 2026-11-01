//! One usage row per served-API request, refusals included (`model_requests`).
//!
//! A [`RequestLedger`] is opened when a request arrives and written once: by
//! [`RequestLedger::finish`] when the reply ends, or by its drop when the
//! client went away first. It never records a prompt or a reply.

use std::sync::Arc;
use std::time::Instant;

use cairn_db::storage::served_api::{self as store, ModelRequestRow, ServedApiClient};
use cairn_models::interface::relay::{RelayDoor, RelayRoute, RelayUsage};

use super::clients;
use crate::storage::{run_db_blocking, LocalDb};

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The provider answered, and its whole reply reached the client.
    Answered,
    /// Cairn refused it before any provider was asked.
    Refused,
    /// The provider failed, or answered with an error status.
    UpstreamError,
    /// The client went away before the reply ended.
    ClientClosed,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Refused => "refused",
            Self::UpstreamError => "upstream_error",
            Self::ClientClosed => "client_closed",
        }
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The usage row of one request, written exactly once.
pub struct RequestLedger {
    db: Arc<LocalDb>,
    row: Option<ModelRequestRow>,
    started: Instant,
}

impl RequestLedger {
    pub fn begin(
        db: Arc<LocalDb>,
        door: RelayDoor,
        client: Option<&ServedApiClient>,
        stream: bool,
        model_asked: Option<String>,
    ) -> Self {
        let ts = now_ms();
        let principal_json =
            client.and_then(|client| serde_json::to_string(&clients::principal(client)).ok());
        let appearance_json = client.and_then(|client| {
            clients::appearance(client, ts / 1000)
                .ok()
                .and_then(|snapshot| serde_json::to_string(&snapshot).ok())
        });
        let id = format!(
            "req_{}",
            rand::random::<[u8; 8]>()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        Self {
            db,
            row: Some(ModelRequestRow {
                id,
                ts,
                door: door.as_str().to_string(),
                client_id: client.map(|client| client.id.clone()),
                principal_json,
                appearance_json,
                model_asked,
                stream,
                outcome: Outcome::ClientClosed.as_str().to_string(),
                ..Default::default()
            }),
            started: Instant::now(),
        }
    }

    pub fn id(&self) -> &str {
        self.row.as_ref().map(|row| row.id.as_str()).unwrap_or("")
    }

    /// The model the name resolved to.
    pub fn resolved(&mut self, model: &str) {
        if let Some(row) = &mut self.row {
            row.model = Some(model.to_string());
        }
    }

    /// Where the request went, how long it waited first, and the grant that
    /// allowed a remote provider.
    pub fn routed(&mut self, route: &RelayRoute, wait_ms: i64, grant_id: Option<String>) {
        if let Some(row) = &mut self.row {
            row.provider = Some(route.backend_key.clone());
            row.account_id = route.account_id.clone();
            row.sent_model = Some(route.sent_model.clone());
            row.wait_ms = (wait_ms > 0).then_some(wait_ms);
            row.grant_id = grant_id;
        }
    }

    /// The provider the request was meant for, when it was refused before a
    /// route was built.
    pub fn provider(&mut self, provider: &str) {
        if let Some(row) = &mut self.row {
            row.provider = Some(provider.to_string());
        }
    }

    pub fn status(&mut self, status: u16) {
        if let Some(row) = &mut self.row {
            row.status = i64::from(status);
        }
    }

    fn close(
        &mut self,
        outcome: Outcome,
        usage: Option<&RelayUsage>,
        detail: Option<String>,
    ) -> Option<ModelRequestRow> {
        let mut row = self.row.take()?;
        row.outcome = outcome.as_str().to_string();
        row.latency_ms = i64::try_from(self.started.elapsed().as_millis()).unwrap_or(i64::MAX);
        row.detail = detail;
        if let Some(usage) = usage {
            let count = |value: Option<u64>| value.and_then(|value| i64::try_from(value).ok());
            row.input_tokens = count(usage.input_tokens);
            row.output_tokens = count(usage.output_tokens);
            row.cached_tokens = count(usage.cached_tokens);
            row.cost = usage.cost;
            row.upstream_request_id = usage.upstream_request_id.clone();
        }
        Some(row)
    }

    /// Write the row now. Blocking: call it off the async runtime.
    pub fn finish(
        mut self,
        status: u16,
        outcome: Outcome,
        usage: Option<&RelayUsage>,
        detail: Option<String>,
    ) {
        self.status(status);
        if let Some(row) = self.close(outcome, usage, detail) {
            write(&self.db, row);
        }
    }
}

fn write(db: &Arc<LocalDb>, row: ModelRequestRow) {
    let id = row.id.clone();
    let db = db.clone();
    if let Err(error) = run_db_blocking(move || async move {
        store::insert_model_request(&db, row)
            .await
            .map_err(|error| error.to_string())
    }) {
        log::warn!("served API: usage row {id} was not recorded: {error}");
    }
}

/// A ledger dropped unwritten is a request whose client went away: written on
/// a thread of its own so a drop on the async runtime never blocks it.
impl Drop for RequestLedger {
    fn drop(&mut self) {
        if let Some(row) = self.close(Outcome::ClientClosed, None, None) {
            let db = self.db.clone();
            std::thread::spawn(move || write(&db, row));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_finished_request_writes_one_row_with_its_usage() {
        let db = Arc::new(crate::storage::migrated_test_db("served-api-ledger.db").await);
        let (client, _) = clients::mint(&db, "dig").await.unwrap();
        let usage = RelayUsage {
            input_tokens: Some(12),
            output_tokens: Some(3),
            cost: Some(0.002),
            upstream_request_id: Some("gen-1".into()),
            ..Default::default()
        };
        let row_db = db.clone();
        tokio::task::spawn_blocking(move || {
            let mut ledger = RequestLedger::begin(
                row_db,
                RelayDoor::ChatCompletions,
                Some(&client),
                true,
                Some("big".into()),
            );
            ledger.resolved("deepseek/deepseek-v4-flash-vision-exp");
            ledger.finish(200, Outcome::Answered, Some(&usage), None);
        })
        .await
        .unwrap();
        let rows = store::list_model_requests(&db, 0, None, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.outcome, "answered");
        assert_eq!(row.input_tokens, Some(12));
        assert_eq!(row.model_asked.as_deref(), Some("big"));
        assert!(row.principal_json.as_deref().unwrap().contains("cairn-api"));
        assert!(row
            .appearance_json
            .as_deref()
            .unwrap()
            .contains("served_api"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_ledger_records_the_client_closing() {
        let db = Arc::new(crate::storage::migrated_test_db("served-api-ledger-drop.db").await);
        drop(RequestLedger::begin(
            db.clone(),
            RelayDoor::Embeddings,
            None,
            false,
            None,
        ));
        for _ in 0..100 {
            let rows = store::list_model_requests(&db, 0, None, 10).await.unwrap();
            if let Some(row) = rows.first() {
                assert_eq!(row.outcome, "client_closed");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("the dropped ledger wrote no row");
    }
}
