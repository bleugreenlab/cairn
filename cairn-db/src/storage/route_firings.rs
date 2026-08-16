use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

pub const ROUTE_FIRING_RETENTION: i64 = 200;

/// A firing journal is read at a glance, not archived, so each content snapshot
/// is bounded. Real payloads (a phone notification, an issue title) sit far
/// under this; the cap only stops one pathological fact from dominating the 200
/// rows a route retains.
pub const SNAPSHOT_CHARS: usize = 2000;

/// Bound a content snapshot for the firing journal, marking any elision so a
/// reader never mistakes a cut for the whole payload.
pub fn firing_snapshot(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut bounded: String = text.chars().take(SNAPSHOT_CHARS).collect();
    if bounded.chars().count() < text.chars().count() {
        bounded.push('…');
    }
    Some(bounded)
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewRouteFiring {
    pub route_id: String,
    pub scope_key: String,
    pub project_id: Option<String>,
    pub trigger_source: String,
    pub fact_identity: String,
    /// What the incoming fact said, rendered by its producer at fire time.
    pub fact_summary: Option<String>,
    pub status: String,
    pub drop_reason: Option<String>,
    pub transforms_json: Option<String>,
    pub sink_kind: String,
    pub sink_ref: Option<String>,
    /// The content this firing carried to its sink. Present whenever the firing
    /// produced a payload, delivered or not — `status` says whether it arrived.
    pub payload_text: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteFiringRecord {
    pub id: String,
    pub route_id: String,
    pub scope_key: String,
    pub seq: i64,
    pub project_id: Option<String>,
    pub trigger_source: String,
    pub fact_identity: String,
    pub fact_summary: Option<String>,
    pub status: String,
    pub drop_reason: Option<String>,
    pub transforms_json: Option<String>,
    pub sink_kind: String,
    pub sink_ref: Option<String>,
    pub payload_text: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
}
const COLUMNS: &str = "id,route_id,scope_key,seq,project_id,trigger_source,fact_identity,status,drop_reason,transforms_json,sink_kind,sink_ref,error,created_at,fact_summary,payload_text";
fn from_row(row: &crate::turso::Row) -> DbResult<RouteFiringRecord> {
    Ok(RouteFiringRecord {
        id: row.text(0)?,
        route_id: row.text(1)?,
        scope_key: row.text(2)?,
        seq: row.i64(3)?,
        project_id: row.opt_text(4)?,
        trigger_source: row.text(5)?,
        fact_identity: row.text(6)?,
        status: row.text(7)?,
        drop_reason: row.opt_text(8)?,
        transforms_json: row.opt_text(9)?,
        sink_kind: row.text(10)?,
        sink_ref: row.opt_text(11)?,
        error: row.opt_text(12)?,
        created_at: row.i64(13)?,
        fact_summary: row.opt_text(14)?,
        payload_text: row.opt_text(15)?,
    })
}
pub async fn insert_route_firing(db: &LocalDb, new: NewRouteFiring) -> DbResult<RouteFiringRecord> {
    let mut new = new;
    new.fact_identity = cairn_common::uri::canonicalize_uri_identity(&new.fact_identity);
    new.sink_ref = new
        .sink_ref
        .map(|value| cairn_common::uri::canonicalize_uri_identity(&value));
    let id = uuid::Uuid::new_v4().to_string();
    let result_id = id.clone();
    let scope = new.scope_key.clone();
    let route = new.route_id.clone();
    db.write(move |conn| { let new=new.clone(); let id=id.clone(); Box::pin(async move {
        let mut rows=conn.query("SELECT COALESCE(MAX(seq),0)+1 FROM route_firings WHERE scope_key=?1 AND route_id=?2", params![new.scope_key.clone(),new.route_id.clone()]).await?;
        let seq=rows.next().await?.expect("aggregate row").i64(0)?;
        conn.execute("INSERT INTO route_firings (id,route_id,scope_key,seq,project_id,trigger_source,fact_identity,status,drop_reason,transforms_json,sink_kind,sink_ref,error,created_at,fact_summary,payload_text) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)", params![id,new.route_id.clone(),new.scope_key.clone(),seq,new.project_id,new.trigger_source,new.fact_identity,new.status,new.drop_reason,new.transforms_json,new.sink_kind,new.sink_ref,new.error,new.created_at,new.fact_summary,new.payload_text]).await?;
        conn.execute("DELETE FROM route_firings WHERE scope_key=?1 AND route_id=?2 AND seq<=?3",params![new.scope_key,new.route_id,seq-ROUTE_FIRING_RETENTION]).await?; Ok(())
    })}).await?;
    let seq = db
        .query_opt_i64("SELECT seq FROM route_firings WHERE id=?1", (result_id,))
        .await?
        .expect("inserted firing");
    get_route_firing(db, &scope, &route, seq)
        .await?
        .ok_or_else(|| super::DbError::internal("inserted route firing disappeared"))
}
pub async fn list_route_firings(
    db: &LocalDb,
    scope: &str,
    route: &str,
    limit: i64,
) -> DbResult<Vec<RouteFiringRecord>> {
    db.query_all(format!("SELECT {COLUMNS} FROM route_firings WHERE scope_key=?1 AND route_id=?2 ORDER BY seq DESC LIMIT ?3"),(scope.to_string(),route.to_string(),limit.clamp(1,ROUTE_FIRING_RETENTION)),from_row).await
}
pub async fn get_route_firing(
    db: &LocalDb,
    scope: &str,
    route: &str,
    seq: i64,
) -> DbResult<Option<RouteFiringRecord>> {
    db.query_opt(
        format!(
            "SELECT {COLUMNS} FROM route_firings WHERE scope_key=?1 AND route_id=?2 AND seq=?3"
        ),
        (scope.to_string(), route.to_string(), seq),
        from_row,
    )
    .await
}
pub async fn count_route_firings(db: &LocalDb, scope: &str, route: &str) -> DbResult<i64> {
    Ok(db
        .query_opt_i64(
            "SELECT COUNT(*) FROM route_firings WHERE scope_key=?1 AND route_id=?2",
            (scope.to_string(), route.to_string()),
        )
        .await?
        .unwrap_or(0))
}
pub async fn has_recent_fact(
    db: &LocalDb,
    scope: &str,
    route: &str,
    identity: &str,
    since: i64,
) -> DbResult<bool> {
    let identity = cairn_common::uri::canonicalize_uri_identity(identity);
    Ok(db.query_opt_i64("SELECT 1 FROM route_firings WHERE scope_key=?1 AND route_id=?2 AND fact_identity=?3 AND status IN ('fired','dropped') AND created_at>=?4 LIMIT 1",(scope.to_string(),route.to_string(),identity,since)).await?.is_some())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn firing(n: i64) -> NewRouteFiring {
        NewRouteFiring {
            route_id: "r".into(),
            scope_key: "workspace".into(),
            project_id: None,
            trigger_source: "attention".into(),
            fact_identity: format!("f{n}"),
            fact_summary: Some(format!("fact {n} needs review")),
            status: "fired".into(),
            drop_reason: None,
            transforms_json: None,
            sink_kind: "channel".into(),
            sink_ref: None,
            payload_text: Some(format!("delivered {n}")),
            error: None,
            created_at: n,
        }
    }
    #[tokio::test]
    async fn allocates_and_prunes() {
        let db = crate::storage::migrated_test_db("route-retention.db").await;
        for n in 1..=250 {
            insert_route_firing(&db, firing(n)).await.unwrap();
        }
        let rows = list_route_firings(&db, "workspace", "r", 200)
            .await
            .unwrap();
        assert_eq!(rows.len(), 200);
        assert_eq!((rows[0].seq, rows[199].seq), (250, 51));
        assert_eq!(
            rows[0].fact_summary.as_deref(),
            Some("fact 250 needs review")
        );
        assert_eq!(rows[0].payload_text.as_deref(), Some("delivered 250"));
        assert!(has_recent_fact(&db, "workspace", "r", "f250", 200)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failed_firings_do_not_poison_dedupe() {
        let db = crate::storage::migrated_test_db("route-failed-dedupe.db").await;
        let mut failed = firing(1);
        failed.fact_identity = "retryable".into();
        failed.status = "failed".into();
        insert_route_firing(&db, failed).await.unwrap();
        assert!(!has_recent_fact(&db, "workspace", "r", "retryable", 0)
            .await
            .unwrap());
    }

    #[test]
    fn snapshots_bound_content_and_mark_elision() {
        assert_eq!(firing_snapshot("   "), None);
        assert_eq!(firing_snapshot("  hello  ").as_deref(), Some("hello"));
        let long = "é".repeat(SNAPSHOT_CHARS + 10);
        let bounded = firing_snapshot(&long).expect("long text snapshots");
        assert_eq!(bounded.chars().count(), SNAPSHOT_CHARS + 1);
        assert!(bounded.ends_with('…'));
    }
}
