use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

#[derive(Debug, Clone, PartialEq)]
pub struct RouteFactSample {
    pub scope_key: String,
    pub source: String,
    pub identity: String,
    pub fields_json: String,
    pub summary: Option<String>,
    pub observed_at: i64,
}

fn from_row(row: &crate::turso::Row) -> DbResult<RouteFactSample> {
    Ok(RouteFactSample {
        scope_key: row.text(0)?,
        source: row.text(1)?,
        identity: row.text(2)?,
        fields_json: row.text(3)?,
        summary: row.opt_text(4)?,
        observed_at: row.i64(5)?,
    })
}

pub async fn upsert_route_fact_sample(db: &LocalDb, sample: RouteFactSample) -> DbResult<()> {
    db.write(move |conn| {
        let sample = sample.clone();
        Box::pin(async move {
            conn.execute("INSERT INTO route_fact_samples (scope_key,source,identity,fields_json,summary,observed_at) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(scope_key,source) DO UPDATE SET identity=excluded.identity,fields_json=excluded.fields_json,summary=excluded.summary,observed_at=excluded.observed_at WHERE excluded.observed_at >= route_fact_samples.observed_at", params![sample.scope_key,sample.source,sample.identity,sample.fields_json,sample.summary,sample.observed_at]).await?;
            Ok(())
        })
    }).await
}

pub async fn list_route_fact_samples(db: &LocalDb, scope: &str) -> DbResult<Vec<RouteFactSample>> {
    db.query_all("SELECT scope_key,source,identity,fields_json,summary,observed_at FROM route_fact_samples WHERE scope_key=?1 ORDER BY observed_at DESC,source", (scope.to_string(),), from_row).await
}

pub async fn get_route_fact_sample(
    db: &LocalDb,
    scope: &str,
    source: &str,
) -> DbResult<Option<RouteFactSample>> {
    db.query_opt("SELECT scope_key,source,identity,fields_json,summary,observed_at FROM route_fact_samples WHERE scope_key=?1 AND source=?2", (scope.to_string(), source.to_string()), from_row).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retains_latest_per_scope_and_source() {
        let db = crate::storage::migrated_test_db("route-fact-samples.db").await;
        for (identity, observed_at) in [("new", 2), ("old", 1)] {
            upsert_route_fact_sample(
                &db,
                RouteFactSample {
                    scope_key: "workspace".into(),
                    source: "attention".into(),
                    identity: identity.into(),
                    fields_json: "{}".into(),
                    summary: None,
                    observed_at,
                },
            )
            .await
            .unwrap();
        }
        upsert_route_fact_sample(
            &db,
            RouteFactSample {
                scope_key: "project:CAIRN".into(),
                source: "attention".into(),
                identity: "project".into(),
                fields_json: "{}".into(),
                summary: None,
                observed_at: 3,
            },
        )
        .await
        .unwrap();
        let rows = list_route_fact_samples(&db, "workspace").await.unwrap();
        assert_eq!((rows.len(), rows[0].identity.as_str()), (1, "new"));
        assert_eq!(
            get_route_fact_sample(&db, "project:CAIRN", "attention")
                .await
                .unwrap()
                .unwrap()
                .identity,
            "project"
        );
    }
}
