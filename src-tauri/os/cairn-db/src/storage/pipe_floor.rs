//! The fresh-pipe floor's engagements and the idle sessions released while one
//! was open (cairn/6440). Private, per-install: the rows describe this
//! install's hosts.

use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

/// What the floor knew when it engaged. Every write carries it, so the open
/// and the clearance land in either order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeFloorEngagementOpen {
    pub id: String,
    pub host: String,
    pub build_id: String,
    pub opened_at_ms: i64,
    pub opening_reading_bytes: Option<i64>,
    pub opening_at_least: bool,
    pub opening_assessment: String,
}

/// The bounded release experiment's state on one engagement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reclamation {
    Pending,
    Running,
    Recovered,
    NotRecovered,
    Inconclusive,
    Disabled,
    Skipped,
}

impl Reclamation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Recovered => "recovered",
            Self::NotRecovered => "not_recovered",
            Self::Inconclusive => "inconclusive",
            Self::Disabled => "disabled",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeFloorEngagement {
    pub open: PipeFloorEngagementOpen,
    pub cleared_at_ms: Option<i64>,
    pub clearance: Option<String>,
    pub census_json: Option<String>,
    pub census_at_ms: Option<i64>,
    pub reclamation: String,
    pub reclamation_detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipeFloorRelease {
    pub engagement_id: String,
    pub seq: i64,
    pub run_id: String,
    pub session_id: Option<String>,
    pub job_id: Option<String>,
    pub backend: Option<String>,
    pub released_at_ms: i64,
    pub leader_exit_confirmed: bool,
    pub before_bytes: Option<i64>,
    pub after_bytes: Option<i64>,
    pub after_at_least: bool,
    pub census_delta_json: Option<String>,
    pub surviving_descendants_json: Option<String>,
    pub outcome: String,
}

const ENGAGEMENT_COLUMNS: &str = "id, host, build_id, opened_at_ms, opening_reading_bytes, \
     opening_at_least, opening_assessment, cleared_at_ms, clearance, census_json, census_at_ms, \
     reclamation, reclamation_detail";

const RELEASE_COLUMNS: &str = "engagement_id, seq, run_id, session_id, job_id, backend, \
     released_at_ms, leader_exit_confirmed, before_bytes, after_bytes, after_at_least, \
     census_delta_json, surviving_descendants_json, outcome";

fn engagement(row: &turso::Row) -> DbResult<PipeFloorEngagement> {
    Ok(PipeFloorEngagement {
        open: PipeFloorEngagementOpen {
            id: row.text(0)?,
            host: row.text(1)?,
            build_id: row.text(2)?,
            opened_at_ms: row.i64(3)?,
            opening_reading_bytes: row.opt_i64(4)?,
            opening_at_least: row.i64(5)? != 0,
            opening_assessment: row.text(6)?,
        },
        cleared_at_ms: row.opt_i64(7)?,
        clearance: row.opt_text(8)?,
        census_json: row.opt_text(9)?,
        census_at_ms: row.opt_i64(10)?,
        reclamation: row.text(11)?,
        reclamation_detail: row.opt_text(12)?,
    })
}

fn release(row: &turso::Row) -> DbResult<PipeFloorRelease> {
    Ok(PipeFloorRelease {
        engagement_id: row.text(0)?,
        seq: row.i64(1)?,
        run_id: row.text(2)?,
        session_id: row.opt_text(3)?,
        job_id: row.opt_text(4)?,
        backend: row.opt_text(5)?,
        released_at_ms: row.i64(6)?,
        leader_exit_confirmed: row.i64(7)? != 0,
        before_bytes: row.opt_i64(8)?,
        after_bytes: row.opt_i64(9)?,
        after_at_least: row.i64(10)? != 0,
        census_delta_json: row.opt_text(11)?,
        surviving_descendants_json: row.opt_text(12)?,
        outcome: row.text(13)?,
    })
}

const INSERT_OPEN: &str = "INSERT INTO pipe_floor_engagements (id, host, build_id, opened_at_ms, \
     opening_reading_bytes, opening_at_least, opening_assessment) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

fn open_params(
    open: &PipeFloorEngagementOpen,
) -> (String, String, String, i64, Option<i64>, i64, String) {
    (
        open.id.clone(),
        open.host.clone(),
        open.build_id.clone(),
        open.opened_at_ms,
        open.opening_reading_bytes,
        i64::from(open.opening_at_least),
        open.opening_assessment.clone(),
    )
}

/// Record an engagement. A repeat, or one that lands after its clearance,
/// changes nothing.
pub async fn open_engagement(db: &LocalDb, open: &PipeFloorEngagementOpen) -> DbResult<()> {
    db.execute(
        format!("{INSERT_OPEN} ON CONFLICT(id) DO NOTHING"),
        open_params(open),
    )
    .await?;
    Ok(())
}

/// Record how an engagement cleared, creating its row if the open has not
/// landed yet.
pub async fn clear_engagement(
    db: &LocalDb,
    open: &PipeFloorEngagementOpen,
    cleared_at_ms: i64,
    clearance: &str,
) -> DbResult<()> {
    let (id, host, build_id, opened_at_ms, bytes, at_least, assessment) = open_params(open);
    db.execute(
        "INSERT INTO pipe_floor_engagements (id, host, build_id, opened_at_ms, \
         opening_reading_bytes, opening_at_least, opening_assessment, cleared_at_ms, clearance) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT(id) DO UPDATE SET \
         cleared_at_ms = excluded.cleared_at_ms, clearance = excluded.clearance",
        params![
            id,
            host,
            build_id,
            opened_at_ms,
            bytes,
            at_least,
            assessment,
            cleared_at_ms,
            clearance.to_string()
        ],
    )
    .await?;
    Ok(())
}

pub async fn record_census(db: &LocalDb, id: &str, census_json: &str, at_ms: i64) -> DbResult<()> {
    db.execute(
        "UPDATE pipe_floor_engagements SET census_json = ?2, census_at_ms = ?3 WHERE id = ?1",
        (id.to_string(), census_json.to_string(), at_ms),
    )
    .await?;
    Ok(())
}

pub async fn set_reclamation(
    db: &LocalDb,
    id: &str,
    state: Reclamation,
    detail: Option<&str>,
) -> DbResult<()> {
    db.execute(
        "UPDATE pipe_floor_engagements SET reclamation = ?2, reclamation_detail = ?3 WHERE id = ?1",
        (
            id.to_string(),
            state.as_str().to_string(),
            detail.map(str::to_string),
        ),
    )
    .await?;
    Ok(())
}

pub async fn insert_release(db: &LocalDb, release: &PipeFloorRelease) -> DbResult<()> {
    db.execute(
        format!(
            "INSERT INTO pipe_floor_releases ({RELEASE_COLUMNS}) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
        ),
        params![
            release.engagement_id.clone(),
            release.seq,
            release.run_id.clone(),
            release.session_id.clone(),
            release.job_id.clone(),
            release.backend.clone(),
            release.released_at_ms,
            i64::from(release.leader_exit_confirmed),
            release.before_bytes,
            release.after_bytes,
            i64::from(release.after_at_least),
            release.census_delta_json.clone(),
            release.surviving_descendants_json.clone(),
            release.outcome.clone()
        ],
    )
    .await?;
    Ok(())
}

/// The host's most recent engagement and its releases in order.
pub async fn latest_engagement(
    db: &LocalDb,
    host: &str,
) -> DbResult<Option<(PipeFloorEngagement, Vec<PipeFloorRelease>)>> {
    let Some(latest) = db
        .query_opt(
            format!(
                "SELECT {ENGAGEMENT_COLUMNS} FROM pipe_floor_engagements WHERE host = ?1 \
                 ORDER BY opened_at_ms DESC, id DESC LIMIT 1"
            ),
            (host.to_string(),),
            engagement,
        )
        .await?
    else {
        return Ok(None);
    };
    let releases = db
        .query_all(
            format!(
                "SELECT {RELEASE_COLUMNS} FROM pipe_floor_releases WHERE engagement_id = ?1 \
                 ORDER BY seq"
            ),
            (latest.open.id.clone(),),
            release,
        )
        .await?;
    Ok(Some((latest, releases)))
}

/// The engagement whose `not_recovered` verdict disabled reclamation on this
/// host for this build, if one has. Another build's verdict does not count:
/// the next lever is a code change, and it deserves a fresh verdict.
pub async fn reclamation_disabled_by(
    db: &LocalDb,
    host: &str,
    build_id: &str,
) -> DbResult<Option<PipeFloorEngagement>> {
    db.query_opt(
        format!(
            "SELECT {ENGAGEMENT_COLUMNS} FROM pipe_floor_engagements WHERE host = ?1 \
             AND build_id = ?2 AND reclamation = 'not_recovered' \
             ORDER BY opened_at_ms, id LIMIT 1"
        ),
        (host.to_string(), build_id.to_string()),
        engagement,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    fn open(id: &str, build: &str, at: i64) -> PipeFloorEngagementOpen {
        PipeFloorEngagementOpen {
            id: id.into(),
            host: "local".into(),
            build_id: build.into(),
            opened_at_ms: at,
            opening_reading_bytes: Some(512),
            opening_at_least: false,
            opening_assessment: "pipe health: degraded".into(),
        }
    }

    fn released(id: &str, seq: i64) -> PipeFloorRelease {
        PipeFloorRelease {
            engagement_id: id.into(),
            seq,
            run_id: format!("run-{seq}"),
            session_id: Some("s".into()),
            job_id: Some("j".into()),
            backend: Some("claude-cli".into()),
            released_at_ms: 100 + seq,
            leader_exit_confirmed: true,
            before_bytes: Some(512),
            after_bytes: Some(512),
            after_at_least: false,
            census_delta_json: Some("{}".into()),
            surviving_descendants_json: Some("[]".into()),
            outcome: "released".into(),
        }
    }

    #[tokio::test]
    async fn an_engagement_round_trips_with_its_releases_in_either_write_order() {
        let db = migrated_test_db("pipe-floor-round-trip").await;
        let first = open("e1", "b1", 10);
        // The clearance can land before the open; the open then changes nothing.
        clear_engagement(&db, &first, 50, "measured recovery")
            .await
            .unwrap();
        open_engagement(&db, &first).await.unwrap();
        record_census(&db, "e1", r#"{"totals":{}}"#, 12)
            .await
            .unwrap();
        insert_release(&db, &released("e1", 2)).await.unwrap();
        insert_release(&db, &released("e1", 1)).await.unwrap();
        set_reclamation(&db, "e1", Reclamation::Recovered, Some("512 -> 65536 B"))
            .await
            .unwrap();

        let (row, releases) = latest_engagement(&db, "local").await.unwrap().unwrap();
        assert_eq!(row.open, first);
        assert_eq!(row.cleared_at_ms, Some(50));
        assert_eq!(row.clearance.as_deref(), Some("measured recovery"));
        assert_eq!(row.census_at_ms, Some(12));
        assert_eq!(row.reclamation, "recovered");
        assert_eq!(
            releases
                .iter()
                .map(|release| release.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(releases[0], released("e1", 1));
        assert!(latest_engagement(&db, "other").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_not_recovered_verdict_disables_only_its_own_build_on_its_own_host() {
        let db = migrated_test_db("pipe-floor-disable").await;
        open_engagement(&db, &open("e1", "b1", 10)).await.unwrap();
        let before = reclamation_disabled_by(&db, "local", "b1").await.unwrap();
        assert!(before.is_none());
        set_reclamation(&db, "e1", Reclamation::NotRecovered, None)
            .await
            .unwrap();
        let by = reclamation_disabled_by(&db, "local", "b1").await.unwrap();
        assert_eq!(by.map(|row| row.open.id), Some("e1".to_string()));
        let new_build = reclamation_disabled_by(&db, "local", "b2").await.unwrap();
        assert!(new_build.is_none(), "a new build runs the experiment again");
        let other_host = reclamation_disabled_by(&db, "fedora", "b1").await.unwrap();
        assert!(other_host.is_none());
    }
}
