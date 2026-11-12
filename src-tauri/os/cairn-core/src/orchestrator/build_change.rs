//! Persist the runner build seen at boot, and tell base camp when it changed.
//!
//! A rebuild concerns every thread, so it is addressed to each active
//! project's base camp once, never to a list of threads: the fact log expands
//! base camp to its members, every open thread (`crate::camps`), and each
//! reads it passively with its next run.
//!
//! A project outside the activity window is still told when a rebuild
//! subscription of one of its threads fires on this boot: the firing
//! (`orchestrator::wakes::rebuild`) mints that project's notice through
//! [`announce_project`], so the subscriber's wake rides beside its camp fact.

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// A project is told about a rebuild when one of its threads took a turn this
/// recently. A project nobody is working in hears about the next rebuild after
/// someone returns, rather than accumulating a line for every boot.
const RECENT_SECONDS: i64 = 48 * 60 * 60;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildChange {
    pub from_version: String,
    pub from_build_id: String,
    pub to_version: String,
    pub to_build_id: String,
    pub booted_at: i64,
}

/// Record one successful runner boot and return the rebuild notice still owed,
/// if any.
///
/// The obligation is written in the same transaction that advances the build
/// identity, so the identity can never be acknowledged without the notice it
/// owes being recorded: a crash before every base camp is told, or a failed
/// mint, leaves it in place for the next boot to finish. An owed notice keeps
/// its original starting build across further rebuilds, so one notice spans
/// them all; a rebuild back to that starting build owes nothing. A first boot
/// owes nothing.
pub async fn record_boot(
    db: &LocalDb,
    version: &str,
    build_id: &str,
    booted_at: i64,
) -> DbResult<Option<BuildChange>> {
    let version = version.to_string();
    let build_id = build_id.to_string();
    db.write(|conn| {
        let version = version.clone();
        let build_id = build_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT version, build_id, pending_from_version, pending_from_build_id
                     FROM app_boot_state WHERE singleton=1",
                    (),
                )
                .await?;
            let previous = match rows.next().await? {
                Some(row) => Some((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                )),
                None => None,
            };
            drop(rows);
            let owed = match previous {
                None => None,
                Some((_, _, Some(from_version), Some(from_build_id))) => {
                    Some((from_version, from_build_id))
                }
                Some((prev_version, prev_build_id, _, _)) => {
                    (prev_build_id != build_id).then_some((prev_version, prev_build_id))
                }
            }
            .filter(|(_, from_build_id)| *from_build_id != build_id);
            let (pending_version, pending_build) = owed.clone().unzip();
            conn.execute(
                "INSERT INTO app_boot_state(singleton, version, build_id, booted_at,
                                            pending_from_version, pending_from_build_id)
                 VALUES(1, ?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(singleton) DO UPDATE SET
                   version=excluded.version, build_id=excluded.build_id, booted_at=excluded.booted_at,
                   pending_from_version=excluded.pending_from_version,
                   pending_from_build_id=excluded.pending_from_build_id",
                params![
                    version.as_str(),
                    build_id.as_str(),
                    booted_at,
                    pending_version,
                    pending_build
                ],
            )
            .await?;
            Ok(owed.map(|(from_version, from_build_id)| BuildChange {
                from_version,
                from_build_id,
                to_version: version.clone(),
                to_build_id: build_id.clone(),
                booted_at,
            }))
        })
    })
    .await
}

/// Clear the owed notice once every base camp was told about `change`. Only
/// the exact obligation clears: one a later boot replaced stays owed.
pub async fn settle(db: &LocalDb, change: &BuildChange) -> DbResult<()> {
    db.execute(
        "UPDATE app_boot_state SET pending_from_version = NULL, pending_from_build_id = NULL
         WHERE singleton = 1 AND pending_from_build_id = ?1 AND build_id = ?2",
        params![change.from_build_id.as_str(), change.to_build_id.as_str()],
    )
    .await?;
    Ok(())
}

pub fn render(change: &BuildChange) -> String {
    let when = chrono::DateTime::from_timestamp(change.booted_at, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| change.booted_at.to_string());
    format!(
        "Cairn was rebuilt: {} ({}) → {} ({}), booted at {}.",
        change.from_version, change.from_build_id, change.to_version, change.to_build_id, when
    )
}

/// The projects of one database whose threads took a turn since `cutoff`.
async fn active_projects(db: &LocalDb, cutoff: i64) -> DbResult<Vec<(String, String)>> {
    db.query_all(
        format!(
            "SELECT p.id, p.key FROM projects p
             WHERE EXISTS (
                 SELECT 1 FROM threads th JOIN jobs j ON j.thread_id = th.id
                 WHERE th.project_id = p.id AND th.status = 'active' AND {}
                   AND EXISTS (SELECT 1 FROM turns t WHERE t.job_id = j.id AND t.updated_at >= ?1)
             )
             ORDER BY p.key",
            crate::threads::SESSION_JOB_SHAPE
        ),
        params![cutoff],
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
}

/// What one announcement did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Announced {
    pub minted: usize,
    pub failures: usize,
}

/// One project's notice identity: the obligation's (from, to) pair and the
/// project, so an announcement retried at a later boot, or a firing that
/// mints a project the announcement passed over, is the same one fact.
pub fn notice_identity(change: &BuildChange, project_id: &str) -> String {
    format!(
        "build-change:{}:{}:{project_id}",
        change.from_build_id, change.to_build_id
    )
}

/// Tell one project's base camp about `change`: one fact, however many
/// threads the project has. Returns the row id, or `None` when the project
/// was already told.
pub async fn announce_project(
    orch: &Orchestrator,
    change: &BuildChange,
    project_id: &str,
    project_key: &str,
) -> Result<Option<i64>, String> {
    crate::camps::notify_camp(
        orch,
        crate::camps::CampNotice {
            project_id,
            project_key,
            camp_slug: crate::models::BASE_CAMP_SLUG,
            kind: "rebuild",
            identity: notice_identity(change, project_id),
            summary: render(change),
            detail_uri: None,
            actor_job_id: None,
        },
    )
    .await
}

/// Address one rebuild notice to the base camp of every active project in
/// every open database, and settle the obligation only when nothing failed.
/// Each project's notice identity is [`notice_identity`], so an announcement
/// retried at a later boot mints only what it did not mint before.
pub async fn announce(orch: &Orchestrator, change: &BuildChange) -> Announced {
    let mut minted = 0;
    let mut failures = 0;
    for db in orch.db.all_dbs().await {
        let projects = match active_projects(&db, change.booted_at - RECENT_SECONDS).await {
            Ok(projects) => projects,
            Err(error) => {
                log::warn!("could not list projects to tell about the rebuild: {error}");
                failures += 1;
                continue;
            }
        };
        for (project_id, project_key) in projects {
            match announce_project(orch, change, &project_id, &project_key).await {
                Ok(Some(_)) => minted += 1,
                Ok(None) => {}
                Err(error) => {
                    failures += 1;
                    log::warn!(
                        "could not tell {project_key}'s base camp about the rebuild: {error}"
                    )
                }
            }
        }
    }
    if failures == 0 {
        if let Err(error) = settle(&orch.db.local, change).await {
            log::warn!("could not settle the rebuild notice: {error}");
        }
    }
    Announced { minted, failures }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ends(change: &BuildChange) -> (&str, &str) {
        (change.from_build_id.as_str(), change.to_build_id.as_str())
    }

    #[tokio::test]
    async fn an_owed_notice_survives_reboots_until_it_is_settled() {
        let db = crate::storage::migrated_test_db("build-change.db").await;
        assert_eq!(record_boot(&db, "1.0.0", "a", 100).await.unwrap(), None);
        assert_eq!(record_boot(&db, "1.0.0", "a", 110).await.unwrap(), None);
        let change = record_boot(&db, "1.0.1", "c", 130).await.unwrap().unwrap();
        assert_eq!(ends(&change), ("a", "c"));
        assert!(render(&change).contains("1.0.0 (a) → 1.0.1 (c)"));

        // A crash before the announcement, or a failed mint, leaves it owed:
        // booting the same build again still returns it.
        let again = record_boot(&db, "1.0.1", "c", 140).await.unwrap().unwrap();
        assert_eq!(ends(&again), ("a", "c"));
        // A further rebuild while owed keeps the original starting build.
        let later = record_boot(&db, "1.0.2", "d", 150).await.unwrap().unwrap();
        assert_eq!(ends(&later), ("a", "d"));
        // Settling a superseded obligation clears nothing.
        settle(&db, &change).await.unwrap();
        assert_eq!(
            ends(&record_boot(&db, "1.0.2", "d", 160).await.unwrap().unwrap()),
            ("a", "d")
        );
        settle(&db, &later).await.unwrap();
        assert_eq!(record_boot(&db, "1.0.2", "d", 170).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_rebuild_back_to_the_owed_start_owes_nothing() {
        let db = crate::storage::migrated_test_db("build-change-back.db").await;
        record_boot(&db, "1", "a", 1).await.unwrap();
        assert!(record_boot(&db, "2", "b", 2).await.unwrap().is_some());
        assert_eq!(record_boot(&db, "1", "a", 3).await.unwrap(), None);
    }
}
