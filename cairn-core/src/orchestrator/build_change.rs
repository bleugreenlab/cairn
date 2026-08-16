//! Persist the runner build seen at boot and queue rebuild facts for active
//! thread sessions. The attention queue supplies passive delivery and durable
//! claiming; this module only owns detection and coalescing.

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage::{DbResult, LocalDb, RowExt};

const PUSH_KEY: &str = "build-change";
const CONTENT_REF_PREFIX: &str = "cairn://system/build-change/";
const RECENT_SECONDS: i64 = 48 * 60 * 60;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildChange {
    pub from_version: String,
    pub from_build_id: String,
    pub to_version: String,
    pub to_build_id: String,
    pub booted_at: i64,
}

/// Record one successful runner boot. A first boot and a same-content boot only
/// advance the singleton. A changed content identity queues one passive push per
/// active thread session with a turn in the preceding 48 hours.
pub async fn record_boot(
    db: &LocalDb,
    version: &str,
    build_id: &str,
    booted_at: i64,
) -> DbResult<usize> {
    let version = version.to_string();
    let build_id = build_id.to_string();
    db.write(|conn| {
        let version = version.clone();
        let build_id = build_id.clone();
        Box::pin(async move {
            let mut state_rows = conn
                .query(
                    "SELECT version, build_id FROM app_boot_state WHERE singleton=1",
                    (),
                )
                .await?;
            let previous = state_rows.next().await?.map(|row| {
                Ok::<_, crate::storage::DbError>((row.text(0)?, row.text(1)?))
            }).transpose()?;
            drop(state_rows);

            conn.execute(
                "INSERT INTO app_boot_state(singleton, version, build_id, booted_at)
                 VALUES(1, ?1, ?2, ?3)
                 ON CONFLICT(singleton) DO UPDATE SET
                   version=excluded.version, build_id=excluded.build_id, booted_at=excluded.booted_at",
                params![version.as_str(), build_id.as_str(), booted_at],
            ).await?;

            let Some((previous_version, previous_build_id)) = previous else {
                return Ok(0);
            };
            if previous_build_id == build_id {
                return Ok(0);
            }

            let cutoff = booted_at - RECENT_SECONDS;
            let mut recipient_rows = conn.query(
                &format!(
                    "SELECT j.id FROM jobs j
                     JOIN threads th ON th.id=j.thread_id
                     WHERE th.status='active'
                       AND {}
                       AND EXISTS (SELECT 1 FROM turns t WHERE t.job_id=j.id AND t.updated_at>=?1)",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![cutoff],
            ).await?;
            let mut recipients = Vec::new();
            while let Some(row) = recipient_rows.next().await? {
                recipients.push(row.text(0)?);
            }
            drop(recipient_rows);

            for recipient in &recipients {
                let mut pending_rows = conn.query(
                    "SELECT n.id FROM attention_pushes p
                     JOIN build_change_notifications n
                       ON p.content_ref=?1 || n.id
                     WHERE p.recipient=?2 AND p.key=?3 AND p.delivered_event_id IS NULL
                     LIMIT 1",
                    params![CONTENT_REF_PREFIX, recipient.as_str(), PUSH_KEY],
                ).await?;
                let pending_id = pending_rows.next().await?.map(|row| row.text(0)).transpose()?;
                drop(pending_rows);

                if let Some(notification_id) = pending_id {
                    // Preserve the original `from`: every intervening boot only
                    // advances the destination of the one undelivered notice.
                    conn.execute(
                        "UPDATE build_change_notifications
                         SET to_version=?1, to_build_id=?2, booted_at=?3 WHERE id=?4",
                        params![version.as_str(), build_id.as_str(), booted_at, notification_id],
                    ).await?;
                    conn.execute(
                        "UPDATE attention_pushes SET created_at=?1
                         WHERE recipient=?2 AND key=?3 AND delivered_event_id IS NULL",
                        params![booted_at, recipient.as_str(), PUSH_KEY],
                    ).await?;
                } else {
                    let notification_id = Uuid::new_v4().to_string();
                    let content_ref = format!("{CONTENT_REF_PREFIX}{notification_id}");
                    conn.execute(
                        "INSERT INTO build_change_notifications
                         (id,recipient,from_version,from_build_id,to_version,to_build_id,booted_at)
                         VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        params![notification_id.as_str(), recipient.as_str(), previous_version.as_str(), previous_build_id.as_str(), version.as_str(), build_id.as_str(), booted_at],
                    ).await?;
                    conn.execute(
                        "INSERT INTO attention_pushes
                         (id,recipient,content_ref,wake,boundary,key,created_at)
                         VALUES(?1,?2,?3,'passive','turn',?4,?5)",
                        params![Uuid::new_v4().to_string(), recipient.as_str(), content_ref.as_str(), PUSH_KEY, booted_at],
                    ).await?;
                }
            }
            Ok(recipients.len())
        })
    }).await
}

pub async fn resolve(db: &LocalDb, content_ref: &str) -> DbResult<Option<BuildChange>> {
    let Some(id) = content_ref.strip_prefix(CONTENT_REF_PREFIX) else {
        return Ok(None);
    };
    let id = id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT from_version,from_build_id,to_version,to_build_id,booted_at
             FROM build_change_notifications WHERE id=?1",
                    params![id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(BuildChange {
                        from_version: row.text(0)?,
                        from_build_id: row.text(1)?,
                        to_version: row.text(2)?,
                        to_build_id: row.text(3)?,
                        booted_at: row.i64(4)?,
                    })
                })
                .transpose()
        })
    })
    .await
}

pub fn render(change: &BuildChange) -> String {
    let when = chrono::DateTime::from_timestamp(change.booted_at, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| change.booted_at.to_string());
    format!(
        "Cairn was rebuilt since your last turn: {} ({}) → {} ({}), booted at {}.",
        change.from_version, change.from_build_id, change.to_version, change.to_build_id, when
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seeded_db() -> LocalDb {
        let db = crate::storage::migrated_test_db("build-change.db").await;
        db.execute_script("\
          INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);\
          INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp/p',1,1);\
          INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('th','p','general','active',1,1);\
          INSERT INTO jobs(id,project_id,status,uri_segment,thread_id,created_at,updated_at) VALUES('thread-job','p','running','thread','th',1,1);\
          INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at) VALUES('s','thread-job','open',1,1,1);\
          INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t','s','thread-job',1,'done','initial',100,100);")
          .await.unwrap();
        db
    }

    #[tokio::test]
    async fn same_build_is_silent_and_changes_coalesce() {
        let db = seeded_db().await;
        assert_eq!(record_boot(&db, "1.0.0", "a", 100).await.unwrap(), 0);
        assert_eq!(record_boot(&db, "1.0.0", "a", 110).await.unwrap(), 0);
        assert!(
            crate::orchestrator::attention_push::list_pending(&db, "thread-job")
                .await
                .unwrap()
                .is_empty()
        );

        assert_eq!(record_boot(&db, "1.0.0", "b", 120).await.unwrap(), 1);
        assert_eq!(record_boot(&db, "1.0.1", "c", 130).await.unwrap(), 1);
        let pushes = crate::orchestrator::attention_push::list_pending(&db, "thread-job")
            .await
            .unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(
            pushes[0].wake,
            crate::orchestrator::attention_push::Wake::Passive
        );
        let change = resolve(&db, &pushes[0].content_ref).await.unwrap().unwrap();
        assert_eq!(
            (change.from_build_id.as_str(), change.to_build_id.as_str()),
            ("a", "c")
        );
        assert!(
            !crate::orchestrator::attention_push::has_pending_waking_live(&db, "thread-job")
                .await
                .unwrap(),
            "a build-change ride-along must never pass the idle wake gate"
        );
        assert!(render(&change).contains("1.0.0 (a) → 1.0.1 (c)"));
    }
}
