//! Durable incident intents and atomic issue/evidence filing.
//!
//! The journal commits before filing. A failed issue/comment transaction leaves
//! an inspectable, replayable intent; check-observation retention cannot erase it.

use crate::issues::crud::{IssueAuthorship, IssueCreationContext};
use crate::issues::{comments, crud, relations};
use crate::models::CreateIssue;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_common::ids;
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

const NEW_ISSUES_PER_PASS: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct IncidentDraft {
    pub fingerprint: String,
    pub identity: String,
    pub signature: String,
    pub class: String,
    pub title: String,
    pub body: String,
    pub owner_uri: Option<String>,
    pub existing_issue_uri: Option<String>,
    pub event_key: String,
    pub observation_id: Option<String>,
    #[serde(default)]
    pub observed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IncidentLink {
    pub issue_uri: String,
    pub created: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingStatus {
    pub count: i64,
    pub last_error: Option<String>,
}

pub(super) async fn pending_status(db: &LocalDb, project_id: &str) -> DbResult<PendingStatus> {
    db.query_one(
        "SELECT COUNT(*), MAX(last_error) FROM main_incident_evidence
         WHERE project_id = ?1 AND filed_at IS NULL",
        params![project_id],
        |row| {
            Ok(PendingStatus {
                count: row.i64(0)?,
                last_error: row.opt_text(1)?,
            })
        },
    )
    .await
}

/// Commit intent before attempting to file; an empty batch still retries pending work.
/// Links include drained work and already-filed deliveries, deduplicated by issue.
pub(super) async fn ingest(
    db: &LocalDb,
    project_id: &str,
    drafts: Vec<IncidentDraft>,
    authorship: IssueAuthorship,
    now: i64,
) -> DbResult<Vec<IncidentLink>> {
    let (author, appearance) = crud::encode_issue_authorship(&authorship)?;
    let project_id = project_id.to_string();
    let intents = drafts
        .iter()
        .map(|draft| {
            serde_json::to_string(draft)
                .map(|json| (draft.clone(), json))
                .map_err(|e| DbError::Row(format!("incident draft serialization: {e}")))
        })
        .collect::<DbResult<Vec<_>>>()?;
    db.write(|conn| {
        let intents = intents.clone();
        let project_id = project_id.clone();
        let author = author.clone();
        let appearance = appearance.clone();
        Box::pin(async move {
            for (draft, json) in intents {
                conn.execute(
                    "INSERT INTO main_incident_evidence
                     (project_id, fingerprint, event_key, draft_json, body, observation_id,
                      author_principal_json, appearance_snapshot_json, created_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
                     ON CONFLICT(project_id, fingerprint, event_key) DO NOTHING",
                    params![
                        project_id.as_str(),
                        draft.fingerprint,
                        draft.event_key,
                        json,
                        draft.body,
                        draft.observation_id,
                        author.as_str(),
                        appearance.as_str(),
                        now
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await?;
    let mut links = drain_pending_scoped(db, Some(&project_id), now).await?;
    // Historical duplicate events resolve to their original issue even after recurrence.
    for draft in drafts {
        let uri = db
            .query_opt(
                "SELECT issue_uri FROM main_incident_evidence
             WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3 AND filed_at IS NOT NULL",
                params![project_id.as_str(), draft.fingerprint, draft.event_key],
                |row| row.text(0),
            )
            .await?;
        if let Some(issue_uri) = uri {
            merge_link(
                &mut links,
                IncidentLink {
                    issue_uri,
                    created: false,
                },
            );
        }
    }
    Ok(links)
}

fn merge_link(links: &mut Vec<IncidentLink>, link: IncidentLink) {
    if let Some(prior) = links
        .iter_mut()
        .find(|prior| prior.issue_uri == link.issue_uri)
    {
        prior.created |= link.created;
    } else {
        links.push(link);
    }
}

/// Startup/heartbeat replay uses stored provenance, not the retrying actor.
/// Every invocation may create at most ten issues across all projects.
pub(super) async fn drain_pending(db: &LocalDb, now: i64) -> DbResult<Vec<IncidentLink>> {
    drain_pending_scoped(db, None, now).await
}

async fn drain_pending_scoped(
    db: &LocalDb,
    project_id: Option<&str>,
    now: i64,
) -> DbResult<Vec<IncidentLink>> {
    let pending = db
        .query_all(
            "SELECT e.project_id, e.fingerprint, e.event_key FROM main_incident_evidence e
             WHERE e.filed_at IS NULL AND (?1 IS NULL OR e.project_id=?1)
             ORDER BY e.attempts, CASE WHEN
                 EXISTS (SELECT 1 FROM main_incidents m JOIN issues i ON i.id=m.issue_id
                     WHERE m.project_id=e.project_id AND m.fingerprint=e.fingerprint
                     AND i.closed_at IS NULL AND i.merged_at IS NULL
                     AND i.status NOT IN ('closed','merged'))
                 OR json_extract(e.draft_json, '$.existing_issue_uri') IS NOT NULL
                 THEN 0 ELSE 1 END,
                 e.created_at, e.project_id, e.fingerprint, e.event_key LIMIT 100",
            params![project_id],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await?;
    let mut links = Vec::new();
    let mut created = 0;
    let mut first_error = None;
    for (project_id, fingerprint, event_key) in pending {
        let result = db
            .write(|conn| {
                let project_id = project_id.clone();
                let fingerprint = fingerprint.clone();
                let event_key = event_key.clone();
                Box::pin(async move {
                    file_conn(
                        conn,
                        &project_id,
                        &fingerprint,
                        &event_key,
                        created < NEW_ISSUES_PER_PASS,
                        now,
                    )
                    .await
                })
            })
            .await;
        match result {
            Ok(Some(link)) => {
                created += usize::from(link.created);
                merge_link(&mut links, link);
            }
            Ok(None) => {}
            Err(error) => {
                // Filing rolled back, including issue allocation and comment. Keep
                // the original snapshot and expose why its durable intent is pending.
                db.execute(
                    "UPDATE main_incident_evidence SET last_error=?4, attempts=attempts+1
                     WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3 AND filed_at IS NULL",
                    params![project_id, fingerprint, event_key, error.to_string()],
                )
                .await?;
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(links),
    }
}

/// Resolve a durable target before considering ownership claims. Delayed evidence
/// belongs to the first generation covering its observation time, even if a
/// newer generation is open.
async fn linked_target_conn(
    conn: &TrackedConnection,
    project_id: &str,
    fingerprint: &str,
    observed_at: Option<i64>,
) -> DbResult<(Option<(String, i64)>, i64)> {
    let mut rows = conn
        .query(
            "SELECT generation, issue_id FROM main_incidents
         WHERE project_id=?1 AND fingerprint=?2 ORDER BY generation DESC LIMIT 1",
            params![project_id, fingerprint],
        )
        .await?;
    let latest = rows
        .next()
        .await?
        .map(|row| Ok::<_, DbError>((row.i64(0)?, row.text(1)?)))
        .transpose()?;
    drop(rows);
    let generation = latest.as_ref().map_or(1, |(generation, _)| generation + 1);
    if let Some(observed_at) = observed_at {
        let mut rows = conn
            .query(
                "SELECT m.issue_id,m.generation FROM main_incidents m
             JOIN issues i ON i.id=m.issue_id AND i.project_id=m.project_id
             WHERE m.project_id=?1 AND m.fingerprint=?2
             AND ((i.closed_at IS NOT NULL AND ?3<=i.closed_at)
                  OR (i.merged_at IS NOT NULL AND ?3<=i.merged_at))
             ORDER BY m.generation LIMIT 1",
                params![project_id, fingerprint, observed_at],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            return Ok((Some((row.text(0)?, row.i64(1)?)), generation));
        }
    }
    if let Some((linked_generation, issue_id)) = latest {
        if issue_accepts_evidence(conn, project_id, &issue_id, observed_at).await? {
            return Ok((Some((issue_id, linked_generation)), generation));
        }
    }
    Ok((None, generation))
}

/// Test incidents are owned per test: while one is unresolved for a class and
/// test identity, every later red of that test is evidence on it, whatever its
/// signature. A changed signature is stated on the attached comment, not filed
/// as a new issue; after resolution the fingerprint alone decides recurrence.
/// Lane, infrastructure and disk-full incidents are not owned by identity here.
fn identity_owned(class: &str) -> bool {
    matches!(class, "test-failure" | "evidence-gap")
}

struct OpenIdentityIncident {
    issue_id: String,
    fingerprint: String,
    signature: String,
}

/// The oldest unresolved issue holding an incident of this class and identity,
/// with the most recent signature recorded on it.
async fn open_identity_incident_conn(
    conn: &TrackedConnection,
    project_id: &str,
    class: &str,
    identity: &str,
) -> DbResult<Option<OpenIdentityIncident>> {
    if !identity_owned(class) {
        return Ok(None);
    }
    let mut rows = conn
        .query(
            "SELECT m.issue_id, m.fingerprint, m.signature FROM main_incidents m
             JOIN issues i ON i.id=m.issue_id AND i.project_id=m.project_id
             WHERE m.project_id=?1 AND m.class=?2 AND m.identity=?3
               AND i.status NOT IN ('closed','merged')
               AND i.closed_at IS NULL AND i.merged_at IS NULL
             ORDER BY i.number, m.created_at DESC, m.generation DESC LIMIT 1",
            params![project_id, class, identity],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(OpenIdentityIncident {
                issue_id: row.text(0)?,
                fingerprint: row.text(1)?,
                signature: row.text(2)?,
            })
        })
        .transpose()
}

/// Attribution's view of the identity owner [`file_conn`] will attach to.
pub(super) async fn open_identity_issue_uri(
    db: &LocalDb,
    project_id: &str,
    class: &str,
    identity: &str,
) -> DbResult<Option<String>> {
    if !identity_owned(class) {
        return Ok(None);
    }
    let project_id = project_id.to_string();
    let class = class.to_string();
    let identity = identity.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let class = class.clone();
        let identity = identity.clone();
        Box::pin(async move {
            match open_identity_incident_conn(conn, &project_id, &class, &identity).await? {
                Some(open) => Ok(Some(
                    relations::issue_uri_for_id(conn, &open.issue_id).await?,
                )),
                None => Ok(None),
            }
        })
    })
    .await
}

fn signature_change_note(prior: &OpenIdentityIncident, draft: &IncidentDraft) -> String {
    fn excerpt(signature: &str) -> String {
        const LIMIT: usize = 2000;
        match signature.char_indices().nth(LIMIT) {
            Some((end, _)) => format!("{}\n[signature truncated]", &signature[..end]),
            None => signature.to_string(),
        }
    }
    let mut note = format!(
        "\n\nThis red is attached to the open incident for `{}` ({}) rather than filed as a new issue: an open incident owns its test identity. Fingerprint changed from `{}` to `{}`.",
        draft.identity, draft.class, prior.fingerprint, draft.fingerprint
    );
    if prior.signature == draft.signature {
        note.push_str(" The assertion signature is unchanged.");
    } else {
        note.push_str(&format!(
            "\n\nSignature changed from:\n~~~text\n{}\n~~~\nto:\n~~~text\n{}\n~~~",
            excerpt(&prior.signature),
            excerpt(&draft.signature)
        ));
    }
    note
}

/// Attribution and filing share the same historical/current-link precedence.
pub(super) async fn linked_issue_uri(
    db: &LocalDb,
    project_id: &str,
    fingerprint: &str,
    observed_at: Option<i64>,
) -> DbResult<Option<String>> {
    let project_id = project_id.to_string();
    let fingerprint = fingerprint.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let fingerprint = fingerprint.clone();
        Box::pin(async move {
            let (target, _) =
                linked_target_conn(conn, &project_id, &fingerprint, observed_at).await?;
            let Some((issue_id, _)) = target else {
                return Ok(None);
            };
            let mut rows = conn
                .query(
                    "SELECT p.key,i.number FROM issues i JOIN projects p ON p.id=i.project_id
                 WHERE i.id=?1 AND i.project_id=?2",
                    params![issue_id, project_id],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(format!(
                        "cairn://p/{}/{}",
                        row.text(0)?.to_ascii_lowercase(),
                        row.i64(1)?
                    ))
                })
                .transpose()
        })
    })
    .await
}

async fn file_conn(
    conn: &TrackedConnection,
    project_id: &str,
    fingerprint: &str,
    event_key: &str,
    may_create: bool,
    now: i64,
) -> DbResult<Option<IncidentLink>> {
    let mut rows = conn
        .query(
            "SELECT draft_json, author_principal_json, appearance_snapshot_json, issue_uri
         FROM main_incident_evidence WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3",
            params![project_id, fingerprint, event_key],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row("incident intent missing".into()))?;
    // This must precede looking at the current issue's resolution.
    if let Some(issue_uri) = row.opt_text(3)? {
        return Ok(Some(IncidentLink {
            issue_uri,
            created: false,
        }));
    }
    let draft: IncidentDraft = serde_json::from_str(&row.text(0)?)
        .map_err(|e| DbError::Row(format!("unreadable incident intent: {e}")))?;
    let authorship = crud::decode_issue_authorship(row.opt_text(1)?, row.opt_text(2)?)?
        .ok_or_else(|| DbError::Row("incident intent has no authorship".into()))?;
    drop(rows);

    let (mut target, generation) =
        linked_target_conn(conn, project_id, fingerprint, draft.observed_at).await?;
    // Decided here, not only when the draft was written: intents queued behind
    // the creation budget were drafted before any of them had filed.
    let mut identity_owner = None;
    if target.is_none() {
        identity_owner =
            open_identity_incident_conn(conn, project_id, &draft.class, &draft.identity).await?;
        if let Some(owner) = &identity_owner {
            target = Some((owner.issue_id.clone(), generation));
        }
    }
    if target.is_none() {
        if let Some(uri) = draft.existing_issue_uri.as_deref() {
            if let Some(issue) = relations::resolve_issue_uri(conn, uri).await? {
                if issue_accepts_evidence(conn, project_id, &issue.issue_id, draft.observed_at)
                    .await?
                {
                    target = Some((issue.issue_id, generation));
                }
            }
        }
    }
    let mut created = false;
    let (issue_id, target_generation) = match target {
        Some(target) => target,
        None if !may_create => {
            conn.execute(
                "UPDATE main_incident_evidence SET attempts=attempts+1,last_error='Issue creation limit reached; queued for retry'
                 WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3 AND filed_at IS NULL",
                params![project_id, fingerprint, event_key],
            ).await?;
            return Ok(None);
        }
        None => {
            // A shared primary-key row, not COUNT-then-insert, protects the rate
            // budget against concurrent hosts and survives process restarts.
            conn.execute(
                "INSERT INTO main_incident_budget(project_id,window_started,created_count)
                 VALUES (?1,?2,0) ON CONFLICT(project_id) DO NOTHING",
                params![project_id, now],
            )
            .await?;
            let mut rows = conn.query(
                "SELECT window_started,created_count FROM main_incident_budget WHERE project_id=?1",
                params![project_id],
            ).await?;
            let budget = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("incident budget missing".into()))?;
            let started = budget.i64(0)?;
            let count = if now.saturating_sub(started) >= 3600 {
                0
            } else {
                budget.i64(1)?
            };
            drop(rows);
            if count >= NEW_ISSUES_PER_PASS as i64 {
                conn.execute(
                    "UPDATE main_incident_evidence SET attempts=attempts+1,last_error='Hourly issue creation limit reached; queued for retry'
                     WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3 AND filed_at IS NULL",
                    params![project_id, fingerprint, event_key],
                ).await?;
                return Ok(None);
            }
            conn.execute(
                "UPDATE main_incident_budget SET window_started=?2,created_count=?3 WHERE project_id=?1",
                params![project_id, if count == 0 { now } else { started }, count + 1],
            ).await?;
            let id = ids::mint_child(project_id);
            let (issue, _) = crud::create_conn(
                conn,
                &id,
                CreateIssue {
                    project_id: project_id.to_string(),
                    title: draft.title.clone(),
                    description: Some(draft.body.clone()),
                    backend_override: None,
                    label_ids: None,
                },
                authorship,
                IssueCreationContext {
                    parent_uri: draft.owner_uri.clone(),
                    ..Default::default()
                },
                now,
            )
            .await?;
            created = true;
            (issue.id, generation)
        }
    };
    let change = identity_owner
        .as_ref()
        .map(|owner| signature_change_note(owner, &draft))
        .unwrap_or_default();
    if target_generation == generation {
        conn.execute(
            "INSERT INTO main_incidents
             (project_id,fingerprint,generation,identity,signature,class,issue_id,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                project_id,
                fingerprint,
                target_generation,
                draft.identity,
                draft.signature,
                draft.class,
                issue_id.as_str(),
                now
            ],
        )
        .await?;
    }
    let issue_uri = relations::issue_uri_for_id(conn, &issue_id).await?;
    let comment_id = ids::mint_child(&issue_id);
    let content = match draft.observation_id.as_deref() {
        Some(observation) => format!("{}{change}\n\nObservation: `{}`", draft.body, observation),
        None => format!("{}{change}", draft.body),
    };
    comments::insert_conn(conn, &comment_id, &issue_id, &content, "agent", now).await?;
    conn.execute(
        "UPDATE main_incident_evidence SET issue_id=?4, issue_uri=?5, generation=?6,
         comment_id=?7, filed_at=?8, last_error=NULL, attempts=attempts+1
         WHERE project_id=?1 AND fingerprint=?2 AND event_key=?3 AND filed_at IS NULL",
        params![
            project_id,
            fingerprint,
            event_key,
            issue_id,
            issue_uri.as_str(),
            target_generation,
            comment_id,
            now
        ],
    )
    .await?;
    Ok(Some(IncidentLink { issue_uri, created }))
}

async fn issue_accepts_evidence(
    conn: &TrackedConnection,
    project_id: &str,
    id: &str,
    observed_at: Option<i64>,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT status, closed_at, merged_at FROM issues WHERE id=?1 AND project_id=?2",
            params![id, project_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    let closed_at = row.opt_i64(1)?;
    let merged_at = row.opt_i64(2)?;
    let open = closed_at.is_none()
        && merged_at.is_none()
        && !matches!(row.text(0)?.as_str(), "closed" | "merged");
    // A delayed observation predating resolution adds history, not recurrence.
    // Missing time is not evidence that a new event preceded the fix.
    let historical = observed_at
        .zip(closed_at.into_iter().chain(merged_at).max())
        .is_some_and(|(observed, resolved)| observed <= resolved);
    Ok(open || historical)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (tempfile::TempDir, LocalDb) {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("incidents.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','Project','proj','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        (temp, db)
    }

    fn author() -> IssueAuthorship {
        crud::installation_machine_authorship("test-device", 1).unwrap()
    }

    fn draft(fingerprint: &str, event: &str) -> IncidentDraft {
        IncidentDraft {
            fingerprint: fingerprint.into(),
            event_key: event.into(),
            identity: format!("crate::test_{fingerprint}"),
            signature: fingerprint.into(),
            class: "test-failure".into(),
            title: "Main test failure".into(),
            body: format!("immutable snapshot for {fingerprint}/{event}"),
            owner_uri: None,
            existing_issue_uri: None,
            observation_id: Some(format!("observation-{event}")),
            observed_at: None,
        }
    }

    async fn count(db: &LocalDb, table: &str) -> i64 {
        db.query_one(format!("SELECT COUNT(*) FROM {table}"), (), |row| {
            row.i64(0)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn duplicate_event_preserves_snapshot_comment_and_backlog() {
        let (_temp, db) = fixture().await;
        let first = ingest(&db, "p", vec![draft("a", "e")], author(), 2)
            .await
            .unwrap();
        assert!(first[0].created);
        let mut replay = draft("a", "e");
        replay.body = "different redelivery body".into();
        let duplicate = ingest(&db, "p", vec![replay], author(), 3).await.unwrap();
        assert_eq!(duplicate[0].issue_uri, first[0].issue_uri);
        assert!(!duplicate[0].created);
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "comments").await, 1);
        assert_eq!(count(&db, "executions").await, 0);
        assert_eq!(count(&db, "main_incident_evidence").await, 1);
        let (status, body) = db.query_one(
            "SELECT i.status,e.body FROM issues i JOIN main_incident_evidence e ON e.issue_id=i.id",
            (), |row| Ok((row.text(0)?,row.text(1)?)),
        ).await.unwrap();
        assert_eq!(status, "backlog");
        assert_eq!(body, "immutable snapshot for a/e");
        assert!(db
            .execute("UPDATE main_incident_evidence SET body='changed'", ())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn concurrent_deliveries_create_one_issue_and_one_comment() {
        let (_temp, db) = fixture().await;
        let (a, b) = tokio::join!(
            ingest(&db, "p", vec![draft("a", "e")], author(), 2),
            ingest(&db, "p", vec![draft("a", "e")], author(), 2)
        );
        assert_eq!(a.unwrap()[0].issue_uri, b.unwrap()[0].issue_uri);
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "comments").await, 1);
        assert_eq!(count(&db, "main_incidents").await, 1);
    }

    #[tokio::test]
    async fn changed_signature_of_an_open_test_attaches_under_its_own_fingerprint() {
        let (_temp, db) = fixture().await;
        let same_test = |fingerprint: &str, event: &str| IncidentDraft {
            identity: "crate::test".into(),
            ..draft(fingerprint, event)
        };
        ingest(
            &db,
            "p",
            vec![same_test("sig-a", "e"), same_test("sig-b", "e")],
            author(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "main_incidents").await, 2);
        let note = db
            .query_one(
                "SELECT content FROM comments WHERE content LIKE '%Signature changed from%'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert!(
            note.contains("Fingerprint changed from `sig-a` to `sig-b`"),
            "{note}"
        );
        // After resolution the fingerprint alone decides: a recurrence files anew.
        db.execute("UPDATE issues SET status='closed',closed_at=3", ())
            .await
            .unwrap();
        let recurrence = ingest(&db, "p", vec![same_test("sig-b", "later")], author(), 4)
            .await
            .unwrap();
        assert!(recurrence[0].created);
        assert_eq!(count(&db, "issues").await, 2);
    }

    #[tokio::test]
    async fn intents_queued_behind_the_budget_for_one_test_file_one_incident() {
        let (_temp, db) = fixture().await;
        ingest(
            &db,
            "p",
            (0..10).map(|n| draft(&format!("other-{n}"), "e")).collect(),
            author(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "issues").await, 10);
        // Two main rows of one carried red, each drafted before either filed and
        // each with a run nonce in its signature.
        let carried = |fingerprint: &str| IncidentDraft {
            identity: "crate::carried".into(),
            ..draft(fingerprint, fingerprint)
        };
        ingest(&db, "p", vec![carried("row-1")], author(), 3)
            .await
            .unwrap();
        ingest(&db, "p", vec![carried("row-2")], author(), 4)
            .await
            .unwrap();
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 2);
        let links = drain_pending(&db, 3603).await.unwrap();
        assert_eq!(links.iter().filter(|link| link.created).count(), 1);
        assert_eq!(count(&db, "issues").await, 11);
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 0);
        let owners = db
            .query_all(
                "SELECT DISTINCT issue_uri FROM main_incident_evidence
                 WHERE fingerprint IN ('row-1','row-2')",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(owners.len(), 1, "{owners:?}");
    }

    #[tokio::test]
    async fn closed_recurrence_keeps_historical_events_on_the_old_issue() {
        let (_temp, db) = fixture().await;
        let first = ingest(&db, "p", vec![draft("a", "old")], author(), 2)
            .await
            .unwrap();
        db.execute("UPDATE issues SET status='closed',closed_at=3", ())
            .await
            .unwrap();
        let duplicate = ingest(&db, "p", vec![draft("a", "old")], author(), 4)
            .await
            .unwrap();
        assert_eq!(duplicate[0].issue_uri, first[0].issue_uri);
        assert_eq!(count(&db, "issues").await, 1);
        let next = ingest(&db, "p", vec![draft("a", "new")], author(), 5)
            .await
            .unwrap();
        assert_ne!(next[0].issue_uri, first[0].issue_uri);
        assert!(next[0].created);
        let duplicate = ingest(&db, "p", vec![draft("a", "old")], author(), 6)
            .await
            .unwrap();
        assert_eq!(duplicate[0].issue_uri, first[0].issue_uri);
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 2);
        assert_eq!(count(&db, "comments").await, 2);
        let mut delayed = draft("a", "unseen-delayed");
        delayed.observed_at = Some(2);
        let historical = ingest(&db, "p", vec![delayed], author(), 7).await.unwrap();
        assert_eq!(historical[0].issue_uri, first[0].issue_uri);
        assert!(!historical[0].created);
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 3);
    }

    #[tokio::test]
    async fn creation_limit_is_global_and_pending_survives_restart() {
        let (temp, db) = fixture().await;
        db.execute_script(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p2','w','Second','second','/tmp/second',1,1);",
        )
        .await
        .unwrap();
        ingest(
            &db,
            "p",
            (0..12).map(|n| draft(&format!("fp-{n:02}"), "e")).collect(),
            author(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "issues").await, 10);
        let pending = pending_status(&db, "p").await.unwrap();
        assert_eq!(pending.count, 2);
        assert!(pending.last_error.unwrap().contains("limit"));
        drop(db);
        let db = LocalDb::open_migrated_for_test(temp.path().join("incidents.db"))
            .await
            .unwrap();
        // A report drains only its project; unrelated issue links never leak.
        let links = ingest(
            &db,
            "p2",
            (0..12).map(|n| draft(&format!("fp-{n:02}"), "e")).collect(),
            author(),
            3,
        )
        .await
        .unwrap();
        assert_eq!(links.iter().filter(|link| link.created).count(), 10);
        assert_eq!(count(&db, "issues").await, 20);
        assert!(links
            .iter()
            .all(|link| link.issue_uri.starts_with("cairn://p/second/")));
        assert_eq!(pending_status(&db, "p2").await.unwrap().count, 2);
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 2);
        assert!(drain_pending(&db, 4).await.unwrap().is_empty());
        drain_pending(&db, 3604).await.unwrap();
        assert_eq!(count(&db, "issues").await, 24);
        assert_eq!(pending_status(&db, "p2").await.unwrap().count, 0);
    }

    #[tokio::test]
    async fn filing_failure_rolls_back_issue_allocation_but_retries_durable_intent() {
        let (temp, db) = fixture().await;
        db.execute_batch(
            "CREATE TRIGGER reject_incident_comment BEFORE INSERT ON comments
             BEGIN SELECT RAISE(ABORT,'injected comment failure'); END;",
        )
        .await
        .unwrap();
        assert!(ingest(&db, "p", vec![draft("a", "e")], author(), 2)
            .await
            .is_err());
        assert_eq!(count(&db, "issues").await, 0);
        assert_eq!(count(&db, "comments").await, 0);
        assert_eq!(count(&db, "main_incidents").await, 0);
        let pending = pending_status(&db, "p").await.unwrap();
        assert_eq!(pending.count, 1);
        assert!(pending.last_error.unwrap().contains("injected"));
        assert_eq!(
            db.query_one(
                "SELECT next_issue_number FROM projects WHERE id='p'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            1
        );
        db.execute_batch("DROP TRIGGER reject_incident_comment;")
            .await
            .unwrap();
        drop(db);
        let db = LocalDb::open_migrated_for_test(temp.path().join("incidents.db"))
            .await
            .unwrap();
        let links = drain_pending(&db, 3).await.unwrap();
        assert!(links[0].created);
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "comments").await, 1);
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 0);
    }

    #[tokio::test]
    async fn resolved_generation_reuses_open_issue_naming_fingerprint() {
        let (_temp, db) = fixture().await;
        let first = ingest(&db, "p", vec![draft("F", "old")], author(), 2)
            .await
            .unwrap();
        assert_eq!(first[0].issue_uri, "cairn://p/proj/1");
        db.execute_script(
            "UPDATE issues SET status='closed',closed_at=3;
             INSERT INTO issues(id,project_id,number,title,description,status,progress,attention,created_at,updated_at)
             VALUES('human-opened','p',2,'Recurring failure F','Fingerprint: F','backlog','backlog','none',4,4);
             UPDATE projects SET next_issue_number=3 WHERE id='p';",
        )
        .await
        .unwrap();

        let mut recurrence = draft("F", "new");
        recurrence.observed_at = Some(5);
        recurrence.existing_issue_uri = Some("cairn://p/proj/2".into());
        let links = ingest(&db, "p", vec![recurrence], author(), 5)
            .await
            .unwrap();
        assert_eq!(
            links,
            vec![IncidentLink {
                issue_uri: "cairn://p/proj/2".into(),
                created: false,
            }]
        );
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(
            db.query_all(
                "SELECT generation,issue_id FROM main_incidents WHERE fingerprint='F' ORDER BY generation",
                (),
                |row| Ok((row.i64(0)?, row.text(1)?)),
            )
            .await
            .unwrap(),
            vec![
                (1, db.query_one("SELECT id FROM issues WHERE number=1", (), |row| row.text(0)).await.unwrap()),
                (2, "human-opened".into()),
            ]
        );

        // Once linked, the new generation remains authoritative without a hint.
        let followup = ingest(&db, "p", vec![draft("F", "followup")], author(), 6)
            .await
            .unwrap();
        assert_eq!(followup, links);
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 3);
        assert_eq!(
            db.query_all(
                "SELECT event_key,issue_uri,generation FROM main_incident_evidence ORDER BY event_key",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?)),
            )
            .await
            .unwrap(),
            vec![
                ("followup".into(), "cairn://p/proj/2".into(), 2),
                ("new".into(), "cairn://p/proj/2".into(), 2),
                ("old".into(), "cairn://p/proj/1".into(), 1),
            ]
        );
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 0);
    }

    #[tokio::test]
    async fn existing_issue_is_same_project_and_fingerprint_wins() {
        let (_temp, db) = fixture().await;
        ingest(
            &db,
            "p",
            vec![draft("a", "first"), draft("b", "first")],
            author(),
            2,
        )
        .await
        .unwrap();
        let mut attachment = draft("c", "attached");
        attachment.existing_issue_uri = Some("cairn://p/proj/1".into());
        let links = ingest(&db, "p", vec![attachment], author(), 3)
            .await
            .unwrap();
        assert_eq!(links[0].issue_uri, "cairn://p/proj/1");
        assert!(!links[0].created);
        let mut same = draft("a", "second");
        same.existing_issue_uri = Some("cairn://p/proj/2".into());
        let links = ingest(&db, "p", vec![same], author(), 4).await.unwrap();
        assert_eq!(links[0].issue_uri, "cairn://p/proj/1");
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 4);
    }
}

#[cfg(test)]
mod rate_tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_host_rows_share_one_durable_creation_budget() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("rate.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','Project','proj','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        let drafts = |host: &str| {
            (0..8)
                .map(|n| IncidentDraft {
                    fingerprint: format!("{host}-{n}"),
                    identity: format!("test-{n}"),
                    signature: "sig".into(),
                    class: "failure".into(),
                    title: "Incident".into(),
                    body: "snapshot".into(),
                    owner_uri: None,
                    existing_issue_uri: None,
                    event_key: format!("{host}-event"),
                    observation_id: None,
                    observed_at: None,
                })
                .collect()
        };
        let author = || crud::installation_machine_authorship("machine", 1).unwrap();
        let (a, b) = tokio::join!(
            ingest(&db, "p", drafts("host-a"), author(), 2),
            ingest(&db, "p", drafts("host-b"), author(), 2)
        );
        a.unwrap();
        b.unwrap();
        let issues = db
            .query_one("SELECT COUNT(*) FROM issues", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(issues, 10);
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 6);
        drain_pending(&db, 3).await.unwrap();
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 6);
        drain_pending(&db, 3602).await.unwrap();
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 0);
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    #[tokio::test]
    async fn attachments_bypass_a_full_overflow_page_and_cross_project_hints_do_not_attach() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("boundaries.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','Project','proj','/tmp/repo',1,1),
                   ('other','w','Other','other','/tmp/other',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
             VALUES('external','other',1,'Existing','backlog','backlog','none',1,1);",
        ).await.unwrap();
        let author = || crud::installation_machine_authorship("machine", 1).unwrap();
        let draft = |fingerprint: String, event: &str| IncidentDraft {
            fingerprint,
            identity: "test".into(),
            signature: "sig".into(),
            class: "failure".into(),
            title: "Incident".into(),
            body: "snapshot".into(),
            owner_uri: None,
            existing_issue_uri: None,
            event_key: event.into(),
            observation_id: None,
            observed_at: None,
        };
        let mut cross_project = draft("000".into(), "first");
        cross_project.existing_issue_uri = Some("cairn://p/other/1".into());
        let first = ingest(&db, "p", vec![cross_project], author(), 2)
            .await
            .unwrap();
        assert!(first[0].created);
        assert_eq!(first[0].issue_uri, "cairn://p/proj/1");
        ingest(
            &db,
            "p",
            (1..112)
                .map(|n| draft(format!("{n:03}"), "first"))
                .collect(),
            author(),
            3,
        )
        .await
        .unwrap();
        assert!(pending_status(&db, "p").await.unwrap().count > 100);
        let links = ingest(
            &db,
            "p",
            vec![draft("000".into(), "attachment")],
            author(),
            4,
        )
        .await
        .unwrap();
        assert_eq!(
            links,
            vec![IncidentLink {
                issue_uri: "cairn://p/proj/1".into(),
                created: false
            }]
        );
        let comments = db.query_one(
            "SELECT COUNT(*) FROM comments WHERE issue_id=(SELECT id FROM issues WHERE project_id='p' AND number=1)",
            (), |row|row.i64(0),
        ).await.unwrap();
        assert_eq!(comments, 2);
        for sql in [
            "UPDATE main_incident_evidence SET observation_id='changed'",
            "UPDATE main_incident_evidence SET issue_id='external' WHERE filed_at IS NOT NULL",
            "UPDATE main_incident_evidence SET comment_id='changed' WHERE filed_at IS NOT NULL",
            "UPDATE main_incidents SET issue_id='external'",
        ] {
            assert!(db.execute(sql, ()).await.is_err(), "{sql}");
        }
    }

    #[tokio::test]
    async fn merged_issue_recurrence_does_not_move_old_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("merged.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','Project','proj','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        let draft = |event: &str| IncidentDraft {
            fingerprint: "fp".into(),
            identity: "test".into(),
            signature: "sig".into(),
            class: "failure".into(),
            title: "Incident".into(),
            body: "saved output".into(),
            owner_uri: None,
            existing_issue_uri: None,
            event_key: event.into(),
            observation_id: Some("already-retained-away".into()),
            observed_at: None,
        };
        let author = || crud::installation_machine_authorship("machine", 1).unwrap();
        ingest(&db, "p", vec![draft("first")], author(), 2)
            .await
            .unwrap();
        db.execute("UPDATE issues SET status='merged',merged_at=3", ())
            .await
            .unwrap();
        ingest(&db, "p", vec![draft("second")], author(), 4)
            .await
            .unwrap();
        let uris = db
            .query_all(
                "SELECT issue_uri,body FROM main_incident_evidence ORDER BY event_key",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(uris[0], ("cairn://p/proj/1".into(), "saved output".into()));
        assert_eq!(uris[1], ("cairn://p/proj/2".into(), "saved output".into()));
    }
}

/// A durable report source references canonical observations, never a second verdict.
#[derive(Debug, Clone)]
pub(super) struct SourceDraft {
    pub event_key: String,
    pub observation_id: Option<String>,
    pub check_name: String,
    pub platform: String,
    pub commit_sha: Option<String>,
    pub notify: Option<String>,
    pub lane_kind: Option<String>,
    pub cause_class: Option<String>,
    pub cause: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct IncidentSource {
    pub project_id: String,
    pub draft: SourceDraft,
    pub created_at: i64,
}

/// For reports with no canonical observation (for example, a launch failure).
pub(super) async fn enqueue_source(
    db: &LocalDb,
    project_id: &str,
    draft: &SourceDraft,
    now: i64,
) -> DbResult<()> {
    db.execute(
        "INSERT INTO main_incident_sources
         (project_id,event_key,observation_id,check_name,platform,commit_sha,
          notify,lane_kind,cause_class,cause,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
         ON CONFLICT(project_id,event_key) DO NOTHING",
        params![
            project_id,
            draft.event_key.as_str(),
            draft.observation_id.as_deref(),
            draft.check_name.as_str(),
            draft.platform.as_str(),
            draft.commit_sha.as_deref(),
            draft.notify.as_deref(),
            draft.lane_kind.as_deref(),
            draft.cause_class.as_deref(),
            draft.cause.as_deref(),
            now
        ],
    )
    .await?;
    Ok(())
}

pub(super) async fn pending_sources(db: &LocalDb, limit: i64) -> DbResult<Vec<IncidentSource>> {
    db.query_all(
        "SELECT project_id,event_key,observation_id,check_name,platform,commit_sha,
                notify,lane_kind,cause_class,cause,created_at
         FROM main_incident_sources WHERE processed_at IS NULL
         ORDER BY attempts,created_at,project_id,event_key LIMIT ?1",
        params![limit.clamp(1, 1000)],
        |row| {
            Ok(IncidentSource {
                project_id: row.text(0)?,
                draft: SourceDraft {
                    event_key: row.text(1)?,
                    observation_id: row.opt_text(2)?,
                    check_name: row.text(3)?,
                    platform: row.text(4)?,
                    commit_sha: row.opt_text(5)?,
                    notify: row.opt_text(6)?,
                    lane_kind: row.opt_text(7)?,
                    cause_class: row.opt_text(8)?,
                    cause: row.opt_text(9)?,
                },
                created_at: row.i64(10)?,
            })
        },
    )
    .await
}

/// A source is acknowledged only after its evidence intents are durable.
/// Failure remains pending and records a visible retry reason.
pub(super) async fn finish_source(
    db: &LocalDb,
    project_id: &str,
    event_key: &str,
    error: Option<String>,
    now: i64,
) -> DbResult<()> {
    db.execute(
        "UPDATE main_incident_sources SET processed_at=?3,error=?4,attempts=attempts+1
         WHERE project_id=?1 AND event_key=?2 AND processed_at IS NULL",
        params![
            project_id,
            event_key,
            if error.is_none() { Some(now) } else { None },
            error
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[tokio::test]
    async fn stale_owner_does_not_starve_later_valid_intents() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("retry.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','Project','proj','/tmp/repo',1,1);",
        )
        .await
        .unwrap();
        let draft = |fp: &str, owner_uri| IncidentDraft {
            fingerprint: fp.into(),
            identity: "test".into(),
            signature: "sig".into(),
            class: "failure".into(),
            title: "Incident".into(),
            body: "saved snapshot".into(),
            owner_uri,
            existing_issue_uri: None,
            event_key: "event".into(),
            observation_id: None,
            observed_at: None,
        };
        let author = crud::installation_machine_authorship("machine", 1).unwrap();
        let result = ingest(
            &db,
            "p",
            vec![
                draft("a-stale", Some("cairn://p/proj/9999".into())),
                draft("b-valid", None),
            ],
            author,
            2,
        )
        .await;
        assert!(result.is_err());
        let issues = db
            .query_one("SELECT COUNT(*) FROM issues", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(issues, 1, "a stale owner must not starve later valid work");
        assert_eq!(pending_status(&db, "p").await.unwrap().count, 1);
        assert!(pending_status(&db, "p")
            .await
            .unwrap()
            .last_error
            .unwrap()
            .contains("not found"));
        let filed = db.query_one(
            "SELECT COUNT(*) FROM main_incident_evidence WHERE fingerprint='b-valid' AND filed_at IS NOT NULL",
            (),|row|row.i64(0),
        ).await.unwrap();
        assert_eq!(filed, 1);

        // Hinted but invalid work must not monopolize a full retry page.
        let bad = (0..100)
            .map(|n| {
                let mut bad = draft(&format!("bad-{n:03}"), Some("cairn://p/proj/9999".into()));
                bad.existing_issue_uri = Some("cairn://p/proj/9999".into());
                bad
            })
            .collect();
        let author = || crud::installation_machine_authorship("machine", 1).unwrap();
        assert!(ingest(&db, "p", bad, author(), 3).await.is_err());
        assert!(
            ingest(&db, "p", vec![draft("z-unblocked", None)], author(), 4)
                .await
                .is_err()
        );
        let filed = db.query_one(
            "SELECT COUNT(*) FROM main_incident_evidence WHERE fingerprint='z-unblocked' AND filed_at IS NOT NULL",
            (),|row|row.i64(0),
        ).await.unwrap();
        assert_eq!(filed, 1, "attempt fairness outranks invalid hinted work");
    }
}

/// The bound names an operational failure, not permission to discard its source.
const SOURCE_STUCK_ATTEMPTS: i64 = 3;
const SOURCE_STUCK_AGE_S: i64 = 3600;

#[derive(Debug)]
pub(crate) struct SourceHealth {
    pub pending: i64,
    pub held_observations: i64,
    pub stuck: i64,
    examples: Vec<String>,
}

impl std::fmt::Display for SourceHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "main incident preparation: {} pending source events; retention holds {} canonical observations until durable filing acknowledgement",
            self.pending,self.held_observations)?;
        if self.stuck > 0 {
            write!(
                f,
                "; FAIL-TO-FILE: {} sources at {} attempts or {}s age",
                self.stuck, SOURCE_STUCK_ATTEMPTS, SOURCE_STUCK_AGE_S
            )?;
            for example in &self.examples {
                write!(f, "\n- {example}")?;
            }
            if self.stuck > self.examples.len() as i64 {
                write!(
                    f,
                    "\n- {} more stuck sources retained in main_incident_sources",
                    self.stuck - self.examples.len() as i64
                )?;
            }
        }
        Ok(())
    }
}

pub(crate) async fn source_health(
    db: &LocalDb,
    project: Option<&str>,
    now: i64,
) -> DbResult<SourceHealth> {
    let (pending, held_observations, stuck) = db
        .query_one(
            "SELECT COUNT(*),COUNT(DISTINCT observation_id),
                COALESCE(SUM(CASE WHEN attempts>=?3 OR created_at<=?2 THEN 1 ELSE 0 END),0)
         FROM main_incident_sources WHERE processed_at IS NULL AND (?1 IS NULL OR project_id=?1)",
            params![project, now - SOURCE_STUCK_AGE_S, SOURCE_STUCK_ATTEMPTS],
            |r| Ok((r.i64(0)?, r.i64(1)?, r.i64(2)?)),
        )
        .await?;
    let examples =
        if stuck > 0 {
            db.query_all(
            "SELECT event_key,observation_id,attempts,created_at,error FROM main_incident_sources
             WHERE processed_at IS NULL AND (?1 IS NULL OR project_id=?1)
               AND (attempts>=?3 OR created_at<=?2)
             ORDER BY created_at,project_id,event_key LIMIT 10",
            params![project,now-SOURCE_STUCK_AGE_S,SOURCE_STUCK_ATTEMPTS],
            move |r| {
                let error=r.opt_text(4)?.unwrap_or_else(||"pending delivery".into());
                let error: String=error.chars().take(200).collect();
                Ok(format!("source `{}`; held observation `{}`; attempts {}; age {}s; reason: {}",
                    r.text(0)?,r.opt_text(1)?.unwrap_or_else(||"none (row-less attempt)".into()),
                    r.i64(2)?,now.saturating_sub(r.i64(3)?),error))
            },
        ).await?
        } else {
            Vec::new()
        };
    Ok(SourceHealth {
        pending,
        held_observations,
        stuck,
        examples,
    })
}

#[cfg(test)]
mod incident_health_tests {
    use super::*;

    #[tokio::test]
    async fn stuck_incident_delivery_reports_its_retention_hold_until_acknowledged() {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("hold.db"))
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','Project','proj','/tmp/repo',1,1);
             INSERT INTO check_result_observations
               (id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,
                exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,cadence,output_tail)
               VALUES('held-red','p','commit','tree','rust-tests','input','env',1,'failed',0,0,1,1,1000,1,'main','raw failure');"
        ).await.unwrap();
        for _ in 0..SOURCE_STUCK_ATTEMPTS {
            finish_source(
                &db,
                "p",
                "observation:held-red",
                Some("injected repeated filing failure".into()),
                2,
            )
            .await
            .unwrap();
        }
        db.execute(
            "DELETE FROM check_result_observations WHERE id='held-red'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM check_result_observations", (), |r| r
                .i64(0))
                .await
                .unwrap(),
            1
        );
        let health = source_health(&db, Some("p"), 3).await.unwrap();
        assert_eq!(
            (health.pending, health.held_observations, health.stuck),
            (1, 1, 1)
        );
        let text = health.to_string();
        for expected in [
            "FAIL-TO-FILE",
            "observation:held-red",
            "held-red",
            "attempts 3",
            "injected repeated filing failure",
        ] {
            assert!(text.contains(expected), "missing {expected}: {text}");
        }
        finish_source(&db, "p", "observation:held-red", None, 4)
            .await
            .unwrap();
        db.execute(
            "DELETE FROM check_result_observations WHERE id='held-red'",
            (),
        )
        .await
        .unwrap();
        let health = source_health(&db, None, 5).await.unwrap();
        assert_eq!(
            (health.pending, health.held_observations, health.stuck),
            (0, 0, 0)
        );
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM check_result_observations", (), |r| r
                .i64(0))
                .await
                .unwrap(),
            0
        );
    }
}
