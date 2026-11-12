//! Camp storage (cairn/6255, cut down by cairn/7002).
//!
//! Every function takes a connection, so it composes into the transaction of
//! whatever is being born or changed with it: base camp is established inside
//! the project's bootstrap, and a camp is created together with its members.
//!
//! Base camp stores no members: its members are its project's open threads.
//! A camp's members are camp-owned `wake_subscriptions` rows, one per subject,
//! and the unique (camp, subject) index is what absorbs two concurrent adds of
//! the same member.

use super::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::models::{
    validate_camp_slug, Camp, CampKind, CampMember, CampStatus, CampSubject, BASE_CAMP_SLUG,
};
use cairn_common::identity::{PrincipalPosition, PrincipalRef};
use turso::params;

/// `wake_subscriptions.source_kind` of a camp member row.
pub const CAMP_SOURCE_KIND: &str = "camp";

/// Base camp's title until someone edits it.
pub const BASE_CAMP_TITLE: &str = "Base camp";

const CAMP_COLUMNS: &str =
    "id, project_id, slug, title, kind, status, archived_at, created_at, updated_at";

fn map_camp(row: &turso::Row) -> DbResult<Camp> {
    let kind = row.text(4)?;
    let status = row.text(5)?;
    Ok(Camp {
        id: row.text(0)?,
        project_id: row.text(1)?,
        slug: row.text(2)?,
        title: row.text(3)?,
        kind: CampKind::parse(&kind)
            .ok_or_else(|| DbError::Row(format!("unknown camp kind: {kind}")))?,
        status: CampStatus::parse(&status)
            .ok_or_else(|| DbError::Row(format!("unknown camp status: {status}")))?,
        archived_at: row.opt_i64(6)?,
        created_at: row.i64(7)?,
        updated_at: row.i64(8)?,
    })
}

/// A camp being created.
#[derive(Debug, Clone)]
pub struct NewCamp {
    pub project_id: String,
    pub slug: String,
    pub title: String,
}

/// What adding a member did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberAdded {
    Added,
    AlreadyMember,
}

pub async fn camp_by_id_conn(conn: &TrackedConnection, camp_id: &str) -> DbResult<Option<Camp>> {
    let mut rows = conn
        .query(
            format!("SELECT {CAMP_COLUMNS} FROM camps WHERE id = ?1"),
            params![camp_id],
        )
        .await?;
    rows.next().await?.map(|row| map_camp(&row)).transpose()
}

pub async fn camp_by_slug_conn(
    conn: &TrackedConnection,
    project_id: &str,
    slug: &str,
) -> DbResult<Option<Camp>> {
    let mut rows = conn
        .query(
            format!("SELECT {CAMP_COLUMNS} FROM camps WHERE project_id = ?1 AND slug = ?2"),
            params![project_id, slug],
        )
        .await?;
    rows.next().await?.map(|row| map_camp(&row)).transpose()
}

pub async fn base_camp_conn(conn: &TrackedConnection, project_id: &str) -> DbResult<Option<Camp>> {
    camp_by_slug_conn(conn, project_id, BASE_CAMP_SLUG).await
}

/// A project's camps: base first, then active camps by title, then archived
/// ones by title.
pub async fn list_camps_conn(conn: &TrackedConnection, project_id: &str) -> DbResult<Vec<Camp>> {
    let mut rows = conn
        .query(
            format!(
                "SELECT {CAMP_COLUMNS} FROM camps WHERE project_id = ?1
                 ORDER BY kind = 'base' DESC, status = 'archived' ASC, title ASC, slug ASC"
            ),
            params![project_id],
        )
        .await?;
    let mut camps = Vec::new();
    while let Some(row) = rows.next().await? {
        camps.push(map_camp(&row)?);
    }
    Ok(camps)
}

/// A thread's project and the durable-subject principal that names it now.
pub async fn thread_principal_conn(
    conn: &TrackedConnection,
    thread_id: &str,
) -> DbResult<Option<(String, PrincipalRef)>> {
    let mut rows = conn
        .query(
            "SELECT t.project_id, p.key, t.name FROM threads t
             JOIN projects p ON p.id = t.project_id WHERE t.id = ?1",
            params![thread_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let principal = PrincipalRef::Agent {
        node: cairn_uri::build_thread_uri(&row.text(1)?, &row.text(2)?),
        run_id: None,
    };
    principal
        .validate_at(PrincipalPosition::DurableSubject)
        .map_err(|error| DbError::internal(format!("thread is not a valid principal: {error}")))?;
    Ok(Some((row.text(0)?, principal)))
}

/// Check that a thread subject is an open thread of the camp's project.
async fn check_member_subject_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
) -> DbResult<()> {
    let Some(thread_id) = subject.thread_id() else {
        return Ok(());
    };
    let mut rows = conn
        .query(
            "SELECT project_id, status FROM threads WHERE id = ?1",
            params![thread_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::internal(format!("thread not found: {thread_id}")));
    };
    let (project_id, status) = (row.text(0)?, row.text(1)?);
    if project_id != camp.project_id {
        return Err(DbError::internal(format!(
            "thread {thread_id} belongs to another project; a camp's members are its own project's threads and the operator"
        )));
    }
    if status == "closed" {
        return Err(DbError::internal(format!(
            "thread {thread_id} is closed; a closed thread is not a member anywhere"
        )));
    }
    Ok(())
}

async fn insert_camp_row_conn(
    conn: &TrackedConnection,
    input: &NewCamp,
    kind: CampKind,
    now: i64,
) -> DbResult<Camp> {
    if input.title.trim().is_empty() {
        return Err(DbError::internal("a camp needs a non-empty title"));
    }
    let id = cairn_common::ids::mint_child(&input.project_id);
    conn.execute(
        "INSERT INTO camps (id, project_id, slug, title, kind, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?6)",
        params![
            id.as_str(),
            input.project_id.as_str(),
            input.slug.as_str(),
            input.title.trim(),
            kind.as_str(),
            now
        ],
    )
    .await
    .map_err(|error| {
        if error.to_string().to_ascii_lowercase().contains("unique") {
            DbError::internal(format!(
                "a camp named `{}` already exists in this project",
                input.slug
            ))
        } else {
            error.into()
        }
    })?;
    camp_by_id_conn(conn, &id)
        .await?
        .ok_or_else(|| DbError::internal("created camp was not found"))
}

/// Create a camp with its first members. `creator` is always a member, and
/// `members` may repeat it or each other.
pub async fn create_camp_conn(
    conn: &TrackedConnection,
    input: &NewCamp,
    creator: &CampSubject,
    members: &[CampSubject],
    now: i64,
) -> DbResult<Camp> {
    validate_camp_slug(&input.slug).map_err(DbError::internal)?;
    let camp = insert_camp_row_conn(conn, input, CampKind::Camp, now).await?;
    for subject in std::iter::once(creator).chain(members) {
        add_camp_member_conn(conn, &camp, subject, creator, now).await?;
    }
    Ok(camp)
}

/// Establish a project's base camp if it has none. Returns the camp and
/// whether this call created it. Base camp seats nobody: its members are the
/// project's open threads.
pub async fn ensure_base_camp_conn(
    conn: &TrackedConnection,
    project_id: &str,
    now: i64,
) -> DbResult<(Camp, bool)> {
    if let Some(camp) = base_camp_conn(conn, project_id).await? {
        return Ok((camp, false));
    }
    let input = NewCamp {
        project_id: project_id.to_string(),
        slug: BASE_CAMP_SLUG.to_string(),
        title: BASE_CAMP_TITLE.to_string(),
    };
    Ok((
        insert_camp_row_conn(conn, &input, CampKind::Base, now).await?,
        true,
    ))
}

/// Add a member to a camp. Authorization is the caller's: this enforces only
/// what the data requires (an active camp that is not base camp, and an open
/// thread of the camp's project or the operator).
pub async fn add_camp_member_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
    added_by: &CampSubject,
    now: i64,
) -> DbResult<MemberAdded> {
    if camp.is_base() {
        return Err(DbError::internal(
            "base camp's members are every open thread of the project; they are not added",
        ));
    }
    if camp.is_archived() {
        return Err(DbError::internal(format!(
            "camp `{}` is archived; it takes no new members",
            camp.slug
        )));
    }
    check_member_subject_conn(conn, camp, subject).await?;
    let subject_key = subject.key();
    let subject_json =
        serde_json::to_string(subject).map_err(|error| DbError::internal(error.to_string()))?;
    let created_by = match added_by {
        CampSubject::Thread { .. } => "agent",
        CampSubject::Operator => "user",
    };
    let inserted = conn
        .execute(
            "INSERT INTO wake_subscriptions
               (id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by,
                created_at, updated_at, one_shot, camp_id, subject_key, subject_json)
             VALUES (?1, NULL, ?2, NULL, NULL, 'active', ?3, ?4, ?4, 0, ?5, ?6, ?7)
             ON CONFLICT (camp_id, subject_key) DO NOTHING",
            params![
                cairn_common::ids::mint_child(&camp.project_id),
                CAMP_SOURCE_KIND,
                created_by,
                now,
                camp.id.as_str(),
                subject_key.as_str(),
                subject_json.as_str()
            ],
        )
        .await?;
    if inserted == 0 {
        return Ok(MemberAdded::AlreadyMember);
    }
    conn.execute(
        "UPDATE camps SET updated_at = ?2 WHERE id = ?1",
        params![camp.id.as_str(), now],
    )
    .await?;
    Ok(MemberAdded::Added)
}

/// Leave a camp. Base camp cannot be left: every open thread is in it.
pub async fn leave_camp_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
) -> DbResult<bool> {
    if camp.is_base() {
        return Err(DbError::internal(
            "base camp cannot be left; every open thread of the project is in it",
        ));
    }
    let removed = conn
        .execute(
            "DELETE FROM wake_subscriptions WHERE camp_id = ?1 AND subject_key = ?2",
            params![camp.id.as_str(), subject.key()],
        )
        .await?;
    Ok(removed > 0)
}

/// Change a camp's title. Base camp's title is fixed, and an archived camp is
/// read-only until it is unarchived.
pub async fn retitle_camp_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    title: &str,
    now: i64,
) -> DbResult<bool> {
    let title = title.trim();
    if title.is_empty() {
        return Err(DbError::internal("a camp needs a non-empty title"));
    }
    if camp.is_base() {
        return Err(DbError::internal("base camp cannot be renamed"));
    }
    if camp.is_archived() {
        return Err(DbError::internal(format!(
            "camp `{}` is archived; unarchive it to rename it",
            camp.slug
        )));
    }
    if camp.title == title {
        return Ok(false);
    }
    conn.execute(
        "UPDATE camps SET title = ?2, updated_at = ?3 WHERE id = ?1",
        params![camp.id.as_str(), title, now],
    )
    .await?;
    Ok(true)
}

/// Archive or unarchive a camp. Returns whether the status changed. Base camp
/// is never archived.
pub async fn set_camp_status_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    status: CampStatus,
    now: i64,
) -> DbResult<bool> {
    if camp.is_base() {
        return Err(DbError::internal("base camp cannot be archived"));
    }
    if camp.status == status {
        return Ok(false);
    }
    let archived_at = (status == CampStatus::Archived).then_some(now);
    conn.execute(
        "UPDATE camps SET status = ?2, archived_at = ?3, updated_at = ?4 WHERE id = ?1",
        params![camp.id.as_str(), status.as_str(), archived_at, now],
    )
    .await?;
    Ok(true)
}

/// Every member of a camp, longest standing first.
///
/// Base camp's members are its project's open threads, since each was born.
/// A camp's are its member rows, minus closed threads: a closed thread's row
/// stays, dormant, so a reopened thread is a member again, but every census,
/// count and recipient list reads through here and never sees it while it is
/// closed.
pub async fn camp_members_conn(conn: &TrackedConnection, camp: &Camp) -> DbResult<Vec<CampMember>> {
    let mut members = Vec::new();
    if camp.is_base() {
        let mut rows = conn
            .query(
                "SELECT id, created_at FROM threads
                 WHERE project_id = ?1 AND status != 'closed'
                 ORDER BY created_at, id",
                params![camp.project_id.as_str()],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            let key = row.text(0)?;
            members.push(CampMember {
                subject: CampSubject::from_key(&key),
                subject_key: key,
                since: row.i64(1)?,
            });
        }
        return Ok(members);
    }
    let mut rows = conn
        .query(
            "SELECT ws.subject_key, ws.created_at
             FROM wake_subscriptions ws
             WHERE ws.camp_id = ?1
               AND NOT EXISTS (
                   SELECT 1 FROM threads t
                   WHERE t.id = ws.subject_key AND t.status = 'closed'
               )
             ORDER BY ws.created_at, ws.subject_key",
            params![camp.id.as_str()],
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let key = row.text(0)?;
        members.push(CampMember {
            subject: CampSubject::from_key(&key),
            subject_key: key,
            since: row.i64(1)?,
        });
    }
    Ok(members)
}

/// Whether `subject` is a member of `camp` now.
pub async fn is_camp_member_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
) -> DbResult<bool> {
    if camp.is_base() {
        let Some(thread_id) = subject.thread_id() else {
            return Ok(false);
        };
        let mut rows = conn
            .query(
                "SELECT 1 FROM threads WHERE id = ?1 AND project_id = ?2 AND status != 'closed'",
                params![thread_id, camp.project_id.as_str()],
            )
            .await?;
        return Ok(rows.next().await?.is_some());
    }
    let mut rows = conn
        .query(
            "SELECT 1 FROM wake_subscriptions ws
             WHERE ws.camp_id = ?1 AND ws.subject_key = ?2
               AND NOT EXISTS (
                   SELECT 1 FROM threads t
                   WHERE t.id = ws.subject_key AND t.status = 'closed'
               )",
            params![camp.id.as_str(), subject.key()],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// The active camps (never base camp, which every open thread is in) whose
/// members include `subject_key`, by title.
pub async fn camps_for_member_conn(
    conn: &TrackedConnection,
    subject_key: &str,
) -> DbResult<Vec<Camp>> {
    let columns = CAMP_COLUMNS
        .split(", ")
        .map(|column| format!("c.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut rows = conn
        .query(
            format!(
                "SELECT {columns} FROM camps c
                 JOIN wake_subscriptions ws ON ws.camp_id = c.id
                 WHERE ws.subject_key = ?1 AND c.kind = 'camp' AND c.status = 'active'
                 ORDER BY c.title, c.slug"
            ),
            params![subject_key],
        )
        .await?;
    let mut camps = Vec::new();
    while let Some(row) = rows.next().await? {
        camps.push(map_camp(&row)?);
    }
    Ok(camps)
}

impl LocalDb {
    pub async fn list_camps(&self, project_id: &str) -> DbResult<Vec<Camp>> {
        let project_id = project_id.to_string();
        self.read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move { list_camps_conn(conn, &project_id).await })
        })
        .await
    }

    pub async fn camp_by_slug(&self, project_id: &str, slug: &str) -> DbResult<Option<Camp>> {
        let (project_id, slug) = (project_id.to_string(), slug.to_string());
        self.read(move |conn| {
            let (project_id, slug) = (project_id.clone(), slug.clone());
            Box::pin(async move { camp_by_slug_conn(conn, &project_id, &slug).await })
        })
        .await
    }

    pub async fn camp_members(&self, camp: &Camp) -> DbResult<Vec<CampMember>> {
        let camp = camp.clone();
        self.read(move |conn| {
            let camp = camp.clone();
            Box::pin(async move { camp_members_conn(conn, &camp).await })
        })
        .await
    }

    pub async fn is_camp_member(&self, camp: &Camp, subject: &CampSubject) -> DbResult<bool> {
        let (camp, subject) = (camp.clone(), subject.clone());
        self.read(move |conn| {
            let (camp, subject) = (camp.clone(), subject.clone());
            Box::pin(async move { is_camp_member_conn(conn, &camp, &subject).await })
        })
        .await
    }

    pub async fn camps_for_member(&self, subject_key: &str) -> DbResult<Vec<Camp>> {
        let subject_key = subject_key.to_string();
        self.read(move |conn| {
            let subject_key = subject_key.clone();
            Box::pin(async move { camps_for_member_conn(conn, &subject_key).await })
        })
        .await
    }

    pub async fn ensure_base_camp(&self, project_id: &str, now: i64) -> DbResult<(Camp, bool)> {
        let project_id = project_id.to_string();
        self.write(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move { ensure_base_camp_conn(conn, &project_id, now).await })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    async fn project_with_threads(db: &LocalDb, threads: &[(&str, &str)]) {
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, repository_id,
                                   created_at, updated_at, is_workspace)
             VALUES ('p', 'default', 'Proj', 'proj', '/tmp/proj', 'p', 1, 1, 0),
                    ('q', 'default', 'Other', 'other', '/tmp/other', 'q', 1, 1, 0);",
        )
        .await
        .unwrap();
        for (index, (id, name)) in threads.iter().enumerate() {
            let project = if id.starts_with('q') { "q" } else { "p" };
            db.execute(
                "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                (
                    id.to_string(),
                    project,
                    name.to_string(),
                    if name.starts_with("closed") {
                        "closed"
                    } else {
                        "active"
                    },
                    index as i64 + 1,
                ),
            )
            .await
            .unwrap();
        }
    }

    fn thread(id: &str) -> CampSubject {
        CampSubject::Thread {
            thread_id: id.to_string(),
        }
    }

    async fn create(
        db: &LocalDb,
        slug: &'static str,
        creator: CampSubject,
        members: Vec<CampSubject>,
    ) -> DbResult<Camp> {
        db.write(move |conn| {
            let (creator, members) = (creator.clone(), members.clone());
            Box::pin(async move {
                create_camp_conn(
                    conn,
                    &NewCamp {
                        project_id: "p".into(),
                        slug: slug.into(),
                        title: format!("Camp {slug}"),
                    },
                    &creator,
                    &members,
                    5,
                )
                .await
            })
        })
        .await
    }

    #[tokio::test]
    async fn base_camp_is_every_open_thread_and_stores_no_members() {
        let db = migrated_test_db("camps-base").await;
        project_with_threads(
            &db,
            &[
                ("t-general", "general"),
                ("t-devops", "devops"),
                ("t-old", "closed-old"),
                ("q-x", "x"),
            ],
        )
        .await;

        let (camp, created) = db.ensure_base_camp("p", 10).await.unwrap();
        assert!(created);
        assert!(camp.is_base());
        assert_eq!(camp.status, CampStatus::Active);
        let (again, created) = db.ensure_base_camp("p", 11).await.unwrap();
        assert!(!created);
        assert_eq!(again.id, camp.id);

        let members = db.camp_members(&camp).await.unwrap();
        let keys: Vec<_> = members.iter().map(|m| m.subject_key.as_str()).collect();
        assert_eq!(
            keys,
            ["t-general", "t-devops"],
            "a closed thread is not a member; other projects' threads are not either"
        );
        assert!(db.is_camp_member(&camp, &thread("t-devops")).await.unwrap());
        assert!(!db.is_camp_member(&camp, &thread("t-old")).await.unwrap());
        let rows = db
            .query_opt_i64("SELECT COUNT(*) FROM wake_subscriptions", ())
            .await
            .unwrap();
        assert_eq!(rows, Some(0));

        for refused in [
            db.write({
                let camp = camp.clone();
                move |conn| {
                    let camp = camp.clone();
                    Box::pin(async move {
                        leave_camp_conn(conn, &camp, &thread("t-devops"))
                            .await
                            .map(|_| ())
                    })
                }
            })
            .await,
            db.write({
                let camp = camp.clone();
                move |conn| {
                    let camp = camp.clone();
                    Box::pin(async move {
                        retitle_camp_conn(conn, &camp, "Elsewhere", 12)
                            .await
                            .map(|_| ())
                    })
                }
            })
            .await,
            db.write({
                let camp = camp.clone();
                move |conn| {
                    let camp = camp.clone();
                    Box::pin(async move {
                        set_camp_status_conn(conn, &camp, CampStatus::Archived, 12)
                            .await
                            .map(|_| ())
                    })
                }
            })
            .await,
            db.write({
                let camp = camp.clone();
                move |conn| {
                    let camp = camp.clone();
                    Box::pin(async move {
                        add_camp_member_conn(
                            conn,
                            &camp,
                            &CampSubject::Operator,
                            &CampSubject::Operator,
                            12,
                        )
                        .await
                        .map(|_| ())
                    })
                }
            })
            .await,
        ] {
            assert!(refused.is_err());
        }
    }

    #[tokio::test]
    async fn a_camp_is_a_flat_member_set() {
        let db = migrated_test_db("camps-members").await;
        project_with_threads(
            &db,
            &[
                ("t-a", "alpha"),
                ("t-b", "beta"),
                ("t-c", "closed-gamma"),
                ("q-x", "x"),
            ],
        )
        .await;
        let camp = create(
            &db,
            "rebuild",
            thread("t-a"),
            vec![thread("t-b"), CampSubject::Operator, thread("t-a")],
        )
        .await
        .unwrap();
        assert_eq!(camp.kind, CampKind::Camp);
        let keys: Vec<_> = db
            .camp_members(&camp)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.subject_key)
            .collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"operator".to_string()));

        assert!(create(&db, "rebuild", thread("t-b"), vec![])
            .await
            .unwrap_err()
            .to_string()
            .contains("already exists"));
        assert!(create(&db, "base", thread("t-b"), vec![]).await.is_err());
        assert!(
            create(&db, "elsewhere", thread("t-b"), vec![thread("q-x")])
                .await
                .is_err(),
            "another project's thread is not a member"
        );
        assert!(
            create(&db, "closed", thread("t-b"), vec![thread("t-c")])
                .await
                .is_err(),
            "a closed thread is not added"
        );

        let c = camp.clone();
        let again = db
            .write(move |conn| {
                let c = c.clone();
                Box::pin(async move {
                    add_camp_member_conn(conn, &c, &thread("t-b"), &thread("t-a"), 6).await
                })
            })
            .await
            .unwrap();
        assert_eq!(again, MemberAdded::AlreadyMember);

        let c = camp.clone();
        let left = db
            .write(move |conn| {
                let c = c.clone();
                Box::pin(async move { leave_camp_conn(conn, &c, &thread("t-a")).await })
            })
            .await
            .unwrap();
        assert!(left, "any member may leave, the creator included");
        assert_eq!(db.camps_for_member("t-b").await.unwrap().len(), 1);
        assert!(db.camps_for_member("t-a").await.unwrap().is_empty());

        let c = camp.clone();
        db.write(move |conn| {
            let c = c.clone();
            Box::pin(async move { set_camp_status_conn(conn, &c, CampStatus::Archived, 7).await })
        })
        .await
        .unwrap();
        let archived = db.camp_by_slug("p", "rebuild").await.unwrap().unwrap();
        assert!(archived.is_archived());
        assert_eq!(archived.archived_at, Some(7));
        assert!(
            db.camps_for_member("t-b").await.unwrap().is_empty(),
            "an archived camp is not one a thread is in"
        );
        let a = archived.clone();
        let refused = db
            .write(move |conn| {
                let a = a.clone();
                Box::pin(async move {
                    add_camp_member_conn(conn, &a, &thread("t-a"), &thread("t-b"), 8).await
                })
            })
            .await;
        assert!(refused.is_err(), "an archived camp takes no members");
        let a = archived.clone();
        db.write(move |conn| {
            let a = a.clone();
            Box::pin(async move { set_camp_status_conn(conn, &a, CampStatus::Active, 9).await })
        })
        .await
        .unwrap();
        let back = db.camp_by_slug("p", "rebuild").await.unwrap().unwrap();
        assert_eq!(back.archived_at, None);
    }

    #[tokio::test]
    async fn job_subscriptions_and_camp_members_cannot_mix() {
        let db = migrated_test_db("camps-owner-check").await;
        project_with_threads(&db, &[("t-general", "general")]).await;
        let camp = create(&db, "x", thread("t-general"), vec![]).await.unwrap();
        let no_subject = db
            .execute(
                "INSERT INTO wake_subscriptions (id, job_id, source_kind, created_at, updated_at,
                    camp_id, subject_key)
                 VALUES ('x', NULL, 'camp', 1, 1, ?1, 'k')",
                (camp.id.clone(),),
            )
            .await;
        assert!(no_subject.is_err());
        let no_owner = db
            .execute(
                "INSERT INTO wake_subscriptions (id, job_id, source_kind, created_at, updated_at)
                 VALUES ('y', NULL, 'fact', 1, 1)",
                (),
            )
            .await;
        assert!(no_owner.is_err());
    }
}
