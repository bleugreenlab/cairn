//! Camp storage (cairn/6255).
//!
//! Every function that changes involvement takes a connection, so it composes
//! into the transaction of whatever is being born or changed with it: a thread
//! joins base camp inside its own creation transaction, and base camp is
//! established inside the project's bootstrap.
//!
//! Role transitions serialize on the camp row's `policy_epoch`. Each one
//! updates that row, so two concurrent writes that would leave contradictory
//! roles conflict on it and one retries, rather than both committing against a
//! stale read of the other's store.

use super::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::models::{
    validate_camp_slug, Camp, CampKind, CampLevel, CampParticipant, CampRole, CampStatus,
    CampSubject, BASE_CAMP_SLUG,
};
use cairn_common::identity::{AppearanceSnapshot, PrincipalPosition, PrincipalRef};
use std::collections::BTreeMap;
use turso::params;

/// `wake_subscriptions.source_kind` of a camp-subject row.
pub const CAMP_SOURCE_KIND: &str = "camp";

/// The reason every base-camp membership created by birth or backfill records.
pub const BASE_MEMBERSHIP_REASON: &str = "every thread of the project is a base camp participant";

/// Base camp's title until someone edits it.
pub const BASE_CAMP_TITLE: &str = "Base camp";

const CAMP_COLUMNS: &str = "id, project_id, slug, title, kind, lead_thread_id, status, \
     policy_epoch, struck_at, struck_reason, created_at, updated_at, lead_offer_thread_id";

fn map_camp(row: &turso::Row) -> DbResult<Camp> {
    let kind = row.text(4)?;
    let status = row.text(6)?;
    Ok(Camp {
        id: row.text(0)?,
        project_id: row.text(1)?,
        slug: row.text(2)?,
        title: row.text(3)?,
        kind: CampKind::parse(&kind)
            .ok_or_else(|| DbError::Row(format!("unknown camp kind: {kind}")))?,
        lead_thread_id: row.text(5)?,
        lead_offer_thread_id: row.opt_text(12)?,
        status: CampStatus::parse(&status)
            .ok_or_else(|| DbError::Row(format!("unknown camp status: {status}")))?,
        policy_epoch: row.i64(7)?,
        struck_at: row.opt_i64(8)?,
        struck_reason: row.opt_text(9)?,
        created_at: row.i64(10)?,
        updated_at: row.i64(11)?,
    })
}

/// Who made a change, recorded beside the journal row and the assignment.
///
/// Absent means the system acted (birth, backfill). No identity is fabricated
/// for it: the actor columns stay null together.
#[derive(Debug, Clone)]
pub struct CampActor {
    pub principal: PrincipalRef,
    pub appearance: AppearanceSnapshot,
}

impl CampActor {
    fn columns(actor: Option<&Self>) -> DbResult<(Option<String>, Option<String>)> {
        let Some(actor) = actor else {
            return Ok((None, None));
        };
        actor
            .principal
            .validate_at(PrincipalPosition::DecisionActor)
            .map_err(|error| DbError::internal(format!("invalid camp actor: {error}")))?;
        let principal = serde_json::to_string(&actor.principal)
            .map_err(|error| DbError::internal(error.to_string()))?;
        let appearance = serde_json::to_string(&actor.appearance)
            .map_err(|error| DbError::internal(error.to_string()))?;
        Ok((Some(principal), Some(appearance)))
    }
}

/// Who created a subscription row, in `wake_subscriptions.created_by`'s terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CampAssignedBy {
    Agent,
    User,
    System,
}

impl CampAssignedBy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::User => "user",
            Self::System => "system",
        }
    }
}

/// A camp being pitched.
#[derive(Debug, Clone)]
pub struct NewCamp {
    pub project_id: String,
    pub slug: String,
    pub title: String,
    pub lead_thread_id: String,
}

/// What a level write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CampLevelChange {
    Joined(CampLevel),
    Changed {
        from: CampLevel,
        to: CampLevel,
    },
    /// Same level and reason: nothing was written and the epoch did not move.
    Unchanged(CampLevel),
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

/// A project's camps: base first, then pitched before struck, oldest first.
pub async fn list_camps_conn(conn: &TrackedConnection, project_id: &str) -> DbResult<Vec<Camp>> {
    let mut rows = conn
        .query(
            format!(
                "SELECT {CAMP_COLUMNS} FROM camps WHERE project_id = ?1
                 ORDER BY kind = 'base' DESC, status = 'struck' ASC, created_at ASC, slug ASC"
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

/// The principal a subject is stored with, after checking that a thread
/// subject belongs to the camp's project.
async fn subject_principal_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
) -> DbResult<PrincipalRef> {
    match subject {
        CampSubject::Thread { thread_id } => {
            let (project_id, principal) = thread_principal_conn(conn, thread_id)
                .await?
                .ok_or_else(|| DbError::internal(format!("thread not found: {thread_id}")))?;
            if project_id != camp.project_id {
                return Err(DbError::internal(format!(
                    "thread {thread_id} belongs to another project; a camp's participants are its own project's"
                )));
            }
            Ok(principal)
        }
        CampSubject::Human {
            issuer,
            subject,
            organization,
        } => {
            let principal = PrincipalRef::Human {
                issuer: issuer.clone(),
                subject: subject.clone(),
                organization: organization.clone(),
            };
            principal
                .validate_at(PrincipalPosition::DurableSubject)
                .map_err(|error| DbError::internal(format!("invalid human subject: {error}")))?;
            Ok(principal)
        }
    }
}

/// Move the camp's policy epoch and return the new value.
async fn bump_epoch_conn(conn: &TrackedConnection, camp_id: &str, now: i64) -> DbResult<i64> {
    let changed = conn
        .execute(
            "UPDATE camps SET policy_epoch = policy_epoch + 1, updated_at = ?2 WHERE id = ?1",
            params![camp_id, now],
        )
        .await?;
    if changed == 0 {
        return Err(DbError::internal(format!("camp not found: {camp_id}")));
    }
    let mut rows = conn
        .query(
            "SELECT policy_epoch FROM camps WHERE id = ?1",
            params![camp_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::internal(format!("camp not found: {camp_id}")))?;
    row.i64(0)
}

struct JournalEntry<'a> {
    kind: &'static str,
    subject: Option<(&'a str, &'a str)>,
    old_value: Option<&'a str>,
    new_value: Option<&'a str>,
    reason: Option<&'a str>,
}

async fn journal_conn(
    conn: &TrackedConnection,
    camp_id: &str,
    epoch: i64,
    entry: JournalEntry<'_>,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<()> {
    let (actor_json, appearance_json) = CampActor::columns(actor)?;
    let (subject_key, subject_json) = entry.subject.unzip();
    conn.execute(
        "INSERT INTO camp_events
           (camp_id, policy_epoch, kind, subject_key, subject_json, old_value, new_value,
            reason, actor_json, appearance_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            camp_id,
            epoch,
            entry.kind,
            subject_key,
            subject_json,
            entry.old_value,
            entry.new_value,
            entry.reason,
            actor_json,
            appearance_json,
            now
        ],
    )
    .await?;
    Ok(())
}

async fn insert_camp_row_conn(
    conn: &TrackedConnection,
    input: &NewCamp,
    kind: CampKind,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<Camp> {
    let (lead_project, lead_principal) = thread_principal_conn(conn, &input.lead_thread_id)
        .await?
        .ok_or_else(|| {
            DbError::internal(format!("lead thread not found: {}", input.lead_thread_id))
        })?;
    if lead_project != input.project_id {
        return Err(DbError::internal(
            "a camp's lead must be a thread of the camp's own project",
        ));
    }
    if input.title.trim().is_empty() {
        return Err(DbError::internal("a camp needs a non-empty title"));
    }
    let id = cairn_common::ids::mint_child(&input.project_id);
    conn.execute(
        "INSERT INTO camps (id, project_id, slug, title, kind, lead_thread_id, status,
                            policy_epoch, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pitched', 0, ?7, ?7)",
        params![
            id.as_str(),
            input.project_id.as_str(),
            input.slug.as_str(),
            input.title.trim(),
            kind.as_str(),
            input.lead_thread_id.as_str(),
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
    let lead_json = serde_json::to_string(&lead_principal)
        .map_err(|error| DbError::internal(error.to_string()))?;
    journal_conn(
        conn,
        &id,
        0,
        JournalEntry {
            kind: "pitched",
            subject: Some((&input.lead_thread_id, &lead_json)),
            old_value: None,
            new_value: Some(CampRole::Lead.as_str()),
            reason: None,
        },
        actor,
        now,
    )
    .await?;
    camp_by_id_conn(conn, &id)
        .await?
        .ok_or_else(|| DbError::internal("created camp was not found"))
}

/// Pitch an ordinary camp. Its lead is the given thread; nobody else is a
/// participant until they join or are assigned.
pub async fn pitch_camp_conn(
    conn: &TrackedConnection,
    input: &NewCamp,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<Camp> {
    validate_camp_slug(&input.slug).map_err(DbError::internal)?;
    insert_camp_row_conn(conn, input, CampKind::Camp, actor, now).await
}

/// Establish a project's base camp if it has none, with `lead_thread_id` as
/// its lead, and seat every thread of the project that is not yet a
/// participant. Returns the camp and whether this call created it.
///
/// An existing base camp is returned as it is: its lead may have been
/// reassigned and participants may have chosen their levels, and a retry or a
/// later boot must not undo either.
pub async fn ensure_base_camp_conn(
    conn: &TrackedConnection,
    project_id: &str,
    lead_thread_id: &str,
    now: i64,
) -> DbResult<(Camp, bool)> {
    let (camp, created) = match base_camp_conn(conn, project_id).await? {
        Some(camp) => (camp, false),
        None => {
            let input = NewCamp {
                project_id: project_id.to_string(),
                slug: BASE_CAMP_SLUG.to_string(),
                title: BASE_CAMP_TITLE.to_string(),
                lead_thread_id: lead_thread_id.to_string(),
            };
            (
                insert_camp_row_conn(conn, &input, CampKind::Base, None, now).await?,
                true,
            )
        }
    };
    seat_project_threads_conn(conn, &camp, now).await?;
    Ok((camp, created))
}

/// Give every thread of base camp's project that has no membership row one at
/// `watch`, closed threads included: a closed thread stays dormant, and
/// reopening it finds its membership waiting.
async fn seat_project_threads_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    now: i64,
) -> DbResult<usize> {
    let mut rows = conn
        .query(
            "SELECT t.id FROM threads t
             WHERE t.project_id = ?1
               AND NOT EXISTS (
                   SELECT 1 FROM wake_subscriptions w
                   WHERE w.camp_id = ?2 AND w.subject_key = t.id
               )
             ORDER BY t.created_at, t.id",
            params![camp.project_id.as_str(), camp.id.as_str()],
        )
        .await?;
    let mut missing = Vec::new();
    while let Some(row) = rows.next().await? {
        missing.push(row.text(0)?);
    }
    drop(rows);
    for thread_id in &missing {
        let subject = CampSubject::Thread {
            thread_id: thread_id.clone(),
        };
        set_camp_level_conn(
            conn,
            camp,
            &subject,
            CampLevel::Watch,
            BASE_MEMBERSHIP_REASON,
            CampAssignedBy::System,
            None,
            now,
        )
        .await?;
    }
    Ok(missing.len())
}

/// Seat a newly born thread in its project's base camp, inside the thread's
/// creation transaction. A project whose base camp is not established yet
/// seats the thread when its bootstrap runs, so this returns false then.
pub async fn join_base_camp_conn(
    conn: &TrackedConnection,
    project_id: &str,
    thread_id: &str,
    now: i64,
) -> DbResult<bool> {
    let Some(camp) = base_camp_conn(conn, project_id).await? else {
        return Ok(false);
    };
    let subject = CampSubject::Thread {
        thread_id: thread_id.to_string(),
    };
    set_camp_level_conn(
        conn,
        &camp,
        &subject,
        CampLevel::Watch,
        BASE_MEMBERSHIP_REASON,
        CampAssignedBy::System,
        None,
        now,
    )
    .await?;
    Ok(true)
}

/// Join a camp at a level, or change the level and reason of an existing
/// membership. Authorization is the caller's: this enforces only what the
/// data requires (a live camp, a same-project subject, a reason).
#[allow(clippy::too_many_arguments)]
pub async fn set_camp_level_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
    level: CampLevel,
    reason: &str,
    assigned_by: CampAssignedBy,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<CampLevelChange> {
    if camp.is_struck() {
        return Err(DbError::internal(format!(
            "camp `{}` is struck; it takes no new participants",
            camp.slug
        )));
    }
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(DbError::internal("joining a camp needs a non-empty reason"));
    }
    let principal = subject_principal_conn(conn, camp, subject).await?;
    let subject_key = subject.key();
    let subject_json =
        serde_json::to_string(&principal).map_err(|error| DbError::internal(error.to_string()))?;
    let (assigned_by_json, assigned_appearance_json) = CampActor::columns(actor)?;

    let mut rows = conn
        .query(
            "SELECT camp_level, reason FROM wake_subscriptions
             WHERE camp_id = ?1 AND subject_key = ?2",
            params![camp.id.as_str(), subject_key.as_str()],
        )
        .await?;
    let existing = match rows.next().await? {
        Some(row) => Some((row.text(0)?, row.opt_text(1)?)),
        None => None,
    };
    drop(rows);

    let change = match existing {
        Some((current, current_reason)) => {
            let from = CampLevel::parse(&current)
                .ok_or_else(|| DbError::Row(format!("unknown camp level: {current}")))?;
            if from == level && current_reason.as_deref() == Some(reason) {
                return Ok(CampLevelChange::Unchanged(level));
            }
            conn.execute(
                "UPDATE wake_subscriptions
                    SET camp_level = ?3, reason = ?4, subject_json = ?5,
                        assigned_by_json = ?6, assigned_appearance_json = ?7, updated_at = ?8
                  WHERE camp_id = ?1 AND subject_key = ?2",
                params![
                    camp.id.as_str(),
                    subject_key.as_str(),
                    level.as_str(),
                    reason,
                    subject_json.as_str(),
                    assigned_by_json,
                    assigned_appearance_json,
                    now
                ],
            )
            .await?;
            CampLevelChange::Changed { from, to: level }
        }
        None => {
            conn.execute(
                "INSERT INTO wake_subscriptions
                   (id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by,
                    created_at, updated_at, one_shot, camp_id, subject_key, subject_json,
                    camp_level, reason, assigned_by_json, assigned_appearance_json)
                 VALUES (?1, NULL, ?2, NULL, NULL, 'active', ?3, ?4, ?4, 0, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    cairn_common::ids::mint_child(&camp.project_id),
                    CAMP_SOURCE_KIND,
                    assigned_by.as_str(),
                    now,
                    camp.id.as_str(),
                    subject_key.as_str(),
                    subject_json.as_str(),
                    level.as_str(),
                    reason,
                    assigned_by_json,
                    assigned_appearance_json
                ],
            )
            .await?;
            CampLevelChange::Joined(level)
        }
    };
    let epoch = bump_epoch_conn(conn, &camp.id, now).await?;
    let (kind, old_value) = match change {
        CampLevelChange::Joined(_) => ("joined", None),
        CampLevelChange::Changed { from, .. } => ("role_changed", Some(from.as_str())),
        CampLevelChange::Unchanged(_) => unreachable!("returned above"),
    };
    journal_conn(
        conn,
        &camp.id,
        epoch,
        JournalEntry {
            kind,
            subject: Some((&subject_key, &subject_json)),
            old_value,
            new_value: Some(level.as_str()),
            reason: Some(reason),
        },
        actor,
        now,
    )
    .await?;
    Ok(change)
}

/// Leave an ordinary camp. Base camp membership is permanent (only its level
/// changes), and the lead cannot leave without handing off first.
pub async fn leave_camp_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    subject: &CampSubject,
    reason: Option<&str>,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<bool> {
    if camp.is_base() {
        return Err(DbError::internal(
            "base camp membership cannot be removed; change its level to watch instead",
        ));
    }
    if subject.thread_id() == Some(camp.lead_thread_id.as_str()) {
        return Err(DbError::internal(format!(
            "the lead cannot leave camp `{}`; hand the lead to another thread first",
            camp.slug
        )));
    }
    let subject_key = subject.key();
    let mut rows = conn
        .query(
            "SELECT camp_level, subject_json FROM wake_subscriptions
             WHERE camp_id = ?1 AND subject_key = ?2",
            params![camp.id.as_str(), subject_key.as_str()],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    let (level, subject_json) = (row.text(0)?, row.text(1)?);
    drop(rows);
    conn.execute(
        "DELETE FROM wake_subscriptions WHERE camp_id = ?1 AND subject_key = ?2",
        params![camp.id.as_str(), subject_key.as_str()],
    )
    .await?;
    let epoch = bump_epoch_conn(conn, &camp.id, now).await?;
    journal_conn(
        conn,
        &camp.id,
        epoch,
        JournalEntry {
            kind: "left",
            subject: Some((&subject_key, &subject_json)),
            old_value: Some(&level),
            new_value: None,
            reason,
        },
        actor,
        now,
    )
    .await?;
    Ok(true)
}

/// Change a camp's title. Struck camps keep the title they were struck with.
pub async fn retitle_camp_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    title: &str,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<bool> {
    let title = title.trim();
    if title.is_empty() {
        return Err(DbError::internal("a camp needs a non-empty title"));
    }
    if camp.is_struck() {
        return Err(DbError::internal(format!(
            "camp `{}` is struck; its record is read-only",
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
    journal_conn(
        conn,
        &camp.id,
        camp.policy_epoch,
        JournalEntry {
            kind: "retitled",
            subject: None,
            old_value: Some(&camp.title),
            new_value: Some(title),
            reason: None,
        },
        actor,
        now,
    )
    .await?;
    Ok(true)
}

/// The slugs of the live camps `thread_id` leads.
pub async fn camps_led_by_conn(conn: &TrackedConnection, thread_id: &str) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT slug FROM camps WHERE lead_thread_id = ?1 AND status = 'pitched' ORDER BY slug",
            params![thread_id],
        )
        .await?;
    let mut slugs = Vec::new();
    while let Some(row) = rows.next().await? {
        slugs.push(row.text(0)?);
    }
    Ok(slugs)
}

/// Offer a camp's lead to another thread of its project. Only the lead offers
/// (the caller's check); the lead moves only when the named thread accepts
/// ([`accept_lead_conn`]). A new offer replaces an earlier one.
pub async fn offer_lead_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    to_thread_id: &str,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<()> {
    if camp.is_struck() {
        return Err(DbError::internal(format!("camp `{}` is struck", camp.slug)));
    }
    if to_thread_id == camp.lead_thread_id {
        return Err(DbError::internal("that thread already leads the camp"));
    }
    let principal = subject_principal_conn(
        conn,
        camp,
        &CampSubject::Thread {
            thread_id: to_thread_id.to_string(),
        },
    )
    .await?;
    let principal =
        serde_json::to_string(&principal).map_err(|error| DbError::internal(error.to_string()))?;
    conn.execute(
        "UPDATE camps SET lead_offer_thread_id = ?2, updated_at = ?3 WHERE id = ?1",
        params![camp.id.as_str(), to_thread_id, now],
    )
    .await?;
    journal_conn(
        conn,
        &camp.id,
        camp.policy_epoch,
        JournalEntry {
            kind: "lead_offered",
            subject: Some((to_thread_id, &principal)),
            old_value: Some(&camp.lead_thread_id),
            new_value: Some(to_thread_id),
            reason: None,
        },
        actor,
        now,
    )
    .await
}

/// Take a camp's lead that was offered to `thread_id`. The thread must be
/// active, so a lead is never handed to a dormant recipient. In an ordinary
/// camp the former lead stays a participant at `wake`.
pub async fn accept_lead_conn(
    conn: &TrackedConnection,
    camp: &Camp,
    thread_id: &str,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<()> {
    if camp.lead_offer_thread_id.as_deref() != Some(thread_id) {
        return Err(DbError::internal(format!(
            "camp `{}` has not offered its lead to this thread; its lead offers it first",
            camp.slug
        )));
    }
    let mut rows = conn
        .query(
            "SELECT status FROM threads WHERE id = ?1",
            params![thread_id],
        )
        .await?;
    let active = match rows.next().await? {
        Some(row) => row.text(0)? == "active",
        None => false,
    };
    drop(rows);
    if !active {
        return Err(DbError::internal(
            "a closed thread cannot take a camp's lead",
        ));
    }
    // Compare-and-set on the lead read with the offer, so a concurrent
    // handoff cannot be overwritten silently.
    let changed = conn
        .execute(
            "UPDATE camps SET lead_thread_id = ?2, lead_offer_thread_id = NULL, updated_at = ?4
             WHERE id = ?1 AND lead_thread_id = ?3 AND lead_offer_thread_id = ?2",
            params![
                camp.id.as_str(),
                thread_id,
                camp.lead_thread_id.as_str(),
                now
            ],
        )
        .await?;
    if changed == 0 {
        return Err(DbError::internal(
            "the camp's lead changed while the offer was being accepted; read it again",
        ));
    }
    let epoch = bump_epoch_conn(conn, &camp.id, now).await?;
    journal_conn(
        conn,
        &camp.id,
        epoch,
        JournalEntry {
            kind: "lead_changed",
            subject: None,
            old_value: Some(&camp.lead_thread_id),
            new_value: Some(thread_id),
            reason: None,
        },
        actor,
        now,
    )
    .await?;
    if !camp.is_base() {
        let mut rows = conn
            .query(
                "SELECT 1 FROM wake_subscriptions WHERE camp_id = ?1 AND subject_key = ?2",
                params![camp.id.as_str(), camp.lead_thread_id.as_str()],
            )
            .await?;
        let seated = rows.next().await?.is_some();
        drop(rows);
        if !seated {
            let updated = camp_by_id_conn(conn, &camp.id)
                .await?
                .ok_or_else(|| DbError::internal("camp vanished during handoff"))?;
            set_camp_level_conn(
                conn,
                &updated,
                &CampSubject::Thread {
                    thread_id: camp.lead_thread_id.clone(),
                },
                CampLevel::Wake,
                "the camp's former lead",
                CampAssignedBy::System,
                actor,
                now,
            )
            .await?;
        }
    }
    Ok(())
}

/// Whether a camp's lead has ever been offered or handed over. A lead that
/// moved by offer and acceptance was chosen, and nothing automatic moves it.
pub async fn camp_lead_was_handed_conn(conn: &TrackedConnection, camp_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM camp_events
             WHERE camp_id = ?1 AND kind IN ('lead_offered', 'lead_changed') LIMIT 1",
            params![camp_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Why a base camp's lead was moved to its project's general thread.
pub const BASE_LEAD_REPAIR_REASON: &str =
    "base camp is led by its project's general thread; the lead seated without one is a watch participant";

/// Seat `general_thread_id` as lead of a base camp that an earlier birth gave
/// to another thread because the project had no general thread. Returns the
/// displaced lead, or `None` when nothing was written.
///
/// The camp is left as it is when general already leads, when its lead was
/// ever offered or handed over, or when the general thread predates the camp:
/// a general that existed at birth was passed over deliberately, while one
/// born after the camp was minted by this repair (a boot interrupted between
/// minting and moving finishes the move on the next boot). The displaced lead
/// keeps the `watch` membership birth seated it with.
pub async fn repair_base_camp_lead_conn(
    conn: &TrackedConnection,
    project_id: &str,
    general_thread_id: &str,
    now: i64,
) -> DbResult<Option<String>> {
    let Some(camp) = base_camp_conn(conn, project_id).await? else {
        return Ok(None);
    };
    if camp.lead_thread_id == general_thread_id || camp.is_struck() {
        return Ok(None);
    }
    if camp_lead_was_handed_conn(conn, &camp.id).await? {
        return Ok(None);
    }
    let mut rows = conn
        .query(
            "SELECT created_at FROM threads WHERE id = ?1 AND project_id = ?2",
            params![general_thread_id, project_id],
        )
        .await?;
    let general_born = match rows.next().await? {
        Some(row) => row.i64(0)?,
        None => {
            return Err(DbError::internal(format!(
                "general thread {general_thread_id} is not a thread of project {project_id}"
            )))
        }
    };
    drop(rows);
    if general_born < camp.created_at {
        return Ok(None);
    }
    let changed = conn
        .execute(
            "UPDATE camps SET lead_thread_id = ?2, updated_at = ?4
             WHERE id = ?1 AND lead_thread_id = ?3 AND lead_offer_thread_id IS NULL",
            params![
                camp.id.as_str(),
                general_thread_id,
                camp.lead_thread_id.as_str(),
                now
            ],
        )
        .await?;
    if changed == 0 {
        return Ok(None);
    }
    let epoch = bump_epoch_conn(conn, &camp.id, now).await?;
    journal_conn(
        conn,
        &camp.id,
        epoch,
        JournalEntry {
            kind: "lead_changed",
            subject: None,
            old_value: Some(&camp.lead_thread_id),
            new_value: Some(general_thread_id),
            reason: Some(BASE_LEAD_REPAIR_REASON),
        },
        None,
        now,
    )
    .await?;
    let repaired = camp_by_id_conn(conn, &camp.id)
        .await?
        .ok_or_else(|| DbError::internal("camp vanished during lead repair"))?;
    seat_project_threads_conn(conn, &repaired, now).await?;
    Ok(Some(camp.lead_thread_id))
}

/// What an issue's camp write did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueCampChange {
    Joined { camp_slug: String },
    Left { camp_slug: String },
    Unchanged,
}

/// Join an issue to a camp, or (`camp: None`) take it out of the one it is in.
///
/// An issue belongs to at most one camp, separately from its parent, so a move
/// between camps is two explicit acts: leave, then join. Both camps' epochs
/// move, which is what serializes a join against a concurrent role change on
/// either. Authorization is the caller's.
pub async fn set_issue_camp_conn(
    conn: &TrackedConnection,
    issue_id: &str,
    camp: Option<&Camp>,
    actor: Option<&CampActor>,
    now: i64,
) -> DbResult<IssueCampChange> {
    let mut rows = conn
        .query(
            "SELECT project_id, number, camp_id FROM issues WHERE id = ?1",
            params![issue_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::internal(format!("issue not found: {issue_id}")));
    };
    let (project_id, number, current) = (row.text(0)?, row.i64(1)?, row.opt_text(2)?);
    drop(rows);
    let number_text = number.to_string();
    match (camp, current) {
        (Some(camp), Some(current)) if current == camp.id => Ok(IssueCampChange::Unchanged),
        (Some(_), Some(current)) => {
            let slug = camp_by_id_conn(conn, &current)
                .await?
                .map(|camp| camp.slug)
                .unwrap_or(current);
            Err(DbError::internal(format!(
                "issue #{number} is already joined to camp `{slug}`; take it out first (camp: null), then join the other"
            )))
        }
        (Some(camp), None) => {
            if camp.project_id != project_id {
                return Err(DbError::internal(
                    "an issue joins a camp of its own project only",
                ));
            }
            if camp.is_struck() {
                return Err(DbError::internal(format!(
                    "camp `{}` is struck; it takes no new issues",
                    camp.slug
                )));
            }
            conn.execute(
                "UPDATE issues SET camp_id = ?2 WHERE id = ?1",
                params![issue_id, camp.id.as_str()],
            )
            .await?;
            let epoch = bump_epoch_conn(conn, &camp.id, now).await?;
            journal_conn(
                conn,
                &camp.id,
                epoch,
                JournalEntry {
                    kind: "issue_joined",
                    subject: None,
                    old_value: None,
                    new_value: Some(&number_text),
                    reason: None,
                },
                actor,
                now,
            )
            .await?;
            Ok(IssueCampChange::Joined {
                camp_slug: camp.slug.clone(),
            })
        }
        (None, Some(current)) => {
            conn.execute(
                "UPDATE issues SET camp_id = NULL WHERE id = ?1",
                params![issue_id],
            )
            .await?;
            let epoch = bump_epoch_conn(conn, &current, now).await?;
            journal_conn(
                conn,
                &current,
                epoch,
                JournalEntry {
                    kind: "issue_left",
                    subject: None,
                    old_value: Some(&number_text),
                    new_value: None,
                    reason: None,
                },
                actor,
                now,
            )
            .await?;
            let slug = camp_by_id_conn(conn, &current)
                .await?
                .map(|camp| camp.slug)
                .unwrap_or(current);
            Ok(IssueCampChange::Left { camp_slug: slug })
        }
        (None, None) => Ok(IssueCampChange::Unchanged),
    }
}

/// One journal row of a camp, newest first when listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampEvent {
    pub id: i64,
    pub policy_epoch: i64,
    pub kind: String,
    pub subject_key: Option<String>,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    pub reason: Option<String>,
    pub actor: Option<PrincipalRef>,
    pub created_at: i64,
}

/// A camp's most recent journal rows, newest first.
pub async fn camp_history_conn(
    conn: &TrackedConnection,
    camp_id: &str,
    limit: i64,
) -> DbResult<Vec<CampEvent>> {
    let mut rows = conn
        .query(
            "SELECT id, policy_epoch, kind, subject_key, old_value, new_value, reason,
                    actor_json, created_at
             FROM camp_events WHERE camp_id = ?1 ORDER BY id DESC LIMIT ?2",
            params![camp_id, limit],
        )
        .await?;
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        events.push(CampEvent {
            id: row.i64(0)?,
            policy_epoch: row.i64(1)?,
            kind: row.text(2)?,
            subject_key: row.opt_text(3)?,
            old_value: row.opt_text(4)?,
            new_value: row.opt_text(5)?,
            reason: row.opt_text(6)?,
            actor: row
                .opt_text(7)?
                .and_then(|json| serde_json::from_str(&json).ok()),
            created_at: row.i64(8)?,
        });
    }
    Ok(events)
}

/// An issue joined to a camp, with the live state the issue row holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampIssue {
    pub number: i64,
    pub title: String,
    pub status: String,
    pub updated_at: i64,
}

/// The issues joined to a camp, by number.
pub async fn camp_issues_conn(conn: &TrackedConnection, camp_id: &str) -> DbResult<Vec<CampIssue>> {
    let mut rows = conn
        .query(
            "SELECT number, title, status, updated_at FROM issues WHERE camp_id = ?1 ORDER BY number",
            params![camp_id],
        )
        .await?;
    let mut issues = Vec::new();
    while let Some(row) = rows.next().await? {
        issues.push(CampIssue {
            number: row.i64(0)?,
            title: row.text(1)?,
            status: row.text(2)?,
            updated_at: row.i64(3)?,
        });
    }
    Ok(issues)
}

fn subject_from_row(key: &str, principal: Option<&PrincipalRef>) -> DbResult<CampSubject> {
    if CampSubject::key_is_thread(key) {
        return Ok(CampSubject::Thread {
            thread_id: key.to_string(),
        });
    }
    principal
        .and_then(CampSubject::human_from_principal)
        .ok_or_else(|| DbError::Row(format!("camp subject {key:?} has no human principal")))
}

/// Every participant of a camp with its effective role: the maximum of the
/// lead edge and its stored level. Highest role first, then longest standing.
pub async fn camp_participants_conn(
    conn: &TrackedConnection,
    camp: &Camp,
) -> DbResult<Vec<CampParticipant>> {
    let mut rows = conn
        .query(
            "SELECT subject_key, subject_json, camp_level, reason, created_at, assigned_by_json
             FROM wake_subscriptions WHERE camp_id = ?1",
            params![camp.id.as_str()],
        )
        .await?;
    let mut by_key: BTreeMap<String, CampParticipant> = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let key = row.text(0)?;
        let principal: Option<PrincipalRef> = serde_json::from_str(&row.text(1)?).ok();
        let level_text = row.text(2)?;
        let level = CampLevel::parse(&level_text)
            .ok_or_else(|| DbError::Row(format!("unknown camp level: {level_text}")))?;
        let subject = subject_from_row(&key, principal.as_ref())?;
        by_key.insert(
            key.clone(),
            CampParticipant {
                subject_key: key,
                subject,
                principal,
                role: level.role(),
                lead: false,
                level: Some(level),
                reason: row.opt_text(3)?,
                assigned_by: row
                    .opt_text(5)?
                    .and_then(|json| serde_json::from_str(&json).ok()),
                since: row.i64(4)?,
            },
        );
    }
    drop(rows);
    match by_key.get_mut(&camp.lead_thread_id) {
        Some(participant) => {
            participant.lead = true;
            participant.role = CampRole::Lead;
        }
        None => {
            let principal = thread_principal_conn(conn, &camp.lead_thread_id)
                .await?
                .map(|(_, principal)| principal);
            by_key.insert(
                camp.lead_thread_id.clone(),
                CampParticipant {
                    subject_key: camp.lead_thread_id.clone(),
                    subject: CampSubject::Thread {
                        thread_id: camp.lead_thread_id.clone(),
                    },
                    principal,
                    role: CampRole::Lead,
                    lead: true,
                    level: None,
                    reason: None,
                    assigned_by: None,
                    since: camp.created_at,
                },
            );
        }
    }
    let mut participants: Vec<_> = by_key.into_values().collect();
    participants.sort_by(|a, b| {
        b.role
            .cmp(&a.role)
            .then(a.since.cmp(&b.since))
            .then(a.subject_key.cmp(&b.subject_key))
    });
    Ok(participants)
}

/// The camps a thread takes part in, each with its effective role, in the
/// project's camp order.
pub async fn camps_for_thread_conn(
    conn: &TrackedConnection,
    thread_id: &str,
) -> DbResult<Vec<(Camp, CampRole)>> {
    let mut roles: BTreeMap<String, CampRole> = BTreeMap::new();
    let mut rows = conn
        .query(
            "SELECT camp_id, camp_level FROM wake_subscriptions
             WHERE subject_key = ?1 AND camp_id IS NOT NULL",
            params![thread_id],
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let level_text = row.text(1)?;
        let level = CampLevel::parse(&level_text)
            .ok_or_else(|| DbError::Row(format!("unknown camp level: {level_text}")))?;
        roles.insert(row.text(0)?, level.role());
    }
    drop(rows);
    let mut rows = conn
        .query(
            "SELECT id FROM camps WHERE lead_thread_id = ?1",
            params![thread_id],
        )
        .await?;
    while let Some(row) = rows.next().await? {
        roles.insert(row.text(0)?, CampRole::Lead);
    }
    drop(rows);
    let mut camps = Vec::with_capacity(roles.len());
    for (camp_id, role) in roles {
        if let Some(camp) = camp_by_id_conn(conn, &camp_id).await? {
            camps.push((camp, role));
        }
    }
    camps.sort_by(|(a, _), (b, _)| {
        b.is_base()
            .cmp(&a.is_base())
            .then(a.is_struck().cmp(&b.is_struck()))
            .then(a.created_at.cmp(&b.created_at))
            .then(a.slug.cmp(&b.slug))
    });
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

    pub async fn camp_participants(&self, camp: &Camp) -> DbResult<Vec<CampParticipant>> {
        let camp = camp.clone();
        self.read(move |conn| {
            let camp = camp.clone();
            Box::pin(async move { camp_participants_conn(conn, &camp).await })
        })
        .await
    }

    pub async fn camps_for_thread(&self, thread_id: &str) -> DbResult<Vec<(Camp, CampRole)>> {
        let thread_id = thread_id.to_string();
        self.read(move |conn| {
            let thread_id = thread_id.clone();
            Box::pin(async move { camps_for_thread_conn(conn, &thread_id).await })
        })
        .await
    }

    pub async fn camp_lead_was_handed(&self, camp_id: &str) -> DbResult<bool> {
        let camp_id = camp_id.to_string();
        self.read(move |conn| {
            let camp_id = camp_id.clone();
            Box::pin(async move { camp_lead_was_handed_conn(conn, &camp_id).await })
        })
        .await
    }

    pub async fn repair_base_camp_lead(
        &self,
        project_id: &str,
        general_thread_id: &str,
        now: i64,
    ) -> DbResult<Option<String>> {
        let (project_id, general) = (project_id.to_string(), general_thread_id.to_string());
        self.write(move |conn| {
            let (project_id, general) = (project_id.clone(), general.clone());
            Box::pin(
                async move { repair_base_camp_lead_conn(conn, &project_id, &general, now).await },
            )
        })
        .await
    }

    pub async fn ensure_base_camp(
        &self,
        project_id: &str,
        lead_thread_id: &str,
        now: i64,
    ) -> DbResult<(Camp, bool)> {
        let (project_id, lead_thread_id) = (project_id.to_string(), lead_thread_id.to_string());
        self.write(move |conn| {
            let (project_id, lead_thread_id) = (project_id.clone(), lead_thread_id.clone());
            Box::pin(
                async move { ensure_base_camp_conn(conn, &project_id, &lead_thread_id, now).await },
            )
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

    async fn write<T: Send + 'static>(
        db: &LocalDb,
        f: impl for<'c> Fn(
                &'c TrackedConnection,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = DbResult<T>> + Send + 'c>,
            > + Send
            + Sync,
    ) -> DbResult<T> {
        db.write(|conn| f(conn)).await
    }

    #[tokio::test]
    async fn base_camp_seats_every_thread_once_and_keeps_chosen_state() {
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

        let (camp, created) = db.ensure_base_camp("p", "t-general", 10).await.unwrap();
        assert!(created);
        assert!(camp.is_base());
        assert_eq!(camp.slug, BASE_CAMP_SLUG);
        assert_eq!(camp.lead_thread_id, "t-general");

        let participants = db.camp_participants(&camp).await.unwrap();
        let keys: Vec<_> = participants
            .iter()
            .map(|p| p.subject_key.as_str())
            .collect();
        assert_eq!(
            keys,
            ["t-general", "t-devops", "t-old"],
            "closed threads are seated; other projects are not"
        );
        assert_eq!(participants[0].role, CampRole::Lead);
        assert!(participants[1..].iter().all(|p| p.role == CampRole::Watch));

        // devops opts up to wake; a retry must keep that, and keep the lead.
        let base = camp.clone();
        write(&db, move |conn| {
            let base = base.clone();
            Box::pin(async move {
                set_camp_level_conn(
                    conn,
                    &base,
                    &thread("t-devops"),
                    CampLevel::Wake,
                    "rebuild notices change my routing",
                    CampAssignedBy::Agent,
                    None,
                    11,
                )
                .await
            })
        })
        .await
        .unwrap();
        let (again, created) = db.ensure_base_camp("p", "t-devops", 12).await.unwrap();
        assert!(!created);
        assert_eq!(again.id, camp.id);
        assert_eq!(
            again.lead_thread_id, "t-general",
            "a retry never reassigns the lead"
        );
        let participants = db.camp_participants(&again).await.unwrap();
        assert_eq!(participants.len(), 3);
        let devops = participants
            .iter()
            .find(|p| p.subject_key == "t-devops")
            .unwrap();
        assert_eq!(devops.role, CampRole::Wake);
        let count = db
            .query_all("SELECT id FROM camps WHERE project_id = 'p'", (), |row| {
                row.text(0)
            })
            .await
            .unwrap();
        assert_eq!(count.len(), 1);
    }

    #[tokio::test]
    async fn a_thread_born_after_base_camp_joins_it_at_watch() {
        let db = migrated_test_db("camps-birth").await;
        project_with_threads(&db, &[("t-general", "general")]).await;
        db.ensure_base_camp("p", "t-general", 10).await.unwrap();
        db.execute(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES ('t-new', 'p', 'new', 'active', 20, 20)",
            (),
        )
        .await
        .unwrap();
        let joined = write(&db, |conn| {
            Box::pin(async move { join_base_camp_conn(conn, "p", "t-new", 20).await })
        })
        .await
        .unwrap();
        assert!(joined);
        let camps = db.camps_for_thread("t-new").await.unwrap();
        assert_eq!(camps.len(), 1);
        assert_eq!(camps[0].1, CampRole::Watch);
        let no_base = write(&db, |conn| {
            Box::pin(async move { join_base_camp_conn(conn, "q", "t-new", 20).await })
        })
        .await
        .unwrap();
        assert!(
            !no_base,
            "a project without base camp seats its threads at bootstrap"
        );
    }

    #[tokio::test]
    async fn pitched_camp_roles_are_journaled_and_bounded() {
        let db = migrated_test_db("camps-pitch").await;
        project_with_threads(
            &db,
            &[
                ("t-devops", "devops"),
                ("t-exec", "execution"),
                ("q-x", "x"),
            ],
        )
        .await;
        let camp = write(&db, |conn| {
            Box::pin(async move {
                pitch_camp_conn(
                    conn,
                    &NewCamp {
                        project_id: "p".into(),
                        slug: "rebuild".into(),
                        title: "Rebuild as an action".into(),
                        lead_thread_id: "t-devops".into(),
                    },
                    None,
                    5,
                )
                .await
            })
        })
        .await
        .unwrap();
        assert_eq!(camp.kind, CampKind::Camp);

        let duplicate = write(&db, |conn| {
            Box::pin(async move {
                pitch_camp_conn(
                    conn,
                    &NewCamp {
                        project_id: "p".into(),
                        slug: "rebuild".into(),
                        title: "Again".into(),
                        lead_thread_id: "t-exec".into(),
                    },
                    None,
                    6,
                )
                .await
            })
        })
        .await;
        assert!(duplicate
            .unwrap_err()
            .to_string()
            .contains("already exists"));
        let reserved = write(&db, |conn| {
            Box::pin(async move {
                pitch_camp_conn(
                    conn,
                    &NewCamp {
                        project_id: "p".into(),
                        slug: "base".into(),
                        title: "Base".into(),
                        lead_thread_id: "t-exec".into(),
                    },
                    None,
                    6,
                )
                .await
            })
        })
        .await;
        assert!(reserved.is_err());
        let cross_project_lead = write(&db, |conn| {
            Box::pin(async move {
                pitch_camp_conn(
                    conn,
                    &NewCamp {
                        project_id: "p".into(),
                        slug: "x".into(),
                        title: "X".into(),
                        lead_thread_id: "q-x".into(),
                    },
                    None,
                    6,
                )
                .await
            })
        })
        .await;
        assert!(cross_project_lead.is_err());

        let c = camp.clone();
        let change = write(&db, move |conn| {
            let c = c.clone();
            Box::pin(async move {
                set_camp_level_conn(
                    conn,
                    &c,
                    &thread("t-exec"),
                    CampLevel::Wake,
                    "owns placement",
                    CampAssignedBy::Agent,
                    None,
                    7,
                )
                .await
            })
        })
        .await
        .unwrap();
        assert_eq!(change, CampLevelChange::Joined(CampLevel::Wake));
        let c = camp.clone();
        let unchanged = write(&db, move |conn| {
            let c = c.clone();
            Box::pin(async move {
                set_camp_level_conn(
                    conn,
                    &c,
                    &thread("t-exec"),
                    CampLevel::Wake,
                    "owns placement",
                    CampAssignedBy::Agent,
                    None,
                    8,
                )
                .await
            })
        })
        .await
        .unwrap();
        assert_eq!(unchanged, CampLevelChange::Unchanged(CampLevel::Wake));

        for (subject, reason) in [("q-x", "other project"), ("t-exec", "   ")] {
            let c = camp.clone();
            let refused = write(&db, move |conn| {
                let c = c.clone();
                Box::pin(async move {
                    set_camp_level_conn(
                        conn,
                        &c,
                        &thread(subject),
                        CampLevel::Watch,
                        reason,
                        CampAssignedBy::Agent,
                        None,
                        9,
                    )
                    .await
                })
            })
            .await;
            assert!(refused.is_err(), "{subject} {reason:?}");
        }
        let c = camp.clone();
        let lead_leaves = write(&db, move |conn| {
            let c = c.clone();
            Box::pin(
                async move { leave_camp_conn(conn, &c, &thread("t-devops"), None, None, 9).await },
            )
        })
        .await;
        assert!(lead_leaves.is_err());
        let c = camp.clone();
        let left = write(&db, move |conn| {
            let c = c.clone();
            Box::pin(async move {
                leave_camp_conn(conn, &c, &thread("t-exec"), Some("done"), None, 10).await
            })
        })
        .await
        .unwrap();
        assert!(left);

        let events = db
            .query_all(
                "SELECT kind, policy_epoch FROM camp_events WHERE camp_id = ?1 ORDER BY id",
                (camp.id.clone(),),
                |row| Ok((row.text(0)?, row.i64(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            events,
            [
                ("pitched".to_string(), 0),
                ("joined".to_string(), 1),
                ("left".to_string(), 2)
            ],
            "an unchanged write journals nothing and does not move the epoch"
        );
        let participants = db.camp_participants(&camp).await.unwrap();
        assert_eq!(participants.len(), 1);
        assert!(participants[0].lead);
    }

    #[tokio::test]
    async fn base_membership_is_permanent() {
        let db = migrated_test_db("camps-permanent").await;
        project_with_threads(&db, &[("t-general", "general"), ("t-other", "other")]).await;
        let (camp, _) = db.ensure_base_camp("p", "t-general", 1).await.unwrap();
        let left = write(&db, move |conn| {
            let camp = camp.clone();
            Box::pin(async move {
                leave_camp_conn(conn, &camp, &thread("t-other"), None, None, 2).await
            })
        })
        .await;
        assert!(left.unwrap_err().to_string().contains("cannot be removed"));
    }

    #[tokio::test]
    async fn job_subscriptions_and_camp_subscriptions_cannot_mix() {
        let db = migrated_test_db("camps-owner-check").await;
        project_with_threads(&db, &[("t-general", "general")]).await;
        let (camp, _) = db.ensure_base_camp("p", "t-general", 1).await.unwrap();
        // A camp row without a reason, and a row with neither owner, are both
        // refused by the table itself.
        let no_reason = db
            .execute(
                "INSERT INTO wake_subscriptions (id, job_id, source_kind, created_at, updated_at,
                    camp_id, subject_key, subject_json, camp_level)
                 VALUES ('x', NULL, 'camp', 1, 1, ?1, 'k', '{}', 'watch')",
                (camp.id.clone(),),
            )
            .await;
        assert!(no_reason.is_err());
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

#[cfg(test)]
mod handoff_tests {
    use super::*;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn the_lead_moves_only_when_the_offered_thread_accepts() {
        let db = migrated_test_db("camps-handoff").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, repository_id, created_at, updated_at, is_workspace)
               VALUES ('p', 'default', 'Proj', 'proj', '/tmp/proj', 'p', 1, 1, 0);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at) VALUES
               ('a', 'p', 'alpha', 'active', 1, 1), ('b', 'p', 'beta', 'active', 2, 2),
               ('c', 'p', 'closed-gamma', 'closed', 3, 3);",
        )
        .await
        .unwrap();
        let camp = db
            .write(|conn| {
                Box::pin(async move {
                    pitch_camp_conn(
                        conn,
                        &NewCamp {
                            project_id: "p".into(),
                            slug: "x".into(),
                            title: "X".into(),
                            lead_thread_id: "a".into(),
                        },
                        None,
                        1,
                    )
                    .await
                })
            })
            .await
            .unwrap();
        let c = camp.clone();
        let unoffered = db
            .write(move |conn| {
                let c = c.clone();
                Box::pin(async move { accept_lead_conn(conn, &c, "b", None, 2).await })
            })
            .await;
        assert!(
            unoffered.is_err(),
            "nobody takes a lead that was not offered"
        );
        for to in ["b", "c"] {
            let c = camp.clone();
            db.write(move |conn| {
                let c = c.clone();
                Box::pin(async move { offer_lead_conn(conn, &c, to, None, 3).await })
            })
            .await
            .unwrap();
        }
        let offered = db.camp_by_slug("p", "x").await.unwrap().unwrap();
        assert_eq!(offered.lead_offer_thread_id.as_deref(), Some("c"));
        let o = offered.clone();
        let closed = db
            .write(move |conn| {
                let o = o.clone();
                Box::pin(async move { accept_lead_conn(conn, &o, "c", None, 4).await })
            })
            .await;
        assert!(closed.is_err(), "a closed thread cannot take the lead");
        let c = offered.clone();
        db.write(move |conn| {
            let c = c.clone();
            Box::pin(async move { offer_lead_conn(conn, &c, "b", None, 5).await })
        })
        .await
        .unwrap();
        let offered = db.camp_by_slug("p", "x").await.unwrap().unwrap();
        let o = offered.clone();
        db.write(move |conn| {
            let o = o.clone();
            Box::pin(async move { accept_lead_conn(conn, &o, "b", None, 6).await })
        })
        .await
        .unwrap();
        let after = db.camp_by_slug("p", "x").await.unwrap().unwrap();
        assert_eq!(after.lead_thread_id, "b");
        assert_eq!(after.lead_offer_thread_id, None);
        let participants = db.camp_participants(&after).await.unwrap();
        let former = participants.iter().find(|p| p.subject_key == "a").unwrap();
        assert_eq!(former.role, CampRole::Wake, "the former lead stays at wake");
    }

    async fn seed_fallback_led_base_camp(db: &LocalDb) -> Camp {
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, repository_id, created_at, updated_at, is_workspace)
               VALUES ('p', 'default', 'Proj', 'proj', '/tmp/proj', 'p', 1, 1, 0);
             INSERT INTO threads (id, project_id, name, status, created_at, updated_at) VALUES
               ('main', 'p', 'main', 'active', 1, 1), ('other', 'p', 'other', 'active', 2, 2);",
        )
        .await
        .unwrap();
        db.ensure_base_camp("p", "main", 10).await.unwrap().0
    }

    async fn add_general(db: &LocalDb, born: i64) {
        db.execute(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES ('g', 'p', 'general', 'active', ?1, ?1)",
            (born,),
        )
        .await
        .unwrap();
        db.write(|conn| Box::pin(async move { join_base_camp_conn(conn, "p", "g", 20).await }))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_fallback_led_base_camp_moves_its_lead_to_a_general_born_after_it() {
        let db = migrated_test_db("camps-lead-repair").await;
        let camp = seed_fallback_led_base_camp(&db).await;
        add_general(&db, 20).await;

        let displaced = db.repair_base_camp_lead("p", "g", 21).await.unwrap();
        assert_eq!(displaced.as_deref(), Some("main"));
        let after = db.camp_by_slug("p", BASE_CAMP_SLUG).await.unwrap().unwrap();
        assert_eq!(after.lead_thread_id, "g");
        let participants = db.camp_participants(&after).await.unwrap();
        assert_eq!(participants[0].subject_key, "g");
        assert_eq!(participants[0].role, CampRole::Lead);
        let main = participants
            .iter()
            .find(|p| p.subject_key == "main")
            .unwrap();
        assert_eq!(main.role, CampRole::Watch, "the displaced lead watches");
        assert!(!main.lead);

        let c = camp.id.clone();
        let history = db
            .read(move |conn| {
                let c = c.clone();
                Box::pin(async move { camp_history_conn(conn, &c, 1).await })
            })
            .await
            .unwrap();
        assert_eq!(history[0].kind, "lead_changed");
        assert_eq!(history[0].old_value.as_deref(), Some("main"));
        assert_eq!(history[0].new_value.as_deref(), Some("g"));
        assert_eq!(history[0].reason.as_deref(), Some(BASE_LEAD_REPAIR_REASON));

        let events = || async {
            db.query_opt_i64("SELECT COUNT(*) FROM camp_events", ())
                .await
                .unwrap()
        };
        let before = events().await;
        assert_eq!(db.repair_base_camp_lead("p", "g", 22).await.unwrap(), None);
        assert_eq!(events().await, before, "a second repair writes nothing");
    }

    #[tokio::test]
    async fn a_handed_lead_or_a_general_older_than_the_camp_is_left_alone() {
        // A general that existed when the camp was born was passed over
        // deliberately (it was closed); the repair does not second-guess it.
        let db = migrated_test_db("camps-lead-repair-older").await;
        seed_fallback_led_base_camp(&db).await;
        add_general(&db, 5).await;
        assert_eq!(db.repair_base_camp_lead("p", "g", 21).await.unwrap(), None);
        let camp = db.camp_by_slug("p", BASE_CAMP_SLUG).await.unwrap().unwrap();
        assert_eq!(camp.lead_thread_id, "main");

        // A lead that was offered and accepted was chosen.
        let db = migrated_test_db("camps-lead-repair-handed").await;
        let camp = seed_fallback_led_base_camp(&db).await;
        let c = camp.clone();
        db.write(move |conn| {
            let c = c.clone();
            Box::pin(async move { offer_lead_conn(conn, &c, "other", None, 11).await })
        })
        .await
        .unwrap();
        let offered = db.camp_by_slug("p", BASE_CAMP_SLUG).await.unwrap().unwrap();
        db.write(move |conn| {
            let o = offered.clone();
            Box::pin(async move { accept_lead_conn(conn, &o, "other", None, 12).await })
        })
        .await
        .unwrap();
        assert!(db.camp_lead_was_handed(&camp.id).await.unwrap());
        add_general(&db, 20).await;
        assert_eq!(db.repair_base_camp_lead("p", "g", 21).await.unwrap(), None);
        let camp = db.camp_by_slug("p", BASE_CAMP_SLUG).await.unwrap().unwrap();
        assert_eq!(camp.lead_thread_id, "other");
    }
}
