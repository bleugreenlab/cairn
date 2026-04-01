//! Durable mailbox for manager actor wake causes.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::diesel_models::{
    DbManagerMailboxEntry, DbManagerWakeBatch, NewManagerMailboxEntry, NewManagerWakeBatch,
    UpdateManagerMailboxEntryChangeset, UpdateManagerWakeBatchChangeset,
};
use crate::managers::wake::WakeTrigger;
use crate::schema::{manager_mailbox, manager_wake_batches};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManagerDeliveryPolicy {
    CoalesceLatest,
    Append,
    MergeBatch,
    DropIfCompleted,
}

impl std::fmt::Display for ManagerDeliveryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerDeliveryPolicy::CoalesceLatest => write!(f, "coalesce_latest"),
            ManagerDeliveryPolicy::Append => write!(f, "append"),
            ManagerDeliveryPolicy::MergeBatch => write!(f, "merge_batch"),
            ManagerDeliveryPolicy::DropIfCompleted => write!(f, "drop_if_completed"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagerMailboxEntry {
    pub id: String,
    pub manager_id: String,
    pub trigger: WakeTrigger,
    pub delivery_policy: ManagerDeliveryPolicy,
    pub dedupe_key: Option<String>,
    pub priority: i32,
    pub available_at: i64,
    pub created_at: i64,
}

fn db_entry_to_entry(db: DbManagerMailboxEntry) -> Result<ManagerMailboxEntry, String> {
    Ok(ManagerMailboxEntry {
        id: db.id,
        manager_id: db.manager_id,
        trigger: serde_json::from_str(&db.cause_json)
            .map_err(|e| format!("Failed to decode manager mailbox trigger: {}", e))?,
        delivery_policy: match db.delivery_policy.as_str() {
            "coalesce_latest" => ManagerDeliveryPolicy::CoalesceLatest,
            "append" => ManagerDeliveryPolicy::Append,
            "merge_batch" => ManagerDeliveryPolicy::MergeBatch,
            _ => ManagerDeliveryPolicy::DropIfCompleted,
        },
        dedupe_key: db.dedupe_key,
        priority: db.priority,
        available_at: db.available_at as i64,
        created_at: db.created_at as i64,
    })
}

pub fn delivery_policy_for_trigger(trigger: &WakeTrigger) -> ManagerDeliveryPolicy {
    match trigger {
        WakeTrigger::UserMessage { .. } => ManagerDeliveryPolicy::Append,
        WakeTrigger::IssueMerged { .. } | WakeTrigger::IssueFailed { .. } => {
            ManagerDeliveryPolicy::MergeBatch
        }
        WakeTrigger::BranchConflict { .. } | WakeTrigger::MainBranchUpdated { .. } => {
            ManagerDeliveryPolicy::CoalesceLatest
        }
    }
}

pub fn dedupe_key_for_trigger(trigger: &WakeTrigger) -> Option<String> {
    match trigger {
        WakeTrigger::MainBranchUpdated { default_branch, .. } => {
            Some(format!("main-branch-updated:{}", default_branch))
        }
        WakeTrigger::BranchConflict {
            issue_number,
            pr_number,
            ..
        } => Some(format!("branch-conflict:{}:{}", issue_number, pr_number)),
        _ => None,
    }
}

pub fn enqueue_manager_wake(
    conn: &mut SqliteConnection,
    manager_id: &str,
    trigger: &WakeTrigger,
    now: i64,
) -> Result<String, String> {
    let id = Uuid::new_v4().to_string();
    let cause_type = trigger.kind();
    let cause_json = serde_json::to_string(trigger)
        .map_err(|e| format!("Failed to encode manager wake trigger: {}", e))?;
    let delivery_policy = delivery_policy_for_trigger(trigger);
    let dedupe_key = dedupe_key_for_trigger(trigger);

    if let Some(ref dedupe_key) = dedupe_key {
        let pending_ids: Vec<String> = manager_mailbox::table
            .filter(manager_mailbox::manager_id.eq(manager_id))
            .filter(manager_mailbox::dedupe_key.eq(dedupe_key))
            .filter(manager_mailbox::processed_at.is_null())
            .select(manager_mailbox::id)
            .load(conn)
            .map_err(|e| format!("Failed to query dedupe mailbox rows: {}", e))?;

        if !pending_ids.is_empty() {
            diesel::update(manager_mailbox::table.filter(manager_mailbox::id.eq_any(&pending_ids)))
                .set(UpdateManagerMailboxEntryChangeset {
                    superseded_by: Some(Some(id.as_str())),
                    processed_at: Some(Some(now as i32)),
                    ..Default::default()
                })
                .execute(conn)
                .map_err(|e| format!("Failed to coalesce mailbox rows: {}", e))?;
        }
    }

    let new_entry = NewManagerMailboxEntry {
        id: &id,
        manager_id,
        cause_type,
        cause_json: &cause_json,
        delivery_policy: &delivery_policy.to_string(),
        dedupe_key: dedupe_key.as_deref(),
        priority: 0,
        available_at: now as i32,
        created_at: now as i32,
        claimed_at: None,
        processed_at: None,
        superseded_by: None,
        source_run_id: None,
        source_issue_id: None,
        source_project_id: None,
        wake_batch_id: None,
    };

    diesel::insert_into(manager_mailbox::table)
        .values(&new_entry)
        .execute(conn)
        .map_err(|e| format!("Failed to enqueue manager wake: {}", e))?;

    Ok(id)
}

pub fn list_pending_manager_mailbox_entries(
    conn: &mut SqliteConnection,
    manager_id: &str,
    now: i64,
) -> Result<Vec<ManagerMailboxEntry>, String> {
    let rows: Vec<DbManagerMailboxEntry> = manager_mailbox::table
        .filter(manager_mailbox::manager_id.eq(manager_id))
        .filter(manager_mailbox::processed_at.is_null())
        .filter(manager_mailbox::available_at.le(now as i32))
        .order((
            manager_mailbox::priority.desc(),
            manager_mailbox::created_at.asc(),
        ))
        .load(conn)
        .map_err(|e| format!("Failed to list manager mailbox rows: {}", e))?;

    rows.into_iter().map(db_entry_to_entry).collect()
}

pub fn create_wake_batch(
    conn: &mut SqliteConnection,
    manager_id: &str,
    created_at: i64,
) -> Result<String, String> {
    let id = Uuid::new_v4().to_string();
    diesel::insert_into(manager_wake_batches::table)
        .values(&NewManagerWakeBatch {
            id: &id,
            manager_id,
            created_at: created_at as i32,
            completed_at: None,
            outcome: None,
        })
        .execute(conn)
        .map_err(|e| format!("Failed to create manager wake batch: {}", e))?;
    Ok(id)
}

pub fn claim_mailbox_entries(
    conn: &mut SqliteConnection,
    entry_ids: &[String],
    wake_batch_id: &str,
    now: i64,
) -> Result<(), String> {
    if entry_ids.is_empty() {
        return Ok(());
    }

    diesel::update(manager_mailbox::table.filter(manager_mailbox::id.eq_any(entry_ids)))
        .set(UpdateManagerMailboxEntryChangeset {
            claimed_at: Some(Some(now as i32)),
            wake_batch_id: Some(Some(wake_batch_id)),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to claim manager mailbox rows: {}", e))?;

    Ok(())
}

pub fn mark_mailbox_entries_processed(
    conn: &mut SqliteConnection,
    entry_ids: &[String],
    processed_at: i64,
) -> Result<(), String> {
    if entry_ids.is_empty() {
        return Ok(());
    }

    diesel::update(manager_mailbox::table.filter(manager_mailbox::id.eq_any(entry_ids)))
        .set(UpdateManagerMailboxEntryChangeset {
            processed_at: Some(Some(processed_at as i32)),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to mark manager mailbox rows processed: {}", e))?;

    Ok(())
}

pub fn release_mailbox_entries(
    conn: &mut SqliteConnection,
    entry_ids: &[String],
) -> Result<(), String> {
    if entry_ids.is_empty() {
        return Ok(());
    }

    diesel::update(manager_mailbox::table.filter(manager_mailbox::id.eq_any(entry_ids)))
        .set(UpdateManagerMailboxEntryChangeset {
            claimed_at: Some(None),
            wake_batch_id: Some(None),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to release manager mailbox rows: {}", e))?;

    Ok(())
}

pub fn complete_wake_batch(
    conn: &mut SqliteConnection,
    wake_batch_id: &str,
    completed_at: i64,
    outcome: &str,
) -> Result<(), String> {
    diesel::update(manager_wake_batches::table.find(wake_batch_id))
        .set(UpdateManagerWakeBatchChangeset {
            completed_at: Some(Some(completed_at as i32)),
            outcome: Some(Some(outcome)),
        })
        .execute(conn)
        .map_err(|e| format!("Failed to update manager wake batch: {}", e))?;
    Ok(())
}

pub fn load_wake_batch(
    conn: &mut SqliteConnection,
    wake_batch_id: &str,
) -> Result<Option<DbManagerWakeBatch>, String> {
    manager_wake_batches::table
        .find(wake_batch_id)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load manager wake batch: {}", e))
}
