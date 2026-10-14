//! Operator-declared machine repairs.
//!
//! Most repairs a machine needs are visible in its own attach-time probes: fix a
//! toolchain, restart the executor, and the measured facts it advertises change
//! — which is what re-arms every check its old state had suppressed. Some
//! repairs no probe can see: a corrupt sccache directory cleared, a helper
//! binary replaced, a permission fixed. This is where an operator says so.
//!
//! The epoch is a counter rather than an instant because what matters is only
//! that it MOVES: it is one component of a machine's repair identity, and a
//! changed identity is what hands every check suppressed on that machine a fresh
//! budget.

use crate::storage::{LocalDb, RowExt};
use std::collections::HashMap;

/// Every machine's current repair epoch, by executor id. A machine with no row
/// has never been declared repaired and reads as `0`.
pub(crate) async fn repair_epochs(db: &LocalDb) -> HashMap<String, i64> {
    db.query_all(
        "SELECT executor_id, repair_epoch FROM executor_repairs",
        (),
        |row| Ok((row.text(0)?, row.i64(1)?)),
    )
    .await
    .unwrap_or_default()
    .into_iter()
    .collect()
}

/// One machine's repair epoch and when it was last declared repaired.
pub(crate) async fn last_repair(db: &LocalDb, executor_id: &str) -> Option<(i64, i64)> {
    db.query_all(
        "SELECT repair_epoch, repaired_at FROM executor_repairs WHERE executor_id = ?1",
        (executor_id.to_string(),),
        |row| Ok((row.i64(0)?, row.i64(1)?)),
    )
    .await
    .ok()?
    .into_iter()
    .next()
}

/// Declare a machine repaired, returning its new epoch.
///
/// Deliberately unfenced: it takes no expected generation, for the same reason
/// draining does not. Requiring a live connection made that control unusable at
/// exactly the moment it mattered (CAIRN-4360), and the same holds here — an
/// operator who has just fixed a machine may well be about to restart its
/// executor, and must be able to say so either way.
pub(crate) async fn mark_repaired(db: &LocalDb, executor_id: &str) -> Result<i64, String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO executor_repairs (executor_id, repair_epoch, repaired_at)
              VALUES (?1, 1, ?2)
         ON CONFLICT(executor_id) DO UPDATE SET
              repair_epoch = executor_repairs.repair_epoch + 1,
              repaired_at = excluded.repaired_at",
        (executor_id.to_string(), now),
    )
    .await
    .map_err(|error| format!("Failed to record executor repair: {error}"))?;
    Ok(last_repair(db, executor_id)
        .await
        .map(|(epoch, _)| epoch)
        .unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_declared_repair_moves_the_machines_epoch() {
        let db = crate::storage::migrated_test_db("executor-repairs.db").await;
        assert!(repair_epochs(&db).await.is_empty());

        assert_eq!(mark_repaired(&db, "machine-a").await.unwrap(), 1);
        assert_eq!(mark_repaired(&db, "machine-a").await.unwrap(), 2);
        assert_eq!(mark_repaired(&db, "machine-b").await.unwrap(), 1);

        let epochs = repair_epochs(&db).await;
        assert_eq!(epochs.get("machine-a"), Some(&2));
        assert_eq!(
            epochs.get("machine-b"),
            Some(&1),
            "one machine's repair says nothing about another's"
        );
    }
}
