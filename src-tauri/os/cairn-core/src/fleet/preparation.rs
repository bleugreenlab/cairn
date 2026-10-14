//! Machines that could not prepare a project's cells.
//!
//! Every cell for a project runs the project's setup commands before its first
//! command, so a host toolchain the setup runs that the machine does not have
//! fails every batch of that project placed there, whatever the batch was. The
//! executor reports it only on host evidence (a bare-named toolchain its own
//! PATH cannot resolve); a missing branch-owned script never lands here. The
//! placement gate that predicts this from advertised toolchains
//! (`setup_toolchain_deficiency` in the fleet) only sees programs the fleet
//! has a toolchain for; this is the after-the-fact half, for everything else.
//!
//! A refusal is scoped to (executor, project) — the machine stays eligible
//! for every other project — to the project's setup identity (a changed setup
//! is not refused on it), and to the machine's probe-derived environment
//! identity: a re-attach advertising different evidence drops it, identical
//! evidence keeps it (a reconnect is not a repair), and an operator's
//! `repaired` clears the machine's refusals outright.
//!
//! Durable writes go through one ordered writer (see `Fleet::attach_preparation_store`)
//! so a repair can never be overtaken by an earlier recording, and startup
//! hydration never restores a row memory has since recorded or forgotten.

use crate::storage::{LocalDb, RowExt};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

/// One durable change, applied by the writer in the order it was made.
pub(crate) enum PreparationWrite {
    Record {
        executor_id: String,
        refusal: PreparationRefusal,
    },
    Forget {
        executor_id: String,
        projects: Option<Vec<String>>,
    },
    /// Answered once every write enqueued before it has been applied: `Err`
    /// names the first write since the previous barrier that failed, so a
    /// caller never reads a failed write as durable.
    Barrier(oneshot::Sender<Result<(), String>>),
}

/// The runner's refusals, the record of what changed them since the store
/// was read, and the ordered writer that persists them — under one lock, so
/// the order memory changes in is the order the store is written in.
#[derive(Default)]
pub(crate) struct PreparationRefusals {
    /// By executor id, then project id.
    pub(crate) held: HashMap<String, HashMap<String, PreparationRefusal>>,
    /// `(executor, Some(project))` recorded or forgotten, or `(executor, None)`
    /// cleared outright, since the store attached. Hydration never restores a
    /// row for any of these: memory has a newer answer than the store's.
    touched: HashSet<(String, Option<String>)>,
    writer: Option<mpsc::UnboundedSender<PreparationWrite>>,
}

impl PreparationRefusals {
    pub(crate) fn attach(&mut self, writer: mpsc::UnboundedSender<PreparationWrite>) {
        self.writer = Some(writer);
        self.touched.clear();
    }

    pub(crate) fn record(&mut self, executor_id: &str, refusal: PreparationRefusal) {
        self.touched
            .insert((executor_id.to_string(), Some(refusal.project_id.clone())));
        self.held
            .entry(executor_id.to_string())
            .or_default()
            .insert(refusal.project_id.clone(), refusal.clone());
        self.send(PreparationWrite::Record {
            executor_id: executor_id.to_string(),
            refusal,
        });
    }

    /// Forget a machine's refusals: all of them, or only the named projects.
    pub(crate) fn forget(&mut self, executor_id: &str, projects: Option<Vec<String>>) {
        match &projects {
            None => {
                self.touched.insert((executor_id.to_string(), None));
                self.held.remove(executor_id);
            }
            Some(projects) if projects.is_empty() => return,
            Some(projects) => {
                for project_id in projects {
                    self.touched
                        .insert((executor_id.to_string(), Some(project_id.clone())));
                    if let Some(refusals) = self.held.get_mut(executor_id) {
                        refusals.remove(project_id);
                    }
                }
            }
        }
        self.send(PreparationWrite::Forget {
            executor_id: executor_id.to_string(),
            projects,
        });
    }

    /// A receiver answered once everything enqueued so far has been applied,
    /// `Ok(None)` when there is no store, and `Err` when the store's writer
    /// has stopped — which is not the same as having no store.
    pub(crate) fn barrier(&self) -> Result<Option<oneshot::Receiver<Result<(), String>>>, String> {
        let Some(writer) = &self.writer else {
            return Ok(None);
        };
        let (tx, rx) = oneshot::channel();
        writer
            .send(PreparationWrite::Barrier(tx))
            .map(|_| Some(rx))
            .map_err(|_| "the preparation refusal store's writer has stopped".to_string())
    }

    /// Adopt the store's rows, except where memory has moved on since.
    pub(crate) fn hydrate(&mut self, rows: Vec<(String, PreparationRefusal)>) {
        for (executor_id, refusal) in rows {
            let cleared = self.touched.contains(&(executor_id.clone(), None));
            let changed = self
                .touched
                .contains(&(executor_id.clone(), Some(refusal.project_id.clone())));
            if cleared || changed {
                continue;
            }
            self.held
                .entry(executor_id)
                .or_default()
                .entry(refusal.project_id.clone())
                .or_insert(refusal);
        }
    }

    fn send(&self, write: PreparationWrite) {
        if let Some(writer) = &self.writer {
            let _ = writer.send(write);
        }
    }
}

/// Apply writes in order until the fleet drops its sender.
pub(crate) async fn apply_writes(
    db: &LocalDb,
    mut writes: mpsc::UnboundedReceiver<PreparationWrite>,
) {
    let mut failed: Option<String> = None;
    while let Some(write) = writes.recv().await {
        let result = match write {
            PreparationWrite::Record {
                executor_id,
                refusal,
            } => record(db, &executor_id, &refusal).await,
            PreparationWrite::Forget {
                executor_id,
                projects,
            } => forget(db, &executor_id, projects.as_deref()).await,
            PreparationWrite::Barrier(done) => {
                let _ = done.send(failed.take().map_or(Ok(()), Err));
                Ok(())
            }
        };
        if let Err(error) = result {
            log::warn!("{error}");
            failed.get_or_insert(error);
        }
    }
}

/// One machine's recorded failure to prepare one project.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparationRefusal {
    pub(crate) project_id: String,
    /// The machine's environment identity when this was recorded.
    pub(crate) environment_identity: String,
    /// The project's setup identity it failed under.
    pub(crate) setup_identity: String,
    /// The setup command that failed.
    pub(crate) command: String,
    /// The host program it could not find.
    pub(crate) program: String,
    /// The preparation diagnostic as the machine reported it.
    pub(crate) diagnostic: String,
    pub(crate) recorded_at_unix_ms: u64,
}

/// Every recorded refusal, by executor id.
pub(crate) async fn load_all(db: &LocalDb) -> Result<Vec<(String, PreparationRefusal)>, String> {
    db.query_all(
        "SELECT executor_id, project_id, environment_identity, setup_identity, command, program,
                diagnostic, recorded_at
           FROM executor_preparation_refusals",
        (),
        |row| {
            Ok((
                row.text(0)?,
                PreparationRefusal {
                    project_id: row.text(1)?,
                    environment_identity: row.text(2)?,
                    setup_identity: row.text(3)?,
                    command: row.text(4)?,
                    program: row.text(5)?,
                    diagnostic: row.text(6)?,
                    recorded_at_unix_ms: u64::try_from(row.i64(7)?).unwrap_or_default(),
                },
            ))
        },
    )
    .await
    .map_err(|error| format!("load executor preparation refusals: {error}"))
}

/// Record (or replace) one machine's refusal for one project.
pub(crate) async fn record(
    db: &LocalDb,
    executor_id: &str,
    refusal: &PreparationRefusal,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO executor_preparation_refusals
              (executor_id, project_id, environment_identity, setup_identity, command, program,
               diagnostic, recorded_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(executor_id, project_id) DO UPDATE SET
              environment_identity = excluded.environment_identity,
              setup_identity = excluded.setup_identity,
              command = excluded.command,
              program = excluded.program,
              diagnostic = excluded.diagnostic,
              recorded_at = excluded.recorded_at",
        (
            executor_id,
            refusal.project_id.as_str(),
            refusal.environment_identity.as_str(),
            refusal.setup_identity.as_str(),
            refusal.command.as_str(),
            refusal.program.as_str(),
            refusal.diagnostic.as_str(),
            i64::try_from(refusal.recorded_at_unix_ms).unwrap_or(i64::MAX),
        ),
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record executor preparation refusal: {error}"))
}

/// Forget one machine's refusals: all of them, or only the named projects.
pub(crate) async fn forget(
    db: &LocalDb,
    executor_id: &str,
    projects: Option<&[String]>,
) -> Result<(), String> {
    let result = match projects {
        None => {
            db.execute(
                "DELETE FROM executor_preparation_refusals WHERE executor_id = ?1",
                (executor_id,),
            )
            .await
        }
        Some(projects) => {
            for project_id in projects {
                db.execute(
                    "DELETE FROM executor_preparation_refusals
                      WHERE executor_id = ?1 AND project_id = ?2",
                    (executor_id, project_id.as_str()),
                )
                .await
                .map_err(|error| format!("forget executor preparation refusal: {error}"))?;
            }
            return Ok(());
        }
    };
    result
        .map(|_| ())
        .map_err(|error| format!("forget executor preparation refusals: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(project_id: &str, identity: &str) -> PreparationRefusal {
        PreparationRefusal {
            project_id: project_id.into(),
            environment_identity: identity.into(),
            setup_identity: "setup".into(),
            command: "bun install --frozen-lockfile".into(),
            program: "bun".into(),
            diagnostic: "/bin/sh: 1: bun: not found".into(),
            recorded_at_unix_ms: 1_700_000_000_000,
        }
    }

    #[tokio::test]
    async fn refusals_round_trip_replace_and_forget_per_machine() {
        let db = crate::storage::migrated_test_db("preparation-refusals.db").await;
        assert!(load_all(&db).await.unwrap().is_empty());

        record(&db, "bgsrv-ub2", &refusal("cairn", "a"))
            .await
            .unwrap();
        record(&db, "bgsrv-ub2", &refusal("cairn", "b"))
            .await
            .unwrap();
        record(&db, "bgsrv-ub2", &refusal("other", "b"))
            .await
            .unwrap();
        record(&db, "bbs", &refusal("cairn", "c")).await.unwrap();
        let mut rows = load_all(&db).await.unwrap();
        rows.sort_by(|a, b| (&a.0, &a.1.project_id).cmp(&(&b.0, &b.1.project_id)));
        assert_eq!(
            rows,
            vec![
                ("bbs".to_string(), refusal("cairn", "c")),
                ("bgsrv-ub2".to_string(), refusal("cairn", "b")),
                ("bgsrv-ub2".to_string(), refusal("other", "b")),
            ],
            "one row per (machine, project), the latest recording winning"
        );

        forget(&db, "bgsrv-ub2", Some(&["other".to_string()]))
            .await
            .unwrap();
        assert_eq!(load_all(&db).await.unwrap().len(), 2);
        forget(&db, "bgsrv-ub2", None).await.unwrap();
        let rows = load_all(&db).await.unwrap();
        assert_eq!(rows, vec![("bbs".to_string(), refusal("cairn", "c"))]);
    }
}
