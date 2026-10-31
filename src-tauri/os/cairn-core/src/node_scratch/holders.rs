//! Who holds a node scratch's writer: the node's live terminals and REPLs.
//!
//! A live terminal or REPL makes the scratch on its machine its `$TMPDIR` and
//! writes it back only when it exits, so while one lives its machine holds the
//! scratch's newest state. That is the only pin: every batch of every job that
//! shares the scratch, and names no machine of its own, runs beside it, and a
//! second live process on another machine is refused. Refusals name the
//! process, never a "home" (tenet #314).

use cairn_common::executor_protocol::ResidencyHolder;

use super::owner;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// How far below the node its tasks may nest.
const MAX_TASK_DEPTH: usize = 16;

/// One live process holding a node's scratch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScratchHolder {
    /// Named the way an agent addresses it: ``terminal `dev` ``.
    pub process: String,
    pub executor_id: String,
    pub job_id: String,
}

/// The node's own job and every task beneath it: the jobs that share its
/// scratch.
pub async fn sharing_jobs(db: &LocalDb, owner_job_id: &str) -> Vec<String> {
    let mut jobs = vec![owner_job_id.to_owned()];
    let mut frontier = vec![owner_job_id.to_owned()];
    for _ in 0..MAX_TASK_DEPTH {
        let mut next = Vec::new();
        for parent in &frontier {
            let children = db
                .query_all(
                    "SELECT id FROM jobs WHERE parent_job_id = ?1",
                    (parent.clone(),),
                    |row| row.text(0),
                )
                .await
                .unwrap_or_default();
            for child in children {
                // Only a task shares its parent's scratch; anything else under a
                // job is a node of its own.
                let is_task = crate::jobs::queries::home_uri_for_job(db, &child)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|uri| uri.contains("/task/"));
                if is_task && !jobs.contains(&child) {
                    jobs.push(child.clone());
                    next.push(child);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    jobs
}

/// Every live terminal and REPL of the jobs sharing the scratch owned by
/// `owner_job_id`, with the machine each runs on.
pub async fn holders(orch: &Orchestrator, db: &LocalDb, owner_job_id: &str) -> Vec<ScratchHolder> {
    let mut holders = Vec::new();
    for job_id in sharing_jobs(db, owner_job_id).await {
        let terminals = db
            .query_all(
                "SELECT slug FROM job_terminals WHERE job_id = ?1 AND status = 'running'
                 ORDER BY slug",
                (job_id.clone(),),
                |row| row.text(0),
            )
            .await
            .unwrap_or_default();
        let processes: Vec<String> = terminals
            .into_iter()
            .map(|slug| format!("terminal `{slug}`"))
            .chain(
                orch.repl_state
                    .resident_slugs(&job_id)
                    .into_iter()
                    .map(|slug| format!("REPL `{slug}`")),
            )
            .collect();
        if processes.is_empty() {
            continue;
        }
        let Some(home) = orch.fleet.predicted_job_home(&ResidencyHolder::Job {
            job_id: job_id.clone(),
        }) else {
            continue;
        };
        holders.extend(processes.into_iter().map(|process| ScratchHolder {
            process,
            executor_id: home.executor_id.clone(),
            job_id: job_id.clone(),
        }));
    }
    holders
}

/// The processes in `holders` on one machine, as a phrase.
pub fn named(holders: &[ScratchHolder]) -> String {
    match holders {
        [only] => only.process.clone(),
        many => many
            .iter()
            .map(|holder| holder.process.as_str())
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// Refuse a new live process (`starting`, e.g. "terminal") when another
/// machine already holds the scratch's writer. `executor_id` is where the new
/// process will run: its job's home, or `None` when the job has none yet.
pub async fn refuse_second_writer(
    orch: &Orchestrator,
    db: &LocalDb,
    owner_job_id: &str,
    executor_id: Option<&str>,
    starting: &str,
) -> Result<(), String> {
    let elsewhere: Vec<ScratchHolder> = holders(orch, db, owner_job_id)
        .await
        .into_iter()
        .filter(|holder| Some(holder.executor_id.as_str()) != executor_id)
        .collect();
    second_writer_refusal(
        &elsewhere,
        executor_id.map(|id| orch.fleet.executor_name_for_text(id)),
        |id| orch.fleet.executor_name_for_text(id),
        starting,
    )
}

/// The refusal for a new live process given the holders on other machines.
/// A job with no machine yet cannot say where its process would land, so it
/// is refused too, with the way to land beside the holder: any batch of the
/// node is placed there, and that places the job.
fn second_writer_refusal(
    elsewhere: &[ScratchHolder],
    here: Option<String>,
    machine_name: impl Fn(&str) -> String,
    starting: &str,
) -> Result<(), String> {
    let Some(first) = elsewhere.first() else {
        return Ok(());
    };
    let machine = machine_name(&first.executor_id);
    let holding = named(elsewhere);
    Err(match here {
        Some(here) => format!(
            "this node's scratch is held by {holding} running on {machine}, and a second live process on {here} would write it from two machines at once. Stop {holding} first, or start this {starting} beside it"
        ),
        None => format!(
            "this node's scratch is held by {holding} running on {machine}, and this job has no machine yet, so a {starting} started now could land elsewhere and write the scratch from two machines at once. Run any batch first (it is placed beside {holding}), then start this {starting}; or stop {holding}"
        ),
    })
}

/// The note a batch that named no machine carries when it was placed beside
/// the live process holding its node's scratch. It rides on every outcome,
/// refusals included, so a batch that cannot run there says why it was there.
pub fn pin_note(holders: &[ScratchHolder], machine: &str) -> String {
    let holding = named(holders);
    format!(
        "This batch named no machine and was placed on {machine} because {holding} holds this node's scratch there. Pass `executor` to run elsewhere against the scratch as last synced, or stop {holding} to release the pin."
    )
}

/// The machine a batch with no selector of its own is pinned to, if a live
/// process holds the scratch somewhere.
pub async fn pin(
    orch: &Orchestrator,
    db: &LocalDb,
    scratch: &owner::ResolvedScratch,
) -> Option<(String, Vec<ScratchHolder>)> {
    let holders = holders(orch, db, &scratch.owner_job_id).await;
    let executor_id = holders.first()?.executor_id.clone();
    let on_machine = holders
        .into_iter()
        .filter(|holder| holder.executor_id == executor_id)
        .collect();
    Some((executor_id, on_machine))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(process: &str, executor_id: &str) -> ScratchHolder {
        ScratchHolder {
            process: process.into(),
            executor_id: executor_id.into(),
            job_id: "job".into(),
        }
    }

    #[test]
    fn a_job_with_no_machine_yet_is_refused_beside_a_holder_and_told_how_to_land_there() {
        let name = |id: &str| format!("**{id}**");
        let holders = [holder("terminal `dev`", "a")];
        assert_eq!(second_writer_refusal(&[], None, name, "terminal"), Ok(()));
        let refusal = second_writer_refusal(&holders, None, name, "REPL").unwrap_err();
        assert!(
            refusal.contains("terminal `dev` running on **a**"),
            "{refusal}"
        );
        assert!(refusal.contains("Run any batch first"), "{refusal}");
        let refusal =
            second_writer_refusal(&holders, Some("**b**".into()), name, "terminal").unwrap_err();
        assert!(
            refusal.contains("a second live process on **b**"),
            "{refusal}"
        );
    }

    #[test]
    fn the_pin_note_names_the_holding_process_and_the_way_out() {
        let note = pin_note(
            &[holder("terminal `dev`", "a"), holder("REPL `py`", "a")],
            "**a**",
        );
        assert!(
            note.contains("placed on **a** because terminal `dev`, REPL `py` holds"),
            "{note}"
        );
        assert!(note.contains("Pass `executor`"), "{note}");
    }
}
