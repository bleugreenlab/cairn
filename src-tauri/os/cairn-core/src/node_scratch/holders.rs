//! Who holds a node scratch: the node's live terminals and REPLs.
//!
//! A live terminal or REPL makes the scratch on its machine its `$TMPDIR`. Its
//! writes reach the head when it exits and, while it lives, on the executor's
//! periodic write-back pass, so its machine usually holds the newest state.
//! That is affinity, never a lock (tenets #290, #314): a batch that names no
//! machine is placed beside a holder whose machine can take it, and anywhere
//! else otherwise. No live process ever refuses another: concurrent residents
//! on two machines merge per path at write-back, exactly as concurrent batches
//! do (`node_scratch::merge`).

use cairn_common::executor_protocol::{ResidencyHolder, ScratchSyncEstimate};

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

/// The live processes on each machine, machines in the order their first
/// process was found.
fn by_machine(holders: Vec<ScratchHolder>) -> Vec<(String, Vec<ScratchHolder>)> {
    let mut machines: Vec<(String, Vec<ScratchHolder>)> = Vec::new();
    for holder in holders {
        match machines
            .iter_mut()
            .find(|(executor_id, _)| *executor_id == holder.executor_id)
        {
            Some((_, on_machine)) => on_machine.push(holder),
            None => machines.push((holder.executor_id.clone(), vec![holder])),
        }
    }
    machines
}

/// The holder machine a batch prefers, among those that can take it: the one
/// whose replica is cheapest to bring current (the placement price, bytes
/// missing over measured sync throughput), then the machine holding the
/// batch's own job environment, which spares the batch a detached cell, then
/// the first found.
fn preferred<'a>(
    machines: &'a [(String, Vec<ScratchHolder>)],
    can_take: impl Fn(&str) -> bool,
    price_ms: impl Fn(&str) -> u64,
    own_home: Option<&str>,
) -> Option<&'a (String, Vec<ScratchHolder>)> {
    machines
        .iter()
        .enumerate()
        .filter(|(_, (executor_id, _))| can_take(executor_id))
        .min_by_key(|(order, (executor_id, _))| {
            (
                price_ms(executor_id),
                Some(executor_id.as_str()) != own_home,
                *order,
            )
        })
        .map(|(_, machine)| machine)
}

/// Where a batch that named no machine goes for its node's scratch, and the
/// note its result carries saying so.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Affinity {
    /// The holder machine the batch is placed on, when one can take it.
    pub executor_id: Option<String>,
    /// Why it went there, or why it did not. Rides on every outcome, refusals
    /// included, so a batch that cannot run beside a holder says why it was
    /// sent there.
    pub note: String,
}

pub(crate) fn affinity_among(
    machines: &[(String, Vec<ScratchHolder>)],
    can_take: impl Fn(&str) -> bool,
    price_ms: impl Fn(&str) -> u64,
    own_home: Option<&str>,
    machine_name: impl Fn(&str) -> String,
) -> Option<Affinity> {
    if machines.is_empty() {
        return None;
    }
    if let Some((executor_id, on_machine)) = preferred(machines, can_take, price_ms, own_home) {
        let machine = machine_name(executor_id);
        let holding = named(on_machine);
        return Some(Affinity {
            executor_id: Some(executor_id.clone()),
            note: format!(
                "This batch named no machine and was placed on {machine} beside {holding}, which holds this node's scratch there. That is affinity, not a pin: pass `executor` to run elsewhere, against the scratch as last written back."
            ),
        });
    }
    let holding = machines
        .iter()
        .map(|(executor_id, on_machine)| {
            format!("{} on {}", named(on_machine), machine_name(executor_id))
        })
        .collect::<Vec<_>>()
        .join("; ");
    Some(Affinity {
        executor_id: None,
        note: format!(
            "This node's scratch is held by {holding}, and no machine holding it can take this batch now, so the batch was placed without that affinity and ran against the scratch as last written back."
        ),
    })
}

/// Where a batch of `job_id` that named no machine of its own should go for
/// its node's scratch, when a live process holds the scratch anywhere. A
/// holder's machine can take the batch when
/// [`Fleet::scratch_holder_can_take`](crate::fleet::Fleet::scratch_holder_can_take)
/// says so; beyond that, placement decides as it would for any batch.
pub async fn affinity(
    orch: &Orchestrator,
    db: &LocalDb,
    scratch: &owner::ResolvedScratch,
    job_id: &str,
) -> Option<Affinity> {
    let machines = by_machine(holders(orch, db, &scratch.owner_job_id).await);
    if machines.is_empty() {
        return None;
    }
    let job_holder = ResidencyHolder::Job {
        job_id: job_id.to_owned(),
    };
    // Only priced when two machines compete; one holder needs no estimate.
    let estimate = match machines.len() {
        1 => ScratchSyncEstimate::default(),
        _ => super::placement::estimate(orch, scratch).await,
    };
    let own_home = orch
        .fleet
        .predicted_job_home(&job_holder)
        .map(|home| home.executor_id);
    affinity_among(
        &machines,
        |executor_id| orch.fleet.scratch_holder_can_take(executor_id, &job_holder),
        |executor_id| estimate.ms_for(executor_id),
        own_home.as_deref(),
        |executor_id| orch.fleet.executor_name_for_text(executor_id),
    )
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

    fn name(id: &str) -> String {
        format!("**{id}**")
    }

    fn machines() -> Vec<(String, Vec<ScratchHolder>)> {
        by_machine(vec![
            holder("terminal `dev`", "a"),
            holder("terminal `test`", "b"),
            holder("REPL `py`", "a"),
        ])
    }

    #[test]
    fn holders_group_by_machine_in_the_order_found() {
        let machines = machines();
        assert_eq!(machines.len(), 2);
        assert_eq!(machines[0].0, "a");
        assert_eq!(named(&machines[0].1), "terminal `dev`, REPL `py`");
        assert_eq!(named(&machines[1].1), "terminal `test`");
    }

    #[test]
    fn no_holder_means_no_affinity() {
        assert_eq!(affinity_among(&[], |_| true, |_| 0, None, name), None);
    }

    #[test]
    fn the_warmer_holder_wins_when_both_can_take_the_batch() {
        let machines = machines();
        let price = |id: &str| if id == "a" { 900 } else { 0 };
        let chosen = affinity_among(&machines, |_| true, price, None, name).unwrap();
        assert_eq!(chosen.executor_id.as_deref(), Some("b"));
        assert!(
            chosen
                .note
                .contains("placed on **b** beside terminal `test`, which holds"),
            "{}",
            chosen.note
        );
        assert!(
            chosen.note.contains("affinity, not a pin"),
            "{}",
            chosen.note
        );
    }

    #[test]
    fn equally_warm_holders_prefer_the_batchs_own_environment_then_the_first_found() {
        let machines = machines();
        let own = affinity_among(&machines, |_| true, |_| 0, Some("b"), name).unwrap();
        assert_eq!(own.executor_id.as_deref(), Some("b"));
        let first = affinity_among(&machines, |_| true, |_| 0, None, name).unwrap();
        assert_eq!(first.executor_id.as_deref(), Some("a"));
    }

    #[test]
    fn a_holder_that_cannot_take_the_batch_is_passed_over_never_a_refusal() {
        let machines = machines();
        let price = |id: &str| if id == "a" { 0 } else { 900 };
        let colder = affinity_among(&machines, |id| id == "b", price, None, name).unwrap();
        assert_eq!(colder.executor_id.as_deref(), Some("b"));

        let neither = affinity_among(&machines, |_| false, price, None, name).unwrap();
        assert_eq!(neither.executor_id, None);
        for named in [
            "terminal `dev`, REPL `py` on **a**",
            "terminal `test` on **b**",
            "placed without that affinity",
        ] {
            assert!(neither.note.contains(named), "{named}: {}", neither.note);
        }
    }
}
