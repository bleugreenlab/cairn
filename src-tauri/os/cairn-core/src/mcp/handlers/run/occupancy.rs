//! Which batch occupies each job's execution home, in this runner process.
//!
//! A job's home is one working tree, and its executor runs one command in it
//! at a time: a resident batch holds the residency's operation lock for its
//! whole run, and acquiring the residency takes the same lock. So a second
//! batch that acquired while the first ran used to wait on the executor with no
//! placement decision, no admission request, and nothing anywhere saying what
//! it waited for (cairn/6426). This registry is the runner's own view of that
//! occupancy, so the second batch decides before it acquires:
//!
//! - without `commit_msg` it needs nothing from the home that the branch's
//!   committed tip lacks (the home is restored to HEAD after every batch), so
//!   it runs on a cell of its own instead of waiting;
//! - with `commit_msg` it must commit from the job's own tree, which the
//!   running batch may still move, so it waits here, visibly, for the home.
//!
//! A terminal create, which can only live in the home, is refused naming the
//! occupant rather than left waiting on the same lock.
//!
//! The registry knows only batches this process placed. A batch an earlier
//! process placed that still runs in its home is invisible here, and a batch
//! behind it waits on the executor as before.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

/// The batch running in a job's home.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Occupant {
    pub(crate) request_id: String,
    /// The machine holding the home, once the occupant has acquired it.
    pub(crate) executor_id: Option<String>,
}

/// What a batch that wants a job's home gets.
#[derive(Debug)]
pub(crate) enum HomeClaim {
    /// The home is this batch's until the tenancy drops.
    Tenant(Tenancy),
    /// Another batch is in the home and this one has no `commit_msg`: it runs
    /// on a cell of its own.
    Elsewhere(Occupant),
    /// Another batch is in the home and this one commits: it waits for it.
    Wait(Occupant),
}

static HOMES: LazyLock<Mutex<HashMap<String, Occupant>>> = LazyLock::new(Default::default);
static VACATED: LazyLock<tokio::sync::Notify> = LazyLock::new(tokio::sync::Notify::new);

fn homes() -> std::sync::MutexGuard<'static, HashMap<String, Occupant>> {
    HOMES.lock().unwrap_or_else(|p| p.into_inner())
}

/// Claim job `job_id`'s home for `request_id`, or learn who has it.
pub(crate) fn claim(job_id: &str, request_id: &str, commits: bool) -> HomeClaim {
    let mut homes = homes();
    match homes.get(job_id) {
        Some(occupant) if occupant.request_id != request_id => match commits {
            true => HomeClaim::Wait(occupant.clone()),
            false => HomeClaim::Elsewhere(occupant.clone()),
        },
        _ => {
            homes.insert(
                job_id.to_string(),
                Occupant {
                    request_id: request_id.to_string(),
                    executor_id: None,
                },
            );
            HomeClaim::Tenant(Tenancy {
                job_id: job_id.to_string(),
                request_id: request_id.to_string(),
            })
        }
    }
}

/// The batch in job `job_id`'s home, if any.
pub(crate) fn occupant(job_id: &str) -> Option<Occupant> {
    homes().get(job_id).cloned()
}

/// Wait until job `job_id`'s home is no longer held by anyone but
/// `request_id`, or `bound` passes. The wakeup is registered before the home
/// is read, so a tenancy that ends in between is never missed.
pub(crate) async fn vacancy(job_id: &str, request_id: &str, bound: Duration) {
    let notified = VACATED.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if occupant(job_id).is_none_or(|occupant| occupant.request_id == request_id) {
        return;
    }
    let _ = tokio::time::timeout(bound, notified).await;
}

/// Why a terminal for a job whose home `occupant` is running in was not
/// started. A terminal lives in the home, and acquiring the home waits for the
/// batch in it, so the create answers now instead of after that batch.
pub(crate) fn terminal_refusal(slug: &str, occupant: &Occupant) -> String {
    format!(
        "Could not start terminal `{slug}`: this job's working tree is running batch `{id}` (`cairn:~/batches/{id}`), and the tree runs one thing at a time. Nothing was started. Create the terminal once that batch settles.",
        id = occupant.request_id
    )
}

/// One batch's hold on its job's home. Dropping it vacates the home and wakes
/// every batch waiting for it.
#[derive(Debug)]
pub(crate) struct Tenancy {
    job_id: String,
    request_id: String,
}

impl Tenancy {
    /// Record the machine the home is on, which a batch sent elsewhere is
    /// pinned to so it runs beside the home on a cell of its own.
    pub(crate) fn located(&self, executor_id: &str) {
        if let Some(occupant) = homes()
            .get_mut(&self.job_id)
            .filter(|occupant| occupant.request_id == self.request_id)
        {
            occupant.executor_id = Some(executor_id.to_string());
        }
    }
}

impl Drop for Tenancy {
    fn drop(&mut self) {
        {
            let mut homes = homes();
            if homes
                .get(&self.job_id)
                .is_some_and(|occupant| occupant.request_id == self.request_id)
            {
                homes.remove(&self.job_id);
            }
        }
        VACATED.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(claim: HomeClaim) -> Tenancy {
        match claim {
            HomeClaim::Tenant(tenancy) => tenancy,
            other => panic!("expected the home, got {other:?}"),
        }
    }

    /// The rule, by `commit_msg`: a vacant home is taken at once; an occupied
    /// one sends a commitless batch elsewhere and holds a committing one back,
    /// both naming the occupant; and a vacated home is taken by the waiter.
    #[tokio::test]
    async fn an_occupied_home_sends_a_commitless_batch_elsewhere_and_holds_a_committing_one() {
        let first = tenant(claim("job-rule", "batch-1", true));
        first.located("bgsrv-mac");
        let expected = Occupant {
            request_id: "batch-1".into(),
            executor_id: Some("bgsrv-mac".into()),
        };
        match claim("job-rule", "batch-2", false) {
            HomeClaim::Elsewhere(occupant) => assert_eq!(occupant, expected),
            other => panic!("a commitless batch never waits: {other:?}"),
        }
        match claim("job-rule", "batch-3", true) {
            HomeClaim::Wait(occupant) => assert_eq!(occupant, expected),
            other => panic!("a committing batch waits for the tree: {other:?}"),
        }
        // Another job's home is its own.
        let other_job = tenant(claim("job-other", "batch-x", true));

        let waiter = tokio::spawn(async {
            loop {
                match claim("job-rule", "batch-3", true) {
                    HomeClaim::Tenant(tenancy) => return tenancy,
                    _ => vacancy("job-rule", "batch-3", Duration::from_secs(30)).await,
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "it waits while batch 1 runs");
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("the waiter is woken when the home is vacated")
            .unwrap();
        assert_eq!(occupant("job-rule").unwrap().request_id, "batch-3");
        drop(third);
        assert_eq!(occupant("job-rule"), None);
        drop(other_job);
    }

    /// A tenancy that outlives a re-claim by someone else never evicts them.
    #[test]
    fn a_stale_tenancy_does_not_vacate_another_batchs_home() {
        let first = tenant(claim("job-stale", "batch-1", false));
        homes().insert(
            "job-stale".into(),
            Occupant {
                request_id: "batch-2".into(),
                executor_id: None,
            },
        );
        drop(first);
        assert_eq!(occupant("job-stale").unwrap().request_id, "batch-2");
        homes().remove("job-stale");
    }
}
