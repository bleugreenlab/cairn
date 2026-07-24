//! Process-wide fair admission for project checks.
//!
//! Tokio's semaphore is fair, including `acquire_many`: once an exclusive check
//! requests the full capacity, later shared checks queue behind it rather than
//! continuously stealing single permits.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::project_settings::CheckResourceClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckAdmissionSnapshot {
    capacity: usize,
    active_permits: usize,
    pub(crate) queued_requests: usize,
}

pub struct CheckAdmissionController {
    semaphore: Arc<Semaphore>,
    capacity: usize,
    active_permits: Arc<AtomicUsize>,
    queued_requests: Arc<AtomicUsize>,
}

impl Default for CheckAdmissionController {
    fn default() -> Self {
        Self::new(Self::capacity_for_host())
    }
}

impl CheckAdmissionController {
    pub(crate) fn capacity_for_host() -> usize {
        std::thread::available_parallelism()
            .map(|n| (n.get() / 4).clamp(2, 4))
            .unwrap_or(2)
    }

    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
            active_permits: Arc::new(AtomicUsize::new(0)),
            queued_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn snapshot(&self) -> CheckAdmissionSnapshot {
        CheckAdmissionSnapshot {
            capacity: self.capacity,
            active_permits: self.active_permits.load(Ordering::Acquire),
            queued_requests: self.queued_requests.load(Ordering::Acquire),
        }
    }

    pub(crate) async fn acquire(
        &self,
        resource_class: CheckResourceClass,
    ) -> Result<CheckAdmissionPermit, tokio::sync::AcquireError> {
        let permits = match resource_class {
            CheckResourceClass::Shared => 1,
            CheckResourceClass::Exclusive => self.capacity,
        };
        let queued = QueuedRequest::new(self.queued_requests.clone());
        let started = Instant::now();
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(permits as u32)
            .await?;
        drop(queued);
        self.active_permits.fetch_add(permits, Ordering::AcqRel);
        Ok(CheckAdmissionPermit {
            _permit: permit,
            permits,
            active_permits: self.active_permits.clone(),
            waited: started.elapsed(),
        })
    }
}

struct QueuedRequest {
    queued_requests: Arc<AtomicUsize>,
}

impl QueuedRequest {
    fn new(queued_requests: Arc<AtomicUsize>) -> Self {
        queued_requests.fetch_add(1, Ordering::AcqRel);
        Self { queued_requests }
    }
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        self.queued_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct CheckAdmissionPermit {
    _permit: OwnedSemaphorePermit,
    permits: usize,
    active_permits: Arc<AtomicUsize>,
    waited: Duration,
}

impl CheckAdmissionPermit {
    pub(crate) fn waited(&self) -> Duration {
        self.waited
    }
}

impl Drop for CheckAdmissionPermit {
    fn drop(&mut self) {
        self.active_permits
            .fetch_sub(self.permits, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn exclusive_waiter_is_not_starved_by_later_shared_requests() {
        let controller = Arc::new(CheckAdmissionController::new(2));
        let first = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let exclusive = {
            let controller = controller.clone();
            tokio::spawn(async move { controller.acquire(CheckResourceClass::Exclusive).await })
        };
        sleep(Duration::from_millis(10)).await;
        let later = {
            let controller = controller.clone();
            tokio::spawn(async move { controller.acquire(CheckResourceClass::Shared).await })
        };
        drop(first);
        let exclusive_permit = timeout(Duration::from_secs(1), exclusive)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!later.is_finished());
        drop(exclusive_permit);
        timeout(Duration::from_secs(1), later)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_queued_request_leaks_nothing() {
        let controller = Arc::new(CheckAdmissionController::new(1));
        let active = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let queued = {
            let controller = controller.clone();
            tokio::spawn(async move { controller.acquire(CheckResourceClass::Shared).await })
        };
        sleep(Duration::from_millis(10)).await;
        assert_eq!(controller.snapshot().queued_requests, 1);
        queued.abort();
        let _ = queued.await;
        assert_eq!(controller.snapshot().queued_requests, 0);
        drop(active);
        assert_eq!(controller.snapshot().active_permits, 0);
        controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_running_permit_admits_next_request() {
        let controller = Arc::new(CheckAdmissionController::new(1));
        let first = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let next = {
            let controller = controller.clone();
            tokio::spawn(async move { controller.acquire(CheckResourceClass::Shared).await })
        };
        sleep(Duration::from_millis(10)).await;
        assert!(!next.is_finished());
        drop(first);
        timeout(Duration::from_secs(1), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn exclusive_never_overlaps_other_work() {
        let controller = Arc::new(CheckAdmissionController::new(2));
        let shared = controller
            .acquire(CheckResourceClass::Shared)
            .await
            .unwrap();
        let exclusive = {
            let controller = controller.clone();
            tokio::spawn(async move { controller.acquire(CheckResourceClass::Exclusive).await })
        };
        sleep(Duration::from_millis(10)).await;
        assert!(!exclusive.is_finished());
        drop(shared);
        let permit = exclusive.await.unwrap().unwrap();
        assert_eq!(controller.snapshot().active_permits, 2);
        drop(permit);
        assert_eq!(controller.snapshot().active_permits, 0);
    }
}
