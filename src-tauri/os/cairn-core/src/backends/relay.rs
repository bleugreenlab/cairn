//! What a backend is asked when an outside client's request is relayed to it
//! ([`super::AgentBackend::relay_route`]), and why it may refuse.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_models::interface::relay::RelayDoor;

/// One relayed request, as the backend sees it.
pub struct RelayRequest<'a> {
    /// The model as the catalog names it on this backend.
    pub model: &'a str,
    pub door: RelayDoor,
    /// Who the request is made for, as a load queue names it
    /// (`served API client dig`).
    pub requester: &'a str,
    pub wait: RelayWait<'a>,
}

/// How long a backend that must first load the model may keep the client
/// waiting, and how it says what it waits on.
pub struct RelayWait<'a> {
    pub budget: Duration,
    /// Set when the client goes away.
    pub cancel: Arc<AtomicBool>,
    /// Hears each new description of what the request waits on.
    pub waiting: &'a mut (dyn FnMut(&str) + Send),
}

impl RelayWait<'_> {
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }
}

/// Why a backend gave no route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayRefusal {
    /// It cannot serve the request now: no credential, a pool it does not
    /// control is down, or the model failed to load.
    Unavailable(String),
    /// The model is still loading and the client's wait ran out; its request
    /// stays queued.
    WaitExpired(String),
    /// The client went away while it waited.
    Cancelled(String),
}

impl RelayRefusal {
    pub fn message(&self) -> &str {
        match self {
            Self::Unavailable(message) | Self::WaitExpired(message) | Self::Cancelled(message) => {
                message
            }
        }
    }

    /// Which refusal a failed wait was: the client's leaving, its budget, or
    /// the backend's own reason.
    pub(crate) fn of_wait(error: String, wait: &RelayWait<'_>, started: Instant) -> Self {
        if wait.cancelled() {
            Self::Cancelled(error)
        } else if started.elapsed() >= wait.budget {
            Self::WaitExpired(error)
        } else {
            Self::Unavailable(error)
        }
    }
}

impl std::fmt::Display for RelayRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_wait_is_told_apart_by_cancel_then_budget() {
        let mut heard = |_: &str| {};
        let cancel = Arc::new(AtomicBool::new(false));
        let wait = RelayWait {
            budget: Duration::from_secs(60),
            cancel: cancel.clone(),
            waiting: &mut heard,
        };
        let now = Instant::now();
        assert!(matches!(
            RelayRefusal::of_wait("did not load".into(), &wait, now),
            RelayRefusal::Unavailable(_)
        ));
        let long_ago = now - Duration::from_secs(61);
        assert!(matches!(
            RelayRefusal::of_wait("stays queued".into(), &wait, long_ago),
            RelayRefusal::WaitExpired(_)
        ));
        cancel.store(true, Ordering::Relaxed);
        assert!(matches!(
            RelayRefusal::of_wait("stopped waiting".into(), &wait, long_ago),
            RelayRefusal::Cancelled(_)
        ));
    }
}
