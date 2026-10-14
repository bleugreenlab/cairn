//! Shared startup-maintenance admission policy.
//!
//! Hosts keep their serving plane live and feed their own demand signals into
//! this small state machine. Work is admitted only after both the boot grace and
//! an uninterrupted quiet window, and only one operation may hold the gate.

use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct StartupMaintenanceGate {
    booted_at: Instant,
    boot_grace: Duration,
    quiet_window: Duration,
    quiet_since: Option<Instant>,
    running: bool,
}

impl StartupMaintenanceGate {
    pub fn new(boot_grace: Duration, quiet_window: Duration) -> Self {
        Self::new_at(Instant::now(), boot_grace, quiet_window)
    }

    fn new_at(booted_at: Instant, boot_grace: Duration, quiet_window: Duration) -> Self {
        Self {
            booted_at,
            boot_grace,
            quiet_window,
            quiet_since: None,
            running: false,
        }
    }

    /// Observe current demand and return whether one maintenance operation may start.
    /// Demand resets the sustained-quiet clock, including while work is running;
    /// callers use the same demand signal to cooperatively yield the active operation.
    pub fn poll(&mut self, quiet: bool) -> bool {
        self.poll_at(Instant::now(), quiet)
    }

    fn poll_at(&mut self, now: Instant, quiet: bool) -> bool {
        if !quiet {
            self.quiet_since = None;
            return false;
        }
        if now.saturating_duration_since(self.booted_at) < self.boot_grace || self.running {
            return false;
        }
        let quiet_since = self.quiet_since.get_or_insert(now);
        if now.saturating_duration_since(*quiet_since) < self.quiet_window {
            return false;
        }
        self.running = true;
        true
    }

    /// Mark the current operation complete. The next operation requires a new
    /// sustained quiet window, preventing adjacent expensive scans.
    pub fn complete(&mut self) {
        self.running = false;
        self.quiet_since = None;
    }

    pub fn is_running(&self) -> bool {
        self.running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_boot_grace_and_sustained_quiet() {
        let start = Instant::now();
        let mut gate =
            StartupMaintenanceGate::new_at(start, Duration::from_secs(10), Duration::from_secs(5));
        assert!(!gate.poll_at(start + Duration::from_secs(9), true));
        assert!(!gate.poll_at(start + Duration::from_secs(10), true));
        assert!(!gate.poll_at(start + Duration::from_secs(14), true));
        assert!(gate.poll_at(start + Duration::from_secs(15), true));
    }

    #[test]
    fn demand_resets_quiet_and_operations_are_serial() {
        let start = Instant::now();
        let mut gate =
            StartupMaintenanceGate::new_at(start, Duration::ZERO, Duration::from_secs(5));
        assert!(!gate.poll_at(start, true));
        assert!(!gate.poll_at(start + Duration::from_secs(4), false));
        assert!(!gate.poll_at(start + Duration::from_secs(5), true));
        assert!(gate.poll_at(start + Duration::from_secs(10), true));
        assert!(!gate.poll_at(start + Duration::from_secs(20), true));
        gate.complete();
        assert!(!gate.poll_at(start + Duration::from_secs(20), true));
        assert!(gate.poll_at(start + Duration::from_secs(25), true));
    }
}
