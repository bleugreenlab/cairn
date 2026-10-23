//! Shared startup-maintenance admission policy.
//!
//! Hosts keep their serving plane live and feed their own demand signals into
//! this small state machine. Work is admitted only after both the boot grace and
//! an uninterrupted quiet window, and only one operation may hold the gate.
//!
//! A host may also bound the deferral. Quiet is opportunistic: on a machine with
//! constant demand it never arrives, so a gate without a bound can hold its work
//! back for the process's whole lifetime. Once the bound has passed since boot,
//! each operation is admitted as soon as no other operation holds the gate.

use std::time::{Duration, Instant};

/// Why the gate admitted an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The boot grace and a sustained quiet window both passed.
    Quiet,
    /// The maximum deferral passed without a quiet window; `since_boot` is how
    /// long after boot the operation was admitted.
    Deadline { since_boot: Duration },
}

#[derive(Debug)]
pub struct StartupMaintenanceGate {
    booted_at: Instant,
    boot_grace: Duration,
    quiet_window: Duration,
    max_deferral: Option<Duration>,
    quiet_since: Option<Instant>,
    running: bool,
}

impl StartupMaintenanceGate {
    /// `max_deferral`, measured from boot, bounds how long quiet may be waited
    /// for. `None` waits for quiet indefinitely, which is only sound for work
    /// that has an independent path to run on a machine that is never quiet.
    pub fn new(
        boot_grace: Duration,
        quiet_window: Duration,
        max_deferral: Option<Duration>,
    ) -> Self {
        Self::new_at(Instant::now(), boot_grace, quiet_window, max_deferral)
    }

    fn new_at(
        booted_at: Instant,
        boot_grace: Duration,
        quiet_window: Duration,
        max_deferral: Option<Duration>,
    ) -> Self {
        Self {
            booted_at,
            boot_grace,
            quiet_window,
            max_deferral,
            quiet_since: None,
            running: false,
        }
    }

    /// Observe current demand and return whether, and why, one maintenance
    /// operation may start. Demand resets the sustained-quiet clock, including
    /// while work is running; callers use the same demand signal to
    /// cooperatively yield the active operation.
    pub fn poll(&mut self, quiet: bool) -> Option<Admission> {
        self.poll_at(Instant::now(), quiet)
    }

    fn poll_at(&mut self, now: Instant, quiet: bool) -> Option<Admission> {
        if !quiet {
            self.quiet_since = None;
        }
        let since_boot = now.saturating_duration_since(self.booted_at);
        if since_boot < self.boot_grace || self.running {
            return None;
        }
        if quiet {
            let quiet_since = *self.quiet_since.get_or_insert(now);
            if now.saturating_duration_since(quiet_since) >= self.quiet_window {
                self.running = true;
                return Some(Admission::Quiet);
            }
        }
        if self
            .max_deferral
            .is_some_and(|max_deferral| since_boot >= max_deferral)
        {
            self.running = true;
            return Some(Admission::Deadline { since_boot });
        }
        None
    }

    /// Mark the current operation complete. Before the deadline, the next
    /// operation requires a new sustained quiet window, preventing adjacent
    /// expensive scans.
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

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn requires_boot_grace_and_sustained_quiet() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(10), secs(5), None);
        assert_eq!(gate.poll_at(start + secs(9), true), None);
        assert_eq!(gate.poll_at(start + secs(10), true), None);
        assert_eq!(gate.poll_at(start + secs(14), true), None);
        assert_eq!(gate.poll_at(start + secs(15), true), Some(Admission::Quiet));
    }

    #[test]
    fn demand_resets_quiet_and_operations_are_serial() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, Duration::ZERO, secs(5), None);
        assert_eq!(gate.poll_at(start, true), None);
        assert_eq!(gate.poll_at(start + secs(4), false), None);
        assert_eq!(gate.poll_at(start + secs(5), true), None);
        assert_eq!(gate.poll_at(start + secs(10), true), Some(Admission::Quiet));
        assert_eq!(gate.poll_at(start + secs(20), true), None);
        gate.complete();
        assert_eq!(gate.poll_at(start + secs(20), true), None);
        assert_eq!(gate.poll_at(start + secs(25), true), Some(Admission::Quiet));
    }

    #[test]
    fn unbounded_gate_never_admits_under_constant_demand() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(10), secs(5), None);
        for t in (0..100_000).step_by(7) {
            assert_eq!(gate.poll_at(start + secs(t), false), None);
        }
    }

    #[test]
    fn constant_demand_admits_at_the_deadline_then_serially() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(60), secs(15), Some(secs(600)));
        for t in 0..600 {
            assert_eq!(gate.poll_at(start + secs(t), false), None, "t={t}");
        }
        assert_eq!(
            gate.poll_at(start + secs(600), false),
            Some(Admission::Deadline {
                since_boot: secs(600)
            })
        );
        // The first unit still holds the gate.
        assert_eq!(gate.poll_at(start + secs(601), false), None);
        assert_eq!(gate.poll_at(start + secs(900), false), None);
        gate.complete();
        assert_eq!(
            gate.poll_at(start + secs(901), false),
            Some(Admission::Deadline {
                since_boot: secs(901)
            })
        );
        assert_eq!(gate.poll_at(start + secs(902), false), None);
        gate.complete();
        assert_eq!(
            gate.poll_at(start + secs(903), false),
            Some(Admission::Deadline {
                since_boot: secs(903)
            })
        );
    }

    #[test]
    fn quiet_before_the_deadline_admits_as_an_unbounded_gate_does() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(60), secs(15), Some(secs(600)));
        assert_eq!(gate.poll_at(start + secs(59), true), None);
        assert_eq!(gate.poll_at(start + secs(60), true), None);
        assert_eq!(gate.poll_at(start + secs(74), true), None);
        assert_eq!(gate.poll_at(start + secs(75), true), Some(Admission::Quiet));
        gate.complete();
        // The next unit needs its own fresh quiet window, and demand resets it.
        assert_eq!(gate.poll_at(start + secs(80), true), None);
        assert_eq!(gate.poll_at(start + secs(90), false), None);
        assert_eq!(gate.poll_at(start + secs(91), true), None);
        assert_eq!(gate.poll_at(start + secs(105), true), None);
        assert_eq!(
            gate.poll_at(start + secs(106), true),
            Some(Admission::Quiet)
        );
    }

    #[test]
    fn running_excludes_a_second_admission_on_either_path() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(60), secs(15), Some(secs(600)));
        assert_eq!(gate.poll_at(start + secs(60), true), None);
        assert_eq!(gate.poll_at(start + secs(75), true), Some(Admission::Quiet));
        assert!(gate.is_running());
        // Neither sustained quiet nor the deadline admits while a unit runs.
        for t in 76..2_000 {
            assert_eq!(gate.poll_at(start + secs(t), true), None, "t={t}");
        }
        for t in 2_000..2_100 {
            assert_eq!(gate.poll_at(start + secs(t), false), None, "t={t}");
        }
        gate.complete();
        assert!(!gate.is_running());
        assert_eq!(
            gate.poll_at(start + secs(2_100), false),
            Some(Admission::Deadline {
                since_boot: secs(2_100)
            })
        );
    }

    #[test]
    fn boot_grace_holds_even_when_the_deadline_is_shorter() {
        let start = Instant::now();
        let mut gate = StartupMaintenanceGate::new_at(start, secs(60), secs(15), Some(secs(30)));
        assert_eq!(gate.poll_at(start + secs(45), false), None);
        assert_eq!(
            gate.poll_at(start + secs(60), false),
            Some(Admission::Deadline {
                since_boot: secs(60)
            })
        );
    }
}
