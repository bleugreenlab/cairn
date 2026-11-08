//! The order scopes leave the hot tier in.
//!
//! Pure: no database, no clock. The pack pass packs every resolved execution
//! and every thread range past the hot window; eviction (C2) then removes hot
//! rows in the order this function gives, which is the whole policy, applied
//! to both kinds of scope together:
//!
//! 1. Everything resolved longer ago than the window, oldest first.
//! 2. Then, while the estimated hot bytes left would still exceed the budget,
//!    the oldest of what remains.
//!
//! A scope's resolution is its catalog row's `resolved_at`: an execution's
//! issue resolution, or the closing of a thread range's last session. Ties go
//! by `scope_id`, so the order is total.

/// One scope eligible to leave the hot tier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierCandidate {
    pub scope_id: String,
    /// When it stopped changing, epoch seconds.
    pub resolved_at: i64,
    /// Its catalog row's `hot_bytes_estimate`: what eviction frees.
    pub hot_bytes_estimate: u64,
}

/// The window and budget eviction is held to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HotPolicy {
    /// How long a resolved scope stays hot, in seconds.
    pub window_secs: i64,
    /// The hot bytes the database should hold at most.
    pub budget_bytes: u64,
    /// The estimated hot bytes the database holds now.
    pub hot_total_bytes: u64,
}

/// The scopes to evict, in eviction order.
pub fn eviction_order(candidates: &[TierCandidate], now: i64, policy: &HotPolicy) -> Vec<String> {
    let mut ordered: Vec<&TierCandidate> = candidates.iter().collect();
    ordered.sort_by(|a, b| {
        (a.resolved_at, a.scope_id.as_str()).cmp(&(b.resolved_at, b.scope_id.as_str()))
    });
    let cutoff = now.saturating_sub(policy.window_secs);

    let mut remaining = policy.hot_total_bytes;
    let mut picked = Vec::new();
    let mut rest = Vec::new();
    for candidate in ordered {
        if candidate.resolved_at < cutoff {
            remaining = remaining.saturating_sub(candidate.hot_bytes_estimate);
            picked.push(candidate.scope_id.clone());
        } else {
            rest.push(candidate);
        }
    }
    for candidate in rest {
        if remaining <= policy.budget_bytes {
            break;
        }
        remaining = remaining.saturating_sub(candidate.hot_bytes_estimate);
        picked.push(candidate.scope_id.clone());
    }
    picked
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400;

    /// `rows` at 10 bytes a row, so the arithmetic reads as before.
    fn candidate(id: &str, resolved_at: i64, rows: u64) -> TierCandidate {
        TierCandidate {
            scope_id: id.to_string(),
            resolved_at,
            hot_bytes_estimate: rows * 10,
        }
    }

    fn policy(hot_total_bytes: u64, budget_bytes: u64) -> HotPolicy {
        HotPolicy {
            window_secs: 30 * DAY,
            budget_bytes,
            hot_total_bytes,
        }
    }

    #[test]
    fn nothing_to_order_is_nothing_evicted() {
        assert!(eviction_order(&[], 100 * DAY, &policy(0, 0)).is_empty());
    }

    #[test]
    fn inside_the_window_and_under_budget_evicts_nothing() {
        let now = 100 * DAY;
        let candidates = [
            candidate("a", now - DAY, 5),
            candidate("b", now - 2 * DAY, 5),
        ];
        assert!(eviction_order(&candidates, now, &policy(100, 1_000)).is_empty());
    }

    #[test]
    fn over_budget_evicts_the_oldest_until_under() {
        let now = 100 * DAY;
        let candidates = [
            candidate("newest", now - DAY, 10),
            candidate("oldest", now - 5 * DAY, 10),
            candidate("middle", now - 3 * DAY, 10),
        ];
        // 300 hot against a 150 budget: each eviction frees 100.
        assert_eq!(
            eviction_order(&candidates, now, &policy(300, 150)),
            ["oldest", "middle"]
        );
    }

    #[test]
    fn ties_on_resolution_go_by_scope_id() {
        let now = 100 * DAY;
        let old = now - 40 * DAY;
        let candidates = [
            candidate("b", old, 1),
            candidate("a", old, 1),
            candidate("c", old, 1),
        ];
        assert_eq!(
            eviction_order(&candidates, now, &policy(0, 0)),
            ["a", "b", "c"]
        );
    }

    #[test]
    fn executions_and_thread_ranges_order_together() {
        let now = 100 * DAY;
        let candidates = [
            candidate("exec-new", now - 2 * DAY, 10),
            candidate("job-1/3-7", now - 4 * DAY, 10),
            candidate("exec-old", now - 6 * DAY, 10),
            candidate("job-1/8-9", now - DAY, 10),
        ];
        assert_eq!(
            eviction_order(&candidates, now, &policy(400, 150)),
            ["exec-old", "job-1/3-7", "exec-new"]
        );
    }

    #[test]
    fn the_window_phase_comes_before_the_budget_phase() {
        let now = 100 * DAY;
        let candidates = [
            candidate("recent-big", now - 2 * DAY, 1_000),
            candidate("aged-small", now - 60 * DAY, 1),
            candidate("recent-older", now - 10 * DAY, 1_000),
        ];
        // Aged-out goes first however small; then the budget picks the oldest
        // of the rest, once, which brings 20,010 down to 10,000.
        assert_eq!(
            eviction_order(&candidates, now, &policy(20_010, 10_000)),
            ["aged-small", "recent-older"]
        );
    }
}
