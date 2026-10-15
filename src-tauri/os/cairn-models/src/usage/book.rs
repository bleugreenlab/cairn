//! The latest usage snapshot per provider and account.

use std::collections::HashMap;
use std::sync::{LockResult, RwLock, RwLockReadGuard};

use cairn_models_types::usage::ProviderUsageSnapshot;

/// A snapshot's key: the backend, and the account it measures (`None` for a
/// provider that reports once for its single credential).
pub type UsageKey = (String, Option<String>);

/// The in-memory record of what each provider account last reported.
///
/// In-memory only, and deliberately: persisted account health is what survives
/// a restart, and a pool reads both (see [`super::pool`]).
#[derive(Debug, Default)]
pub struct UsageBook {
    snapshots: RwLock<HashMap<UsageKey, ProviderUsageSnapshot>>,
}

impl UsageBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a snapshot, keeping the richer measurement when this one is
    /// poorer. Returns what is now stored when anything changed, `None` when
    /// the snapshot added nothing.
    ///
    /// A coarse live snapshot (Claude `rate_limit_event`, a single status
    /// window) must not overwrite a richer canonical snapshot
    /// (`claude_usage_oauth` / `codex_rate_limits`, the canonical 5-hour +
    /// weekly windows) already recorded, or the panel would flip shape mid-run.
    /// Equal-or-greater rank still updates, so Codex's rich live events and
    /// every manual refresh flow through.
    ///
    /// A failed probe is not a competing measurement of those windows — it is
    /// the status of the most recent attempt. Dropping it outright would leave
    /// the panel presenting the retained capacity as though the refresh had
    /// succeeded, so the richer windows stay and the failure is recorded
    /// against them. The next success replaces the pair wholesale, which is
    /// what clears the failure.
    pub fn record(
        &self,
        account_id: Option<&str>,
        snapshot: ProviderUsageSnapshot,
    ) -> Option<ProviderUsageSnapshot> {
        let key = (snapshot.backend.clone(), account_id.map(str::to_string));
        let mut guard = self.snapshots.write().ok()?;
        let outranked_by = guard
            .get(&key)
            .filter(|existing| snapshot.panel_rank() < existing.panel_rank());
        let stored = match (outranked_by, snapshot.error.as_ref()) {
            (Some(existing), Some(error)) => {
                let mut retained = existing.clone();
                retained.error = Some(error.clone());
                retained.error_kind = snapshot.error_kind;
                retained
            }
            // A poorer result with nothing to report adds nothing.
            (Some(_), None) => return None,
            (None, _) => snapshot,
        };
        guard.insert(key, stored.clone());
        Some(stored)
    }

    /// Every recorded snapshot, for readers that walk or clone the whole book.
    pub fn read(
        &self,
    ) -> LockResult<RwLockReadGuard<'_, HashMap<UsageKey, ProviderUsageSnapshot>>> {
        self.snapshots.read()
    }

    pub fn get(&self, backend: &str, account_id: Option<&str>) -> Option<ProviderUsageSnapshot> {
        self.snapshots
            .read()
            .ok()?
            .get(&(backend.to_string(), account_id.map(str::to_string)))
            .cloned()
    }

    /// A copy of every recorded snapshot.
    pub fn all(&self) -> HashMap<UsageKey, ProviderUsageSnapshot> {
        self.snapshots
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Forget one account's snapshot.
    pub fn remove(&self, backend: &str, account_id: Option<&str>) {
        if let Ok(mut guard) = self.snapshots.write() {
            guard.remove(&(backend.to_string(), account_id.map(str::to_string)));
        }
    }

    /// Forget everything, as a restart would.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.snapshots.write() {
            guard.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::usage::{ProviderUsageScope, ProviderUsageWindow};

    fn canonical(remaining: f64) -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "claude_usage_oauth".to_string(),
            captured_at: 1,
            windows: vec![ProviderUsageWindow {
                id: "session".to_string(),
                label: "Current session".to_string(),
                scope: ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: None,
                reset_at_text: None,
                window_duration_mins: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_failure_is_recorded_against_the_richer_windows_it_could_not_refresh() {
        let book = UsageBook::new();
        book.record(Some("a"), canonical(60.0)).unwrap();
        let stored = book
            .record(
                Some("a"),
                ProviderUsageSnapshot::error("claude", "provider_usage", "probe failed", None),
            )
            .expect("a failure changes what is stored");
        assert_eq!(stored.windows[0].remaining_percent, 60.0);
        assert_eq!(stored.error.as_deref(), Some("probe failed"));
    }

    #[test]
    fn a_poorer_result_with_nothing_to_report_changes_nothing() {
        let book = UsageBook::new();
        book.record(Some("a"), canonical(60.0)).unwrap();
        let poorer = ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "live".to_string(),
            ..Default::default()
        };
        assert!(book.record(Some("a"), poorer).is_none());
        assert_eq!(
            book.get("claude", Some("a")).unwrap().source,
            "claude_usage_oauth"
        );
    }
}
