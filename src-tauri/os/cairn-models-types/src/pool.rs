//! One provider's capacity, pooled across every account signed into it.
//!
//! Every account's segment of every window family sits beside the family's
//! pooled total, so one snapshot answers both "how much of this provider is
//! left" and "how much does this account have". This module is pure
//! aggregation over per-account measurements, with no knowledge of where those
//! measurements came from.
//!
//! Two invariants carry the weight here. Missing data is *unknown* and never
//! silently becomes capacity. And exhaustion is computed per window family with
//! its own reach, never collapsed into one provider-wide verdict, because a
//! spent model-scoped window stops that model while leaving every other model
//! perfectly launchable.

use serde::{Deserialize, Serialize};

use crate::usage::{
    ProviderCreditsSnapshot, ProviderModelUsageRow, ProviderUsageErrorKind, ProviderUsageFamily,
    ProviderUsageResetCredits, ProviderUsageScope, ProviderUsageWindow,
};

/// One provider's pooled capacity as the settings surface renders it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolSnapshot {
    pub backend: String,
    /// Every account in the pool, in the order settings lists them.
    pub accounts: Vec<ProviderPoolAccount>,
    /// The families this provider's display policy shows, one section each.
    pub families: Vec<ProviderPoolFamily>,
    /// Every family the pool is currently exhausted on, each carrying how far
    /// its exhaustion actually reaches.
    pub constraints: Vec<ProviderPoolConstraint>,
    pub credits: Option<ProviderCreditsSnapshot>,
    pub model_breakdown: Option<Vec<ProviderModelUsageRow>>,
    pub unsupported_reason: Option<String>,
    /// The OLDEST contributing capture, so "Updated" never overstates freshness.
    pub captured_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolAccount {
    pub account_id: String,
    pub label: String,
    pub state: ProviderPoolAccountState,
    /// Which measurement produced this account's numbers, or None when they
    /// came from persisted health rather than a live snapshot. The card uses it
    /// to decide whether the provider is still worth asking on open.
    pub source: Option<String>,
    pub captured_at: Option<i64>,
    pub blocked_until: Option<i64>,
    pub error: Option<String>,
    pub error_kind: Option<ProviderUsageErrorKind>,
    /// The usage resets this account holds, for a provider that has them.
    /// A reset is spent from one account, so the account is where it lives.
    pub reset_credits: Option<ProviderPoolAccountResets>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolAccountResets {
    pub available_count: i64,
    /// The soonest expiry among the account's credits, or None when no credit
    /// reports one.
    pub soonest_expires_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPoolAccountState {
    Measured,
    Unknown,
    Blocked,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolFamily {
    pub key: String,
    pub label: String,
    /// The short name a row or column under the provider uses: `Session`,
    /// `Week`, `Fable`, `5-hour`.
    pub name: String,
    pub scope: ProviderUsageScope,
    pub scope_target: Option<String>,
    /// 100 x pool size. The denominator the UI renders.
    pub capacity_percent: f64,
    /// Sum of measured segments only. Unknown adds nothing.
    pub remaining_percent: f64,
    pub unknown_count: i64,
    /// One per pool account, in the same order as `accounts`.
    pub segments: Vec<ProviderPoolSegment>,
    /// Soonest reset among measured segments, and whether they disagree.
    pub next_reset_at: Option<i64>,
    pub resets_differ: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolSegment {
    pub account_id: String,
    /// None means unknown. NEVER defaulted to 100.
    pub remaining_percent: Option<f64>,
    pub state: ProviderPoolSegmentState,
    pub resets_at: Option<i64>,
    pub reset_at_text: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPoolSegmentState {
    Measured,
    /// Blocked accounts contribute 0, not unknown: the provider told us the
    /// allowance cannot be drawn right now, which is a measurement.
    Blocked,
    /// No snapshot, or the account's window for this family has rolled over
    /// since it was measured. A rolled-over window is history, and the new
    /// allowance has not been observed.
    Unknown,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderPoolConstraint {
    /// The exhausted family, or None when every account is blocked outright
    /// and no single window is at fault.
    pub family_key: Option<String>,
    pub family_label: Option<String>,
    pub reach: ProviderPoolConstraintReach,
    /// Some("Fable") for a model-scoped family; None for a global one.
    pub scope_target: Option<String>,
    pub next_reset_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPoolConstraintReach {
    /// No `scope_target`: this family constrains every model on the provider.
    Provider,
    /// Constrains only models matching `scope_target`. An exhausted Fable
    /// window must not disqualify Opus.
    Model,
}

/// One account's contribution to the pool, as the caller resolved it.
#[derive(Debug, Clone, Default)]
pub struct PoolAccountInput {
    pub account_id: String,
    pub label: String,
    pub source: Option<String>,
    pub windows: Vec<ProviderUsageWindow>,
    pub windows_captured_at: Option<i64>,
    pub blocked_until: Option<i64>,
    pub error: Option<String>,
    pub error_kind: Option<ProviderUsageErrorKind>,
    pub reset_credits: Option<ProviderUsageResetCredits>,
}

impl PoolAccountInput {
    fn resets(&self) -> Option<ProviderPoolAccountResets> {
        self.reset_credits
            .as_ref()
            .map(|credits| ProviderPoolAccountResets {
                available_count: credits.available_count,
                soonest_expires_at: credits
                    .credits
                    .iter()
                    .filter_map(|credit| credit.expires_at)
                    .min(),
            })
    }

    fn is_blocked(&self, now: i64) -> bool {
        self.blocked_until.is_some_and(|until| until > now)
    }

    fn live_windows(&self, now: i64) -> impl Iterator<Item = &ProviderUsageWindow> {
        let captured_at = self.windows_captured_at.unwrap_or(0);
        self.windows
            .iter()
            .filter(move |window| window.is_live_at(captured_at, now))
    }

    fn state(&self, now: i64) -> ProviderPoolAccountState {
        if self.is_blocked(now) {
            ProviderPoolAccountState::Blocked
        } else if self.error.is_some() {
            ProviderPoolAccountState::Error
        } else if self.live_windows(now).next().is_some() {
            ProviderPoolAccountState::Measured
        } else {
            ProviderPoolAccountState::Unknown
        }
    }

    /// This account's segment of one family.
    ///
    /// Every account contributes exactly one segment to every family the pool
    /// knows about. An account with nothing to say about a family says
    /// `Unknown` rather than dropping out of it, because a missing segment and
    /// a full one are indistinguishable once the bar is drawn.
    ///
    /// A segment answers what is known about this account's capacity, which is
    /// not the same question the account's own `state` answers. An account
    /// whose last probe failed can still hold windows a previous probe
    /// measured, and reporting those as unknown would fabricate ignorance the
    /// same way an implied full segment fabricates capacity. The failure is
    /// reported once, on the account.
    fn segment_for(&self, family: &ProviderUsageFamily, now: i64) -> ProviderPoolSegment {
        // A block is the provider's own statement that this account cannot be
        // drawn from, so it outranks both a stale window and a failed probe.
        if self.is_blocked(now) {
            return ProviderPoolSegment {
                account_id: self.account_id.clone(),
                remaining_percent: Some(0.0),
                state: ProviderPoolSegmentState::Blocked,
                resets_at: self.blocked_until,
                reset_at_text: None,
            };
        }
        if let Some(window) = self
            .live_windows(now)
            .find(|window| window.family().key == family.key)
        {
            return ProviderPoolSegment {
                account_id: self.account_id.clone(),
                remaining_percent: Some(window.remaining_percent),
                state: ProviderPoolSegmentState::Measured,
                resets_at: window.resets_at,
                reset_at_text: window.reset_at_text.clone(),
            };
        }
        ProviderPoolSegment {
            account_id: self.account_id.clone(),
            remaining_percent: None,
            state: match self.error {
                Some(_) => ProviderPoolSegmentState::Error,
                None => ProviderPoolSegmentState::Unknown,
            },
            resets_at: None,
            reset_at_text: None,
        }
    }
}

/// Aggregate per-account measurements into one provider pool.
///
/// Every family any account reports live is in the pool, Codex's five-hour
/// window included: each account's line and the usage table show every
/// window the provider measures, so none is filtered here.
// The provider-level passthroughs (credits, breakdown, unsupported reason) are
// separate measurements a backend may report beside its windows, not facets of
// one object; bundling them purely to shorten this signature would make callers
// build a bag whose fields have no relationship to each other.
pub fn build_provider_pool(
    backend: &str,
    inputs: Vec<PoolAccountInput>,
    credits: Option<ProviderCreditsSnapshot>,
    model_breakdown: Option<Vec<ProviderModelUsageRow>>,
    unsupported_reason: Option<String>,
    now: i64,
) -> ProviderPoolSnapshot {
    let accounts: Vec<ProviderPoolAccount> = inputs
        .iter()
        .map(|input| ProviderPoolAccount {
            account_id: input.account_id.clone(),
            label: input.label.clone(),
            state: input.state(now),
            source: input.source.clone(),
            captured_at: input.windows_captured_at,
            blocked_until: input.blocked_until.filter(|until| *until > now),
            error: input.error.clone(),
            error_kind: input.error_kind,
            reset_credits: input.resets(),
        })
        .collect();

    // A family exists in the pool if any account reports it live. Discovery
    // preserves first-seen order so Claude's session/weekly/scoped sections keep
    // the order the provider itself reported them in.
    let mut discovered: Vec<ProviderUsageFamily> = Vec::new();
    for input in &inputs {
        for window in input.live_windows(now) {
            let family = window.family();
            if !discovered.iter().any(|seen| seen.key == family.key) {
                discovered.push(family);
            }
        }
    }

    let capacity_percent = 100.0 * inputs.len() as f64;
    let all_blocked = !inputs.is_empty() && inputs.iter().all(|input| input.is_blocked(now));

    let mut families = Vec::new();
    let mut constraints = Vec::new();
    for family in &discovered {
        let segments: Vec<ProviderPoolSegment> = inputs
            .iter()
            .map(|input| input.segment_for(family, now))
            .collect();
        let remaining_percent: f64 = segments.iter().filter_map(|s| s.remaining_percent).sum();
        let unknown_count = segments
            .iter()
            .filter(|s| s.remaining_percent.is_none())
            .count() as i64;
        let mut measured_resets: Vec<i64> = segments
            .iter()
            .filter(|s| s.state == ProviderPoolSegmentState::Measured)
            .filter_map(|s| s.resets_at)
            .collect();
        measured_resets.sort_unstable();
        measured_resets.dedup();

        // The same predicate routing applies: an account is out of this family
        // when it is blocked, or when its live window for the family measures
        // zero. An Unknown or Error segment defeats the claim, because routing
        // would still try that account and might well succeed.
        let exhausted = !segments.is_empty()
            && segments.iter().all(|segment| match segment.state {
                ProviderPoolSegmentState::Blocked => true,
                ProviderPoolSegmentState::Measured => {
                    segment.remaining_percent.is_some_and(|value| value <= 0.0)
                }
                ProviderPoolSegmentState::Unknown | ProviderPoolSegmentState::Error => false,
            });
        // When every account is blocked no single window is at fault, so the
        // pool speaks once below rather than blaming each family in turn.
        if exhausted && !all_blocked {
            constraints.push(ProviderPoolConstraint {
                family_key: Some(family.key.clone()),
                family_label: Some(family.label.clone()),
                reach: match family.scope_target {
                    Some(_) => ProviderPoolConstraintReach::Model,
                    None => ProviderPoolConstraintReach::Provider,
                },
                scope_target: family.scope_target.clone(),
                next_reset_at: segments.iter().filter_map(|s| s.resets_at).min(),
            });
        }

        families.push(ProviderPoolFamily {
            key: family.key.clone(),
            label: family.label.clone(),
            name: family.name.clone(),
            scope: family.scope,
            scope_target: family.scope_target.clone(),
            capacity_percent,
            remaining_percent,
            unknown_count,
            segments,
            next_reset_at: measured_resets.first().copied(),
            resets_differ: measured_resets.len() > 1,
        });
    }

    if all_blocked {
        constraints.push(ProviderPoolConstraint {
            family_key: None,
            family_label: None,
            reach: ProviderPoolConstraintReach::Provider,
            scope_target: None,
            next_reset_at: inputs.iter().filter_map(|input| input.blocked_until).min(),
        });
    }

    ProviderPoolSnapshot {
        backend: backend.to_string(),
        accounts,
        families,
        constraints,
        credits,
        model_breakdown,
        unsupported_reason,
        // An account still measuring on stale data is what "Updated" has to
        // speak for, so the oldest capture wins over the newest.
        captured_at: inputs
            .iter()
            .filter_map(|input| input.windows_captured_at)
            .filter(|at| *at > 0)
            .min(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::ProviderUsageResetCredit;

    const NOW: i64 = 1_000;
    const LATER: i64 = 9_000;

    fn window(
        id: &str,
        scope: ProviderUsageScope,
        scope_target: Option<&str>,
        remaining_percent: f64,
        resets_at: Option<i64>,
    ) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: id.to_string(),
            label: id.to_string(),
            scope,
            scope_target: scope_target.map(str::to_string),
            used_percent: 100.0 - remaining_percent,
            remaining_percent,
            resets_at,
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    /// The three windows Claude reports, with a per-window remaining value.
    fn claude_account(id: &str, session: f64, weekly: f64, fable: f64) -> PoolAccountInput {
        PoolAccountInput {
            account_id: id.to_string(),
            label: format!("{id}@example.com"),
            windows: vec![
                window(
                    "session",
                    ProviderUsageScope::Session,
                    None,
                    session,
                    Some(LATER),
                ),
                window(
                    "weekly_all",
                    ProviderUsageScope::Weekly,
                    None,
                    weekly,
                    Some(LATER),
                ),
                window(
                    "weekly_scoped-fable",
                    ProviderUsageScope::Weekly,
                    Some("Fable"),
                    fable,
                    Some(LATER),
                ),
            ],
            windows_captured_at: Some(NOW - 60),
            ..Default::default()
        }
    }

    fn codex_account(id: &str, rolling: f64, weekly: f64) -> PoolAccountInput {
        let mut rolling_window = window(
            "primary",
            ProviderUsageScope::RollingWindow,
            None,
            rolling,
            Some(LATER),
        );
        rolling_window.window_duration_mins = Some(300);
        let mut weekly_window = window(
            "secondary",
            ProviderUsageScope::Weekly,
            None,
            weekly,
            Some(LATER),
        );
        weekly_window.window_duration_mins = Some(10_080);
        PoolAccountInput {
            account_id: id.to_string(),
            label: format!("{id}@example.com"),
            windows: vec![rolling_window, weekly_window],
            windows_captured_at: Some(NOW - 60),
            ..Default::default()
        }
    }

    fn pool(backend: &str, inputs: Vec<PoolAccountInput>) -> ProviderPoolSnapshot {
        build_provider_pool(backend, inputs, None, None, None, NOW)
    }

    fn family<'a>(pool: &'a ProviderPoolSnapshot, key: &str) -> &'a ProviderPoolFamily {
        pool.families
            .iter()
            .find(|family| family.key == key)
            .unwrap_or_else(|| panic!("family {key} present"))
    }

    #[test]
    fn two_claude_accounts_pool_each_family_separately() {
        let pool = pool(
            "claude",
            vec![
                claude_account("a", 60.0, 90.0, 20.0),
                claude_account("b", 40.0, 30.0, 80.0),
            ],
        );

        assert_eq!(
            pool.families
                .iter()
                .map(|family| family.key.as_str())
                .collect::<Vec<_>>(),
            vec!["session", "weekly", "weekly:fable"]
        );
        assert_eq!(
            pool.families
                .iter()
                .map(|family| family.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Session", "Week", "Fable"]
        );
        for family in &pool.families {
            assert_eq!(family.capacity_percent, 200.0);
            assert_eq!(family.segments.len(), 2);
            assert_eq!(family.unknown_count, 0);
        }
        assert_eq!(family(&pool, "session").remaining_percent, 100.0);
        assert_eq!(family(&pool, "weekly").remaining_percent, 120.0);
        // A model-scoped window pools with the same model, never with the
        // global weekly allowance.
        assert_eq!(family(&pool, "weekly:fable").remaining_percent, 100.0);
        assert_eq!(
            family(&pool, "weekly:fable").scope_target.as_deref(),
            Some("Fable")
        );
    }

    #[test]
    fn codex_accounts_each_carry_their_five_hour_and_weekly_windows() {
        let pool = pool(
            "codex",
            vec![
                codex_account("a", 82.0, 70.0),
                codex_account("b", 10.0, 50.0),
            ],
        );

        assert_eq!(
            pool.families
                .iter()
                .map(|family| (family.key.as_str(), family.name.as_str()))
                .collect::<Vec<_>>(),
            vec![("rolling:300", "5-hour"), ("weekly", "Week")]
        );
        // Each account's own five-hour number, on its own segment.
        let five_hour = family(&pool, "rolling:300");
        assert_eq!(
            five_hour
                .segments
                .iter()
                .map(|segment| (segment.account_id.as_str(), segment.remaining_percent))
                .collect::<Vec<_>>(),
            vec![("a", Some(82.0)), ("b", Some(10.0))]
        );
        assert_eq!(family(&pool, "weekly").remaining_percent, 120.0);
    }

    #[test]
    fn differing_resets_stay_on_their_own_segments() {
        let mut first = claude_account("a", 60.0, 90.0, 20.0);
        first.windows[0].resets_at = Some(5_000);
        let mut second = claude_account("b", 40.0, 30.0, 80.0);
        second.windows[0].resets_at = Some(7_000);
        let pool = pool("claude", vec![first, second]);

        let session = family(&pool, "session");
        assert_eq!(session.segments[0].resets_at, Some(5_000));
        assert_eq!(session.segments[1].resets_at, Some(7_000));
        assert_eq!(session.next_reset_at, Some(5_000));
        assert!(session.resets_differ);
        // The weekly family shares one reset, so it makes no such claim.
        assert!(!family(&pool, "weekly").resets_differ);
    }

    #[test]
    fn a_missing_snapshot_is_unknown_and_never_fabricates_capacity() {
        let unmeasured = PoolAccountInput {
            account_id: "b".to_string(),
            label: "b@example.com".to_string(),
            ..Default::default()
        };
        let pool = pool(
            "claude",
            vec![claude_account("a", 60.0, 90.0, 20.0), unmeasured],
        );

        let session = family(&pool, "session");
        assert_eq!(session.capacity_percent, 200.0);
        assert_eq!(session.unknown_count, 1);
        assert_eq!(session.segments[1].state, ProviderPoolSegmentState::Unknown);
        assert_eq!(session.segments[1].remaining_percent, None);
        // The measured account's own value, and emphatically not 100 more.
        assert_eq!(session.remaining_percent, 60.0);
        assert_eq!(pool.accounts[1].state, ProviderPoolAccountState::Unknown);
    }

    #[test]
    fn one_probe_error_coexists_with_another_accounts_capacity() {
        let failed = PoolAccountInput {
            account_id: "b".to_string(),
            label: "b@example.com".to_string(),
            error: Some("probe failed".to_string()),
            error_kind: Some(ProviderUsageErrorKind::UsageProbeAuthentication),
            ..Default::default()
        };
        let pool = pool(
            "claude",
            vec![claude_account("a", 60.0, 90.0, 20.0), failed],
        );

        let session = family(&pool, "session");
        assert_eq!(session.remaining_percent, 60.0);
        assert_eq!(session.unknown_count, 1);
        assert_eq!(session.segments[1].state, ProviderPoolSegmentState::Error);
        assert_eq!(pool.accounts[1].state, ProviderPoolAccountState::Error);
        assert_eq!(
            pool.accounts[1].error_kind,
            Some(ProviderUsageErrorKind::UsageProbeAuthentication)
        );
    }

    #[test]
    fn a_failed_refresh_reports_itself_without_discarding_what_was_measured() {
        let mut stale = claude_account("a", 60.0, 90.0, 20.0);
        stale.error = Some("Timed out waiting for claude usage data.".to_string());
        let pool = pool("claude", vec![stale, claude_account("b", 40.0, 30.0, 80.0)]);

        // The account says its last attempt failed — that is what drives the
        // error banner and the re-authenticate affordance.
        assert_eq!(pool.accounts[0].state, ProviderPoolAccountState::Error);
        assert_eq!(
            pool.accounts[0].error.as_deref(),
            Some("Timed out waiting for claude usage data.")
        );
        // Its windows are older, not wrong, so the bar still shows them rather
        // than pretending the account's capacity is unknown.
        let session = family(&pool, "session");
        assert_eq!(
            session.segments[0].state,
            ProviderPoolSegmentState::Measured
        );
        assert_eq!(session.remaining_percent, 100.0);
        assert_eq!(session.unknown_count, 0);
    }

    #[test]
    fn a_failure_with_nothing_measured_is_an_error_segment() {
        let failed = PoolAccountInput {
            account_id: "b".to_string(),
            label: "b@example.com".to_string(),
            error: Some("probe failed".to_string()),
            ..Default::default()
        };
        let pool = pool(
            "claude",
            vec![claude_account("a", 60.0, 90.0, 20.0), failed],
        );
        let session = family(&pool, "session");
        assert_eq!(session.segments[1].state, ProviderPoolSegmentState::Error);
        assert_eq!(session.unknown_count, 1);
    }

    #[test]
    fn a_rolled_over_window_is_unknown_rather_than_its_old_number() {
        let mut stale = claude_account("a", 0.0, 90.0, 20.0);
        stale.windows[0].resets_at = Some(NOW - 1);
        let pool = pool("claude", vec![stale]);

        // Nothing reports a live session window, so the family is gone entirely
        // rather than standing at the exhausted number it used to hold.
        assert!(pool.families.iter().all(|family| family.key != "session"));
        assert_eq!(family(&pool, "weekly").remaining_percent, 90.0);
    }

    #[test]
    fn a_rolled_over_window_beside_a_live_one_is_an_unknown_segment() {
        let mut stale = claude_account("a", 0.0, 90.0, 20.0);
        stale.windows[0].resets_at = Some(NOW - 1);
        let pool = pool("claude", vec![stale, claude_account("b", 45.0, 30.0, 80.0)]);

        let session = family(&pool, "session");
        assert_eq!(session.segments[0].state, ProviderPoolSegmentState::Unknown);
        assert_eq!(session.remaining_percent, 45.0);
        assert_eq!(session.unknown_count, 1);
    }

    #[test]
    fn a_blocked_account_contributes_zero_not_unknown() {
        let mut blocked = claude_account("b", 90.0, 90.0, 90.0);
        blocked.blocked_until = Some(LATER);
        let pool = pool(
            "claude",
            vec![claude_account("a", 60.0, 90.0, 20.0), blocked],
        );

        let session = family(&pool, "session");
        assert_eq!(session.segments[1].state, ProviderPoolSegmentState::Blocked);
        assert_eq!(session.segments[1].remaining_percent, Some(0.0));
        assert_eq!(session.unknown_count, 0);
        assert_eq!(session.remaining_percent, 60.0);
        assert_eq!(pool.accounts[1].blocked_until, Some(LATER));
    }

    #[test]
    fn the_pool_reports_its_oldest_capture_not_its_newest() {
        let mut old = claude_account("a", 60.0, 90.0, 20.0);
        old.windows_captured_at = Some(500);
        let mut fresh = claude_account("b", 40.0, 30.0, 80.0);
        fresh.windows_captured_at = Some(900);
        assert_eq!(pool("claude", vec![old, fresh]).captured_at, Some(500));
    }

    #[test]
    fn fable_exhaustion_constrains_only_fable() {
        let pool = pool(
            "claude",
            vec![
                claude_account("a", 60.0, 90.0, 0.0),
                claude_account("b", 40.0, 30.0, 0.0),
            ],
        );

        // Global capacity is untouched: Opus is still perfectly launchable.
        assert_eq!(family(&pool, "session").remaining_percent, 100.0);
        assert_eq!(family(&pool, "weekly").remaining_percent, 120.0);
        assert_eq!(family(&pool, "weekly:fable").remaining_percent, 0.0);

        assert_eq!(pool.constraints.len(), 1);
        let constraint = &pool.constraints[0];
        assert_eq!(constraint.family_key.as_deref(), Some("weekly:fable"));
        assert_eq!(constraint.reach, ProviderPoolConstraintReach::Model);
        assert_eq!(constraint.scope_target.as_deref(), Some("Fable"));
        // Claiming the provider is out would contradict the two families above.
        assert!(pool
            .constraints
            .iter()
            .all(|c| c.reach != ProviderPoolConstraintReach::Provider));
    }

    #[test]
    fn an_exhausted_codex_five_hour_window_constrains_the_whole_provider() {
        let pool = pool(
            "codex",
            vec![codex_account("a", 0.0, 70.0), codex_account("b", 0.0, 50.0)],
        );

        assert_eq!(pool.constraints.len(), 1);
        let constraint = &pool.constraints[0];
        assert_eq!(constraint.family_key.as_deref(), Some("rolling:300"));
        assert_eq!(constraint.reach, ProviderPoolConstraintReach::Provider);
        assert_eq!(constraint.next_reset_at, Some(LATER));
    }

    #[test]
    fn an_unknown_segment_defeats_an_exhaustion_claim() {
        let unmeasured = PoolAccountInput {
            account_id: "b".to_string(),
            label: "b@example.com".to_string(),
            ..Default::default()
        };
        let pool = pool(
            "claude",
            vec![claude_account("a", 0.0, 0.0, 0.0), unmeasured],
        );

        // Routing would still try the unmeasured account and might succeed, so
        // asserting exhaustion would fabricate a negative.
        assert!(pool.constraints.is_empty());
    }

    #[test]
    fn every_account_blocked_speaks_once_rather_than_once_per_family() {
        let mut first = claude_account("a", 60.0, 90.0, 20.0);
        first.blocked_until = Some(4_000);
        let mut second = claude_account("b", 40.0, 30.0, 80.0);
        second.blocked_until = Some(6_000);
        let pool = pool("claude", vec![first, second]);

        assert_eq!(pool.constraints.len(), 1);
        assert_eq!(pool.constraints[0].family_key, None);
        assert_eq!(
            pool.constraints[0].reach,
            ProviderPoolConstraintReach::Provider
        );
        assert_eq!(pool.constraints[0].next_reset_at, Some(4_000));
    }

    #[test]
    fn two_exhausted_global_families_produce_two_constraints() {
        let mut first = claude_account("a", 0.0, 0.0, 50.0);
        first.windows[0].resets_at = Some(2_000);
        first.windows[1].resets_at = Some(8_000);
        let mut second = claude_account("b", 0.0, 0.0, 50.0);
        second.windows[0].resets_at = Some(3_000);
        second.windows[1].resets_at = Some(8_000);
        let pool = pool("claude", vec![first, second]);

        assert_eq!(
            pool.constraints
                .iter()
                .map(|c| (c.family_key.as_deref(), c.next_reset_at))
                .collect::<Vec<_>>(),
            vec![
                (Some("session"), Some(2_000)),
                (Some("weekly"), Some(8_000))
            ]
        );
    }

    #[test]
    fn an_empty_pool_makes_no_claims() {
        let pool = pool("claude", Vec::new());
        assert!(pool.families.is_empty());
        assert!(pool.constraints.is_empty());
        assert_eq!(pool.captured_at, None);
    }

    fn resets(available_count: i64, expiries: &[i64]) -> ProviderUsageResetCredits {
        ProviderUsageResetCredits {
            available_count,
            credits: expiries
                .iter()
                .map(|at| ProviderUsageResetCredit {
                    expires_at: Some(*at),
                    expires_at_text: None,
                })
                .collect(),
        }
    }

    #[test]
    fn each_account_carries_its_own_reset_credits() {
        let mut a = codex_account("a", 80.0, 70.0);
        a.reset_credits = Some(resets(1, &[9_999]));
        let mut b = codex_account("b", 60.0, 50.0);
        b.reset_credits = Some(resets(2, &[7_777, 4_444]));
        let c = codex_account("c", 40.0, 30.0);
        let pool = pool("codex", vec![a, b, c]);

        let held: Vec<_> = pool
            .accounts
            .iter()
            .map(|account| {
                (
                    account.account_id.as_str(),
                    account
                        .reset_credits
                        .as_ref()
                        .map(|r| (r.available_count, r.soonest_expires_at)),
                )
            })
            .collect();
        assert_eq!(
            held,
            vec![
                ("a", Some((1, Some(9_999)))),
                // The soonest of the account's own credits.
                ("b", Some((2, Some(4_444)))),
                // An account the provider reported no credits for says nothing.
                ("c", None),
            ]
        );
    }

    #[test]
    fn an_account_with_none_left_reports_zero_rather_than_nothing() {
        let mut a = codex_account("a", 80.0, 70.0);
        a.reset_credits = Some(resets(0, &[]));
        let pool = pool("codex", vec![a]);
        assert_eq!(
            pool.accounts[0].reset_credits,
            Some(ProviderPoolAccountResets {
                available_count: 0,
                soonest_expires_at: None,
            })
        );
    }

    #[test]
    fn a_pool_serializes_camel_case_for_the_ui() {
        let pool = pool("claude", vec![claude_account("a", 60.0, 90.0, 20.0)]);
        let json = serde_json::to_value(&pool).unwrap();
        assert_eq!(json["families"][0]["capacityPercent"], 100.0);
        assert_eq!(json["families"][0]["remainingPercent"], 60.0);
        assert_eq!(json["families"][0]["unknownCount"], 0);
        assert_eq!(json["families"][0]["name"], "Session");
        assert_eq!(json["families"][0]["segments"][0]["accountId"], "a");
        assert_eq!(json["families"][0]["segments"][0]["state"], "measured");
    }
}
