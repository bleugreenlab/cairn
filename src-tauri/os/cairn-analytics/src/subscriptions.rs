//! Which subscription plans were held, and when: `subscription_periods`.
//!
//! The credential lifecycle reports what each subscription account holds now
//! ([`reconcile_periods`]); this keeps one open period per held account,
//! closing it when the account goes or its plan changes. Effective cost reads
//! the periods back ([`plan_periods`]) and prices each month with the shipped
//! price table ([`month_subscriptions`]).

use std::collections::{HashMap, HashSet};

use cairn_db::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_models_types::subscription::{plan_price, SubscriptionProvider};
use serde::{Deserialize, Serialize};

/// What one subscription account holds now, as the credential store says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldSubscription {
    pub account_id: String,
    pub account_label: String,
    /// The backend the subscription serves.
    pub provider: String,
    /// The priced plan in effect; `None` when the plan has no price.
    pub plan_key: Option<String>,
    pub plan_label: String,
    /// When the account was added, where its first period starts.
    pub created_at: i64,
}

/// One stretch of one account on one plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanPeriod {
    pub account_id: String,
    pub account_label: String,
    pub provider: String,
    pub plan_key: Option<String>,
    pub plan_label: String,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

struct OpenPeriod {
    id: String,
    account_id: String,
    provider: String,
    plan_key: Option<String>,
    plan_label: String,
    account_label: String,
}

impl OpenPeriod {
    fn holds(&self, held: &HeldSubscription) -> bool {
        self.provider == held.provider
            && self.plan_key == held.plan_key
            && self.plan_label == held.plan_label
    }
}

/// Bring the periods in line with what is held at `now`: close the open
/// period of every account no longer held, close and reopen the period of
/// every account whose plan changed, and open one for every held account
/// without one. An account with no period at all starts at its `created_at`,
/// which is how existing accounts are backfilled; one that had periods before
/// starts at `now`. A renamed account's open period takes the new name.
/// Answers whether anything changed.
pub async fn reconcile_periods(
    db: &LocalDb,
    held: &[HeldSubscription],
    now: i64,
) -> DbResult<bool> {
    let held = held.to_vec();
    db.write(move |conn| {
        let held = held.clone();
        Box::pin(async move {
            let mut open = Vec::new();
            {
                let mut rows = conn
                    .query(
                        "SELECT id, account_id, provider, plan_key, plan_label, account_label \
                         FROM subscription_periods WHERE ended_at IS NULL",
                        (),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    open.push(OpenPeriod {
                        id: row.text(0)?,
                        account_id: row.text(1)?,
                        provider: row.text(2)?,
                        plan_key: row.opt_text(3)?,
                        plan_label: row.text(4)?,
                        account_label: row.text(5)?,
                    });
                }
            }
            let mut known: HashSet<String> = HashSet::new();
            {
                let mut rows = conn
                    .query("SELECT DISTINCT account_id FROM subscription_periods", ())
                    .await?;
                while let Some(row) = rows.next().await? {
                    known.insert(row.text(0)?);
                }
            }
            let held_by_id: HashMap<&str, &HeldSubscription> = held
                .iter()
                .map(|held| (held.account_id.as_str(), held))
                .collect();
            let mut changed = false;
            let mut still_open: HashSet<&str> = HashSet::new();
            for period in &open {
                match held_by_id.get(period.account_id.as_str()) {
                    Some(held) if period.holds(held) => {
                        still_open.insert(period.account_id.as_str());
                        if period.account_label != held.account_label {
                            conn.execute(
                                "UPDATE subscription_periods SET account_label = ?1 WHERE id = ?2",
                                params![held.account_label.as_str(), period.id.as_str()],
                            )
                            .await?;
                            changed = true;
                        }
                    }
                    _ => {
                        conn.execute(
                            "UPDATE subscription_periods SET ended_at = MAX(started_at, ?1) \
                             WHERE id = ?2",
                            params![now, period.id.as_str()],
                        )
                        .await?;
                        changed = true;
                    }
                }
            }
            for held in &held {
                if still_open.contains(held.account_id.as_str()) {
                    continue;
                }
                let started_at = if known.contains(&held.account_id) {
                    now
                } else {
                    held.created_at.min(now)
                };
                conn.execute(
                    "INSERT INTO subscription_periods \
                     (id, account_id, provider, plan_key, plan_label, account_label, started_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        uuid::Uuid::new_v4().to_string(),
                        held.account_id.as_str(),
                        held.provider.as_str(),
                        held.plan_key.clone(),
                        held.plan_label.as_str(),
                        held.account_label.as_str(),
                        started_at
                    ],
                )
                .await?;
                changed = true;
            }
            Ok(changed)
        })
    })
    .await
}

/// Every period, oldest first. Periods that start in the same second (a plan
/// chosen the moment its account was added) order closed before open, so the
/// plan an account went on to hold reads last.
pub async fn plan_periods(db: &LocalDb) -> DbResult<Vec<PlanPeriod>> {
    db.query_all(
        "SELECT account_id, account_label, provider, plan_key, plan_label, started_at, ended_at \
         FROM subscription_periods \
         ORDER BY started_at, ended_at IS NULL, ended_at, rowid",
        (),
        |row| {
            Ok(PlanPeriod {
                account_id: row.text(0)?,
                account_label: row.text(1)?,
                provider: row.text(2)?,
                plan_key: row.opt_text(3)?,
                plan_label: row.text(4)?,
                started_at: row.i64(5)?,
                ended_at: row.opt_i64(6)?,
            })
        },
    )
    .await
}

/// How many accounts held one priced plan in a month.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanCount {
    pub label: String,
    pub count: u32,
}

/// An account whose plan in a month has no price, so it is left out of
/// what the month paid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnpricedAccount {
    pub account_id: String,
    /// The account's name.
    pub label: String,
    /// What its plan is called (`Go`, `Enterprise`).
    pub plan: String,
}

/// What one backend's subscriptions cost in one month.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct MonthSubscriptions {
    /// The summed monthly list price of the priced plans held; `None` when
    /// no account held a priced plan that month.
    pub(crate) paid: Option<f64>,
    pub(crate) plans: Vec<PlanCount>,
    pub(crate) unpriced: Vec<UnpricedAccount>,
}

impl MonthSubscriptions {
    /// Whether any subscription account served the backend that month.
    pub(crate) fn any(&self) -> bool {
        self.paid.is_some() || !self.unpriced.is_empty()
    }
}

/// `backend`'s subscriptions in the month `[month_start, month_end)`.
///
/// Subscriptions bill by the month, so any overlap counts the whole month.
/// An account counts once, at the plan it last held that month: a plan
/// toggled from Pro 200 to Pro 500 is a correction, not a second purchase.
pub(crate) fn month_subscriptions(
    periods: &[PlanPeriod],
    backend: &str,
    month_start: i64,
    month_end: i64,
) -> MonthSubscriptions {
    let Some(provider) = SubscriptionProvider::for_backend(backend) else {
        return MonthSubscriptions::default();
    };
    let mut latest: HashMap<&str, &PlanPeriod> = HashMap::new();
    for period in periods.iter().filter(|period| {
        period.provider == backend
            && period.started_at < month_end
            && period.ended_at.is_none_or(|ended| ended > month_start)
    }) {
        let entry = latest.entry(period.account_id.as_str()).or_insert(period);
        if period.started_at >= entry.started_at {
            *entry = period;
        }
    }
    let mut held: Vec<&PlanPeriod> = latest.into_values().collect();
    held.sort_by(|a, b| a.account_id.cmp(&b.account_id));

    let mut month = MonthSubscriptions::default();
    for period in held {
        let price = period
            .plan_key
            .as_deref()
            .and_then(|key| plan_price(provider, key))
            .and_then(|entry| entry.monthly_usd);
        match price {
            Some(usd) => {
                *month.paid.get_or_insert(0.0) += f64::from(usd);
                match month
                    .plans
                    .iter_mut()
                    .find(|plan| plan.label == period.plan_label)
                {
                    Some(plan) => plan.count += 1,
                    None => month.plans.push(PlanCount {
                        label: period.plan_label.clone(),
                        count: 1,
                    }),
                }
            }
            None => month.unpriced.push(UnpricedAccount {
                account_id: period.account_id.clone(),
                label: period.account_label.clone(),
                plan: period.plan_label.clone(),
            }),
        }
    }
    month
        .plans
        .sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.label.cmp(&b.label)));
    month
}
