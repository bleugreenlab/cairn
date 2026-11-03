//! Supply: among the candidates a requirement has already placed in one band,
//! which runs cheapest right now, on which account, and why in one line.
//!
//! Supply never moves work across bands; it only orders what the band holds.
//! The cost of one candidate is its cheapest account's [`Cost`] (see
//! `cairn_models_types::supply` for the classes and the formula). Accounts a
//! provider routes by usage (Claude and Codex subscription logins) are read
//! from the credential store through the same ranked list
//! [`CredentialStore::select_routed_account`] takes its first entry from, so
//! the account a provenance line names is the account the session runs on.

use std::cmp::Ordering;

use cairn_credentials::{CredentialStore, RoutedProvider};
use cairn_models_types::supply::{account_cost, metered_cost, Cost};
use cairn_models_types::usage::ProviderUsageWindow;
use serde::{Deserialize, Serialize};

use crate::interface::CatalogProvider;

/// The account a candidate would run on, when its provider routes accounts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyAccount {
    pub id: String,
    pub label: String,
}

/// What one candidate costs now, and on which account.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Supply {
    pub cost: Cost,
    pub account: Option<SupplyAccount>,
}

/// Usage windows a provider reported outside the credential store (OpenCode
/// Go's rolling windows), with their capture time.
pub type ProviderWindows<'a> = &'a dyn Fn(&str) -> Option<(Vec<ProviderUsageWindow>, i64)>;

/// Everything supply reads besides the catalog.
pub struct SupplyInputs<'a> {
    pub store: Option<&'a CredentialStore>,
    pub project_id: Option<&'a str>,
    /// Recent `(account id, assigned at)` pairs, which spread equal accounts.
    pub assignments: &'a [(String, i64)],
    pub provider_windows: ProviderWindows<'a>,
    pub now: i64,
}

/// What running `model` on `provider` costs now, on its cheapest account, or
/// why no account of it can serve the model.
pub fn supply_for(
    provider: &CatalogProvider,
    model: &str,
    inputs: &SupplyInputs<'_>,
) -> Result<Supply, String> {
    let key = provider.descriptor.key.as_ref();
    let name = provider.descriptor.display_name.as_ref();
    if let (Some(routed), Some(store)) = (RoutedProvider::for_backend(key), inputs.store) {
        if store.routes_any(routed, inputs.project_id) {
            let ranked = store.routed_account_costs(
                routed,
                inputs.project_id,
                None,
                Some(model),
                inputs.assignments,
                inputs.now,
            );
            let first = ranked.into_iter().next().ok_or_else(|| {
                // Each account's own reason, so a hold says what releases it
                // (accepting new terms, signing back in) rather than only
                // that nothing is free.
                let reasons: Vec<String> = store
                    .routed_account_diagnostics(
                        routed,
                        inputs.project_id,
                        None,
                        None,
                        Some(model),
                        inputs.now,
                    )
                    .into_iter()
                    .filter_map(|account| {
                        account
                            .rejection
                            .map(|why| format!("{}: {why}", account.account_id))
                    })
                    .collect();
                let mut refusal =
                    format!("every {name} account is blocked or out of usage for {model}");
                if !reasons.is_empty() {
                    refusal.push_str(&format!(" ({})", reasons.join("; ")));
                }
                refusal
            })?;
            return Ok(Supply {
                cost: first.cost,
                account: Some(SupplyAccount {
                    id: first.account.id.clone(),
                    label: first.account.label.clone(),
                }),
            });
        }
    }
    if let Some((windows, captured_at)) = (inputs.provider_windows)(key) {
        let cost = account_cost(Some((&windows, captured_at)), Some(model), inputs.now)
            .ok_or_else(|| format!("{name} is out of usage for {model}"))?;
        return Ok(Supply {
            cost,
            account: None,
        });
    }
    Ok(Supply {
        cost: metered_cost(provider.pricing(model)),
        account: None,
    })
}

/// One candidate as supply orders it.
#[derive(Debug, Clone, PartialEq)]
pub struct SupplyKey<'a> {
    pub supply: &'a Supply,
    pub level: Option<f64>,
    /// Position of the candidate's provider in the workspace's order.
    pub provider_order: usize,
    pub model: &'a str,
}

/// The supply order: cost, then the higher ladder level, then the
/// workspace's provider order, then the model id. `Less` runs first.
pub fn compare(left: &SupplyKey<'_>, right: &SupplyKey<'_>) -> Ordering {
    left.supply
        .cost
        .compare(&right.supply.cost)
        .then_with(|| match (left.level, right.level) {
            (Some(a), Some(b)) => b.total_cmp(&a),
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
        .then_with(|| left.provider_order.cmp(&right.provider_order))
        .then_with(|| left.model.cmp(right.model))
}

/// The supply clause of a provenance line: who pays, and what it costs now.
/// `astra (OpenAI): Week 64% left, resets in 14h, ~61% would expire unused`.
pub fn describe(provider_name: &str, supply: &Supply) -> String {
    match &supply.account {
        Some(account) => format!(
            "{} ({provider_name}): {}",
            account.label,
            supply.cost.describe()
        ),
        None => format!("{provider_name}: {}", supply.cost.describe()),
    }
}

/// One selection's provenance, on one line: what was asked for, what was
/// chosen at what effort, why it was the cheapest, and every note (band
/// relaxations, constraints) after it.
pub fn provenance_line(
    asked: &str,
    chosen: &str,
    effort: Option<&str>,
    clause: &str,
    notes: &[String],
) -> String {
    let mut line = format!("{asked} → {chosen}");
    if let Some(effort) = effort {
        line.push_str(&format!(" @ {effort}"));
    }
    line.push_str(&format!(": {clause}"));
    for note in notes {
        line.push_str(&format!("; {note}"));
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::supply::WindowReading;

    fn subscription(label: &str, cost: Cost) -> Supply {
        Supply {
            cost,
            account: Some(SupplyAccount {
                id: label.into(),
                label: label.into(),
            }),
        }
    }

    fn week(remaining: f64, hours: f64, expiring: Option<f64>) -> Cost {
        let window = WindowReading {
            name: "Week".into(),
            remaining_percent: remaining,
            hours_to_reset: Some(hours),
            projected_unused_percent: expiring,
        };
        match expiring {
            Some(percent) => Cost::Expiring { percent, window },
            None => Cost::Headroom {
                percent: remaining,
                window,
            },
        }
    }

    #[test]
    fn the_provenance_text_is_exact() {
        let astra = subscription("astra", week(64.0, 14.0, Some(31.0)));
        assert_eq!(
            provenance_line(
                "lg = top of [sm, md, lg]",
                "codex/gpt-5.6-sol",
                Some("high"),
                &describe("OpenAI", &astra),
                &[],
            ),
            "lg = top of [sm, md, lg] → codex/gpt-5.6-sol @ high: astra (OpenAI): Week 64% left, resets in 14h, ~31% would expire unused"
        );
        let metered = Supply {
            cost: Cost::Metered {
                output_per_mtok: Some(15.0),
                input_per_mtok: Some(3.0),
            },
            account: None,
        };
        assert_eq!(
            provenance_line(
                "md (band relaxed)",
                "openrouter/anthropic/claude-sonnet-4.5",
                None,
                &describe("OpenRouter", &metered),
                &["band relaxed md→lg (no non-Anthropic md model in allotment)".into()],
            ),
            "md (band relaxed) → openrouter/anthropic/claude-sonnet-4.5: OpenRouter: metered $3.00/$15.00 per Mtok; band relaxed md→lg (no non-Anthropic md model in allotment)"
        );
        let claude = subscription("mitch", week(22.0, 72.0, None));
        assert_eq!(
            describe("Anthropic", &claude),
            "mitch (Anthropic): Week 22% left, resets in 3d"
        );
    }

    #[test]
    fn cost_decides_before_level_and_level_before_provider_order() {
        let expiring = subscription("a", week(64.0, 14.0, Some(31.0)));
        let headroom = subscription("b", week(90.0, 100.0, None));
        let key = |supply, level, provider_order, model| SupplyKey {
            supply,
            level,
            provider_order,
            model,
        };
        // A lower model on expiring allotment beats a higher one on headroom.
        assert_eq!(
            compare(
                &key(&expiring, Some(0.5), 1, "glm"),
                &key(&headroom, Some(0.6), 0, "sonnet")
            ),
            Ordering::Less
        );
        // Equal cost: the higher level wins, then provider order, then id.
        let same = subscription("c", week(90.0, 100.0, None));
        assert_eq!(
            compare(
                &key(&headroom, Some(0.5), 1, "x"),
                &key(&same, Some(0.4), 0, "y")
            ),
            Ordering::Less
        );
        assert_eq!(
            compare(
                &key(&headroom, Some(0.5), 0, "z"),
                &key(&same, Some(0.5), 1, "a")
            ),
            Ordering::Less
        );
        assert_eq!(
            compare(
                &key(&headroom, Some(0.5), 0, "a"),
                &key(&same, Some(0.5), 0, "b")
            ),
            Ordering::Less
        );
    }
}
