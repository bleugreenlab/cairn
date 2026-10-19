//! What a subscription account's plan is, and what it costs.
//!
//! [`PLAN_PRICES`] is a shipped table of monthly list prices keyed by
//! (provider, plan key). [`resolve`] reads a provider's own plan signal —
//! Claude's `subscriptionType` and `rateLimitTier`, ChatGPT's
//! `chatgpt_plan_type` — into one of those keys, into the set of keys the
//! signal cannot tell apart, or into a plan the table cannot price.
//! [`settle`] applies the operator's choice among ambiguous candidates.
//!
//! Prices are monthly list prices in whole US dollars. The billing cycle is
//! not observable, so an annually billed Claude Pro still reads 20, and a
//! regional or app-store price is not modelled. Each entry names the page its
//! figure was read from and when; a changed figure updates both.

use serde::{Deserialize, Serialize};

use crate::catalog::OPENCODE_BACKEND_KEY;

/// A provider whose accounts are priced by plan rather than by token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionProvider {
    /// A Claude subscription, through a Cairn-managed Claude sign-in.
    Claude,
    /// A ChatGPT subscription, through a Cairn-held Codex sign-in.
    Codex,
    /// An OpenCode Go subscription, on an OpenCode Zen key.
    OpenCodeGo,
}

impl SubscriptionProvider {
    pub const ALL: [Self; 3] = [Self::Claude, Self::Codex, Self::OpenCodeGo];

    /// The backend key the subscription's usage is recorded under.
    pub fn backend(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::OpenCodeGo => OPENCODE_BACKEND_KEY,
        }
    }

    pub fn for_backend(key: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.backend() == key)
    }
}

/// One plan's monthly list price, and where that figure came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanPrice {
    pub provider: SubscriptionProvider,
    pub key: &'static str,
    pub label: &'static str,
    /// `None` where the provider publishes no list price (contact sales, or a
    /// seat price that excludes usage billed on top).
    pub monthly_usd: Option<u32>,
    pub source: &'static str,
    /// The day `monthly_usd` was last read from `source`.
    pub as_of: &'static str,
}

const CLAUDE_PRICING: &str = "https://claude.com/pricing";
const CLAUDE_MAX: &str = "https://support.claude.com/en/articles/11049741-what-is-the-max-plan";
const CHATGPT_PRICING: &str = "https://chatgpt.com/pricing";
const CHATGPT_PRO: &str = "https://help.openai.com/en/articles/9793128-what-is-chatgpt-pro";
const CHATGPT_BUSINESS: &str =
    "https://help.openai.com/en/articles/8792828-chatgpt-business-overview";
const CHATGPT_SALES: &str = "https://openai.com/business/pricing";
const OPENCODE_GO: &str = "https://opencode.ai/docs/go/";
const CHECKED: &str = "2026-10-18";

const fn price(
    provider: SubscriptionProvider,
    key: &'static str,
    label: &'static str,
    monthly_usd: Option<u32>,
    source: &'static str,
) -> PlanPrice {
    PlanPrice {
        provider,
        key,
        label,
        monthly_usd,
        source,
        as_of: CHECKED,
    }
}

use SubscriptionProvider::{Claude, Codex, OpenCodeGo};

/// Every plan Cairn can name, with its monthly list price.
pub const PLAN_PRICES: &[PlanPrice] = &[
    price(Claude, "pro", "Pro", Some(20), CLAUDE_PRICING),
    price(Claude, "max_5x", "Max 5x", Some(100), CLAUDE_MAX),
    price(Claude, "max_20x", "Max 20x", Some(200), CLAUDE_MAX),
    price(Claude, "team_standard", "Team", Some(25), CLAUDE_PRICING),
    price(
        Claude,
        "team_premium",
        "Team Premium",
        Some(125),
        CLAUDE_PRICING,
    ),
    // A seat fee plus usage at API rates: the seat alone is not what it costs.
    price(Claude, "enterprise", "Enterprise", None, CLAUDE_PRICING),
    price(Codex, "free", "Free", Some(0), CHATGPT_PRICING),
    price(Codex, "go", "Go", Some(8), CHATGPT_PRICING),
    price(Codex, "plus", "Plus", Some(20), CHATGPT_PRICING),
    price(Codex, "pro_100", "Pro 100", Some(100), CHATGPT_PRO),
    price(Codex, "pro_200", "Pro 200", Some(200), CHATGPT_PRO),
    price(Codex, "pro_500", "Pro 500", Some(500), CHATGPT_PRO),
    price(
        Codex,
        "business_standard",
        "Business",
        Some(25),
        CHATGPT_BUSINESS,
    ),
    price(
        Codex,
        "business_premium",
        "Business Premium",
        Some(125),
        CHATGPT_BUSINESS,
    ),
    price(Codex, "enterprise", "Enterprise", None, CHATGPT_SALES),
    price(Codex, "edu", "Edu", None, CHATGPT_SALES),
    price(OpenCodeGo, "go", "Go", Some(10), OPENCODE_GO),
    price(OpenCodeGo, "go_plus", "Go Plus", Some(40), OPENCODE_GO),
];

/// The table entry for `key` on `provider`.
pub fn plan_price(provider: SubscriptionProvider, key: &str) -> Option<&'static PlanPrice> {
    PLAN_PRICES
        .iter()
        .find(|entry| entry.provider == provider && entry.key == key)
}

/// What a provider's plan signal says an account is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanResolution {
    /// Exactly one priced plan.
    Known(&'static str),
    /// One of these priced plans; the signal cannot tell them apart. `family`
    /// is what they share (`Pro`, `Max`).
    Ambiguous {
        family: &'static str,
        candidates: &'static [&'static str],
    },
    /// A plan the table cannot price, as the provider named it. Empty when
    /// the provider named none.
    Unpriced(String),
}

const CLAUDE_MAX_PLANS: &[&str] = &["max_5x", "max_20x"];
const CLAUDE_TEAM_PLANS: &[&str] = &["team_standard", "team_premium"];
// ChatGPT's own claim names the $100 tier `prolite` and the $500 tier
// `promax` (codex-rs/protocol/src/auth.rs displays them as "Pro" and
// "Pro (Max)"). `pro` is kept open between the two tiers it has been worn by.
const CHATGPT_PRO_PLANS: &[&str] = &["pro_200", "pro_500"];
const CHATGPT_BUSINESS_PLANS: &[&str] = &["business_standard", "business_premium"];
// One OpenCode Zen key serves either Go plan; nothing in it says which.
const OPENCODE_GO_PLANS: &[&str] = &["go", "go_plus"];

/// Read a provider's plan signal.
///
/// `plan` is Claude's `subscriptionType` or ChatGPT's `chatgpt_plan_type`;
/// `tier` is Claude's `rateLimitTier`, which is what separates Max 5x from
/// Max 20x. Matching ignores case and surrounding space.
pub fn resolve(
    provider: SubscriptionProvider,
    plan: Option<&str>,
    tier: Option<&str>,
) -> PlanResolution {
    let plan = plan.map(|plan| plan.trim().to_ascii_lowercase());
    let tier = tier.map(|tier| tier.trim().to_ascii_lowercase());
    let ambiguous = |family, candidates| PlanResolution::Ambiguous { family, candidates };
    let unpriced = || PlanResolution::Unpriced(plan.clone().unwrap_or_default());
    match provider {
        Claude => {
            match tier.as_deref() {
                Some(tier) if tier.ends_with("max_20x") => return PlanResolution::Known("max_20x"),
                Some(tier) if tier.ends_with("max_5x") => return PlanResolution::Known("max_5x"),
                _ => {}
            }
            match plan.as_deref() {
                Some("pro") => PlanResolution::Known("pro"),
                Some("max") => ambiguous("Max", CLAUDE_MAX_PLANS),
                Some("team") => ambiguous("Team", CLAUDE_TEAM_PLANS),
                _ => unpriced(),
            }
        }
        Codex => match plan.as_deref() {
            Some("free") => PlanResolution::Known("free"),
            Some("go") => PlanResolution::Known("go"),
            Some("plus") => PlanResolution::Known("plus"),
            Some("prolite") => PlanResolution::Known("pro_100"),
            Some("promax") => PlanResolution::Known("pro_500"),
            Some("pro") => ambiguous("Pro", CHATGPT_PRO_PLANS),
            Some("team" | "business") => ambiguous("Business", CHATGPT_BUSINESS_PLANS),
            _ => unpriced(),
        },
        OpenCodeGo => ambiguous("Go", OPENCODE_GO_PLANS),
    }
}

/// What an unpriced plan is called: the table's label where it names the
/// plan, the provider's own display name for the plans it documents, the raw
/// value otherwise.
fn unpriced_label(provider: SubscriptionProvider, raw: &str) -> String {
    if raw.is_empty() {
        return "Unknown".to_string();
    }
    let known = match (provider, raw) {
        (Codex, "ent26" | "hc") => Some("Enterprise"),
        (Codex, "enterprise_cbp_automation") => Some("Enterprise (Automation)"),
        (Codex, "enterprise_cbp_usage_based") => Some("Enterprise CBP Usage Based"),
        (Codex, "self_serve_business_prolite") => Some("Self Serve Business ProLite"),
        (Codex, "self_serve_business_usage_based") => Some("Self Serve Business Usage Based"),
        (Codex, "education") => Some("Edu"),
        (Codex, "edu_plus") => Some("Edu Plus"),
        (Codex, "edu_pro") => Some("Edu Pro"),
        _ => plan_price(provider, raw).map(|entry| entry.label),
    };
    known.map(str::to_string).unwrap_or_else(|| raw.to_string())
}

impl PlanResolution {
    /// Whether `key` is a plan the operator may choose here.
    pub fn admits_choice(&self, key: &str) -> bool {
        matches!(self, Self::Ambiguous { candidates, .. } if candidates.contains(&key))
    }
}

/// One plan an ambiguous account may be on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanOption {
    pub key: String,
    pub label: String,
    pub monthly_usd: Option<u32>,
}

/// An account's plan as a surface shows it: the resolution with the
/// operator's choice applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPlan {
    /// The backend the subscription serves.
    pub provider: String,
    /// The plan in effect (`Max 20x`, `Pro 200`), or what an unsettled or
    /// unpriced plan is called (`Pro`, `Enterprise`).
    pub label: String,
    /// The priced plan in effect; `None` while ambiguous and unchosen, or
    /// when the table cannot price the plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monthly_usd: Option<u32>,
    /// What ambiguous candidates share (`Pro`); absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    /// The candidates of an ambiguous plan; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<PlanOption>,
}

/// Apply `choice` to `resolution`. A choice that is not among the current
/// candidates — the plan changed since it was made — is ignored.
pub fn settle(
    provider: SubscriptionProvider,
    resolution: &PlanResolution,
    choice: Option<&str>,
) -> AccountPlan {
    let priced = |key: &str| plan_price(provider, key);
    let base = AccountPlan {
        provider: provider.backend().to_string(),
        label: String::new(),
        key: None,
        monthly_usd: None,
        family: None,
        options: Vec::new(),
    };
    match resolution {
        PlanResolution::Known(key) => {
            let entry = priced(key);
            AccountPlan {
                label: entry.map_or_else(|| key.to_string(), |entry| entry.label.to_string()),
                key: Some(key.to_string()),
                monthly_usd: entry.and_then(|entry| entry.monthly_usd),
                ..base
            }
        }
        PlanResolution::Ambiguous { family, candidates } => {
            let options: Vec<PlanOption> = candidates
                .iter()
                .filter_map(|key| priced(key))
                .map(|entry| PlanOption {
                    key: entry.key.to_string(),
                    label: entry.label.to_string(),
                    monthly_usd: entry.monthly_usd,
                })
                .collect();
            let chosen = choice
                .filter(|choice| resolution.admits_choice(choice))
                .and_then(|choice| options.iter().find(|option| option.key == choice))
                .cloned();
            AccountPlan {
                label: chosen
                    .as_ref()
                    .map_or_else(|| family.to_string(), |option| option.label.clone()),
                key: chosen.as_ref().map(|option| option.key.clone()),
                monthly_usd: chosen.and_then(|option| option.monthly_usd),
                family: Some(family.to_string()),
                options,
                ..base
            }
        }
        PlanResolution::Unpriced(raw) => AccountPlan {
            label: unpriced_label(provider, raw),
            ..base
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(resolution: PlanResolution) -> Option<&'static str> {
        match resolution {
            PlanResolution::Known(key) => Some(key),
            _ => None,
        }
    }

    #[test]
    fn every_resolvable_key_is_priced() {
        for provider in SubscriptionProvider::ALL {
            let signals = [
                "pro", "max", "team", "free", "go", "plus", "prolite", "promax", "business",
            ];
            for signal in signals {
                for tier in [
                    None,
                    Some("default_claude_max_5x"),
                    Some("default_claude_max_20x"),
                ] {
                    let keys: Vec<&str> = match resolve(provider, Some(signal), tier) {
                        PlanResolution::Known(key) => vec![key],
                        PlanResolution::Ambiguous { candidates, .. } => candidates.to_vec(),
                        PlanResolution::Unpriced(_) => vec![],
                    };
                    for key in keys {
                        let entry = plan_price(provider, key)
                            .unwrap_or_else(|| panic!("{provider:?}/{key} is not in the table"));
                        assert!(
                            entry.monthly_usd.is_some(),
                            "{provider:?}/{key} has no price"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn every_entry_names_its_source() {
        for entry in PLAN_PRICES {
            assert!(entry.source.starts_with("https://"), "{entry:?}");
            assert_eq!(entry.as_of.len(), 10, "{entry:?}");
            let duplicates = PLAN_PRICES
                .iter()
                .filter(|other| other.provider == entry.provider && other.key == entry.key)
                .count();
            assert_eq!(duplicates, 1, "{entry:?}");
        }
    }

    #[test]
    fn claude_reads_the_rate_limit_tier_before_the_subscription_type() {
        let resolve = |plan, tier| resolve(Claude, plan, tier);
        assert_eq!(
            key(resolve(Some("max"), Some("default_claude_max_20x"))),
            Some("max_20x")
        );
        assert_eq!(
            key(resolve(Some("max"), Some("default_claude_max_5x"))),
            Some("max_5x")
        );
        assert_eq!(
            key(resolve(Some("Max"), Some("DEFAULT_CLAUDE_MAX_20X"))),
            Some("max_20x")
        );
        assert_eq!(
            resolve(Some("max"), Some("default_claude_ai")),
            PlanResolution::Ambiguous {
                family: "Max",
                candidates: &["max_5x", "max_20x"]
            }
        );
        assert_eq!(
            resolve(Some("max"), None),
            PlanResolution::Ambiguous {
                family: "Max",
                candidates: &["max_5x", "max_20x"]
            }
        );
        assert_eq!(key(resolve(Some("pro"), None)), Some("pro"));
        assert_eq!(
            resolve(Some("team"), None),
            PlanResolution::Ambiguous {
                family: "Team",
                candidates: &["team_standard", "team_premium"]
            }
        );
        assert_eq!(
            resolve(Some("enterprise"), None),
            PlanResolution::Unpriced("enterprise".to_string())
        );
        assert_eq!(
            resolve(Some("free"), None),
            PlanResolution::Unpriced("free".to_string())
        );
        assert_eq!(resolve(None, None), PlanResolution::Unpriced(String::new()));
    }

    #[test]
    fn chatgpt_reads_every_plan_type_codex_knows() {
        let resolve = |plan| resolve(Codex, Some(plan), None);
        assert_eq!(key(resolve("free")), Some("free"));
        assert_eq!(key(resolve("go")), Some("go"));
        assert_eq!(key(resolve("plus")), Some("plus"));
        assert_eq!(key(resolve("prolite")), Some("pro_100"));
        assert_eq!(key(resolve("promax")), Some("pro_500"));
        assert_eq!(
            resolve("pro"),
            PlanResolution::Ambiguous {
                family: "Pro",
                candidates: &["pro_200", "pro_500"]
            }
        );
        for business in ["team", "business"] {
            assert_eq!(
                resolve(business),
                PlanResolution::Ambiguous {
                    family: "Business",
                    candidates: &["business_standard", "business_premium"]
                }
            );
        }
        for (raw, label) in [
            ("ent26", "Enterprise"),
            ("enterprise", "Enterprise"),
            ("hc", "Enterprise"),
            ("enterprise_cbp_automation", "Enterprise (Automation)"),
            ("enterprise_cbp_usage_based", "Enterprise CBP Usage Based"),
            ("self_serve_business_prolite", "Self Serve Business ProLite"),
            (
                "self_serve_business_usage_based",
                "Self Serve Business Usage Based",
            ),
            ("edu", "Edu"),
            ("education", "Edu"),
            ("edu_plus", "Edu Plus"),
            ("edu_pro", "Edu Pro"),
            ("guest", "guest"),
        ] {
            let resolution = resolve(raw);
            assert_eq!(resolution, PlanResolution::Unpriced(raw.to_string()));
            let plan = settle(Codex, &resolution, None);
            assert_eq!(plan.label, label);
            assert_eq!(plan.monthly_usd, None);
            assert_eq!(plan.key, None);
        }
    }

    #[test]
    fn opencode_go_is_always_a_choice_between_its_two_plans() {
        let resolution = resolve(OpenCodeGo, None, None);
        let plan = settle(OpenCodeGo, &resolution, None);
        assert_eq!(plan.label, "Go");
        assert_eq!(plan.monthly_usd, None);
        assert_eq!(plan.options.len(), 2);
        let plus = settle(OpenCodeGo, &resolution, Some("go_plus"));
        assert_eq!(
            (plus.label.as_str(), plus.monthly_usd),
            ("Go Plus", Some(40))
        );
    }

    #[test]
    fn a_choice_settles_an_ambiguous_plan_and_a_stale_one_is_ignored() {
        let pro = resolve(Codex, Some("pro"), None);
        let unchosen = settle(Codex, &pro, None);
        assert_eq!(unchosen.label, "Pro");
        assert_eq!(unchosen.family.as_deref(), Some("Pro"));
        assert_eq!(unchosen.key, None);
        assert_eq!(unchosen.monthly_usd, None);
        assert_eq!(
            unchosen
                .options
                .iter()
                .map(|option| (option.label.as_str(), option.monthly_usd))
                .collect::<Vec<_>>(),
            [("Pro 200", Some(200)), ("Pro 500", Some(500))]
        );

        let chosen = settle(Codex, &pro, Some("pro_500"));
        assert_eq!(chosen.label, "Pro 500");
        assert_eq!(chosen.key.as_deref(), Some("pro_500"));
        assert_eq!(chosen.monthly_usd, Some(500));

        // A choice from a plan the account no longer holds settles nothing.
        assert!(!pro.admits_choice("business_premium"));
        assert_eq!(settle(Codex, &pro, Some("business_premium")), unchosen);
        // A known plan offers no choice and ignores one.
        let plus = resolve(Codex, Some("plus"), None);
        assert!(!plus.admits_choice("plus"));
        let settled = settle(Codex, &plus, Some("pro_500"));
        assert_eq!(
            (settled.label.as_str(), settled.monthly_usd),
            ("Plus", Some(20))
        );
        assert!(settled.options.is_empty());
    }

    #[test]
    fn the_plan_serializes_for_the_account_row() {
        let plan = settle(
            Claude,
            &resolve(Claude, Some("max"), Some("default_claude_max_20x")),
            None,
        );
        assert_eq!(
            serde_json::to_value(&plan).unwrap(),
            serde_json::json!({
                "provider": "claude",
                "label": "Max 20x",
                "key": "max_20x",
                "monthlyUsd": 200
            })
        );
    }
}
