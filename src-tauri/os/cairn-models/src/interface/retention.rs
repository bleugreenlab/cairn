//! What a call requires of where its text goes, how that requirement was met,
//! and what the call cost.
//!
//! Zero data retention (ZDR) is the one requirement today: the call's text may
//! reach only a deployment that keeps none of it once it has answered. Every
//! verb takes it the same way, as [`Requirements`], and it is decided once per
//! call, before anything is sent, by the backend that would serve the call.
//! A requirement that backend cannot meet is refused, never sent in hope.
//!
//! Two things can require it of a call ([`RequiredBy`]): the call itself, for
//! any provider, or the workspace's OpenRouter routing setting, for every call
//! OpenRouter serves. The setting is a floor rather than a default a call can
//! lower: it is the operator's, and a call saying nothing, or `false`, leaves
//! it in force. It never reaches past OpenRouter, because that is all the
//! setting has ever said.
//!
//! How a requirement was met ([`ZdrBasis`]) is stated in the reply, so the
//! caller records the reason it may rely on rather than a bare `true`.

use cairn_models_types::catalog::ModelPricing;
use cairn_models_types::usage::MeteredUsage;
use serde::{Deserialize, Serialize};

/// What a call requires of the deployment that serves it. The one shape every
/// verb's door takes as `requirements`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Requirements {
    /// Only a deployment that retains none of the call's text may serve it.
    /// `false`, the default, leaves the workspace's OpenRouter floor in force.
    #[serde(default)]
    pub zero_data_retention: bool,
}

impl Requirements {
    /// Requiring zero data retention.
    pub fn zero_data_retention() -> Self {
        Self {
            zero_data_retention: true,
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// What required zero data retention of a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequiredBy {
    /// The call's own `requirements`.
    Call,
    /// The workspace's OpenRouter routing setting, which requires it of every
    /// call OpenRouter serves.
    #[serde(rename = "workspace-openrouter-setting")]
    WorkspaceOpenRouterSetting,
}

/// How a zero-data-retention requirement was met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ZdrBasis {
    /// OpenRouter lists a zero-retention endpoint for the model, and the
    /// request carried `provider.zdr`, which routes only to such endpoints.
    #[serde(rename = "openrouter-zdr-routing")]
    OpenRouterZdrRouting,
    /// The deployment runs on the operator's own hardware (Cairn's inference
    /// deployments, an Ollama host): no provider is there to retain anything.
    OperatorHardware,
    /// A custom endpoint on a loopback or private-network host.
    PrivateHost,
    /// The operator declared this provider zero-retention
    /// (`zeroDataRetentionProviders`).
    OperatorDeclaration,
}

/// A met requirement, as the reply states it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ZeroDataRetention {
    pub required_by: RequiredBy,
    pub satisfied_by: ZdrBasis,
    /// The basis as a sentence: which endpoints, host, or declaration.
    pub detail: String,
}

/// What a call's requirements came to on one provider: whether zero data
/// retention is required of it, and by what.
pub fn zdr_required_by(
    requirements: &Requirements,
    provider: &str,
    openrouter_floor: bool,
) -> Option<RequiredBy> {
    if requirements.zero_data_retention {
        Some(RequiredBy::Call)
    } else if openrouter_floor && provider == OPENROUTER_PROVIDER_KEY {
        Some(RequiredBy::WorkspaceOpenRouterSetting)
    } else {
        None
    }
}

/// OpenRouter's backend key, the provider the workspace floor applies to.
pub const OPENROUTER_PROVIDER_KEY: &str = "openrouter";

/// Where a reply's cost came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CostSource {
    /// The provider reported what it charged.
    Provider,
    /// The catalog's list price for the offer, times the quantities the
    /// provider reported.
    Catalog,
}

/// A call's cost and where it came from: the provider's own figure when it
/// gave one, else `usage` priced at `pricing`, else nothing. Never a guess:
/// an unmeasured call or an unpriced meter leaves the cost absent.
pub fn call_cost(
    usage: &MeteredUsage,
    pricing: Option<&ModelPricing>,
) -> (Option<f64>, Option<CostSource>) {
    if let Some(cost) = usage.cost_usd {
        return (Some(cost), Some(CostSource::Provider));
    }
    match pricing.and_then(|pricing| pricing.price(usage)) {
        Some(cost) => (Some(cost), Some(CostSource::Catalog)),
        None => (None, None),
    }
}

/// Whether `host` (a URL's host, without its port) is this machine or a
/// private network: loopback, an RFC 1918 or link-local IPv4 address, an IPv6
/// loopback, unique-local or link-local address, `localhost`, or a `.local`,
/// `.lan` or `.localhost` name. A public name or address is not, even when it
/// happens to resolve somewhere private: what a name resolves to is not
/// something the operator declared.
pub fn is_private_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return match ip {
            std::net::IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
            std::net::IpAddr::V6(ip) => {
                let first = ip.segments()[0];
                ip.is_loopback()
                    // fc00::/7, unique local.
                    || (first & 0xfe00) == 0xfc00
                    // fe80::/10, link local.
                    || (first & 0xffc0) == 0xfe80
            }
        };
    }
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || [".localhost", ".local", ".lan"]
            .iter()
            .any(|suffix| host.ends_with(suffix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models_types::catalog::Price;
    use cairn_models_types::usage::{Charge, Measure, Meter};

    #[test]
    fn the_call_requires_it_anywhere_and_the_setting_only_on_openrouter() {
        let asked = Requirements::zero_data_retention();
        let silent = Requirements::default();
        assert_eq!(
            zdr_required_by(&asked, "claude", false),
            Some(RequiredBy::Call)
        );
        assert_eq!(
            zdr_required_by(&silent, "openrouter", true),
            Some(RequiredBy::WorkspaceOpenRouterSetting)
        );
        assert_eq!(zdr_required_by(&silent, "claude", true), None);
        assert_eq!(zdr_required_by(&silent, "openrouter", false), None);
        let explicit_false: Requirements =
            serde_json::from_value(serde_json::json!({"zeroDataRetention": false})).unwrap();
        assert_eq!(
            zdr_required_by(&explicit_false, "openrouter", true),
            Some(RequiredBy::WorkspaceOpenRouterSetting),
            "a call cannot lower the operator's floor"
        );
        assert!(
            serde_json::from_value::<Requirements>(serde_json::json!({"zdr": true})).is_err(),
            "an unknown requirement is refused, not ignored"
        );
        assert_eq!(
            serde_json::to_value(RequiredBy::WorkspaceOpenRouterSetting).unwrap(),
            "workspace-openrouter-setting"
        );
        assert_eq!(
            serde_json::to_value(ZdrBasis::OpenRouterZdrRouting).unwrap(),
            "openrouter-zdr-routing"
        );
    }

    #[test]
    fn private_hosts_are_loopback_private_networks_and_local_names() {
        for host in [
            "127.0.0.1",
            "localhost",
            "10.1.2.3",
            "172.20.0.4",
            "192.168.1.20",
            "169.254.3.3",
            "[::1]",
            "fd12::1",
            "fe80::1",
            "gpu.lan",
            "mac-studio.local",
            "api.localhost",
        ] {
            assert!(is_private_host(host), "{host}");
        }
        for host in [
            "api.openai.com",
            "8.8.8.8",
            "172.32.0.1",
            "2001:db8::1",
            "local.example.com",
            "lan.example.com",
        ] {
            assert!(!is_private_host(host), "{host}");
        }
    }

    fn tokens(charge: Charge, quantity: u64) -> Measure {
        Measure {
            meter: Meter::tokens(charge),
            quantity,
        }
    }

    #[test]
    fn the_providers_cost_wins_and_the_catalog_prices_what_was_measured() {
        let pricing = ModelPricing {
            prices: vec![
                Price {
                    meter: Meter::tokens(Charge::Input),
                    usd: "0.000000042".into(),
                },
                Price {
                    meter: Meter::tokens(Charge::Output),
                    usd: "0.000001".into(),
                },
            ],
            ..ModelPricing::default()
        };
        let usage = MeteredUsage {
            measures: vec![tokens(Charge::Input, 1_000_000), tokens(Charge::Output, 10)],
            cost_usd: None,
        };
        let (cost, source) = call_cost(&usage, Some(&pricing));
        assert!((cost.unwrap() - 0.04201).abs() < 1e-12, "{cost:?}");
        assert_eq!(source, Some(CostSource::Catalog));

        let reported = usage.clone().with_cost(Some(0.5));
        assert_eq!(
            call_cost(&reported, Some(&pricing)),
            (Some(0.5), Some(CostSource::Provider))
        );
        assert_eq!(call_cost(&usage, None), (None, None));
        let unpriced = MeteredUsage {
            measures: vec![tokens(Charge::CacheRead, 5)],
            cost_usd: None,
        };
        assert_eq!(
            call_cost(&unpriced, Some(&pricing)),
            (None, None),
            "an unpriced meter is never priced at zero"
        );
        assert_eq!(
            call_cost(&MeteredUsage::default(), Some(&pricing)),
            (None, None),
            "nothing measured is not free"
        );
    }
}
