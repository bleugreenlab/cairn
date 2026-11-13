//! Session-observed identities for the Claude CLI's moving aliases.

use cairn_models_types::identity;
use serde::{Deserialize, Serialize};

use super::CatalogProvider;

/// How long a session observation may identify a moving alias, in seconds.
pub const ALIAS_RESOLUTION_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// An observed alias target, with the session and clock that supplied it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AliasResolution {
    pub alias: String,
    pub reported: String,
    pub canonical: Option<String>,
    /// Unix seconds.
    pub observed_at: i64,
    pub session_id: String,
    #[serde(default)]
    pub cli_version: Option<String>,
    #[serde(default)]
    pub current_cli_version: Option<String>,
}

fn known_alias(model: &str) -> bool {
    matches!(model, "haiku" | "sonnet" | "opus" | "fable")
}

impl AliasResolution {
    fn version_changed(&self) -> bool {
        matches!(
            (&self.cli_version, &self.current_cli_version),
            (Some(observed), Some(current)) if observed != current
        )
    }

    /// The verified target while this observation is fresh. Stored canonical
    /// data is checked against the identity table rather than trusted.
    pub fn canonical_at(&self, now: i64) -> Option<&str> {
        let age = now.checked_sub(self.observed_at)?;
        if !known_alias(&self.alias)
            || self.observed_at < 0
            || !(0..ALIAS_RESOLUTION_TTL_SECS).contains(&age)
            || self.version_changed()
        {
            return None;
        }
        let canonical = self.canonical.as_deref()?;
        let verified = identity::for_offer("claude", &self.reported, None)?;
        (canonical == verified
            && identity::canonical(verified)?.maker.as_deref() == Some("anthropic"))
        .then_some(canonical)
    }

    pub fn canonical(&self) -> Option<&str> {
        self.canonical_at(chrono::Utc::now().timestamp())
    }

    /// Human-readable evidence, including stale or unrecognized observations.
    pub fn provenance_at(&self, now: i64) -> String {
        let state = if self.version_changed() {
            "stale: CLI version changed"
        } else if self.observed_at < 0
            || now
                .checked_sub(self.observed_at)
                .is_none_or(|age| !(0..ALIAS_RESOLUTION_TTL_SECS).contains(&age))
        {
            "stale"
        } else if self.canonical_at(now).is_none() {
            "unrecognized"
        } else {
            "fresh"
        };
        let observed = chrono::DateTime::from_timestamp(self.observed_at, 0)
            .filter(|_| self.observed_at >= 0)
            .map(|date| date.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "an invalid timestamp".to_string());
        let age = match now.checked_sub(self.observed_at) {
            Some(0..60) => "just now".to_string(),
            Some(seconds) if seconds >= 60 => {
                let (count, unit) = if seconds < 3_600 {
                    (seconds / 60, "minute")
                } else if seconds < 86_400 {
                    (seconds / 3_600, "hour")
                } else {
                    (seconds / 86_400, "day")
                };
                format!("{count} {unit}{} ago", if count == 1 { "" } else { "s" })
            }
            Some(_) => "timestamp is in the future".to_string(),
            None => "invalid clock".to_string(),
        };
        let version = match (&self.cli_version, &self.current_cli_version) {
            (Some(observed), Some(current)) if observed != current => {
                format!("; CLI {observed}; latest reported CLI {current}")
            }
            (Some(observed), _) => format!("; CLI {observed}"),
            _ => String::new(),
        };
        let unused = if state == "fresh" {
            ""
        } else {
            "; identity not used for benchmark placement"
        };
        format!(
            "alias {} reported {} ({state}; session {}; observed {observed}, {age}{version}{unused})",
            self.alias, self.reported, self.session_id
        )
    }

    pub fn provenance(&self) -> String {
        self.provenance_at(chrono::Utc::now().timestamp())
    }
}

impl CatalogProvider {
    /// Latest observation, even when it cannot currently identify the alias.
    /// Equal timestamps use input order's last observation.
    pub fn alias_resolution(&self, model: &str) -> Option<&AliasResolution> {
        if self.descriptor.key != "claude" || !known_alias(model) {
            return None;
        }
        self.alias_resolutions
            .iter()
            .filter(|resolution| resolution.alias == model)
            .max_by_key(|resolution| resolution.observed_at)
    }

    /// Evidence behind a Claude alias's identity, or why no identity is known.
    pub fn identity_note(&self, model: &str) -> Option<String> {
        if self.descriptor.key != "claude" || !known_alias(model) {
            return None;
        }
        Some(self.alias_resolution(model).map_or_else(
            || format!("alias {model} has no observed identity; awaiting a Claude session"),
            AliasResolution::provenance,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::Catalog;
    use cairn_models_types::catalog::{claude_models, descriptor, ProviderModelCatalog};

    const OBSERVED: i64 = 1_700_000_000;

    fn observation(alias: &str, reported: &str, observed_at: i64) -> AliasResolution {
        AliasResolution {
            alias: alias.into(),
            reported: reported.into(),
            canonical: identity::for_offer("claude", reported, None).map(str::to_string),
            observed_at,
            session_id: "session-observed".into(),
            cli_version: None,
            current_cli_version: None,
        }
    }

    fn provider(key: &str, observations: Vec<AliasResolution>) -> CatalogProvider {
        CatalogProvider {
            descriptor: descriptor(key).unwrap().clone(),
            installed: true,
            models: Some(ProviderModelCatalog {
                backend: key.into(),
                models: claude_models(),
                options: vec![],
                refreshed_at: None,
                error: None,
            }),
            serves: vec![],
            unable: vec![],
            alias_resolutions: observations,
        }
    }

    #[test]
    fn observations_expire_at_exactly_seven_days() {
        let resolution = observation("opus", "claude-opus-4-6", OBSERVED);
        for now in [OBSERVED, OBSERVED + ALIAS_RESOLUTION_TTL_SECS - 1] {
            assert_eq!(resolution.canonical_at(now), Some("claude-opus-4.6"));
            assert!(resolution.provenance_at(now).contains("fresh"));
        }
        for now in [OBSERVED + ALIAS_RESOLUTION_TTL_SECS, i64::MAX] {
            assert_eq!(resolution.canonical_at(now), None);
            assert!(resolution.provenance_at(now).contains("stale"));
        }
        let provenance = resolution.provenance_at(OBSERVED);
        assert!(provenance.contains("session-observed"));
        assert!(provenance.contains("2023-11-14 22:13 UTC"));
        assert!(provenance.contains("just now"));
        assert!(resolution
            .provenance_at(OBSERVED + 2 * 86_400)
            .contains("2 days ago"));
        assert!(provenance.contains("claude-opus-4-6"));
    }

    #[test]
    fn invalid_clocks_and_future_observations_fail_closed() {
        let mut resolution = observation("opus", "claude-opus-4-6", OBSERVED);
        for now in [OBSERVED - 1, -1, i64::MIN] {
            assert_eq!(resolution.canonical_at(now), None);
            assert!(resolution.provenance_at(now).contains("stale"));
        }
        resolution.observed_at = -1;
        assert_eq!(resolution.canonical_at(0), None);
        resolution.observed_at = i64::MIN;
        assert_eq!(resolution.canonical_at(i64::MAX), None);
    }

    #[test]
    fn a_known_cli_version_change_expires_an_observation() {
        let mut resolution = observation("opus", "claude-opus-4-6", OBSERVED);
        resolution.cli_version = Some("2.1.300".into());
        assert!(resolution.canonical_at(OBSERVED).is_some());
        resolution.current_cli_version = Some("2.1.300".into());
        assert!(resolution.canonical_at(OBSERVED).is_some());
        resolution.current_cli_version = Some("2.1.311".into());
        assert_eq!(resolution.canonical_at(OBSERVED), None);
        assert!(resolution
            .provenance_at(OBSERVED)
            .contains("CLI version changed"));
        let note = resolution.provenance_at(OBSERVED);
        assert!(note.contains("CLI 2.1.300"));
        assert!(note.contains("latest reported CLI 2.1.311"));
        assert!(note.contains("identity not used for benchmark placement"));
        resolution.cli_version = None;
        assert!(resolution.canonical_at(OBSERVED).is_some());
    }

    #[test]
    fn fable_identifies_as_each_generation_the_cli_has_run_it_as() {
        for (reported, canonical) in [
            ("claude-fable-5-1", "claude-fable-5.1"),
            ("claude-fable-5-2", "claude-fable-5.2"),
        ] {
            let resolution = observation("fable", reported, OBSERVED);
            assert_eq!(resolution.canonical_at(OBSERVED), Some(canonical));
        }
    }

    #[test]
    fn unrecognized_or_forged_targets_never_identify_an_alias() {
        for reported in [
            "claude-opus-99-7",
            "opus",
            "claude-opus-4-6-20990101",
            "someone/claude-opus-4.6",
            "gpt-5.5",
        ] {
            let mut resolution = observation("opus", reported, OBSERVED);
            resolution.canonical = Some("claude-opus-4.6".into());
            assert_eq!(resolution.canonical_at(OBSERVED), None, "{reported}");
            assert!(resolution.provenance_at(OBSERVED).contains("unrecognized"));
        }
        let mut resolution = observation("opus", "claude-opus-4-6", OBSERVED);
        resolution.canonical = Some("claude-opus-4.7".into());
        assert_eq!(resolution.canonical_at(OBSERVED), None);
        resolution.canonical = None;
        assert_eq!(resolution.canonical_at(OBSERVED), None);
    }

    #[test]
    fn only_the_four_claude_aliases_use_observations() {
        let now = chrono::Utc::now().timestamp();
        for alias in ["haiku", "sonnet", "opus", "fable"] {
            let resolution = observation(alias, "claude-opus-4-6", now);
            let claude = provider("claude", vec![resolution.clone()]);
            assert_eq!(claude.canonical(alias).as_deref(), Some("claude-opus-4.6"));
            let other = provider("codex", vec![resolution]);
            assert_eq!(other.canonical(alias), None);
            assert!(other.alias_resolution(alias).is_none());
            assert!(other.identity_note(alias).is_none());
        }
        let claude = provider(
            "claude",
            vec![observation("unknown", "claude-opus-4-6", now)],
        );
        assert!(claude.alias_resolution("unknown").is_none());
        assert!(claude.identity_note("unknown").is_none());
        assert_eq!(claude.canonical("unknown"), None);
        assert_eq!(
            claude.canonical("claude-opus-4-6").as_deref(),
            Some("claude-opus-4.6")
        );
        assert!(claude.identity_note("claude-opus-4-6").is_none());
    }

    #[test]
    fn latest_observation_wins_even_when_unrecognized_or_from_the_future() {
        let now = chrono::Utc::now().timestamp();
        let older = observation("opus", "claude-opus-4-6", now - 10);
        let mut claude = provider(
            "claude",
            vec![
                observation("opus", "unrecognized-release", now),
                older.clone(),
            ],
        );
        assert_eq!(
            claude.alias_resolution("opus").unwrap().reported,
            "unrecognized-release"
        );
        assert_eq!(claude.canonical("opus"), None);
        assert!(claude
            .identity_note("opus")
            .unwrap()
            .contains("unrecognized"));
        claude
            .alias_resolutions
            .push(observation("opus", "claude-opus-4-7", now));
        assert_eq!(claude.canonical("opus").as_deref(), Some("claude-opus-4.7"));
        claude
            .alias_resolutions
            .push(observation("opus", "claude-opus-4-6", now + 60));
        assert_eq!(claude.canonical("opus"), None);
        assert!(claude.identity_note("opus").unwrap().contains("stale"));
    }

    #[test]
    fn catalog_groups_observed_aliases_without_changing_their_native_names() {
        let now = chrono::Utc::now().timestamp();
        let claude = provider("claude", vec![observation("opus", "claude-opus-4-6", now)]);
        let models = Catalog {
            providers: vec![claude],
        }
        .models();
        let opus = models
            .iter()
            .find(|model| model.catalog_id == "claude-opus-4.6")
            .unwrap();
        assert_eq!(opus.offers[0].model, "opus");
        assert_eq!(opus.offers[0].canonical.as_deref(), Some("claude-opus-4.6"));
        assert!(opus.offers[0]
            .identity_note
            .as_deref()
            .unwrap()
            .contains("fresh"));
        let sonnet = models
            .iter()
            .find(|model| model.catalog_id == "claude/sonnet")
            .unwrap();
        assert_eq!(sonnet.offers[0].canonical, None);
        assert!(sonnet.offers[0]
            .identity_note
            .as_deref()
            .unwrap()
            .contains("no observed identity"));
        let json = serde_json::to_value(&opus.offers[0]).unwrap();
        assert!(json["identityNote"]
            .as_str()
            .unwrap()
            .contains("session-observed"));
        assert_eq!(identity::for_offer("claude", "opus", None), None);
    }

    #[test]
    fn stale_observations_keep_provenance_but_not_identity() {
        let now = chrono::Utc::now().timestamp();
        let claude = provider(
            "claude",
            vec![observation(
                "opus",
                "claude-opus-4-6",
                now - ALIAS_RESOLUTION_TTL_SECS,
            )],
        );
        assert_eq!(claude.canonical("opus"), None);
        assert!(claude.identity_note("opus").unwrap().contains("stale"));
        assert!(Catalog {
            providers: vec![claude]
        }
        .models()
        .iter()
        .any(|model| model.catalog_id == "claude/opus"));
        let missing = provider("claude", vec![]);
        assert_eq!(missing.canonical("opus"), None);
        assert!(missing
            .identity_note("opus")
            .unwrap()
            .contains("no observed identity"));
    }
}
