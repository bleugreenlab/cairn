//! How long a subscription sign-in lasts, and when its operator must hear about it.
//!
//! A Claude sign-in mints a refresh-token family with a deadline the provider
//! declares (`refresh_token_expires_in` on the token response; observed between
//! 27 and 30 days). Rotating the refresh token does not move that deadline: a
//! token minted hours before it is refused as `Refresh token expired` once it
//! passes. So Cairn cannot keep a sign-in alive past it. What Cairn can do is
//! know it, stop routing to an account whose sign-in has lapsed, and tell the
//! operator days ahead so the reconnect is a heads-up, never an outage.
//!
//! The deadline lives in the stored credential itself, beside the tokens it
//! governs, so a reconnect that replaces the credential replaces the deadline
//! with it and nothing else has to be cleared.

use serde::{Deserialize, Serialize};

use crate::{ProviderAccount, ProviderAuth};

/// The Claude CLI's field for the refresh-token deadline, in milliseconds since
/// the epoch. Cairn keeps the CLI's name so a harvested credential carries it
/// verbatim.
pub const CLAUDE_REFRESH_EXPIRES_AT_FIELD: &str = "refreshTokenExpiresAt";

/// Cairn-owned field: on whose word [`CLAUDE_REFRESH_EXPIRES_AT_FIELD`] stands
/// (a [`SignInExpirySource`]). Absent on a credential harvested before Cairn
/// recorded it, which reads as [`SignInExpirySource::SignIn`].
pub const CLAUDE_REFRESH_EXPIRY_SOURCE_FIELD: &str = "cairn_refresh_expiry_source";

/// The lifetime the Claude CLI itself assumes when a sign-in response declares
/// none, and the one Cairn infers for a credential that carries no deadline.
pub const INFERRED_SIGN_IN_LIFETIME_SECS: i64 = 30 * 24 * 60 * 60;

/// How long before a deadline the operator is first told, matching the window
/// in which the Claude CLI starts its own "sign in again" notice.
pub const SIGN_IN_WARNING_SECS: i64 = 3 * 24 * 60 * 60;

/// How long before a deadline the operator is told again.
pub const SIGN_IN_FINAL_WARNING_SECS: i64 = 24 * 60 * 60;

/// On whose word a sign-in deadline stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignInExpirySource {
    /// Cairn read it from the provider's token response.
    Provider,
    /// The CLI recorded it at sign-in: the provider's declaration, or where
    /// the provider declared none, the CLI's own 30-day assumption.
    SignIn,
    /// Nothing declared one, so Cairn assumed 30 days from when it took the
    /// credential over.
    Inferred,
}

impl SignInExpirySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::SignIn => "sign_in",
            Self::Inferred => "inferred",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "provider" => Some(Self::Provider),
            "sign_in" => Some(Self::SignIn),
            "inferred" => Some(Self::Inferred),
            _ => None,
        }
    }
}

/// Where an account stands against its sign-in deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignInStage {
    /// More than [`SIGN_IN_WARNING_SECS`] left.
    Current,
    /// Inside [`SIGN_IN_WARNING_SECS`].
    Expiring,
    /// Inside [`SIGN_IN_FINAL_WARNING_SECS`].
    ExpiringSoon,
    /// The deadline has passed: the provider will refuse every renewal.
    Lapsed,
}

/// When a subscription sign-in stops being renewable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignInExpiry {
    /// Unix seconds.
    pub expires_at: i64,
    pub source: SignInExpirySource,
}

impl SignInExpiry {
    /// The deadline a stored Claude credential (the CLI's `claudeAiOauth`
    /// object plus Cairn's fields) carries, if it carries one.
    pub fn from_claude_oauth(json: &str) -> Option<Self> {
        match serde_json::from_str(json).ok()? {
            serde_json::Value::Object(object) => Self::from_claude_object(&object),
            _ => None,
        }
    }

    /// [`Self::from_claude_oauth`], over the parsed object.
    pub fn from_claude_object(object: &serde_json::Map<String, serde_json::Value>) -> Option<Self> {
        let expires_at_ms = object.get(CLAUDE_REFRESH_EXPIRES_AT_FIELD)?.as_i64()?;
        let source = object
            .get(CLAUDE_REFRESH_EXPIRY_SOURCE_FIELD)
            .and_then(serde_json::Value::as_str)
            .and_then(SignInExpirySource::parse)
            .unwrap_or(SignInExpirySource::SignIn);
        Some(Self {
            expires_at: expires_at_ms.div_euclid(1000),
            source,
        })
    }

    pub fn stage(&self, now: i64) -> SignInStage {
        let left = self.expires_at - now;
        if left <= 0 {
            SignInStage::Lapsed
        } else if left <= SIGN_IN_FINAL_WARNING_SECS {
            SignInStage::ExpiringSoon
        } else if left <= SIGN_IN_WARNING_SECS {
            SignInStage::Expiring
        } else {
            SignInStage::Current
        }
    }

    pub fn is_lapsed(&self, now: i64) -> bool {
        self.stage(now) == SignInStage::Lapsed
    }

    /// Why a lapsed account is not routed to, naming the act that restores
    /// it. Unterminated, like every routing rejection.
    pub fn describe_lapse(&self, signed_in_as: &str) -> String {
        format!(
            "its sign-in expired at {}. Reconnect {signed_in_as} from Settings → Providers",
            utc(self.expires_at)
        )
    }

    /// When [`Self::stage`] next changes, if it still will.
    pub fn next_stage_at(&self, now: i64) -> Option<i64> {
        [
            self.expires_at - SIGN_IN_WARNING_SECS,
            self.expires_at - SIGN_IN_FINAL_WARNING_SECS,
            self.expires_at,
        ]
        .into_iter()
        .find(|at| *at > now)
    }
}

/// `2026-11-18 04:50 UTC`.
pub fn utc(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| unix_seconds.to_string())
}

impl ProviderAccount {
    /// When this account's sign-in stops being renewable, for a managed Claude
    /// account whose credential says so. Other accounts declare no deadline.
    pub fn sign_in_expiry(&self) -> Option<SignInExpiry> {
        match &self.auth {
            ProviderAuth::ClaudeProfile { oauth: Some(json) } => {
                SignInExpiry::from_claude_oauth(json)
            }
            _ => None,
        }
    }

    /// Whether this account's sign-in deadline has passed at `now`.
    pub fn sign_in_lapsed(&self, now: i64) -> bool {
        self.sign_in_expiry()
            .is_some_and(|expiry| expiry.is_lapsed(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 24 * 60 * 60;

    fn expiry(json: serde_json::Value) -> Option<SignInExpiry> {
        SignInExpiry::from_claude_oauth(&json.to_string())
    }

    #[test]
    fn reads_the_cli_deadline_and_who_declared_it() {
        let declared = expiry(serde_json::json!({
            "accessToken": "at",
            CLAUDE_REFRESH_EXPIRES_AT_FIELD: 1_795_050_241_877_i64,
            CLAUDE_REFRESH_EXPIRY_SOURCE_FIELD: "provider",
        }))
        .unwrap();
        assert_eq!(declared.expires_at, 1_795_050_241);
        assert_eq!(declared.source, SignInExpirySource::Provider);

        let harvested = expiry(serde_json::json!({
            "accessToken": "at",
            CLAUDE_REFRESH_EXPIRES_AT_FIELD: 5_000_i64,
        }))
        .unwrap();
        assert_eq!(harvested.source, SignInExpirySource::SignIn);

        assert!(expiry(serde_json::json!({"accessToken": "at"})).is_none());
    }

    #[test]
    fn stages_step_at_three_days_one_day_and_the_deadline() {
        let expiry = SignInExpiry {
            expires_at: 10 * DAY,
            source: SignInExpirySource::SignIn,
        };
        assert_eq!(expiry.stage(6 * DAY), SignInStage::Current);
        assert_eq!(expiry.next_stage_at(6 * DAY), Some(7 * DAY));
        assert_eq!(expiry.stage(7 * DAY), SignInStage::Expiring);
        assert_eq!(expiry.next_stage_at(7 * DAY), Some(9 * DAY));
        assert_eq!(expiry.stage(9 * DAY), SignInStage::ExpiringSoon);
        assert_eq!(expiry.stage(10 * DAY), SignInStage::Lapsed);
        assert_eq!(expiry.next_stage_at(10 * DAY), None);
        assert!(expiry.is_lapsed(10 * DAY));
        assert!(!expiry.is_lapsed(10 * DAY - 1));
    }
}
