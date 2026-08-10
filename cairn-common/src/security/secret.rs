//! Non-leaking secret material and its non-secret identity.
//!
//! [`SecretMaterial`] is the only type in Cairn that holds credential plaintext
//! for scrubbing purposes. It has no `Debug`, no `Display`, no `serde`, no
//! equality, and no error conversion, so there is no formatting or serialization
//! API through which its bytes can reach a log line, a transcript, or a panic
//! message. `security::crossing` tests that absence at compile time.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// Shortest value that may be registered for scrubbing.
///
/// A short value is not a credential worth scrubbing; it is a false-positive
/// generator. Registering `"true"` or a four-character token would replace that
/// substring everywhere it legitimately appears in observed output, corrupting
/// far more than it protects.
pub const MIN_REGISTERABLE_LEN: usize = 12;

/// Fewest distinct bytes a registerable value must contain.
///
/// A conservative stand-in for entropy: real credentials (base64 secrets, UUIDs,
/// provider keys) clear it comfortably, while a long run of one character or a
/// short repeated pattern does not. This is a guard against a degenerate
/// registration, not a cryptographic entropy estimate.
pub const MIN_DISTINCT_BYTES: usize = 6;

/// Stable, non-secret identity for one registered secret.
///
/// The id names the *producer* (`mcp-callback`, `mcp-server:linear:API_KEY`),
/// never the value, so it is safe in logs, detection reports, and metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SecretId(String);

impl SecretId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What kind of credential a registered value is.
///
/// Non-secret metadata. It classifies the producer so a detection report can say
/// *which* credential leaked without carrying the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretCategory {
    /// The runner's MCP callback bearer credential (`CAIRN_MCP_SECRET`).
    CallbackCredential,
    /// A configured external MCP server's resolved `${VAR}` value, from the OS
    /// keychain or the process environment.
    ConfiguredMcp,
    /// An OAuth access token for a configured MCP server. Distinguished from
    /// [`Self::ConfiguredMcp`] because it is short-lived and refreshable: a
    /// detection naming this category tells an operator that rotating the
    /// credential means re-authorizing, not editing settings.
    OAuthToken,
    /// An API key for a web search or web fetch provider, sent to a third-party
    /// service on every query.
    ProviderKey,
    /// A short-lived per-batch executor relay capability (CAIRN-3385). Secret
    /// equivalent: possession authorizes relayed MCP calls for that batch.
    BatchCapability,
    /// The desktop operator credential (`~/.cairn/operator_auth_secret`), which
    /// distinguishes a real desktop answer to an authority prompt from anything
    /// else on the machine that can reach loopback (CAIRN-3834).
    ///
    /// Unlike the callback credential this one is never injected into an agent,
    /// so scrubbing is not protecting an intentional exposure. It is the last
    /// line behind the sandbox read-denylist and the protected-path refusals
    /// that keep an agent away from the file. Under `fence: allow` there is no
    /// sandbox left to enforce with, and this is what still stands between a
    /// shell read of the file and a value the agent can use.
    OperatorCredential,
    /// A long-lived asymmetric key Cairn *signs with* rather than sends: the
    /// GitHub App private key. It authenticates Cairn as the application
    /// itself, so it outranks every token derived from it — one signature mints
    /// a token for any repository the app is installed on.
    ///
    /// It is the category most worth never handing out, and the broker never
    /// does: it signs inside and returns the short-lived result. Registering it
    /// covers the residual case of a provider echoing it back in an error.
    /// Rotating means generating a new key on the app, not re-authenticating.
    ProviderSigningKey,
    /// A short-lived bearer a provider minted for Cairn — a GitHub App
    /// installation token, a team sync token. Distinguished from
    /// [`Self::ProviderSigningKey`] because it expires on its own: a detection
    /// naming this category tells an operator the exposure has a deadline, and
    /// that the remedy is revoking the lease rather than rotating a stored key.
    ProviderToken,
    /// A model backend's API key or OAuth token — Anthropic, OpenAI. Injected
    /// into the agent subprocess environment, which is the plaintext exposure
    /// this system cannot yet remove: the backend CLI reads it from there.
    /// Rotating means re-authenticating that backend account.
    ModelBackendKey,
    /// This desktop's own account credential — the device JWT it presents to
    /// the Cairn API. Everything the account can do, it authorizes. Rotating
    /// means signing out and back in on this machine.
    AccountToken,
}

impl SecretCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CallbackCredential => "callback-credential",
            Self::ConfiguredMcp => "configured-mcp",
            Self::OAuthToken => "oauth-token",
            Self::ProviderKey => "provider-key",
            Self::BatchCapability => "batch-capability",
            Self::OperatorCredential => "operator-credential",
            Self::ProviderSigningKey => "provider-signing-key",
            Self::ProviderToken => "provider-token",
            Self::ModelBackendKey => "model-backend-key",
            Self::AccountToken => "account-token",
        }
    }
}

/// Which derived form of a registered secret a detection matched.
///
/// The set is deliberately closed. Recognizing an open-ended space of encodings
/// would imply a guarantee this system does not provide; see the module docs on
/// what redaction is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MatchRule {
    /// The credential's own UTF-8 bytes.
    ExactRaw,
    /// RFC 3986 percent-encoding of the raw bytes (upper-case hex).
    ExactPercentUpper,
    /// Percent-encoding with lower-case hex, which some encoders emit.
    ExactPercentLower,
    /// Standard base64 of the raw bytes, padded.
    ExactBase64,
    /// Standard base64 of the raw bytes, unpadded.
    ExactBase64NoPad,
    /// URL-safe base64 of the raw bytes, padded.
    ExactBase64Url,
    /// URL-safe base64 of the raw bytes, unpadded.
    ExactBase64UrlNoPad,
    /// A structural heuristic (sensitive field name, bearer/JWT/assignment
    /// shape, URL userinfo). Never used to reject an invocation.
    Structural,
}

impl MatchRule {
    pub fn is_exact(self) -> bool {
        !matches!(self, Self::Structural)
    }
}

/// Credential plaintext, held only so that observed output can be scrubbed of
/// it.
///
/// Deliberately missing: `Debug`, `Display`, `Serialize`, `Deserialize`, `Clone`,
/// `PartialEq`, and any `From`/`Into` that would let it become an error message.
/// Storage zeroizes on drop, which narrows the window in which the bytes sit in
/// this process's heap — it is not a claim about copies a dependency or the
/// operating system already made.
pub struct SecretMaterial {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretMaterial {
    /// Take ownership of a resolved credential.
    ///
    /// Takes the `String` by value and moves its buffer, so the caller cannot
    /// keep an un-zeroized copy by accident.
    pub fn from_string(value: String) -> Self {
        Self {
            bytes: Zeroizing::new(value.into_bytes()),
        }
    }

    /// Adopt raw credential bytes.
    ///
    /// Prefer this over [`Self::from_string`] for a credential that exists on
    /// disk or on the wire in **both** raw and encoded form. [`Self::derived_forms`]
    /// computes encodings *of whatever it is given*, so registering the raw
    /// bytes yields rules for the raw value and its base64 forms, while
    /// registering the base64 text yields rules for the text and base64-of-text
    /// — leaving the raw bytes uncovered.
    pub fn from_bytes(value: &[u8]) -> Self {
        Self {
            bytes: Zeroizing::new(value.to_vec()),
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Whether this value is long and varied enough to scrub for without
    /// generating false positives. See [`MIN_REGISTERABLE_LEN`] and
    /// [`MIN_DISTINCT_BYTES`].
    pub(crate) fn is_scrubbable(&self) -> bool {
        if self.bytes.len() < MIN_REGISTERABLE_LEN {
            return false;
        }
        let distinct: HashSet<u8> = self.bytes.iter().copied().collect();
        distinct.len() >= MIN_DISTINCT_BYTES
    }

    /// The bounded set of forms this credential is recognized in.
    ///
    /// Every form is derived once at registration and held zeroized. Duplicate
    /// encodings (an all-unreserved value percent-encodes to itself; padded and
    /// unpadded base64 coincide when the length is a multiple of three) are
    /// dropped, keeping the raw rule as the reported one.
    pub(crate) fn derived_forms(&self) -> DerivedForms {
        use base64::Engine;

        let mut forms: Vec<(MatchRule, Zeroizing<Vec<u8>>)> = Vec::new();
        let mut push = |rule: MatchRule, value: Vec<u8>| {
            let value = Zeroizing::new(value);
            if value.is_empty() || forms.iter().any(|(_, existing)| **existing == *value) {
                return;
            }
            forms.push((rule, value));
        };

        push(MatchRule::ExactRaw, self.bytes.to_vec());
        push(
            MatchRule::ExactPercentUpper,
            percent_encode(&self.bytes, true),
        );
        push(
            MatchRule::ExactPercentLower,
            percent_encode(&self.bytes, false),
        );
        push(
            MatchRule::ExactBase64,
            base64::engine::general_purpose::STANDARD
                .encode(&*self.bytes)
                .into_bytes(),
        );
        push(
            MatchRule::ExactBase64NoPad,
            base64::engine::general_purpose::STANDARD_NO_PAD
                .encode(&*self.bytes)
                .into_bytes(),
        );
        push(
            MatchRule::ExactBase64Url,
            base64::engine::general_purpose::URL_SAFE
                .encode(&*self.bytes)
                .into_bytes(),
        );
        push(
            MatchRule::ExactBase64UrlNoPad,
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&*self.bytes)
                .into_bytes(),
        );

        DerivedForms { forms }
    }
}

/// Every recognized encoding of one registered credential.
pub(crate) struct DerivedForms {
    forms: Vec<(MatchRule, Zeroizing<Vec<u8>>)>,
}

impl DerivedForms {
    pub(crate) fn into_vec(self) -> Vec<(MatchRule, Zeroizing<Vec<u8>>)> {
        self.forms
    }
}

/// RFC 3986 percent-encoding of arbitrary bytes, preserving only the unreserved
/// set (`ALPHA / DIGIT / "-" / "." / "_" / "~"`).
fn percent_encode(bytes: &[u8], upper: bool) -> Vec<u8> {
    const UPPER: &[u8; 16] = b"0123456789ABCDEF";
    const LOWER: &[u8; 16] = b"0123456789abcdef";
    let digits = if upper { UPPER } else { LOWER };
    let mut out = Vec::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte);
        } else {
            out.push(b'%');
            out.push(digits[(byte >> 4) as usize]);
            out.push(digits[(byte & 0x0f) as usize]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_or_low_variety_values_are_not_scrubbable() {
        assert!(!SecretMaterial::from_string("short".into()).is_scrubbable());
        assert!(!SecretMaterial::from_string("aaaaaaaaaaaaaaaaaaaa".into()).is_scrubbable());
        assert!(!SecretMaterial::from_string("ababababababab".into()).is_scrubbable());
        assert!(SecretMaterial::from_string("sk-live-9fA3xQ2mZ7".into()).is_scrubbable());
    }

    #[test]
    fn derived_forms_cover_every_documented_encoding() {
        use base64::Engine;

        // Chosen so its standard base64 contains `+`, which makes the URL-safe
        // alphabet produce a genuinely different string. For most inputs the two
        // coincide and dedupe collapses them, which is correct but proves less.
        let value = "Zx9~Qw8~Lm7~";
        let material = SecretMaterial::from_string(value.to_string());
        let forms = material.derived_forms().into_vec();
        let bytes: Vec<&[u8]> = forms.iter().map(|(_, form)| form.as_slice()).collect();

        let standard = base64::engine::general_purpose::STANDARD.encode(value);
        assert!(
            standard.contains('+'),
            "fixture must exercise the url alphabet"
        );

        for expected in [
            value.to_string(),
            "Zx9~Qw8~Lm7~".to_string(),
            standard.clone(),
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(value),
            base64::engine::general_purpose::URL_SAFE.encode(value),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value),
        ] {
            assert!(
                bytes.contains(&expected.as_bytes()),
                "missing derived form {expected}"
            );
        }

        let rules: Vec<MatchRule> = forms.iter().map(|(rule, _)| *rule).collect();
        assert!(rules.contains(&MatchRule::ExactRaw));
        assert!(rules.contains(&MatchRule::ExactBase64));
        assert!(rules.contains(&MatchRule::ExactBase64Url));
    }

    #[test]
    fn identical_encodings_are_registered_once() {
        // An all-unreserved value percent-encodes to itself, so the percent
        // rules must not add a duplicate needle for the raw bytes.
        let plain = SecretMaterial::from_string("abcdefghijklmnop".into());
        let forms = plain.derived_forms().into_vec();
        let percent_rules = forms
            .iter()
            .filter(|(rule, _)| {
                matches!(
                    rule,
                    MatchRule::ExactPercentUpper | MatchRule::ExactPercentLower
                )
            })
            .count();
        assert_eq!(percent_rules, 0);

        let mut seen: Vec<&[u8]> = forms.iter().map(|(_, form)| form.as_slice()).collect();
        let total = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), total, "forms must be distinct");
    }

    /// Compile-time proof that no formatting or serialization API can reveal
    /// [`SecretMaterial`].
    ///
    /// The inherent method wins method resolution only when its bound is
    /// satisfied; otherwise the blanket trait method answers. So `implements()`
    /// reporting `false` *is* the absence of the impl, checked by the compiler
    /// rather than by a reviewer reading the derive list.
    mod no_leaking_impls {
        /// Build a probe for one trait: an inherent method guarded by the trait
        /// bound, shadowing a blanket fallback. Each probe needs its own type,
        /// because two inherent impls of the same method on one type collide.
        macro_rules! absence_probe {
            ($module:ident, $bound:path, $test:ident, $message:literal) => {
                mod $module {
                    use crate::security::secret::SecretMaterial;
                    use std::marker::PhantomData;

                    pub struct Probe<T>(PhantomData<T>);

                    pub trait Absent {
                        fn implements() -> bool {
                            false
                        }
                    }
                    impl<T> Absent for Probe<T> {}

                    impl<T: $bound> Probe<T> {
                        fn implements() -> bool {
                            true
                        }
                    }

                    #[test]
                    fn $test() {
                        assert!(!Probe::<SecretMaterial>::implements(), $message);
                        // The probe reports `true` for a type that does
                        // implement the trait, so a false negative here would be
                        // the probe itself being broken.
                        assert!(Probe::<String>::implements(), "probe is inert");
                    }
                }
            };
        }

        absence_probe!(
            debug,
            std::fmt::Debug,
            secret_material_has_no_debug,
            "SecretMaterial must not be formattable"
        );
        absence_probe!(
            display,
            std::fmt::Display,
            secret_material_has_no_display,
            "SecretMaterial must not be displayable"
        );
        absence_probe!(
            serialize,
            serde::Serialize,
            secret_material_has_no_serde,
            "SecretMaterial must not be serializable"
        );
        absence_probe!(
            clone,
            Clone,
            secret_material_cannot_be_cloned,
            "SecretMaterial must not be cloneable into an un-zeroized copy"
        );
    }

    #[test]
    fn percent_encoding_matches_rfc_3986_unreserved_set() {
        assert_eq!(percent_encode(b"a-b.c_d~e", true), b"a-b.c_d~e".to_vec());
        assert_eq!(percent_encode(b"a/b", true), b"a%2Fb".to_vec());
        assert_eq!(percent_encode(b"a/b", false), b"a%2fb".to_vec());
    }
}
