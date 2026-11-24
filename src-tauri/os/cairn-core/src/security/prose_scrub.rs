//! Secret stripping for prose bound for a hosted model: an issue's title and
//! description, sent to a decide offer or an embedding endpoint.
//!
//! The model crossings run the shared sanitizer exact-only, because the
//! agent's own observed work must not be mangled (see the module docs of
//! [`crate::security`]). Prose an operator filed and Cairn forwards to a
//! third party is the other case: a false positive costs a word of context,
//! a false negative leaks a credential. So this runs, in order:
//!
//! 1. Credential shapes the publisher refuses: Anthropic and OpenAI-style
//!    keys, PEM private-key blocks, AWS access-key ids, GitHub and Slack
//!    tokens, and URL userinfo.
//! 2. The shared sanitizer in structural mode: every value a credential
//!    producer registered (the configured secret literals), bearer and basic
//!    blobs, JWTs, and `token|secret|password|api_key = value` assignments.
//! 3. Long mixed-class tokens: 32 or more characters of key alphabet mixing
//!    upper case, lower case and digits (UUIDs and git hashes are lower case
//!    and survive).
//!
//! The result is rescanned with the same layers; any remaining match refuses
//! the text, so a caller fails closed rather than sending it.
//!
//! These are the layers of `bench/lib/redact.py`, which cairn/6271's measured
//! filing text went through, less its operator-literal file: in production the
//! registry holds the literals.

use std::sync::LazyLock;

use regex::Regex;

use super::sanitize::{RedactionPolicy, Sanitizer};
use super::REDACTED;

/// Why prose could not be made safe to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubRefused {
    /// The layers that still matched after scrubbing.
    pub remaining: Vec<&'static str>,
}

impl std::fmt::Display for ScrubRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "secret shapes remained after scrubbing ({}); nothing was sent",
            self.remaining.join(", ")
        )
    }
}

struct Shape {
    label: &'static str,
    pattern: Regex,
    replacement: &'static str,
}

static SHAPES: LazyLock<Vec<Shape>> = LazyLock::new(|| {
    let shape = |label, pattern: &str, replacement| Shape {
        label,
        pattern: Regex::new(pattern).expect("valid regex"),
        replacement,
    };
    vec![
        shape("anthropic_key", r"sk-ant-[A-Za-z0-9_\-]{8,}", REDACTED),
        shape("openai_style_key", r"\bsk-[A-Za-z0-9_\-]{20,}", REDACTED),
        shape(
            "private_key_block",
            r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|\z)",
            REDACTED,
        ),
        shape("aws_access_key", r"\bAKIA[0-9A-Z]{16}\b", REDACTED),
        shape("github_token", r"\bgh[pousr]_[A-Za-z0-9]{30,}\b", REDACTED),
        shape("slack_token", r"\bxox[abposr]-[A-Za-z0-9-]{8,}", REDACTED),
        shape(
            "url_userinfo",
            r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/@\[\]]+@",
            "${1}[REDACTED]@",
        ),
    ]
});

/// A maximal run of key alphabet; runs are maximal, so no boundary check is
/// needed beyond the length.
static KEY_RUN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[A-Za-z0-9_\-+=]{32,}").expect("valid regex"));

fn mixed_class(token: &str) -> bool {
    token.chars().any(|c| c.is_ascii_uppercase())
        && token.chars().any(|c| c.is_ascii_lowercase())
        && token.chars().any(|c| c.is_ascii_digit())
}

fn strip_long_tokens(text: &str) -> String {
    KEY_RUN
        .replace_all(text, |caps: &regex::Captures<'_>| {
            let token = &caps[0];
            if mixed_class(token) {
                REDACTED.to_string()
            } else {
                token.to_string()
            }
        })
        .into_owned()
}

fn remaining(text: &str, policy: &RedactionPolicy) -> Vec<&'static str> {
    let mut hits: Vec<&'static str> = SHAPES
        .iter()
        .filter(|shape| match shape.label {
            // A scrubbed userinfo leaves `://[REDACTED]@`, which is the shape.
            "url_userinfo" => shape
                .pattern
                .find_iter(text)
                .any(|m| !m.as_str().contains(REDACTED)),
            _ => shape.pattern.is_match(text),
        })
        .map(|shape| shape.label)
        .collect();
    let mut sanitizer = Sanitizer::structural(policy);
    if sanitizer.text(text) != text {
        hits.push("structural");
    }
    if KEY_RUN.find_iter(text).any(|m| mixed_class(m.as_str())) {
        hits.push("long_mixed_token");
    }
    hits
}

/// `text` with every secret shape replaced by [`REDACTED`], or a refusal
/// when the rescan still finds one.
pub fn scrub_prose(text: &str) -> Result<String, ScrubRefused> {
    let policy = RedactionPolicy::default();
    let mut out = text.to_string();
    for shape in SHAPES.iter() {
        out = shape
            .pattern
            .replace_all(&out, shape.replacement)
            .into_owned();
    }
    let mut sanitizer = Sanitizer::structural(&policy);
    out = sanitizer.text(&out).into_owned();
    out = strip_long_tokens(&out);
    let remaining = remaining(&out, &policy);
    if remaining.is_empty() {
        Ok(out)
    } else {
        Err(ScrubRefused { remaining })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_shape_is_removed_and_ordinary_prose_survives() {
        let samples = [
            "key is sk-ant-api03-AbCdEfGh12345678_xyz ok",
            "OPENAI=sk-proj9a8b7c6d5e4f3g2h1i0jKLMNOP",
            "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\n-----END OPENSSH PRIVATE KEY-----",
            "aws AKIAIOSFODNN7EXAMPLE here",
            "push with ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789 now",
            "xoxb-1234-5678-abcdefgh",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
            "Authorization: Bearer abcdefgh.ijklmnop",
            "password = hunter22 and api_key: q1w2e3r4",
            "session Zx9Qw8Er7Ty6Ui5Op4As3Df2Gh1Jk0LmNb end",
            "clone https://deploy:s3cretpass@git.example.test/repo.git",
        ];
        let secrets = [
            "AbCdEfGh12345678",
            "9a8b7c6d5e4f3g2h1i0j",
            "b3BlbnNzaC1rZXktdjEAAAAA",
            "AKIAIOSFODNN7EXAMPLE",
            "aBcDeFgHiJkLmNoPqRsTuVwXyZ",
            "xoxb-1234",
            "dozjgNryP4J3",
            "abcdefgh.ijklmnop",
            "hunter22",
            "Zx9Qw8Er7Ty6",
            "s3cretpass",
        ];
        for (sample, secret) in samples.iter().zip(secrets) {
            let out = scrub_prose(sample).unwrap();
            assert!(!out.contains(secret), "{sample} -> {out}");
            assert!(out.contains(REDACTED), "{sample} -> {out}");
        }
        let keep = "job 3415b3cd-ecdd-48de-a54b-b9dbe6b353b1 at 25ce52d1a09f8e7d6c5b4a39281706f5e4d3c2b1 in src/lib.rs; see https://example.test/a";
        assert_eq!(scrub_prose(keep).unwrap(), keep);
    }
}
