//! Secret registry, shared sanitizer, and the typed crossings that separate
//! credential *use* from credential *disclosure*.
//!
//! # What this module guarantees today
//!
//! This is the foundation slice of CAIRN-3822, not the completed system-wide
//! invariant. [`COVERAGE`] states so in the type system, and it must stay
//! [`Coverage::SinkGuarded`] until every follow-on sink and producer is
//! migrated.
//!
//! Six typed crossings are enforced:
//!
//! 1. **Inbound invocation** — [`CheckedInvocation`] is required before any
//!    model-originated tool call reaches a handler. A call whose recursive tool
//!    input carries a currently registered exact secret is rejected with a
//!    generic, non-secret error *before* any side effect: no preview, no replay
//!    row, no file mutation, no artifact insert, no commit. The payload is never
//!    rewritten — a write that would have to be corrupted to be safe is refused
//!    instead.
//! 2. **Final response** — [`ObservedSafe`] wraps every `DispatchOutput` leaving
//!    authenticated tool dispatch.
//! 3. **Transcript** — [`ObservedSafe`] wraps every backend event on its way to a
//!    `TranscriptEvent`, and transcript serialization accepts only the wrapper.
//! 4. **Live event** — the sanitized value used for persistence is the same
//!    value emitted to frontend subscribers, so raw and sanitized cannot fork.
//! 5. **Process output** — everything a process writes back is scrubbed at the
//!    pipe that produces it, so a terminal's scrollback, its live frames, its
//!    log, and its exit tail are four readers of one scrubbed stream rather than
//!    four chances to forget. Remote output is scrubbed at both ends of the
//!    executor link, because the two registries deliberately hold different
//!    sets. See `docs/secret-redaction.md`.
//! 6. **External tool** — every result, catalog, and error coming back from an
//!    external MCP server is sanitized before any Cairn code sees it. Cairn
//!    hands those servers real credentials, so a server possesses secrets and
//!    the protocol gives it many ways to hand them back. See
//!    [`crate::mcp::untrusted`].
//!
//! Credential *resolution* is brokered rather than accessed: [`broker`] is the
//! one place a stored credential becomes plaintext, and it registers what it
//! resolves before returning a carrier that cannot be logged or persisted.
//!
//! # What it deliberately does not guarantee
//!
//! Redaction protects *observed output*. It is defense in depth, not isolation.
//! It does not protect against a malicious tool transforming a credential before
//! echoing it, against external egress, against binary or image disclosure,
//! against process memory or filesystem access, or against arbitrary
//! fragmentation and encoding. Only the derived forms listed in
//! [`secret::DerivedForms`] are recognized.
//!
//! One further sink is guarded outside these crossings, in
//! `cairn_common::logging`: every record reaching the rotated JSONL files or the
//! stderr layer is scrubbed at the writer. Logging inverts the shape process
//! output has — many call sites, one writer — so the writer is the narrow waist,
//! and a structural test keeps a new layer from configuring a raw one. The
//! durable stores downstream (the check-result cache, the full-text index,
//! embeddings, archival blobs and the CAS) need no seam of their own: each reads
//! a value a crossing already sanitized, and each bounds or hashes it afterwards
//! rather than before.
//!
//! Not yet covered, and gated by named child issues: historical record
//! remediation, including log files already on disk (CAIRN-3828). One
//! CAIRN-3827 residual is open here too:
//! `UserIdentity` still carries its backend credentials as plain `String`s
//! between the identity store and the injection point where the broker
//! registers them. See `docs/secret-redaction.md`.
//!
//! # Use, lease, inject
//!
//! Registration protects observed output; it says nothing about how far a
//! credential itself travels. [`broker`] answers that second question with
//! three dispositions, preferred in this order:
//!
//! 1. **Broker-performed.** The credential never leaves: the broker signs, or
//!    makes the call, and returns a typed non-secret result. GitHub App
//!    authentication works this way.
//! 2. **Leased.** A bearer that must reach its provider becomes a [`lease`]:
//!    the same bytes with a deadline, an audience, and a revocation, where
//!    reaching the plaintext requires naming the destination.
//! 3. **Injected.** A credential a third-party process reads out of its own
//!    environment. Registered and recorded, but the consuming process's blast
//!    radius is unchanged, and nothing here pretends otherwise.
//!
//! # Exact versus structural sanitization
//!
//! The shared sanitizer has two modes, and the difference is deliberate.
//!
//! [`SanitizeMode::ExactOnly`] replaces only values that a credential producer
//! registered with [`registry`], plus their bounded derived forms. This is what
//! the model/transcript crossings use, because those carry the agent's own
//! observed work — file contents, diffs, prose. Structural redaction there would
//! silently mangle legitimate output (a config file that legitimately reads
//! `api_key: ${LINEAR_KEY}`, a test fixture, a doc paragraph about tokens), which
//! is a correctness regression dressed as a security win.
//!
//! [`SanitizeMode::ExactAndStructural`] adds field-name, header, URL, and
//! shaped-value heuristics. It is for *untrusted third-party* payloads where a
//! false positive costs nothing: captured browser network traffic (CAIRN-2692).
//!
//! Both modes share one implementation so there is a single tested definition of
//! what "sanitized" means.

pub mod broker;
pub mod crossing;
pub mod lease;
pub mod sanitize;

// The registry, the secret material, and the streaming scrubber live in
// `cairn-common`: the remote executor scrubs its own output against the same
// implementation, and a second copy of a security primitive is a second thing
// to keep correct. Re-exported as modules so `crate::security::registry` and
// friends stay the one path callers name.
pub use cairn_common::security::{registry, secret, stream};

pub use broker::{BrokeredMcpConfig, BrokeredSecret, CredentialSource, Declared};
pub use cairn_common::security::{REDACTED, REDACTION_MARKER_BYTES};
pub use crossing::{
    CheckedInvocation, Crossing, DetectionReport, ModelInvocation, ObservedSafe,
    RejectedInvocation, Sanitize,
};
// `registry` itself — both the module and the accessor function — already
// arrives with the re-export above.
pub use lease::{
    leases, CredentialLease, LeaseAudience, LeaseBook, LeaseDenied, LeaseId, LeaseRecord,
    LeaseTerms, Presented,
};
pub use registry::{
    Detection, Detections, RegistrationRefused, RegistrySnapshot, SecretGuard, SecretMetadata,
    SecretRegistry,
};
pub use sanitize::{redaction_policy, RedactionPolicy, SanitizeMode, Sanitizer};
pub use secret::{MatchRule, SecretCategory, SecretId, SecretMaterial};
pub use stream::StreamingScrubber;

/// How much of the system the secret invariant actually covers.
///
/// `Enforced` is deliberately absent from this enum. Adding it is the last step
/// of the CAIRN-3822 program, after every child issue's acceptance bar is met;
/// until then no surface may claim the full invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    /// No crossing is guarded. Reserved for a process that has explicitly opted
    /// out; not reachable in a normal runner.
    Unenforced,
    /// The typed crossings in this module are guarded, including process,
    /// terminal, REPL, workflow, and remote-executor output, the durable log
    /// sinks and the stores derived from them, and every migrated credential
    /// producer resolving through the broker. The identity store's own
    /// credential fields are not.
    SinkGuarded,
}

/// The coverage this build actually provides. See [`Coverage`].
pub const COVERAGE: Coverage = Coverage::SinkGuarded;
