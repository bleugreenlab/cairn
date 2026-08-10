//! Secret material, the process-local registry, and the scrubbers that run
//! against it.
//!
//! These are the primitives, and they live here rather than in `cairn-core`
//! because two processes need them. The runner registers the credentials it
//! resolves; the remote executor registers the per-batch relay capability it
//! injects into a batch it is about to run. Scrubbing at both ends of that link
//! is only defense in depth if both ends agree on what "scrubbed" means, and
//! they agree by sharing this code rather than by two implementations staying
//! accidentally aligned.
//!
//! The registry is process-local by design: a registration protects output
//! produced by *this* process, and shipping the live credential set anywhere
//! would be the exact disclosure this module exists to prevent. The runner and
//! the executor therefore hold different sets, which is why output crossing
//! between them is scrubbed at each end against that end's own registry.
//!
//! `cairn-core` layers the structural sanitizer and the typed crossings on top;
//! see `cairn_core::security`.

pub mod registry;
pub mod secret;
pub mod stream;

pub use registry::{
    registry, Detection, Detections, RegistrationRefused, RegistrySnapshot, SecretGuard,
    SecretMetadata, SecretRegistry,
};
pub use secret::{MatchRule, SecretCategory, SecretId, SecretMaterial};
pub use stream::StreamingScrubber;

/// What replaces a redacted value. One marker for every rule: the marker itself
/// must not encode which secret matched, because it appears in output the model
/// and the user both read.
pub const REDACTED: &str = "[REDACTED]";

/// [`REDACTED`] as bytes, for byte-level scrubbing.
pub const REDACTION_MARKER_BYTES: &[u8] = REDACTED.as_bytes();
