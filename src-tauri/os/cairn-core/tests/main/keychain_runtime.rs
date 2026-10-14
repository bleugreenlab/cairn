//! The OS keychain must stay readable from inside the async runtime.
//!
//! On Linux the keychain backend is `keyring`'s Secret Service client, and the
//! Cargo feature that selects its D-Bus stack also selects how its synchronous
//! `Entry` API waits on the asynchronous protocol underneath. Under zbus's
//! `tokio` feature that wait is `Runtime::block_on` against a shared
//! current-thread runtime, which panics — "cannot start a runtime from within a
//! runtime" — the moment it is reached from a thread already inside a Tokio
//! runtime. Every keychain read in this codebase is reached exactly that way,
//! from invoke handlers the transport runs on its core runtime. Under zbus's
//! `async-io` feature the wait is `async_io::block_on`, which drives the future
//! on the calling thread and is indifferent to any ambient runtime.
//!
//! Nothing about that distinction is visible at a call site: both spellings
//! compile, and the difference appears only as a panic on Linux at runtime. So
//! it is asserted here instead, with a keychain read performed the way
//! production performs it — directly on a multi-threaded runtime worker, not
//! deferred to `spawn_blocking`.
//!
//! This lives in the integration binary on purpose. The crate's unit tests
//! install a process-wide in-memory `CredentialBuilder`, which would replace
//! the very backend under test; an integration test links the library without
//! `cfg(test)`, so the real OS backend is what answers.
//!
//! No Secret Service daemon is required, and whether a secret is found is not
//! the point. A headless host has no session bus and answers `None`, which is
//! the intended degradation. What is proved is that an answer arrives at all.
//!
//! Linux-only, because the invariant is Linux-only: the macOS and Windows
//! backends are synchronous native calls with no runtime underneath them, so
//! running this elsewhere would read a developer's real keychain to assert
//! something that cannot fail there.

use cairn_core::security::broker;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keychain_reads_resolve_on_a_runtime_worker() {
    // The broker is the only sanctioned reader of a credential store, and
    // `web_provider_key` is its synchronous keychain path: it opens an `Entry`
    // and reads it, which on Linux is the complete Secret Service round trip.
    // The provider name is a probe no settings file can configure, so this is a
    // pure read that stores nothing and cannot collide with a real credential.
    //
    // Returning at all is the assertion, so the result is discarded rather than
    // examined. Whether a credential comes back is ambient OS state this test
    // does not control, while the failure it guards against is unambiguous:
    // under the `tokio` spelling of the backend's runtime feature this call
    // panics and never returns a value to inspect.
    let _ = broker::web_provider_key(
        "cairn-keychain-runtime-probe",
        "TOKEN",
        "keychain runtime-nesting regression test",
    );
}
