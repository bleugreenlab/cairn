//! Following a published pin: the one rule every pin Cairn follows at run time
//! obeys, the managed CLIs' ([`super::ManagedTool`]) and the inference engines'
//! (`cairn_inference::engine_pins`) alike.
//!
//! The effective pin is the published one when it is accepted for this build;
//! else the last published one that was, cached as `<dir>/pin.json` with its
//! ETag; else the pin compiled into the build. Outside production the
//! compiled-in pin wins outright, so a branch's bump is what its dev instance
//! runs. What "accepted" means is the follower's: a managed CLI's pin must be
//! for this build's integration and carry a package for this host, an engine
//! pin file must be for this build's schema and pin every template it ships.

use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{write_json_atomically, PinFetch, PinOrigin, PinSource};

/// The file a follower caches the last accepted published pin in.
pub const CACHED_PIN: &str = "pin.json";

/// `<dir>/pin.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CachedPin<P> {
    pub(super) etag: Option<String>,
    pub(super) pin: P,
}

/// One pin to follow.
pub struct Follow<'a, P> {
    /// What log lines name it by (`managed codex`, `engine pins`).
    pub name: &'a str,
    pub source: &'a dyn PinSource,
    /// Where [`CACHED_PIN`] lives.
    pub cache_dir: &'a Path,
    /// The pin compiled into this build.
    pub embedded: &'a P,
    /// Follow `embedded` and never the published pin.
    pub prefer_embedded: bool,
}

impl<P: Clone + Serialize + DeserializeOwned> Follow<'_, P> {
    /// The pin to follow and where it came from. `accept` parses published
    /// text into a pin this build may use, or says why not; `still_valid`
    /// re-checks a cached one, which an upgrade can make stale.
    pub fn effective(
        &self,
        accept: impl Fn(&str) -> Result<P, String>,
        still_valid: impl Fn(&P) -> bool,
    ) -> (P, PinOrigin) {
        if self.prefer_embedded {
            return (self.embedded.clone(), PinOrigin::Embedded);
        }
        let name = self.name;
        let cached = std::fs::read(self.cache_dir.join(CACHED_PIN))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<CachedPin<P>>(&bytes).ok())
            .filter(|cached| still_valid(&cached.pin));
        let fallback = |cached: Option<CachedPin<P>>| match cached {
            Some(cached) => (cached.pin, PinOrigin::Cached),
            None => (self.embedded.clone(), PinOrigin::Embedded),
        };
        match self
            .source
            .fetch(cached.as_ref().and_then(|cached| cached.etag.as_deref()))
        {
            Ok(PinFetch::NotModified) => match cached {
                Some(cached) => (cached.pin, PinOrigin::Published),
                None => fallback(None),
            },
            Ok(PinFetch::Body { text, etag }) => match accept(&text) {
                Ok(pin) => {
                    let record = CachedPin {
                        etag,
                        pin: pin.clone(),
                    };
                    if let Err(error) = write_json_atomically(self.cache_dir, CACHED_PIN, &record) {
                        log::warn!("{name}: could not cache the published pin: {error}");
                    }
                    (pin, PinOrigin::Published)
                }
                Err(error) => {
                    log::warn!("{name}: ignoring the published pin: {error}");
                    fallback(cached)
                }
            },
            Err(error) => {
                log::info!("{name}: the published pin is unreachable: {error}");
                fallback(cached)
            }
        }
    }
}
