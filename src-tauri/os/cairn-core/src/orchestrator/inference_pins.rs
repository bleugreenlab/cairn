//! The engine pins this host follows: which release of each engine the
//! shipped templates run (`cairn_inference::engine_pins`).
//!
//! The pins are followed by the managed CLIs' rule
//! (`cairn_models::managed::follow`): the file published to
//! `engine-pins-v{schema}` when it validates for this build, else the last one
//! that did (cached under `<cairn_home>/inference/engine-pins/`), else the one
//! compiled in; outside production the compiled-in file wins, so a branch's
//! bump is what its dev instance offers. They are read again at startup and
//! every [`UPDATE_INTERVAL`].
//!
//! Only what new profiles start from follows the pins: generation's starting
//! profiles and the template list. Nothing is installed here. A native
//! engine's artifact is installed on the head by the profile's own rendered
//! start script, into a directory named by its sha256, and a profile embeds
//! the engine it was made with, so a new pin never changes an existing
//! profile or a running deployment.

use std::sync::{Arc, OnceLock, RwLock};

use cairn_inference::engine_pins::{self, EnginePins};
use cairn_models::managed::follow::Follow;
use cairn_models::managed::spec::published_url;
use cairn_models::managed::{PinOrigin, PublishedPin, UPDATE_INTERVAL};

static CURRENT: RwLock<Option<(Arc<EnginePins>, PinOrigin)>> = RwLock::new(None);

fn embedded() -> Arc<EnginePins> {
    static EMBEDDED: OnceLock<Arc<EnginePins>> = OnceLock::new();
    EMBEDDED
        .get_or_init(|| Arc::new(EnginePins::embedded().clone()))
        .clone()
}

/// The engine pins new profiles start from now: the compiled-in ones until
/// the first follow has read the publication.
pub fn current() -> Arc<EnginePins> {
    match &*CURRENT
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        Some((pins, _)) => pins.clone(),
        None => embedded(),
    }
}

/// Read the effective pins once, blocking on the publication's fetch.
fn follow_once() {
    let source = PublishedPin::new(
        published_url(&engine_pins::release_tag(), engine_pins::ENGINE_PINS_FILE),
        "engine-pins",
    );
    let cache_dir = cairn_common::paths::cairn_home()
        .join("inference")
        .join("engine-pins");
    let (pins, origin) = Follow {
        name: "engine pins",
        source: &source,
        cache_dir: &cache_dir,
        embedded: EnginePins::embedded(),
        prefer_embedded: cairn_common::paths::is_dev(),
    }
    // A cached file is validated as it is read back: it deserializes only
    // through the same check a published one passes.
    .effective(EnginePins::parse, |_| true);
    let mut current = CURRENT
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let changed = current
        .as_ref()
        .is_none_or(|(was, was_origin)| **was != pins || *was_origin != origin);
    if changed {
        log::info!("engine pins: following the {origin} engine pins");
    }
    *current = Some((Arc::new(pins), origin));
}

impl super::Orchestrator {
    /// Follow the published engine pins: once now, then every
    /// [`UPDATE_INTERVAL`].
    pub fn spawn_engine_pins_follow(&self) {
        cairn_common::task::spawn(async move {
            let mut interval = tokio::time::interval(UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(error) = tokio::task::spawn_blocking(follow_once).await {
                    log::warn!("engine pins: the follow task failed: {error}");
                }
            }
        });
    }
}
