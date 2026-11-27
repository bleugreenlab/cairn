//! The extension point for holds and warnings.
//!
//! A hold is something the system is deliberately not doing yet on the
//! operator's behalf (fleet admission closed on unreadable settings), or a
//! warning that waits on an act only the operator can take (unlocking the
//! keychain, reconnecting an account). Like every entry in the center it must
//! wait on the operator: a pacing state that clears by itself, with nothing for
//! him to do, is not a hold source. The producers live with the subsystems that
//! own those decisions; this module only gives them one shape and one place to
//! surface.
//!
//! An override is optional. A hold whose remedy lives elsewhere carries none,
//! and the center renders no button for it, so a button that can only refuse
//! cannot be expressed.
//!
//! A source signals that its answer changed by emitting
//! `db-change {table: "operator_holds", projectId}`, which dirties the
//! notification projection like any other input.

use serde::Serialize;
use std::sync::{Arc, RwLock};

/// When a hold expects to let go: at a known instant, or in words when the
/// release depends on something with no clock ("position 3 of 7").
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExpectedRelease {
    /// Unix seconds.
    At(i64),
    Text(String),
}

/// One hold, as its source reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    pub id: String,
    /// The project the hold applies to, or `None` for a workspace-wide hold,
    /// which every project's listing carries once.
    pub project_id: Option<String>,
    pub reason: String,
    pub expected_release: Option<ExpectedRelease>,
    /// What the source's override button says, when the source can release
    /// the hold on the operator's say-so. `None` renders no button.
    pub override_label: Option<String>,
    /// Where the held thing lives, when it has an address.
    pub source_uri: Option<String>,
    /// Unix seconds.
    pub opened_at: i64,
    /// Unix seconds. A released hold is still reported for a while so the
    /// center's "all" view can show it as resolved.
    pub released_at: Option<i64>,
}

pub trait HoldSource: Send + Sync {
    /// Stable name, used in entry keys and to route an override back here.
    fn name(&self) -> &'static str;
    /// Live holds for the project, plus recently released ones.
    fn holds(&self, project_id: &str) -> Vec<Hold>;
    /// Release the hold now, on the operator's say-so. Only a source whose
    /// holds carry an `override_label` implements it; the default refuses,
    /// because a source without an override never offers the button.
    fn override_hold(&self, _hold_id: &str) -> Result<(), String> {
        Err(format!("{} offers no override", self.name()))
    }
}

/// The registered sources.
#[derive(Default)]
pub struct HoldRegistry {
    sources: RwLock<Vec<Arc<dyn HoldSource>>>,
}

impl HoldRegistry {
    pub(crate) fn register(&self, source: Arc<dyn HoldSource>) {
        if let Ok(mut sources) = self.sources.write() {
            sources.retain(|existing| existing.name() != source.name());
            sources.push(source);
        }
    }

    /// Every hold any source reports for the project, with its source's name.
    pub(crate) fn holds_for(&self, project_id: &str) -> Vec<(&'static str, Hold)> {
        let Ok(sources) = self.sources.read() else {
            return Vec::new();
        };
        sources
            .iter()
            .flat_map(|source| {
                let name = source.name();
                source
                    .holds(project_id)
                    .into_iter()
                    .filter(|hold| {
                        hold.project_id
                            .as_deref()
                            .is_none_or(|owner| owner == project_id)
                    })
                    .map(move |hold| (name, hold))
            })
            .collect()
    }

    pub(crate) fn override_hold(&self, source: &str, hold_id: &str) -> Result<(), String> {
        let found = self.sources.read().ok().and_then(|sources| {
            sources
                .iter()
                .find(|candidate| candidate.name() == source)
                .cloned()
        });
        match found {
            Some(source) => source.override_hold(hold_id),
            None => Err(format!("no hold source named {source}")),
        }
    }
}
