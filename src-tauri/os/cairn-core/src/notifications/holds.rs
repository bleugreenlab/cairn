//! The extension point for holds and warnings.
//!
//! A hold is something the system is deliberately not doing yet on the
//! operator's behalf (an execution queued behind fleet capacity, work paced
//! against a weekly quota), with an expected release and an override the
//! operator can take. The producers live with the subsystems that own those
//! decisions; this module only gives them one shape and one place to surface.
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
    /// What the override button says ("Start now").
    pub override_label: String,
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
    /// Release the hold now, on the operator's say-so.
    fn override_hold(&self, hold_id: &str) -> Result<(), String>;
}

/// The registered sources. None are registered in production yet.
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
