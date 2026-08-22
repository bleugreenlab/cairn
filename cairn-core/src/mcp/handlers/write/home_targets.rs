//! Home-relative (`cairn:~`) targets across one write batch.
//!
//! `cairn:~` names the calling run's own home, and the write handler resolves
//! every home-relative target to canonical BEFORE any item applies, because
//! blocking-append classification reads the raw target ahead of the dispatch
//! that would otherwise resolve it.
//!
//! Resolving once, against the home the batch started with, is right for every
//! batch but the one a new thread always sends first: rename this thread off its
//! `thread-<n>` placeholder, then write its arc. The rename moves the home the
//! second item names, so a resolution taken before it lands addresses a thread
//! that no longer exists — and the resulting rejection names only the old name,
//! which the caller never typed.
//!
//! So this pass carries the one event that provably moves a run's home
//! mid-batch: a rename of the run's OWN thread. Items after it resolve against
//! the new name, and an item that resolved through a rename which did not apply
//! is refused naming that dependency rather than dispatched at a URI nothing
//! answers to. Nothing else re-resolves. Under `atomic:false`, general per-item
//! re-resolution would make a target depend on how many earlier items happened
//! to succeed, which is harder to reason about than a stale shorthand.

use super::types::AppliedChange;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::mutations::{is_home_relative_target, resolve_change_target_against_home};
use cairn_common::uri::{canonicalize_uri_identity, parse_uri, CairnResource};

/// A rename of the run's own thread, sitting earlier in the same batch.
#[derive(Clone)]
struct HomeRename {
    index: usize,
    new_name: String,
}

/// A thread rename can move only the node-relative anchor. The project anchor is
/// stable across that rename, so a later `cairn:@` item must not depend on the
/// rename landing in a partial batch.
fn rename_dependency(shorthand: &str, rename: &Option<HomeRename>) -> Option<HomeRename> {
    (shorthand == "cairn:~" || shorthand.starts_with("cairn:~/"))
        .then(|| rename.clone())
        .flatten()
}

/// How one item's `cairn:~` shorthand was resolved.
enum Origin {
    Resolved {
        /// The shorthand the caller actually typed.
        shorthand: String,
        /// The canonical URI it became, and the item's target from here on.
        resolved: String,
        /// The earlier item whose rename this resolution passed through.
        via_rename: Option<HomeRename>,
    },
    /// The run's home could not be resolved, so the shorthand names nothing
    /// canonical and the item's target was left as written.
    Unresolved { shorthand: String, error: String },
}

/// Where the run's home moved to, and the name that moved it.
struct RenamedHome {
    home_uri: String,
    new_name: String,
}

/// Per-item record of every `cairn:~` target a batch carried.
#[derive(Default)]
pub(super) struct HomeTargets {
    /// Indexed by batch position; `None` for an item that named no shorthand.
    origins: Vec<Option<Origin>>,
}

/// Resolve every home-relative target in `changes` to canonical, in place.
pub(super) async fn resolve(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    changes: &mut [ChangeItem],
) -> HomeTargets {
    // A batch that names no shorthand needs no home, and must not be made to
    // depend on one resolving.
    if !changes
        .iter()
        .any(|item| is_home_relative_target(&item.target))
    {
        return HomeTargets::default();
    }

    let home = crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request).await;
    let mut home_uri = home.clone().unwrap_or_default();
    let mut rename: Option<HomeRename> = None;
    let mut origins = Vec::with_capacity(changes.len());

    for (index, item) in changes.iter_mut().enumerate() {
        let origin = match (&home, is_home_relative_target(&item.target)) {
            (_, false) => None,
            (Err(error), true) => Some(Origin::Unresolved {
                shorthand: item.target.clone(),
                error: error.clone(),
            }),
            (Ok(_), true) => {
                let shorthand = std::mem::take(&mut item.target);
                let resolved = resolve_change_target_against_home(&home_uri, &shorthand)
                    .expect("a home-relative target joins onto a home URI");
                item.target = resolved.clone();
                let via_rename = rename_dependency(&shorthand, &rename);
                Some(Origin::Resolved {
                    shorthand,
                    resolved,
                    via_rename,
                })
            }
        };
        origins.push(origin);

        // Asked after this item's own target is canonical, so a rename written
        // as `cairn:~` is recognized the same as one written in full.
        if home.is_ok() {
            if let Some(renamed) = renamed_home(&home_uri, item) {
                home_uri = renamed.home_uri;
                rename = Some(HomeRename {
                    index,
                    new_name: renamed.new_name,
                });
            }
        }
    }

    HomeTargets { origins }
}

/// Where this item moves the run's home, if it renames the thread the home
/// belongs to.
///
/// The item must address that thread ITSELF: a patch of something beneath it —
/// the arc, a message — is not a rename however its payload reads. A run whose
/// home is a task beneath the thread moves with it, because the thread name is a
/// segment of the task's address too.
fn renamed_home(home_uri: &str, item: &ChangeItem) -> Option<RenamedHome> {
    if item.mode != ChangeMode::Patch {
        return None;
    }
    // `parse_uri` is the judge of whether this home is a thread's at all, and of
    // where the thread name ends. The address itself is rebuilt from the home
    // URI's own text, because parsing canonicalizes the project segment's case
    // while the home URI carries the project key as the database spells it — and
    // an uppercase-keyed project would otherwise never match its own thread.
    let CairnResource::Thread { name, .. } = parse_uri(home_uri)? else {
        return None;
    };
    let (key, rest) = home_uri.strip_prefix("cairn://p/")?.split_once('/')?;
    // Whatever hung beneath the thread in the home URI still hangs beneath it.
    let beneath = rest.strip_prefix(name.as_str())?;
    let thread_uri = format!("cairn://p/{key}/{name}");
    if canonicalize_uri_identity(&item.target) != canonicalize_uri_identity(&thread_uri) {
        return None;
    }
    let new_name = item.payload.as_ref()?.get("name")?.as_str()?.trim();
    if new_name.is_empty() || new_name == name {
        return None;
    }
    Some(RenamedHome {
        home_uri: format!("cairn://p/{key}/{new_name}{beneath}"),
        new_name: new_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn patch(target: &str, payload: serde_json::Value) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode: ChangeMode::Patch,
            payload: Some(payload),
        }
    }

    #[test]
    fn only_node_relative_targets_depend_on_a_preceding_home_rename() {
        let rename = Some(HomeRename {
            index: 0,
            new_name: "execution".to_string(),
        });
        assert!(rename_dependency("cairn:~/arc", &rename).is_some());
        assert!(rename_dependency("cairn:@/messages", &rename).is_none());
        assert!(rename_dependency("cairn:@messages", &rename).is_none());
    }

    #[test]
    fn a_thread_session_moves_its_own_home() {
        let renamed = renamed_home(
            "cairn://p/cairn/thread-7",
            &patch("cairn://p/cairn/thread-7", json!({"name": "execution"})),
        )
        .expect("the run's own thread was renamed");
        assert_eq!(renamed.home_uri, "cairn://p/cairn/execution");
        assert_eq!(renamed.new_name, "execution");
    }

    /// A task's home is an address BENEATH the thread, and the thread name is a
    /// segment of it, so the task moves with the rename that moves its thread.
    #[test]
    fn a_task_beneath_the_thread_moves_with_it() {
        let renamed = renamed_home(
            "cairn://p/cairn/thread-7/task/probe",
            &patch("cairn://p/cairn/thread-7", json!({"name": "execution"})),
        )
        .expect("the task's own thread was renamed");
        assert_eq!(renamed.home_uri, "cairn://p/cairn/execution/task/probe");
    }

    /// Parsing lowercases the project segment while the home URI carries the key
    /// as the database spells it. Rebuilding the thread address from the parsed
    /// project made an uppercase-keyed project fail to recognize its own rename,
    /// silently — the batch simply went back to resolving a stale name.
    #[test]
    fn an_uppercase_project_key_still_recognizes_its_own_rename() {
        let renamed = renamed_home(
            "cairn://p/MCP/thread-7",
            &patch("cairn://p/MCP/thread-7", json!({"name": "execution"})),
        )
        .expect("case is not identity");
        assert_eq!(renamed.home_uri, "cairn://p/MCP/execution");
        // And the same rename written in the other case is the same rename.
        assert!(renamed_home(
            "cairn://p/MCP/thread-7",
            &patch("cairn://p/mcp/thread-7", json!({"name": "execution"})),
        )
        .is_some());
    }

    #[test]
    fn nothing_else_moves_the_home() {
        // A patch of something BENEATH the thread is not a rename, however its
        // payload reads.
        assert!(renamed_home(
            "cairn://p/cairn/thread-7",
            &patch("cairn://p/cairn/thread-7/arc", json!({"name": "execution"})),
        )
        .is_none());
        // Another thread's rename is not this run's home moving.
        assert!(renamed_home(
            "cairn://p/cairn/thread-7",
            &patch("cairn://p/cairn/library", json!({"name": "execution"})),
        )
        .is_none());
        // A patch that changes anything but the name leaves the address alone.
        assert!(renamed_home(
            "cairn://p/cairn/thread-7",
            &patch("cairn://p/cairn/thread-7", json!({"status": "closed"})),
        )
        .is_none());
        // A rename to the name it already has moves nothing.
        assert!(renamed_home(
            "cairn://p/cairn/thread-7",
            &patch("cairn://p/cairn/thread-7", json!({"name": "thread-7"})),
        )
        .is_none());
        // An issue node's home has no thread to rename.
        assert!(renamed_home(
            "cairn://p/cairn/4403/1/builder",
            &patch(
                "cairn://p/cairn/4403/1/builder",
                json!({"name": "execution"})
            ),
        )
        .is_none());
    }
}

impl HomeTargets {
    /// Why this item cannot be dispatched at all, if it cannot.
    ///
    /// `applied` is the batch's progress so far, which is what answers whether a
    /// rename an item resolved through actually landed.
    pub(super) fn blocked(&self, index: usize, applied: &[AppliedChange]) -> Option<String> {
        match self.origins.get(index)?.as_ref()? {
            Origin::Unresolved { shorthand, error } => Some(format!(
                "`{shorthand}` names this run's home, which could not be resolved: {error}"
            )),
            Origin::Resolved {
                shorthand,
                resolved,
                via_rename: Some(rename),
            } => {
                let landed = applied.iter().any(|change| change.index == rename.index);
                (!landed).then(|| {
                    format!(
                        "item {index}'s `{shorthand}` resolves through item {}'s rename of this \
                         run's thread to '{}', which did not apply — so `{resolved}` names \
                         nothing. Nothing was written for this item; re-send it once the rename \
                         lands.",
                        rename.index, rename.new_name
                    )
                })
            }
            Origin::Resolved { .. } => None,
        }
    }

    /// Restate a failure in terms of what the caller wrote.
    ///
    /// A home-derived item fails against a canonical URI the caller never typed,
    /// and a bare `Thread 'executor' not found in cairn` then describes a lookup
    /// rather than a cause. Naming the bridge — shorthand to canonical — is what
    /// makes the rejection traceable back to the target as written.
    pub(super) fn annotate(&self, index: usize, error: String) -> String {
        match self.origins.get(index).and_then(Option::as_ref) {
            Some(Origin::Resolved {
                shorthand,
                resolved,
                ..
            }) => format!("{error} (`{shorthand}` resolved to `{resolved}`)"),
            _ => error,
        }
    }
}
