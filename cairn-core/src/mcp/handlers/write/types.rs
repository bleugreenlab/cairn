use super::super::target::{target_family, TargetFamily};
use crate::mcp::types::{ChangeItem, ChangeMode};
use crate::resources::mutations::{
    ResourceAppliedChange, ResourceMutationFailure, ResourceTargetHash,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChangeReport {
    pub(super) applied: Vec<AppliedChange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) failures: Vec<ChangeFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) commit: Option<CommitReport>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) partial_success: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) transactional: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) preview: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) event_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) apply_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) target_hashes: Vec<TargetHash>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AppliedChange {
    pub(super) index: usize,
    pub(super) target: String,
    pub(super) mode: String,
    pub(super) kind: String,
    pub(super) summary: String,
    /// Structured echo of the post-mutation state for UI renderers (e.g. the
    /// todos snapshot). Omitted on the wire when absent, so non-todos results
    /// are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) data: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ChangeFailure {
    pub(super) index: usize,
    pub(super) target: String,
    pub(super) mode: String,
    pub(super) kind: String,
    pub(super) error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct CommitReport {
    pub(super) status: String,
    pub(super) sha: Option<String>,
    pub(super) pr_number: Option<i32>,
    pub(super) message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct TargetHash {
    pub(super) target: String,
    pub(super) kind: String,
    pub(super) exists: bool,
    pub(super) hash: String,
}

impl From<ResourceAppliedChange> for AppliedChange {
    fn from(change: ResourceAppliedChange) -> Self {
        Self {
            index: change.index,
            target: change.target,
            mode: change.mode,
            kind: change.kind,
            summary: change.summary,
            data: change.data,
        }
    }
}

impl From<ResourceTargetHash> for TargetHash {
    fn from(hash: ResourceTargetHash) -> Self {
        Self {
            target: hash.target,
            kind: hash.kind,
            exists: hash.exists,
            hash: hash.hash,
        }
    }
}

pub(super) struct IndexedChange<'a> {
    pub(super) index: usize,
    pub(super) item: &'a ChangeItem,
}

#[derive(Debug)]
pub(super) struct IndexedFailure {
    pub(super) failure: ChangeFailure,
    pub(super) commit: Option<CommitReport>,
}

pub(super) type IndexedResult<T> = Result<T, Box<IndexedFailure>>;

pub(super) fn resource_failure(failure: ResourceMutationFailure) -> Box<IndexedFailure> {
    Box::new(IndexedFailure {
        failure: ChangeFailure {
            index: failure.index,
            target: failure.target,
            mode: failure.mode,
            kind: failure.kind,
            error: failure.error,
        },
        commit: None,
    })
}

pub(super) fn empty_change_report(
    applied: Vec<AppliedChange>,
    failures: Vec<ChangeFailure>,
    commit: Option<CommitReport>,
    partial_success: bool,
    transactional: bool,
) -> ChangeReport {
    ChangeReport {
        applied,
        failures,
        commit,
        partial_success,
        transactional,
        preview: false,
        event_uri: None,
        apply_uri: None,
        target_hashes: Vec::new(),
    }
}

fn change_kind_for_target(target: &str) -> &'static str {
    match target_family(target) {
        Ok(TargetFamily::Resource) => "resource",
        Ok(TargetFamily::File) => "file",
        Err(_) => "unknown",
    }
}

pub(super) fn mode_name(mode: ChangeMode) -> &'static str {
    match mode {
        ChangeMode::Create => "create",
        ChangeMode::Append => "append",
        ChangeMode::Patch => "patch",
        ChangeMode::UnifiedPatch => "unified_patch",
        ChangeMode::Replace => "replace",
        ChangeMode::Delete => "delete",
        ChangeMode::Rename => "rename",
        ChangeMode::Apply => "apply",
    }
}

pub(super) fn build_failure(
    index: usize,
    item: &ChangeItem,
    error: impl Into<String>,
) -> Box<IndexedFailure> {
    Box::new(IndexedFailure {
        failure: ChangeFailure {
            index,
            target: item.target.clone(),
            mode: mode_name(item.mode).to_string(),
            kind: change_kind_for_target(&item.target).to_string(),
            error: error.into(),
        },
        commit: None,
    })
}
