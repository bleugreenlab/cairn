//! Resource-contract type definitions.
//!
//! The data-free enums and the spec structs the `RESOURCE_CONTRACTS` table is
//! built from. The table itself and the reusable key specs live in sibling
//! modules. `ResourceKind`, the key every contract row carries, lives in
//! `cairn-uri` beside the `CairnResource` grammar it mirrors one-to-one.

use cairn_uri::ResourceKind;

/// Supported operations for the canonical `write` carrier.
///
/// Lives here (not in `cairn-core`) so the contract table and the core
/// dispatcher share one definition. `cairn-core` re-exports it.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeMode {
    Create,
    Append,
    Patch,
    #[serde(rename = "unified_patch")]
    UnifiedPatch,
    Replace,
    Delete,
    Rename,
    Apply,
    Revert,
}

impl ChangeMode {
    /// Every mode, for exhaustive enumeration (parity test).
    pub const ALL: &'static [ChangeMode] = &[
        ChangeMode::Create,
        ChangeMode::Append,
        ChangeMode::Patch,
        ChangeMode::UnifiedPatch,
        ChangeMode::Replace,
        ChangeMode::Delete,
        ChangeMode::Rename,
        ChangeMode::Apply,
        ChangeMode::Revert,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeMode::Create => "create",
            ChangeMode::Append => "append",
            ChangeMode::Patch => "patch",
            ChangeMode::UnifiedPatch => "unified_patch",
            ChangeMode::Replace => "replace",
            ChangeMode::Delete => "delete",
            ChangeMode::Rename => "rename",
            ChangeMode::Apply => "apply",
            ChangeMode::Revert => "revert",
        }
    }
}

/// True for the kinds whose accepted payload keys come from the addressed
/// artifact's JSON Schema at write time rather than from this table, so the
/// contract gate cannot enumerate them.
///
/// These are not unchecked: the artifact handler closes them against the
/// resolved schema in `reject_undeclared_artifact_keys`. This is the same
/// seam the read side carves when it routes artifact kinds to
/// `artifact_affordance_block` instead of the contract-static block.
///
/// Openness cannot be inferred from an empty key list: artifact `create`
/// enumerates nothing and is open, artifact `patch` enumerates eight keys
/// and is *still* open, and `delete` mutations across the table enumerate
/// nothing while being genuinely closed. It has to be declared by kind.
///
/// Thread database rows are the same seam over a different schema: an
/// append or patch payload is a column map, closed against the live table
/// by the row engine (`resources::db_rows`), which names the table's
/// columns when it refuses one.
pub fn payload_keys_are_schema_resolved(kind: ResourceKind) -> bool {
    matches!(
        kind,
        ResourceKind::NodeArtifact
            | ResourceKind::TaskArtifact
            | ResourceKind::ThreadDbTable
            | ResourceKind::ThreadDbRow
    )
}

/// Rough JSON type of a payload key, for documentation in rejections/affordances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Str,
    Bool,
    Int,
    Float,
    Array,
    Object,
    Any,
}

impl KeyType {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyType::Str => "str",
            KeyType::Bool => "bool",
            KeyType::Int => "int",
            KeyType::Float => "float",
            KeyType::Array => "array",
            KeyType::Object => "object",
            KeyType::Any => "any JSON value",
        }
    }
}

/// One payload key a mutation reads.
#[derive(Debug, Clone, Copy)]
pub struct KeySpec {
    /// Canonical key name (advertised in examples).
    pub key: &'static str,
    /// Accepted alternate spellings (e.g. snake_case for a camelCase key).
    ///
    /// Every alias advertised here must be honored by the downstream handler or
    /// deserializer, not just by the gate: `satisfied_by` lets an alias clear the
    /// required-key gate, but a handler that only reads the canonical spelling
    /// would then silently drop the field. The dispatch-level
    /// `advertised_aliases_are_honored_by_dispatch` test pins the gate-observable
    /// (required-key) cases; struct-deserialized optional aliases are pinned
    /// individually (see `agent_frontmatter_honors_model_alias_for_tier`).
    pub aliases: &'static [&'static str],
    pub ty: KeyType,
    note: &'static str,
}

impl KeySpec {
    pub fn note(&self) -> &'static str {
        self.note
    }

    pub(crate) const fn new(key: &'static str, ty: KeyType, note: &'static str) -> Self {
        Self {
            key,
            aliases: &[],
            ty,
            note,
        }
    }

    pub(crate) const fn with_aliases(
        key: &'static str,
        aliases: &'static [&'static str],
        ty: KeyType,
        note: &'static str,
    ) -> Self {
        Self {
            key,
            aliases,
            ty,
            note,
        }
    }

    /// True when `key` or any alias is present among the supplied payload keys.
    pub fn satisfied_by<'a>(&self, mut keys: impl Iterator<Item = &'a str>) -> bool {
        keys.any(|present| present == self.key || self.aliases.contains(&present))
    }

    /// `key(type)` for self-evident keys, `key(type, note)` when the note carries
    /// value guidance (enumerations, defaults). Shared by the affordance blocks
    /// and the system-prompt mutation reference so they never diverge.
    pub fn display(&self) -> String {
        if self.note.is_empty() {
            format!("{}({})", self.key, self.ty.as_str())
        } else {
            format!("{}({}, {})", self.key, self.ty.as_str(), self.note)
        }
    }
}

/// One supported mutation for a resource.
///
/// Editing a `label` or `example` here also changes the affordance blocks
/// rendered on read, which the `affordance_for_*_kind_*` assertion tests in
/// `cairn-core/src/resources/common.rs` consume verbatim. Those tests surface
/// only at a full `bun run test:rust`, never at `bun run check:rust`, so update
/// them in the same change.
#[derive(Debug, Clone, Copy)]
pub struct MutationSpec {
    pub mode: ChangeMode,
    pub required: &'static [KeySpec],
    pub optional: &'static [KeySpec],
    /// Short human label ("create issue", "start terminal").
    pub label: &'static str,
    /// One-line, ready-to-copy `write` call.
    pub example: &'static str,
}

/// ``required `a(str)`; optional `b(bool)` ``, or `None` when nothing is
/// enumerated.
fn accepted_keys_display(required: &[KeySpec], optional: &[KeySpec]) -> Option<String> {
    fn keys(specs: &[KeySpec]) -> String {
        specs
            .iter()
            .map(|k| format!("`{}`", k.display()))
            .collect::<Vec<_>>()
            .join(", ")
    }
    let mut parts: Vec<String> = Vec::new();
    if !required.is_empty() {
        parts.push(format!("required {}", keys(required)));
    }
    if !optional.is_empty() {
        parts.push(format!("optional {}", keys(optional)));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

impl MutationSpec {
    /// ``required `a(str)`; optional `b(bool)` `` — the accepted-key list shared
    /// by the read-side affordance block and the write-side unknown-key
    /// rejection, so what a resource advertises and what it enforces cannot
    /// drift. `None` when the mutation enumerates no keys at all.
    pub fn accepted_keys_display(&self) -> Option<String> {
        accepted_keys_display(self.required, self.optional)
    }

    /// True when `key` is the canonical name or a declared alias of any key this
    /// mutation accepts.
    pub fn accepts_key(&self, key: &str) -> bool {
        self.required
            .iter()
            .chain(self.optional.iter())
            .any(|spec| spec.key == key || spec.aliases.contains(&key))
    }
}

/// A resource that is invoked rather than written: a host-executed `run`
/// target, called as `run({commands:[{target, payload:{args_json}}]})`. Its
/// keys are the named arguments `args_json` carries.
///
/// Kept beside [`ResourceContract`] rather than in it because `mutations` are
/// `write` modes; this is the one place a contract kind says it is also a
/// `run` target, so affordances and help can advertise it.
#[derive(Debug, Clone, Copy)]
pub struct RunTargetSpec {
    pub kind: ResourceKind,
    /// Short human label ("answer typed questions").
    pub label: &'static str,
    pub required: &'static [KeySpec],
    pub optional: &'static [KeySpec],
    /// One-line, ready-to-copy `run` call.
    pub example: &'static str,
}

impl RunTargetSpec {
    /// The accepted-argument list, in the same form a mutation's renders.
    pub fn accepted_keys_display(&self) -> Option<String> {
        accepted_keys_display(self.required, self.optional)
    }
}

/// One read-side query projection (`?key=values`).
#[derive(Debug, Clone, Copy)]
pub struct ProjectionSpec {
    pub key: &'static str,
    pub values: &'static str,
}

/// A related resource surfaced from a resource's affordance block.
#[derive(Debug, Clone, Copy)]
pub struct RelatedSpec {
    pub label: &'static str,
    pub kind: ResourceKind,
    pub actions: bool,
}

/// A mutation that lives on ANOTHER resource but takes THIS resource as input.
///
/// Some workflows can't be expressed by a resource's own mutation set: a recipe
/// is the `{recipe}` input to starting an execution, but that `append` mutation
/// lives on the executions resource, not the recipe. A cross-action references
/// the owning `(kind, mode)` so the affordance can render "this resource feeds
/// that mutation over there" using the target's `uri_template` and example —
/// restoring a deliberate hint the contract-derived affordances otherwise drop.
#[derive(Debug, Clone, Copy)]
pub struct CrossActionSpec {
    /// The resource kind that owns the mutation.
    pub kind: ResourceKind,
    /// Which mutation on that kind this resource feeds.
    pub mode: ChangeMode,
    /// Action label framed from this resource's perspective.
    pub label: &'static str,
}

/// The full contract for one resource kind.
#[derive(Debug, Clone, Copy)]
pub struct ResourceContract {
    pub kind: ResourceKind,
    pub uri_template: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub read_projections: &'static [ProjectionSpec],
    pub related: &'static [RelatedSpec],
    /// Mutations on OTHER resources that take this resource as input. Rendered in
    /// this kind's actions section from the target's `uri_template` + example.
    pub cross_actions: &'static [CrossActionSpec],
    pub mutations: &'static [MutationSpec],
}

impl ResourceContract {
    /// Find the spec for a mode, if this resource supports it.
    pub fn mutation(&self, mode: ChangeMode) -> Option<&'static MutationSpec> {
        self.mutations.iter().find(|spec| spec.mode == mode)
    }
}
