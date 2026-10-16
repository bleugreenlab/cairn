use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::execution::gates::vocabulary::{GateFact, GateVocabulary};

/// One clause in the `when` grammar: an AND of field tests over a named fact
/// source. Clauses OR with each other wherever a list of them appears — a
/// route's trigger nodes, a thread definition's standing triggers.
pub type TriggerClause = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Scalar,
    List,
}

/// Where a thread standing trigger is evaluated from, and where the row it is
/// evaluated against lives. Routes take no scope: a route set is already
/// selected per project, so only thread evaluation carries one.
#[derive(Debug, Clone, Copy)]
pub struct ThreadScope<'a> {
    /// The project of the thread's job; `None` when it cannot be resolved.
    pub home_project_id: Option<&'a str>,
    pub home_project_key: Option<&'a str>,
    /// The fact log row's project; `None` for a workspace (project-less) row.
    pub row_project_id: Option<&'a str>,
}

/// Which scope kept a row from a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeExclusion {
    /// The clause names no `project`, so the default applied: the thread's own
    /// project plus project-less rows.
    OwnProject,
    /// The clause names `project`, which replaces the default and admits only
    /// rows in the projects it names.
    NamedProjects,
}

/// The jurisdiction rule for every recipient the fact log's index lane wakes.
/// `named` is the recipient's `project` test, if it has one. Without one, a
/// row reaches the recipient when it is project-less or in the recipient's own
/// project, so a recipient whose project is unknown sees no scoped row. With
/// one, a row reaches it only when the row belongs to a project and its
/// `project` field passes that test.
pub fn scope_exclusion(
    named: Option<&Value>,
    fact: &Fact<'_>,
    scope: &ThreadScope<'_>,
) -> Option<ScopeExclusion> {
    match named {
        None => scope
            .row_project_id
            .is_some_and(|row| scope.home_project_id != Some(row))
            .then_some(ScopeExclusion::OwnProject),
        Some(expected) => {
            let inside = scope.row_project_id.is_some()
                && fact
                    .fields
                    .get("project")
                    .is_some_and(|actual| value_matches(actual, expected));
            (!inside).then_some(ScopeExclusion::NamedProjects)
        }
    }
}

fn describe_exclusion(
    exclusion: ScopeExclusion,
    clause: &BTreeMap<String, Value>,
    fact: &Fact<'_>,
    scope: &ThreadScope<'_>,
) -> String {
    let row = match (scope.row_project_id, fact.fields.get("project")) {
        (None, _) => "a workspace event with no project".to_string(),
        (Some(_), Some(Value::String(key))) => format!("in project {key}"),
        (Some(id), _) => format!("in project {id}"),
    };
    match exclusion {
        ScopeExclusion::OwnProject => format!(
            "project: the clause names none, so the default scope applies (this thread's own project {} plus workspace events); the row is {row}",
            scope.home_project_key.unwrap_or("(unresolved)")
        ),
        ScopeExclusion::NamedProjects => format!(
            "project: the clause names {}, which replaces the own-project default; the row is {row}",
            clause.get("project").cloned().unwrap_or(Value::Null)
        ),
    }
}

/// Explain every test in one trigger clause. This shares the exact matcher for
/// its verdict so editor diagnostics cannot drift from dispatch behavior. A
/// thread's clause passes its `scope`, and a row outside it is explained first
/// by which scope excluded it; a route's passes `None`.
pub fn explain_clause(
    clause: &BTreeMap<String, Value>,
    fact: &Fact<'_>,
    scope: Option<&ThreadScope<'_>>,
) -> Vec<String> {
    let exclusion =
        scope.and_then(|scope| Some((scope, scope_exclusion(clause.get("project"), fact, scope)?)));
    let scope_failure =
        exclusion.map(|(scope, exclusion)| describe_exclusion(exclusion, clause, fact, scope));
    let project_explained =
        exclusion.is_some_and(|(_, exclusion)| exclusion == ScopeExclusion::NamedProjects);
    let field_failures = clause
        .iter()
        .filter(|(key, _)| !(project_explained && key.as_str() == "project"))
        .filter_map(|(key, expected)| {
            let one = BTreeMap::from([(key.clone(), expected.clone())]);
            clause_matches(&one, fact)
                .then_some(())
                .map(|_| None)
                .unwrap_or_else(|| {
                    let actual = match key.as_str() {
                        "fact" => Value::String(fact.source.into()),
                        "presence" => Value::String(
                            match fact.presence {
                                Presence::Active => "active",
                                Presence::Away => "away",
                            }
                            .into(),
                        ),
                        key if key.ends_with("Prefix") => fact
                            .fields
                            .get(key.trim_end_matches("Prefix"))
                            .cloned()
                            .unwrap_or(Value::Null),
                        field => fact.fields.get(field).cloned().unwrap_or(Value::Null),
                    };
                    Some(match (&actual, expected) {
                        (Value::Array(_), Value::Array(_)) if !key.ends_with("Prefix") => {
                            format!("{key}: list {actual} does not intersect {expected}")
                        }
                        (Value::Array(_), _) if !key.ends_with("Prefix") => {
                            format!("{key}: list {actual} does not contain {expected}")
                        }
                        _ => format!("{key} expected {expected}, observed {actual}"),
                    })
                })
        });
    scope_failure.into_iter().chain(field_failures).collect()
}

/// Where a field's legal values come from. Declaring this with the field is what
/// lets an editor offer the actual values instead of a free-text box: a closed
/// enum ships its variants inline, and the collection-backed vocabularies name a
/// collection the client already knows how to list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldVocabulary {
    /// Any string: URIs, identifiers, and message text.
    Free,
    Enum(&'static [&'static str]),
    Labels,
    Projects,
}

const ISSUE_STATUSES: &[&str] = &[
    "backlog", "active", "waiting", "complete", "failed", "merged", "closed",
];
const ISSUE_ATTENTIONS: &[&str] = &[
    "none",
    "needs_input",
    "needs_authorization",
    "needs_approval",
    "idle",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    Active,
    Away,
}

#[derive(Debug, Clone)]
pub struct Fact<'a> {
    pub source: &'a str,
    pub fields: &'a BTreeMap<String, Value>,
    pub presence: Presence,
}

pub use crate::orchestrator::wakes::MutedDelivery;
pub use crate::storage::LogDedupe;

/// Who receives an undirected channel sink (`ChannelDestination::Subscriptions`)
/// for a fact of this source. A sink addressed to one conversation always goes
/// to that conversation's provider; the audience only answers "which providers
/// does the default destination mean".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Audience {
    /// Every registered channel provider.
    AllProviders,
    /// Only providers holding a live follow on the URI in this field, and only
    /// for facts that happened after the follow began.
    FollowersOf(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub kind: FieldKind,
    /// Whether the fact log row carries this field. A field that is not stored
    /// is still matchable and bindable: the log worker restores it from the
    /// canonical record the row points at (`routes::log::hydrate`). Content, such
    /// as a transcript utterance, is never copied into the log.
    pub stored: bool,
}

#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub fields: HashMap<&'static str, FieldSpec>,
    pub dedupe: LogDedupe,
    pub audience: Audience,
    /// The longest stored scalar for this source, when it is not the log's
    /// default bound. Only a source whose row is the sole local copy of its
    /// content sets this, and then to the external record's own limit, so a
    /// route never sees less than the producer observed.
    pub max_stored_chars: Option<usize>,
    /// Stored fields whose values are canonical URIs naming who the fact is
    /// addressed to. The index lane expands each to the jobs it reaches now and
    /// wakes them with no standing trigger, beside the clause consumers and
    /// under the same actor, jurisdiction, and one-wake-per-row rules. Empty
    /// for a source whose rows address no one: a field like `mentions` on
    /// `thread_stream` is then matchable but delivers nothing by itself.
    pub addressees: &'static [&'static str],
    /// The stored field naming the home whose own writing a row is, beside
    /// the minting job. The index lane resolves it when the row is indexed and
    /// excludes that job too, so a home that rotated sessions between mint and
    /// indexing is still never woken by its own words.
    pub actor_home: Option<&'static str>,
    /// Whether list fields are stored whole rather than cut at the log's list
    /// bound. Set for sources whose lists are reference sets the lane resolves
    /// or matches (`mentions`): a cut list would silently drop recipients and
    /// matches past the bound. Each entry is still bounded.
    pub whole_lists: bool,
    /// What a recipient that muted this source's wake still receives.
    pub muted: MutedDelivery,
}

/// GitHub's own limit on an issue or pull request comment body.
const GITHUB_COMMENT_MAX_CHARS: usize = 65_536;

impl SourceSpec {
    pub fn is_hydrated(&self, field: &str) -> bool {
        self.fields.get(field).is_some_and(|spec| !spec.stored)
    }
}

/// Which question a fact source answers, and therefore which authoring surface
/// composes from it.
///
/// One registry holds the whole fact space. The surface says what a source is
/// FOR, not which registry it lives in: a `Trigger` source is matched against a
/// live event by [`matches`], a `Gate` source is level-resolved by query in
/// [`crate::execution::gates::vocabulary`]. Keeping both in one registry is what
/// stops a second predicate vocabulary growing beside this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactSurface {
    Trigger,
    Gate,
}

#[derive(Debug, Clone)]
struct TriggerSource {
    /// Declaration order, so a refusal can name the vocabulary in the order it
    /// is documented rather than in hash order.
    order: usize,
    spec: SourceSpec,
}

/// The whole fact space, across both surfaces.
///
/// The two halves are stored differently on purpose. Trigger sources are field
/// declarations plus how the fact log stores them — dispatch matches them
/// generically, so there is no per-source code to own. A gate source carries a
/// RESOLVER, so the registry HOLDS the gate vocabulary rather than copying field
/// metadata out of it: registering a resolver is then the same act as
/// registering its authoring metadata, and there is exactly one place either can
/// be added. Copying would make `register` update the resolver a sweep uses
/// while leaving the shape an author is offered stale — two registries wearing
/// one name.
#[derive(Debug, Clone)]
pub struct FactRegistry {
    triggers: HashMap<&'static str, TriggerSource>,
    gates: GateVocabulary,
}

const FILE_CHANGE_STATUSES: &[&str] = &["added", "modified", "deleted", "renamed"];

/// The value vocabulary for one fact field. Fields not named here are free text.
fn vocabulary_for(source: &str, field: &str) -> FieldVocabulary {
    use crate::execution::gates::vocabulary::{GATE_PR_STATES, GATE_RESOLUTIONS, GATE_VERDICTS};
    match (source, field) {
        ("file_changed", "status") => FieldVocabulary::Enum(FILE_CHANGE_STATUSES),
        (_, "status") => FieldVocabulary::Enum(ISSUE_STATUSES),
        (_, "attention") => FieldVocabulary::Enum(ISSUE_ATTENTIONS),
        (_, "label") => FieldVocabulary::Labels,
        (_, "project") => FieldVocabulary::Projects,
        (_, "resolution") => FieldVocabulary::Enum(GATE_RESOLUTIONS),
        (_, "state") => FieldVocabulary::Enum(GATE_PR_STATES),
        (_, "verdict") => FieldVocabulary::Enum(GATE_VERDICTS),
        _ => FieldVocabulary::Free,
    }
}

const STORED: bool = true;
const HYDRATED: bool = false;

fn fields(entries: &[(&'static str, FieldKind, bool)]) -> HashMap<&'static str, FieldSpec> {
    entries
        .iter()
        .map(|&(name, kind, stored)| (name, FieldSpec { kind, stored }))
        .collect()
}

impl Default for FactRegistry {
    fn default() -> Self {
        use FieldKind::{List, Scalar};
        let attention = SourceSpec {
            dedupe: LogDedupe::Transition,
            audience: Audience::AllProviders,
            max_stored_chars: None,
            addressees: &[],
            actor_home: None,
            whole_lists: false,
            muted: MutedDelivery::Drop,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("attention", Scalar, STORED),
                ("status", Scalar, STORED),
                ("label", List, STORED),
                ("detailUri", Scalar, STORED),
                ("text", Scalar, STORED),
            ]),
        };
        let thread = SourceSpec {
            dedupe: LogDedupe::Unique,
            audience: Audience::FollowersOf("threadUri"),
            max_stored_chars: None,
            addressees: &[],
            actor_home: None,
            whole_lists: true,
            muted: MutedDelivery::Drop,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("threadUri", Scalar, STORED),
                ("detailUri", Scalar, STORED),
                ("text", Scalar, HYDRATED),
                ("context", Scalar, STORED),
                ("jobId", Scalar, STORED),
                ("mentions", List, STORED),
            ]),
        };
        // The relay holds GitHub events only in memory, so the row is the only
        // local copy of the comment (its canonical record is the comment on
        // GitHub, at `url`). The comment text is stored once, whole, up to
        // GitHub's own limit; `body` is the same text under a provenance header
        // and is rebuilt from the stored fields rather than stored twice.
        let github_comment = SourceSpec {
            dedupe: LogDedupe::Unique,
            audience: Audience::AllProviders,
            max_stored_chars: Some(GITHUB_COMMENT_MAX_CHARS),
            addressees: &[],
            actor_home: None,
            whole_lists: false,
            muted: MutedDelivery::Drop,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("repository", Scalar, STORED),
                ("number", Scalar, STORED),
                ("kind", Scalar, STORED),
                ("author", Scalar, STORED),
                ("url", Scalar, STORED),
                ("title", Scalar, STORED),
                ("body", Scalar, HYDRATED),
                ("text", Scalar, STORED),
            ]),
        };
        let file_changed = SourceSpec {
            dedupe: LogDedupe::Unique,
            audience: Audience::AllProviders,
            max_stored_chars: None,
            addressees: &[],
            actor_home: None,
            whole_lists: false,
            muted: MutedDelivery::Drop,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("path", Scalar, STORED),
                ("previousPath", Scalar, STORED),
                ("status", Scalar, STORED),
                ("node", Scalar, STORED),
                ("additions", Scalar, STORED),
                ("deletions", Scalar, STORED),
            ]),
        };
        // A post row is an index entry: the writing stays in `posts` and
        // `post_comments` and is rendered into the wake from there. Authors
        // and mentions are home or resource URIs, resolved to a job only at
        // delivery, so a thread that rotated sessions receives on the new one.
        // A muted Posts watch is still told, quietly (CAIRN-1900).
        let post_created = SourceSpec {
            dedupe: LogDedupe::Unique,
            audience: Audience::AllProviders,
            max_stored_chars: None,
            addressees: &["mentions"],
            actor_home: Some("author"),
            whole_lists: true,
            muted: MutedDelivery::RideAlong,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("postUri", Scalar, STORED),
                ("author", Scalar, STORED),
                ("scope", Scalar, STORED),
                ("title", Scalar, STORED),
                ("mentions", List, STORED),
            ]),
        };
        // A comment is not a feed event: it addresses the post's author and
        // whoever it names, and reaches anyone else only through a trigger.
        let post_commented = SourceSpec {
            dedupe: LogDedupe::Unique,
            audience: Audience::AllProviders,
            max_stored_chars: None,
            addressees: &["postAuthor", "mentions"],
            actor_home: Some("author"),
            whole_lists: true,
            muted: MutedDelivery::RideAlong,
            fields: fields(&[
                ("project", Scalar, STORED),
                ("postUri", Scalar, STORED),
                ("commentUri", Scalar, STORED),
                ("author", Scalar, STORED),
                ("postAuthor", Scalar, STORED),
                ("mentions", List, STORED),
            ]),
        };
        let triggers = [
            ("attention", attention),
            ("thread_stream", thread),
            ("github_comment", github_comment),
            ("file_changed", file_changed),
            ("post_created", post_created),
            ("post_commented", post_commented),
        ]
        .into_iter()
        .enumerate()
        .map(|(order, (fact, spec))| (fact, TriggerSource { order, spec }))
        .collect();
        Self {
            triggers,
            gates: GateVocabulary::default(),
        }
    }
}

/// One fact source and its fields, ordered for display. The settings surface
/// composes its trigger controls from this, so the authoring vocabulary and the
/// vocabulary `validate_clause` enforces are the same list.
#[derive(Debug, Clone)]
pub struct FactSourceShape {
    pub fact: &'static str,
    pub fields: Vec<FactFieldShape>,
}

#[derive(Debug, Clone)]
pub struct FactFieldShape {
    pub name: &'static str,
    pub kind: FieldKind,
    pub vocabulary: FieldVocabulary,
}

impl FactRegistry {
    /// The TRIGGER sources, which is what the settings surface composes its
    /// trigger controls from. Gate facts are level-resolved and belong to the
    /// `after:` authoring surface, not to this one.
    pub fn describe(&self) -> Vec<FactSourceShape> {
        self.describe_surface(FactSurface::Trigger)
    }

    /// A registry over a custom gate vocabulary. The trigger half is fixed —
    /// those sources are dispatch's, not an author's — so this is the whole of
    /// what an extension can change.
    pub fn with_gates(gates: GateVocabulary) -> Self {
        Self {
            gates,
            ..Self::default()
        }
    }

    /// Register a gate fact. This is the ONE registration point: the resolver a
    /// sweep will call and the shape an author is offered are the same object,
    /// so they cannot be added to one surface and missing from the other.
    pub fn register_gate(&mut self, fact: Arc<dyn GateFact>) {
        self.gates.register(fact);
    }

    /// The gate vocabulary this registry resolves `after:` predicates through.
    pub fn gates(&self) -> &GateVocabulary {
        &self.gates
    }

    pub fn describe_surface(&self, surface: FactSurface) -> Vec<FactSourceShape> {
        let shape = |fact: &'static str, fields: Vec<(&'static str, FieldKind)>| {
            let mut fields: Vec<_> = fields
                .into_iter()
                .map(|(name, kind)| FactFieldShape {
                    name,
                    kind,
                    vocabulary: vocabulary_for(fact, name),
                })
                .collect();
            fields.sort_by_key(|field| field.name);
            FactSourceShape { fact, fields }
        };
        let mut sources: Vec<FactSourceShape> = match surface {
            FactSurface::Trigger => self
                .triggers
                .iter()
                .map(|(fact, source)| {
                    let fields = source
                        .spec
                        .fields
                        .iter()
                        .map(|(name, field)| (*name, field.kind))
                        .collect();
                    shape(fact, fields)
                })
                .collect(),
            // Projected from the vocabulary rather than stored, so a resolver
            // registered at runtime is described here without a second write.
            FactSurface::Gate => self
                .gates
                .sources()
                .iter()
                .map(|source| shape(source.fact(), source.fields().to_vec()))
                .collect(),
        };
        sources.sort_by_key(|source| source.fact);
        sources
    }

    /// The fact names on one surface, in declaration order — the list a refusal
    /// names so an author learns the vocabulary from being refused.
    pub fn fact_names(&self, surface: FactSurface) -> Vec<&'static str> {
        match surface {
            FactSurface::Trigger => {
                let mut sources: Vec<(usize, &'static str)> = self
                    .triggers
                    .iter()
                    .map(|(fact, source)| (source.order, *fact))
                    .collect();
                sources.sort_by_key(|(order, _)| *order);
                sources.into_iter().map(|(_, fact)| fact).collect()
            }
            FactSurface::Gate => self.gates.fact_names(),
        }
    }

    pub fn source_has_field(&self, source: &str, field: &str) -> bool {
        if let Some(trigger) = self.triggers.get(source) {
            return trigger.spec.fields.contains_key(field);
        }
        self.gates
            .sources()
            .iter()
            .find(|gate| gate.fact() == source)
            .is_some_and(|gate| gate.fields().iter().any(|(name, _)| *name == field))
    }

    /// How the fact log stores and delivers one TRIGGER source. Gate facts are
    /// resolved by query, never logged, so they have no spec.
    pub fn source(&self, source: &str) -> Option<&SourceSpec> {
        self.triggers.get(source).map(|trigger| &trigger.spec)
    }

    /// Every trigger source name, sorted.
    pub fn source_names(&self) -> Vec<&'static str> {
        let mut names = self.fact_names(FactSurface::Trigger);
        names.sort_unstable();
        names
    }

    pub fn validate_clause(
        &self,
        source: &str,
        clause: &BTreeMap<String, Value>,
    ) -> Result<(), String> {
        // A gate fact is level-resolved, so there is no stream to match it
        // against and a route may not name one.
        let fields = &self
            .triggers
            .get(source)
            .ok_or_else(|| format!("unknown fact source '{source}'"))?
            .spec
            .fields;
        for key in clause.keys() {
            if key == "fact" || key == "presence" {
                continue;
            }
            let base = key.strip_suffix("Prefix").unwrap_or(key);
            if !fields.contains_key(base) {
                return Err(format!("unknown field '{key}' for fact '{source}'"));
            }
        }
        if let Some(value) = clause.get("presence") {
            if !matches!(value.as_str(), Some("active" | "away")) {
                return Err("presence must be active or away".into());
            }
        }
        Ok(())
    }
}

pub fn matches(clauses: &[BTreeMap<String, Value>], fact: &Fact<'_>) -> bool {
    clauses.iter().any(|clause| clause_matches(clause, fact))
}

fn clause_matches(clause: &BTreeMap<String, Value>, fact: &Fact<'_>) -> bool {
    clause.iter().all(|(key, expected)| match key.as_str() {
        "fact" => expected.as_str() == Some(fact.source),
        "presence" => {
            expected.as_str()
                == Some(match fact.presence {
                    Presence::Active => "active",
                    Presence::Away => "away",
                })
        }
        key if key.ends_with("Prefix") => {
            let field = key.trim_end_matches("Prefix");
            fact.fields
                .get(field)
                .and_then(Value::as_str)
                .zip(expected.as_str())
                .is_some_and(|(actual, prefix)| actual.starts_with(prefix))
        }
        field => fact
            .fields
            .get(field)
            .is_some_and(|actual| value_matches(actual, expected)),
    })
}

/// One field test. An array on either side means membership: a list field
/// matches a scalar it contains or an array it intersects, and a scalar field
/// matches an array that contains it. Scalar against scalar is equality.
/// Shared unchanged with the `after:` gate vocabulary, so
/// `resolution: ["merged", "closed"]` means there exactly what it means in a
/// route trigger.
pub(crate) fn value_matches(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Array(actual), Value::Array(expected)) => {
            let set: HashSet<_> = actual.iter().collect();
            expected.iter().any(|value| set.contains(value))
        }
        (Value::Array(actual), expected) => actual.contains(expected),
        (actual, Value::Array(expected)) => expected.contains(actual),
        (actual, expected) => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicates_are_or_of_ands_and_support_prefix_and_presence() {
        let clauses: Vec<BTreeMap<String, Value>> = serde_yaml::from_str(
            "- { fact: attention, status: [failed], presence: away }\n- { fact: thread_stream, detailUriPrefix: 'cairn://p/cairn/', presence: away }"
        ).unwrap();
        let fields = BTreeMap::from([
            (
                "detailUri".into(),
                Value::String("cairn://p/cairn/1".into()),
            ),
            ("status".into(), Value::String("active".into())),
        ]);
        assert!(matches(
            &clauses,
            &Fact {
                source: "thread_stream",
                fields: &fields,
                presence: Presence::Away
            }
        ));
        assert!(!matches(
            &clauses,
            &Fact {
                source: "thread_stream",
                fields: &fields,
                presence: Presence::Active
            }
        ));
    }

    #[test]
    fn describe_exposes_every_source_and_field_the_validator_accepts() {
        let registry = FactRegistry::default();
        let described = registry.describe();
        assert_eq!(
            described.iter().map(|s| s.fact).collect::<Vec<_>>(),
            vec![
                "attention",
                "file_changed",
                "github_comment",
                "post_commented",
                "post_created",
                "thread_stream"
            ]
        );
        let github_comment = described
            .iter()
            .find(|source| source.fact == "github_comment")
            .unwrap();
        assert_eq!(
            github_comment
                .fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>(),
            vec![
                "author",
                "body",
                "kind",
                "number",
                "project",
                "repository",
                "text",
                "title",
                "url",
            ]
        );
        for source in &described {
            for field in &source.fields {
                assert!(
                    registry.source_has_field(source.fact, field.name),
                    "described field {} is not accepted for {}",
                    field.name,
                    source.fact
                );
            }
        }
        let attention = described.iter().find(|s| s.fact == "attention").unwrap();
        let field = |name: &str| {
            attention
                .fields
                .iter()
                .find(|f| f.name == name)
                .unwrap_or_else(|| panic!("attention declares {name}"))
        };
        assert_eq!(field("label").kind, FieldKind::List);
        assert_eq!(field("label").vocabulary, FieldVocabulary::Labels);
        assert_eq!(field("project").vocabulary, FieldVocabulary::Projects);
        assert!(matches!(
            field("status").vocabulary,
            FieldVocabulary::Enum(values) if values.contains(&"failed")
        ));
        assert_eq!(field("detailUri").vocabulary, FieldVocabulary::Free);
    }

    #[test]
    fn file_change_status_is_its_own_vocabulary_not_issue_statuses() {
        let described = FactRegistry::default().describe();
        let status = |fact: &str| {
            described
                .iter()
                .find(|source| source.fact == fact)
                .and_then(|source| source.fields.iter().find(|field| field.name == "status"))
                .map(|field| field.vocabulary.clone())
                .unwrap()
        };
        assert_eq!(
            status("file_changed"),
            FieldVocabulary::Enum(FILE_CHANGE_STATUSES)
        );
        assert_eq!(status("attention"), FieldVocabulary::Enum(ISSUE_STATUSES));
    }

    #[test]
    fn only_content_fields_are_hydrated() {
        let registry = FactRegistry::default();
        let hydrated = |source: &str| {
            let mut names: Vec<_> = registry
                .source(source)
                .unwrap()
                .fields
                .iter()
                .filter(|(_, field)| !field.stored)
                .map(|(name, _)| *name)
                .collect();
            names.sort_unstable();
            names
        };
        assert_eq!(hydrated("thread_stream"), vec!["text"]);
        assert_eq!(hydrated("github_comment"), vec!["body"]);
        for source in [
            "attention",
            "file_changed",
            "post_created",
            "post_commented",
        ] {
            assert!(hydrated(source).is_empty(), "{source} hydrates nothing");
        }
        assert_eq!(
            registry.source("thread_stream").unwrap().audience,
            Audience::FollowersOf("threadUri")
        );
        assert_eq!(
            registry.source("attention").unwrap().dedupe,
            LogDedupe::Transition
        );
    }

    /// An addressee the registry names must be a stored field, or the index
    /// lane would expand a value no row ever carries.
    #[test]
    fn every_addressee_names_a_stored_field_and_only_posts_address_anyone() {
        let registry = FactRegistry::default();
        let mut addressed = Vec::new();
        for name in registry.source_names() {
            let spec = registry.source(name).unwrap();
            for field in spec.addressees {
                assert!(
                    spec.fields.get(field).is_some_and(|f| f.stored),
                    "{name}.{field} is an addressee but not a stored field"
                );
                addressed.push(format!("{name}.{field}"));
            }
        }
        assert_eq!(
            addressed,
            vec![
                "post_commented.postAuthor",
                "post_commented.mentions",
                "post_created.mentions"
            ]
        );
        assert_eq!(
            registry.source("thread_stream").unwrap().fields["mentions"].kind,
            FieldKind::List,
            "thread_stream mentions are matchable with the same predicate shape"
        );
    }

    /// An actor home must be a stored field, or the lane could never resolve it.
    #[test]
    fn every_actor_home_names_a_stored_field() {
        let registry = FactRegistry::default();
        for name in registry.source_names() {
            let spec = registry.source(name).unwrap();
            if let Some(field) = spec.actor_home {
                assert!(
                    spec.fields.get(field).is_some_and(|f| f.stored),
                    "{name}.{field} is an actor home but not a stored field"
                );
            }
        }
    }

    /// Every source declares what a muted recipient gets. Only the post
    /// sources ride along; the rest keep the drop that standing triggers
    /// have always had.
    #[test]
    fn every_source_declares_its_muted_delivery() {
        let registry = FactRegistry::default();
        let riding: Vec<_> = registry
            .source_names()
            .into_iter()
            .filter(|name| registry.source(name).unwrap().muted == MutedDelivery::RideAlong)
            .collect();
        assert_eq!(riding, vec!["post_commented", "post_created"]);
    }

    #[test]
    fn an_array_on_either_side_of_a_field_test_is_membership() {
        let list = serde_json::json!(["ui", "signal"]);
        // Scalar clause value against a list field: the list contains it.
        assert!(value_matches(&list, &serde_json::json!("signal")));
        assert!(!value_matches(&list, &serde_json::json!("absent")));
        // Array clause value against a list field: they intersect.
        assert!(value_matches(&list, &serde_json::json!(["absent", "ui"])));
        assert!(!value_matches(&list, &serde_json::json!(["absent"])));
        // Scalar against scalar: equality.
        assert!(value_matches(
            &serde_json::json!("active"),
            &serde_json::json!("active")
        ));
        assert!(!value_matches(
            &serde_json::json!("active"),
            &serde_json::json!("failed")
        ));
        // Array clause value against a scalar field: the array contains it.
        assert!(value_matches(
            &serde_json::json!("failed"),
            &serde_json::json!(["failed", "closed"])
        ));
        assert!(!value_matches(
            &serde_json::json!("active"),
            &serde_json::json!(["failed", "closed"])
        ));
    }

    /// The validator accepts a scalar for a list field, so the matcher must
    /// honor it: `label: signal` fires for an issue carrying `signal`.
    #[test]
    fn a_scalar_label_clause_matches_an_attention_fact_carrying_that_label() {
        let registry = FactRegistry::default();
        let clause: BTreeMap<String, Value> =
            serde_yaml::from_str("{ fact: attention, label: signal }").unwrap();
        registry.validate_clause("attention", &clause).unwrap();
        let fact = |labels: Value| {
            let fields = BTreeMap::from([("label".to_string(), labels)]);
            matches(
                std::slice::from_ref(&clause),
                &Fact {
                    source: "attention",
                    fields: &fields,
                    presence: Presence::Away,
                },
            )
        };
        assert!(fact(serde_json::json!(["noise", "signal"])));
        assert!(!fact(serde_json::json!(["noise"])));
    }

    #[test]
    fn a_failed_list_test_is_explained_as_membership() {
        let fields = BTreeMap::from([("label".to_string(), serde_json::json!(["noise"]))]);
        let fact = Fact {
            source: "attention",
            fields: &fields,
            presence: Presence::Away,
        };
        let explain =
            |yaml: &str| explain_clause(&serde_yaml::from_str(yaml).unwrap(), &fact, None);
        assert_eq!(
            explain("{ fact: attention, label: signal }"),
            vec![r#"label: list ["noise"] does not contain "signal""#]
        );
        assert_eq!(
            explain("{ fact: attention, label: [signal, ui] }"),
            vec![r#"label: list ["noise"] does not intersect ["signal","ui"]"#]
        );
        assert!(explain("{ fact: attention, label: noise }").is_empty());
    }

    fn scope<'a>(home: Option<&'a str>, row: Option<&'a str>) -> ThreadScope<'a> {
        ThreadScope {
            home_project_id: home,
            home_project_key: home.map(|id| if id == "p" { "cairn" } else { "other" }),
            row_project_id: row,
        }
    }

    #[test]
    fn a_thread_sees_its_own_project_by_default_and_only_named_projects_when_it_names_any() {
        let fields =
            |project: &str| BTreeMap::from([("project".to_string(), serde_json::json!(project))]);
        let check = |named: Option<Value>, project: &str, home: Option<&str>, row: Option<&str>| {
            let fields = fields(project);
            let fact = Fact {
                source: "file_changed",
                fields: &fields,
                presence: Presence::Away,
            };
            scope_exclusion(named.as_ref(), &fact, &scope(home, row))
        };
        use ScopeExclusion::{NamedProjects, OwnProject};
        // Default: own project and workspace rows.
        assert_eq!(check(None, "cairn", Some("p"), Some("p")), None);
        assert_eq!(check(None, "cairn", Some("p"), None), None);
        assert_eq!(check(None, "other", Some("p"), Some("q")), Some(OwnProject));
        assert_eq!(
            check(None, "cairn", None, Some("p")),
            Some(OwnProject),
            "an unresolved home project sees no scoped row"
        );
        // Named: exactly those projects, by key, scalar or list.
        let other = Some(serde_json::json!("other"));
        assert_eq!(check(other.clone(), "other", Some("p"), Some("q")), None);
        assert_eq!(
            check(other.clone(), "cairn", Some("p"), Some("p")),
            Some(NamedProjects),
            "naming a project replaces the default rather than adding to it"
        );
        let both = Some(serde_json::json!(["cairn", "other"]));
        assert_eq!(check(both.clone(), "cairn", Some("p"), Some("p")), None);
        assert_eq!(check(both, "other", Some("p"), Some("q")), None);
        assert_eq!(
            check(Some(serde_json::json!("cairn")), "cairn", Some("p"), None),
            Some(NamedProjects),
            "a project-less row is outside every named scope, whatever its field says"
        );
    }

    #[test]
    fn a_row_outside_a_threads_scope_is_explained_by_the_scope_that_excluded_it() {
        let fields = BTreeMap::from([
            ("project".to_string(), serde_json::json!("other")),
            ("path".to_string(), serde_json::json!("src/b.rs")),
        ]);
        let fact = Fact {
            source: "file_changed",
            fields: &fields,
            presence: Presence::Away,
        };
        let explain = |yaml: &str, scope: Option<&ThreadScope<'_>>| {
            explain_clause(&serde_yaml::from_str(yaml).unwrap(), &fact, scope)
        };
        let row_in_other = scope(Some("p"), Some("q"));

        assert_eq!(
            explain("{ fact: file_changed, pathPrefix: src/ }", Some(&row_in_other)),
            vec!["project: the clause names none, so the default scope applies (this thread's own project cairn plus workspace events); the row is in project other"]
        );
        assert_eq!(
            explain(
                "{ fact: file_changed, project: [cairn, third], pathPrefix: src/ }",
                Some(&row_in_other)
            ),
            vec![
                r#"project: the clause names ["cairn","third"], which replaces the own-project default; the row is in project other"#
            ],
            "the scope reason replaces the plain field failure rather than repeating it"
        );
        assert_eq!(
            explain(
                "{ fact: file_changed, project: other }",
                Some(&scope(Some("p"), None))
            ),
            vec![
                r#"project: the clause names "other", which replaces the own-project default; the row is a workspace event with no project"#
            ]
        );
        assert!(explain(
            "{ fact: file_changed, project: other }",
            Some(&row_in_other)
        )
        .is_empty());
        assert!(
            explain("{ fact: file_changed, pathPrefix: src/ }", None).is_empty(),
            "a route carries no thread scope"
        );
    }

    #[test]
    fn registry_rejects_unknown_sources_and_fields() {
        let registry = FactRegistry::default();
        let bad = BTreeMap::from([
            ("fact".into(), Value::String("thread_stream".into())),
            ("mystery".into(), Value::Bool(true)),
        ]);
        assert!(registry
            .validate_clause("thread_stream", &bad)
            .unwrap_err()
            .contains("unknown field"));
        assert!(registry
            .validate_clause("missing", &bad)
            .unwrap_err()
            .contains("unknown fact"));
    }

    #[test]
    fn the_gate_facts_live_in_this_registry_and_stay_off_the_trigger_surface() {
        let registry = FactRegistry::default();
        assert_eq!(
            registry.fact_names(FactSurface::Gate),
            vec![
                "issue_resolution",
                "pr_state",
                "artifact_confirmed",
                "check_verdict",
                "clock",
            ]
        );
        // A gate fact is level-resolved, not matched against a stream, so a
        // route may not name one as its trigger, and the fact log has no spec
        // for it.
        for fact in registry.fact_names(FactSurface::Gate) {
            assert!(registry
                .validate_clause(fact, &BTreeMap::new())
                .unwrap_err()
                .contains("unknown fact source"));
            assert!(registry.source(fact).is_none(), "{fact} has a log spec");
            assert!(!registry.source_names().contains(&fact));
        }
        assert_eq!(
            registry
                .describe()
                .iter()
                .map(|s| s.fact)
                .collect::<Vec<_>>(),
            vec![
                "attention",
                "file_changed",
                "github_comment",
                "post_commented",
                "post_created",
                "thread_stream"
            ]
        );
    }

    #[test]
    fn trigger_fact_names_follow_declaration_order() {
        let registry = FactRegistry::default();
        assert_eq!(
            registry.fact_names(FactSurface::Trigger),
            vec![
                "attention",
                "thread_stream",
                "github_comment",
                "file_changed",
                "post_created",
                "post_commented"
            ]
        );
        assert_eq!(
            registry.source_names(),
            vec![
                "attention",
                "file_changed",
                "github_comment",
                "post_commented",
                "post_created",
                "thread_stream"
            ]
        );
    }

    #[test]
    fn the_registry_describes_exactly_what_the_gate_vocabulary_declares() {
        // One declaration for a gate fact's fields: the resolver's own
        // `fields()`. If these ever disagree, the registry describes a field the
        // resolver refuses or misses one it accepts.
        let registry = FactRegistry::default();
        for source in registry.gates().sources() {
            let described = registry
                .describe_surface(FactSurface::Gate)
                .into_iter()
                .find(|shape| shape.fact == source.fact())
                .unwrap_or_else(|| panic!("{} is not in the shared registry", source.fact()));
            let mut declared: Vec<&str> = source.fields().iter().map(|(name, _)| *name).collect();
            declared.sort();
            assert_eq!(
                described
                    .fields
                    .iter()
                    .map(|field| field.name)
                    .collect::<Vec<_>>(),
                declared,
                "{} drifted",
                source.fact()
            );
        }
    }

    #[test]
    fn registering_a_resolver_extends_the_shared_registry_not_a_copy_of_it() {
        // The contract the whole `after:` extension point rests on: registering
        // ONE resolver must move the authoring surface too. A registry that
        // copied field metadata at construction would pass every test above and
        // still fail this one, because the copy is taken before the resolver
        // exists.
        use crate::execution::gates::vocabulary::test_support::DecisionFact;

        let mut registry = FactRegistry::default();
        assert!(!registry.fact_names(FactSurface::Gate).contains(&"decision"));

        registry.register_gate(std::sync::Arc::new(DecisionFact));

        assert!(registry.fact_names(FactSurface::Gate).contains(&"decision"));
        assert!(registry.source_has_field("decision", "state"));
        let described = registry
            .describe_surface(FactSurface::Gate)
            .into_iter()
            .find(|shape| shape.fact == "decision")
            .expect("a registered resolver is described by the shared registry");
        assert_eq!(
            described
                .fields
                .iter()
                .map(|field| field.name)
                .collect::<Vec<_>>(),
            vec!["decision", "state"]
        );
        // And it is still refused on the trigger surface, which registering a
        // gate resolver must not widen.
        assert!(registry
            .validate_clause("decision", &BTreeMap::new())
            .unwrap_err()
            .contains("unknown fact source"));
    }
}
