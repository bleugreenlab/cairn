use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

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
}

/// GitHub's own limit on an issue or pull request comment body.
const GITHUB_COMMENT_MAX_CHARS: usize = 65_536;

impl SourceSpec {
    pub fn is_hydrated(&self, field: &str) -> bool {
        self.fields.get(field).is_some_and(|spec| !spec.stored)
    }
}

#[derive(Debug, Clone)]
pub struct FactRegistry {
    sources: HashMap<&'static str, SourceSpec>,
}

const FILE_CHANGE_STATUSES: &[&str] = &["added", "modified", "deleted", "renamed"];

/// The value vocabulary for one fact field. Fields not named here are free text.
fn vocabulary_for(source: &str, field: &str) -> FieldVocabulary {
    match (source, field) {
        ("file_changed", "status") => FieldVocabulary::Enum(FILE_CHANGE_STATUSES),
        (_, "status") => FieldVocabulary::Enum(ISSUE_STATUSES),
        (_, "attention") => FieldVocabulary::Enum(ISSUE_ATTENTIONS),
        (_, "label") => FieldVocabulary::Labels,
        (_, "project") => FieldVocabulary::Projects,
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
            fields: fields(&[
                ("project", Scalar, STORED),
                ("threadUri", Scalar, STORED),
                ("detailUri", Scalar, STORED),
                ("text", Scalar, HYDRATED),
                ("context", Scalar, STORED),
                ("jobId", Scalar, STORED),
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
        Self {
            sources: HashMap::from([
                ("attention", attention),
                ("thread_stream", thread),
                ("github_comment", github_comment),
                ("file_changed", file_changed),
            ]),
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
    pub fn describe(&self) -> Vec<FactSourceShape> {
        let mut sources: Vec<FactSourceShape> = self
            .sources
            .iter()
            .map(|(fact, spec)| {
                let mut fields: Vec<_> = spec
                    .fields
                    .iter()
                    .map(|(name, field)| FactFieldShape {
                        name,
                        kind: field.kind,
                        vocabulary: vocabulary_for(fact, name),
                    })
                    .collect();
                fields.sort_by_key(|field| field.name);
                FactSourceShape { fact, fields }
            })
            .collect();
        sources.sort_by_key(|source| source.fact);
        sources
    }

    pub fn source_has_field(&self, source: &str, field: &str) -> bool {
        self.sources
            .get(source)
            .is_some_and(|spec| spec.fields.contains_key(field))
    }

    pub fn source(&self, source: &str) -> Option<&SourceSpec> {
        self.sources.get(source)
    }

    /// Every registered source name, sorted.
    pub fn source_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.sources.keys().copied().collect();
        names.sort_unstable();
        names
    }

    pub fn validate_clause(
        &self,
        source: &str,
        clause: &BTreeMap<String, Value>,
    ) -> Result<(), String> {
        let fields = &self
            .sources
            .get(source)
            .ok_or_else(|| format!("unknown fact source '{source}'"))?
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
fn value_matches(actual: &Value, expected: &Value) -> bool {
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
        for source in ["attention", "file_changed"] {
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
}
