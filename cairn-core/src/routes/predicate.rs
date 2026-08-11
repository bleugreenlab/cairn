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

/// Explain every test in one trigger clause. This shares the exact matcher for
/// its verdict so editor diagnostics cannot drift from dispatch behavior.
pub fn explain_clause(clause: &BTreeMap<String, Value>, fact: &Fact<'_>) -> Vec<String> {
    clause
        .iter()
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
                    Some(format!("{key} expected {expected}, observed {actual}"))
                })
        })
        .collect()
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

#[derive(Debug, Clone)]
pub struct FactRegistry {
    sources: HashMap<&'static str, HashMap<&'static str, FieldKind>>,
}

/// The value vocabulary for one fact field. Fields not named here are free text.
fn vocabulary_for(field: &str) -> FieldVocabulary {
    match field {
        "status" => FieldVocabulary::Enum(ISSUE_STATUSES),
        "attention" => FieldVocabulary::Enum(ISSUE_ATTENTIONS),
        "label" => FieldVocabulary::Labels,
        "project" => FieldVocabulary::Projects,
        _ => FieldVocabulary::Free,
    }
}

impl Default for FactRegistry {
    fn default() -> Self {
        let attention = HashMap::from([
            ("project", FieldKind::Scalar),
            ("attention", FieldKind::Scalar),
            ("status", FieldKind::Scalar),
            ("label", FieldKind::List),
            ("detailUri", FieldKind::Scalar),
            ("text", FieldKind::Scalar),
        ]);
        let thread = HashMap::from([
            ("project", FieldKind::Scalar),
            ("threadUri", FieldKind::Scalar),
            ("detailUri", FieldKind::Scalar),
            ("text", FieldKind::Scalar),
            ("context", FieldKind::Scalar),
            ("jobId", FieldKind::Scalar),
        ]);
        let github_comment = HashMap::from([
            ("project", FieldKind::Scalar),
            ("repository", FieldKind::Scalar),
            ("number", FieldKind::Scalar),
            ("kind", FieldKind::Scalar),
            ("author", FieldKind::Scalar),
            ("url", FieldKind::Scalar),
            ("title", FieldKind::Scalar),
            ("body", FieldKind::Scalar),
            ("text", FieldKind::Scalar),
        ]);
        Self {
            sources: HashMap::from([
                ("attention", attention),
                ("thread_stream", thread),
                ("github_comment", github_comment),
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
            .map(|(fact, fields)| {
                let mut fields: Vec<_> = fields
                    .iter()
                    .map(|(name, kind)| FactFieldShape {
                        name,
                        kind: *kind,
                        vocabulary: vocabulary_for(name),
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
            .is_some_and(|fields| fields.contains_key(field))
    }

    pub fn validate_clause(
        &self,
        source: &str,
        clause: &BTreeMap<String, Value>,
    ) -> Result<(), String> {
        let fields = self
            .sources
            .get(source)
            .ok_or_else(|| format!("unknown fact source '{source}'"))?;
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

fn value_matches(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Array(values) => match actual {
            Value::Array(actual) => {
                let set: HashSet<_> = actual.iter().collect();
                values.iter().any(|value| set.contains(value))
            }
            _ => values.contains(actual),
        },
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicates_are_or_of_ands_and_support_prefix_and_presence() {
        let clauses: Vec<BTreeMap<String, Value>> = serde_yaml::from_str(
            "- { fact: attention, status: [failed], presence: away }\n- { fact: thread_stream, detailUriPrefix: 'cairn://p/CAIRN/', presence: away }"
        ).unwrap();
        let fields = BTreeMap::from([
            (
                "detailUri".into(),
                Value::String("cairn://p/CAIRN/1".into()),
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
            vec!["attention", "github_comment", "thread_stream"]
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
