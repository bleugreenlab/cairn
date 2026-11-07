use crate::routes::TriggerClause;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A thread keeps no artifacts: its authored record is its database's
/// `intent` and `journal` (`cairn://skills/thread-db`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ThreadDefinition {
    pub agent: String,
    pub triggers: Vec<TriggerClause>,
}

impl ThreadDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.agent.trim().is_empty() {
            return Err("thread definition must contain exactly one agent".into());
        }
        let registry = crate::routes::FactRegistry::default();
        for clause in &self.triggers {
            let source = clause
                .get("fact")
                .and_then(Value::as_str)
                .ok_or_else(|| "each thread trigger requires a scalar 'fact'".to_string())?;
            registry.validate_clause(source, clause)?;
        }
        crate::orchestrator::wakes::validate_derived_thread_triggers(&self.triggers)?;
        Ok(())
    }
}

pub fn default_thread_definition() -> ThreadDefinition {
    ThreadDefinition {
        agent: "thread".into(),
        triggers: Vec::new(),
    }
}

pub fn resolve_thread_definition(stored: Option<&str>) -> Result<ThreadDefinition, String> {
    let definition = match stored {
        Some(json) => serde_json::from_str(json).map_err(|error| error.to_string())?,
        None => default_thread_definition(),
    };
    definition.validate()?;
    Ok(definition)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_and_resolves_default() {
        let definition = default_thread_definition();
        let json = serde_json::to_string(&definition).unwrap();
        assert_eq!(
            serde_json::from_str::<ThreadDefinition>(&json).unwrap(),
            definition
        );
        assert_eq!(resolve_thread_definition(None).unwrap(), definition);
    }

    #[test]
    fn rejects_zero_or_multiple_agents() {
        assert!(resolve_thread_definition(Some(r#"{"agent":"","triggers":[]}"#)).is_err());
        assert!(
            resolve_thread_definition(Some(r#"{"agent":["one","two"],"triggers":[]}"#)).is_err()
        );
    }

    /// The arc retired with the `artifacts` key; a definition naming it is a
    /// definition from before, and says so rather than being half-read.
    #[test]
    fn a_definition_declaring_artifacts_is_refused() {
        let error = resolve_thread_definition(Some(
            r#"{"agent":"thread","artifacts":["arc"],"triggers":[]}"#,
        ))
        .unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    /// Any clause the fact registry accepts is a standing trigger; the registry
    /// is what refuses one.
    #[test]
    fn accepts_any_registry_clause_and_rejects_unknown_fields() {
        for triggers in [
            r#"[{"fact":"attention","detailUri":"cairn://p/cairn/1","status":"merged"}]"#,
            r#"[{"fact":"file_changed","pathPrefix":"src/ui/"}]"#,
        ] {
            resolve_thread_definition(Some(&format!(
                r#"{{"agent":"thread","triggers":{triggers}}}"#
            )))
            .unwrap_or_else(|error| panic!("{triggers}: {error}"));
        }
        let error = resolve_thread_definition(Some(
            r#"{"agent":"thread","triggers":[{"fact":"file_changed","folderPrefix":"src/"}]}"#,
        ))
        .unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }
}
