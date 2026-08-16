use crate::routes::TriggerClause;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The thread's living document, addressed as `cairn:~/arc`. The name is the
/// artifact's identity, the key of its version chain, and the preset schema it
/// validates against — one string, so the three cannot drift apart.
pub const ARC_ARTIFACT_NAME: &str = "arc";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ThreadDefinition {
    pub agent: String,
    pub artifacts: Vec<String>,
    pub triggers: Vec<TriggerClause>,
}

impl ThreadDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.agent.trim().is_empty() {
            return Err("thread definition must contain exactly one agent".into());
        }
        if self.artifacts != [ARC_ARTIFACT_NAME] {
            return Err("thread definition must declare exactly the arc artifact".into());
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
        artifacts: vec![ARC_ARTIFACT_NAME.into()],
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
        assert!(
            resolve_thread_definition(Some(r#"{"agent":"","artifacts":[],"triggers":[]}"#))
                .is_err()
        );
        assert!(resolve_thread_definition(Some(
            r#"{"agent":["one","two"],"artifacts":[],"triggers":[]}"#
        ))
        .is_err());
        assert!(resolve_thread_definition(Some(
            r#"{"agent":"thread","artifacts":[],"triggers":[]}"#
        ))
        .is_err());
    }

    #[test]
    fn rejects_terminal_status_subsets_that_wake_rows_cannot_preserve() {
        let error = resolve_thread_definition(Some(
            r#"{"agent":"thread","artifacts":["arc"],"triggers":[{"fact":"attention","detailUri":"cairn://p/cairn/1","status":"merged"}]}"#,
        ))
        .unwrap_err();
        assert!(error.contains("all terminal statuses"), "{error}");
    }
}
