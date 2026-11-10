//! Issue and comment business logic.

pub mod comments;
pub mod crud;
pub mod delete;
pub mod relations;
pub mod status;

/// The refusal for a payload that still names `depends_on`.
///
/// The issue-level dependency edge is gone: nothing stores it and nothing
/// enforces it. An execution now waits through its own `after:` gates. A caller
/// still sending the old key believes it is setting a hold, so the key is
/// refused with the shape that replaced it rather than dropped, which would
/// report success for a hold that does not exist.
pub const DEPENDS_ON_RETIRED: &str = "depends_on retired into after: an issue no longer carries dependency edges. File a HELD execution that starts by itself once its gates are satisfied, e.g. write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions\",mode:\"append\",payload:{recipe:\"planbuild\",after:[{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/OTHER\",resolution:\"merged\"}]}}]}). Creating the issue, pass the same `after` inside its `execution` object.";

/// [`DEPENDS_ON_RETIRED`] when `payload` carries the retired key at its top
/// level, in either spelling it was ever accepted under.
pub fn retired_depends_on(payload: Option<&serde_json::Value>) -> Option<&'static str> {
    let map = payload?.as_object()?;
    ["depends_on", "dependsOn"]
        .iter()
        .any(|key| map.contains_key(*key))
        .then_some(DEPENDS_ON_RETIRED)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_payload_naming_the_retired_key_is_refused_in_either_spelling() {
        for payload in [
            serde_json::json!({"depends_on": []}),
            serde_json::json!({"dependsOn": ["cairn://p/cairn/1"]}),
        ] {
            assert_eq!(
                super::retired_depends_on(Some(&payload)),
                Some(super::DEPENDS_ON_RETIRED)
            );
        }
        assert_eq!(
            super::retired_depends_on(Some(&serde_json::json!({"title": "t"}))),
            None
        );
        assert_eq!(super::retired_depends_on(None), None);
    }
}
