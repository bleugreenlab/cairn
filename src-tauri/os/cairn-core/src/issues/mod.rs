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
    use std::path::{Path, PathBuf};

    /// Every surface that TEACHES a caller what to write: the resource
    /// contracts that render every affordance block, the docs, and the agent
    /// prompts. The retired key may be named only by the refusal that retires
    /// it and by migration history.
    fn teaching_surfaces() -> Vec<PathBuf> {
        let src_tauri = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        vec![
            src_tauri.join("os/cairn-common/src/contract"),
            src_tauri.join("packs"),
            src_tauri.join("../docs"),
        ]
    }

    fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let path = entry.unwrap().path();
            // Dated design records describe the system as it was when written.
            if path.ends_with("docs/superpowers") {
                continue;
            }
            if path.is_dir() {
                files_under(&path, out);
            } else {
                out.push(path);
            }
        }
    }

    #[test]
    fn the_retired_dependency_key_is_taught_nowhere() {
        let mut files = Vec::new();
        for surface in teaching_surfaces() {
            files_under(&surface, &mut files);
        }
        assert!(files.len() > 100, "walked only {} files", files.len());
        let offenders: Vec<String> = files
            .iter()
            .filter_map(|path| {
                let text = std::fs::read_to_string(path).ok()?;
                (text.contains("depends_on") || text.contains("dependsOn"))
                    .then(|| path.display().to_string())
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "these still teach the retired key; teach `after:` instead: {offenders:#?}"
        );
    }

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
