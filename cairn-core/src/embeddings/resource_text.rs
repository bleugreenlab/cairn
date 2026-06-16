//! Prose extraction for corpus resources.
//!
//! Resources are embedded as-is (no fenced-code stripping). For artifacts, the
//! embeddable text is the concatenation of the prose fields that carry meaning
//! for recall, selected per artifact type. Unknown/custom types fall back to
//! joining all top-level string values.

use serde_json::Value;

/// Build the embeddable text for an artifact of `artifact_type` from its `data`
/// JSON. Returns `None` when no prose is present (an empty result should enqueue
/// a delete rather than an upsert).
pub fn artifact_embed_text(artifact_type: &str, data: &Value) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    match artifact_type {
        "document" | "plan" => {
            push_str(&mut parts, data, "title");
            push_str(&mut parts, data, "summary");
            push_str(&mut parts, data, "content");
        }
        "implementation" => {
            push_str(&mut parts, data, "title");
            push_str(&mut parts, data, "summary");
            push_str(&mut parts, data, "body");
        }
        "review" => {
            push_str(&mut parts, data, "summary");
            if let Some(comments) = data.get("comments").and_then(Value::as_array) {
                for comment in comments {
                    push_str(&mut parts, comment, "message");
                }
            }
        }
        "tasklist" => {
            push_str(&mut parts, data, "objective");
            push_str_array(&mut parts, data, "requirements");
            if let Some(tasks) = data.get("tasks").and_then(Value::as_array) {
                for task in tasks {
                    push_str(&mut parts, task, "title");
                }
            }
        }
        "checklist" => {
            push_str(&mut parts, data, "title");
            if let Some(items) = data.get("items").and_then(Value::as_array) {
                for item in items {
                    push_str(&mut parts, item, "task");
                }
            }
        }
        "return" => {
            push_str(&mut parts, data, "content");
        }
        _ => {
            // Unknown/custom type: join all top-level string values.
            if let Some(obj) = data.as_object() {
                for value in obj.values() {
                    if let Some(text) = non_empty_str(value) {
                        parts.push(text.to_string());
                    }
                }
            }
        }
    }

    let joined = parts.join("\n");
    if joined.trim().is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn non_empty_str(value: &Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

fn push_str(parts: &mut Vec<String>, obj: &Value, key: &str) {
    if let Some(text) = obj.get(key).and_then(non_empty_str) {
        parts.push(text.to_string());
    }
}

fn push_str_array(parts: &mut Vec<String>, obj: &Value, key: &str) {
    if let Some(items) = obj.get(key).and_then(Value::as_array) {
        for item in items {
            if let Some(text) = non_empty_str(item) {
                parts.push(text.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn document_concatenates_title_summary_content() {
        let data = json!({
            "title": "My Doc",
            "summary": "A short summary",
            "content": "The body text"
        });
        assert_eq!(
            artifact_embed_text("document", &data),
            Some("My Doc\nA short summary\nThe body text".to_string())
        );
    }

    #[test]
    fn plan_uses_same_fields_as_document() {
        let data = json!({ "title": "Plan", "content": "steps" });
        assert_eq!(
            artifact_embed_text("plan", &data),
            Some("Plan\nsteps".to_string())
        );
    }

    #[test]
    fn implementation_uses_body() {
        let data = json!({
            "title": "PR title",
            "summary": "impl summary",
            "body": "PR body markdown"
        });
        assert_eq!(
            artifact_embed_text("implementation", &data),
            Some("PR title\nimpl summary\nPR body markdown".to_string())
        );
    }

    #[test]
    fn review_concatenates_summary_and_comment_messages() {
        let data = json!({
            "summary": "Looks good",
            "approval": "approved",
            "comments": [
                { "message": "nit: rename this", "file": "a.rs" },
                { "message": "add a test", "line": 10 }
            ]
        });
        assert_eq!(
            artifact_embed_text("review", &data),
            Some("Looks good\nnit: rename this\nadd a test".to_string())
        );
    }

    #[test]
    fn tasklist_concatenates_objective_requirements_task_titles() {
        let data = json!({
            "objective": "Ship feature",
            "requirements": ["req one", "req two"],
            "tasks": [
                { "id": "a", "title": "Task A", "agent": "build", "prompt": "do a" },
                { "id": "b", "title": "Task B", "agent": "build", "prompt": "do b" }
            ]
        });
        assert_eq!(
            artifact_embed_text("tasklist", &data),
            Some("Ship feature\nreq one\nreq two\nTask A\nTask B".to_string())
        );
    }

    #[test]
    fn checklist_concatenates_title_and_item_tasks() {
        let data = json!({
            "title": "Pre-flight",
            "items": [
                { "task": "check oil", "completed": false },
                { "task": "check fuel", "completed": true }
            ]
        });
        assert_eq!(
            artifact_embed_text("checklist", &data),
            Some("Pre-flight\ncheck oil\ncheck fuel".to_string())
        );
    }

    #[test]
    fn return_uses_content() {
        let data = json!({ "content": "the result" });
        assert_eq!(
            artifact_embed_text("return", &data),
            Some("the result".to_string())
        );
    }

    #[test]
    fn unknown_type_joins_top_level_strings() {
        let data = json!({
            "foo": "alpha",
            "bar": "beta",
            "count": 3,
            "flag": true
        });
        let text = artifact_embed_text("custom", &data).unwrap();
        // Order of object keys is preserved by serde_json; both strings present.
        assert!(text.contains("alpha"));
        assert!(text.contains("beta"));
        assert!(!text.contains('3'));
    }

    #[test]
    fn returns_none_when_empty() {
        assert_eq!(artifact_embed_text("document", &json!({})), None);
        assert_eq!(
            artifact_embed_text("document", &json!({ "title": "   ", "content": "" })),
            None
        );
        assert_eq!(
            artifact_embed_text("return", &json!({ "content": "" })),
            None
        );
        assert_eq!(artifact_embed_text("custom", &json!({ "n": 1 })), None);
    }
}
