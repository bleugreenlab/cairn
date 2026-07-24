use crate::storage::LocalDb;

/// Find the provider id of the newest tool invocation accepted by `matches`.
/// Assistant event blobs must be supplied newest first.
pub(crate) fn find_tool_use_id<P>(event_data: &[String], matches: P) -> Option<String>
where
    P: Fn(&str, &serde_json::Value) -> bool,
{
    for data in event_data {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        let Some(tools) = value.get("toolUses").and_then(|value| value.as_array()) else {
            continue;
        };
        for tool in tools.iter().rev() {
            let Some(name) = tool.get("name").and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(input) = tool.get("input") else {
                continue;
            };
            if !matches(name, input) {
                continue;
            }
            if let Some(id) = tool
                .get("id")
                .or_else(|| tool.get("toolUseId"))
                .and_then(|value| value.as_str())
            {
                return Some(id.to_string());
            }
        }
    }
    None
}

/// Correlate a callback with its provider tool invocation in the run transcript.
/// The brief retry covers the race between callback delivery and assistant-event
/// persistence.
pub(crate) async fn resolve_tool_use_id<P>(
    db: &LocalDb,
    run_id: &str,
    turn_id: Option<&str>,
    matches: P,
) -> Option<String>
where
    P: Fn(&str, &serde_json::Value) -> bool,
{
    for _ in 0..20 {
        let run_id = run_id.to_string();
        let turn_id = turn_id.map(str::to_string);
        let rows: Vec<String> = db
            .read(|conn| {
                let (run_id, turn_id) = (run_id.clone(), turn_id.clone());
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT data FROM events
                             WHERE run_id = ?1 AND event_type = 'assistant'
                               AND (?2 IS NULL OR turn_id = ?2)
                             ORDER BY sequence DESC LIMIT 8",
                            cairn_db::turso::params![run_id, turn_id],
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(row.text(0)?);
                    }
                    Ok(out)
                })
            })
            .await
            .unwrap_or_default();
        if let Some(id) = find_tool_use_id(&rows, &matches) {
            return Some(id);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    None
}

use crate::storage::RowExt;

#[cfg(test)]
mod tests {
    use super::*;

    fn run_wait(name: &str, id_key: &str, id: &str, duration: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            id_key: id,
            "input": {"commands": [{"waitFor": {"duration": duration}}]}
        })
    }

    fn wait_match(name: &str, input: &serde_json::Value) -> bool {
        (name == "run" || name.ends_with("__run"))
            && input == &serde_json::json!({"commands": [{"waitFor": {"duration": "3m"}}]})
    }

    #[test]
    fn accepts_both_provider_id_spellings_and_namespaced_run() {
        for (key, name) in [("id", "run"), ("toolUseId", "mcp__cairn__run")] {
            let event = serde_json::json!({"toolUses": [run_wait(name, key, "provider-id", "3m")]})
                .to_string();
            assert_eq!(
                find_tool_use_id(&[event], wait_match),
                Some("provider-id".into())
            );
        }
    }

    #[test]
    fn requires_exact_wait_input_and_ignores_malformed_or_unrelated_events() {
        let rows = vec![
            "not json".into(),
            serde_json::json!({"toolUses": [run_wait("run", "id", "wrong", "4m")]}).to_string(),
            serde_json::json!({"toolUses": [{"id":"read-id","name":"read","input":{}}]})
                .to_string(),
        ];
        assert_eq!(find_tool_use_id(&rows, wait_match), None);
    }

    #[test]
    fn selects_newest_matching_invocation() {
        let newest = serde_json::json!({"toolUses": [
            run_wait("run", "id", "earlier-in-event", "3m"),
            run_wait("run", "id", "newest", "3m")
        ]})
        .to_string();
        let older =
            serde_json::json!({"toolUses": [run_wait("run", "id", "older", "3m")]}).to_string();
        assert_eq!(
            find_tool_use_id(&[newest, older], wait_match),
            Some("newest".into())
        );
    }
}
