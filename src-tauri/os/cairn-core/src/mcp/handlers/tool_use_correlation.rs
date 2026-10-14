use crate::storage::LocalDb;
use std::collections::HashSet;

/// What correlating a callback with its own provider tool invocation found. The
/// three cases are distinct outcomes, not degrees of success: only [`Claim::One`]
/// licenses answering a call.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    /// Exactly one candidate invocation matched; this is its provider id.
    One(String),
    /// No candidate matched.
    None,
    /// Several indistinguishable candidates matched, so no id can be claimed
    /// without risking another call's answer. Carries the count, because a
    /// refusal nobody asked for has to explain itself in the log.
    Ambiguous(usize),
}

/// Claim the single unanswered invocation accepted by `matches`, ignoring any id
/// in `spoken_for`. Assistant event blobs must be supplied newest first.
///
/// Content cannot be identity on its own. A provider may emit several `run` tool
/// uses in one assistant event, and two of them may carry byte-identical input;
/// picking the newest match would then answer one call with another's result and
/// leave a call double-answered. So the candidate set is narrowed to invocations
/// that are still unanswered and unclaimed, and a tie among those is refused
/// rather than guessed — what makes a claim safe is exclusivity, not recency.
pub(crate) fn claim_from_events<P>(
    event_data: &[String],
    spoken_for: &HashSet<String>,
    matches: P,
) -> Claim
where
    P: Fn(&str, &serde_json::Value) -> bool,
{
    let mut candidates: Vec<String> = Vec::new();
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
            let Some(id) = tool
                .get("id")
                .or_else(|| tool.get("toolUseId"))
                .and_then(|value| value.as_str())
            else {
                continue;
            };
            if spoken_for.contains(id) || candidates.iter().any(|seen| seen == id) {
                continue;
            }
            candidates.push(id.to_string());
        }
    }
    match candidates.len() {
        0 => Claim::None,
        1 => Claim::One(candidates.remove(0)),
        n => Claim::Ambiguous(n),
    }
}

/// Claim the provider invocation a callback came from.
///
/// This is the one way a callback names its own call, and it is exclusive by
/// construction. An earlier sibling took the newest content match instead, which
/// is safe only for a caller that does nothing with the id — and every caller
/// here does something: it writes a synthetic tool result to that id, or links a
/// spawned child job by it. So the candidate set narrows to unanswered, unclaimed
/// invocations and a tie is refused rather than guessed.
///
/// An already-answered invocation is excluded because it cannot be the live call:
/// that exclusion is also what keeps the ordinary case working, where a turn runs
/// the same command twice and only the second one is still open.
///
/// The brief retry covers the race between callback delivery and assistant-event
/// persistence.
pub(crate) async fn claim_tool_use_id<P>(
    db: &LocalDb,
    run_id: &str,
    turn_id: &str,
    matches: P,
) -> Claim
where
    P: Fn(&str, &serde_json::Value) -> bool,
{
    for _ in 0..20 {
        let spoken_for = spoken_for_ids(db, run_id).await;
        let rows = assistant_events(db, run_id, Some(turn_id)).await;
        match claim_from_events(&rows, &spoken_for, &matches) {
            // Only absence is a race worth retrying: the assistant event carrying
            // the call may not be persisted at the instant the callback fires.
            Claim::None => {}
            settled => return settled,
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Claim::None
}

/// Provider ids that can no longer be claimed: invocations of this run already
/// answered, and invocations some suspension of it already bound.
///
/// Answered ids are gathered per RUN rather than per turn on purpose. A tool
/// result is not guaranteed to carry a turn (2.6% of recorded ones do not), and
/// missing an answered id here would cost a legitimate claim by making its
/// already-finished twin look like a live rival. Widening cannot over-exclude:
/// candidates come from this turn's assistant events, and an answered invocation
/// is never the live call regardless of which turn recorded it.
async fn spoken_for_ids(db: &LocalDb, run_id: &str) -> HashSet<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut out = HashSet::new();
            let mut answered = conn
                .query(
                    "SELECT json_extract(data, '$.toolUseId') FROM events
                     WHERE run_id = ?1 AND event_type = 'tool_result'",
                    cairn_db::turso::params![run_id.clone()],
                )
                .await?;
            while let Some(row) = answered.next().await? {
                if let Some(id) = row.opt_text(0)? {
                    out.insert(id);
                }
            }
            let mut claimed = conn
                .query(
                    "SELECT tool_use_id FROM agent_waits WHERE run_id = ?1",
                    cairn_db::turso::params![run_id],
                )
                .await?;
            while let Some(row) = claimed.next().await? {
                out.insert(row.text(0)?);
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_default()
}

/// The newest assistant events of a run, scoped to one turn when given one.
async fn assistant_events(db: &LocalDb, run_id: &str, turn_id: Option<&str>) -> Vec<String> {
    let (run_id, turn_id) = (run_id.to_string(), turn_id.map(str::to_string));
    db.read(|conn| {
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
    .unwrap_or_default()
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
                claim_from_events(&[event], &HashSet::new(), wait_match),
                Claim::One("provider-id".into())
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
        assert_eq!(
            claim_from_events(&rows, &HashSet::new(), wait_match),
            Claim::None
        );
    }

    fn run_batch(id: &str, command: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "name": "mcp__cairn__run",
            "input": {"commands": [{"command": command}]}
        })
    }

    fn batch_match(command: &'static str) -> impl Fn(&str, &serde_json::Value) -> bool {
        move |name, input| {
            (name == "run" || name.ends_with("__run"))
                && input == &serde_json::json!({"commands": [{"command": command}]})
        }
    }

    fn event(tools: Vec<serde_json::Value>) -> String {
        serde_json::json!({"toolUses": tools}).to_string()
    }

    /// A claim identifies a call, so it must pick the invocation whose contents
    /// match rather than the newest one in the event. Answering by recency would
    /// hand this batch's result to whichever sibling the model happened to emit
    /// last.
    #[test]
    fn a_claim_picks_the_matching_invocation_not_the_newest() {
        let rows = vec![event(vec![
            run_batch("toolu-mine", "sleep 300"),
            run_batch("toolu-sibling", "bun run check:rust"),
        ])];
        assert_eq!(
            claim_from_events(&rows, &HashSet::new(), batch_match("sleep 300")),
            Claim::One("toolu-mine".into())
        );
    }

    /// The state that makes contents-as-identity unsafe: one assistant event
    /// carrying two byte-identical `run` calls. Neither can be claimed, because
    /// nothing at this boundary distinguishes them and a guess would answer the
    /// wrong call.
    #[test]
    fn indistinguishable_concurrent_invocations_are_refused_not_guessed() {
        let rows = vec![event(vec![
            run_batch("toolu-first", "sleep 300"),
            run_batch("toolu-second", "sleep 300"),
        ])];
        assert_eq!(
            claim_from_events(&rows, &HashSet::new(), batch_match("sleep 300")),
            Claim::Ambiguous(2)
        );
    }

    /// The ordinary case that exclusion must not break: a turn runs the same
    /// command twice in sequence, the first is already answered, and only the
    /// second is still open. Refusing here would make repeating a command in one
    /// turn forfeit its suspension.
    #[test]
    fn an_answered_twin_leaves_the_open_invocation_claimable() {
        let rows = vec![
            event(vec![run_batch("toolu-second", "bun run test:rust")]),
            event(vec![run_batch("toolu-first", "bun run test:rust")]),
        ];
        let answered = HashSet::from(["toolu-first".to_string()]);
        assert_eq!(
            claim_from_events(&rows, &answered, batch_match("bun run test:rust")),
            Claim::One("toolu-second".into())
        );
    }

    /// A claim is exclusive: an id some other suspension already holds is not a
    /// candidate, so two claims can never name one call.
    #[test]
    fn an_already_claimed_invocation_is_not_a_candidate() {
        let rows = vec![event(vec![run_batch("toolu-only", "sleep 300")])];
        let claimed = HashSet::from(["toolu-only".to_string()]);
        assert_eq!(
            claim_from_events(&rows, &claimed, batch_match("sleep 300")),
            Claim::None
        );
    }

    #[test]
    fn a_claim_ignores_unrelated_and_malformed_events() {
        let rows = vec![
            "not json".to_string(),
            event(vec![
                serde_json::json!({"id": "read-id", "name": "read", "input": {}}),
            ]),
            event(vec![run_batch("toolu-other", "echo other")]),
        ];
        assert_eq!(
            claim_from_events(&rows, &HashSet::new(), batch_match("sleep 300")),
            Claim::None
        );
    }
}
