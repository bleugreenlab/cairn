use super::CODEX_BACKEND_NAME;
use crate::backends::run_state::run_backend_db;
use crate::backends::SessionStart;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::sync::Arc;

const RESUME_FALLBACK_TRANSCRIPT_CHARS: usize = 24_000;

// Codex's `baseInstructions` and `developerInstructions` payloads are derived by
// slicing the assembled prompt segments in `backend_impl` (via
// `base_instructions_from_segments` / `developer_instructions_from_segments`),
// so sent bytes equal the persisted `system:prompt` segments by construction.

/// The opening text for a thread rebuilt because Codex no longer has the one
/// the session starts from: the transcript of the Cairn session that thread
/// belonged to, then the latest message. A resume's thread is its own
/// session's; a fork's is its parent session's, whose thread it forks.
pub(super) fn build_resume_fallback_prompt(
    run_db: &Arc<LocalDb>,
    start: &SessionStart,
    latest_user_message: &str,
) -> Option<String> {
    let stale_session_id = match start {
        SessionStart::Fork { session_id, .. } => parent_session(run_db, session_id)?,
        _ => start.session_id().to_string(),
    };
    let db = run_db.clone();
    let event_rows: Vec<(String, i32, String, String)> =
        run_backend_db(CODEX_BACKEND_NAME, async move {
            db.read(|conn| {
                let stale_session_id = stale_session_id.clone();
                Box::pin(async move {
                    // Live-only reader (no archival reconstruction): this runs
                    // mid-session to rebuild Codex thread params from the active
                    // session's events, before teardown makes anything
                    // archivable. It never sees a gitcoord/zstd stub.
                    let mut rows = conn
                        .query(
                            "SELECT run_id, sequence, event_type, data
                         FROM events
                         WHERE session_id = ?1
                         ORDER BY created_at ASC, sequence ASC",
                            params![stale_session_id.as_str()],
                        )
                        .await?;
                    let mut event_rows = Vec::new();
                    while let Some(row) = rows.next().await? {
                        event_rows.push((
                            row.text(0)?,
                            row.i64(1)? as i32,
                            row.text(2)?,
                            row.text(3)?,
                        ));
                    }
                    Ok(event_rows)
                })
            })
            .await
            .map_err(|e| e.to_string())
        })
        .ok()?;

    if event_rows.is_empty() {
        return None;
    }

    let transcript = crate::transcripts::format_transcript_full(&event_rows);
    let transcript = trim_resume_transcript(&transcript, RESUME_FALLBACK_TRANSCRIPT_CHARS);

    Some(format_resume_fallback_prompt(
        &transcript,
        latest_user_message,
    ))
}

/// The session `session_id` continues from.
fn parent_session(run_db: &Arc<LocalDb>, session_id: &str) -> Option<String> {
    let db = run_db.clone();
    let session_id = session_id.to_string();
    run_backend_db(CODEX_BACKEND_NAME, async move {
        db.query_opt(
            "SELECT parent_session_id FROM sessions WHERE id = ?1",
            params![session_id.as_str()],
            |row| row.opt_text(0),
        )
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
    .flatten()
}

/// Wrap a prior-session transcript and the latest reply into the fresh-thread
/// fallback prompt. Pure so the ordering invariant (prior context precedes the
/// latest message) is unit-testable without a database.
fn format_resume_fallback_prompt(transcript: &str, latest_user_message: &str) -> String {
    format!(
        "The previous Codex thread could not be resumed, so you are continuing from a transcript snapshot instead.\n\n\
## Prior Transcript\n\n{}\n\n\
## Latest User Message\n\n{}",
        transcript, latest_user_message
    )
}

fn trim_resume_transcript(transcript: &str, max_chars: usize) -> String {
    if transcript.chars().count() <= max_chars {
        return transcript.to_string();
    }

    let tail: String = transcript
        .chars()
        .rev()
        .take(max_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("[Earlier transcript omitted]\n\n{}", tail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_fallback_prompt_includes_prior_context_and_latest_reply() {
        // The fresh-thread fallback must carry prior user and assistant context
        // plus the latest reply, in that order — not just system instructions
        // (CAIRN-2598).
        let transcript = "User: earlier question\nAssistant: earlier answer";
        let prompt = format_resume_fallback_prompt(transcript, "the new reply");
        assert!(prompt.contains("earlier question"));
        assert!(prompt.contains("earlier answer"));
        assert!(prompt.contains("the new reply"));
        assert!(prompt.contains("## Prior Transcript"));
        assert!(prompt.contains("## Latest User Message"));
        // Prior transcript precedes the latest reply.
        assert!(prompt.find("earlier answer").unwrap() < prompt.find("the new reply").unwrap());
    }

    #[test]
    fn trim_resume_transcript_keeps_tail_when_over_limit() {
        let trimmed = trim_resume_transcript("AAAABBBBCCCC", 4);
        assert!(trimmed.starts_with("[Earlier transcript omitted]"));
        assert!(trimmed.contains("CCCC"));
        assert!(!trimmed.contains("AAAA"));
    }

    #[test]
    fn trim_resume_transcript_untouched_within_limit() {
        assert_eq!(trim_resume_transcript("short", 100), "short");
    }
}
