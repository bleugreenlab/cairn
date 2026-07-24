use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;

use super::routing::{route_wake, route_wake_sync};
use super::types::*;

/// Compact runtime formatting for terminal-exit messages: `45s`, `2m05s`, `1h03m`.
fn fmt_runtime(secs: i64) -> String {
    let s = secs.max(0);
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// The rich resume message an agent sees when a subscribed terminal exits:
/// slug, exit code, runtime, the canonical URI to read, and a short output tail.
pub(crate) fn format_terminal_exit_message(
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> String {
    let code = match exit_code {
        Some(code) => format!("exit code {code}"),
        None => "exit code unknown".to_string(),
    };
    let mut out = format!("[Terminal exit] `{slug}` finished — {code}.");
    if let Some(rt) = runtime_secs {
        out.push_str(&format!(" Ran {}.", fmt_runtime(rt)));
    }
    out.push_str(&format!(" Read {detail_uri} for full output."));
    if let Some(tail) = tail.map(str::trim).filter(|t| !t.is_empty()) {
        out.push_str(&format!("\n\nFinal output:\n```\n{tail}\n```"));
    }
    out
}

fn terminal_exit_event(
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> WakeEvent {
    WakeEvent {
        // The match key is the terminal's canonical URI, NOT the bare slug:
        // slugs are only unique per job (and promotion auto-mints run-1, run-2,
        // … in every job), so a slug reference would cross-match every job's
        // same-slug subscriber. The slug stays in the human-readable message.
        source: WakeSource::Process {
            reference: detail_uri.to_string(),
        },
        fact_kind: FACT_KIND_TERMINAL_EXIT.to_string(),
        detail_uri: Some(detail_uri.to_string()),
        delivery: WakeDelivery::Broadcast {
            message: format_terminal_exit_message(slug, detail_uri, exit_code, runtime_secs, tail),
        },
        urgency: DeliveryUrgency::Queue,
    }
}

/// Route a terminal-exit wake (sync, from the PTY/promoted exit threads).
pub(crate) fn route_terminal_exit(
    orch: &Orchestrator,
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake_sync(
        orch,
        terminal_exit_event(slug, detail_uri, exit_code, runtime_secs, tail),
    )
}

/// Route a terminal-exit wake from an async context (the immediate-fire path when
/// subscribing to an already-exited terminal).
pub async fn route_terminal_exit_async(
    orch: &Orchestrator,
    slug: &str,
    detail_uri: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    tail: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake(
        orch,
        terminal_exit_event(slug, detail_uri, exit_code, runtime_secs, tail),
    )
    .await
}

/// The resume message an agent sees when a watched phrase appears in a live
/// terminal's output: slug, the matched phrase, the canonical URI to read, and a
/// short one-line excerpt (ANSI-stripped, capped) of the matching line.
pub(crate) fn format_terminal_output_message(
    slug: &str,
    detail_uri: &str,
    phrase: &str,
    excerpt: Option<&str>,
) -> String {
    let mut out = format!(
        "[Terminal output match] `{slug}` printed the watched phrase \"{phrase}\". Read {detail_uri} for full output."
    );
    if let Some(excerpt) = excerpt.map(str::trim).filter(|e| !e.is_empty()) {
        out.push_str(&format!("\n\nMatched line:\n```\n{excerpt}\n```"));
    }
    out
}

fn terminal_output_event(
    subscriber_job_id: &str,
    slug: &str,
    detail_uri: &str,
    phrase: &str,
    excerpt: Option<&str>,
) -> WakeEvent {
    WakeEvent {
        source: WakeSource::Process {
            reference: detail_uri.to_string(),
        },
        fact_kind: FACT_KIND_TERMINAL_OUTPUT.to_string(),
        detail_uri: Some(detail_uri.to_string()),
        // Targeted, not broadcast: a phrase match is private to the one job whose
        // watcher matched. Two jobs (or two phrases) watching the same terminal
        // must not cross-wake on each other's match.
        delivery: WakeDelivery::Targeted {
            subscriber_job_id: subscriber_job_id.to_string(),
            message: format_terminal_output_message(slug, detail_uri, phrase, excerpt),
        },
        urgency: DeliveryUrgency::Queue,
    }
}

/// Route a terminal-output phrase wake (sync, from the agent PTY read loop).
fn route_terminal_output(
    orch: &Orchestrator,
    subscriber_job_id: &str,
    slug: &str,
    detail_uri: &str,
    phrase: &str,
    excerpt: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake_sync(
        orch,
        terminal_output_event(subscriber_job_id, slug, detail_uri, phrase, excerpt),
    )
}

/// Route a terminal-output phrase wake from an async context (the immediate-fire
/// path when the phrase is already in the buffer at subscribe time).
pub(crate) async fn route_terminal_output_async(
    orch: &Orchestrator,
    subscriber_job_id: &str,
    slug: &str,
    detail_uri: &str,
    phrase: &str,
    excerpt: Option<&str>,
) -> Result<WakeRouteAction, String> {
    route_wake(
        orch,
        terminal_output_event(subscriber_job_id, slug, detail_uri, phrase, excerpt),
    )
    .await
}

/// The trailing `<slug>` of a canonical `.../terminal/<slug>` URI.
fn terminal_slug_from_uri(uri: &str) -> &str {
    uri.rsplit("/terminal/").next().unwrap_or(uri)
}

/// Scan one freshly read output chunk against a terminal's live phrase-watcher
/// registry and route a one-shot `terminal_output` wake for each matched phrase,
/// dropping the matched watcher. Shared by both terminal readers — the agent-MCP
/// reader in `mcp::handlers::run` and the interactive reader in the app's
/// terminal command — so output watching behaves identically regardless of which
/// path created the terminal. Each watcher carries its own canonical
/// `terminal_uri`, so routing needs no `CairnResource`. A cheap no-op when no
/// watchers are registered, which is the common case for every terminal.
pub fn scan_and_route_terminal_output(
    orch: &Orchestrator,
    watchers: &std::sync::Arc<std::sync::Mutex<Vec<crate::services::TerminalOutputWatcher>>>,
    data: &str,
) {
    // Collect matches under the lock, then route after releasing it so wake
    // routing never runs while holding the watcher mutex.
    let matched: Vec<(String, String, String, Option<String>)> = {
        let Ok(mut guard) = watchers.lock() else {
            return;
        };
        if guard.is_empty() {
            return;
        }
        let mut matched = Vec::new();
        guard.retain_mut(|watcher| {
            let scan = crate::services::scan_for_phrase(&watcher.carry, data, &watcher.phrase);
            match scan.matched_excerpt {
                Some(excerpt) => {
                    matched.push((
                        watcher.job_id.clone(),
                        watcher.terminal_uri.clone(),
                        watcher.phrase.clone(),
                        Some(excerpt),
                    ));
                    false // drop the matched watcher (one-shot)
                }
                None => {
                    watcher.carry = scan.carry;
                    true
                }
            }
        });
        matched
    };
    for (job_id, terminal_uri, phrase, excerpt) in matched {
        let slug = terminal_slug_from_uri(&terminal_uri);
        if let Err(error) = route_terminal_output(
            orch,
            &job_id,
            slug,
            &terminal_uri,
            &phrase,
            excerpt.as_deref(),
        ) {
            log::warn!("Failed to route terminal-output wake for {terminal_uri}: {error}");
        }
    }
}
