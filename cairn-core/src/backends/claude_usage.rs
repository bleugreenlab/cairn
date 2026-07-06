//! Claude provider usage snapshot collector.
//!
//! Drives the interactive Claude CLI `/usage` TUI through a PTY and scrapes the
//! rendered session/week windows into a [`ProviderUsageSnapshot`]. This is the
//! rich manual probe (source `claude_usage_tui`) behind the Providers settings
//! card, distinct from the coarse live `claude_rate_limit_snapshot` in
//! `backends/claude.rs`, which is fed from streamed rate-limit events.

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use regex::Regex;
use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::models::{ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow};
use crate::orchestrator::session::get_claude_path;
use crate::orchestrator::Orchestrator;

pub fn collect_claude_usage_snapshot(orch: &Orchestrator) -> ProviderUsageSnapshot {
    log::info!("Refreshing Claude provider usage snapshot");
    let claude_path = match get_claude_path(&orch.process_state) {
        Ok(path) => path,
        Err(err) => {
            return ProviderUsageSnapshot::unsupported(
                "claude",
                "claude_usage_tui",
                format!("Claude CLI not found: {err}"),
            );
        }
    };

    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows: 40,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(pair) => pair,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_tui",
                format!("Failed to create PTY: {err}"),
                None,
            );
        }
    };

    let mut cmd = CommandBuilder::new(&claude_path);
    cmd.env_remove("ANTHROPIC_API_KEY");
    cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
    let claude_usage_cwd = resolve_claude_usage_cwd(orch);
    log::info!("Using local Claude CLI auth for usage probe");
    log::info!(
        "Launching Claude usage probe in {}",
        claude_usage_cwd.display()
    );
    cmd.cwd(&claude_usage_cwd);

    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(child) => child,
        Err(err) => {
            return ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_tui",
                format!("Failed to start Claude CLI: {err}"),
                None,
            );
        }
    };

    let mut writer = match pair.master.take_writer() {
        Ok(writer) => writer,
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_tui",
                format!("Failed to open Claude PTY writer: {err}"),
                None,
            );
        }
    };
    let mut reader = match pair.master.try_clone_reader() {
        Ok(reader) => reader,
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_tui",
                format!("Failed to open Claude PTY reader: {err}"),
                None,
            );
        }
    };

    drop(pair.slave);

    let output = Arc::new(Mutex::new(Vec::new()));
    let output_clone = output.clone();
    let reader_thread = thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if let Ok(mut guard) = output_clone.lock() {
                        guard.extend_from_slice(&buf[..n]);
                    }
                }
                Err(_) => break,
            }
        }
    });

    thread::sleep(Duration::from_millis(1500));

    // The interactive TUI gates a new or untrusted directory behind a workspace
    // trust prompt ("Quick safety check"). The probe runs in a dedicated,
    // Cairn-owned directory, so accepting trust is safe and scoped. Accept it
    // once; Claude persists the decision, so future probes skip the prompt.
    // Only wait for the prompt when the directory isn't already trusted, to
    // avoid stalling the common fast path.
    if !claude_dir_is_trusted(&claude_usage_cwd)
        && wait_for_claude_trust_prompt(&output, Duration::from_secs(5))
    {
        log::info!("Claude usage probe: accepting workspace trust prompt");
        // "1. Yes, I trust this folder" is highlighted by default; Enter confirms.
        let _ = writer.write_all(b"\r");
        let _ = writer.flush();
        thread::sleep(Duration::from_millis(1500));
    }

    let _ = writer.write_all(b"/usage\r");
    let _ = writer.flush();
    log::info!("Sent /usage to Claude CLI PTY");

    let usage_ready = wait_for_claude_usage_markers(&output, Duration::from_secs(10));
    log::info!("Claude usage markers observed: {usage_ready}");

    let _ = writer.write_all(b"/exit\r");
    let _ = writer.flush();
    if usage_ready {
        thread::sleep(Duration::from_millis(500));
    } else {
        thread::sleep(Duration::from_millis(1500));
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader_thread.join();

    let raw_output = output
        .lock()
        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        .unwrap_or_default();
    log::info!("Collected Claude PTY output bytes: {}", raw_output.len());
    let snapshot = match std::panic::catch_unwind(|| parse_claude_usage_snapshot(&raw_output)) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let sanitized = sanitize_terminal_output(&raw_output);
            let normalized = normalize_claude_usage_text(&sanitized);
            log::error!(
                "Claude usage parser panicked; sanitized=`{}` normalized=`{}`",
                truncate_for_log(&sanitized, 4000),
                truncate_for_log(&normalized, 4000)
            );
            ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_tui",
                "Claude usage parsing failed unexpectedly.",
                Some(json!({ "output": sanitized, "normalized": normalized })),
            )
        }
    };
    if let Some(error) = snapshot.error.as_deref() {
        log::warn!("Claude usage snapshot refresh failed: {error}");
    } else {
        let labels = snapshot
            .windows
            .iter()
            .map(|window| window.label.clone())
            .collect::<Vec<_>>()
            .join(", ");
        log::info!("Claude usage snapshot parsed windows: {labels}");
    }
    snapshot
}

/// Resolve the working directory for the Claude usage probe.
///
/// The probe drives the interactive TUI, which gates unfamiliar directories
/// behind a workspace trust prompt. Running it in the user's home directory
/// (the previous behavior) reliably tripped that gate, so usage never loaded.
/// Instead we use a dedicated, empty, Cairn-owned directory whose only purpose
/// is this probe — trusting it is safe and the prompt only ever appears once.
fn resolve_claude_usage_cwd(orch: &Orchestrator) -> PathBuf {
    let probe_dir = orch.config_dir.join("claude-usage-probe");
    match fs::create_dir_all(&probe_dir) {
        Ok(()) => {
            log::info!("Resolved Claude usage probe cwd to {}", probe_dir.display());
            probe_dir
        }
        Err(err) => {
            let fallback = dirs::home_dir().unwrap_or_else(|| orch.config_dir.clone());
            log::warn!(
                "Failed to create Claude usage probe dir {}: {err}; falling back to {}",
                probe_dir.display(),
                fallback.display()
            );
            fallback
        }
    }
}

/// Whether Claude has already recorded trust for `dir` in `~/.claude.json`.
///
/// The Claude CLI stores per-directory trust as
/// `projects[<abs path>].hasTrustDialogAccepted`. Reading this lets the probe
/// skip waiting for a trust prompt that won't appear, keeping the common path
/// fast. Reads only; the CLI itself persists trust when we accept the prompt.
fn claude_dir_is_trusted(dir: &std::path::Path) -> bool {
    let Some(home) = dirs::home_dir() else {
        return false;
    };
    let Ok(contents) = fs::read_to_string(home.join(".claude.json")) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(&contents) else {
        return false;
    };
    claude_json_marks_trusted(&value, &dir.to_string_lossy())
}

/// Pure check against parsed `~/.claude.json` contents. Split out for testing.
fn claude_json_marks_trusted(claude_json: &Value, dir: &str) -> bool {
    claude_json
        .get("projects")
        .and_then(|projects| projects.get(dir))
        .and_then(|entry| entry.get("hasTrustDialogAccepted"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Poll PTY output for the workspace trust prompt ("Quick safety check").
///
/// Reuses the same normalization and markers as the parse-failure branch so
/// detection stays consistent. Returns true as soon as the prompt is seen.
fn wait_for_claude_trust_prompt(output: &Arc<Mutex<Vec<u8>>>, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Ok(bytes) = output.lock() {
            let text = String::from_utf8_lossy(&bytes).to_string();
            let normalized = normalize_claude_usage_text(&sanitize_terminal_output(&text));
            if contains_claude_trust_prompt(&normalized) {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(150));
    }
    false
}

/// Whether normalized TUI text contains the workspace trust prompt markers.
fn contains_claude_trust_prompt(normalized: &str) -> bool {
    let lower = normalized.to_lowercase();
    lower.contains("quicksafetycheck") || lower.contains("securityguide")
}

fn wait_for_claude_usage_markers(output: &Arc<Mutex<Vec<u8>>>, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Ok(bytes) = output.lock() {
            let text = String::from_utf8_lossy(&bytes).to_string();
            let normalized = normalize_claude_usage_text(&sanitize_terminal_output(&text));
            if contains_claude_usage_markers(&normalized) {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
    false
}

fn parse_claude_usage_snapshot(raw_output: &str) -> ProviderUsageSnapshot {
    let sanitized = sanitize_terminal_output(raw_output);
    let normalized = normalize_claude_usage_text(&sanitized);
    let heading_re = Regex::new(r"Current\s*session|Current\s*week(?:\s*\([^)]+\))?")
        .expect("valid heading regex");
    let percent_re =
        Regex::new(r"(?P<value>\d+(?:\.\d+)?)\s*%\s*used").expect("valid percent regex");
    let paren_re = Regex::new(r"\((?P<target>[^)]+)\)").expect("valid paren regex");

    let headings: Vec<_> = heading_re.find_iter(&normalized).collect();
    let esc_pos = normalized.rfind("Esc to cancel");
    let mut windows = Vec::new();
    for (index, heading) in headings.iter().enumerate() {
        let start = heading.start();
        let mut end = headings
            .get(index + 1)
            .map(|next| next.start())
            .unwrap_or_else(|| esc_pos.unwrap_or(normalized.len()));
        if end < start {
            end = normalized.len();
        }
        let block = &normalized[start..end];
        let label = normalize_claude_usage_text(heading.as_str().trim());
        let body_start = heading.end().saturating_sub(start).min(block.len());
        let body = &block[body_start..];
        let Some(percent_caps) = percent_re.captures(body) else {
            continue;
        };
        let used_percent = percent_caps
            .name("value")
            .and_then(|m| m.as_str().parse::<f64>().ok())
            .unwrap_or(0.0);
        let scope = classify_claude_scope(&label);
        let scope_target = paren_re
            .captures(&label)
            .and_then(|caps| caps.name("target"))
            .map(|m| m.as_str().trim().to_string());
        let reset_text = extract_claude_reset_text(body);

        windows.push(ProviderUsageWindow {
            id: slugify_label(&label),
            label,
            scope,
            scope_target,
            used_percent,
            remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
            resets_at: None,
            reset_at_text: reset_text,
            window_duration_mins: None,
        });
    }

    if windows.is_empty() {
        let lower = normalized.to_lowercase();
        let message = if contains_claude_trust_prompt(&normalized) {
            "Claude CLI usage probe was blocked by a workspace trust prompt."
        } else if lower.contains("bypasspermissionsmode") || lower.contains("yes,iaccept") {
            "Claude CLI usage probe was blocked by the bypass-permissions warning screen."
        } else if lower.contains("/usage is only available for subscription plans")
            || lower.contains("claude api")
        {
            "Claude usage refresh is running under API auth instead of a subscription session."
        } else if lower.contains("login") || lower.contains("auth") {
            "Claude CLI did not return usage data. Check CLI authentication."
        } else {
            "Claude CLI returned usage output in an unexpected format."
        };
        log::warn!(
            "Claude usage parse failed; sanitized=`{}` normalized=`{}`",
            truncate_for_log(&sanitized, 4000),
            truncate_for_log(&normalized, 4000)
        );
        return ProviderUsageSnapshot::error(
            "claude",
            "claude_usage_tui",
            message,
            Some(json!({ "output": sanitized, "normalized": normalized })),
        );
    }

    ProviderUsageSnapshot {
        backend: "claude".to_string(),
        source: "claude_usage_tui".to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits: None,
        reset_credits: None,
        error: None,
        unsupported_reason: None,
        raw: Some(json!({ "output": sanitized, "normalized": normalized })),
        model_breakdown: None,
    }
}

/// Extract only the human reset time from a usage block body.
///
/// The weekly window in Claude's `/usage` TUI is followed by an explanatory
/// wall ("What's contributing to your limits usage…"). Because the whole TUI is
/// normalized onto a single line, the previous greedy `Resets …$` capture
/// swallowed that wall into `reset_at_text`. The reset time always ends with a
/// parenthesized timezone (e.g. "Jun 8 at 1pm (America/Los_Angeles)"), so we
/// capture up to and including the first parenthetical after "Resets" and
/// discard everything after it.
fn extract_claude_reset_text(body: &str) -> Option<String> {
    let reset_re = Regex::new(r"Resets?\s*(?P<reset>[^()]*?\([^)]*\))").expect("valid reset regex");
    reset_re
        .captures(body)
        .and_then(|caps| caps.name("reset"))
        .map(|m| normalize_claude_usage_text(m.as_str().trim()))
        .filter(|text| !text.is_empty())
}

fn sanitize_terminal_output(output: &str) -> String {
    let ansi_re = Regex::new(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07]*(?:\x07|\x1b\\)")
        .expect("valid ansi regex");
    ansi_re
        .replace_all(&output.replace('\r', "\n"), "")
        .to_string()
}

fn normalize_claude_usage_text(text: &str) -> String {
    let mut normalized = text
        .replace("Curre t", "Current")
        .replace("Curret", "Current")
        .replace("Rese s", "Resets")
        .replace("Reses", "Resets");
    for (pattern, replacement) in [
        (r"C\s*u\s*r\s*r\s*e\s*n?\s*t", "Current"),
        (r"R\s*e\s*s\s*e\s*t?\s*s", "Resets"),
        (r"s\s*e\s*s\s*s\s*i\s*o\s*n", "session"),
        (r"w\s*e\s*e\s*k", "week"),
    ] {
        let re = Regex::new(pattern).expect("valid normalization regex");
        normalized = re.replace_all(&normalized, replacement).to_string();
    }
    for (pattern, replacement) in [
        (r"Currentsession", "Current session"),
        (r"Currentweek", "Current week"),
        (r"Esctocancel", "Esc to cancel"),
        (r"([A-Za-z\)])(\d)", "$1 $2"),
        (r"(\d)%used", "$1% used"),
        (r"%\s*used", "% used"),
        (r"session\(", "session ("),
        (r"week\(", "week ("),
        (r"models\)(\d)", "models) $1"),
        (r"only\)(\d)", "only) $1"),
        (r"Resets([A-Z])", "Resets $1"),
        (r"Resets(\d)", "Resets $1"),
    ] {
        let re = Regex::new(pattern).expect("valid spacing regex");
        normalized = re.replace_all(&normalized, replacement).to_string();
    }
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn contains_claude_usage_markers(text: &str) -> bool {
    let heading_re = Regex::new(r"Current\s*session|Current\s*week(?:\s*\([^)]+\))?")
        .expect("valid heading regex");
    let percent_re = Regex::new(r"\d+(?:\.\d+)?\s*%\s*used").expect("valid percent regex");
    heading_re.is_match(text) && percent_re.is_match(text)
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    let truncated = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn classify_claude_scope(label: &str) -> ProviderUsageScope {
    let lower = label.to_lowercase();
    if lower.contains("session") {
        ProviderUsageScope::Session
    } else if lower.contains("week") {
        ProviderUsageScope::Weekly
    } else {
        ProviderUsageScope::Custom
    }
}

fn slugify_label(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claude_json_marks_trusted() {
        let value = serde_json::json!({
            "projects": {
                "/trusted/dir": { "hasTrustDialogAccepted": true },
                "/untrusted/dir": { "hasTrustDialogAccepted": false },
                "/no/field/dir": { "allowedTools": [] },
            }
        });
        assert!(claude_json_marks_trusted(&value, "/trusted/dir"));
        assert!(!claude_json_marks_trusted(&value, "/untrusted/dir"));
        assert!(!claude_json_marks_trusted(&value, "/no/field/dir"));
        assert!(!claude_json_marks_trusted(&value, "/missing/dir"));
        assert!(!claude_json_marks_trusted(
            &serde_json::json!({}),
            "/trusted/dir"
        ));
    }

    // Captured from the Claude CLI trust gate: the TUI renders styled chars with
    // no internal spacing, so words arrive concatenated ("Quicksafetycheck").
    const TRUST_PROMPT_RAW: &str = concat!(
        "Accessingworkspace:\n/private/tmp/probe\n",
        "Quicksafetycheck:Isthisaprojectyoucreatedoroneyoutrust?\n",
        "Securityguide\n",
        "\u{276f}1.Yes,Itrustthisfolder\n2.No,exit\nEntertoconfirm\u{b7}Esctocancel"
    );

    #[test]
    fn test_contains_claude_trust_prompt() {
        let normalized = normalize_claude_usage_text(&sanitize_terminal_output(TRUST_PROMPT_RAW));
        assert!(contains_claude_trust_prompt(&normalized));

        let usage = normalize_claude_usage_text(&sanitize_terminal_output(
            "Current session\n42% used\nResets in 2 hours",
        ));
        assert!(!contains_claude_trust_prompt(&usage));
    }

    #[test]
    fn test_wait_for_claude_trust_prompt_detects_prompt() {
        let output = Arc::new(Mutex::new(TRUST_PROMPT_RAW.as_bytes().to_vec()));
        assert!(wait_for_claude_trust_prompt(
            &output,
            Duration::from_millis(500)
        ));
    }

    #[test]
    fn test_wait_for_claude_trust_prompt_times_out_without_prompt() {
        let output = Arc::new(Mutex::new(b"Current session\n42% used".to_vec()));
        assert!(!wait_for_claude_trust_prompt(
            &output,
            Duration::from_millis(300)
        ));
    }

    #[test]
    fn parses_claude_usage_tui_output() {
        let output = concat!(
            "Curret session 0% used Reses 1m (America/New_York) ",
            "Current week (all models) 19% used Resets Apr 2 at 11pm (America/New_York) ",
            "Current week (Sonnet only) 3% used Resets Apr 3 at 9pm (America/New_York) Esc to cancel",
        );

        let snapshot = parse_claude_usage_snapshot(output);
        assert_eq!(snapshot.error, None);
        assert_eq!(snapshot.windows.len(), 3);
        assert_eq!(snapshot.windows[0].label, "Current session");
        assert_eq!(snapshot.windows[1].label, "Current week (all models)");
        assert_eq!(snapshot.windows[2].label, "Current week (Sonnet only)");
        assert_eq!(snapshot.windows[1].remaining_percent, 81.0);
    }

    #[test]
    fn parses_claude_usage_tui_output_with_collapsed_spacing() {
        let output = concat!(
            "Currentsession0%usedResets1m(America/New_York)",
            "Currentweek(allmodels)19%usedResetsApr2at11pm(America/New_York)",
            "Currentweek(Sonnetonly)3%usedResetsApr3at9pm(America/New_York)Esctocancel",
        );

        let snapshot = parse_claude_usage_snapshot(output);
        assert_eq!(snapshot.error, None);
        assert_eq!(snapshot.windows.len(), 3);
        assert_eq!(snapshot.windows[0].label, "Current session");
        assert_eq!(snapshot.windows[1].label, "Current week (allmodels)");
        assert_eq!(snapshot.windows[2].label, "Current week (Sonnetonly)");
        assert_eq!(snapshot.windows[2].remaining_percent, 97.0);
    }

    #[test]
    fn extract_claude_reset_text_stops_at_timezone() {
        assert_eq!(
            extract_claude_reset_text("19% used Resets Apr 2 at 11pm (America/New_York)"),
            Some("Apr 2 at 11pm (America/New_York)".to_string())
        );
        // No parenthetical timezone -> nothing to anchor on -> None (no garbage).
        assert_eq!(extract_claude_reset_text("19% used Resets soon"), None);
    }

    #[test]
    fn parse_claude_usage_reset_text_excludes_explanatory_wall() {
        // The weekly window is followed by the "What's contributing…" wall the
        // CLI renders before "Esc to cancel". reset_at_text must carry only the
        // human reset time, not that wall.
        let output = concat!(
            "Current session 0% used Resets 1m (America/Los_Angeles) ",
            "Current week (all models) 19% used Resets Jun 8 at 1pm (America/Los_Angeles) ",
            "What's contributing to your limits usage? Most of your usage comes from ",
            "large attachments and long conversations. Esc to cancel",
        );

        let snapshot = parse_claude_usage_snapshot(output);
        assert_eq!(snapshot.error, None);
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(
            snapshot.windows[1].reset_at_text.as_deref(),
            Some("Jun 8 at 1pm (America/Los_Angeles)")
        );
        // Confirm the wall did not leak in.
        assert!(!snapshot.windows[1]
            .reset_at_text
            .as_deref()
            .unwrap()
            .contains("contributing"));
    }
}
