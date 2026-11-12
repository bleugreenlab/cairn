//! The build center's reads: builds of Cairn itself, as the operator's app shows
//! them beside the notification bell.
//!
//! [`list`] projects the rebuild ledger into what a person reads about a build:
//! where it stands, its phases and their times, the version it built and the
//! actor's one summary. The actor's events, facts and identifiers stay in the
//! ledger (`cairn://rebuilds/<id>` renders them whole).
//!
//! [`build_log_tail`] serves the last lines of the acting rebuild's build log,
//! read from the rebuild's own directory on the runner's host. It answers only
//! while that rebuild is acting; a settled rebuild's log is evidence for the
//! ledger record, not a live view.

use crate::storage::LocalDb;
use cairn_db::storage::rebuilds::RebuildRecord;
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Where the rebuild actor appends its dependency and build output, inside the
/// rebuild's directory (`scripts/rebuild.ts`).
const BUILD_LOG: &str = "build.log";
/// The most a tail reads from the end of the log, whatever its size.
const TAIL_BYTES: u64 = 64 * 1024;
const MAX_TAIL_LINES: usize = 50;
const MAX_LISTED: usize = 50;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RebuildPhaseView {
    pub name: String,
    pub started: Option<String>,
    pub ended: Option<String>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RebuildView {
    pub id: String,
    /// `acting` until the actor's outcome event, or a death the runner
    /// observed, settles the ledger row.
    pub status: String,
    pub outcome: Option<String>,
    pub summary: Option<String>,
    /// The operator request this rebuild acts under; absent for one no request
    /// launched.
    pub request_id: Option<String>,
    pub version: Option<String>,
    /// When the actor began: its first phase, else the launch the runner
    /// recorded before bootstrapping it.
    pub started_at: Option<String>,
    /// When the actor reported its outcome.
    pub ended_at: Option<String>,
    pub phases: Vec<RebuildPhaseView>,
}

fn view(row: RebuildRecord) -> RebuildView {
    let launched_at = row
        .launch
        .as_ref()
        .and_then(|launch| chrono::DateTime::from_timestamp(launch.recorded_at, 0))
        .map(|at| at.to_rfc3339());
    let started_at = row
        .phases
        .iter()
        .find_map(|phase| phase.started.clone())
        .or(launched_at);
    let ended_at = row
        .events
        .iter()
        .rev()
        .find(|event| event["type"] == "outcome")
        .and_then(|event| event["at"].as_str())
        .map(str::to_string);
    RebuildView {
        id: row.id,
        status: row.status,
        outcome: row.outcome,
        summary: row.summary,
        request_id: row.request_id,
        version: row.version,
        started_at,
        ended_at,
        phases: row
            .phases
            .into_iter()
            .map(|phase| RebuildPhaseView {
                name: phase.name,
                started: phase.started,
                ended: phase.ended,
                outcome: phase.outcome,
            })
            .collect(),
    }
}

/// The newest rebuilds first.
pub async fn list(db: &LocalDb, limit: usize) -> Result<Vec<RebuildView>, String> {
    let rows = db
        .list_rebuilds(limit.clamp(1, MAX_LISTED), 0)
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows.into_iter().map(view).collect())
}

/// A ledger identifier is one path segment the journal reader also accepts.
fn ledger_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// The last `lines` lines of the acting rebuild's build log, as a terminal would
/// show them. `None` when no rebuild `id` is acting; empty while the actor has
/// not logged yet.
pub async fn build_log_tail(
    db: &LocalDb,
    rebuilds: &Path,
    id: &str,
    lines: usize,
) -> Result<Option<Vec<String>>, String> {
    if !ledger_identifier(id) {
        return Err(format!("{id:?} is not a rebuild identifier"));
    }
    let acting = db
        .get_rebuild(id)
        .await
        .map_err(|error| error.to_string())?
        .is_some_and(|row| row.status == "acting");
    if !acting {
        return Ok(None);
    }
    let path = rebuilds.join(id).join(BUILD_LOG);
    let lines = lines.clamp(1, MAX_TAIL_LINES);
    tokio::task::spawn_blocking(move || read_tail(&path, lines))
        .await
        .map_err(|error| error.to_string())?
        .map(Some)
}

fn read_tail(path: &PathBuf, lines: usize) -> Result<Vec<String>, String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    // The rebuild directory is the actor's; a link planted in it is not its log.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(error) => return Err(format!("could not open the build log: {error}")),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not read the build log: {error}"))?;
    if !metadata.is_file() {
        return Err("the build log is not a regular file".into());
    }
    let start = metadata.len().saturating_sub(TAIL_BYTES);
    file.seek(SeekFrom::Start(start))
        .map_err(|error| format!("could not read the build log: {error}"))?;
    let mut bytes = Vec::new();
    file.take(TAIL_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read the build log: {error}"))?;
    let text = String::from_utf8_lossy(&bytes);
    let mut rows = text.split('\n');
    if start > 0 {
        // A read that began mid-file began mid-line.
        rows.next();
    }
    Ok(last_lines(rows, lines))
}

fn last_lines<'a>(rows: impl Iterator<Item = &'a str>, lines: usize) -> Vec<String> {
    let mut shown: Vec<String> = rows
        .map(terminal_line)
        .filter(|line| !line.is_empty())
        .collect();
    let skip = shown.len().saturating_sub(lines);
    shown.drain(..skip);
    shown
}

/// What a terminal leaves on screen for one logged line: the frame after its
/// last carriage return (a progress bar redraws in place), without escape
/// sequences or other control characters.
fn terminal_line(raw: &str) -> String {
    let raw = raw.trim_end_matches('\r');
    let frame = raw.rsplit('\r').next().unwrap_or(raw);
    let mut out = String::with_capacity(frame.len());
    let mut chars = frame.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => match chars.next() {
                // CSI: parameters, then one final byte in @..~.
                Some('[') => {
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                // OSC: up to BEL or the ESC \ string terminator.
                Some(']') => {
                    while let Some(next) = chars.next() {
                        if next == '\u{7}' {
                            break;
                        }
                        if next == '\u{1b}' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\t' => out.push(' '),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out.trim_end().to_string()
}

#[cfg(test)]
mod tests;
