//! Retained terminal output: the one capture stream a terminal tees into, kept
//! in runner-owned storage outside disposable scratch and addressed by an opaque
//! output id.
//!
//! # Why it lives outside scratch
//!
//! A terminal's transcript used to sit in its job's scratch directory, keyed by
//! slug. Resolving the issue removed that directory and deleted the terminal
//! rows in one teardown, so an exit notification that arrived afterwards pointed
//! at nothing (bug sink #544: two test terminals stopped by a merge reported
//! exits 100 and 1, and their summaries were unrecoverable minutes later). The
//! bytes now live under `<cairn_home>/terminal-output/<id>.log` and outlive the
//! terminal row, the job's scratch, the live session and the executor.
//!
//! # One record per capture
//!
//! A `terminal_outputs` row describes one capture: who owns it (the canonical
//! terminal URI it was created under), whether capture is still running or
//! ended complete or incomplete, how many older bytes compaction dropped, the
//! exit, the resolution time and the retention deadline. `terminal_id` points at
//! the `job_terminals` row currently capturing into it. A recovery restart
//! re-points the same record at its replacement row, so a terminal Cairn starts
//! again keeps one transcript. A deliberate same-slug create mints a fresh
//! record; the previous one keeps its own id, bytes and deadline, and its
//! `terminal_id` is left naming a row that no longer exists. There is no
//! incarnation history beyond that: the current terminal is still found by
//! `job_terminals`' own uniqueness rules, and a record is reached only by its id.
//!
//! # Retention
//!
//! An issue node's or task's output is kept for [`RETENTION_SECS`] after the
//! later of its exit and its issue's resolution; until the issue resolves the
//! deadline is open. Project and thread terminals have no resolution, so theirs
//! is seven days after exit. A deadline only ever moves later ([`extend`]).
//! [`prune`] deletes expired records a bounded page at a time, file first and
//! row second, so an interrupted pass leaves a row whose file is already gone
//! and the next pass finishes it. A record still capturing for a live terminal
//! row is never deleted, whatever its deadline.
//!
//! Deleting an issue or a project is destructive and outside this policy: its
//! records are expired at once ([`expire_for_jobs`], [`expire_for_project`])
//! and go at the next prune.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

/// How long a record is kept past the later of exit and resolution.
pub const RETENTION_SECS: i64 = 7 * 86_400;

/// Compaction cap for a capture. When the file grows past this it is rewritten
/// down to its trailing [`KEEP_BYTES`]: for a long-running process (a dev
/// server) the tail is the valuable part.
const MAX_BYTES: u64 = 24 * 1024 * 1024;
/// Bytes kept when a capture is compacted.
const KEEP_BYTES: u64 = 12 * 1024 * 1024;

/// The first line of a compacted capture. It carries the running total of
/// dropped bytes, so the file states its own truncation even when the runner
/// stopped before the record was finalized, and a reopened capture resumes the
/// count.
const COMPACTED_PREFIX: &str = "=== compacted: ";
const COMPACTED_SUFFIX: &str = " earlier bytes dropped ===\n";

/// Records deleted per prune page.
const PRUNE_PAGE_ROWS: i64 = 64;
/// Pages one prune pass walks at most.
const PRUNE_PAGES: usize = 16;

const OUTPUT_DIR: &str = "terminal-output";

/// Where retained captures live: `<cairn_home>/terminal-output`.
///
/// Derived from the scratch root's parent, which is the Cairn home in
/// production and the per-process private home a test pins, so a test never
/// writes into the machine's real home.
pub fn output_root() -> PathBuf {
    let scratch = cairn_common::scratch::scratch_root();
    scratch
        .parent()
        .map(|home| home.join(OUTPUT_DIR))
        .unwrap_or_else(|| scratch.join(OUTPUT_DIR))
}

/// A fresh opaque output id.
pub fn mint_output_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Whether `id` has the shape [`mint_output_id`] produces, so an id read from
/// a URI can never name a path outside [`output_root`].
pub fn is_output_id(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok()
}

/// The capture file of `output_id`.
pub fn output_log_path(output_id: &str) -> PathBuf {
    let name = if is_output_id(output_id) {
        output_id
    } else {
        "invalid"
    };
    output_root().join(format!("{name}.log"))
}

/// The address of one retained capture: the owning terminal's canonical URI
/// with the record's id. Reading it never resolves to whichever terminal holds
/// the slug now.
pub fn retained_uri(terminal_uri: &str, output_id: &str) -> String {
    format!("{terminal_uri}?output={output_id}")
}

/// The append handle a terminal tees every chunk into.
///
/// File writes are unbuffered, so what was delivered is on disk on every
/// end-of-life path, including a crash where the finalizer never runs. A
/// failure to open or write is recorded rather than raised: teeing never blocks
/// the terminal, and the record says its capture is incomplete.
pub struct TerminalLog {
    output_id: String,
    path: PathBuf,
    file: Option<File>,
    /// Tracked length, so the cap check is a comparison rather than a `stat`.
    len: u64,
    dropped_bytes: u64,
    failure: Option<String>,
    /// The executor behind this capture does not report the end of its
    /// output, so the capture cannot say it is whole.
    unreported: bool,
}

/// What a capture can say about itself when it ends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureSummary {
    pub dropped_bytes: u64,
    pub bytes: u64,
    pub failure: Option<String>,
    pub unreported: bool,
}

impl TerminalLog {
    /// Open (append) the capture of `output_id`, creating the output root.
    /// Reopening a non-empty capture (a recovery restart or a re-adoption of the
    /// same terminal) writes a separator first, so the two processes' output
    /// stays distinguishable.
    pub fn open(output_id: &str) -> Self {
        let path = output_log_path(output_id);
        let mut log = Self {
            output_id: output_id.to_string(),
            path: path.clone(),
            file: None,
            len: 0,
            dropped_bytes: 0,
            failure: None,
            unreported: false,
        };
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                log.failure = Some(format!("could not create {}: {error}", parent.display()));
                return log;
            }
        }
        let existing_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        log.dropped_bytes = read_dropped_bytes(&path);
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => file,
            Err(error) => {
                log.failure = Some(format!("could not open {}: {error}", path.display()));
                return log;
            }
        };
        log.len = existing_len;
        if existing_len > 0 {
            let marker = format!(
                "\n=== session restarted {} ===\n",
                chrono::Utc::now().to_rfc3339()
            );
            if file.write_all(marker.as_bytes()).is_ok() {
                log.len += marker.len() as u64;
            }
        }
        log.file = Some(file);
        log
    }

    pub fn output_id(&self) -> &str {
        &self.output_id
    }

    /// Append one delivered chunk, compacting past the cap.
    pub fn append(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let Some(file) = self.file.as_mut() else {
            return;
        };
        if let Err(error) = file.write_all(bytes) {
            self.failure.get_or_insert_with(|| {
                format!("a write to {} failed: {error}", self.path.display())
            });
            return;
        }
        self.len += bytes.len() as u64;
        if self.len > MAX_BYTES {
            self.compact();
        }
    }

    pub fn flush(&mut self) {
        if let Some(file) = self.file.as_mut() {
            let _ = file.flush();
        }
    }

    pub fn summary(&self) -> CaptureSummary {
        CaptureSummary {
            dropped_bytes: self.dropped_bytes,
            bytes: self.len,
            failure: self.failure.clone(),
            unreported: self.unreported,
        }
    }

    /// Record a reason this capture may not hold everything its terminal
    /// produced, beside any it already has.
    pub fn mark_incomplete(&mut self, reason: &str) {
        self.failure = Some(match self.failure.take() {
            Some(earlier) => format!("{earlier}; {reason}"),
            None => reason.to_string(),
        });
    }

    /// Record what the stream behind this capture showed at its exit.
    pub fn settle_stream(&mut self, verdict: StreamVerdict) {
        match verdict {
            StreamVerdict::Whole => {}
            StreamVerdict::Unreported => self.unreported = true,
            StreamVerdict::Short(reason) => self.mark_incomplete(&reason),
        }
    }

    /// Rewrite the file down to its trailing [`KEEP_BYTES`] through a temp-file
    /// swap, then reopen the append handle. Any failure leaves the oversized
    /// file as it was.
    fn compact(&mut self) {
        self.flush();
        let Ok(mut src) = File::open(&self.path) else {
            return;
        };
        let file_len = src.metadata().map(|m| m.len()).unwrap_or(0);
        let keep = KEEP_BYTES.min(file_len);
        let start = file_len - keep;
        if src.seek(SeekFrom::Start(start)).is_err() {
            return;
        }
        let mut tail = Vec::with_capacity(keep as usize);
        if src.read_to_end(&mut tail).is_err() {
            return;
        }
        drop(src);
        // The previous marker (if any) sits at the head of what is dropped, so
        // its bytes count as dropped too; the new marker carries the total.
        let dropped = self.dropped_bytes + start;
        let marker = format!("{COMPACTED_PREFIX}{dropped}{COMPACTED_SUFFIX}");
        let tmp = self.path.with_extension("log.compacting");
        let written = (|| -> std::io::Result<()> {
            let mut out = File::create(&tmp)?;
            out.write_all(marker.as_bytes())?;
            out.write_all(&tail)?;
            out.flush()
        })();
        if written.is_err() || std::fs::rename(&tmp, &self.path).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        if let Ok(file) = OpenOptions::new().append(true).open(&self.path) {
            self.len = marker.len() as u64 + tail.len() as u64;
            self.dropped_bytes = dropped;
            self.file = Some(file);
        }
    }
}

/// The running total a compacted capture's first line states, or 0.
/// What the runner can vouch for about the executor stream behind a capture.
///
/// An executor numbers a PTY's chunks from 1. One advertising the output end
/// also sends, ahead of the exit, an end naming the last number. A capture can
/// claim to be complete only when that end arrived closed and no number before
/// it went missing: an executor drops what a process writes while its link is
/// down, and an exit announced without the end may have overtaken the final
/// output. From an executor that does not advertise the end, a missing end
/// proves nothing, and completeness is not reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamLedger {
    /// The highest chunk number heard, or `None` before the first chunk of a
    /// stream joined partway through.
    last: Option<u64>,
    missing: u64,
    end: Option<StreamEnd>,
}

/// How a PTY's output ended, as its executor reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEnd {
    /// The PTY closed: nothing follows.
    Closed,
    /// The executor's drain bound passed with the PTY still open.
    StillOpen,
}

/// What a capture can say about the stream behind it once its terminal exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamVerdict {
    /// Every chunk the executor sent arrived ahead of a closed end.
    Whole,
    /// The executor does not report the end, so completeness is unknown.
    Unreported,
    /// Output may be missing, for the stated reason.
    Short(String),
}

/// The note a capture carries when its exit arrived without the stream's end
/// from an executor that reports one.
const UNCONFIRMED_END_NOTE: &str =
    "the exit arrived without the end of the output, so it may have overtaken the final output";

/// The note a capture carries when the drain bound passed with the PTY open.
const STILL_OPEN_NOTE: &str =
    "the output was still open at the executor's drain bound, so anything written after it is missing";

/// The note a capture carries when its executor went away under it.
const EXECUTOR_LOST_NOTE: &str =
    "the executor went away before the output ended, so the final output may be missing";

/// What a capture whose executor does not report the end says about itself.
pub const UNREPORTED_NOTE: &str =
    "completeness not reported: this terminal's executor predates the output-end marker";

impl StreamLedger {
    /// A capture that listened from before its process's first chunk.
    pub fn from_first_chunk() -> Self {
        Self {
            last: Some(0),
            missing: 0,
            end: None,
        }
    }

    /// A capture that joined a process already running (a re-adoption), which
    /// cannot count the chunks sent before it listened.
    pub fn mid_stream() -> Self {
        Self {
            last: None,
            missing: 0,
            end: None,
        }
    }

    /// Whether the chunk numbered `sequence` is new. A repeated or older number
    /// is refused, so a chunk delivered twice is captured once; a jump counts
    /// the chunks between that never arrived.
    pub fn accept(&mut self, sequence: u64) -> bool {
        match self.last {
            Some(last) if sequence <= last => false,
            Some(last) => {
                self.missing += sequence - last - 1;
                self.last = Some(sequence);
                true
            }
            None => {
                self.last = Some(sequence);
                true
            }
        }
    }

    /// The stream ended at chunk `sequence`; any number up to it not yet heard
    /// never arrived. Only the first end counts.
    pub fn end(&mut self, sequence: u64, end: StreamEnd) {
        if self.end.is_some() {
            return;
        }
        if let Some(last) = self.last {
            self.missing += sequence.saturating_sub(last);
        }
        self.last = Some(self.last.map_or(sequence, |last| last.max(sequence)));
        self.end = Some(end);
    }

    /// What the capture can say about its stream at the exit. `reports_end` is
    /// whether the executor advertises the end; `executor_lost` whether the
    /// exit says the executor went away. A gap in the numbers is lost output
    /// whichever executor sent them.
    pub fn verdict(&self, reports_end: bool, executor_lost: bool) -> StreamVerdict {
        let mut reasons = Vec::new();
        if executor_lost {
            reasons.push(EXECUTOR_LOST_NOTE.to_string());
        } else {
            match (self.end, reports_end) {
                (Some(StreamEnd::Closed), _) | (None, false) => {}
                (Some(StreamEnd::StillOpen), _) => reasons.push(STILL_OPEN_NOTE.to_string()),
                (None, true) => reasons.push(UNCONFIRMED_END_NOTE.to_string()),
            }
        }
        match self.missing {
            0 => {}
            1 => reasons.push("1 output chunk from the executor never arrived".to_string()),
            n => reasons.push(format!("{n} output chunks from the executor never arrived")),
        }
        if !reasons.is_empty() {
            StreamVerdict::Short(reasons.join("; "))
        } else if self.end.is_none() {
            StreamVerdict::Unreported
        } else {
            StreamVerdict::Whole
        }
    }
}

fn read_dropped_bytes(path: &std::path::Path) -> u64 {
    let Ok(mut file) = File::open(path) else {
        return 0;
    };
    let mut head = [0u8; 96];
    let read = file.read(&mut head).unwrap_or(0);
    parse_compacted_marker(&head[..read]).unwrap_or(0)
}

fn parse_compacted_marker(head: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(head)
        .ok()
        .or_else(|| std::str::from_utf8(&head[..head.len().min(64)]).ok())?;
    let rest = text.strip_prefix(COMPACTED_PREFIX)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// Read a capture whole. `None` when the file is gone or empty.
pub fn read_capture(output_id: &str) -> Option<Vec<u8>> {
    let raw = std::fs::read(output_log_path(output_id)).ok()?;
    (!raw.is_empty()).then_some(raw)
}

/// What starts a record's retention clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryBasis {
    /// An issue node or task: the later of exit and the issue's resolution.
    Resolution,
    /// A project or thread terminal, which no resolution ends: its exit.
    Exit,
}

impl ExpiryBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resolution => "resolution",
            Self::Exit => "exit",
        }
    }

    fn parse(value: &str) -> Self {
        if value == "exit" {
            Self::Exit
        } else {
            Self::Resolution
        }
    }

    /// The basis of a terminal created under `resource`.
    pub fn for_resource(resource: &cairn_uri::CairnResource) -> Self {
        use cairn_uri::CairnResource;
        match resource {
            CairnResource::ProjectTerminal { .. } => Self::Exit,
            CairnResource::NodeTerminal {
                number,
                exec_seq,
                node_id,
                ..
            }
            | CairnResource::TaskTerminal {
                number,
                exec_seq,
                node_id,
                ..
            } => match cairn_uri::NodeAddress::new(*number, *exec_seq, node_id) {
                cairn_uri::NodeAddress::Thread { .. } => Self::Exit,
                _ => Self::Resolution,
            },
            _ => Self::Resolution,
        }
    }
}

/// The deadline the rule gives a record now, before [`extend`] keeps it from
/// moving earlier. A record still capturing whose issue has resolved gets a
/// provisional deadline from the resolution, which its exit can only extend.
pub fn deadline(
    basis: ExpiryBasis,
    exited_at: Option<i64>,
    resolved_at: Option<i64>,
) -> Option<i64> {
    match basis {
        ExpiryBasis::Exit => exited_at.map(|at| at + RETENTION_SECS),
        ExpiryBasis::Resolution => {
            resolved_at.map(|resolved| resolved.max(exited_at.unwrap_or(resolved)) + RETENTION_SECS)
        }
    }
}

/// A promised deadline never moves earlier.
pub fn extend(promised: Option<i64>, computed: Option<i64>) -> Option<i64> {
    match (promised, computed) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// One retained capture's record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRecord {
    pub id: String,
    pub terminal_id: Option<String>,
    pub job_id: Option<String>,
    pub project_id: String,
    pub slug: String,
    pub terminal_uri: String,
    pub basis: ExpiryBasis,
    /// `capturing`, `complete`, or `incomplete`.
    pub capture: String,
    pub capture_note: Option<String>,
    pub dropped_bytes: i64,
    pub bytes: i64,
    pub exit_code: Option<i32>,
    pub exit_reason: Option<String>,
    pub exited_at: Option<i64>,
    pub resolved_at: Option<i64>,
    pub expires_at: Option<i64>,
    pub created_at: i64,
}

const RECORD_COLUMNS: &str =
    "id, terminal_id, job_id, project_id, slug, terminal_uri, expiry_basis, \
     capture, capture_note, dropped_bytes, bytes, exit_code, exit_reason, exited_at, resolved_at, \
     expires_at, created_at";

fn record_from_row(row: &cairn_db::turso::Row) -> DbResult<OutputRecord> {
    Ok(OutputRecord {
        id: row.text(0)?,
        terminal_id: row.opt_text(1)?,
        job_id: row.opt_text(2)?,
        project_id: row.text(3)?,
        slug: row.text(4)?,
        terminal_uri: row.text(5)?,
        basis: ExpiryBasis::parse(&row.text(6)?),
        capture: row.text(7)?,
        capture_note: row.opt_text(8)?,
        dropped_bytes: row.i64(9)?,
        bytes: row.i64(10)?,
        exit_code: row.opt_i64(11)?.map(|code| code as i32),
        exit_reason: row.opt_text(12)?,
        exited_at: row.opt_i64(13)?,
        resolved_at: row.opt_i64(14)?,
        expires_at: row.opt_i64(15)?,
        created_at: row.i64(16)?,
    })
}

impl OutputRecord {
    pub fn retained_uri(&self) -> String {
        retained_uri(&self.terminal_uri, &self.id)
    }

    pub fn is_capturing(&self) -> bool {
        self.capture == "capturing"
    }

    /// When the record is kept until, or what will set that.
    pub fn retention_clause(&self) -> String {
        match (self.expires_at, self.basis) {
            (Some(at), _) => format!("kept until {}", describe_deadline(at)),
            (None, ExpiryBasis::Resolution) => {
                "kept for 7 days after its issue resolves".to_string()
            }
            (None, ExpiryBasis::Exit) => "kept for 7 days after it exits".to_string(),
        }
    }

    /// What a reader must know about the capture's completeness, or `None`
    /// when it is complete and nothing was dropped.
    pub fn disclosure(&self) -> Option<String> {
        let mut parts = Vec::new();
        if self.dropped_bytes > 0 {
            parts.push(format!(
                "older output was dropped ({} bytes) to keep the capture under 24 MiB",
                self.dropped_bytes
            ));
        }
        if self.capture == "incomplete" {
            parts.push(match self.capture_note.as_deref() {
                Some(note) => format!("capture is incomplete: {note}"),
                None => "capture is incomplete".to_string(),
            });
        } else if self.capture == "unreported" {
            parts.push(UNREPORTED_NOTE.to_string());
        } else if let Some(note) = self.capture_note.as_deref() {
            parts.push(note.to_string());
        }
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

fn describe_deadline(unix_secs: i64) -> String {
    chrono::DateTime::from_timestamp(unix_secs, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| format!("unix {unix_secs}"))
}

/// What the exit notification says about a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedNotice {
    pub uri: String,
    pub retention: String,
    pub disclosure: Option<String>,
}

impl From<&OutputRecord> for RetainedNotice {
    fn from(record: &OutputRecord) -> Self {
        Self {
            uri: record.retained_uri(),
            retention: record.retention_clause(),
            disclosure: record.disclosure(),
        }
    }
}

/// The record `output_id` names.
pub async fn load(db: &LocalDb, output_id: &str) -> DbResult<Option<OutputRecord>> {
    db.query_opt(
        format!("SELECT {RECORD_COLUMNS} FROM terminal_outputs WHERE id = ?1"),
        params![output_id.to_string()],
        record_from_row,
    )
    .await
}

/// The record the terminal row `terminal_id` captures into.
pub async fn load_for_terminal(db: &LocalDb, terminal_id: &str) -> DbResult<Option<OutputRecord>> {
    db.query_opt(
        format!("SELECT {RECORD_COLUMNS} FROM terminal_outputs WHERE terminal_id = ?1 LIMIT 1"),
        params![terminal_id.to_string()],
        record_from_row,
    )
    .await
}

/// The record the terminal process `session_id` captures into.
pub async fn load_for_session(db: &LocalDb, session_id: &str) -> DbResult<Option<OutputRecord>> {
    let id = db
        .query_opt(
            "SELECT o.id FROM job_terminals t JOIN terminal_outputs o ON o.terminal_id = t.id
              WHERE t.session_id = ?1 LIMIT 1",
            params![session_id.to_string()],
            |row| row.text(0),
        )
        .await?;
    match id {
        Some(id) => load(db, &id).await,
        None => Ok(None),
    }
}

/// The newest record created under `terminal_uri`: what a subscriber of a
/// terminal whose row is already gone is owed.
pub async fn load_newest_for_uri(
    db: &LocalDb,
    terminal_uri: &str,
) -> DbResult<Option<OutputRecord>> {
    db.query_opt(
        format!(
            "SELECT {RECORD_COLUMNS} FROM terminal_outputs WHERE terminal_uri = ?1
              ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ),
        params![terminal_uri.to_string()],
        record_from_row,
    )
    .await
}

/// What a new terminal row captures into.
#[derive(Debug, Clone)]
pub struct NewCapture {
    pub terminal_uri: String,
    pub slug: String,
    pub job_id: Option<String>,
    pub project_id: String,
    pub basis: ExpiryBasis,
}

/// Why a recovery restart's capture may have a gap.
pub const RESTART_GAP_NOTE: &str =
    "Cairn restarted this terminal after losing its process, so output from the lost process's last moments may be missing";
/// Why a re-adopted terminal's capture may have a gap.
pub const READOPT_GAP_NOTE: &str =
    "Cairn re-attached to this terminal after a restart, so output delivered while it was away may be missing";

/// Attach a capture to the terminal row `terminal_id` inside the transaction
/// that inserts it, and return the record's id.
///
/// A recovery restart (`replaces`) carries the replaced row's record across,
/// noting the possible gap; any other create mints a fresh record, so the
/// previous same-slug terminal's record is left exactly as it was.
pub(crate) async fn attach_in(
    conn: &cairn_db::storage::TrackedConnection,
    terminal_id: &str,
    replaces: Option<&str>,
    capture: &NewCapture,
    now: i64,
) -> DbResult<String> {
    if let Some(replaces) = replaces {
        let mut rows = conn
            .query(
                "SELECT id FROM terminal_outputs WHERE terminal_id = ?1 LIMIT 1",
                params![replaces],
            )
            .await?;
        let carried = rows.next().await?.map(|row| row.text(0)).transpose()?;
        drop(rows);
        if let Some(id) = carried {
            conn.execute(
                "UPDATE terminal_outputs
                    SET terminal_id = ?2, capture = 'capturing',
                        capture_note = COALESCE(capture_note, ?3), updated_at = ?4
                  WHERE id = ?1",
                params![id.as_str(), terminal_id, RESTART_GAP_NOTE, now],
            )
            .await?;
            return Ok(id);
        }
    }
    let id = mint_output_id();
    conn.execute(
        "INSERT INTO terminal_outputs (
             id, terminal_id, job_id, project_id, slug, terminal_uri, expiry_basis,
             capture, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'capturing', ?8, ?8)",
        params![
            id.as_str(),
            terminal_id,
            capture.job_id.as_deref(),
            capture.project_id.as_str(),
            capture.slug.as_str(),
            capture.terminal_uri.as_str(),
            capture.basis.as_str(),
            now
        ],
    )
    .await?;
    Ok(id)
}

/// [`attach_in`] in its own transaction, for a terminal row that exists
/// without a record: one re-adopted from before retained capture.
pub async fn attach(db: &LocalDb, terminal_id: &str, capture: &NewCapture) -> DbResult<String> {
    let terminal_id = terminal_id.to_string();
    let capture = capture.clone();
    db.write(move |conn| {
        let terminal_id = terminal_id.clone();
        let capture = capture.clone();
        Box::pin(async move {
            attach_in(
                conn,
                &terminal_id,
                None,
                &capture,
                chrono::Utc::now().timestamp(),
            )
            .await
        })
    })
    .await
}

/// Why an adopted scratch log may be partial.
const LEGACY_NOTE: &str =
    "this capture began in the terminal's scratch log before retained output existed, and may be partial";

/// Move the scratch log a terminal wrote before retained capture into its new
/// record, when that log is attributable: the same job and slug, adopted only
/// for the same terminal continuing (a recovery restart or a re-adoption),
/// never for a new same-slug terminal, and only into a record with no capture
/// yet. The record notes that the adopted capture may be partial.
pub async fn adopt_legacy_capture(db: &LocalDb, job_id: &str, slug: &str, output_id: &str) {
    let legacy = crate::scratch::legacy_terminal_log_path(job_id, slug);
    let target = output_log_path(output_id);
    if target.exists() || !legacy.exists() {
        return;
    }
    if let Some(parent) = target.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(error) = std::fs::rename(&legacy, &target) {
        log::warn!(
            "terminal output {output_id}: could not adopt {}: {error}",
            legacy.display()
        );
        return;
    }
    if let Err(error) = note_gap(db, output_id, LEGACY_NOTE).await {
        log::warn!("terminal output {output_id}: could not note its adopted capture: {error}");
    }
}

/// Note a gap in a capture that keeps running, such as a re-adoption after a
/// runner restart.
pub async fn note_gap(db: &LocalDb, output_id: &str, note: &str) -> DbResult<()> {
    db.execute(
        "UPDATE terminal_outputs SET capture_note = COALESCE(capture_note, ?2), updated_at = ?3
          WHERE id = ?1",
        params![output_id, note, chrono::Utc::now().timestamp()],
    )
    .await?;
    Ok(())
}

/// Delete a record and its file: a start that failed before anything ran.
pub async fn discard(db: &LocalDb, output_id: &str) -> DbResult<()> {
    remove_capture_file(output_id);
    db.execute(
        "DELETE FROM terminal_outputs WHERE id = ?1",
        params![output_id],
    )
    .await?;
    Ok(())
}

/// How a capture ended.
#[derive(Debug, Clone, Default)]
pub struct CaptureEnd {
    pub exit_code: Option<i32>,
    pub exit_reason: Option<String>,
    /// What the live capture stream reports, or `None` when no live stream was
    /// attached when the terminal settled, which makes the capture incomplete.
    pub summary: Option<CaptureSummary>,
}

/// The note a capture that ended without its live stream carries.
const NO_LIVE_CAPTURE_NOTE: &str =
    "the capture stream was not attached when the terminal settled, so its final output may be missing";

/// Finalize a capture once its terminal has ended: persist completeness and
/// truncation, record the exit and set the deadline. Exactly one call makes the
/// transition; it returns the finalized record, and every later call (or a
/// call for a record already finalized) returns `None`.
pub async fn finalize(
    db: &LocalDb,
    output_id: &str,
    end: CaptureEnd,
    now: i64,
) -> DbResult<Option<OutputRecord>> {
    let output_id = output_id.to_string();
    db.write(move |conn| {
        let output_id = output_id.clone();
        let end = end.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    format!(
                        "SELECT {RECORD_COLUMNS} FROM terminal_outputs
                          WHERE id = ?1 AND capture = 'capturing'"
                    ),
                    params![output_id.as_str()],
                )
                .await?;
            let Some(mut record) = rows
                .next()
                .await?
                .map(|row| record_from_row(&row))
                .transpose()?
            else {
                return Ok(None);
            };
            drop(rows);
            let failure = match &end.summary {
                Some(summary) => summary.failure.clone(),
                None => Some(NO_LIVE_CAPTURE_NOTE.to_string()),
            };
            if let Some(summary) = &end.summary {
                record.dropped_bytes = record.dropped_bytes.max(summary.dropped_bytes as i64);
                record.bytes = summary.bytes as i64;
            }
            record.capture_note = match (record.capture_note.take(), failure.as_ref()) {
                (Some(note), Some(failure)) => Some(format!("{note}; {failure}")),
                (note, failure) => note.or_else(|| failure.cloned()),
            };
            let unreported = end
                .summary
                .as_ref()
                .is_some_and(|summary| summary.unreported);
            record.capture = if record.capture_note.is_some() {
                "incomplete".to_string()
            } else if unreported {
                "unreported".to_string()
            } else {
                "complete".to_string()
            };
            record.exit_code = end.exit_code;
            record.exit_reason = end.exit_reason.clone();
            record.exited_at = Some(now);
            record.expires_at = extend(
                record.expires_at,
                deadline(record.basis, record.exited_at, record.resolved_at),
            );
            conn.execute(
                "UPDATE terminal_outputs
                    SET capture = ?2, capture_note = ?3, dropped_bytes = ?4, bytes = ?5,
                        exit_code = ?6, exit_reason = ?7, exited_at = ?8, expires_at = ?9,
                        updated_at = ?8
                  WHERE id = ?1",
                params![
                    record.id.as_str(),
                    record.capture.as_str(),
                    record.capture_note.as_deref(),
                    record.dropped_bytes,
                    record.bytes,
                    record.exit_code.map(i64::from),
                    record.exit_reason.as_deref(),
                    now,
                    record.expires_at
                ],
            )
            .await?;
            Ok(Some(record))
        })
    })
    .await
}

/// Stamp the resolution of the issue that owns `job_ids` on their records and
/// move each deadline to seven days past the later of exit and resolution,
/// never earlier than it already was. Idempotent: a repeated teardown can only
/// extend.
pub async fn record_resolution(
    db: &LocalDb,
    job_ids: &[String],
    resolved_at: i64,
) -> DbResult<usize> {
    let mut updated = 0;
    for job_id in job_ids {
        let job_id = job_id.clone();
        updated += db
            .write(move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            format!(
                                "SELECT {RECORD_COLUMNS} FROM terminal_outputs
                                  WHERE job_id = ?1 AND expiry_basis = 'resolution'"
                            ),
                            params![job_id.as_str()],
                        )
                        .await?;
                    let mut records = Vec::new();
                    while let Some(row) = rows.next().await? {
                        records.push(record_from_row(&row)?);
                    }
                    drop(rows);
                    let mut updated = 0;
                    for record in records {
                        let resolved = extend(record.resolved_at, Some(resolved_at));
                        let expires = extend(
                            record.expires_at,
                            deadline(record.basis, record.exited_at, resolved),
                        );
                        if resolved == record.resolved_at && expires == record.expires_at {
                            continue;
                        }
                        conn.execute(
                            "UPDATE terminal_outputs SET resolved_at = ?2, expires_at = ?3,
                                    updated_at = ?4
                              WHERE id = ?1",
                            params![record.id.as_str(), resolved, expires, resolved_at],
                        )
                        .await?;
                        updated += 1;
                    }
                    Ok(updated)
                })
            })
            .await?;
    }
    Ok(updated)
}

/// Expire every record of `job_ids` now: their issue is being deleted.
pub async fn expire_for_jobs(db: &LocalDb, job_ids: &[String], now: i64) -> DbResult<()> {
    for job_id in job_ids {
        db.execute(
            "UPDATE terminal_outputs SET expires_at = ?2, updated_at = ?2 WHERE job_id = ?1",
            params![job_id.as_str(), now],
        )
        .await?;
    }
    Ok(())
}

/// Expire every record of `project_id` now: the project is being deleted.
pub async fn expire_for_project(db: &LocalDb, project_id: &str, now: i64) -> DbResult<()> {
    db.execute(
        "UPDATE terminal_outputs SET expires_at = ?2, updated_at = ?2 WHERE project_id = ?1",
        params![project_id, now],
    )
    .await?;
    Ok(())
}

pub(crate) fn remove_capture_file(output_id: &str) -> bool {
    match std::fs::remove_file(output_log_path(output_id)) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            log::warn!("terminal output {output_id}: could not remove its capture: {error}");
            false
        }
    }
}

/// What one [`prune`] pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PruneReport {
    pub pages: usize,
    pub deleted: usize,
    pub bytes: u64,
    /// Expired records kept because their terminal is still capturing.
    pub live: usize,
    /// Expired records whose file could not be removed, retried next pass.
    pub failed: usize,
}

/// Delete expired records, at most [`PRUNE_PAGES`] pages of
/// [`PRUNE_PAGE_ROWS`].
///
/// The walk is keyed on `(expires_at, id)`, so a page of records it keeps
/// (still capturing for a live terminal row, or a file it could not remove)
/// never stalls it. Each record's file goes before its row: a pass interrupted
/// between the two leaves a row whose file is already gone, which the next
/// pass deletes.
pub async fn prune(db: &LocalDb, now: i64) -> DbResult<PruneReport> {
    let mut report = PruneReport::default();
    let mut after: (i64, String) = (i64::MIN, String::new());
    for _ in 0..PRUNE_PAGES {
        let (after_at, after_id) = after.clone();
        let page: Vec<(i64, String, i64, bool)> = db
            .query_all(
                "SELECT o.expires_at, o.id, o.bytes,
                        o.capture = 'capturing' AND EXISTS (
                            SELECT 1 FROM job_terminals t WHERE t.id = o.terminal_id
                        )
                   FROM terminal_outputs o
                  WHERE o.expires_at IS NOT NULL AND o.expires_at <= ?1
                    AND (o.expires_at > ?2 OR (o.expires_at = ?2 AND o.id > ?3))
                  ORDER BY o.expires_at, o.id
                  LIMIT ?4",
                params![now, after_at, after_id, PRUNE_PAGE_ROWS],
                |row| Ok((row.i64(0)?, row.text(1)?, row.i64(2)?, row.i64(3)? != 0)),
            )
            .await?;
        let Some(last) = page.last() else {
            break;
        };
        after = (last.0, last.1.clone());
        let full = page.len() as i64 == PRUNE_PAGE_ROWS;
        report.pages += 1;
        let mut deletable = Vec::new();
        for (_, id, bytes, live) in page {
            if live {
                report.live += 1;
            } else if remove_capture_file(&id) {
                report.bytes += bytes.max(0) as u64;
                deletable.push(id);
            } else {
                report.failed += 1;
            }
        }
        if !deletable.is_empty() {
            let count = deletable.len();
            db.write(move |conn| {
                let deletable = deletable.clone();
                Box::pin(async move {
                    for id in &deletable {
                        conn.execute(
                            "DELETE FROM terminal_outputs WHERE id = ?1",
                            params![id.as_str()],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await?;
            report.deleted += count;
        }
        if !full {
            break;
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_output_id_cannot_name_a_path_outside_the_root() {
        for id in ["../etc/passwd", "a/b", "", ".."] {
            assert_eq!(output_log_path(id), output_root().join("invalid.log"));
        }
        let id = mint_output_id();
        assert_eq!(
            output_log_path(&id),
            output_root().join(format!("{id}.log"))
        );
    }

    #[test]
    fn the_deadline_is_seven_days_past_the_later_of_exit_and_resolution() {
        let day = 86_400;
        let r = ExpiryBasis::Resolution;
        // Unresolved: no deadline yet, whatever the exit.
        assert_eq!(deadline(r, Some(10), None), None);
        // Exit before resolution, and after.
        assert_eq!(deadline(r, Some(10), Some(100)), Some(100 + 7 * day));
        assert_eq!(deadline(r, Some(500), Some(100)), Some(500 + 7 * day));
        // Still capturing at resolution: provisional from the resolution.
        assert_eq!(deadline(r, None, Some(100)), Some(100 + 7 * day));
        // Project and thread terminals: exit alone.
        assert_eq!(
            deadline(ExpiryBasis::Exit, Some(10), Some(900)),
            Some(10 + 7 * day)
        );
        assert_eq!(deadline(ExpiryBasis::Exit, None, None), None);
    }

    #[test]
    fn the_stream_ledger_vouches_only_for_an_ended_unbroken_stream() {
        let short = |reason: &str| StreamVerdict::Short(reason.to_string());
        let mut whole = StreamLedger::from_first_chunk();
        assert!(whole.accept(1) && whole.accept(2));
        assert!(!whole.accept(2) && !whole.accept(1), "repeats are refused");
        assert_eq!(
            whole.verdict(true, false),
            short(UNCONFIRMED_END_NOTE),
            "no end from an executor that reports one, no claim"
        );
        assert_eq!(
            whole.verdict(false, false),
            StreamVerdict::Unreported,
            "no end from an executor that never reports one proves nothing"
        );
        whole.end(2, StreamEnd::Closed);
        whole.end(9, StreamEnd::StillOpen);
        assert_eq!(
            whole.verdict(true, false),
            StreamVerdict::Whole,
            "the first end counts"
        );
        assert_eq!(whole.verdict(true, true), short(EXECUTOR_LOST_NOTE));

        let mut open = StreamLedger::from_first_chunk();
        open.accept(1);
        open.end(1, StreamEnd::StillOpen);
        assert_eq!(open.verdict(true, false), short(STILL_OPEN_NOTE));

        let mut gapped = StreamLedger::from_first_chunk();
        assert!(gapped.accept(2), "chunk 1 never arrived");
        assert_eq!(
            gapped.verdict(false, false),
            short("1 output chunk from the executor never arrived"),
            "a gap is lost output from any executor"
        );
        gapped.end(4, StreamEnd::Closed);
        assert_eq!(
            gapped.verdict(true, false),
            short("3 output chunks from the executor never arrived")
        );

        let mut silent = StreamLedger::from_first_chunk();
        silent.end(0, StreamEnd::Closed);
        assert_eq!(
            silent.verdict(true, false),
            StreamVerdict::Whole,
            "a process that wrote nothing"
        );

        let mut joined = StreamLedger::mid_stream();
        assert!(joined.accept(40), "a re-adoption starts wherever it joins");
        assert!(joined.accept(41));
        joined.end(41, StreamEnd::Closed);
        assert_eq!(joined.verdict(true, false), StreamVerdict::Whole);
    }

    #[test]
    fn a_promised_deadline_never_moves_earlier() {
        assert_eq!(extend(Some(50), Some(40)), Some(50));
        assert_eq!(extend(Some(50), Some(60)), Some(60));
        assert_eq!(extend(Some(50), None), Some(50));
        assert_eq!(extend(None, Some(40)), Some(40));
        assert_eq!(extend(None, None), None);
    }

    #[test]
    fn thread_and_project_terminals_expire_from_exit() {
        use cairn_uri::CairnResource;
        let project = CairnResource::ProjectTerminal {
            project: "p".into(),
            slug: "dev".into(),
        };
        assert_eq!(ExpiryBasis::for_resource(&project), ExpiryBasis::Exit);
        let node = cairn_uri::parse_uri("cairn://p/p/12/1/builder/terminal/dev").unwrap();
        assert_eq!(ExpiryBasis::for_resource(&node), ExpiryBasis::Resolution);
        let thread = cairn_uri::delegate_thread_descendant(
            cairn_uri::parse_uri("cairn://p/p/thread-ux/terminal/dev").unwrap(),
        )
        .unwrap();
        assert_eq!(ExpiryBasis::for_resource(&thread), ExpiryBasis::Exit);
    }

    fn capture(slug: &str, job_id: Option<&str>, basis: ExpiryBasis) -> NewCapture {
        NewCapture {
            terminal_uri: format!("cairn://p/k/terminal/{slug}"),
            slug: slug.to_string(),
            job_id: job_id.map(str::to_string),
            project_id: "proj".to_string(),
            basis,
        }
    }

    fn ended(exit_code: i32) -> CaptureEnd {
        CaptureEnd {
            exit_code: Some(exit_code),
            exit_reason: None,
            summary: Some(CaptureSummary::default()),
        }
    }

    /// Both orders of exit and resolution, a capture still running when its
    /// issue resolves, and a repeated teardown with an earlier stamp: the
    /// deadline is seven days past the later of the two and never moves back.
    #[tokio::test]
    async fn resolution_and_exit_extend_a_deadline_and_never_shorten_it() {
        let db = crate::storage::migrated_test_db("terminal_output_deadline.db").await;
        let job = ["job-a".to_string()];

        let exited_first = attach(
            &db,
            "t1",
            &capture("a", Some("job-a"), ExpiryBasis::Resolution),
        )
        .await
        .unwrap();
        let record = finalize(&db, &exited_first, ended(1), 1_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.expires_at, None, "unresolved: the deadline is open");
        assert_eq!(
            record.retention_clause(),
            "kept for 7 days after its issue resolves"
        );
        assert!(
            finalize(&db, &exited_first, ended(2), 1_001)
                .await
                .unwrap()
                .is_none(),
            "a capture finalizes once"
        );

        let still_running = attach(
            &db,
            "t2",
            &capture("b", Some("job-a"), ExpiryBasis::Resolution),
        )
        .await
        .unwrap();
        record_resolution(&db, &job, 5_000).await.unwrap();
        let a = load(&db, &exited_first).await.unwrap().unwrap();
        assert_eq!(a.expires_at, Some(5_000 + RETENTION_SECS));
        let b = load(&db, &still_running).await.unwrap().unwrap();
        assert_eq!(
            b.expires_at,
            Some(5_000 + RETENTION_SECS),
            "provisional, from the resolution"
        );

        // A retried teardown stamping an earlier time changes nothing.
        record_resolution(&db, &job, 3_000).await.unwrap();
        assert_eq!(
            load(&db, &exited_first).await.unwrap().unwrap().expires_at,
            Some(5_000 + RETENTION_SECS)
        );

        // Exiting after the resolution moves the deadline to the exit.
        let b = finalize(&db, &still_running, ended(0), 9_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.expires_at, Some(9_000 + RETENTION_SECS));

        // A project or thread terminal: seven days after exit, whatever else.
        let project = attach(&db, "t3", &capture("c", None, ExpiryBasis::Exit))
            .await
            .unwrap();
        let c = finalize(&db, &project, ended(0), 700)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c.expires_at, Some(700 + RETENTION_SECS));
    }

    /// A capture that ended without its live stream says so, and a capture
    /// whose compaction dropped bytes discloses them.
    #[tokio::test]
    async fn an_incomplete_or_truncated_capture_says_so() {
        let db = crate::storage::migrated_test_db("terminal_output_disclose.db").await;
        let lost = attach(&db, "t1", &capture("a", None, ExpiryBasis::Exit))
            .await
            .unwrap();
        let end = CaptureEnd {
            exit_code: None,
            exit_reason: Some("process lost".into()),
            summary: None,
        };
        let record = finalize(&db, &lost, end, 10).await.unwrap().unwrap();
        assert_eq!(record.capture, "incomplete");
        assert!(
            record
                .disclosure()
                .unwrap()
                .starts_with("capture is incomplete: the capture stream was not attached"),
            "{record:?}"
        );

        let truncated = attach(&db, "t2", &capture("b", None, ExpiryBasis::Exit))
            .await
            .unwrap();
        let end = CaptureEnd {
            exit_code: Some(0),
            exit_reason: None,
            summary: Some(CaptureSummary {
                dropped_bytes: 4096,
                bytes: 10,
                failure: None,
                unreported: false,
            }),
        };
        let record = finalize(&db, &truncated, end, 10).await.unwrap().unwrap();
        assert_eq!(record.capture, "complete");
        assert!(
            record
                .disclosure()
                .unwrap()
                .contains("older output was dropped (4096 bytes)"),
            "{record:?}"
        );
        let notice = RetainedNotice::from(&record);
        assert_eq!(
            notice.uri,
            format!("cairn://p/k/terminal/b?output={truncated}")
        );
        assert_eq!(notice.retention, "kept until 1970-01-08 00:00 UTC");
    }

    /// Prune deletes exactly the expired, finalized captures, file and row,
    /// across more than one page; keeps one still capturing for a live terminal
    /// row whatever its deadline; and keeps everything one second early.
    #[tokio::test]
    async fn prune_deletes_expired_captures_across_pages_and_keeps_a_live_one() {
        let db = crate::storage::migrated_test_db("terminal_output_prune.db").await;
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('ws', 'WS', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj', 'ws', 'K', 'k', '/tmp/k', 1, 1);
             INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug)
               VALUES ('live-row', NULL, 'proj', NULL, 'sess-live', 'x', 'running', 1, 'live');",
        )
        .await
        .unwrap();

        let mut expired = Vec::new();
        for index in 0..(PRUNE_PAGE_ROWS + 6) {
            let slug = format!("gone-{index}");
            let id = attach(&db, &slug, &capture(&slug, None, ExpiryBasis::Exit))
                .await
                .unwrap();
            TerminalLog::open(&id).append(b"bytes\n");
            finalize(&db, &id, ended(0), 100).await.unwrap().unwrap();
            expired.push(id);
        }
        let live = attach(&db, "live-row", &capture("live", None, ExpiryBasis::Exit))
            .await
            .unwrap();
        db.execute(
            "UPDATE terminal_outputs SET expires_at = 50 WHERE id = ?1",
            params![live.as_str()],
        )
        .await
        .unwrap();
        let fresh = attach(&db, "fresh", &capture("fresh", None, ExpiryBasis::Exit))
            .await
            .unwrap();
        finalize(&db, &fresh, ended(0), 10_000)
            .await
            .unwrap()
            .unwrap();

        let deadline = 100 + RETENTION_SECS;
        let early = prune(&db, deadline - 1).await.unwrap();
        assert_eq!((early.deleted, early.live), (0, 1), "{early:?}");

        let report = prune(&db, deadline).await.unwrap();
        assert_eq!(report.deleted, expired.len(), "{report:?}");
        assert_eq!(report.live, 1, "{report:?}");
        assert!(report.pages >= 2, "{report:?}");
        for id in &expired {
            assert!(load(&db, id).await.unwrap().is_none());
            assert!(!output_log_path(id).exists());
        }
        assert!(load(&db, &live).await.unwrap().is_some());
        assert!(load(&db, &fresh).await.unwrap().is_some());

        // An interrupted pass that removed a file but not its row is finished
        // by the next one.
        db.execute(
            "UPDATE terminal_outputs SET expires_at = 1 WHERE id = ?1",
            params![fresh.as_str()],
        )
        .await
        .unwrap();
        remove_capture_file(&fresh);
        let resumed = prune(&db, deadline).await.unwrap();
        assert_eq!(resumed.deleted, 1, "{resumed:?}");
        assert!(load(&db, &fresh).await.unwrap().is_none());
    }

    #[test]
    fn a_capture_persists_its_history_and_separates_processes() {
        let id = mint_output_id();
        {
            let mut log = TerminalLog::open(&id);
            log.append(b"first process output\n");
        }
        {
            let mut log = TerminalLog::open(&id);
            log.append(b"second process output\n");
            assert_eq!(log.summary().failure, None);
        }
        let content = String::from_utf8(read_capture(&id).unwrap()).unwrap();
        assert!(content.contains("first process output"));
        assert!(content.contains("second process output"));
        assert!(content.contains("session restarted"));
        assert!(output_log_path(&id).starts_with(output_root()));
        assert!(!output_log_path(&id).starts_with(cairn_common::scratch::scratch_root()));
    }

    #[test]
    fn compaction_keeps_the_tail_and_states_what_it_dropped_across_reopens() {
        let id = mint_output_id();
        let mut log = TerminalLog::open(&id);
        log.append(b"EARLY_SENTINEL\n");
        let chunk = vec![b'x'; 1024 * 1024];
        for _ in 0..30 {
            log.append(&chunk);
        }
        log.append(b"FINAL_SENTINEL\n");
        let summary = log.summary();
        assert!(summary.dropped_bytes > 0, "{summary:?}");
        let raw = read_capture(&id).unwrap();
        assert!(raw.len() as u64 <= MAX_BYTES);
        let text = String::from_utf8_lossy(&raw);
        assert!(text.starts_with(COMPACTED_PREFIX), "{}", &text[..80]);
        assert!(text.contains("FINAL_SENTINEL"));
        assert!(!text.contains("EARLY_SENTINEL"));
        drop(log);
        // A reopened capture resumes the count from the file itself.
        let reopened = TerminalLog::open(&id);
        assert_eq!(reopened.summary().dropped_bytes, summary.dropped_bytes);
    }
}
