//! The plan audit: every statement's bytecode, read once, for the ways it walks
//! a row carrying a large blob.
//!
//! Four statements found one profile at a time on 11-02 shared one shape: a
//! predicate or projection on a column stored after a large blob, planned as a
//! walk over a table whose rows overflow, so every row evaluation read that
//! row's overflow chain (cairn/5866, 5894, 5864, 5876). This module names the
//! shape from the plan instead of from a profile.
//!
//! [`classify`] reads a statement's plain `EXPLAIN` listing, trigger and
//! foreign-key subprograms included, against [`WIDE_ROW_TABLES`], and returns
//! one [`Finding`] per flagged shape under three rules: `wide-scan` and
//! `trigger-scan`, which gate, and `range-past-blob`, which is report-only.
//! [`AUDITED_TRIGGERS`] declares every trigger either lineage creates, and
//! `every_trigger_body_seeks` explains a statement that fires each one.
//!
//! # The hook
//!
//! [`PlanAudit`] lives on each database's connection gate, so every
//! connection to one database shares it. `TrackedConnection`'s `query`,
//! `execute` and `prepare`, and `LocalDb`'s own statement helpers, hand it each
//! statement with its caller's `Location`. The first sighting of a
//! `(call site, SQL)` pair runs `EXPLAIN` on the caller's own connection,
//! inside its transaction, which is safe because `EXPLAIN` executes nothing.
//! The pair is never explained again. A finding is logged once, as a WARN for
//! a gating rule and an INFO for `range-past-blob`, under
//! [`LOG_TARGET`]. When [`PLAN_AUDIT_LOG_ENV`] names a file, as the Rust test
//! runner arranges for every test process, each sighting is also appended
//! there as JSON lines for the runner's suite-wide ledger. Production and the
//! suite run the same code; only the file differs.
//!
//! A database is audited only once its migration ledger has reached the head of
//! the private or team chain. A test that builds a toy `events` table without
//! the real indexes would otherwise report shapes production never plans, and
//! every production database is migrated, so the check costs it nothing.
//!
//! Exempt, explicitly: the migration runner, whose statements run against a
//! half-built schema, and the agent SQL `cairn://db` executes, which marks its
//! call with `TrackedConnection::exempt_from_plan_audit`. A statement issued
//! through `TrackedConnection`'s `Deref` (a helper typed `&turso::Connection`)
//! is not seen at all; the test sink records those call sites as `raw` so the
//! runner can rank what the audit cannot yet see.

mod classify;
mod registry;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

pub use classify::{
    classify, explain, Classified, Finding, Instr, Listing, Object, Rule, SchemaView, WideLayout,
};
pub use registry::{
    wide_row_table, AuditedTrigger, WideRowTable, AUDITED_TRIGGERS, WIDE_ROW_TABLES,
};

use crate::storage::{Caller, DbResult, TEAM_MIGRATIONS, TURSO_MIGRATIONS};
use crate::turso::{params, Connection};

/// The file a test run's sightings are appended to, one JSON object per line.
pub const PLAN_AUDIT_LOG_ENV: &str = "CAIRN_PLAN_AUDIT_LOG";

/// The `log` target every finding is reported under.
pub const LOG_TARGET: &str = "cairn_db::plan_audit";

/// How many distinct statements one call site may have explained. A
/// `format!`-built statement shares its call site across every variant it
/// renders, and eight is enough to see each shape it takes.
const MAX_VARIANTS_PER_SITE: u8 = 8;

/// How many statements one database explains at most, so the seen-set stays
/// bounded however much dynamic SQL a process issues.
const MAX_STATEMENTS: usize = 10_000;

const ELIGIBILITY_UNKNOWN: u8 = 0;
const ELIGIBLE: u8 = 1;
const INELIGIBLE: u8 = 2;

/// The SQL head a sighting carries, matching the statement labels' width.
const SQL_HEAD_CHARS: usize = 80;

/// One database's audit state: what it has already explained, whether its
/// schema is one the audit covers, and the schema view the classifier reads.
pub(crate) struct PlanAudit {
    seen: Mutex<Seen>,
    eligibility: AtomicU8,
    schema: Mutex<Option<Arc<SchemaView>>>,
}

#[derive(Default)]
struct Seen {
    /// `(call site address, SQL hash)` of every statement explained or being
    /// explained.
    statements: FastSet<(usize, u64)>,
    /// How many of those each call site has.
    per_site: FastMap<usize, u8>,
}

/// A statement seen for the first time, waiting to be explained.
pub(crate) struct Sighting {
    at: Caller,
    key: (usize, u64),
    sql: String,
}

impl std::fmt::Debug for PlanAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PlanAudit")
    }
}

impl PlanAudit {
    pub(crate) fn new() -> Self {
        Self {
            seen: Mutex::new(Seen::default()),
            eligibility: AtomicU8::new(ELIGIBILITY_UNKNOWN),
            schema: Mutex::new(None),
        }
    }

    fn seen(&self) -> MutexGuard<'_, Seen> {
        self.seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether `sql` issued at `at` is new here, and so worth one `EXPLAIN`.
    ///
    /// The common path for every statement: an atomic load, a prefix test, a
    /// word-at-a-time hash and one uncontended set lookup.
    #[inline]
    pub(crate) fn first_sighting(&self, at: Caller, sql: &str) -> Option<Sighting> {
        if self.eligibility.load(Ordering::Relaxed) == INELIGIBLE || !is_auditable(sql) {
            return None;
        }
        let site = std::ptr::from_ref(at) as usize;
        let key = (site, text_hash(sql));
        {
            let mut seen = self.seen();
            if seen.statements.contains(&key) || seen.statements.len() >= MAX_STATEMENTS {
                return None;
            }
            let variants = seen.per_site.entry(site).or_insert(0);
            if *variants >= MAX_VARIANTS_PER_SITE {
                return None;
            }
            *variants += 1;
            seen.statements.insert(key);
        }
        // Inserted before this test so the comparison runs once per statement.
        if at.file() == crate::storage::migration::RUNNER_FILE {
            return None;
        }
        Some(Sighting {
            at,
            key,
            sql: sql.to_string(),
        })
    }

    /// Drop a sighting the audit could not judge, so a later one is explained.
    fn forget(&self, sighting: &Sighting) {
        let mut seen = self.seen();
        if seen.statements.remove(&sighting.key) {
            if let Some(variants) = seen.per_site.get_mut(&sighting.key.0) {
                *variants = variants.saturating_sub(1);
            }
        }
    }

    /// The schema may have moved: a migration ran, a DDL batch committed, or a
    /// synced replica pulled. Eligibility is asked again, and the schema view
    /// reloaded, at the next sighting.
    pub(crate) fn schema_changed(&self) {
        self.eligibility
            .store(ELIGIBILITY_UNKNOWN, Ordering::Relaxed);
        *self
            .schema
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Explain `sighting` on `conn`, classify it, and report what it shows.
    /// Never fails the caller's statement: anything that goes wrong here is
    /// logged at debug and the statement runs as it would have.
    pub(crate) async fn run(&self, conn: &Connection, sighting: Sighting) {
        if !self.eligible(conn).await {
            self.forget(&sighting);
            return;
        }
        match self.explain_and_classify(conn, &sighting.sql).await {
            Ok(classified) => report(&sighting, &classified),
            Err(error) => log::debug!(
                target: LOG_TARGET,
                "plan audit: {}:{} not explained: {error}",
                sighting.at.file(),
                sighting.at.line()
            ),
        }
    }

    async fn eligible(&self, conn: &Connection) -> bool {
        match self.eligibility.load(Ordering::Relaxed) {
            ELIGIBLE => return true,
            INELIGIBLE => return false,
            _ => {}
        }
        let at_head = at_a_lineage_head(conn).await.unwrap_or(false);
        self.eligibility.store(
            if at_head { ELIGIBLE } else { INELIGIBLE },
            Ordering::Relaxed,
        );
        at_head
    }

    async fn explain_and_classify(&self, conn: &Connection, sql: &str) -> DbResult<Classified> {
        let listing = explain(conn, sql).await?;
        let classified = classify(&listing, &*self.schema_view(conn, false).await?);
        if !classified.unresolved {
            return Ok(classified);
        }
        // An object created since the view was read: read it again, once.
        Ok(classify(&listing, &*self.schema_view(conn, true).await?))
    }

    async fn schema_view(&self, conn: &Connection, refresh: bool) -> DbResult<Arc<SchemaView>> {
        let cached = self
            .schema
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(view) = cached.filter(|_| !refresh) {
            return Ok(view);
        }
        let view = Arc::new(SchemaView::load(conn, WIDE_ROW_TABLES).await?);
        *self
            .schema
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(view.clone());
        Ok(view)
    }
}

/// Whether the ledger holds the head migration of either lineage, by version
/// and name.
async fn at_a_lineage_head(conn: &Connection) -> DbResult<bool> {
    let (Some(private), Some(team)) = (TURSO_MIGRATIONS.last(), TEAM_MIGRATIONS.last()) else {
        return Ok(false);
    };
    let mut rows = conn
        .query(
            "SELECT 1 FROM cairn_schema_migrations \
             WHERE (version = ?1 AND name = ?2) OR (version = ?3 AND name = ?4) LIMIT 1",
            params![private.version, private.name, team.version, team.name],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Statements the audit explains: data reads and writes. Transaction control,
/// pragmas, `EXPLAIN` itself and DDL are not plans anyone pays for per row.
fn is_auditable(sql: &str) -> bool {
    let head = sql.trim_start().as_bytes();
    [
        b"SELECT".as_slice(),
        b"WITH",
        b"INSERT",
        b"UPDATE",
        b"DELETE",
        b"REPLACE",
    ]
    .iter()
    .any(|keyword| {
        head.len() >= keyword.len() && head[..keyword.len()].eq_ignore_ascii_case(keyword)
    })
}

/// Whether a batch may change the schema, so the audit's view of it goes stale.
pub(crate) fn is_schema_change(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    ["CREATE ", "DROP ", "ALTER "]
        .iter()
        .any(|keyword| upper.contains(keyword))
}

/// Every finding this process reported, as `file:line subject`, for the hook's
/// tests to read without capturing the log.
#[cfg(test)]
pub(crate) static REPORTED: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn report(sighting: &Sighting, classified: &Classified) {
    let (file, line) = (sighting.at.file(), sighting.at.line());
    for finding in &classified.findings {
        #[cfg(test)]
        REPORTED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(format!("{file}:{line} {}", finding.subject()));
        let level = if finding.rule.gating() {
            log::Level::Warn
        } else {
            log::Level::Info
        };
        log::log!(
            target: LOG_TARGET,
            level,
            "plan audit: {file}:{line} {}: {}",
            finding.subject(),
            finding.excerpt.replace('\n', " | ")
        );
    }
    if let Some(log) = test_log() {
        append(log, &sighting_lines(sighting, classified));
    }
}

/// The JSON lines one sighting adds to the test log: a `seen` line naming
/// every table the statement opens, which is what lets the runner call an
/// allowlist entry stale only when its statement was explained and came back
/// clean, then one `finding` line per finding.
fn sighting_lines(sighting: &Sighting, classified: &Classified) -> String {
    let (file, line) = (sighting.at.file(), sighting.at.line());
    let statement = statement_hash(&sighting.sql);
    let mut lines = serde_json::json!({
        "kind": "seen",
        "file": file,
        "line": line,
        "statement": statement,
        "tables": classified.tables,
    })
    .to_string();
    lines.push('\n');
    for finding in &classified.findings {
        lines.push_str(
            &serde_json::json!({
                "kind": "finding",
                "file": file,
                "line": line,
                "statement": statement,
                "sql_head": sql_head(&sighting.sql),
                "rule": finding.rule.as_str(),
                "gating": finding.rule.gating(),
                "table": finding.table,
                "column": finding.column,
                "excerpt": finding.excerpt,
            })
            .to_string(),
        );
        lines.push('\n');
    }
    lines
}

/// Record that a statement ran through `TrackedConnection`'s `Deref` at `at`,
/// unseen by the audit. Test runs only, once per site per process.
pub(crate) fn record_raw_site(at: Caller) {
    let Some(log) = test_log() else {
        return;
    };
    static RAW_SITES: OnceLock<Mutex<FastSet<usize>>> = OnceLock::new();
    let fresh = RAW_SITES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(std::ptr::from_ref(at) as usize);
    if fresh {
        let mut line =
            serde_json::json!({ "kind": "raw", "file": at.file(), "line": at.line() }).to_string();
        line.push('\n');
        append(log, &line);
    }
}

fn test_log() -> Option<&'static Path> {
    static LOG: OnceLock<Option<PathBuf>> = OnceLock::new();
    LOG.get_or_init(|| std::env::var_os(PLAN_AUDIT_LOG_ENV).map(PathBuf::from))
        .as_deref()
}

fn append(log: &Path, lines: &str) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
    {
        // One write_all per sighting keeps its lines together when parallel
        // tests in one process append at once, as the skip ledger does.
        let _ = file.write_all(lines.as_bytes());
    }
}

/// A line-insensitive identity for a statement: its text with whitespace
/// collapsed, hashed. What the runner keys findings on across commits.
fn statement_hash(sql: &str) -> String {
    use sha2::{Digest, Sha256};
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let digest = Sha256::digest(normalized.as_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sql_head(sql: &str) -> String {
    let collapsed = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(SQL_HEAD_CHARS).collect()
}

// --- The seen-set's hash. ---
//
// Every statement pays for this hash, so it reads the SQL a word at a time
// rather than through SipHash, and the set keys on it with the same mixer. A
// collision costs one skipped EXPLAIN of a variant, never a wrong finding.

const MIX: u64 = 0x517c_c1b7_2722_0a95;

fn mix(hash: u64, word: u64) -> u64 {
    (hash.rotate_left(5) ^ word).wrapping_mul(MIX)
}

fn text_hash(text: &str) -> u64 {
    let bytes = text.as_bytes();
    let mut hash = bytes.len() as u64;
    let mut words = bytes.chunks_exact(8);
    for word in &mut words {
        hash = mix(
            hash,
            u64::from_le_bytes(word.try_into().unwrap_or_default()),
        );
    }
    let mut tail = [0u8; 8];
    tail[..words.remainder().len()].copy_from_slice(words.remainder());
    mix(hash, u64::from_le_bytes(tail))
}

#[derive(Default)]
struct FastHasher(u64);

impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = mix(self.0, u64::from(*byte));
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = mix(self.0, value);
    }

    fn write_usize(&mut self, value: usize) {
        self.0 = mix(self.0, value as u64);
    }
}

type FastSet<K> = HashSet<K, BuildHasherDefault<FastHasher>>;
type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;
