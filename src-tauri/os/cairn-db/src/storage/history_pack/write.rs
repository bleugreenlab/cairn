//! Writing a history pack.
//!
//! Two halves. [`PackWriter`] is pure and synchronous: it takes rows section by
//! section, cuts them into frames ([`super::FRAME_ROWS`] rows or
//! [`super::FRAME_RAW_BYTES`] of JSONL, never across keys), and assembles the
//! object. [`ExecutionPacker`] reads one execution out of the database for it,
//! one keyset page at a time:
//!
//! - [`ExecutionPacker::start`] reads the manifest skeleton (jobs, runs,
//!   sessions, turns) and the live column lists, in one short read.
//! - [`ExecutionPacker::read_next`] reads the next page in its own short read
//!   and, for an `events` page, gathers what reconstruction needs (its own
//!   short read, then any store fetch). It holds nothing open when it returns.
//! - [`ExecutionPacker::absorb`] does the CPU work for that page: reconstruct,
//!   apply the quarantine gate, encode each row as the export does, compress
//!   the frames it fills. It is synchronous and touches no database, so a
//!   caller can run it on a blocking thread.
//! - [`ExecutionPacker::finish`] completes the manifest and the object.
//!
//! No statement here writes. [`pack_execution`] drives the loop with no pacing
//! between pages, for tests and tools; the maintenance pass drives it itself.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{Map, Value as Json};
use sha2::{Digest, Sha256};
use turso::Value;

use super::encode::{event_columns_object, finish_event_object, json_value};
use super::{
    hex, ColumnDecl, IndexEntry, Manifest, ManifestJob, ManifestRun, ManifestSession, ManifestTurn,
    PackError, Scope, Section, SectionEntry, FOOTER_MAGIC, FORMAT, FRAME_RAW_BYTES, FRAME_ROWS,
    MAGIC, NOT_PACKED, TOKEN_ROLLUP_NOTE,
};
use crate::models::Event;
use crate::storage::content_store::content_hash;
use crate::storage::events::columns::{event_from_row, EVENT_COLUMNS};
use crate::storage::events::reconstruct::{finish_gathered, gather, Gathered};
use crate::storage::events::{compress, RECONSTRUCT_BATCH_BYTES, RECONSTRUCT_BATCH_ROWS};
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};

/// A finished pack and what went into it.
#[derive(Debug, Clone)]
pub struct BuiltPack {
    pub bytes: Vec<u8>,
    /// The object's key: the sha256 of `bytes`.
    pub content_hash: String,
    pub manifest: Manifest,
    pub stats: PackStats,
}

/// Counts gathered while packing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackStats {
    /// Pages read, across every section.
    pub pages: u64,
    /// Rows written per section table.
    pub rows: BTreeMap<String, u64>,
    /// The summed length of the stored `data` and `data_blob` of every event
    /// read: what the execution's events hold hot.
    pub hot_event_bytes: u64,
    /// Frames written.
    pub frames: u64,
}

impl PackStats {
    pub fn event_rows(&self) -> u64 {
        self.rows.get(Section::Events.table()).copied().unwrap_or(0)
    }
}

struct OpenFrame {
    key: String,
    first_rowid: i64,
    rows: u32,
    raw: Vec<u8>,
}

struct SectionFrames {
    columns: Vec<ColumnDecl>,
    /// Compressed frames, back to back; entry offsets are relative to this.
    bytes: Vec<u8>,
    entries: Vec<IndexEntry>,
    open: Option<OpenFrame>,
    rows: u64,
    hasher: Sha256,
}

impl SectionFrames {
    fn flush(&mut self, section: Section) -> Result<(), PackError> {
        let Some(frame) = self.open.take() else {
            return Ok(());
        };
        let compressed = compress(&frame.raw).map_err(PackError::Corrupt)?;
        self.hasher.update(&frame.raw);
        self.entries.push(IndexEntry {
            section,
            key: frame.key,
            first_rowid: frame.first_rowid,
            rows: frame.rows,
            offset: self.bytes.len() as u64,
            len: compressed.len() as u64,
            raw_len: frame.raw.len() as u64,
        });
        self.bytes.extend_from_slice(&compressed);
        Ok(())
    }
}

/// Rows in, a pack object out.
pub struct PackWriter {
    sections: BTreeMap<Section, SectionFrames>,
}

impl PackWriter {
    /// A writer for the given sections, each with its columns in row order.
    pub fn new(columns: BTreeMap<Section, Vec<ColumnDecl>>) -> PackWriter {
        PackWriter {
            sections: columns
                .into_iter()
                .map(|(section, columns)| {
                    (
                        section,
                        SectionFrames {
                            columns,
                            bytes: Vec::new(),
                            entries: Vec::new(),
                            open: None,
                            rows: 0,
                            hasher: Sha256::new(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Append one row under `key`. `rowid` is added to the row as `rowid`.
    /// A serialized row, including its newline, over [`FRAME_RAW_BYTES`] is
    /// rejected without changing the writer; rows are never truncated.
    pub fn push(
        &mut self,
        section: Section,
        key: &str,
        rowid: i64,
        mut row: Map<String, Json>,
    ) -> Result<(), PackError> {
        row.insert("rowid".to_string(), Json::from(rowid));
        let mut line = serde_json::to_vec(&Json::Object(row))
            .map_err(|error| PackError::Corrupt(error.to_string()))?;
        line.push(b'\n');
        if line.len() > FRAME_RAW_BYTES {
            return Err(PackError::Corrupt(format!(
                "{} key {key:?} rowid {rowid} encodes to {} JSONL bytes, exceeds FRAME_RAW_BYTES ({FRAME_RAW_BYTES})",
                section.table(),
                line.len()
            )));
        }
        let frames = self
            .sections
            .get_mut(&section)
            .ok_or_else(|| PackError::Corrupt(format!("{section:?} was not declared")))?;
        let cut = frames.open.as_ref().is_some_and(|open| {
            open.key != key
                || open.rows as usize >= FRAME_ROWS
                || open.raw.len() + line.len() > FRAME_RAW_BYTES
        });
        if cut {
            frames.flush(section)?;
        }
        let open = frames.open.get_or_insert_with(|| OpenFrame {
            key: key.to_string(),
            first_rowid: rowid,
            rows: 0,
            raw: Vec::new(),
        });
        open.raw.extend_from_slice(&line);
        open.rows += 1;
        frames.rows += 1;
        Ok(())
    }

    /// Close every frame and assemble the object. The manifest's `sections`
    /// are filled here, from what was written.
    pub fn finish(mut self, mut manifest: Manifest) -> Result<(Vec<u8>, Manifest, u64), PackError> {
        let mut bytes = MAGIC.to_vec();
        let mut index = Vec::new();
        let mut sections = Vec::new();
        for section in Section::ALL {
            let Some(mut frames) = self.sections.remove(&section) else {
                continue;
            };
            frames.flush(section)?;
            let base = bytes.len() as u64;
            for mut entry in frames.entries {
                entry.offset += base;
                index.push(entry);
            }
            bytes.extend_from_slice(&frames.bytes);
            sections.push(SectionEntry {
                table: section.table().to_string(),
                packed: true,
                reason: section.reason().to_string(),
                rows: Some(frames.rows),
                sha256: Some(hex(&frames.hasher.finalize())),
                columns: Some(frames.columns),
            });
        }
        sections.extend(NOT_PACKED.iter().map(|(table, reason)| SectionEntry {
            table: table.to_string(),
            packed: false,
            reason: reason.to_string(),
            rows: None,
            sha256: None,
            columns: None,
        }));
        manifest.sections = sections;

        let frames = index.len() as u64;
        let manifest_json =
            serde_json::to_vec(&manifest).map_err(|error| PackError::Corrupt(error.to_string()))?;
        let index_json =
            serde_json::to_vec(&index).map_err(|error| PackError::Corrupt(error.to_string()))?;
        let manifest_frame = compress(&manifest_json).map_err(PackError::Corrupt)?;
        let index_frame = compress(&index_json).map_err(PackError::Corrupt)?;
        let manifest_off = bytes.len() as u64;
        bytes.extend_from_slice(&manifest_frame);
        let index_off = bytes.len() as u64;
        bytes.extend_from_slice(&index_frame);
        for word in [
            manifest_off,
            manifest_frame.len() as u64,
            index_off,
            index_frame.len() as u64,
        ] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(FOOTER_MAGIC);
        Ok((bytes, manifest, frames))
    }
}

/// The order the pack (and the export) select `events` columns in: the
/// canonical [`EVENT_COLUMNS`] projection first, so the prefix parses as an
/// [`Event`], then every other live column in table order.
pub fn event_column_order<'a>(table_columns: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut columns: Vec<String> = EVENT_COLUMNS
        .split(',')
        .map(|name| name.trim().to_string())
        .collect();
    for name in table_columns {
        if !columns.iter().any(|column| column == name) {
            columns.push(name.to_string());
        }
    }
    columns
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn select_list(columns: &[ColumnDecl]) -> String {
    columns
        .iter()
        .map(|column| quoted(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn table_columns(conn: &TrackedConnection, table: &str) -> DbResult<Vec<ColumnDecl>> {
    let mut rows = conn
        .query(&format!("PRAGMA table_info({table})"), ())
        .await?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().await? {
        columns.push(ColumnDecl {
            name: row.text(1)?,
            decl_type: row.opt_text(2)?.unwrap_or_default(),
        });
    }
    Ok(columns)
}

#[derive(Clone, Debug, Default)]
struct Skeleton {
    project_id: Option<String>,
    issue_id: Option<String>,
    migration_high_water: Option<String>,
    jobs: Vec<String>,
    /// `(run_id, job_id, run's own session_id)`, in job order then creation
    /// order.
    runs: Vec<(String, Option<String>, Option<String>)>,
    sessions: Vec<(String, Option<String>)>,
    turns: Vec<ManifestTurn>,
    columns: BTreeMap<Section, Vec<ColumnDecl>>,
}

async fn read_skeleton(conn: &TrackedConnection, execution_id: &str) -> DbResult<Option<Skeleton>> {
    let mut rows = conn
        .query(
            "SELECT project_id, issue_id FROM executions WHERE id = ?1",
            (execution_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let mut skeleton = Skeleton {
        project_id: row.opt_text(0)?,
        issue_id: row.opt_text(1)?,
        ..Skeleton::default()
    };
    drop(rows);

    let mut rows = conn
        .query("SELECT MAX(version) FROM cairn_schema_migrations", ())
        .await?;
    if let Some(row) = rows.next().await? {
        skeleton.migration_high_water = row.opt_text(0)?;
    }
    drop(rows);

    let mut rows = conn
        .query(
            "SELECT id FROM jobs WHERE execution_id = ?1 ORDER BY created_at ASC, rowid ASC",
            (execution_id,),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        skeleton.jobs.push(row.text(0)?);
    }
    drop(rows);

    for job in skeleton.jobs.clone() {
        let mut rows = conn
            .query(
                "SELECT id, session_id FROM runs WHERE job_id = ?1 ORDER BY created_at ASC, rowid ASC",
                (job.as_str(),),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            skeleton
                .runs
                .push((row.text(0)?, Some(job.clone()), row.opt_text(1)?));
        }
        drop(rows);
        let mut rows = conn
            .query(
                "SELECT id FROM sessions WHERE job_id = ?1 ORDER BY created_at ASC, rowid ASC",
                (job.as_str(),),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            skeleton.sessions.push((row.text(0)?, Some(job.clone())));
        }
        drop(rows);
        let mut rows = conn
            .query(
                "SELECT id, session_id, run_id, sequence FROM turns WHERE job_id = ?1 ORDER BY rowid ASC",
                (job.as_str(),),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            skeleton.turns.push(ManifestTurn {
                id: row.text(0)?,
                job_id: Some(job.clone()),
                session_id: row.opt_text(1)?,
                run_id: row.opt_text(2)?,
                sequence: row.opt_i64(3)?,
            });
        }
    }

    for section in Section::ALL {
        let mut columns = table_columns(conn, section.table()).await?;
        if section == Section::Events {
            let order = event_column_order(columns.iter().map(|column| column.name.as_str()));
            columns.sort_by_key(|column| {
                order
                    .iter()
                    .position(|name| *name == column.name)
                    .unwrap_or(usize::MAX)
            });
        }
        skeleton.columns.insert(section, columns);
    }
    Ok(Some(skeleton))
}

/// One `events` row as read: its output object (less `data`), the stored
/// event, and its rowid.
struct StoredRow {
    rowid: i64,
    object: Map<String, Json>,
    event: Event,
}

/// One page, read and gathered, waiting to be absorbed.
pub struct PackPage {
    body: PageBody,
}

enum PageBody {
    Events {
        run: String,
        rows: Vec<StoredRow>,
        gathered: Gathered,
        vibes: Vec<(i64, Map<String, Json>)>,
        read_tokens: Vec<(i64, Map<String, Json>)>,
    },
    Plain {
        section: Section,
        key: String,
        rows: Vec<(i64, Map<String, Json>)>,
    },
}

impl PackPage {
    /// Rows the page carries, across its sections.
    pub fn rows(&self) -> usize {
        match &self.body {
            PageBody::Events {
                rows,
                vibes,
                read_tokens,
                ..
            } => rows.len() + vibes.len() + read_tokens.len(),
            PageBody::Plain { rows, .. } => rows.len(),
        }
    }
}

#[derive(Clone, Debug)]
enum Step {
    Events { run: usize, after: i64 },
    ToolInvocations { run: usize, after: i64 },
    TurnEmbeddings { job: usize, offset: usize },
    Done,
}

#[derive(Default)]
struct RunTally {
    sessions: BTreeSet<String>,
    turns: BTreeSet<String>,
    rows: u64,
    max_rowid: Option<i64>,
}

/// Packs one execution, a page at a time. See the module docs.
pub struct ExecutionPacker {
    execution_id: String,
    skeleton: Skeleton,
    writer: PackWriter,
    step: Step,
    run_tally: HashMap<String, RunTally>,
    session_tally: BTreeMap<String, (u64, Option<i64>)>,
    stats: PackStats,
}

/// Why packing an execution failed.
#[derive(Debug, thiserror::Error)]
pub enum PackBuildError {
    #[error("execution {0} not found")]
    NotFound(String),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Pack(#[from] PackError),
}

impl ExecutionPacker {
    /// Read the execution's manifest skeleton and live columns.
    pub async fn start(
        db: &LocalDb,
        execution_id: &str,
    ) -> Result<ExecutionPacker, PackBuildError> {
        let id = execution_id.to_string();
        let skeleton = db
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move { read_skeleton(conn, &id).await })
            })
            .await?
            .ok_or_else(|| PackBuildError::NotFound(execution_id.to_string()))?;
        let writer = PackWriter::new(skeleton.columns.clone());
        Ok(ExecutionPacker {
            execution_id: execution_id.to_string(),
            writer,
            step: Step::Events {
                run: 0,
                after: i64::MIN,
            },
            skeleton,
            run_tally: HashMap::new(),
            session_tally: BTreeMap::new(),
            stats: PackStats::default(),
        })
    }

    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub fn stats(&self) -> &PackStats {
        &self.stats
    }

    fn columns(&self, section: Section) -> Vec<ColumnDecl> {
        self.skeleton
            .columns
            .get(&section)
            .cloned()
            .unwrap_or_default()
    }

    /// Read the next non-empty page, or `None` once every section is read.
    pub async fn read_next(&mut self, db: &LocalDb) -> DbResult<Option<PackPage>> {
        loop {
            match self.step.clone() {
                Step::Done => return Ok(None),
                Step::Events { run, after } => {
                    let Some((run_id, _, _)) = self.skeleton.runs.get(run).cloned() else {
                        self.step = Step::ToolInvocations {
                            run: 0,
                            after: i64::MIN,
                        };
                        continue;
                    };
                    let (rows, exhausted, vibes, read_tokens) = read_events_page(
                        db,
                        &run_id,
                        after,
                        self.columns(Section::Events),
                        self.columns(Section::EventVibes),
                        self.columns(Section::EventReadTokens),
                    )
                    .await?;
                    self.stats.pages += 1;
                    self.step = match (exhausted, rows.last()) {
                        (false, Some(last)) => Step::Events {
                            run,
                            after: last.rowid,
                        },
                        _ => Step::Events {
                            run: run + 1,
                            after: i64::MIN,
                        },
                    };
                    if rows.is_empty() {
                        continue;
                    }
                    let events: Vec<Event> = rows.iter().map(|row| row.event.clone()).collect();
                    let gathered = gather(db, &events).await;
                    return Ok(Some(PackPage {
                        body: PageBody::Events {
                            run: run_id,
                            rows,
                            gathered,
                            vibes,
                            read_tokens,
                        },
                    }));
                }
                Step::ToolInvocations { run, after } => {
                    let Some((run_id, _, _)) = self.skeleton.runs.get(run).cloned() else {
                        self.step = Step::TurnEmbeddings { job: 0, offset: 0 };
                        continue;
                    };
                    let columns = self.columns(Section::ToolInvocations);
                    let sql = format!(
                        "SELECT {}, rowid FROM tool_invocations WHERE run_id = ?1 AND rowid > ?2 ORDER BY rowid ASC LIMIT ?3",
                        select_list(&columns)
                    );
                    let rows = read_plain(
                        db,
                        sql,
                        vec![
                            Value::Text(run_id.clone()),
                            Value::Integer(after),
                            Value::Integer(RECONSTRUCT_BATCH_ROWS as i64),
                        ],
                        columns,
                    )
                    .await?;
                    self.stats.pages += 1;
                    self.step = match rows.last() {
                        Some((rowid, _)) if rows.len() >= RECONSTRUCT_BATCH_ROWS => {
                            Step::ToolInvocations { run, after: *rowid }
                        }
                        _ => Step::ToolInvocations {
                            run: run + 1,
                            after: i64::MIN,
                        },
                    };
                    if rows.is_empty() {
                        continue;
                    }
                    return Ok(Some(PackPage {
                        body: PageBody::Plain {
                            section: Section::ToolInvocations,
                            key: run_id,
                            rows,
                        },
                    }));
                }
                Step::TurnEmbeddings { job, offset } => {
                    let Some(job_id) = self.skeleton.jobs.get(job).cloned() else {
                        self.step = Step::Done;
                        continue;
                    };
                    let turn_ids: Vec<String> = self
                        .skeleton
                        .turns
                        .iter()
                        .filter(|turn| turn.job_id.as_deref() == Some(job_id.as_str()))
                        .map(|turn| turn.id.clone())
                        .skip(offset)
                        .take(RECONSTRUCT_BATCH_ROWS)
                        .collect();
                    if turn_ids.is_empty() {
                        self.step = Step::TurnEmbeddings {
                            job: job + 1,
                            offset: 0,
                        };
                        continue;
                    }
                    self.step = Step::TurnEmbeddings {
                        job,
                        offset: offset + turn_ids.len(),
                    };
                    let columns = self.columns(Section::TurnEmbeddings);
                    let sql = format!(
                        "SELECT {}, rowid FROM turn_embeddings WHERE turn_id IN ({})",
                        select_list(&columns),
                        placeholders(turn_ids.len())
                    );
                    let mut rows = read_plain(
                        db,
                        sql,
                        turn_ids.iter().cloned().map(Value::Text).collect(),
                        columns,
                    )
                    .await?;
                    self.stats.pages += 1;
                    order_by_owner(&mut rows, "turn_id", &turn_ids);
                    if rows.is_empty() {
                        continue;
                    }
                    return Ok(Some(PackPage {
                        body: PageBody::Plain {
                            section: Section::TurnEmbeddings,
                            key: job_id,
                            rows,
                        },
                    }));
                }
            }
        }
    }

    /// Reconstruct, gate, encode and frame one page. Synchronous and
    /// database-free.
    pub fn absorb(&mut self, page: PackPage) -> Result<(), PackError> {
        match page.body {
            PageBody::Events {
                run,
                rows,
                gathered,
                vibes,
                read_tokens,
            } => {
                let stored: Vec<Event> = rows.iter().map(|row| row.event.clone()).collect();
                let reconstructed = finish_gathered(gathered, stored);
                let event_rows = rows.len() as u64;
                let tally = self.run_tally.entry(run.clone()).or_default();
                for (row, (event, outcome)) in rows.into_iter().zip(reconstructed) {
                    self.stats.hot_event_bytes += (row.event.data.len()
                        + row.event.data_blob.as_ref().map_or(0, Vec::len))
                        as u64;
                    tally.rows += 1;
                    tally.max_rowid =
                        Some(tally.max_rowid.map_or(row.rowid, |max| max.max(row.rowid)));
                    if let Some(session) = &row.event.session_id {
                        tally.sessions.insert(session.clone());
                        let entry = self.session_tally.entry(session.clone()).or_default();
                        entry.0 += 1;
                        entry.1 = Some(entry.1.map_or(row.rowid, |max| max.max(row.rowid)));
                    }
                    if let Some(turn) = &row.event.turn_id {
                        tally.turns.insert(turn.clone());
                    }
                    let mut object = row.object;
                    finish_event_object(&mut object, &row.event, event.data, outcome);
                    self.writer.push(Section::Events, &run, row.rowid, object)?;
                }
                self.count(Section::Events, event_rows);
                let (vibe_rows, token_rows) = (vibes.len() as u64, read_tokens.len() as u64);
                for (rowid, row) in vibes {
                    self.writer.push(Section::EventVibes, &run, rowid, row)?;
                }
                for (rowid, row) in read_tokens {
                    self.writer
                        .push(Section::EventReadTokens, &run, rowid, row)?;
                }
                self.count(Section::EventVibes, vibe_rows);
                self.count(Section::EventReadTokens, token_rows);
            }
            PageBody::Plain { section, key, rows } => {
                let count = rows.len() as u64;
                for (rowid, row) in rows {
                    self.writer.push(section, &key, rowid, row)?;
                }
                self.count(section, count);
            }
        }
        Ok(())
    }

    fn count(&mut self, section: Section, rows: u64) {
        *self
            .stats
            .rows
            .entry(section.table().to_string())
            .or_default() += rows;
    }

    /// Complete the manifest and assemble the object.
    pub fn finish(self) -> Result<BuiltPack, PackError> {
        let ExecutionPacker {
            execution_id,
            skeleton,
            writer,
            mut run_tally,
            session_tally,
            mut stats,
            ..
        } = self;
        let runs = skeleton
            .runs
            .iter()
            .map(|(id, job_id, own_session)| {
                let tally = run_tally.remove(id).unwrap_or_default();
                let mut sessions = tally.sessions;
                sessions.extend(own_session.iter().cloned());
                ManifestRun {
                    id: id.clone(),
                    job_id: job_id.clone(),
                    session_ids: sessions.into_iter().collect(),
                    turn_ids: tally.turns.into_iter().collect(),
                    event_rows: tally.rows,
                    max_rowid: tally.max_rowid,
                }
            })
            .collect();
        let mut sessions: Vec<ManifestSession> = skeleton
            .sessions
            .iter()
            .map(|(id, job_id)| {
                let (rows, max_rowid) = session_tally.get(id).copied().unwrap_or_default();
                ManifestSession {
                    id: id.clone(),
                    job_id: job_id.clone(),
                    event_rows: rows,
                    max_rowid,
                }
            })
            .collect();
        for (id, (rows, max_rowid)) in &session_tally {
            if !sessions.iter().any(|session| &session.id == id) {
                sessions.push(ManifestSession {
                    id: id.clone(),
                    job_id: None,
                    event_rows: *rows,
                    max_rowid: *max_rowid,
                });
            }
        }
        let manifest = Manifest {
            format: FORMAT,
            migration_high_water: skeleton.migration_high_water.clone(),
            scope: Scope {
                kind: "execution".to_string(),
                id: execution_id.clone(),
            },
            project_id: skeleton.project_id.clone(),
            issue_id: skeleton.issue_id.clone(),
            execution_id,
            jobs: skeleton
                .jobs
                .iter()
                .map(|id| ManifestJob { id: id.clone() })
                .collect(),
            runs,
            sessions,
            turns: skeleton.turns.clone(),
            sections: Vec::new(),
            token_rollup: TOKEN_ROLLUP_NOTE.to_string(),
        };
        let (bytes, manifest, frames) = writer.finish(manifest)?;
        stats.frames = frames;
        Ok(BuiltPack {
            content_hash: content_hash(&bytes),
            bytes,
            manifest,
            stats,
        })
    }
}

/// Pack one execution with no pacing between pages.
pub async fn pack_execution(db: &LocalDb, execution_id: &str) -> Result<BuiltPack, PackBuildError> {
    let mut packer = ExecutionPacker::start(db, execution_id).await?;
    while let Some(page) = packer.read_next(db).await? {
        packer.absorb(page)?;
    }
    Ok(packer.finish()?)
}

fn placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

/// A row's columns as a JSON object.
fn row_object(row: &turso::Row, columns: &[ColumnDecl]) -> DbResult<Map<String, Json>> {
    let mut object = Map::with_capacity(columns.len() + 1);
    for (index, column) in columns.iter().enumerate() {
        object.insert(column.name.clone(), json_value(&row.get_value(index)?));
    }
    Ok(object)
}

/// Put rows selected by an owner id back in the order of `owners`.
fn order_by_owner(rows: &mut [(i64, Map<String, Json>)], column: &str, owners: &[String]) {
    let position: HashMap<&str, usize> = owners
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    rows.sort_by_key(|(rowid, row)| {
        let owner = row
            .get(column)
            .and_then(Json::as_str)
            .and_then(|id| position.get(id).copied())
            .unwrap_or(usize::MAX);
        (owner, *rowid)
    });
}

async fn read_plain(
    db: &LocalDb,
    sql: String,
    binds: Vec<Value>,
    columns: Vec<ColumnDecl>,
) -> DbResult<Vec<(i64, Map<String, Json>)>> {
    db.read(move |conn| {
        let (sql, binds, columns) = (sql.clone(), binds.clone(), columns.clone());
        Box::pin(async move {
            let mut rows = conn.query(&sql, binds).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.i64(columns.len())?, row_object(&row, &columns)?));
            }
            Ok(out)
        })
    })
    .await
}

/// One keyset page of a run's events, at most [`RECONSTRUCT_BATCH_ROWS`] rows
/// or [`RECONSTRUCT_BATCH_BYTES`] of stored body, with the page's
/// `event_vibes` and `event_read_tokens` rows, in one short read. The flag is
/// whether the run is exhausted.
async fn read_events_page(
    db: &LocalDb,
    run_id: &str,
    after: i64,
    columns: Vec<ColumnDecl>,
    vibe_columns: Vec<ColumnDecl>,
    token_columns: Vec<ColumnDecl>,
) -> DbResult<(
    Vec<StoredRow>,
    bool,
    Vec<(i64, Map<String, Json>)>,
    Vec<(i64, Map<String, Json>)>,
)> {
    let names: Vec<String> = columns.iter().map(|column| column.name.clone()).collect();
    let sql = format!(
        "SELECT {}, rowid FROM events WHERE run_id = ?1 AND rowid > ?2 ORDER BY rowid ASC LIMIT ?3",
        select_list(&columns)
    );
    let run_id = run_id.to_string();
    db.read(move |conn| {
        let (sql, run_id, names) = (sql.clone(), run_id.clone(), names.clone());
        let (vibe_columns, token_columns) = (vibe_columns.clone(), token_columns.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &sql,
                    (run_id.as_str(), after, RECONSTRUCT_BATCH_ROWS as i64),
                )
                .await?;
            let mut page = Vec::new();
            let (mut fetched, mut bytes) = (0usize, 0usize);
            let mut stopped_on_bytes = false;
            while let Some(row) = rows.next().await? {
                fetched += 1;
                let values = (0..names.len())
                    .map(|index| row.get_value(index))
                    .collect::<Result<Vec<_>, _>>()?;
                let event = event_from_row(&row)?;
                bytes += event.data.len() + event.data_blob.as_ref().map_or(0, Vec::len);
                page.push(StoredRow {
                    rowid: row.i64(names.len())?,
                    object: event_columns_object(&names, &values),
                    event,
                });
                if bytes >= RECONSTRUCT_BATCH_BYTES {
                    stopped_on_bytes = true;
                    break;
                }
            }
            drop(rows);
            let exhausted = !stopped_on_bytes && fetched < RECONSTRUCT_BATCH_ROWS;
            let ids: Vec<String> = page.iter().map(|row| row.event.id.clone()).collect();
            let mut owned = Vec::new();
            for (table, columns) in [
                ("event_vibes", &vibe_columns),
                ("event_read_tokens", &token_columns),
            ] {
                let mut found = Vec::new();
                if !ids.is_empty() {
                    let sql = format!(
                        "SELECT {}, rowid FROM {table} WHERE event_id IN ({})",
                        select_list(columns),
                        placeholders(ids.len())
                    );
                    let binds: Vec<Value> = ids.iter().cloned().map(Value::Text).collect();
                    let mut rows = conn.query(&sql, binds).await?;
                    while let Some(row) = rows.next().await? {
                        found.push((row.i64(columns.len())?, row_object(&row, columns)?));
                    }
                }
                order_by_owner(&mut found, "event_id", &ids);
                owned.push(found);
            }
            let read_tokens = owned.pop().unwrap_or_default();
            let vibes = owned.pop().unwrap_or_default();
            Ok((page, exhausted, vibes, read_tokens))
        })
    })
    .await
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    #[tokio::test]
    async fn pack_metadata_helpers_keep_statement_attribution() {
        let db = crate::storage::migrated_test_db("pack-metadata-attribution.db").await;
        let conn = db.connect().await.unwrap();
        assert!(!table_columns(&conn, "events").await.unwrap().is_empty());
        let held = db.overdue_holders(std::time::Duration::ZERO, std::time::Duration::ZERO);
        assert_eq!(held.len(), 1, "{held:?}");
        assert!(
            held[0].statement.contains("history_pack/write.rs:"),
            "{held:?}"
        );
        assert!(
            held[0].statement.contains("PRAGMA table_info(events)"),
            "{held:?}"
        );
        assert!(!held[0].statement.contains("raw connection"), "{held:?}");

        assert!(read_skeleton(&conn, "missing").await.unwrap().is_none());
        let held = db.overdue_holders(std::time::Duration::ZERO, std::time::Duration::ZERO);
        assert_eq!(held.len(), 1, "{held:?}");
        assert!(
            held[0].statement.contains("history_pack/write.rs:"),
            "{held:?}"
        );
        assert!(
            held[0]
                .statement
                .contains("SELECT project_id, issue_id FROM executions"),
            "{held:?}"
        );
        assert!(!held[0].statement.contains("raw connection"), "{held:?}");
    }
}
