//! The boot-time import that turns every thread's pre-tenet rulings into
//! migrated tenets.
//!
//! Rulings lived in two places before tenets: the `_cairn_rulings` table of a
//! thread's own database, and — for threads whose arc was never rewritten
//! after that table arrived — embedded in the `rulings` array of the arc
//! artifact. Some threads have no database file at all, so their rulings exist
//! only in the arc. The importer reads both, merges them per thread the way the
//! old lazy arc import did, and writes one transaction per project so tenet
//! numbers come out contiguous in the order the rulings were made.
//!
//! It is idempotent by content key rather than by ledger alone: each migrated
//! tenet carries `legacy_json.legacy_key = "<thread_id>:<thread-local slug>"`,
//! and a candidate whose key is already present is skipped. The ledger
//! (`tenet_ruling_imports`) short-circuits only threads whose local table is
//! already gone, which is the state a teammate's machine sees for a thread
//! that lives elsewhere.
//!
//! Thread migration 0002 drops `_cairn_rulings`. The thread store's open holds
//! it back until this import has committed the store's rows, so a failed
//! project transaction leaves every store it touched at 0001 for the next boot
//! to retry.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use cairn_common::identity::PrincipalRef;
use cairn_db::turso::params;
use serde_json::{json, Value};

use super::{emit_changed, insert_conn, AnswerSource, NewTenet, TenetOrigin, TenetStatus, Verdict};
use crate::db::DbState;
use crate::services::EventEmitter;
use crate::storage::{LocalDb, RowExt};
use crate::threads::db_store::{self, LegacyRulingRow};

/// What one boot's import did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    pub projects: Vec<ProjectImport>,
    /// Threads or projects that could not be imported this boot. Their stores
    /// keep `_cairn_rulings` and the next boot retries.
    pub errors: Vec<String>,
}

impl ImportReport {
    /// Tenets inserted across every project.
    pub fn inserted(&self) -> usize {
        self.projects.iter().map(|project| project.inserted).sum()
    }
}

/// One project's share of an import.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectImport {
    pub project_key: String,
    /// Tenets inserted this run.
    pub inserted: usize,
    /// Threads reconciled this run, with or without rulings.
    pub threads: usize,
    /// Migrated slugs renamed because another thread in the project already
    /// held them.
    pub slug_collisions: usize,
}

/// Where a candidate came from. Table rows sort before arc rows made at the
/// same moment, matching the order the old lazy import merged them in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Source {
    Table = 0,
    Arc = 1,
}

/// One ruling about to become a tenet.
#[derive(Debug, Clone)]
struct Candidate {
    thread_id: String,
    thread_name: String,
    source: Source,
    /// The row id, or the arc array index.
    id: i64,
    /// Unique within the thread.
    slug: String,
    text: String,
    status: String,
    rationale: String,
    provenance: Value,
    created_at: i64,
    updated_at: i64,
}

impl Candidate {
    fn legacy_key(&self) -> String {
        format!("{}:{}", self.thread_id, self.slug)
    }

    fn same_ruling(&self, other: &Candidate) -> bool {
        self.text == other.text
            && self.status == other.status
            && self.rationale == other.rationale
            && self.provenance == other.provenance
    }
}

struct ThreadRow {
    id: String,
    name: String,
    project_id: String,
    project_key: String,
}

/// Import every thread's rulings, in the private database and every open team
/// replica, as migrated tenets.
pub async fn import_thread_rulings(
    db: &DbState,
    emitter: &dyn EventEmitter,
    now: i64,
) -> ImportReport {
    let mut report = ImportReport::default();
    for store in db.all_dbs().await {
        import_database(&store, emitter, now, &mut report).await;
    }
    report
}

/// Import the rulings of every thread in one database. Exposed beside
/// [`import_thread_rulings`] for callers holding a bare database.
pub async fn import_database(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    report: &mut ImportReport,
) {
    let threads = match db
        .query_all(
            "SELECT t.id, t.name, t.project_id, p.key
             FROM threads t JOIN projects p ON p.id = t.project_id
             ORDER BY p.key, t.id",
            (),
            |row| {
                Ok(ThreadRow {
                    id: row.text(0)?,
                    name: row.text(1)?,
                    project_id: row.text(2)?,
                    project_key: row.text(3)?,
                })
            },
        )
        .await
    {
        Ok(threads) => threads,
        Err(error) => {
            let message = format!("ruling import could not list threads: {error}");
            tracing::error!("{message}");
            report.errors.push(message);
            return;
        }
    };
    let ledger: HashSet<String> = match db
        .query_all("SELECT thread_id FROM tenet_ruling_imports", (), |row| {
            row.text(0)
        })
        .await
    {
        Ok(rows) => rows.into_iter().collect(),
        Err(error) => {
            let message = format!("ruling import could not read its ledger: {error}");
            tracing::error!("{message}");
            report.errors.push(message);
            return;
        }
    };
    log_orphan_arcs(db).await;

    let mut by_project: Vec<(String, String, Vec<ThreadRow>)> = Vec::new();
    for thread in threads {
        match by_project.last_mut() {
            Some((project_id, _, rows)) if *project_id == thread.project_id => rows.push(thread),
            _ => by_project.push((
                thread.project_id.clone(),
                thread.project_key.clone(),
                vec![thread],
            )),
        }
    }
    for (project_id, project_key, threads) in by_project {
        import_project(
            db,
            emitter,
            now,
            &project_id,
            &project_key,
            threads,
            &ledger,
            report,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn import_project(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    project_id: &str,
    project_key: &str,
    threads: Vec<ThreadRow>,
    ledger: &HashSet<String>,
    report: &mut ImportReport,
) {
    let mut candidates: Vec<Candidate> = Vec::new();
    // Threads this run reconciles, with the store each one's guard is keyed by.
    let mut reconciled: Vec<(String, PathBuf)> = Vec::new();
    for thread in &threads {
        if !db_store::is_valid_thread_id(&thread.id) {
            tracing::warn!(thread = %thread.id, "ruling import skipped a thread with an invalid id");
            continue;
        }
        let path = db_store::store_path(&thread.id);
        let store = match db_store::inspect_legacy(&path).await {
            Ok(store) => store,
            Err(error) => {
                let message = format!(
                    "ruling import could not read thread {project_key}/{}'s store: {error}",
                    thread.name
                );
                tracing::error!("{message}");
                report.errors.push(message);
                continue;
            }
        };
        let table_gone = store.as_ref().is_none_or(|store| !store.rulings_table);
        if ledger.contains(&thread.id) && table_gone {
            db_store::mark_rulings_imported(&path);
            continue;
        }

        let mut thread_candidates: Vec<Candidate> = Vec::new();
        if let Some(store) = &store {
            for row in &store.rulings {
                merge(&mut thread_candidates, table_candidate(thread, row));
            }
        }
        // A pre-cutover marker means the arc's rulings were already copied into
        // the table, and the table is authoritative: rows may have been
        // superseded since. A post-cutover marker copied only open questions,
        // so it does not. Without a table to have copied into — no file, or
        // 0002 already applied — the arc is the only record.
        let include_arc = match &store {
            None => true,
            Some(store) => !store.arc_rulings_copied || store.rulings_dropped,
        };
        if include_arc {
            match arc_candidates(db, thread).await {
                Ok(arc) => {
                    for candidate in arc {
                        merge(&mut thread_candidates, candidate);
                    }
                }
                Err(error) => {
                    let message = format!(
                        "ruling import could not read thread {project_key}/{}'s arc: {error}",
                        thread.name
                    );
                    tracing::error!("{message}");
                    report.errors.push(message);
                    continue;
                }
            }
        }
        candidates.extend(thread_candidates);
        reconciled.push((thread.id.clone(), path));
    }
    if reconciled.is_empty() {
        return;
    }

    let outcome = write_project(db, now, project_id, project_key, candidates, &reconciled).await;
    match outcome {
        Ok((inserted, slug_collisions)) => {
            for (_, path) in &reconciled {
                db_store::mark_rulings_imported(path);
            }
            if inserted > 0 {
                emit_changed(emitter, project_id, "insert");
            }
            report.projects.push(ProjectImport {
                project_key: project_key.to_string(),
                inserted,
                threads: reconciled.len(),
                slug_collisions,
            });
        }
        Err(error) => {
            let message = format!(
                "ruling import for project {project_key} rolled back; its thread stores keep their rulings until the next boot: {error}"
            );
            tracing::error!("{message}");
            report.errors.push(message);
        }
    }
}

/// Insert `candidates` as tenets and record every reconciled thread in the
/// ledger, all in one transaction. Returns how many tenets were inserted and
/// how many slugs were renamed.
async fn write_project(
    db: &LocalDb,
    now: i64,
    project_id: &str,
    project_key: &str,
    candidates: Vec<Candidate>,
    reconciled: &[(String, PathBuf)],
) -> Result<(usize, usize), String> {
    let project_id = project_id.to_string();
    let project_key = project_key.to_string();
    let thread_ids: Vec<String> = reconciled.iter().map(|(id, _)| id.clone()).collect();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let project_key = project_key.clone();
        let mut candidates = candidates.clone();
        let thread_ids = thread_ids.clone();
        Box::pin(async move {
            let mut present: HashSet<String> = HashSet::new();
            let mut taken: HashMap<String, String> = HashMap::new();
            {
                let mut rows = conn
                    .query(
                        "SELECT slug, proposing_thread_id, origin,
                                json_extract(legacy_json, '$.legacy_key')
                         FROM tenets WHERE project_id = ?1",
                        params![project_id.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    if let Some(slug) = row.opt_text(0)? {
                        taken.insert(slug, row.text(1)?);
                    }
                    if row.text(2)? == TenetOrigin::MigratedRuling.as_str() {
                        if let Some(key) = row.opt_text(3)? {
                            present.insert(key);
                        }
                    }
                }
            }
            candidates.retain(|candidate| !present.contains(&candidate.legacy_key()));
            candidates.sort_by(|a, b| {
                (a.created_at, &a.thread_id, a.source, a.id).cmp(&(
                    b.created_at,
                    &b.thread_id,
                    b.source,
                    b.id,
                ))
            });

            let mut collisions = 0;
            for candidate in &candidates {
                let slug = match taken.get(&candidate.slug) {
                    None => candidate.slug.clone(),
                    Some(holder) => {
                        let renamed = (2..)
                            .map(|n| format!("{}-{n}", candidate.slug))
                            .find(|slug| !taken.contains_key(slug))
                            .expect("an unbounded suffix search finds a free slug");
                        tracing::warn!(
                            project = %project_key,
                            slug = %candidate.slug,
                            holder_thread = %holder,
                            thread = %candidate.thread_id,
                            renamed = %renamed,
                            "migrated ruling slug already held in this project; imported under a suffixed slug"
                        );
                        collisions += 1;
                        renamed
                    }
                };
                taken.insert(slug.clone(), candidate.thread_id.clone());
                let tenet = new_tenet(&project_id, &project_key, candidate, slug, now);
                let (id, _) = insert_conn(conn, &tenet).await?;
                // `insert_conn` stamps the import clock, which also moves the
                // project's `updated_at` forward; the tenet itself keeps the
                // ruling's own history.
                conn.execute(
                    "UPDATE tenets SET created_at = ?1, updated_at = ?2 WHERE id = ?3",
                    params![candidate.created_at, candidate.updated_at, id.as_str()],
                )
                .await?;
            }
            for thread_id in &thread_ids {
                conn.execute(
                    "INSERT OR IGNORE INTO tenet_ruling_imports (thread_id, project_id, imported_at)
                     VALUES (?1, ?2, ?3)",
                    params![thread_id.as_str(), project_id.as_str(), now],
                )
                .await?;
            }
            Ok((candidates.len(), collisions))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

fn new_tenet(
    project_id: &str,
    project_key: &str,
    candidate: &Candidate,
    slug: String,
    now: i64,
) -> NewTenet {
    let (status, verdict) = match candidate.status.as_str() {
        "rejected" => (TenetStatus::Declined, Verdict::No),
        "superseded" => (TenetStatus::Superseded, Verdict::Yes),
        _ => (TenetStatus::Approved, Verdict::Yes),
    };
    NewTenet {
        project_id: project_id.to_string(),
        proposing_thread_id: candidate.thread_id.clone(),
        slug: Some(slug),
        statement: candidate.text.clone(),
        // A ruling's rationale is kept verbatim in `legacy` with its
        // provenance, as every earlier import kept it.
        rationale: None,
        status,
        verdict: Some(verdict),
        comment: None,
        answered_by: None,
        answer_source: Some(AnswerSource::Migrated),
        answered_at: Some(candidate.updated_at),
        supersedes: None,
        origin: TenetOrigin::MigratedRuling,
        legacy: Some(json!({
            "rationale": candidate.rationale,
            "provenance": candidate.provenance,
            "legacy_key": candidate.legacy_key(),
            "original_slug": candidate.slug,
        })),
        // The thread decided; no run is recorded for a ruling.
        proposer: PrincipalRef::Agent {
            node: cairn_common::uri::build_thread_uri(project_key, &candidate.thread_name),
            run_id: None,
        },
        now,
    }
}

/// Fold `candidate` into one thread's candidates the way the old lazy arc
/// import folded an arc ruling into the table: an identical ruling under the
/// same slug is the same ruling, and a different one keeps both, the newcomer
/// under `<slug>-legacy`, `-legacy-2`, and so on.
fn merge(thread: &mut Vec<Candidate>, mut candidate: Candidate) {
    let base = candidate.slug.clone();
    let mut collision = 0;
    loop {
        match thread
            .iter()
            .find(|existing| existing.slug == candidate.slug)
        {
            None => {
                thread.push(candidate);
                return;
            }
            Some(existing) if existing.same_ruling(&candidate) => return,
            Some(_) => {
                collision += 1;
                candidate.slug = if collision == 1 {
                    format!("{base}-legacy")
                } else {
                    format!("{base}-legacy-{collision}")
                };
            }
        }
    }
}

fn table_candidate(thread: &ThreadRow, row: &LegacyRulingRow) -> Candidate {
    Candidate {
        thread_id: thread.id.clone(),
        thread_name: thread.name.clone(),
        source: Source::Table,
        id: row.id,
        slug: row.slug.clone(),
        text: row.text.clone(),
        status: row.status.clone(),
        rationale: row.rationale.clone(),
        provenance: serde_json::from_str(&row.provenance).unwrap_or_else(|_| json!([])),
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

/// The rulings embedded in the arc of every job the thread owns, read from
/// each job's latest arc version that still carries a `rulings` key.
///
/// Not simply the latest version: every arc write since the collections left
/// the artifact strips the key, so a prose patch made after a failed import
/// would otherwise hide the rulings the retry is meant to recover. A version
/// that kept the key but emptied it is the thread's own latest word and is
/// honored.
async fn arc_candidates(db: &LocalDb, thread: &ThreadRow) -> Result<Vec<Candidate>, String> {
    let jobs = db
        .query_all(
            "SELECT id FROM jobs WHERE thread_id = ?1 ORDER BY created_at, id",
            (thread.id.clone(),),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    for job in jobs {
        let latest = db
            .query_opt(
                "SELECT data, created_at, updated_at FROM artifacts
                 WHERE job_id = ?1 AND output_name = ?2
                   AND json_valid(data) AND json_type(data, '$.rulings') IS NOT NULL
                 ORDER BY version DESC LIMIT 1",
                (job.clone(), crate::threads::ARC_ARTIFACT_NAME.to_string()),
                |row| Ok((row.text(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        let Some((data, created_at, updated_at)) = latest else {
            continue;
        };
        let data: Value = serde_json::from_str(&data).map_err(|error| error.to_string())?;
        let rulings = data
            .get("rulings")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (index, ruling) in rulings.iter().enumerate() {
            match arc_candidate(thread, ruling, index, created_at, updated_at) {
                Some(candidate) => out.push(candidate),
                None => tracing::warn!(
                    thread = %thread.id,
                    job = %job,
                    index,
                    "ruling import skipped an arc ruling with no text"
                ),
            }
        }
    }
    Ok(out)
}

fn arc_candidate(
    thread: &ThreadRow,
    ruling: &Value,
    index: usize,
    created_at: i64,
    updated_at: i64,
) -> Option<Candidate> {
    let text = ruling
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| ruling.as_str())
        .map(str::trim)
        .filter(|text| !text.is_empty())?
        .to_string();
    let slug = ruling
        .get("slug")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|slug| !slug.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| derive_slug(&text, index));
    let status = ruling
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("accepted")
        .to_string();
    let rationale = ruling
        .get("rationale")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("No rationale recorded")
        .to_string();
    Some(Candidate {
        thread_id: thread.id.clone(),
        thread_name: thread.name.clone(),
        source: Source::Arc,
        id: index as i64,
        slug,
        text,
        status,
        rationale,
        provenance: normalized_provenance(ruling.get("provenance")),
        created_at,
        updated_at,
    })
}

fn normalized_provenance(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Array(items)) => Value::Array(items.clone()),
        Some(Value::String(uri)) if !uri.trim().is_empty() => json!([uri]),
        _ => json!([]),
    }
}

fn derive_slug(text: &str, index: usize) -> String {
    let slug = text
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join("-");
    if slug.chars().any(|ch| ch.is_ascii_alphabetic()) {
        slug
    } else {
        format!("ruling-{}", index + 1)
    }
}

/// Arcs carrying rulings on a job no thread owns cannot be attributed to a
/// proposing thread, so they are named in the log and left alone.
async fn log_orphan_arcs(db: &LocalDb) {
    let orphans = db
        .query_all(
            "SELECT DISTINCT a.job_id FROM artifacts a JOIN jobs j ON j.id = a.job_id
             WHERE a.output_name = ?1 AND j.thread_id IS NULL
               AND json_valid(a.data) AND json_array_length(a.data, '$.rulings') > 0",
            (crate::threads::ARC_ARTIFACT_NAME.to_string(),),
            |row| row.text(0),
        )
        .await;
    match orphans {
        Ok(jobs) => {
            for job in jobs {
                tracing::warn!(job = %job, "ruling import skipped an arc with rulings on a job that belongs to no thread");
            }
        }
        Err(error) => tracing::warn!(%error, "ruling import could not look for orphan arcs"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(slug: &str, text: &str) -> Candidate {
        Candidate {
            thread_id: "t".into(),
            thread_name: "n".into(),
            source: Source::Table,
            id: 1,
            slug: slug.into(),
            text: text.into(),
            status: "accepted".into(),
            rationale: "r".into(),
            provenance: json!([]),
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn merge_skips_identical_rulings_and_suffixes_different_ones() {
        let mut thread = Vec::new();
        merge(&mut thread, candidate("same", "A"));
        merge(&mut thread, candidate("same", "A"));
        merge(&mut thread, candidate("same", "B"));
        merge(&mut thread, candidate("same", "C"));
        let slugs: Vec<_> = thread.iter().map(|c| c.slug.as_str()).collect();
        assert_eq!(slugs, ["same", "same-legacy", "same-legacy-2"]);
    }

    #[test]
    fn derives_stable_valid_slug() {
        assert_eq!(
            derive_slug("The Arc's collections dissolve!", 0),
            "the-arc-s-collections-dissolve"
        );
        assert_eq!(derive_slug("123", 2), "ruling-3");
    }

    #[test]
    fn statuses_map_onto_verdicts() {
        let mut row = candidate("s", "Text");
        for (status, expected, verdict) in [
            ("accepted", TenetStatus::Approved, Verdict::Yes),
            ("rejected", TenetStatus::Declined, Verdict::No),
            ("superseded", TenetStatus::Superseded, Verdict::Yes),
        ] {
            row.status = status.into();
            let tenet = new_tenet("p", "proj", &row, "s".into(), 9);
            assert_eq!(tenet.status, expected);
            assert_eq!(tenet.verdict, Some(verdict));
            assert_eq!(tenet.statement, "Text");
            assert_eq!(tenet.answered_at, Some(1));
        }
    }
}
