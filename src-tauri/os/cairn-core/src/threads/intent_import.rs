//! The boot-time move of every thread's retired arc into its starter record.
//!
//! Before thread migration 0005 a thread authored its intent into the `arc`
//! artifact (`current_intent` and `working`), and some arcs still embed the
//! `open_questions` the old lazy import never copied. This pass reads the
//! newest arc version across a thread's jobs and fills the thread's `intent`
//! row from it, but only while that row has never been written: the row's
//! trigger stamps `updated_at` on every write, so a thread that has patched its
//! intent is never overwritten. In the same transaction it copies an unmarked
//! arc's open questions in as `question` rows and writes the `_cairn_arc_imports`
//! marker, so the import is idempotent on both halves.
//!
//! Arc versions stay in the main database as history; nothing here deletes
//! them, and the ruling import still reads them.

use std::collections::HashMap;

use cairn_db::turso::params;
use serde_json::Value;

use crate::db::DbState;
use crate::storage::{LocalDb, RowExt};
use crate::threads::db_store;

/// The output name the retired arc artifact was stored under.
pub(crate) const RETIRED_ARC_NAME: &str = "arc";

/// What one boot's import did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntentImportReport {
    /// Intent rows filled from an arc.
    pub intents: usize,
    /// Open questions copied in as journal rows.
    pub questions: usize,
    /// Threads that could not be imported this boot; the next boot retries.
    pub errors: Vec<String>,
}

/// Import every thread's arc, in the private database and every open team
/// replica.
pub async fn import_arc_intents(db: &DbState) -> IntentImportReport {
    let mut report = IntentImportReport::default();
    for store in db.all_dbs().await {
        import_database(&store, &mut report).await;
    }
    report
}

/// One thread's arc, as the import reads it.
#[derive(Debug, Clone, Default)]
struct ThreadArc {
    /// The newest version's prose and when it was written.
    newest: Option<(Value, i64)>,
    /// The newest version still embedding `open_questions`. Every arc write
    /// since the collections left the artifact stripped the key, so a later
    /// prose version does not hide questions an import never copied.
    questions: Option<(Vec<Value>, i64)>,
}

/// Import the arcs of every thread in one database.
pub async fn import_database(db: &LocalDb, report: &mut IntentImportReport) {
    let rows = match db
        .query_all(
            "SELECT j.thread_id, a.data, a.updated_at FROM artifacts a
             JOIN jobs j ON j.id = a.job_id
             WHERE a.output_name = ?1 AND j.thread_id IS NOT NULL AND json_valid(a.data)
             ORDER BY j.thread_id, a.updated_at DESC, a.version DESC",
            params![RETIRED_ARC_NAME],
            |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)?)),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            let message = format!("intent import could not read arcs: {error}");
            tracing::error!("{message}");
            report.errors.push(message);
            return;
        }
    };
    let mut arcs: HashMap<String, ThreadArc> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for (thread_id, data, updated_at) in rows {
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let arc = arcs.entry(thread_id.clone()).or_insert_with(|| {
            order.push(thread_id.clone());
            ThreadArc::default()
        });
        if arc.questions.is_none() {
            if let Some(questions) = data.get("open_questions").and_then(Value::as_array) {
                arc.questions = Some((questions.clone(), updated_at));
            }
        }
        if arc.newest.is_none() {
            arc.newest = Some((data, updated_at));
        }
    }
    for thread_id in order {
        if !db_store::is_valid_thread_id(&thread_id) {
            tracing::warn!(thread = %thread_id, "intent import skipped a thread with an invalid id");
            continue;
        }
        let arc = arcs.remove(&thread_id).unwrap_or_default();
        match import_thread(&thread_id, arc).await {
            Ok((intent, questions)) => {
                if intent || questions > 0 {
                    crate::orchestrator::wakes::conditions::notify_thread_db_written(
                        &thread_id,
                        &[],
                    );
                }
                report.intents += usize::from(intent);
                report.questions += questions;
            }
            Err(error) => {
                let message = format!("intent import failed for thread {thread_id}: {error}");
                tracing::error!("{message}");
                report.errors.push(message);
            }
        }
    }
}

/// Fill one thread's record from its arc in one transaction: whether the
/// intent row was written, and how many questions were copied.
async fn import_thread(thread_id: &str, arc: ThreadArc) -> Result<(bool, usize), String> {
    let intent = arc
        .newest
        .as_ref()
        .and_then(|(data, at)| arc_intent(data).map(|(current, working)| (current, working, *at)));
    let questions: Vec<(String, String, i64)> = arc
        .questions
        .map(|(questions, at)| {
            questions
                .iter()
                .filter_map(|question| {
                    let text = question
                        .get("question")
                        .and_then(Value::as_str)
                        .or_else(|| question.as_str())
                        .map(str::trim)
                        .filter(|text| !text.is_empty())?;
                    Some((text.to_string(), refs_json(question.get("provenance")), at))
                })
                .collect()
        })
        .unwrap_or_default();
    if intent.is_none() && questions.is_empty() {
        return Ok((false, 0));
    }
    let store = db_store::open(thread_id).await?;
    store
        .write(move |conn| {
            let intent = intent.clone();
            let questions = questions.clone();
            Box::pin(async move {
                let mut wrote_intent = false;
                if let Some((current, working, at)) = intent {
                    wrote_intent = conn
                        .execute(
                            "UPDATE intent SET current_intent = ?1, working = ?2, updated_at = ?3
                             WHERE id = 1 AND updated_at IS NULL",
                            params![current, working, iso(at)],
                        )
                        .await?
                        > 0;
                }
                let mut copied = 0;
                if !questions.is_empty() {
                    let mut marker = conn
                        .query("SELECT 1 FROM _cairn_arc_imports WHERE id = 1", ())
                        .await?;
                    let marked = marker.next().await?.is_some();
                    drop(marker);
                    if !marked {
                        for (note, refs, at) in &questions {
                            conn.execute(
                                "INSERT INTO journal(ts, kind, note, refs) VALUES (?1, 'question', ?2, ?3)",
                                params![iso(*at), note.as_str(), refs.as_str()],
                            )
                            .await?;
                            copied += 1;
                        }
                        // `rulings_copied = 0`: this import never copies
                        // rulings, so the ruling import still reads them from
                        // the arc.
                        conn.execute(
                            "INSERT INTO _cairn_arc_imports(id, rulings_copied) VALUES (1, 0)",
                            (),
                        )
                        .await?;
                    }
                }
                Ok((wrote_intent, copied))
            })
        })
        .await
        .map_err(db_store::sanitize_error)
}

/// The intent an arc carried: its `current_intent` and `working`, or, for an
/// arc in another shape, its `content` or `body` (or the rest of it as JSON)
/// as the intent, the way the seed passed such an arc through. `None` when the
/// arc authored nothing.
fn arc_intent(data: &Value) -> Option<(String, String)> {
    let text = |key: &str| {
        data.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or_default()
            .to_string()
    };
    let (current, working) = (text("current_intent"), text("working"));
    if !current.is_empty() || !working.is_empty() {
        return Some((current, working));
    }
    let free_form = ["content", "body"]
        .into_iter()
        .map(text)
        .find(|value| !value.is_empty())
        .or_else(|| {
            let mut rest = data.as_object()?.clone();
            rest.remove("rulings");
            rest.remove("open_questions");
            (!rest.is_empty()).then(|| Value::Object(rest).to_string())
        })?;
    Some((free_form, String::new()))
}

fn refs_json(provenance: Option<&Value>) -> String {
    let refs: Vec<&str> = match provenance {
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        Some(Value::String(uri)) if !uri.trim().is_empty() => vec![uri.as_str()],
        _ => Vec::new(),
    };
    serde_json::to_string(&refs).unwrap_or_else(|_| "[]".to_string())
}

fn iso(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|time| time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| at.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_arc_in_another_shape_becomes_its_intent_whole() {
        assert_eq!(
            arc_intent(&json!({"current_intent": " Ship ", "working": "Bench"})),
            Some(("Ship".into(), "Bench".into()))
        );
        assert_eq!(
            arc_intent(&json!({"content": "free-form arc"})),
            Some(("free-form arc".into(), String::new()))
        );
        assert_eq!(
            arc_intent(&json!({"goal": "x", "rulings": []})),
            Some((r#"{"goal":"x"}"#.into(), String::new()))
        );
        assert_eq!(
            arc_intent(&json!({"rulings": [], "open_questions": []})),
            None
        );
    }

    #[test]
    fn provenance_becomes_a_refs_array() {
        assert_eq!(
            refs_json(Some(&json!(["cairn://p/a/1"]))),
            r#"["cairn://p/a/1"]"#
        );
        assert_eq!(
            refs_json(Some(&json!("cairn://p/a/2"))),
            r#"["cairn://p/a/2"]"#
        );
        assert_eq!(refs_json(None), "[]");
        assert_eq!(iso(1_762_387_200), "2025-11-06T00:00:00Z");
    }

    async fn fixture(name: &str) -> (LocalDb, String) {
        let db = crate::storage::migrated_test_db(name).await;
        let thread_id = format!("thread-{name}").replace('.', "-");
        db.execute_script(&format!(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('p','default','P','p','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
             VALUES ('{thread_id}','p','topic','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,created_at,updated_at)
             VALUES ('old-job','{thread_id}','p','complete',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,created_at,updated_at)
             VALUES ('new-job','{thread_id}','p','running',2,2);"
        ))
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap().keep();
        db_store::use_test_path(&thread_id, dir.join("thread.db"));
        (db, thread_id)
    }

    async fn arc(db: &LocalDb, id: &str, job: &str, version: i64, at: i64, data: Value) {
        db.execute(
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, created_at, updated_at, confirmed)
             VALUES (?1, ?2, 'arc', 1, ?3, ?4, 'arc', ?5, ?5, 1)",
            params![id, job, data.to_string(), version, at],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn the_newest_arc_across_jobs_fills_an_unwritten_intent_once() {
        let (db, thread_id) = fixture("intent-import-newest.db").await;
        arc(&db, "a1", "old-job", 7, 1_762_300_000, json!({
            "current_intent": "Older",
            "open_questions": [{"question": "Who owns aliases?", "provenance": ["cairn://p/p/4"]}, {"question": ""}]
        })).await;
        arc(
            &db,
            "a2",
            "new-job",
            1,
            1_762_387_200,
            json!({
                "current_intent": "Newest", "working": "In motion"
            }),
        )
        .await;

        let mut report = IntentImportReport::default();
        import_database(&db, &mut report).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!((report.intents, report.questions), (1, 1));
        let store = db_store::open(&thread_id).await.unwrap();
        let intent = store
            .query_one(
                "SELECT current_intent, working, updated_at FROM intent WHERE id = 1",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            intent,
            (
                "Newest".to_string(),
                "In motion".to_string(),
                "2025-11-06T00:00:00Z".to_string()
            )
        );
        let question = store
            .query_one(
                "SELECT note, refs FROM journal WHERE kind = 'question'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            question,
            (
                "Who owns aliases?".to_string(),
                r#"["cairn://p/p/4"]"#.to_string()
            )
        );

        // A second boot changes nothing, and a row the thread wrote since is
        // never overwritten.
        store
            .execute("UPDATE intent SET working = 'Mine now' WHERE id = 1", ())
            .await
            .unwrap();
        let mut again = IntentImportReport::default();
        import_database(&db, &mut again).await;
        assert_eq!((again.intents, again.questions), (0, 0));
        assert_eq!(
            store
                .query_opt_text("SELECT working FROM intent WHERE id = 1", ())
                .await
                .unwrap()
                .as_deref(),
            Some("Mine now")
        );
        assert_eq!(
            store
                .query_opt_i64("SELECT COUNT(*) FROM journal", ())
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn a_written_intent_is_never_overwritten_and_marked_questions_are_not_recopied() {
        let (db, thread_id) = fixture("intent-import-written.db").await;
        let store = db_store::open(&thread_id).await.unwrap();
        store
            .execute_script(
                "UPDATE intent SET current_intent = 'Authored' WHERE id = 1;
                 INSERT INTO _cairn_arc_imports(id, rulings_copied) VALUES (1, 0);",
            )
            .await
            .unwrap();
        arc(
            &db,
            "a1",
            "new-job",
            1,
            1_762_387_200,
            json!({
                "current_intent": "From the arc",
                "open_questions": [{"question": "Already copied?"}]
            }),
        )
        .await;

        let mut report = IntentImportReport::default();
        import_database(&db, &mut report).await;
        assert_eq!((report.intents, report.questions), (0, 0));
        assert_eq!(
            store
                .query_opt_text("SELECT current_intent FROM intent WHERE id = 1", ())
                .await
                .unwrap()
                .as_deref(),
            Some("Authored")
        );
    }
}
