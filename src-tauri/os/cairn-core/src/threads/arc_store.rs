use cairn_db::turso::params;
use serde_json::{json, Value};

use crate::storage::{LocalDb, RowExt};
use crate::tenets::TenetView;

/// What an arc reads as: the prose the thread authored, the tenets it
/// proposed, and the open questions in its store.
#[derive(Debug, Clone)]
pub struct ArcProjection {
    pub prose: Value,
    /// Every tenet the thread proposed, oldest first.
    pub tenets: Vec<TenetView>,
    pub open_questions: Vec<Value>,
    pub thread_id: String,
    pub project_id: String,
    pub project_key: String,
    pub thread_name: String,
}

pub async fn render(db: &LocalDb, job_id: &str, prose: &Value, now: i64) -> Result<String, String> {
    use crate::resources::thread_seed::{
        default_tenet_selector, render_arc_projection, TenetSelection,
    };
    let projection = project(db, job_id, prose).await?;
    let ranked = default_tenet_selector()
        .rank(
            db,
            &TenetSelection {
                thread_id: &projection.thread_id,
                thread_name: &projection.thread_name,
                project_id: &projection.project_id,
                project_key: &projection.project_key,
                own: &projection.tenets,
                vectors: crate::tenets::evidence::vectors::vector_db(db),
                now,
            },
        )
        .await;
    Ok(render_arc_projection(&projection, &ranked))
}

pub async fn prose_json(db: &LocalDb, job_id: &str, legacy: &Value) -> Result<Value, String> {
    import_legacy_collections(db, job_id, legacy).await?;
    let mut prose = legacy.clone();
    retain_prose_only(&mut prose);
    Ok(prose)
}

pub(crate) fn retain_prose_only(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.remove("rulings");
        object.remove("open_questions");
    }
}

pub async fn project(db: &LocalDb, job_id: &str, prose: &Value) -> Result<ArcProjection, String> {
    let owner = arc_owner(db, job_id).await?;
    let store = super::db_store::open(&owner.thread_id).await?;
    if import_legacy_collections_into(&store, prose).await? {
        crate::orchestrator::wakes::conditions::notify_thread_db_written(&owner.thread_id);
    }
    let open_questions = store
        .read(|conn| {
            Box::pin(async move {
                let mut question_rows = conn
                    .query(
                        "SELECT question, provenance FROM _cairn_open_questions ORDER BY id",
                        (),
                    )
                    .await?;
                let mut questions = Vec::new();
                while let Some(row) = question_rows.next().await? {
                    let provenance = row
                        .get::<Option<String>>(1)?
                        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
                    questions.push(json!({
                        "question": row.get::<String>(0)?,
                        "provenance": provenance,
                    }));
                }
                Ok(questions)
            })
        })
        .await
        .map_err(|error| format!("failed to read arc open questions: {error}"))?;
    let tenets = crate::tenets::for_thread(db, &owner.thread_id).await?;

    let mut prose = prose.clone();
    retain_prose_only(&mut prose);
    Ok(ArcProjection {
        prose,
        tenets,
        open_questions,
        thread_id: owner.thread_id,
        project_id: owner.project_id,
        project_key: owner.project_key,
        thread_name: owner.thread_name,
    })
}

struct ArcOwner {
    thread_id: String,
    thread_name: String,
    project_id: String,
    project_key: String,
}

async fn arc_owner(db: &LocalDb, job_id: &str) -> Result<ArcOwner, String> {
    db.query_opt(
        "SELECT t.id, t.name, p.id, p.key
         FROM jobs j
         JOIN threads t ON t.id = j.thread_id
         JOIN projects p ON p.id = t.project_id
         WHERE j.id = ?1",
        params![job_id.to_string()],
        |row| {
            Ok(ArcOwner {
                thread_id: row.text(0)?,
                thread_name: row.text(1)?,
                project_id: row.text(2)?,
                project_key: row.text(3)?,
            })
        },
    )
    .await
    .map_err(|error| format!("failed to resolve arc thread: {error}"))?
    .ok_or_else(|| "arc job has no thread owner".to_string())
}

pub(crate) async fn import_legacy_collections(
    db: &LocalDb,
    job_id: &str,
    prose: &Value,
) -> Result<(), String> {
    let owner = arc_owner(db, job_id).await?;
    let store = super::db_store::open(&owner.thread_id).await?;
    if import_legacy_collections_into(&store, prose).await? {
        crate::orchestrator::wakes::conditions::notify_thread_db_written(&owner.thread_id);
    }
    Ok(())
}

/// Migrate a legacy arc's open questions into the thread store, reporting
/// whether this call was the one that wrote them.
///
/// The arc's embedded rulings are not copied here: they became tenets at boot
/// (`tenets::import_thread_rulings`), which reads the arc directly.
///
/// The answer is what the caller notifies the condition evaluator on. It is
/// deliberately not "was this called": `project` runs on every arc render, and
/// the import is idempotent and almost always a no-op, so announcing a commit
/// here would turn a read path into a steady source of condition evaluations.
async fn import_legacy_collections_into(store: &LocalDb, prose: &Value) -> Result<bool, String> {
    let questions = prose
        .get("open_questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if questions.is_empty() {
        return Ok(false);
    }
    store
        .write(|conn| {
            let questions = questions.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT 1 FROM _cairn_arc_imports WHERE id = 1", ())
                    .await?;
                if rows.next().await?.is_some() {
                    return Ok(false);
                }
                drop(rows);
                for question in &questions {
                    let text = question
                        .get("question")
                        .and_then(Value::as_str)
                        .unwrap_or_else(|| question.as_str().unwrap_or_default());
                    let provenance = question
                        .get("provenance")
                        .map(|value| normalized_optional_provenance(Some(value)));
                    conn.execute(
                        "INSERT INTO _cairn_open_questions (question, provenance) VALUES (?1, ?2)",
                        params![text, provenance],
                    )
                    .await?;
                }
                // `rulings_copied = 0`: this import never copies rulings, so the
                // boot-time ruling import must still read them from the arc.
                conn.execute(
                    "INSERT INTO _cairn_arc_imports(id, rulings_copied) VALUES (1, 0)",
                    (),
                )
                .await?;
                Ok(true)
            })
        })
        .await
        .map_err(|error| format!("failed to migrate legacy arc open questions: {error}"))
}

fn normalized_optional_provenance(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Array(items)) => serde_json::to_string(items).ok(),
        Some(Value::String(uri)) if !uri.trim().is_empty() => {
            serde_json::to_string(&vec![uri]).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{import_legacy_collections_into, retain_prose_only};
    use crate::storage::RowExt;
    use serde_json::json;

    #[test]
    fn legacy_collection_keys_are_removed_from_the_stored_prose_shape() {
        let mut arc = json!({
            "current_intent": "Ship it",
            "working": "Carefully",
            "rulings": [],
            "open_questions": []
        });
        retain_prose_only(&mut arc);
        assert_eq!(
            arc,
            json!({"current_intent": "Ship it", "working": "Carefully"})
        );
    }

    async fn counts(store: &crate::storage::LocalDb) -> (i64, i64) {
        store
            .query_one(
                "SELECT (SELECT COUNT(*) FROM _cairn_open_questions),
                        (SELECT COUNT(*) FROM _cairn_arc_imports)",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)?)),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn legacy_import_copies_open_questions_exactly_once_and_never_rulings() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::threads::db_store::open_path(dir.path().join("thread.db"))
            .await
            .unwrap();
        let legacy = json!({
            "rulings": [{
                "slug": "legacy-ruling", "text": "Legacy", "status": "accepted",
                "rationale": "Legacy rationale", "provenance": ["cairn://p/cairn/old"]
            }],
            "open_questions": [{
                "question": "What remains?",
                "provenance": ["cairn://p/cairn/old"]
            }]
        });

        assert!(import_legacy_collections_into(&store, &legacy)
            .await
            .unwrap());
        assert!(!import_legacy_collections_into(&store, &legacy)
            .await
            .unwrap());

        assert_eq!(counts(&store).await, (1, 1));
        let rulings_table = store
            .query_one(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = '_cairn_rulings'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(rulings_table, 0, "a fresh store has no rulings table");
    }

    #[tokio::test]
    async fn failed_import_rolls_back_rows_and_marker_then_retries_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::threads::db_store::open_path(dir.path().join("rollback.db"))
            .await
            .unwrap();
        let invalid = json!({"open_questions": [{"question": "First?"}, {"question": ""}]});
        assert!(import_legacy_collections_into(&store, &invalid)
            .await
            .is_err());
        assert_eq!(counts(&store).await, (0, 0));

        let corrected =
            json!({"open_questions": [{"question": "First?"}, {"question": "Second?"}]});
        import_legacy_collections_into(&store, &corrected)
            .await
            .unwrap();
        import_legacy_collections_into(&store, &corrected)
            .await
            .unwrap();
        assert_eq!(counts(&store).await, (2, 1));
    }

    async fn arc_job(name: &str) -> (crate::storage::LocalDb, String) {
        let db = crate::storage::migrated_test_db(name).await;
        let thread_id = format!("thread-{name}");
        db.execute_script(&format!(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES ('p','default','P','p','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at)
             VALUES ('{thread_id}','p','arc-test','active','none',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,created_at,updated_at)
             VALUES ('job','{thread_id}','p','running',1,1);"
        ))
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap().keep();
        crate::threads::db_store::use_test_path(&thread_id, dir.join("thread.db"));
        (db, "job".to_string())
    }

    #[tokio::test]
    async fn first_prose_replacement_keeps_legacy_open_questions_and_drops_arc_rulings() {
        let (db, job) = arc_job("arc-create-first.db").await;
        let legacy = json!({
            "current_intent": "Before",
            "rulings": [{
                "slug": "survive-replacement", "text": "Survive", "status": "accepted",
                "rationale": "It matters", "provenance": ["cairn://p/cairn/old"]
            }],
            "open_questions": [{"question": "Also survive?"}]
        });

        super::import_legacy_collections(&db, &job, &legacy)
            .await
            .unwrap();
        let replacement = json!({"current_intent": "After", "working": "Replacement"});
        let projected = super::project(&db, &job, &replacement).await.unwrap();
        assert_eq!(projected.prose["current_intent"], "After");
        assert_eq!(projected.open_questions[0]["question"], "Also survive?");
        assert!(
            projected.tenets.is_empty(),
            "arc rulings become tenets only through the boot import"
        );
        assert_eq!(projected.project_key, "p");
        assert_eq!(projected.thread_name, "arc-test");
    }

    #[tokio::test]
    async fn json_projection_is_prose_only_while_importing_legacy_open_questions() {
        let (db, job) = arc_job("arc-json-contract.db").await;
        let legacy = json!({
            "current_intent": "Intent", "working": "Work",
            "rulings": [{
                "slug": "decision", "text": "Decision", "status": "accepted",
                "rationale": "Because", "provenance": ["cairn://p/cairn/old"]
            }],
            "open_questions": [{"question": "Question?"}]
        });

        let json = super::prose_json(&db, &job, &legacy).await.unwrap();
        assert_eq!(json, json!({"current_intent": "Intent", "working": "Work"}));
        let projected = super::project(&db, &job, &json).await.unwrap();
        assert_eq!(projected.prose, json);
        assert_eq!(projected.open_questions.len(), 1);
    }
}
