use cairn_db::turso::params;
use serde_json::{json, Value};

use crate::storage::{LocalDb, RowExt};

pub async fn render(db: &LocalDb, job_id: &str, prose: &Value) -> Result<String, String> {
    let projected = project(db, job_id, prose).await?;
    Ok(super::super::resources::thread_seed::render_arc_data(
        &projected,
    ))
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

pub async fn project(db: &LocalDb, job_id: &str, prose: &Value) -> Result<Value, String> {
    let (thread_id, store) = legacy_store(db, job_id).await?;
    if import_legacy_collections_into(&store, prose).await? {
        crate::orchestrator::wakes::conditions::notify_thread_db_written(&thread_id);
    }
    let (rulings, questions) = store
        .read(|conn| {
            Box::pin(async move {
                let mut ruling_rows = conn
                    .query(
                        "SELECT slug, text, status, rationale, provenance
                         FROM _cairn_rulings ORDER BY id",
                        (),
                    )
                    .await?;
                let mut rulings = Vec::new();
                while let Some(row) = ruling_rows.next().await? {
                    rulings.push(json!({
                        "slug": row.get::<String>(0)?,
                        "text": row.get::<String>(1)?,
                        "status": row.get::<String>(2)?,
                        "rationale": row.get::<String>(3)?,
                        "provenance": serde_json::from_str::<Value>(&row.get::<String>(4)?)
                            .unwrap_or(Value::Array(Vec::new())),
                    }));
                }
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
                Ok((rulings, questions))
            })
        })
        .await
        .map_err(|error| format!("failed to read arc collections: {error}"))?;

    let mut projected = prose.clone();
    if let Some(object) = projected.as_object_mut() {
        object.remove("rulings");
        object.remove("open_questions");
        object.insert("rulings".to_string(), Value::Array(rulings));
        object.insert("open_questions".to_string(), Value::Array(questions));
    }
    Ok(projected)
}

async fn legacy_store(
    db: &LocalDb,
    job_id: &str,
) -> Result<(String, std::sync::Arc<LocalDb>), String> {
    let thread_id = db
        .query_opt(
            "SELECT thread_id FROM jobs WHERE id = ?1",
            params![job_id.to_string()],
            |row| row.text(0),
        )
        .await
        .map_err(|error| format!("failed to resolve arc thread: {error}"))?
        .ok_or_else(|| "arc job has no thread owner".to_string())?;
    let store = super::db_store::open(&thread_id).await?;
    Ok((thread_id, store))
}

pub(crate) async fn import_legacy_collections(
    db: &LocalDb,
    job_id: &str,
    prose: &Value,
) -> Result<(), String> {
    let (thread_id, store) = legacy_store(db, job_id).await?;
    if import_legacy_collections_into(&store, prose).await? {
        crate::orchestrator::wakes::conditions::notify_thread_db_written(&thread_id);
    }
    Ok(())
}

/// Migrate a legacy arc's collections into the thread store, reporting whether
/// this call was the one that wrote them.
///
/// The answer is what the caller notifies the condition evaluator on. It is
/// deliberately not "was this called": `project` runs on every arc render, and
/// the import is idempotent and almost always a no-op, so announcing a commit
/// here would turn a read path into a steady source of condition evaluations.
async fn import_legacy_collections_into(store: &LocalDb, prose: &Value) -> Result<bool, String> {
    let rulings = prose
        .get("rulings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let questions = prose
        .get("open_questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rulings.is_empty() && questions.is_empty() {
        return Ok(false);
    }
    store
        .write(|conn| {
            let rulings = rulings.clone();
            let questions = questions.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT 1 FROM _cairn_arc_imports WHERE id = 1", ())
                    .await?;
                if rows.next().await?.is_some() {
                    return Ok(false);
                }
                for (index, ruling) in rulings.iter().enumerate() {
                    let text = ruling
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let slug = ruling
                        .get("slug")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| derive_slug(text, index));
                    let status = ruling
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("accepted");
                    let rationale = ruling
                        .get("rationale")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or("No rationale recorded");
                    let provenance = normalized_provenance(ruling.get("provenance"));
                    let mut candidate = slug.clone();
                    let mut collision = 0;
                    loop {
                        let mut existing = conn
                            .query(
                                "SELECT text, status, rationale, provenance
                                 FROM _cairn_rulings WHERE slug = ?1",
                                params![candidate.clone()],
                            )
                            .await?;
                        let Some(row) = existing.next().await? else {
                            conn.execute(
                                "INSERT INTO _cairn_rulings
                                 (slug, text, status, rationale, provenance)
                                 VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![candidate, text, status, rationale, provenance],
                            )
                            .await?;
                            break;
                        };
                        let same_ruling = row.get::<String>(0)? == text
                            && row.get::<String>(1)? == status
                            && row.get::<String>(2)? == rationale
                            && row.get::<String>(3)? == provenance;
                        drop(existing);
                        if same_ruling {
                            break;
                        }
                        collision += 1;
                        candidate = if collision == 1 {
                            format!("{slug}-legacy")
                        } else {
                            format!("{slug}-legacy-{collision}")
                        };
                    }
                }
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
                conn.execute("INSERT INTO _cairn_arc_imports(id) VALUES (1)", ())
                    .await?;
                Ok(true)
            })
        })
        .await
        .map_err(|error| format!("failed to migrate legacy arc collections: {error}"))
}

fn normalized_provenance(value: Option<&Value>) -> String {
    normalized_optional_provenance(value).unwrap_or_else(|| "[]".to_string())
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

#[cfg(test)]
mod tests {
    use super::{derive_slug, import_legacy_collections_into, retain_prose_only};
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

    #[tokio::test]
    async fn legacy_import_reconciles_with_preexisting_rows_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::threads::db_store::open_path(dir.path().join("thread.db"))
            .await
            .unwrap();
        store
            .execute_script(
                "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance)
                 VALUES ('new-ruling','New','accepted','New rationale','[\"cairn://p/cairn/new\"]')",
            )
            .await
            .unwrap();
        let legacy = json!({
            "rulings": [{
                "slug": "legacy-ruling",
                "text": "Legacy",
                "status": "accepted",
                "rationale": "Legacy rationale",
                "provenance": ["cairn://p/cairn/old"]
            }],
            "open_questions": [{
                "question": "What remains?",
                "provenance": ["cairn://p/cairn/old"]
            }]
        });

        import_legacy_collections_into(&store, &legacy)
            .await
            .unwrap();
        import_legacy_collections_into(&store, &legacy)
            .await
            .unwrap();

        let counts = store
            .query_one(
                "SELECT
                   (SELECT COUNT(*) FROM _cairn_rulings),
                   (SELECT COUNT(*) FROM _cairn_open_questions),
                   (SELECT COUNT(*) FROM _cairn_arc_imports)",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(counts, (2, 1, 1));
    }

    #[tokio::test]
    async fn legacy_slug_collision_preserves_both_distinct_rulings() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::threads::db_store::open_path(dir.path().join("collision.db"))
            .await
            .unwrap();
        store
            .execute_script(
                "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance)
             VALUES ('same-slug','New','accepted','New rationale','[\"cairn://p/cairn/new\"]')",
            )
            .await
            .unwrap();
        let legacy = json!({"rulings": [{
            "slug": "same-slug", "text": "Legacy", "status": "rejected",
            "rationale": "Legacy rationale", "provenance": ["cairn://p/cairn/old"]
        }]});

        import_legacy_collections_into(&store, &legacy)
            .await
            .unwrap();

        let rows = store
            .query_all(
                "SELECT slug, text FROM _cairn_rulings ORDER BY id",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("same-slug".to_string(), "New".to_string()),
                ("same-slug-legacy".to_string(), "Legacy".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn failed_import_rolls_back_rows_and_marker_then_retries_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::threads::db_store::open_path(dir.path().join("rollback.db"))
            .await
            .unwrap();
        let ruling = json!({
            "slug": "legacy", "text": "Legacy", "status": "accepted",
            "rationale": "Because", "provenance": ["cairn://p/cairn/old"]
        });
        let invalid = json!({"rulings": [ruling.clone()], "open_questions": [
            {"question": "First?"}, {"question": ""}
        ]});
        assert!(import_legacy_collections_into(&store, &invalid)
            .await
            .is_err());
        let empty = store
            .query_one(
                "SELECT (SELECT COUNT(*) FROM _cairn_rulings),
                    (SELECT COUNT(*) FROM _cairn_open_questions),
                    (SELECT COUNT(*) FROM _cairn_arc_imports)",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(empty, (0, 0, 0));

        let corrected = json!({"rulings": [ruling], "open_questions": [
            {"question": "First?"}, {"question": "Second?"}
        ]});
        import_legacy_collections_into(&store, &corrected)
            .await
            .unwrap();
        import_legacy_collections_into(&store, &corrected)
            .await
            .unwrap();
        let counts = store
            .query_one(
                "SELECT (SELECT COUNT(*) FROM _cairn_rulings),
                    (SELECT COUNT(*) FROM _cairn_open_questions),
                    (SELECT COUNT(*) FROM _cairn_arc_imports)",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(counts, (1, 2, 1));
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
    async fn first_prose_patch_imports_legacy_collections_before_stripping() {
        let (db, job) = arc_job("arc-patch-first.db").await;
        let legacy = json!({
            "current_intent": "Before", "working": "Old",
            "rulings": [{
                "slug": "keep-this", "text": "Keep this", "status": "accepted",
                "rationale": "It matters", "provenance": ["cairn://p/cairn/old"]
            }],
            "open_questions": [{"question": "Still open?"}]
        });

        super::import_legacy_collections(&db, &job, &legacy)
            .await
            .unwrap();
        let mut stored_patch = legacy.clone();
        stored_patch["working"] = json!("Patched first");
        retain_prose_only(&mut stored_patch);
        assert!(stored_patch.get("rulings").is_none());

        let rendered = super::project(&db, &job, &stored_patch).await.unwrap();
        assert_eq!(rendered["working"], "Patched first");
        assert_eq!(rendered["rulings"][0]["slug"], "keep-this");
        assert_eq!(rendered["open_questions"][0]["question"], "Still open?");
    }

    #[tokio::test]
    async fn first_prose_replacement_imports_superseded_legacy_collections() {
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
        let rendered = super::project(&db, &job, &replacement).await.unwrap();
        assert_eq!(rendered["current_intent"], "After");
        assert_eq!(rendered["rulings"][0]["slug"], "survive-replacement");
        assert_eq!(rendered["open_questions"][0]["question"], "Also survive?");
    }

    #[tokio::test]
    async fn json_projection_is_prose_only_while_importing_legacy_collections() {
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
        assert_eq!(projected["rulings"].as_array().unwrap().len(), 1);
        assert_eq!(projected["open_questions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn derives_stable_valid_slug() {
        assert_eq!(
            derive_slug("The Arc's collections dissolve!", 0),
            "the-arc-s-collections-dissolve"
        );
        assert_eq!(derive_slug("123", 2), "ruling-3");
    }
}
