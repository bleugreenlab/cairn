//! Stored, replayable run batches owned by a thread database.

use serde_json::Value;

use super::db_store;

const TABLE: &str = "_cairn_procedures";

#[derive(Debug, Clone, PartialEq)]
pub struct Procedure {
    pub name: String,
    pub description: Option<String>,
    pub body: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

pub async fn list(thread_id: &str) -> Result<Vec<Procedure>, String> {
    let db = db_store::open(thread_id).await?;
    ensure_table(&db).await?;
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT name, description, body, created_at, updated_at FROM _cairn_procedures ORDER BY name",
                    (),
                )
                .await?;
            let mut procedures = Vec::new();
            while let Some(row) = rows.next().await? {
                procedures.push(decode_row(&row)?);
            }
            Ok(procedures)
        })
    })
    .await
    .map_err(db_store::sanitize_error)
}

pub async fn get(thread_id: &str, name: &str) -> Result<Option<Procedure>, String> {
    validate_name(name)?;
    let db = db_store::open(thread_id).await?;
    ensure_table(&db).await?;
    let name = name.to_owned();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(
            "SELECT name, description, body, created_at, updated_at FROM _cairn_procedures WHERE name = ?1",
            cairn_db::turso::params![name],
        ).await?;
            rows.next().await?.map(|row| decode_row(&row)).transpose()
        })
    })
    .await
    .map_err(db_store::sanitize_error)
}

pub async fn create(
    thread_id: &str,
    name: &str,
    description: Option<String>,
    body: Value,
) -> Result<Procedure, String> {
    validate_name(name)?;
    validate_body(&body)?;
    let db = db_store::open(thread_id).await?;
    ensure_table(&db).await?;
    let name = name.to_owned();
    let stored_name = name.clone();
    let encoded = serde_json::to_string(&body).map_err(|error| error.to_string())?;
    db.write(move |conn| {
        let stored_name = stored_name.clone();
        let description = description.clone();
        let encoded = encoded.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO _cairn_procedures (name, description, body) VALUES (?1, ?2, ?3)",
                cairn_db::turso::params![stored_name, description, encoded],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| map_write_error(error, &name))?;
    crate::orchestrator::wakes::conditions::notify_thread_db_written(thread_id);
    get(thread_id, name.as_str())
        .await?
        .ok_or_else(|| "Procedure was not stored".to_string())
}

pub async fn update(
    thread_id: &str,
    name: &str,
    body: Option<Value>,
    description: Option<Option<String>>,
) -> Result<Option<Procedure>, String> {
    validate_name(name)?;
    if let Some(body) = &body {
        validate_body(body)?;
    }
    let db = db_store::open(thread_id).await?;
    ensure_table(&db).await?;
    let lookup_name = name.to_owned();
    let encoded = body
        .map(|body| serde_json::to_string(&body))
        .transpose()
        .map_err(|error| error.to_string())?;
    let set_description = description.is_some();
    let description = description.flatten();
    let changed = db
        .write(move |conn| {
            let lookup_name = lookup_name.clone();
            let encoded = encoded.clone();
            let description = description.clone();
            Box::pin(async move {
                Ok(conn.execute(
                    "UPDATE _cairn_procedures SET body = COALESCE(?2, body), description = CASE WHEN ?3 THEN ?4 ELSE description END, updated_at = unixepoch() WHERE name = ?1",
                    cairn_db::turso::params![lookup_name, encoded, set_description, description],
                ).await?)
            })
        })
        .await
        .map_err(db_store::sanitize_error)?;
    crate::orchestrator::wakes::conditions::notify_thread_db_written(thread_id);
    if changed == 0 {
        Ok(None)
    } else {
        get(thread_id, name).await
    }
}

pub async fn delete(thread_id: &str, name: &str) -> Result<bool, String> {
    validate_name(name)?;
    let db = db_store::open(thread_id).await?;
    ensure_table(&db).await?;
    let name = name.to_owned();
    let deleted = db
        .write(move |conn| {
            let name = name.clone();
            Box::pin(async move {
                Ok(conn
                    .execute(
                        "DELETE FROM _cairn_procedures WHERE name = ?1",
                        cairn_db::turso::params![name],
                    )
                    .await?
                    > 0)
            })
        })
        .await
        .map_err(db_store::sanitize_error)?;
    crate::orchestrator::wakes::conditions::notify_thread_db_written(thread_id);
    Ok(deleted)
}

async fn ensure_table(db: &crate::storage::LocalDb) -> Result<(), String> {
    db.exclusive(|conn| {
        Box::pin(async move {
            conn.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (\
                 name TEXT PRIMARY KEY NOT NULL,\
                 description TEXT,\
                 body TEXT NOT NULL CHECK (json_valid(body) AND json_type(body) = 'object'),\
                 created_at INTEGER NOT NULL DEFAULT (unixepoch()),\
                 updated_at INTEGER NOT NULL DEFAULT (unixepoch())\
                 )"
            ))
            .await?;
            let has_description = conn
                .query(
                    "SELECT name FROM pragma_table_info('_cairn_procedures') WHERE name = 'description'",
                    (),
                )
                .await?
                .next()
                .await?
                .is_some();
            if !has_description {
                conn.execute(
                    "ALTER TABLE _cairn_procedures ADD COLUMN description TEXT",
                    (),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(db_store::sanitize_error)
}

fn decode_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<Procedure> {
    let body = row.get::<String>(2)?;
    let body = serde_json::from_str(&body).map_err(|error| {
        crate::storage::DbError::Row(format!("invalid stored procedure body: {error}"))
    })?;
    Ok(Procedure {
        name: row.get(0)?,
        description: row.get(1)?,
        body,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

pub fn validate_name(name: &str) -> Result<(), String> {
    if (1..=64).contains(&name.len())
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
    {
        Ok(())
    } else {
        Err("Procedure name must match `[a-z0-9][a-z0-9-]{0,63}`".to_string())
    }
}

/// Procedures persist the ordinary run batch envelope, but cannot persist
/// invocation-specific lifecycle controls or durable waits.
pub fn validate_body(body: &Value) -> Result<(), String> {
    let object = body
        .as_object()
        .ok_or_else(|| "Procedure body must be a run batch object".to_string())?;
    for forbidden in ["branch", "commit_msg", "conflict_markers_reason"] {
        if object.contains_key(forbidden) {
            return Err(format!("Procedure body cannot contain `{forbidden}`"));
        }
    }
    let commands = object
        .get("commands")
        .and_then(Value::as_array)
        .filter(|commands| !commands.is_empty())
        .ok_or_else(|| "Procedure body must contain a non-empty `commands` array".to_string())?;
    for (index, command) in commands.iter().enumerate() {
        let item = command
            .as_object()
            .ok_or_else(|| format!("commands[{index}] must be an object"))?;
        if item.contains_key("waitFor") || item.contains_key("repl") {
            return Err(format!("commands[{index}] cannot persist waitFor or repl"));
        }
        let kinds = ["command", "code", "target"]
            .iter()
            .filter(|key| item.get(**key).and_then(Value::as_str).is_some())
            .count();
        if kinds != 1 {
            return Err(format!(
                "commands[{index}] must contain exactly one of `command`, `code`, or `target`"
            ));
        }
        if item.contains_key("code") != item.get("interpreter").and_then(Value::as_str).is_some() {
            return Err(format!("commands[{index}] inline code requires `interpreter`, and `interpreter` requires code"));
        }
    }
    Ok(())
}

fn map_write_error(error: impl std::fmt::Display, name: &str) -> String {
    let message = db_store::sanitize_error(error);
    if message.contains("UNIQUE constraint failed") {
        format!("Procedure `{name}` already exists")
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initializes_and_migrates_the_resource_owned_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::storage::LocalDb::open(dir.path().join("thread.db"))
            .await
            .unwrap();

        ensure_table(&db).await.unwrap();
        assert!(db
            .query_text(
                "SELECT name FROM pragma_table_info('_cairn_procedures') WHERE name = 'description'",
                (),
            )
            .await
            .unwrap()
            .is_some());

        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute("DROP TABLE _cairn_procedures", ()).await?;
                conn.execute_batch(
                    "CREATE TABLE _cairn_procedures (\
                     name TEXT PRIMARY KEY NOT NULL,\
                     body TEXT NOT NULL,\
                     created_at INTEGER NOT NULL DEFAULT (unixepoch()),\
                     updated_at INTEGER NOT NULL DEFAULT (unixepoch()))",
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        ensure_table(&db).await.unwrap();
        assert!(db
            .query_text(
                "SELECT name FROM pragma_table_info('_cairn_procedures') WHERE name = 'description'",
                (),
            )
            .await
            .unwrap()
            .is_some());
    }

    #[test]
    fn validates_names_and_stored_batch_boundaries() {
        assert!(validate_name("collect-health-2").is_ok());
        assert!(validate_name("bad name").is_err());
        assert!(validate_body(&serde_json::json!({
            "commands": [{"code": "console.log(process.argv)", "interpreter": "typescript"}],
            "sequential": true
        }))
        .is_ok());
        assert!(
            validate_body(&serde_json::json!({"commands": [{"waitFor": {"duration": "1m"}}]}))
                .is_err()
        );
        assert!(validate_body(
            &serde_json::json!({"commands": [{"command": "true"}], "branch": "main"})
        )
        .is_err());
    }
}
