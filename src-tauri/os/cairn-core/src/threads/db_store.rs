//! Durable, app-hosted SQL storage owned by threads.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::UNIX_EPOCH,
};

use tokio::sync::RwLock;

use crate::{
    resources::{sql_policy, sql_schema::USER_TABLE_PREDICATE},
    storage::{LocalDb, MigrationRunner},
};

static CACHE: OnceLock<RwLock<HashMap<PathBuf, Arc<LocalDb>>>> = OnceLock::new();
#[cfg(any(test, feature = "test-utils"))]
static TEST_PATHS: OnceLock<std::sync::Mutex<HashMap<String, PathBuf>>> = OnceLock::new();

fn store_path(thread_id: &str) -> PathBuf {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(path) = TEST_PATHS
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .ok()
        .and_then(|paths| paths.get(thread_id).cloned())
    {
        return path;
    }
    cairn_common::paths::thread_db_path(thread_id)
}

#[cfg(any(test, feature = "test-utils"))]
pub fn use_test_path(thread_id: &str, path: PathBuf) {
    TEST_PATHS
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .insert(thread_id.to_string(), path);
}

fn cache() -> &'static RwLock<HashMap<PathBuf, Arc<LocalDb>>> {
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

pub async fn apply_write(
    thread_id: &str,
    statements: Vec<String>,
    params: Vec<Vec<cairn_db::turso::Value>>,
) -> Result<u64, String> {
    if statements.len() != params.len() {
        return Err(format!(
            "received {} parameter arrays for {} statements",
            params.len(),
            statements.len()
        ));
    }
    let db = open(thread_id).await?;
    let changes_schema = statements.iter().any(|statement| {
        matches!(
            sql_policy::sql_tokens(statement)
                .first()
                .map(String::as_str),
            Some("CREATE" | "DROP" | "ALTER")
        )
    });
    // Turso requires schema changes to run outside its concurrent transaction
    // mode. The resource still owns transaction control: callers cannot submit
    // BEGIN/COMMIT, and a mixed DDL/DML batch remains one atomic transaction.
    let changed = if changes_schema {
        db.exclusive(move |conn| {
            Box::pin(execute_statements(conn, statements.clone(), params.clone()))
        })
        .await
    } else {
        db.write(move |conn| Box::pin(execute_statements(conn, statements.clone(), params.clone())))
            .await
    }
    .map_err(sanitize_error)?;
    crate::orchestrator::wakes::conditions::notify_thread_db_written(thread_id);
    Ok(changed)
}

/// Mint the database that belongs to a newly-created thread and install every
/// current app-owned migration before its session can observe the thread.
pub(crate) async fn initialize_for_creation(
    thread_id: &str,
    main_db_path: &Path,
) -> Result<PathBuf, String> {
    validate_thread_id(thread_id)?;
    #[cfg(test)]
    use_test_path(
        thread_id,
        main_db_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("thread-databases")
            .join(format!("{thread_id}.db")),
    );
    #[cfg(not(test))]
    let _ = main_db_path;
    let path = store_path(thread_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(sanitize_error)?;
    }
    open_path(path.clone()).await?;
    Ok(path)
}

async fn execute_statements(
    conn: &cairn_db::turso::Connection,
    statements: Vec<String>,
    params: Vec<Vec<cairn_db::turso::Value>>,
) -> crate::storage::DbResult<u64> {
    let mut changed = 0;
    for (index, (statement, values)) in statements.iter().zip(params).enumerate() {
        changed += conn.execute(statement, values).await.map_err(|error| {
            crate::storage::DbError::Row(format!("statement {} failed: {error}", index + 1))
        })?;
    }
    Ok(changed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadDbCensus {
    pub tables: Vec<(String, i64)>,
    pub bytes: u64,
    pub last_write: Option<i64>,
}

pub async fn open(thread_id: &str) -> Result<Arc<LocalDb>, String> {
    validate_thread_id(thread_id)?;
    let path = store_path(thread_id);
    let dir = path
        .parent()
        .ok_or("Invalid thread database path")?
        .to_path_buf();
    std::fs::create_dir_all(&dir).map_err(sanitize_error)?;
    let ignore = dir.join(".gitignore");
    if !ignore.exists() {
        std::fs::write(&ignore, "*\n").map_err(sanitize_error)?;
    }

    open_path(path).await
}

pub async fn open_existing(thread_id: &str) -> Result<Option<Arc<LocalDb>>, String> {
    validate_thread_id(thread_id)?;
    let path = store_path(thread_id);
    if !path.is_file() {
        return Ok(None);
    }
    open_path(path).await.map(Some)
}

pub(crate) async fn open_path(path: PathBuf) -> Result<Arc<LocalDb>, String> {
    if let Some(db) = cache().read().await.get(&path).cloned() {
        return Ok(db);
    }
    let db = Arc::new(LocalDb::open(&path).await.map_err(sanitize_error)?);
    MigrationRunner::new(super::db_schema::THREAD_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .map_err(sanitize_error)?;
    let mut cache = cache().write().await;
    Ok(cache.entry(path).or_insert(db).clone())
}

pub async fn census(thread_id: &str) -> Option<ThreadDbCensus> {
    let db = open_existing(thread_id).await.ok().flatten()?;
    let tables = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT name FROM sqlite_schema WHERE {USER_TABLE_PREDICATE} ORDER BY name"
                        ),
                        (),
                    )
                    .await?;
                let mut names = Vec::new();
                while let Some(row) = rows.next().await? {
                    names.push(row.get::<String>(0)?);
                }
                let mut tables = Vec::with_capacity(names.len());
                for name in names {
                    let sql = format!("SELECT COUNT(*) FROM {}", quote_identifier(&name));
                    let mut count_rows = conn.query(&sql, ()).await?;
                    let count = count_rows.next().await?.expect("COUNT always returns a row").get(0)?;
                    tables.push((name, count));
                }
                Ok(tables)
            })
        })
        .await
        .ok()?;
    let metadata = std::fs::metadata(db.path()).ok()?;
    Some(ThreadDbCensus {
        tables,
        bytes: metadata.len(),
        last_write: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs() as i64),
    })
}

pub async fn remove(thread_id: &str) -> Result<(), String> {
    validate_thread_id(thread_id)?;
    let path = store_path(thread_id);
    remove_path(&path).await
}

pub(crate) async fn remove_path(path: &Path) -> Result<(), String> {
    cache().write().await.remove(path);
    for candidate in sidecar_paths(path) {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(sanitize_error(error)),
        }
    }
    Ok(())
}

fn sidecar_paths(path: &Path) -> [PathBuf; 4] {
    let display = path.to_string_lossy();
    [
        path.to_path_buf(),
        PathBuf::from(format!("{display}-wal")),
        PathBuf::from(format!("{display}-log")),
        PathBuf::from(format!("{display}-shm")),
    ]
}

pub fn validate_write_sql(sql: &str) -> Result<Vec<String>, String> {
    let statements = sql_policy::split_statements(sql);
    if statements.is_empty() {
        return Err("SQL write is empty".to_string());
    }
    for statement in &statements {
        let tokens = sql_policy::sql_tokens(statement);
        if tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("_cairn_procedures"))
        {
            return Err(
                "`_cairn_procedures` is resource-only; use cairn:~/procedures instead of raw SQL"
                    .to_string(),
            );
        }
        let first = tokens
            .first()
            .map(String::as_str)
            .ok_or_else(|| "SQL write is empty".to_string())?;
        match first {
            "ATTACH" | "DETACH" => return Err("A thread's store is confined to its own file; ATTACH and DETACH would reach another database".to_string()),
            "VACUUM" => return Err("VACUUM is not permitted because VACUUM INTO can write an arbitrary path".to_string()),
            "PRAGMA" => sql_policy::validate_schema_pragma(&tokens)?,
            "SELECT" | "WITH" | "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "CREATE"
            | "DROP" | "ALTER" | "ANALYZE" | "REINDEX" | "EXPLAIN" => {}
            other => return Err(format!("Unsupported SQL statement '{other}' for a thread database write")),
        }
        if matches!(first, "CREATE" | "DROP" | "ALTER") {
            if let Some(identifier) = sql_policy::sql_identifiers(statement)
                .into_iter()
                .find(|identifier| identifier.to_ascii_lowercase().starts_with("_cairn_"))
            {
                return Err(format!(
                    "`{identifier}` is app-contract schema — INSERT, UPDATE, and DELETE its rows, but its shape is Cairn's"
                ));
            }
        }
    }
    Ok(statements)
}

pub fn sanitize_error(error: impl std::fmt::Display) -> String {
    let mut message = error.to_string();
    let mut sensitive = vec![cairn_common::paths::thread_db_dir()];
    if let Some(home) = cairn_common::paths::os_home_dir() {
        sensitive.push(home);
    }
    sensitive.sort_by_key(|path| std::cmp::Reverse(path.as_os_str().len()));
    for path in sensitive {
        let path = path.to_string_lossy();
        if !path.is_empty() {
            message = message.replace(path.as_ref(), "<redacted>");
        }
    }
    message
}

fn validate_thread_id(thread_id: &str) -> Result<(), String> {
    if !thread_id.is_empty()
        && thread_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '~' | '.' | '-'))
    {
        Ok(())
    } else {
        Err("Invalid thread identifier".to_string())
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_policy_reserves_procedure_rows_for_the_resource() {
        for sql in [
            "INSERT INTO _cairn_procedures(name, body) VALUES ('x', '{}')",
            "DROP TABLE _CAIRN_PROCEDURES",
        ] {
            assert!(validate_write_sql(sql)
                .unwrap_err()
                .contains("cairn:~/procedures"));
        }
    }

    #[test]
    fn write_policy_confines_the_database_file() {
        for sql in ["ATTACH 'other.db' AS other", "DETACH other"] {
            assert!(validate_write_sql(sql)
                .unwrap_err()
                .contains("confined to its own file"));
        }
        assert!(validate_write_sql("VACUUM INTO '/tmp/export.db'")
            .unwrap_err()
            .contains("arbitrary path"));
        assert!(validate_write_sql("PRAGMA journal_mode=delete")
            .unwrap_err()
            .contains("schema PRAGMAs"));
    }

    #[test]
    fn write_policy_accepts_ddl_dml_and_multiple_statements() {
        let statements =
            validate_write_sql("CREATE TABLE x(id INTEGER); INSERT INTO x VALUES (1);").unwrap();
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn write_policy_protects_app_contract_ddl_in_every_identifier_form() {
        for sql in [
            "DROP TABLE _cairn_rulings",
            "DROP TABLE \"_cairn_rulings\"",
            "ALTER TABLE `_cairn_rulings` ADD COLUMN extra TEXT",
            "CREATE INDEX [_cairn_private] ON x(id)",
        ] {
            let error = validate_write_sql(sql).unwrap_err();
            assert!(error.contains("app-contract schema"), "{sql}: {error}");
            assert!(error.contains("INSERT, UPDATE, and DELETE"));
        }

        for sql in [
            "INSERT INTO _cairn_rulings(slug) VALUES ('x')",
            "UPDATE \"_cairn_rulings\" SET slug = 'x'",
            "DELETE FROM [_cairn_rulings]",
        ] {
            assert!(validate_write_sql(sql).is_ok(), "{sql}");
        }
    }

    #[tokio::test]
    async fn opening_a_preexisting_store_installs_app_schema_and_hides_it_from_census() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("thread.db");
        let preexisting = LocalDb::open(&path).await.unwrap();
        preexisting
            .execute_batch(
                "CREATE TABLE xcairn_notes(id INTEGER); INSERT INTO xcairn_notes VALUES (1)",
            )
            .await
            .unwrap();
        drop(preexisting);

        let thread_id = "census-preexisting";
        use_test_path(thread_id, path.clone());
        let db = open_path(path).await.unwrap();
        let names = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
                            (),
                        )
                        .await?;
                    let mut names = Vec::new();
                    while let Some(row) = rows.next().await? {
                        names.push(row.get::<String>(0)?);
                    }
                    Ok(names)
                })
            })
            .await
            .unwrap();
        assert!(names.iter().any(|name| name == "_cairn_rulings"));
        assert!(names.iter().any(|name| name == "_cairn_open_questions"));
        assert!(names.iter().any(|name| name == "cairn_schema_migrations"));
        assert!(names
            .iter()
            .any(|name| name == "__turso_internal_mvcc_meta"));

        let census = census(thread_id).await.unwrap();
        assert_eq!(census.tables, [("xcairn_notes".to_string(), 1)]);
    }

    #[tokio::test]
    async fn ruling_schema_enforces_contract_values() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_path(dir.path().join("thread.db")).await.unwrap();
        db.execute_script(
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance)
             VALUES ('valid-ruling','A ruling','accepted','Because','[\"cairn://p/cairn/1\"]')",
        )
        .await
        .unwrap();

        for sql in [
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES ('bad-status','x','pending','x','[\"uri\"]')",
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES ('empty-provenance','x','accepted','x','[]')",
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES ('valid-ruling','x','accepted','x','[\"uri\"]')",
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES ('bad_slug','x','accepted','x','[\"uri\"]')",
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance) VALUES ('-leading-hyphen','x','accepted','x','[\"uri\"]')",
        ] {
            assert!(db.execute_script(sql).await.is_err(), "accepted invalid SQL: {sql}");
        }
    }

    #[test]
    fn errors_hide_database_and_os_home_paths() {
        let raw = format!(
            "failed at {}/x.db and {}",
            cairn_common::paths::thread_db_dir().display(),
            cairn_common::paths::os_home_dir().unwrap().display()
        );
        let sanitized = sanitize_error(raw);
        assert!(!sanitized.contains(
            &cairn_common::paths::thread_db_dir()
                .to_string_lossy()
                .to_string()
        ));
        assert!(!sanitized.contains(
            &cairn_common::paths::os_home_dir()
                .unwrap()
                .to_string_lossy()
                .to_string()
        ));
    }
}
