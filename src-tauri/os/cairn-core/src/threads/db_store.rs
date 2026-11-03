//! Durable, app-hosted SQL storage owned by threads.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::UNIX_EPOCH,
};

use tokio::sync::RwLock;

use crate::{
    resources::{
        sql_policy,
        sql_schema::{quote_identifier, USER_TABLE_PREDICATE},
    },
    storage::{LocalDb, MigrationRunner},
};

static CACHE: OnceLock<RwLock<HashMap<PathBuf, Arc<LocalDb>>>> = OnceLock::new();
/// Stores whose rulings the boot import has turned into tenets this process.
/// Thread migration 0002 drops `_cairn_rulings`; a store still holding rows
/// that are not in this set keeps 0002 pending, so a failed or skipped import
/// never destroys a ruling it did not copy.
static IMPORTED: OnceLock<std::sync::RwLock<HashSet<PathBuf>>> = OnceLock::new();
#[cfg(any(test, feature = "test-utils"))]
static TEST_PATHS: OnceLock<std::sync::Mutex<HashMap<String, PathBuf>>> = OnceLock::new();

pub(crate) fn store_path(thread_id: &str) -> PathBuf {
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

/// What a structured statement does to one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowAction {
    Insert,
    Update,
    Delete,
}

impl RowAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// How a structured statement's row is addressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowKey {
    /// The caller names the key (an update or delete by URI).
    Known(String),
    /// An insert whose statement ends in `RETURNING <pk>`; the first value of
    /// its first returned row is the key, generated or not.
    Returned,
    /// The table has no single-column primary key, so the row has no URI.
    Unaddressed,
    /// A collection patch or delete scoped by `where=`: any number of rows,
    /// none named, and matching none is an answer (a count of 0), not a
    /// refusal.
    Matched,
}

/// The row (or, for [`RowKey::Matched`], the rows) a structured statement
/// touches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowTarget {
    pub table: String,
    pub action: RowAction,
    pub key: RowKey,
    /// The row's index in a bulk append's `rows`, which a failure names.
    pub position: Option<usize>,
}

/// One statement of a thread-database write, with its bound values. A
/// statement with a `row` is a structured row write; one without is SQL the
/// caller supplied.
#[derive(Debug, Clone)]
pub struct WriteStatement {
    pub sql: String,
    pub params: Vec<cairn_db::turso::Value>,
    pub row: Option<RowTarget>,
}

/// Whose store a write lands in, named both ways: by id for the store and by
/// project and name for the URIs and events it reports.
#[derive(Debug, Clone)]
pub struct WriteScope {
    pub thread_id: String,
    pub project_id: String,
    /// The canonical project slug, as it appears in `cairn://p/<project>`.
    pub project: String,
    pub thread_name: String,
}

impl WriteScope {
    /// `cairn://p/<project>/<thread>/db`.
    pub fn db_root(&self) -> String {
        format!(
            "{}/db",
            cairn_common::uri::build_thread_uri(&self.project, &self.thread_name)
        )
    }
}

/// One table-level effect of a committed write, as its `db-change` reports it.
/// `table` is `None` when a SQL statement could not be attributed, which a
/// consumer reads as "any table in this thread".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    pub table: Option<String>,
    pub action: &'static str,
    pub uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOutcome {
    pub changed: u64,
    /// What the write's `db-change` events announced. A single-row structured
    /// write: one entry naming the row. Several structured rows of one table
    /// and action (a bulk append), a `where=`-scoped write, and SQL: one entry
    /// per distinct (table, action), without a row.
    pub changes: Vec<RowChange>,
    /// Each structured statement's row URI, in statement order; `None` for a
    /// row without one (a keyless table's, or a `where=`-scoped write's).
    pub rows: Vec<Option<String>>,
}

/// The `table` every thread-database `db-change` carries. It is a pseudo-table
/// on purpose: the app's invalidators key on real table names, and a thread
/// table called `issues` must never trip the issue list's.
pub const THREAD_DB_CHANGE_TABLE: &str = "thread_db";

/// The single write path into a thread's database. Structured row writes and
/// SQL writes alike are policed here, statement by statement, run as one
/// transaction, and announced after commit: one `db-change` per effect and a
/// wake notice naming the rows the write could name.
pub async fn apply_write(
    scope: &WriteScope,
    statements: Vec<WriteStatement>,
    emitter: &dyn crate::services::EventEmitter,
) -> Result<WriteOutcome, String> {
    police(&statements)?;
    let db = open(&scope.thread_id).await?;
    let changes_schema = statements.iter().any(|statement| {
        matches!(
            sql_policy::sql_tokens(&statement.sql)
                .first()
                .map(String::as_str),
            Some("CREATE" | "DROP" | "ALTER")
        )
    });
    // Turso requires schema changes to run outside its concurrent transaction
    // mode. The resource still owns transaction control: callers cannot submit
    // BEGIN/COMMIT, and a mixed DDL/DML batch remains one atomic transaction.
    let executed = if changes_schema {
        let statements = statements.clone();
        db.exclusive(move |conn| Box::pin(execute_statements(conn, statements.clone())))
            .await
    } else {
        let statements = statements.clone();
        db.write(move |conn| Box::pin(execute_statements(conn, statements.clone())))
            .await
    }
    .map_err(sanitize_error)?;

    let root = scope.db_root();
    // Rows of one (table, action) written by several structured statements (a
    // bulk append) announce once, for the table: a page re-reads a table, not
    // a row, and a thousand events would each re-read it.
    let structured_rows = |table: &str, action: RowAction| {
        statements
            .iter()
            .filter_map(|statement| statement.row.as_ref())
            .filter(|target| target.table == table && target.action == action)
            .count()
    };
    let mut changes: Vec<RowChange> = Vec::new();
    let mut rows: Vec<Option<String>> = Vec::new();
    for (statement, result) in statements.iter().zip(&executed.per_statement) {
        let change = match &statement.row {
            Some(target) => {
                let key = match (&target.key, &result.returned_key) {
                    (RowKey::Known(key), _) => Some(key.clone()),
                    (RowKey::Returned, Some(key)) => Some(key.clone()),
                    _ => None,
                };
                let uri =
                    key.map(|key| cairn_common::uri::build_db_row_uri(&root, &target.table, &key));
                // A structured update or delete of one named row that matched
                // nothing changed nothing; it is refused rather than reported
                // or announced. A `where=`-scoped one reports its count.
                if result.changed == 0
                    && target.action != RowAction::Insert
                    && target.key != RowKey::Matched
                {
                    return Err(format!(
                        "No row at {}",
                        uri.unwrap_or_else(|| cairn_common::uri::build_db_table_uri(
                            &root,
                            &target.table
                        ))
                    ));
                }
                rows.push(uri.clone());
                RowChange {
                    table: Some(target.table.clone()),
                    action: target.action.as_str(),
                    uri: uri.filter(|_| structured_rows(&target.table, target.action) == 1),
                }
            }
            None => match sql_policy::statement_target(&statement.sql) {
                Some((table, action)) => RowChange {
                    table: Some(table),
                    action,
                    uri: None,
                },
                None if is_read_statement(&statement.sql) => continue,
                None => RowChange {
                    table: None,
                    action: "update",
                    uri: None,
                },
            },
        };
        if !changes.iter().any(|seen| {
            seen.table == change.table && seen.action == change.action && seen.uri == change.uri
        }) {
            changes.push(change);
        }
    }

    for change in &changes {
        let _ = emitter.emit(
            "db-change",
            serde_json::json!({
                "table": THREAD_DB_CHANGE_TABLE,
                "action": change.action,
                "projectId": scope.project_id,
                "threadId": scope.thread_id,
                "name": scope.thread_name,
                "dbTable": change.table,
                "rowUri": change.uri,
            }),
        );
    }
    let row_uris: Vec<String> = rows.iter().flatten().cloned().collect();
    crate::orchestrator::wakes::conditions::notify_thread_db_written(&scope.thread_id, &row_uris);
    Ok(WriteOutcome {
        changed: executed.changed,
        changes,
        rows,
    })
}

/// A statement that writes nothing a consumer could observe, so it earns no
/// `db-change`: a SELECT or EXPLAIN carried in a write batch, or a schema
/// PRAGMA (the policy admits only read-only ones).
fn is_read_statement(sql: &str) -> bool {
    matches!(
        sql_policy::sql_tokens(sql).first().map(String::as_str),
        Some("SELECT" | "EXPLAIN" | "PRAGMA")
    )
}

/// Run the write policy over every statement of a write: the check
/// [`apply_write`] makes before it opens a transaction, exposed so a dry run
/// refuses exactly what the real write would.
pub fn police(statements: &[WriteStatement]) -> Result<(), String> {
    if statements.is_empty() {
        return Err("SQL write is empty".to_string());
    }
    statements
        .iter()
        .try_for_each(|statement| validate_write_statement(&statement.sql))
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

struct StatementResult {
    changed: u64,
    /// The key a `RETURNING` insert reported, as URI segment text.
    returned_key: Option<String>,
}

struct Executed {
    changed: u64,
    per_statement: Vec<StatementResult>,
}

async fn execute_statements(
    conn: &cairn_db::turso::Connection,
    statements: Vec<WriteStatement>,
) -> crate::storage::DbResult<Executed> {
    let mut executed = Executed {
        changed: 0,
        per_statement: Vec::with_capacity(statements.len()),
    };
    for (index, statement) in statements.into_iter().enumerate() {
        let position = statement.row.as_ref().and_then(|target| target.position);
        let failed = |error: cairn_db::turso::Error| {
            crate::storage::DbError::Row(match position {
                Some(position) => format!("rows[{position}] failed: {error}"),
                None => format!("statement {} failed: {error}", index + 1),
            })
        };
        let returning = statement
            .row
            .as_ref()
            .is_some_and(|target| target.key == RowKey::Returned);
        let result = if returning {
            let mut rows = conn
                .query(&statement.sql, statement.params)
                .await
                .map_err(failed)?;
            let mut changed = 0;
            let mut returned_key = None;
            while let Some(row) = rows.next().await.map_err(failed)? {
                if returned_key.is_none() {
                    returned_key =
                        crate::resources::db_rows::pk_segment(&row.get_value(0).map_err(failed)?);
                }
                changed += 1;
            }
            StatementResult {
                changed,
                returned_key,
            }
        } else {
            StatementResult {
                changed: conn
                    .execute(&statement.sql, statement.params)
                    .await
                    .map_err(failed)?,
                returned_key: None,
            }
        };
        executed.changed += result.changed;
        executed.per_statement.push(result);
    }
    Ok(executed)
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
    // A first open creates the file and runs its migrations. Two of those
    // racing on one path would each open the file and each apply 0001, so
    // first opens are serialized and the cache re-checked under the lock.
    static OPENING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _opening = OPENING.lock().await;
    if let Some(db) = cache().read().await.get(&path).cloned() {
        return Ok(db);
    }
    let db = Arc::new(LocalDb::open(&path).await.map_err(sanitize_error)?);
    let migrations: Vec<_> = match unimported_rulings(&db, &path)
        .await
        .map_err(sanitize_error)?
    {
        Some(count) => {
            tracing::warn!(
                path = %sanitize_error(path.display()),
                count,
                "thread store still holds {count} rulings not yet imported as tenets; thread migration 0002 left pending"
            );
            super::db_schema::THREAD_MIGRATIONS
                .iter()
                .filter(|migration| migration.version() != super::db_schema::RULINGS_TO_TENETS)
                .cloned()
                .collect()
        }
        None => super::db_schema::THREAD_MIGRATIONS.to_vec(),
    };
    MigrationRunner::new(migrations)
        .run(&db)
        .await
        .map_err(sanitize_error)?;
    let mut cache = cache().write().await;
    Ok(cache.entry(path).or_insert(db).clone())
}

fn imported() -> &'static std::sync::RwLock<HashSet<PathBuf>> {
    IMPORTED.get_or_init(|| std::sync::RwLock::new(HashSet::new()))
}

/// Record that the store at `path` has had its rulings imported as tenets, so
/// its next open may apply thread migration 0002. A store already open in this
/// process keeps the schema it opened with until the next process.
pub(crate) fn mark_rulings_imported(path: &Path) {
    imported()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(path.to_path_buf());
}

fn rulings_imported(path: &Path) -> bool {
    imported()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(path)
}

/// How many rulings the store at `path` holds that thread migration 0002 would
/// destroy uncopied: `Some(n)` while 0002 is unapplied, `_cairn_rulings` has
/// rows, and this process has not imported them; `None` when 0002 may run.
async fn unimported_rulings(db: &LocalDb, path: &Path) -> crate::storage::DbResult<Option<i64>> {
    if rulings_imported(path) {
        return Ok(None);
    }
    let state = read_legacy_state(db, false).await?;
    Ok((!state.rulings_dropped && state.ruling_count > 0).then_some(state.ruling_count))
}

/// One row of a pre-tenet thread's `_cairn_rulings`, as the importer reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyRulingRow {
    pub id: i64,
    pub slug: String,
    pub text: String,
    pub status: String,
    pub rationale: String,
    pub provenance: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// What a thread store holds that bears on the ruling import.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LegacyStore {
    /// `_cairn_rulings` still exists.
    pub rulings_table: bool,
    pub ruling_count: i64,
    /// Every ruling row, oldest id first. Empty unless asked for.
    pub rulings: Vec<LegacyRulingRow>,
    /// A pre-cutover arc import copied the arc's embedded rulings into
    /// `_cairn_rulings`. A marker written after the cutover copied only open
    /// questions and does not count.
    pub arc_rulings_copied: bool,
    /// Thread migration 0002 has run.
    pub rulings_dropped: bool,
}

async fn read_legacy_state(db: &LocalDb, with_rows: bool) -> crate::storage::DbResult<LegacyStore> {
    db.read(move |conn| {
        Box::pin(async move {
            async fn table_exists(
                conn: &cairn_db::turso::Connection,
                name: &str,
            ) -> crate::storage::DbResult<bool> {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                        [name],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            }
            let mut state = LegacyStore::default();
            if table_exists(conn, "cairn_schema_migrations").await? {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM cairn_schema_migrations WHERE version = ?1",
                        [super::db_schema::RULINGS_TO_TENETS],
                    )
                    .await?;
                state.rulings_dropped = rows.next().await?.is_some();
            }
            if table_exists(conn, "_cairn_arc_imports").await? {
                // Before thread migration 0003 the column does not exist, and
                // every marker is a pre-cutover one that copied rulings.
                let mut columns = conn
                    .query(
                        "SELECT 1 FROM pragma_table_info('_cairn_arc_imports') WHERE name = 'rulings_copied'",
                        (),
                    )
                    .await?;
                let scoped = columns.next().await?.is_some();
                drop(columns);
                let sql = if scoped {
                    "SELECT rulings_copied FROM _cairn_arc_imports WHERE id = 1"
                } else {
                    "SELECT 1 FROM _cairn_arc_imports WHERE id = 1"
                };
                let mut rows = conn.query(sql, ()).await?;
                state.arc_rulings_copied = match rows.next().await? {
                    Some(row) => row.get::<i64>(0)? != 0,
                    None => false,
                };
            }
            state.rulings_table = table_exists(conn, "_cairn_rulings").await?;
            if !state.rulings_table {
                return Ok(state);
            }
            let mut rows = conn.query("SELECT COUNT(*) FROM _cairn_rulings", ()).await?;
            state.ruling_count = match rows.next().await? {
                Some(row) => row.get(0)?,
                None => 0,
            };
            drop(rows);
            if with_rows {
                let mut rows = conn
                    .query(
                        "SELECT id, slug, text, status, rationale, provenance, created_at, updated_at
                         FROM _cairn_rulings ORDER BY id",
                        (),
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    state.rulings.push(LegacyRulingRow {
                        id: row.get(0)?,
                        slug: row.get(1)?,
                        text: row.get(2)?,
                        status: row.get(3)?,
                        rationale: row.get(4)?,
                        provenance: row.get(5)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                    });
                }
            }
            Ok(state)
        })
    })
    .await
}

/// Read what the thread store at `path` holds for the ruling import, without
/// creating, migrating, or caching it. `None` when no store file exists.
///
/// A store already open in this process is read through its cached handle;
/// otherwise the file is opened raw and the handle dropped before returning,
/// so the guarded open in [`open_path`] is still the first to cache it.
pub(crate) async fn inspect_legacy(path: &Path) -> Result<Option<LegacyStore>, String> {
    if !path.is_file() {
        return Ok(None);
    }
    let cached = cache().read().await.get(path).cloned();
    let db = match cached {
        Some(db) => db,
        None => Arc::new(LocalDb::open(path).await.map_err(sanitize_error)?),
    };
    let state = read_legacy_state(&db, true).await.map_err(sanitize_error)?;
    drop(db);
    Ok(Some(state))
}

/// Stage a thread store at the app schema as of `through` (inclusive),
/// uncached, the way an older build left it on disk.
#[cfg(any(test, feature = "test-utils"))]
pub async fn stage_store_for_test(path: &Path, through: &str) -> LocalDb {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let db = LocalDb::open(path).await.unwrap();
    let migrations: Vec<_> = super::db_schema::THREAD_MIGRATIONS
        .iter()
        .filter(|migration| migration.version() <= through)
        .cloned()
        .collect();
    MigrationRunner::new(migrations).run(&db).await.unwrap();
    db
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

/// Split SQL a caller supplied into statements, refusing an empty write. The
/// per-statement policy runs inside [`apply_write`] (and [`police`]).
pub fn split_write_sql(sql: &str) -> Result<Vec<String>, String> {
    let statements = sql_policy::split_statements(sql);
    if statements.is_empty() {
        return Err("SQL write is empty".to_string());
    }
    Ok(statements)
}

/// The write policy for one statement, whoever composed it. Identifiers are
/// matched in every SQLite quoting form, so a generated statement (which
/// always quotes) is held to the same rule as hand-written SQL.
fn validate_write_statement(statement: &str) -> Result<(), String> {
    // Before the app-contract DDL check, so `DROP TABLE _cairn_rulings` is told
    // where rulings went rather than that the table's shape is Cairn's.
    sql_policy::refuse_retired_rulings(statement)?;
    let identifiers = sql_policy::sql_identifiers(statement);
    let tokens = sql_policy::sql_tokens(statement);
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
        if let Some(identifier) = identifiers
            .into_iter()
            .find(|identifier| identifier.to_ascii_lowercase().starts_with("_cairn_"))
        {
            return Err(format!(
                "`{identifier}` is app-contract schema — INSERT, UPDATE, and DELETE its rows, but its shape is Cairn's"
            ));
        }
    }
    Ok(())
}

/// [`split_write_sql`] then the per-statement policy: the whole check on SQL
/// text, for tests and callers that hold no statements yet.
#[cfg(test)]
fn validate_write_sql(sql: &str) -> Result<Vec<String>, String> {
    let statements = split_write_sql(sql)?;
    statements
        .iter()
        .try_for_each(|statement| validate_write_statement(statement))?;
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

pub(crate) fn is_valid_thread_id(thread_id: &str) -> bool {
    validate_thread_id(thread_id).is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;

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
            "DROP TABLE _cairn_open_questions",
            "DROP TABLE \"_cairn_open_questions\"",
            "ALTER TABLE `_cairn_open_questions` ADD COLUMN extra TEXT",
            "CREATE INDEX [_cairn_private] ON x(id)",
        ] {
            let error = validate_write_sql(sql).unwrap_err();
            assert!(error.contains("app-contract schema"), "{sql}: {error}");
            assert!(error.contains("INSERT, UPDATE, and DELETE"));
        }

        for sql in [
            "INSERT INTO _cairn_open_questions(question) VALUES ('x')",
            "UPDATE \"_cairn_open_questions\" SET question = 'x'",
            "DELETE FROM [_cairn_open_questions]",
        ] {
            assert!(validate_write_sql(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn write_policy_points_retired_ruling_sql_at_tenets() {
        for sql in [
            "INSERT INTO _cairn_rulings(slug) VALUES ('x')",
            "UPDATE \"_cairn_rulings\" SET slug = 'x'",
            "DELETE FROM [_CAIRN_RULINGS]",
            "DROP TABLE _cairn_rulings",
        ] {
            let error = validate_write_sql(sql).unwrap_err();
            assert!(
                error.contains(sql_policy::RULINGS_ARE_TENETS),
                "{sql}: {error}"
            );
        }
        // A string literal that merely mentions the table is data, not a reference.
        assert!(validate_write_sql("INSERT INTO notes(body) VALUES ('_cairn_rulings')").is_ok());
    }

    async fn insert_legacy_ruling(db: &LocalDb, slug: &str) {
        db.execute_script(format!(
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance)
             VALUES ('{slug}','A ruling','accepted','Because','[\"cairn://p/cairn/1\"]')"
        ))
        .await
        .unwrap();
    }

    async fn applied_versions(db: &LocalDb) -> Vec<String> {
        db.query_all(
            "SELECT version FROM cairn_schema_migrations ORDER BY version",
            (),
            |row| crate::storage::RowExt::text(row, 0),
        )
        .await
        .unwrap()
    }

    /// Every registered thread migration's version, in order, less `held`. The
    /// guard tests compare against this rather than a literal list so a new
    /// thread migration does not read as a guard regression.
    fn every_version_but(held: &[&str]) -> Vec<String> {
        crate::threads::db_schema::THREAD_MIGRATIONS
            .iter()
            .map(|migration| migration.version())
            .filter(|version| !held.contains(version))
            .map(str::to_owned)
            .collect()
    }

    #[tokio::test]
    async fn guard_holds_0002_on_a_store_with_unimported_rulings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held.db");
        let staged = stage_store_for_test(&path, "0001").await;
        insert_legacy_ruling(&staged, "kept-ruling").await;
        drop(staged);

        let db = open_path(path.clone()).await.unwrap();
        // Only 0002 is held; the migrations after it still apply.
        assert_eq!(
            applied_versions(&db).await,
            every_version_but(&[crate::threads::db_schema::RULINGS_TO_TENETS])
        );
        let state = inspect_legacy(&path).await.unwrap().unwrap();
        assert!(state.rulings_table && !state.rulings_dropped);
        assert_eq!(state.rulings.len(), 1);
        assert_eq!(state.rulings[0].slug, "kept-ruling");
    }

    #[tokio::test]
    async fn guard_applies_0002_once_the_store_is_marked_imported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("imported.db");
        let staged = stage_store_for_test(&path, "0001").await;
        insert_legacy_ruling(&staged, "imported-ruling").await;
        drop(staged);

        mark_rulings_imported(&path);
        let db = open_path(path.clone()).await.unwrap();
        assert_eq!(applied_versions(&db).await, every_version_but(&[]));
        let state = inspect_legacy(&path).await.unwrap().unwrap();
        assert!(!state.rulings_table && state.rulings_dropped);
    }

    #[tokio::test]
    async fn guard_applies_0002_to_an_empty_rulings_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.db");
        drop(stage_store_for_test(&path, "0001").await);

        let db = open_path(path).await.unwrap();
        assert_eq!(applied_versions(&db).await, every_version_but(&[]));
    }

    #[tokio::test]
    async fn a_pre_cutover_marker_vouches_for_copied_rulings_and_a_new_one_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old-marker.db");
        let staged = stage_store_for_test(&old, "0001").await;
        staged
            .execute_script("INSERT INTO _cairn_arc_imports(id) VALUES (1)")
            .await
            .unwrap();
        drop(staged);
        assert!(
            inspect_legacy(&old)
                .await
                .unwrap()
                .unwrap()
                .arc_rulings_copied
        );
        // 0003 keeps an existing marker's meaning.
        drop(open_path(old.clone()).await.unwrap());
        assert!(
            inspect_legacy(&old)
                .await
                .unwrap()
                .unwrap()
                .arc_rulings_copied
        );

        let new = dir.path().join("new-marker.db");
        let db = open_path(new.clone()).await.unwrap();
        db.execute_script("INSERT INTO _cairn_arc_imports(id, rulings_copied) VALUES (1, 0)")
            .await
            .unwrap();
        assert!(
            !inspect_legacy(&new)
                .await
                .unwrap()
                .unwrap()
                .arc_rulings_copied
        );
    }

    #[tokio::test]
    async fn inspecting_a_missing_store_creates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.db");
        assert_eq!(inspect_legacy(&path).await.unwrap(), None);
        assert!(!path.exists());
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
        assert!(!names.iter().any(|name| name == "_cairn_rulings"));
        assert!(names.iter().any(|name| name == "_cairn_open_questions"));
        assert!(names.iter().any(|name| name == "cairn_schema_migrations"));
        assert!(names
            .iter()
            .any(|name| name == "__turso_internal_mvcc_meta"));

        let census = census(thread_id).await.unwrap();
        assert_eq!(census.tables, [("xcairn_notes".to_string(), 1)]);
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
