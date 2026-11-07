//! Compact schema discovery shared by the application and thread databases.

use cairn_db::storage::{DbResult, LocalDb};

pub(crate) const OVERVIEW_DEFAULT_TABLES: usize = 40;
pub(crate) const OVERVIEW_MAX_TABLES: usize = 200;
const OVERVIEW_MAX_COLUMNS: usize = 8;

/// The tables a database's catalog lists: everything but engine storage and
/// the `_cairn_`-prefixed app contract. In a thread database that includes the
/// app-known starter record (`intent`, `journal`), which is addressable through
/// the row grammar like any table the thread made.
///
/// Prefix comparison is deliberate: `_` is a wildcard in SQL `LIKE`, which
/// would also hide unrelated names such as `xcairn_notes`.
pub(crate) const CATALOG_TABLE_PREDICATE: &str = "type = 'table'
  AND substr(name, 1, 7) <> 'sqlite_'
  AND substr(name, 1, 17) <> '__turso_internal_'
  AND substr(name, 1, 7) <> '_cairn_'
  AND name <> 'cairn_schema_migrations'";

/// The tables a thread made for itself: the catalog less the starter record,
/// which the seed renders in sections of its own.
pub(crate) const USER_TABLE_PREDICATE: &str = "type = 'table'
  AND substr(name, 1, 7) <> 'sqlite_'
  AND substr(name, 1, 17) <> '__turso_internal_'
  AND substr(name, 1, 7) <> '_cairn_'
  AND name <> 'cairn_schema_migrations'
  AND name NOT IN ('intent', 'journal')";

/// Whether `name` is schema Cairn owns: the `_cairn_` contract, the migration
/// ledger, and the app-known starter record.
pub(crate) fn is_cairn_owned(name: &str) -> bool {
    name.starts_with("_cairn_")
        || name == "cairn_schema_migrations"
        || crate::threads::db_schema::APP_TABLES.contains(&name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OverviewParams {
    pub table: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

/// Render the database catalog as recreatable SQL. SQLite's catalog retains the
/// original object definitions; ordering here makes the projection stable and
/// places tables before objects that depend on them.
pub(crate) async fn render_source(db: &LocalDb) -> DbResult<String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT schema.type, schema.name, schema.sql \
                     FROM sqlite_schema AS schema \
                     WHERE schema.sql IS NOT NULL \
                       AND substr(schema.name, 1, 7) <> 'sqlite_' \
                       AND substr(schema.name, 1, 17) <> '__turso_internal_' \
                     ORDER BY CASE schema.type \
                         WHEN 'table' THEN 0 WHEN 'view' THEN 1 \
                         WHEN 'index' THEN 2 WHEN 'trigger' THEN 3 ELSE 4 END, \
                         schema.name, schema.type",
                    (),
                )
                .await?;
            let mut objects = Vec::new();
            while let Some(row) = rows.next().await? {
                let kind = row.get::<String>(0)?;
                let name = row.get::<String>(1)?;
                let sql = row.get::<String>(2)?;
                let owner = if is_cairn_owned(&name) {
                    "cairn-owned"
                } else {
                    "agent-owned"
                };
                objects.push(format!(
                    "-- {owner} {kind}: {name}\n{};",
                    sql.trim().trim_end_matches(';')
                ));
            }
            Ok(if objects.is_empty() {
                "-- No schema objects".to_string()
            } else {
                objects.join("\n\n")
            })
        })
    })
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaOverview {
    pub body: String,
    pub total_tables: usize,
    pub shown_tables: usize,
}

/// Describe the user tables a page selects, without scanning any of them.
///
/// A table's size is its rowid high-water (`max(rowid)`, a seek to the last
/// b-tree entry), never `COUNT(*)`: on the production file a count is a full
/// walk of millions of rows through a cold page cache. Each table is described
/// in its own short read, so no connection or snapshot is held across tables.
pub(crate) async fn render_overview(
    db: &LocalDb,
    params: &OverviewParams,
) -> DbResult<SchemaOverview> {
    let mut catalog = read_catalog(db).await?;
    let total_tables = catalog.tables.len();
    let selected: Vec<String> = if let Some(table) = &params.table {
        catalog
            .tables
            .into_iter()
            .filter(|name| name == table)
            .collect()
    } else {
        catalog
            .tables
            .into_iter()
            .skip(params.offset)
            .take(params.limit)
            .collect()
    };

    let detail = params.table.is_some();
    // The newest storage census's live-row estimates, from one stored row; a
    // database with no census (a thread's, or one too young) shows none.
    let census = cairn_db::storage::census::latest_table_rows(db)
        .await
        .ok()
        .flatten();
    let mut rendered = Vec::with_capacity(selected.len());
    for table in &selected {
        let indexes = catalog.indexes.remove(table).unwrap_or_default();
        let counted = census
            .as_ref()
            .and_then(|c| c.rows.get(table).map(|rows| (*rows, c.taken_at)));
        rendered.extend(describe_table(db, table.clone(), indexes, detail, counted).await?);
    }
    if let Some(table) = &params.table {
        if selected.is_empty() {
            rendered.push(format!("Table '{table}' not found"));
        }
    }
    Ok(SchemaOverview {
        body: if rendered.is_empty() {
            "(no user tables)".to_string()
        } else {
            rendered.join("\n")
        },
        total_tables: if detail { selected.len() } else { total_tables },
        shown_tables: selected.len(),
    })
}

type IndexEntry = (String, Option<String>);

struct Catalog {
    /// Listed tables: the app-known starter record first, then the rest by
    /// name.
    tables: Vec<String>,
    /// Every index in the catalog by its table, each list ordered by index
    /// name. Read from `sqlite_schema` itself so no kind of index (non-unique,
    /// partial, expression) is filtered out; constraint autoindexes have no
    /// SQL text.
    indexes: std::collections::HashMap<String, Vec<IndexEntry>>,
}

/// The user tables and every index, in one statement outside any transaction.
async fn read_catalog(db: &LocalDb) -> DbResult<Catalog> {
    let objects = db
        .query_all(
            format!(
                "SELECT type, name, tbl_name, sql FROM sqlite_schema \
                 WHERE ({CATALOG_TABLE_PREDICATE}) OR type = 'index' \
                 ORDER BY name"
            ),
            (),
            |row| {
                Ok((
                    row.get::<String>(0)?,
                    row.get::<String>(1)?,
                    row.get::<String>(2)?,
                    row.get::<Option<String>>(3)?,
                ))
            },
        )
        .await?;
    let mut catalog = Catalog {
        tables: Vec::new(),
        indexes: std::collections::HashMap::new(),
    };
    for (kind, name, table, sql) in objects {
        if kind == "index" {
            catalog.indexes.entry(table).or_default().push((name, sql));
        } else {
            catalog.tables.push(name);
        }
    }
    // Stable, so each half keeps its name order.
    catalog
        .tables
        .sort_by_key(|name| !crate::threads::db_schema::APP_TABLES.contains(&name.as_str()));
    Ok(catalog)
}

/// The statement that sizes a table: an index-only seek to the last rowid.
fn rowid_high_water_sql(quoted_table: &str) -> String {
    format!("SELECT max(rowid) FROM {quoted_table}")
}

/// The size note beside a table: its rowid high-water, and the census's
/// live-row estimate with the census's date when there is one. The high-water
/// is an upper bound that drifts above the live rows as evictors delete.
fn size_note(high_water: Option<i64>, counted: Option<(i64, i64)>) -> String {
    let Some(rowid) = high_water else {
        return " — empty".to_string();
    };
    match counted {
        Some((rows, taken_at)) => {
            let date = chrono::DateTime::from_timestamp(taken_at, 0).map_or_else(
                || taken_at.to_string(),
                |at| at.format("%Y-%m-%d").to_string(),
            );
            format!(" — max rowid {rowid}, ~{rows} rows as of {date}")
        }
        None => format!(" — max rowid {rowid}"),
    }
}

/// One table's overview lines, read in a read of its own. `counted` is the
/// storage census's `(rows, taken_at)` for the table.
async fn describe_table(
    db: &LocalDb,
    table: String,
    indexes: Vec<IndexEntry>,
    detail: bool,
    counted: Option<(i64, i64)>,
) -> DbResult<Vec<String>> {
    db.read(move |conn| {
        Box::pin(async move {
            let quoted = quote_identifier(&table);
            let mut names = Vec::new();
            let mut columns = Vec::new();
            {
                let mut column_rows = conn
                    .query(&format!("PRAGMA table_info({quoted})"), ())
                    .await?;
                while let Some(row) = column_rows.next().await? {
                    let name = row.get::<String>(1)?;
                    let kind = row.get::<String>(2)?;
                    let not_null = row.get::<i64>(3)? != 0;
                    let default = row.get::<Option<String>>(4)?;
                    let primary_key = row.get::<i64>(5)? != 0;
                    let mut column = if kind.is_empty() {
                        name.clone()
                    } else {
                        format!("{name} {kind}")
                    };
                    if primary_key {
                        column.push_str(" pk");
                    }
                    if not_null {
                        column.push_str(" not null");
                    }
                    if let Some(default) = default {
                        column.push_str(&format!(" default {default}"));
                    }
                    names.push(name);
                    columns.push(column);
                }
            }
            let omitted = columns.len().saturating_sub(OVERVIEW_MAX_COLUMNS);
            if !detail && omitted > 0 {
                columns.truncate(OVERVIEW_MAX_COLUMNS);
                columns.push(format!("… {omitted} more → ?table={table}"));
            }

            // A declared column named `rowid` shadows the rowid, and its max
            // would be a scan of an unindexed column; such a table goes
            // unsized rather than scanned.
            let size = if names.iter().any(|name| name.eq_ignore_ascii_case("rowid")) {
                String::new()
            } else {
                let mut rows = conn.query(rowid_high_water_sql(&quoted), ()).await?;
                let high_water = match rows.next().await? {
                    Some(row) => row.get::<Option<i64>>(0)?,
                    None => None,
                };
                size_note(high_water, counted)
            };
            let owner = if is_cairn_owned(&table) {
                " · cairn-owned shape, rows are yours"
            } else {
                ""
            };
            let mut lines = vec![format!("{table}({}){size}{owner}", columns.join(", "))];

            if detail {
                for (name, sql) in indexes {
                    lines.push(format!("  {}", render_index(conn, &name, sql).await?));
                }
            } else if !indexes.is_empty() {
                let noun = if indexes.len() == 1 {
                    "index"
                } else {
                    "indexes"
                };
                lines.push(format!("  {} {noun} → ?table={table}", indexes.len()));
            }
            Ok(lines)
        })
    })
    .await
}

/// The overview as one JSON document, for a program that lays a database out
/// (the app's database browser) rather than a reader. The listing names each
/// table with its size and primary key; with `table`, that table carries every
/// column (declared type, nullability, default, whether it is the key, whether
/// SQLite generates it, whether an insert must name it) and every index.
/// Columns are computed by the row engine's own [`TableShape`], so the document
/// says exactly what a row write will be held to. A table's size is its rowid
/// high-water, as in [`render_overview`] and for the same reason, never a
/// count; it is null for an empty table and for one a declared `rowid` column
/// shadows. `root` is the store's canonical URI, and `writable` whether it takes
/// row writes at all.
///
/// A writable store is a thread's, which is small and bounded, so each of its
/// tables also carries `rows`, the exact count, in the same short read: a
/// rowid high-water is no size at all for a table a service rewrites. The
/// application database (never writable) is never counted.
pub(crate) async fn overview_document(
    db: &LocalDb,
    params: &OverviewParams,
    root: &str,
    writable: bool,
) -> DbResult<SchemaOverview> {
    let mut catalog = read_catalog(db).await?;
    let total_tables = catalog.tables.len();
    let detail = params.table.is_some();
    let selected: Vec<String> = match &params.table {
        Some(table) => catalog
            .tables
            .into_iter()
            .filter(|name| name == table)
            .collect(),
        None => catalog
            .tables
            .into_iter()
            .skip(params.offset)
            .take(params.limit)
            .collect(),
    };
    let mut tables = Vec::with_capacity(selected.len());
    for table in &selected {
        let indexes = catalog.indexes.remove(table).unwrap_or_default();
        if let Some(entry) = table_document(db, table.clone(), indexes, detail, writable).await? {
            tables.push(entry);
        }
    }
    let shown_tables = tables.len();
    let total = if detail { shown_tables } else { total_tables };
    let document = serde_json::json!({
        "uri": root,
        "writable": writable,
        "total": total,
        "offset": if detail { 0 } else { params.offset },
        "limit": params.limit,
        "tables": tables,
    });
    Ok(SchemaOverview {
        body: document.to_string(),
        total_tables: total,
        shown_tables,
    })
}

/// One table's entry in the overview document, read in a read of its own.
/// `count` adds the table's exact `rows`.
async fn table_document(
    db: &LocalDb,
    table: String,
    indexes: Vec<IndexEntry>,
    detail: bool,
    count: bool,
) -> DbResult<Option<serde_json::Value>> {
    use super::db_rows::TableShape;
    use serde_json::{json, Value};

    db.read(move |conn| {
        Box::pin(async move {
            let Some(shape) = TableShape::load(conn, &table).await? else {
                return Ok(None);
            };
            let shadowed = shape
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case("rowid"));
            let max_rowid = if shadowed {
                None
            } else {
                let mut rows = conn
                    .query(rowid_high_water_sql(&quote_identifier(&table)), ())
                    .await?;
                match rows.next().await? {
                    Some(row) => row.get::<Option<i64>>(0)?,
                    None => None,
                }
            };
            let mut entry = json!({
                "name": table,
                "maxRowid": max_rowid,
                "primaryKey": shape.primary_key().map(|column| column.name.clone()),
            });
            if is_cairn_owned(&table) {
                entry["cairnOwned"] = json!(true);
            }
            if count {
                let sql = format!("SELECT count(*) FROM {}", quote_identifier(&table));
                let mut rows = conn.query(sql, ()).await?;
                let counted = match rows.next().await? {
                    Some(row) => row.get::<i64>(0)?,
                    None => 0,
                };
                entry["rows"] = json!(counted);
            }
            if detail {
                let columns: Vec<Value> = shape
                    .columns
                    .iter()
                    .map(|column| {
                        json!({
                            "name": column.name,
                            "type": column.decl_type,
                            "notNull": column.not_null,
                            "default": column.default,
                            "primaryKey": column.pk > 0,
                            "generated": shape.is_rowid_alias(column),
                            "requiredForInsert": shape.required_for_insert(column),
                        })
                    })
                    .collect();
                let mut rendered = Vec::with_capacity(indexes.len());
                for (name, sql) in indexes {
                    rendered.push(Value::String(render_index(conn, &name, sql).await?));
                }
                entry["columns"] = Value::Array(columns);
                entry["indexes"] = Value::Array(rendered);
            }
            Ok(Some(entry))
        })
    })
    .await
}

/// One index as `[unique] [partial] index NAME (TERMS) [WHERE PREDICATE]`.
async fn render_index(
    conn: &cairn_db::storage::TrackedConnection,
    name: &str,
    sql: Option<String>,
) -> DbResult<String> {
    let Some(sql) = sql else {
        // An autoindex backs a PRIMARY KEY or UNIQUE constraint: unique, plain
        // columns, never partial.
        let mut rows = conn
            .query(
                &format!("PRAGMA index_info({})", quote_identifier(name)),
                (),
            )
            .await?;
        let mut columns = Vec::new();
        while let Some(row) = rows.next().await? {
            columns.push(row.get::<String>(2)?);
        }
        return Ok(format!("unique index {name} ({})", columns.join(", ")));
    };
    Ok(match super::sql_policy::parse_create_index(&sql) {
        Some(definition) => {
            let mut line = String::new();
            if definition.unique {
                line.push_str("unique ");
            }
            if definition.predicate.is_some() {
                line.push_str("partial ");
            }
            line.push_str(&format!("index {name} ({})", definition.terms.join(", ")));
            if let Some(predicate) = definition.predicate {
                line.push_str(&format!(" WHERE {predicate}"));
            }
            line
        }
        None => format!(
            "index {name}: {}",
            super::sql_policy::collapse_whitespace(&sql)
        ),
    })
}

/// Quote an SQL identifier, doubling embedded quotes. The one quoting rule for
/// every identifier Cairn writes into SQL itself.
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn source_is_deterministic_dependency_ordered_and_labels_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("source.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE z_agent(id INTEGER PRIMARY KEY, value TEXT);\n\
             CREATE TABLE _cairn_rulings(id INTEGER PRIMARY KEY);\n\
             CREATE UNIQUE INDEX z_agent_value ON z_agent(value);\n\
             CREATE TRIGGER z_agent_insert AFTER INSERT ON z_agent BEGIN UPDATE z_agent SET value = NEW.value || '; from trigger' WHERE id = NEW.id; END;\n\
             CREATE VIEW z_agent_values AS SELECT value FROM z_agent;",
        )
        .await
        .unwrap();

        let first = render_source(&db).await.unwrap();
        assert_eq!(first, render_source(&db).await.unwrap());
        assert!(first.contains("-- cairn-owned table: _cairn_rulings"));
        assert!(first.contains("-- agent-owned table: z_agent"));
        let table = first.find("table: z_agent").unwrap();
        let view = first.find("view: z_agent_values").unwrap();
        let index = first.find("index: z_agent_value").unwrap();
        let trigger = first.find("trigger: z_agent_insert").unwrap();
        assert!(table < view && view < index && index < trigger);

        let replay_dir = tempfile::tempdir().unwrap();
        let replay = LocalDb::open(replay_dir.path().join("replay.db"))
            .await
            .unwrap();
        replay.execute_batch(&first).await.unwrap();
        assert_eq!(render_source(&replay).await.unwrap(), first);
        replay
            .execute("INSERT INTO z_agent(value) VALUES ('hello')", ())
            .await
            .unwrap();
        assert_eq!(
            replay
                .query_opt_text("SELECT value FROM z_agent_values", ())
                .await
                .unwrap(),
            Some("hello; from trigger".to_string())
        );
        assert!(
            replay
                .execute(
                    "INSERT INTO z_agent(value) VALUES ('hello; from trigger')",
                    ()
                )
                .await
                .is_err(),
            "the replayed unique index must still enforce uniqueness"
        );
    }

    #[tokio::test]
    async fn source_replays_autoincrement_without_exporting_engine_storage() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("sequence.db")).await.unwrap();
        // MVCC databases cannot host virtual tables. AUTOINCREMENT exercises
        // the engine-owned storage that a supported user schema actually adds.
        db.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);")
            .await
            .unwrap();
        let source = render_source(&db).await.unwrap();
        assert!(source.contains("AUTOINCREMENT"));
        assert!(!source.contains("__turso_internal_"));
        assert!(!source.contains("sqlite_sequence"));

        let replay = LocalDb::open(dir.path().join("sequence-replay.db"))
            .await
            .unwrap();
        replay.execute_batch(&source).await.unwrap();
        assert_eq!(render_source(&replay).await.unwrap(), source);
        replay
            .execute_script(
                "INSERT INTO docs(body) VALUES ('first'); DELETE FROM docs;
                               INSERT INTO docs(body) VALUES ('second');",
            )
            .await
            .unwrap();
        assert_eq!(
            replay
                .query_opt_i64("SELECT id FROM docs", ())
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn source_includes_app_objects_when_there_is_no_agent_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("app-only.db")).await.unwrap();
        assert_eq!(render_source(&db).await.unwrap(), "-- No schema objects");
        db.execute_batch(
            "CREATE TABLE _cairn_rulings(id INTEGER PRIMARY KEY);\n\
             CREATE INDEX _cairn_rulings_id ON _cairn_rulings(id);\n\
             CREATE TABLE cairn_schema_migrations(version TEXT);",
        )
        .await
        .unwrap();

        let source = render_source(&db).await.unwrap();
        assert!(source.contains("-- cairn-owned table: _cairn_rulings"));
        assert!(source.contains("-- cairn-owned index: _cairn_rulings_id"));
        assert!(source.contains("-- cairn-owned table: cairn_schema_migrations"));
        assert!(!source.contains("agent-owned"));
    }

    #[tokio::test]
    async fn overview_counts_tables_and_elides_wide_columns_only_in_the_listing() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("schema.db")).await.unwrap();
        let columns = (0..20)
            .map(|i| format!("c{i} TEXT"))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute_batch(&format!("CREATE TABLE wide(id INTEGER PRIMARY KEY, {columns}); INSERT INTO wide(id) VALUES(1); CREATE TABLE small(value TEXT NOT NULL DEFAULT 'x');")).await.unwrap();

        let listing = render_overview(
            &db,
            &OverviewParams {
                table: None,
                offset: 1,
                limit: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!((listing.shown_tables, listing.total_tables), (1, 2));
        assert!(listing.body.contains("… 13 more → ?table=wide"));
        assert!(listing.body.contains("— max rowid 1"));

        let detail = render_overview(
            &db,
            &OverviewParams {
                table: Some("wide".into()),
                offset: 0,
                limit: 1,
            },
        )
        .await
        .unwrap();
        assert!(detail.body.contains("c19 TEXT"));
        assert!(!detail.body.contains("more →"));
        assert!(detail.body.contains("id INTEGER pk"));
    }

    fn detail(table: &str) -> OverviewParams {
        OverviewParams {
            table: Some(table.to_string()),
            offset: 0,
            limit: 1,
        }
    }

    /// The index lines of a `?table=` detail, by index name.
    fn listed_indexes(body: &str) -> Vec<(String, String)> {
        let mut indexes: Vec<(String, String)> = body
            .lines()
            .filter_map(|line| line.strip_prefix("  "))
            .map(|line| {
                let mut words = line.split_whitespace();
                words.by_ref().find(|word| *word == "index").unwrap();
                let name = words.next().unwrap().trim_end_matches(':').to_string();
                (name, line.to_string())
            })
            .collect();
        indexes.sort();
        indexes
    }

    async fn catalog_indexes(db: &LocalDb, table: &str) -> Vec<(String, Option<String>)> {
        db.query_all(
            "SELECT name, sql FROM sqlite_schema \
             WHERE type = 'index' AND tbl_name = ?1 ORDER BY name",
            cairn_db::turso::params![table.to_string()],
            |row| Ok((row.get::<String>(0)?, row.get::<Option<String>>(1)?)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn overview_sizes_tables_by_rowid_high_water_one_read_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("sizes.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE sparse(id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO sparse(id, value) VALUES (1, 'a'), (1000, 'b');
             CREATE TABLE bare(value TEXT);
             INSERT INTO bare(value) VALUES ('x'), ('y'), ('z');
             CREATE TABLE vacant(value TEXT);
             CREATE TABLE shadowed(rowid TEXT, value TEXT);
             INSERT INTO shadowed(rowid, value) VALUES ('r', 'v');",
        )
        .await
        .unwrap();

        let before = db.read_transaction_count();
        let listing = render_overview(
            &db,
            &OverviewParams {
                table: None,
                offset: 0,
                limit: OVERVIEW_DEFAULT_TABLES,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.read_transaction_count() - before,
            listing.shown_tables,
            "each table is described in exactly one read of its own; the catalog in none"
        );
        let lines: Vec<&str> = listing.body.lines().collect();
        assert_eq!(
            lines,
            [
                "bare(value TEXT) — max rowid 3",
                "shadowed(rowid TEXT, value TEXT)",
                "sparse(id INTEGER pk, value TEXT) — max rowid 1000",
                "vacant(value TEXT) — empty",
            ],
            "sizes are rowid high-waters, never counts: sparse holds 2 rows"
        );

        let before = db.read_transaction_count();
        let detail = render_overview(&db, &detail("sparse")).await.unwrap();
        assert_eq!(db.read_transaction_count() - before, 1);
        assert_eq!(
            detail.body,
            "sparse(id INTEGER pk, value TEXT) — max rowid 1000"
        );
    }

    /// With a storage census recorded, a table's size note carries the
    /// census's live-row estimate and date beside the high-water.
    #[tokio::test]
    async fn overview_prints_the_census_count_beside_the_high_water() {
        let db = crate::storage::migrated_test_db("sql-schema-census.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1);
             INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws2','w',1,1);",
        )
        .await
        .unwrap();
        let taken_at = 1_793_677_584; // 2026-11-03 UTC
        let report = cairn_db::storage::census::CensusReport {
            taken_at,
            tables: vec![cairn_db::storage::census::TableCensus {
                table: "workspaces".to_string(),
                rows: 2,
                ..Default::default()
            }],
            ..Default::default()
        };
        cairn_db::storage::census::record(&db, &report)
            .await
            .unwrap();
        let body = render_overview(&db, &detail("workspaces"))
            .await
            .unwrap()
            .body;
        let first = body.lines().next().unwrap();
        assert!(first.ends_with(", ~2 rows as of 2026-11-03"), "{first}");
        assert!(first.contains(" — max rowid "), "{first}");
        // A table the census did not count keeps the bare high-water.
        let turns = render_overview(&db, &detail("turns")).await.unwrap().body;
        assert!(!turns.contains("rows as of"), "{turns}");
    }

    /// `(addr, opcode, p2)` of each instruction in `sql`'s compiled program.
    async fn program(db: &LocalDb, sql: &str) -> Vec<(i64, String, i64)> {
        db.query_all(format!("EXPLAIN {sql}"), (), |row| {
            Ok((
                row.get::<i64>(0)?,
                row.get::<String>(1)?,
                row.get::<i64>(3)?,
            ))
        })
        .await
        .unwrap()
    }

    /// Whether a program loops over rows: some `Next`/`Prev` back-edge whose
    /// body has no unconditional `Goto` past it. The min/max fast path keeps a
    /// back-edge but leaves the loop that way after the first row.
    fn iterates_rows(program: &[(i64, String, i64)]) -> bool {
        program.iter().any(|(edge, op, start)| {
            (op == "Next" || op == "Prev")
                && !program.iter().any(|(at, inner, to)| {
                    inner == "Goto" && (*start..*edge).contains(at) && to > edge
                })
        })
    }

    /// The sizing statement compiles, on the pinned engine, to a jump to the
    /// table's last row that reads one row. The unindexed `max(value)` control
    /// proves the loop check sees a scan when there is one.
    #[tokio::test]
    async fn rowid_high_water_is_a_seek_not_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("seek.db")).await.unwrap();
        db.execute_batch("CREATE TABLE big(id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();

        let seek = program(&db, &rowid_high_water_sql(&quote_identifier("big"))).await;
        assert!(seek.iter().any(|(_, op, _)| op == "Last"), "{seek:?}");
        assert!(
            !iterates_rows(&seek),
            "max(rowid) must read one row: {seek:?}"
        );

        let scan = program(&db, "SELECT max(value) FROM big").await;
        assert!(iterates_rows(&scan), "control must iterate rows: {scan:?}");
    }

    #[tokio::test]
    async fn table_detail_lists_exactly_the_catalog_indexes_of_a_migrated_database() {
        let db = crate::storage::migrated_test_db("sql-schema-indexes.db").await;
        for table in ["turns", "agent_waits"] {
            let body = render_overview(&db, &detail(table)).await.unwrap().body;
            let listed = listed_indexes(&body);
            let catalog = catalog_indexes(&db, table).await;
            assert_eq!(
                listed
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                catalog
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                "{table} detail:\n{body}"
            );
            for ((_, line), (_, sql)) in listed.iter().zip(&catalog) {
                let partial = sql
                    .as_deref()
                    .and_then(super::super::sql_policy::parse_create_index)
                    .and_then(|definition| definition.predicate);
                match partial {
                    Some(predicate) => {
                        assert!(line.contains("partial index "), "{line}");
                        assert!(line.ends_with(&format!(" WHERE {predicate}")), "{line}");
                    }
                    None => assert!(
                        !line.contains("partial") && !line.contains(" WHERE "),
                        "{line}"
                    ),
                }
            }
        }

        let turns = render_overview(&db, &detail("turns")).await.unwrap().body;
        assert!(
            turns.contains("\n  index idx_turns_job_id (job_id)"),
            "{turns}"
        );
        let waits = render_overview(&db, &detail("agent_waits"))
            .await
            .unwrap()
            .body;
        for expected in [
            "\n  partial index idx_agent_waits_mcp_due (created_at, due_at_ms, next_poll_at_ms, deadline_ms, pending_prompt_id, id) WHERE state = 'pending' AND kind = 'mcp_continuation'",
            "\n  partial index idx_agent_waits_resolving (resolving_at, created_at) WHERE state = 'resolving'",
            "\n  unique partial index idx_agent_waits_one_pending_call (job_id, tool_use_id) WHERE state IN ('pending', 'resolving')",
        ] {
            assert!(waits.contains(expected), "missing {expected:?} in:\n{waits}");
        }
    }

    #[tokio::test]
    async fn table_detail_marks_every_kind_of_index() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("indexes.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, sku TEXT UNIQUE, name TEXT, owner TEXT, state TEXT);
             CREATE INDEX items_owner ON items(owner);
             CREATE UNIQUE INDEX items_name ON items(name);
             CREATE INDEX items_open ON items(owner, state)
               WHERE state <> 'closed';
             CREATE INDEX items_lower_name ON items(lower(name));",
        )
        .await
        .unwrap();

        let body = render_overview(&db, &detail("items")).await.unwrap().body;
        let catalog = catalog_indexes(&db, "items").await;
        let autoindex = catalog
            .iter()
            .find(|(_, sql)| sql.is_none())
            .map(|(name, _)| name.clone())
            .expect("the UNIQUE column constraint has an autoindex");
        let lines: std::collections::BTreeMap<String, String> =
            listed_indexes(&body).into_iter().collect();
        let mut expected_names = vec![
            "items_lower_name".to_string(),
            "items_name".to_string(),
            "items_open".to_string(),
            "items_owner".to_string(),
            autoindex.clone(),
        ];
        expected_names.sort();
        assert_eq!(
            lines.keys().cloned().collect::<Vec<_>>(),
            expected_names,
            "{body}"
        );
        assert_eq!(lines["items_owner"], "index items_owner (owner)");
        assert_eq!(lines["items_name"], "unique index items_name (name)");
        assert_eq!(lines[&autoindex], format!("unique index {autoindex} (sku)"));
        // Turso re-serialises catalog SQL (`lower (name)`, `!=` for `<>`), so
        // expression and predicate text is checked by its meaningful parts.
        let expression = &lines["items_lower_name"];
        assert!(
            expression.starts_with("index items_lower_name (lower")
                && expression.ends_with("(name))"),
            "{expression}"
        );
        let partial = &lines["items_open"];
        assert!(
            partial.starts_with("partial index items_open (owner, state) WHERE state ")
                && partial.ends_with(" 'closed'"),
            "{partial}"
        );

        let listing = render_overview(
            &db,
            &OverviewParams {
                table: None,
                offset: 0,
                limit: OVERVIEW_DEFAULT_TABLES,
            },
        )
        .await
        .unwrap();
        assert!(
            listing.body.ends_with("\n  5 indexes → ?table=items"),
            "{}",
            listing.body
        );
    }

    #[tokio::test]
    async fn the_overview_document_lists_tables_and_details_one_with_the_row_engines_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("document.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT NOT NULL, code TEXT, created_at INTEGER NOT NULL DEFAULT 0);
             CREATE INDEX attempts_problem ON attempts(problem);
             CREATE TABLE commits(sha TEXT PRIMARY KEY, subject TEXT NOT NULL);
             CREATE TABLE pairs(a TEXT, b TEXT, PRIMARY KEY (a, b));
             INSERT INTO attempts(problem) VALUES ('o'), ('p'), ('q');
             DELETE FROM attempts WHERE id = 1;",
        )
        .await
        .unwrap();
        let params = |table: Option<&str>| OverviewParams {
            table: table.map(str::to_string),
            offset: 0,
            limit: OVERVIEW_DEFAULT_TABLES,
        };
        let root = "cairn://p/cairn/t/db";

        let before = db.read_transaction_count();
        let listing = overview_document(&db, &params(None), root, true)
            .await
            .unwrap();
        assert_eq!(
            db.read_transaction_count() - before,
            3,
            "one read per table, as the text overview"
        );
        let listing: serde_json::Value = serde_json::from_str(&listing.body).unwrap();
        assert_eq!(listing["uri"], root);
        assert_eq!(listing["writable"], true);
        assert_eq!(listing["total"], 3);
        assert_eq!(
            listing["tables"],
            serde_json::json!([
                {"name": "attempts", "maxRowid": 3, "rows": 2, "primaryKey": "id"},
                {"name": "commits", "maxRowid": null, "rows": 0, "primaryKey": "sha"},
                {"name": "pairs", "maxRowid": null, "rows": 0, "primaryKey": null},
            ])
        );

        // A read-only store (the application database) is never counted.
        let unwritable = overview_document(&db, &params(None), "cairn://db", false)
            .await
            .unwrap();
        let unwritable: serde_json::Value = serde_json::from_str(&unwritable.body).unwrap();
        assert_eq!(
            unwritable["tables"][0],
            serde_json::json!({"name": "attempts", "maxRowid": 3, "primaryKey": "id"})
        );

        let detail = overview_document(&db, &params(Some("attempts")), root, false)
            .await
            .unwrap();
        let detail: serde_json::Value = serde_json::from_str(&detail.body).unwrap();
        assert_eq!(detail["writable"], false);
        let columns = &detail["tables"][0]["columns"];
        assert_eq!(
            columns[0],
            serde_json::json!({"name": "id", "type": "INTEGER", "notNull": false, "default": null,
                               "primaryKey": true, "generated": true, "requiredForInsert": false})
        );
        assert_eq!(columns[1]["requiredForInsert"], true, "{columns}");
        assert_eq!(columns[2]["requiredForInsert"], false, "{columns}");
        assert_eq!(
            columns[3]["requiredForInsert"], false,
            "a default answers for a NOT NULL column: {columns}"
        );
        assert_eq!(
            detail["tables"][0]["indexes"],
            serde_json::json!(["index attempts_problem (problem)"])
        );

        let commits = overview_document(&db, &params(Some("commits")), root, true)
            .await
            .unwrap();
        let commits: serde_json::Value = serde_json::from_str(&commits.body).unwrap();
        assert_eq!(
            commits["tables"][0]["columns"][0]["requiredForInsert"], true,
            "a text key is the row's address, so an insert names it"
        );

        let missing = overview_document(&db, &params(Some("nope")), root, true)
            .await
            .unwrap();
        let missing: serde_json::Value = serde_json::from_str(&missing.body).unwrap();
        assert_eq!(missing["tables"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn overview_hides_app_tables_without_treating_underscore_as_a_wildcard() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(dir.path().join("schema.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE _cairn_internal(id INTEGER);
             CREATE TABLE cairn_schema_migrations(version TEXT);
             CREATE TABLE xcairn_notes(id INTEGER);
             CREATE TABLE sqlitex_notes(id INTEGER);
             CREATE TABLE xxturso_internal_notes(id INTEGER);",
        )
        .await
        .unwrap();

        let overview = render_overview(
            &db,
            &OverviewParams {
                table: None,
                offset: 0,
                limit: OVERVIEW_DEFAULT_TABLES,
            },
        )
        .await
        .unwrap();
        assert_eq!((overview.shown_tables, overview.total_tables), (3, 3));
        for table in ["sqlitex_notes", "xcairn_notes", "xxturso_internal_notes"] {
            assert!(overview.body.contains(&format!("{table}(")));
            assert!(render_source(&db)
                .await
                .unwrap()
                .contains(&format!("-- agent-owned table: {table}\n")));
        }
        assert!(!overview.body.contains("__turso_internal_"));
        assert!(!overview.body.contains("_cairn_internal("));

        for (offset, shown, prefix) in [(1, 1, "xcairn_notes("), (3, 0, "(no user tables)")] {
            let page = render_overview(
                &db,
                &OverviewParams {
                    table: None,
                    offset,
                    limit: 1,
                },
            )
            .await
            .unwrap();
            assert_eq!((page.shown_tables, page.total_tables), (shown, 3));
            assert!(page.body.starts_with(prefix));
        }
        for table in [
            "__turso_internal_mvcc_meta",
            "_cairn_internal",
            "cairn_schema_migrations",
        ] {
            let detail = render_overview(
                &db,
                &OverviewParams {
                    table: Some(table.to_string()),
                    offset: 0,
                    limit: 1,
                },
            )
            .await
            .unwrap();
            assert_eq!((detail.shown_tables, detail.total_tables), (0, 0));
            assert_eq!(detail.body, format!("Table '{table}' not found"));
        }
    }
}
