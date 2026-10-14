//! Compact schema discovery shared by the application and thread databases.

use cairn_db::storage::{DbResult, LocalDb};

pub(crate) const OVERVIEW_DEFAULT_TABLES: usize = 40;
pub(crate) const OVERVIEW_MAX_TABLES: usize = 200;
const OVERVIEW_MAX_COLUMNS: usize = 8;

/// The canonical predicate for user-owned tables in a thread database.
///
/// Prefix comparison is deliberate: `_` is a wildcard in SQL `LIKE`, which
/// would also hide unrelated names such as `xcairn_notes`.
pub(crate) const USER_TABLE_PREDICATE: &str = "type = 'table'
  AND substr(name, 1, 7) <> 'sqlite_'
  AND substr(name, 1, 17) <> '__turso_internal_'
  AND substr(name, 1, 7) <> '_cairn_'
  AND name <> 'cairn_schema_migrations'";

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
                let owner = if name.starts_with("_cairn_") || name == "cairn_schema_migrations" {
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

pub(crate) async fn render_overview(
    db: &LocalDb,
    params: &OverviewParams,
) -> DbResult<SchemaOverview> {
    let params = params.clone();
    db.read(move |conn| {
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
            let total_tables = names.len();
            let selected: Vec<String> = if let Some(table) = &params.table {
                names.into_iter().filter(|name| name == table).collect()
            } else {
                names
                    .into_iter()
                    .skip(params.offset)
                    .take(params.limit)
                    .collect()
            };

            let mut rendered = Vec::with_capacity(selected.len());
            for table in &selected {
                let quoted = quote_identifier(table);
                let mut column_rows = conn
                    .query(&format!("PRAGMA table_info({quoted})"), ())
                    .await?;
                let mut columns = Vec::new();
                while let Some(row) = column_rows.next().await? {
                    let name = row.get::<String>(1)?;
                    let kind = row.get::<String>(2)?;
                    let not_null = row.get::<i64>(3)? != 0;
                    let default = row.get::<Option<String>>(4)?;
                    let primary_key = row.get::<i64>(5)? != 0;
                    let mut column = if kind.is_empty() {
                        name
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
                    columns.push(column);
                }
                let all_columns = params.table.is_some();
                let omitted = columns.len().saturating_sub(OVERVIEW_MAX_COLUMNS);
                if !all_columns && omitted > 0 {
                    columns.truncate(OVERVIEW_MAX_COLUMNS);
                    columns.push(format!("… {omitted} more → ?table={table}"));
                }

                let mut count_rows = conn
                    .query(&format!("SELECT COUNT(*) FROM {quoted}"), ())
                    .await?;
                let count = count_rows
                    .next()
                    .await?
                    .expect("COUNT always returns one row")
                    .get::<i64>(0)?;
                rendered.push(format!("{table}({}) — {count} rows", columns.join(", ")));

                let mut index_rows = conn
                    .query(&format!("PRAGMA index_list({quoted})"), ())
                    .await?;
                while let Some(row) = index_rows.next().await? {
                    if row.get::<i64>(2)? != 0 {
                        rendered.push(format!("  unique index {}", row.get::<String>(1)?));
                    }
                }
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
                total_tables: if params.table.is_some() {
                    selected.len()
                } else {
                    total_tables
                },
                shown_tables: selected.len(),
            })
        })
    })
    .await
}

fn quote_identifier(identifier: &str) -> String {
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
        assert!(listing.body.contains("— 1 rows"));

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
