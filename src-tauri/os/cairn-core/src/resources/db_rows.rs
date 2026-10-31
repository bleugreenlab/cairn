//! The row grammar shared by both database stores: a thread's own database
//! (`cairn://p/P/NAME/db/<table>[/<pk>]`, writable by its owner) and the
//! application database (`cairn://db/<table>[/<pk>]`, structurally read-only).
//!
//! One module answers for both so the two stores cannot grow different read
//! grammars: the table shape, the query keys, value coercion, the SQL builders,
//! the rendering, and the live-schema affordance all live here, and the callers
//! differ only in which `LocalDb` they hand in, the URI root, and whether the
//! store is writable.
//!
//! Every identifier is quoted and every value is bound. No caller-supplied text
//! is ever spliced into SQL.

use cairn_common::contract::ChangeMode;
use cairn_common::query::QueryParam;
use cairn_common::read::{
    ActionSpec, Affordance, AffordanceSpec, KeyInfo, NaturalUnit, SegmentKind,
};
use cairn_common::uri::{build_db_row_uri, build_db_table_uri, CairnResource};
use cairn_db::turso::{Connection, Value};

use super::common::{affordance_spec_for_kind, find_query_value, render_affordance};
use super::read::{
    enter_query_only, render_sql_value, sql_value_to_json, RenderedResource, DB_SQL_DEFAULT_LIMIT,
    DB_SQL_MAX_LIMIT,
};
use super::sql_schema::quote_identifier;
use crate::storage::{DbResult, LocalDb};

/// The path segment under every `/db/` that never names a table. Under a thread
/// it is the generated schema projection (`.../db/schema`); under `cairn://db`
/// it is held back so the two roots keep one grammar.
pub(crate) const RESERVED_TABLE_SEGMENT: &str = "schema";

/// Markdown cells longer than this are cut; `format=json` carries full values.
const CELL_MAX_CHARS: usize = 120;

/// The refusal for a table or row address whose table segment is the reserved
/// one. `db_root` is the store's canonical root URI.
pub(crate) fn reserved_table_segment_refusal(db_root: &str) -> String {
    format!(
        "`{RESERVED_TABLE_SEGMENT}` is a reserved segment under {db_root} and never names a table. \
         Read a table called `{RESERVED_TABLE_SEGMENT}` with {db_root}?sql=SELECT * FROM \"{RESERVED_TABLE_SEGMENT}\" \
         or {db_root}?table={RESERVED_TABLE_SEGMENT}."
    )
}

/// One column, as `PRAGMA table_info` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: String,
    /// The declared type, verbatim; empty when the column declared none.
    pub decl_type: String,
    pub not_null: bool,
    pub default: Option<String>,
    /// 1-based position in the primary key, 0 when not part of it.
    pub pk: i64,
}

/// The live shape of one table or view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableShape {
    pub table: String,
    pub columns: Vec<Column>,
}

impl TableShape {
    /// `None` when the table does not exist (`table_info` reports no columns).
    pub(crate) async fn load(conn: &Connection, table: &str) -> DbResult<Option<Self>> {
        let mut rows = conn
            .query(
                &format!("PRAGMA table_info({})", quote_identifier(table)),
                (),
            )
            .await?;
        let mut columns = Vec::new();
        while let Some(row) = rows.next().await? {
            columns.push(Column {
                name: row.get::<String>(1)?,
                decl_type: row.get::<Option<String>>(2)?.unwrap_or_default(),
                not_null: row.get::<i64>(3)? != 0,
                default: row.get::<Option<String>>(4)?,
                pk: row.get::<i64>(5)?,
            });
        }
        Ok((!columns.is_empty()).then(|| Self {
            table: table.to_string(),
            columns,
        }))
    }

    /// The primary-key column, only when exactly one column forms the key. A
    /// composite key and an absent one (a view, or a rowid table that declared
    /// none) both answer `None`: those rows have no URI.
    pub(crate) fn primary_key(&self) -> Option<&Column> {
        let mut keys = self.columns.iter().filter(|column| column.pk > 0);
        let first = keys.next()?;
        keys.next().is_none().then_some(first)
    }

    /// A column by name. SQLite identifiers are case-insensitive, so the match
    /// is too; the returned column carries the declared spelling.
    pub(crate) fn column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
    }

    /// `id, problem, created_at` — for refusals that name the table's columns.
    pub(crate) fn column_names(&self) -> String {
        self.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// True when the column is the table's single `INTEGER PRIMARY KEY`, the
    /// rowid alias SQLite fills itself on insert.
    pub(crate) fn is_rowid_alias(&self, column: &Column) -> bool {
        self.primary_key().is_some_and(|pk| {
            pk.name == column.name && pk.decl_type.eq_ignore_ascii_case("INTEGER")
        })
    }

    /// True when an insert must name this column: not null with no default,
    /// or the addressing primary key unless SQLite generates it.
    pub(crate) fn required_for_insert(&self, column: &Column) -> bool {
        if self.is_rowid_alias(column) {
            return false;
        }
        let addressing_key = self.primary_key().is_some_and(|pk| pk.name == column.name);
        (column.not_null || addressing_key) && column.default.is_none()
    }
}

/// Load a table's shape inside a read-only read.
pub(crate) async fn load_shape(db: &LocalDb, table: &str) -> DbResult<Option<TableShape>> {
    let table = table.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            enter_query_only(conn).await;
            TableShape::load(conn, &table).await
        })
    })
    .await
}

/// Coerce query-string text to the value SQLite's column affinity would store
/// for it, so a filter or a URI's primary key compares as the column's own
/// type (`7` against an INTEGER key) whatever the engine's comparison rules.
///
/// The affinity rules are SQLite's, in its order: a declared type containing
/// INT is INTEGER; CHAR, CLOB or TEXT is TEXT; BLOB or no type stores the value
/// as given (here, text); REAL, FLOA or DOUB is REAL; anything else is NUMERIC.
/// Text that does not parse as the affinity's number stays text, as SQLite
/// would store it.
pub(crate) fn coerce_for_column(decl_type: &str, text: &str) -> Value {
    let declared = decl_type.to_ascii_uppercase();
    let integer = || text.parse::<i64>().ok().map(Value::Integer);
    let real = || {
        text.parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Value::Real)
    };
    let as_text = || Value::Text(text.to_string());
    if declared.contains("INT") {
        integer().unwrap_or_else(as_text)
    } else if ["CHAR", "CLOB", "TEXT"]
        .iter()
        .any(|marker| declared.contains(marker))
        || declared.contains("BLOB")
        || declared.is_empty()
    {
        as_text()
    } else if ["REAL", "FLOA", "DOUB"]
        .iter()
        .any(|marker| declared.contains(marker))
    {
        real().unwrap_or_else(as_text)
    } else {
        integer().or_else(real).unwrap_or_else(as_text)
    }
}

/// True for a declared type SQLite gives no affinity (BLOB, or no type at
/// all). Such a column stores each value with the type it was written with.
fn has_no_affinity(decl_type: &str) -> bool {
    let declared = decl_type.to_ascii_uppercase();
    // SQLite's order: INT, then CHAR/CLOB/TEXT, are decided before BLOB.
    let earlier = ["INT", "CHAR", "CLOB", "TEXT"]
        .iter()
        .any(|marker| declared.contains(marker));
    declared.is_empty() || (declared.contains("BLOB") && !earlier)
}

/// The values query-string text is compared against to match what a column
/// holds. A column with an affinity holds the coerced form only. A column with
/// none may hold `7` as an integer or as text, depending on how it was
/// written, and text from a URI has no type of its own, so text that is a
/// number's canonical spelling (the one [`pk_segment`] writes) matches either
/// reading: both are returned. `007` is no number's canonical spelling, so it
/// matches as text only. A table holding both the integer 7 and the text `7`
/// as distinct keys is the one case these readings cannot tell apart.
pub(crate) fn match_values(decl_type: &str, text: &str) -> Vec<Value> {
    if !has_no_affinity(decl_type) {
        return vec![coerce_for_column(decl_type, text)];
    }
    let number = text
        .parse::<i64>()
        .ok()
        .map(Value::Integer)
        .or_else(|| {
            text.parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .map(Value::Real)
        })
        .filter(|number| pk_segment(number).as_deref() == Some(text));
    let text = Value::Text(text.to_string());
    match number {
        Some(number) => vec![number, text],
        None => vec![text],
    }
}

/// `"col" = ?` for one candidate value, `"col" IN (?, ?)` for several: the one
/// equality every filter and key comparison is written with.
fn equality_sql(column: &str, candidates: &[Value]) -> String {
    if candidates.len() == 1 {
        format!("{} = ?", quote_identifier(column))
    } else {
        format!(
            "{} IN ({})",
            quote_identifier(column),
            vec!["?"; candidates.len()].join(", ")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowFormat {
    Markdown,
    Json,
}

fn parse_format(params: &[QueryParam]) -> Result<RowFormat, String> {
    match find_query_value(params, "format") {
        None => Ok(RowFormat::Markdown),
        Some("json") => Ok(RowFormat::Json),
        Some(value) => Err(format!(
            "Invalid value for query parameter 'format': {value} (accepted: json; omit format for a markdown table)"
        )),
    }
}

/// A collection read's filters, order and window, resolved against the shape.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowQuery {
    /// `(column, candidates)` equality filters, ANDed, columns in declared
    /// spelling; see [`match_values`] for why a filter may have two candidates.
    pub filters: Vec<(String, Vec<Value>)>,
    /// `(column, descending)`.
    pub order: Option<(String, bool)>,
    pub offset: usize,
    pub limit: usize,
    pub format: RowFormat,
}

impl RowQuery {
    /// Parse a collection's query keys. Every `where` occurrence counts (they
    /// AND), each split on its first `=`; an unknown column, an unknown key, or
    /// a malformed value is refused rather than ignored.
    pub(crate) fn parse(
        params: &[QueryParam],
        shape: &TableShape,
        collection_uri: &str,
    ) -> Result<Self, String> {
        if let Some(unsupported) = params.iter().find(|param| {
            !matches!(
                param.key.as_str(),
                "where" | "order" | "offset" | "limit" | "format"
            )
        }) {
            return Err(format!(
                "Unsupported query parameter '{}' for {collection_uri} (accepted: where, order, offset, limit, format)",
                unsupported.key
            ));
        }
        let unknown_column = |name: &str| {
            format!(
                "`{}` has no column `{name}`; its columns are: {}",
                shape.table,
                shape.column_names()
            )
        };
        let mut filters = Vec::new();
        for param in params.iter().filter(|param| param.key == "where") {
            let (name, value) = param.value.split_once('=').ok_or_else(|| {
                format!(
                    "where takes COLUMN=VALUE (equality only; LIKE and ranges stay in ?sql=); got '{}'",
                    param.value
                )
            })?;
            let column = shape.column(name).ok_or_else(|| unknown_column(name))?;
            filters.push((column.name.clone(), match_values(&column.decl_type, value)));
        }
        let order = match find_query_value(params, "order") {
            None => None,
            Some(value) => {
                let (name, descending) = match value.strip_prefix('-') {
                    Some(name) => (name, true),
                    None => (value, false),
                };
                let column = shape.column(name).ok_or_else(|| unknown_column(name))?;
                Some((column.name.clone(), descending))
            }
        };
        let parse_usize = |key: &str, default: usize| -> Result<usize, String> {
            find_query_value(params, key).map_or(Ok(default), |value| {
                value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
            })
        };
        Ok(Self {
            filters,
            order,
            offset: parse_usize("offset", 0)?,
            limit: parse_usize("limit", DB_SQL_DEFAULT_LIMIT)?.min(DB_SQL_MAX_LIMIT),
            format: parse_format(params)?,
        })
    }

    fn where_clause(&self) -> (String, Vec<Value>) {
        if self.filters.is_empty() {
            return (String::new(), Vec::new());
        }
        let terms: Vec<String> = self
            .filters
            .iter()
            .map(|(column, candidates)| equality_sql(column, candidates))
            .collect();
        (
            format!(" WHERE {}", terms.join(" AND ")),
            self.filters
                .iter()
                .flat_map(|(_, candidates)| candidates.iter().cloned())
                .collect(),
        )
    }

    /// The requested order, then the primary key ascending as the tie-breaker
    /// that keeps paging stable. Without a single-column key and without a
    /// requested order, the order is unspecified.
    fn order_clause(&self, shape: &TableShape) -> String {
        let mut terms = Vec::new();
        if let Some((column, descending)) = &self.order {
            terms.push(format!(
                "{} {}",
                quote_identifier(column),
                if *descending { "DESC" } else { "ASC" }
            ));
        }
        if let Some(pk) = shape.primary_key() {
            if self
                .order
                .as_ref()
                .is_none_or(|(column, _)| *column != pk.name)
            {
                terms.push(format!("{} ASC", quote_identifier(&pk.name)));
            }
        }
        if terms.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", terms.join(", "))
        }
    }

    /// `SELECT * … WHERE … ORDER BY … LIMIT ? OFFSET ?` with its bindings.
    pub(crate) fn select_page_sql(&self, shape: &TableShape) -> (String, Vec<Value>) {
        let (where_sql, mut values) = self.where_clause();
        let sql = format!(
            "SELECT * FROM {}{where_sql}{} LIMIT ? OFFSET ?",
            quote_identifier(&shape.table),
            self.order_clause(shape)
        );
        values.push(Value::Integer(self.limit as i64));
        values.push(Value::Integer(self.offset as i64));
        (sql, values)
    }

    /// `SELECT COUNT(*)` under the same filters.
    pub(crate) fn count_sql(&self, shape: &TableShape) -> (String, Vec<Value>) {
        let (where_sql, values) = self.where_clause();
        (
            format!(
                "SELECT COUNT(*) FROM {}{where_sql}",
                quote_identifier(&shape.table)
            ),
            values,
        )
    }

    /// ` where problem = two-sum and lang = rust`, for the page header.
    fn describe_filters(&self) -> String {
        if self.filters.is_empty() {
            return String::new();
        }
        let terms: Vec<String> = self
            .filters
            .iter()
            .map(|(column, candidates)| {
                // The last candidate is always the text as given.
                let shown = candidates.last().cloned().unwrap_or(Value::Null);
                format!("{column} = {}", render_sql_value(shown))
            })
            .collect();
        format!(" where {}", terms.join(" and "))
    }
}

/// `WHERE pk = ?` (or `IN (?, ?)` for a key without affinity) and its values.
fn key_match(pk: &Column, pk_text: &str) -> (String, Vec<Value>) {
    let candidates = match_values(&pk.decl_type, pk_text);
    (
        format!(" WHERE {}", equality_sql(&pk.name, &candidates)),
        candidates,
    )
}

/// `SELECT * FROM t WHERE pk = ?`, the pk matched as [`match_values`] says.
pub(crate) fn select_row_sql(
    shape: &TableShape,
    pk: &Column,
    pk_text: &str,
) -> (String, Vec<Value>) {
    let (where_sql, values) = key_match(pk, pk_text);
    (
        format!(
            "SELECT * FROM {}{where_sql}",
            quote_identifier(&shape.table)
        ),
        values,
    )
}

/// A JSON payload value as the SQL value it binds to: the one mapping for
/// `?sql=` params and structured row payloads alike. Booleans bind as 0/1;
/// arrays and objects have no SQL value.
pub(crate) fn sql_param(value: &serde_json::Value) -> Result<Value, String> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Integer(i64::from(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                Value::Real(value)
            } else {
                return Err("a number SQLite cannot represent".into());
            }
        }
        serde_json::Value::String(value) => Value::Text(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Err("values must be null, booleans, numbers, or strings".into());
        }
    })
}

/// Which columns a structured payload may name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadUse {
    /// Any column, the primary key included.
    Insert,
    /// Any column but the primary key, which is the row's address.
    Update,
}

/// Close a structured payload (a column map) against the live shape: every key
/// must name a column, values must be scalars, and an update may not name the
/// primary key. Returns `(column, value)` pairs in declared column spelling.
pub(crate) fn payload_columns(
    shape: &TableShape,
    payload: &serde_json::Map<String, serde_json::Value>,
    use_: PayloadUse,
) -> Result<Vec<(String, Value)>, String> {
    let mut columns = Vec::with_capacity(payload.len());
    for (key, value) in payload {
        let column = shape.column(key).ok_or_else(|| {
            format!(
                "`{}` has no column `{key}`; its columns are: {}",
                shape.table,
                shape.column_names()
            )
        })?;
        if use_ == PayloadUse::Update
            && shape.primary_key().is_some_and(|pk| pk.name == column.name)
        {
            return Err(format!(
                "`{}` is the primary key, the row's address, so it cannot be patched; delete the row and append it again to re-key it",
                column.name
            ));
        }
        let value = sql_param(value).map_err(|error| format!("payload.{key}: {error}"))?;
        columns.push((column.name.clone(), value));
    }
    Ok(columns)
}

fn quoted_list<'a>(names: impl Iterator<Item = &'a str>) -> String {
    names.map(quote_identifier).collect::<Vec<_>>().join(", ")
}

/// `INSERT INTO t(cols) VALUES(?…) [RETURNING pk]`, or `DEFAULT VALUES` for an
/// empty map. `RETURNING` is added when the table has a single-column key, so
/// the reply can name the row whether the key was generated or supplied.
pub(crate) fn insert_sql(shape: &TableShape, columns: &[(String, Value)]) -> (String, Vec<Value>) {
    let table = quote_identifier(&shape.table);
    let mut sql = if columns.is_empty() {
        format!("INSERT INTO {table} DEFAULT VALUES")
    } else {
        format!(
            "INSERT INTO {table}({}) VALUES({})",
            quoted_list(columns.iter().map(|(name, _)| name.as_str())),
            vec!["?"; columns.len()].join(", ")
        )
    };
    if let Some(pk) = shape.primary_key() {
        sql.push_str(&format!(" RETURNING {}", quote_identifier(&pk.name)));
    }
    (
        sql,
        columns.iter().map(|(_, value)| value.clone()).collect(),
    )
}

/// `UPDATE t SET c = ?, … WHERE pk = ?`, the key matched as [`match_values`] says.
pub(crate) fn update_sql(
    shape: &TableShape,
    pk: &Column,
    pk_text: &str,
    columns: &[(String, Value)],
) -> (String, Vec<Value>) {
    let assignments: Vec<String> = columns
        .iter()
        .map(|(name, _)| format!("{} = ?", quote_identifier(name)))
        .collect();
    let (where_sql, key_values) = key_match(pk, pk_text);
    let mut values: Vec<Value> = columns.iter().map(|(_, value)| value.clone()).collect();
    values.extend(key_values);
    (
        format!(
            "UPDATE {} SET {}{where_sql}",
            quote_identifier(&shape.table),
            assignments.join(", ")
        ),
        values,
    )
}

/// `DELETE FROM t WHERE pk = ?`, the key matched as [`match_values`] says.
pub(crate) fn delete_sql(shape: &TableShape, pk: &Column, pk_text: &str) -> (String, Vec<Value>) {
    let (where_sql, values) = key_match(pk, pk_text);
    (
        format!("DELETE FROM {}{where_sql}", quote_identifier(&shape.table)),
        values,
    )
}

/// The text a primary-key value takes as a URI segment. Null and blob keys
/// have no URI.
pub(crate) fn pk_segment(value: &Value) -> Option<String> {
    match value {
        Value::Integer(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Text(value) => Some(value.clone()),
        Value::Null | Value::Blob(_) => None,
    }
}

/// Rows as fetched: the column names and one value vector per row.
struct FetchedRows {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

async fn fetch(conn: &Connection, sql: &str, values: Vec<Value>) -> DbResult<FetchedRows> {
    let mut rows = conn.query(sql, values).await?;
    let columns = rows.column_names();
    let mut fetched = Vec::new();
    while let Some(row) = rows.next().await? {
        let mut values = Vec::with_capacity(row.column_count());
        for index in 0..row.column_count() {
            values.push(row.get_value(index)?);
        }
        fetched.push(values);
    }
    Ok(FetchedRows {
        columns,
        rows: fetched,
    })
}

/// One database store as the row engine sees it.
pub(crate) struct Store<'a> {
    pub db: &'a LocalDb,
    /// The canonical root: `cairn://db` or `cairn://p/P/NAME/db`.
    pub root: String,
    /// Only a thread's own store accepts structured writes.
    pub writable: bool,
}

impl Store<'_> {
    pub(crate) fn table_uri(&self, table: &str) -> String {
        build_db_table_uri(&self.root, table)
    }

    pub(crate) fn row_uri(&self, table: &str, pk: &str) -> String {
        build_db_row_uri(&self.root, table, pk)
    }
}

/// The refusal for a table the store does not hold.
pub(crate) fn no_table(db_root: &str, table: &str) -> String {
    format!("No table `{table}` in {db_root}; read {db_root} for the catalog of tables")
}

/// The row-form refusal for a table without a single-column primary key.
pub(crate) fn rows_have_no_uri(db_root: &str, table: &str) -> String {
    let collection = build_db_table_uri(db_root, table);
    format!(
        "`{table}` has a composite (or no) primary key, so its rows have no URI; read them with {db_root}?sql=, or {collection}?where=COLUMN=VALUE"
    )
}

/// Read a table's row collection (`pk: None`) or one row (`pk: Some`). The
/// affordance is rebuilt from the live schema when the table resolves; when
/// the caller passed none (a content-only read), none is attached.
pub(crate) async fn produce(
    store: &Store<'_>,
    table: &str,
    pk: Option<&str>,
    params: &[QueryParam],
    current: &CairnResource,
    affordance: Option<Affordance>,
) -> RenderedResource {
    let line = |content: String, affordance: Option<Affordance>| RenderedResource {
        content,
        natural_unit: NaturalUnit::Line,
        unit_noun: None,
        total_units: None,
        shown_units: None,
        offset: None,
        limit: None,
        match_count: None,
        grep_shown: None,
        record_prelude_lines: None,
        more_records: false,
        affordance,
        images: Vec::new(),
    };
    if table == RESERVED_TABLE_SEGMENT {
        return line(reserved_table_segment_refusal(&store.root), affordance);
    }
    let shape = match load_shape(store.db, table).await {
        Ok(Some(shape)) => shape,
        Ok(None) => return line(no_table(&store.root, table), affordance),
        Err(error) => {
            return line(
                format!("Failed to inspect table `{table}`: {error}"),
                affordance,
            )
        }
    };
    let affordance = affordance.map(|_| {
        let spec = table_affordance(&shape, store, current);
        Affordance {
            kind: SegmentKind::Resource,
            block: render_affordance(&spec),
            spec: Some(spec),
        }
    });
    match pk {
        None => match read_collection(store, &shape, params).await {
            Ok(rendered) => RenderedResource {
                affordance,
                ..rendered
            },
            Err(error) => line(error, affordance),
        },
        Some(pk) => match read_row(store, &shape, pk, params).await {
            Ok(body) => line(body, affordance),
            Err(error) => line(error, affordance),
        },
    }
}

async fn read_collection(
    store: &Store<'_>,
    shape: &TableShape,
    params: &[QueryParam],
) -> Result<RenderedResource, String> {
    let collection = store.table_uri(&shape.table);
    let query = RowQuery::parse(params, shape, &collection)?;
    let (page_sql, page_values) = query.select_page_sql(shape);
    let (count_sql, count_values) = query.count_sql(shape);
    let (fetched, total) = store
        .db
        .read(move |conn| {
            Box::pin(async move {
                enter_query_only(conn).await;
                let fetched = fetch(conn, &page_sql, page_values).await?;
                let mut count = conn.query(&count_sql, count_values).await?;
                let total = match count.next().await? {
                    Some(row) => row.get::<i64>(0)?.max(0) as usize,
                    None => 0,
                };
                Ok((fetched, total))
            })
        })
        .await
        .map_err(|error| format!("Reading `{}` failed: {error}", shape.table))?;

    let pk_index = shape
        .primary_key()
        .and_then(|pk| fetched.columns.iter().position(|name| *name == pk.name));
    let shown = fetched.rows.len();
    let has_more = query.offset + shown < total;
    let (content, prelude) = match query.format {
        RowFormat::Json => {
            let rows: Vec<serde_json::Value> = fetched
                .rows
                .iter()
                .map(|values| row_object(&fetched.columns, values))
                .collect();
            let document = serde_json::json!({
                "uri": collection,
                "table": shape.table,
                "primaryKey": shape.primary_key().map(|pk| pk.name.clone()),
                "total": total,
                "offset": query.offset,
                "limit": query.limit,
                "hasMore": has_more,
                "rows": rows,
            });
            (document.to_string(), None)
        }
        RowFormat::Markdown => {
            let mut truncated = false;
            let mut lines = Vec::with_capacity(shown);
            for values in &fetched.rows {
                let mut cells = Vec::with_capacity(values.len() + 1);
                if let Some(index) = pk_index {
                    cells.push(
                        pk_segment(&values[index])
                            .map(|pk| store.row_uri(&shape.table, &pk))
                            .unwrap_or_default(),
                    );
                }
                for value in values {
                    let (cell, cut) = markdown_cell(value.clone(), Some(CELL_MAX_CHARS));
                    truncated |= cut;
                    cells.push(cell);
                }
                lines.push(format!("| {} |", cells.join(" | ")));
            }
            let filters = query.describe_filters();
            let mut header = if query.offset == 0 && shown == total {
                format!("{total} rows in {collection}{filters}")
            } else if shown == 0 {
                format!(
                    "0 of {total} rows at offset {} in {collection}{filters}",
                    query.offset
                )
            } else {
                format!(
                    "rows {}–{} of {total} in {collection}{filters} (offset {}, limit {})",
                    query.offset + 1,
                    query.offset + shown,
                    query.offset,
                    query.limit
                )
            };
            if truncated {
                header.push_str(&format!(
                    " · cells over {CELL_MAX_CHARS} characters are cut; format=json carries full values"
                ));
            }
            if lines.is_empty() {
                (header, Some(1))
            } else {
                let mut names: Vec<String> = Vec::new();
                if pk_index.is_some() {
                    names.push("uri".to_string());
                }
                names.extend(fetched.columns.iter().map(|name| escape_cell(name)));
                let rule = vec!["---"; names.len()].join(" | ");
                (
                    format!(
                        "{header}\n\n| {} |\n| {rule} |\n{}",
                        names.join(" | "),
                        lines.join("\n")
                    ),
                    Some(4),
                )
            }
        }
    };
    Ok(RenderedResource {
        content,
        natural_unit: NaturalUnit::Record,
        unit_noun: Some("rows"),
        total_units: Some(total),
        shown_units: Some(shown),
        offset: Some(query.offset as i64),
        limit: Some(query.limit),
        match_count: None,
        grep_shown: None,
        record_prelude_lines: prelude,
        more_records: false,
        affordance: None,
        images: Vec::new(),
    })
}

async fn read_row(
    store: &Store<'_>,
    shape: &TableShape,
    pk_text: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    let row_uri = store.row_uri(&shape.table, pk_text);
    if let Some(unsupported) = params.iter().find(|param| param.key != "format") {
        return Err(format!(
            "Unsupported query parameter '{}' for {row_uri} (accepted: format)",
            unsupported.key
        ));
    }
    let format = parse_format(params)?;
    let pk = shape
        .primary_key()
        .ok_or_else(|| rows_have_no_uri(&store.root, &shape.table))?;
    let (sql, values) = select_row_sql(shape, pk, pk_text);
    let fetched = store
        .db
        .read(move |conn| {
            Box::pin(async move {
                enter_query_only(conn).await;
                fetch(conn, &sql, values).await
            })
        })
        .await
        .map_err(|error| format!("Reading {row_uri} failed: {error}"))?;
    let values = fetched
        .rows
        .first()
        .ok_or_else(|| format!("No row at {row_uri}"))?;
    Ok(match format {
        RowFormat::Json => serde_json::json!({
            "uri": row_uri,
            "table": shape.table,
            "primaryKey": pk.name,
            "row": row_object(&fetched.columns, values),
        })
        .to_string(),
        RowFormat::Markdown => {
            let lines: Vec<String> = fetched
                .columns
                .iter()
                .zip(values)
                .map(|(name, value)| {
                    format!(
                        "| {} | {} |",
                        escape_cell(name),
                        markdown_cell(value.clone(), None).0
                    )
                })
                .collect();
            format!(
                "Row {row_uri}\n\n| column | value |\n| --- | --- |\n{}",
                lines.join("\n")
            )
        }
    })
}

/// A row as `{column: value}`, values in the SQL JSON projection's encoding
/// (reals and blobs keep their tagged forms). Only columns appear, so a column
/// named `uri` can never collide with the envelope's metadata.
fn row_object(columns: &[String], values: &[Value]) -> serde_json::Value {
    serde_json::Value::Object(
        columns
            .iter()
            .zip(values)
            .map(|(name, value)| (name.clone(), sql_value_to_json(value.clone())))
            .collect(),
    )
}

fn escape_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

/// A value as a markdown table cell: the SQL projection's rendering (newlines
/// and tabs already escaped) with pipes escaped, optionally cut to `max`
/// characters. Returns whether it was cut.
fn markdown_cell(value: Value, max: Option<usize>) -> (String, bool) {
    let rendered = render_sql_value(value);
    match max {
        Some(max) if rendered.chars().count() > max => {
            let cut: String = rendered.chars().take(max).collect();
            (format!("{}…", escape_cell(&cut)), true)
        }
        _ => (escape_cell(&rendered), false),
    }
}

fn column_key(shape: &TableShape, column: &Column) -> KeyInfo {
    let mut notes = Vec::new();
    if shape.primary_key().is_some_and(|pk| pk.name == column.name) {
        notes.push(if shape.is_rowid_alias(column) {
            "primary key, generated when omitted".to_string()
        } else {
            "primary key".to_string()
        });
    } else if column.pk > 0 {
        notes.push(format!("primary key part {}", column.pk));
    }
    if column.not_null {
        notes.push("not null".to_string());
    }
    if let Some(default) = &column.default {
        notes.push(format!("default {default}"));
    }
    KeyInfo {
        key: column.name.clone(),
        ty: if column.decl_type.is_empty() {
            "any".to_string()
        } else {
            column.decl_type.clone()
        },
        note: notes.join(", "),
        aliases: Vec::new(),
    }
}

/// A placeholder value for a column in a copy-paste example.
fn example_value(column: &Column) -> &'static str {
    match coerce_for_column(&column.decl_type, "0") {
        Value::Integer(_) => "0",
        Value::Real(_) => "0.0",
        _ => "\"...\"",
    }
}

fn example_payload<'c>(columns: impl Iterator<Item = &'c Column>) -> String {
    let fields: Vec<String> = columns
        .map(|column| {
            let key = if column
                .name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                && !column.name.starts_with(|ch: char| ch.is_ascii_digit())
            {
                column.name.clone()
            } else {
                serde_json::Value::String(column.name.clone()).to_string()
            };
            format!("{key}:{}", example_value(column))
        })
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// The affordance for a table or row, generated from its live schema: every
/// column with its declared type, nullability, default, and key role, on a
/// read action that filters by any of them; and, for a writable store, the
/// append/patch/delete actions whose payload keys are those columns, so an
/// agent can write a row without reading the schema first.
///
/// It starts from the static contract spec (name, links, filters) and keeps
/// the related resources' actions the contract advertises after its own,
/// replacing only this resource's own actions with live ones.
pub(crate) fn table_affordance(
    shape: &TableShape,
    store: &Store<'_>,
    current: &CairnResource,
) -> AffordanceSpec {
    let kind = current.kind();
    let collection = store.table_uri(&shape.table);
    let row_template = format!("{collection}/{{pk}}");
    let mut spec =
        affordance_spec_for_kind(kind, Some(current)).unwrap_or_else(|| AffordanceSpec {
            kind: kind.slug(),
            name: shape.table.clone(),
            links: Vec::new(),
            filters: Vec::new(),
            actions: Vec::new(),
        });
    // `affordance_spec_for_kind` lists the contract's own mutations first, then
    // its related resources'. Keep the related ones (the SQL write on the
    // database root) and rebuild the own ones from the live shape.
    let own =
        cairn_common::contract::contract_for(kind).map_or(0, |contract| contract.mutations.len());
    let related = spec.actions.split_off(own.min(spec.actions.len()));
    let pk = shape.primary_key();
    let columns: Vec<KeyInfo> = shape
        .columns
        .iter()
        .map(|column| column_key(shape, column))
        .collect();
    let first_filter = shape
        .columns
        .iter()
        .find(|column| pk.is_none_or(|pk| pk.name != column.name))
        .or(shape.columns.first())
        .map(|column| column.name.clone())
        .unwrap_or_default();

    let mut actions = vec![ActionSpec {
        label: "read rows (columns below; filter with where=COLUMN=VALUE)".to_string(),
        mode: "read".to_string(),
        uri_template: collection.clone(),
        uri: Some(collection.clone()),
        required: Vec::new(),
        optional: columns.clone(),
        example: format!(
            "read({{paths:[\"{collection}?where={first_filter}=...&format=json\"]}})"
        ),
        guidance: Some(match (pk, store.writable) {
            (Some(_), true) => "; each row's URI is the collection plus its primary key".to_string(),
            (Some(_), false) => "; each row's URI is the collection plus its primary key; this store is read-only, so every write mode is refused".to_string(),
            (None, true) => "; this table has a composite (or no) primary key, so its rows have no URI".to_string(),
            (None, false) => "; this table has a composite (or no) primary key, so its rows have no URI; this store is read-only, so every write mode is refused".to_string(),
        }),
    }];

    if store.writable {
        let (required, optional): (Vec<&Column>, Vec<&Column>) = shape
            .columns
            .iter()
            .partition(|column| shape.required_for_insert(column));
        let example_columns: Vec<&Column> = if required.is_empty() {
            optional
                .iter()
                .copied()
                .filter(|column| !shape.is_rowid_alias(column))
                .take(1)
                .collect()
        } else {
            required.clone()
        };
        actions.push(ActionSpec {
            label: "insert one row; the reply carries its URI".to_string(),
            mode: ChangeMode::Append.as_str().to_string(),
            uri_template: collection.clone(),
            uri: Some(collection.clone()),
            required: required
                .iter()
                .map(|column| column_key(shape, column))
                .collect(),
            optional: optional
                .iter()
                .map(|column| column_key(shape, column))
                .collect(),
            example: format!(
                "write({{changes:[{{target:\"{collection}\",mode:\"append\",payload:{}}}]}})",
                example_payload(example_columns.into_iter())
            ),
            guidance: None,
        });
        if let Some(pk) = pk {
            let patchable: Vec<&Column> = shape
                .columns
                .iter()
                .filter(|column| column.name != pk.name)
                .collect();
            let row_uri = match current {
                CairnResource::ThreadDbRow { pk, .. } => Some(store.row_uri(&shape.table, pk)),
                _ => None,
            };
            let target = row_uri
                .clone()
                .unwrap_or_else(|| format!("{collection}/PK"));
            actions.push(ActionSpec {
                label: "update columns of one row".to_string(),
                mode: ChangeMode::Patch.as_str().to_string(),
                uri_template: row_template.clone(),
                uri: row_uri.clone(),
                required: Vec::new(),
                optional: patchable
                    .iter()
                    .map(|column| column_key(shape, column))
                    .collect(),
                example: format!(
                    "write({{changes:[{{target:\"{target}\",mode:\"patch\",payload:{}}}]}})",
                    example_payload(patchable.iter().copied().take(1))
                ),
                guidance: Some(
                    "; the primary key is the row's address and is not patchable".to_string(),
                ),
            });
            actions.push(ActionSpec {
                label: "delete one row".to_string(),
                mode: ChangeMode::Delete.as_str().to_string(),
                uri_template: row_template,
                uri: row_uri,
                required: Vec::new(),
                optional: Vec::new(),
                example: format!("write({{changes:[{{target:\"{target}\",mode:\"delete\"}}]}})"),
                guidance: None,
            });
        }
    }
    actions.extend(related);
    spec.actions = actions;
    spec
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(
        name: &str,
        decl_type: &str,
        not_null: bool,
        default: Option<&str>,
        pk: i64,
    ) -> Column {
        Column {
            name: name.to_string(),
            decl_type: decl_type.to_string(),
            not_null,
            default: default.map(str::to_string),
            pk,
        }
    }

    fn attempts() -> TableShape {
        TableShape {
            table: "attempts".to_string(),
            columns: vec![
                column("id", "INTEGER", false, None, 1),
                column("problem", "TEXT", true, None, 0),
                column("lang", "TEXT", true, Some("'rust'"), 0),
                column("created_at", "INTEGER", false, None, 0),
            ],
        }
    }

    fn params(query: &str) -> Vec<QueryParam> {
        cairn_common::query::parse_query_params(query).unwrap()
    }

    const URI: &str = "cairn://p/cairn/t/db/attempts";

    #[test]
    fn repeated_where_filters_all_apply_with_coerced_values() {
        let query = RowQuery::parse(
            &params("where=problem=two-sum&where=CREATED_AT=7"),
            &attempts(),
            URI,
        )
        .unwrap();
        assert_eq!(
            query.filters,
            vec![
                (
                    "problem".to_string(),
                    vec![Value::Text("two-sum".to_string())]
                ),
                ("created_at".to_string(), vec![Value::Integer(7)]),
            ]
        );
        let (sql, values) = query.select_page_sql(&attempts());
        assert_eq!(
            sql,
            "SELECT * FROM \"attempts\" WHERE \"problem\" = ? AND \"created_at\" = ? ORDER BY \"id\" ASC LIMIT ? OFFSET ?"
        );
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn a_where_value_may_itself_contain_equals() {
        let query = RowQuery::parse(&params("where=problem=a=b"), &attempts(), URI).unwrap();
        assert_eq!(query.filters[0].1, vec![Value::Text("a=b".to_string())]);
    }

    #[test]
    fn an_untyped_column_matches_both_readings_of_numeric_text() {
        assert_eq!(
            match_values("", "7"),
            vec![Value::Integer(7), Value::Text("7".into())]
        );
        assert_eq!(
            match_values("BLOB", "1.5"),
            vec![Value::Real(1.5), Value::Text("1.5".into())]
        );
        assert_eq!(match_values("", "abc"), vec![Value::Text("abc".into())]);
        // An affinity decides one reading, BLOB included once INT or TEXT came first.
        assert_eq!(match_values("INTEGER", "7"), vec![Value::Integer(7)]);
        assert_eq!(match_values("TEXTBLOB", "7"), vec![Value::Text("7".into())]);

        let shape = TableShape {
            table: "kv".to_string(),
            columns: vec![
                column("k", "", false, None, 1),
                column("score", "", false, None, 0),
            ],
        };
        let pk = shape.primary_key().unwrap();
        assert_eq!(
            select_row_sql(&shape, pk, "7"),
            (
                "SELECT * FROM \"kv\" WHERE \"k\" IN (?, ?)".to_string(),
                vec![Value::Integer(7), Value::Text("7".into())]
            )
        );
        // `007` is no integer's spelling, so it matches the text key only and
        // never the row keyed by the integer 7.
        assert_eq!(
            select_row_sql(&shape, pk, "007"),
            (
                "SELECT * FROM \"kv\" WHERE \"k\" = ?".to_string(),
                vec![Value::Text("007".into())]
            )
        );
        assert_eq!(
            delete_sql(&shape, pk, "x").0,
            "DELETE FROM \"kv\" WHERE \"k\" = ?"
        );
        let query = RowQuery::parse(&params("where=score=5"), &shape, URI).unwrap();
        assert!(query
            .count_sql(&shape)
            .0
            .ends_with("WHERE \"score\" IN (?, ?)"));
    }

    #[test]
    fn unknown_columns_and_keys_are_refused_by_name() {
        let error = RowQuery::parse(&params("where=nope=1"), &attempts(), URI).unwrap_err();
        assert!(error.contains("no column `nope`"), "{error}");
        assert!(error.contains("id, problem, lang, created_at"), "{error}");
        let error = RowQuery::parse(&params("order=-nope"), &attempts(), URI).unwrap_err();
        assert!(error.contains("no column `nope`"), "{error}");
        let error = RowQuery::parse(&params("sql=SELECT 1"), &attempts(), URI).unwrap_err();
        assert!(
            error.contains("Unsupported query parameter 'sql'"),
            "{error}"
        );
        let error = RowQuery::parse(&params("where=problem"), &attempts(), URI).unwrap_err();
        assert!(error.contains("COLUMN=VALUE"), "{error}");
        let error = RowQuery::parse(&params("format=csv"), &attempts(), URI).unwrap_err();
        assert!(error.contains("format"), "{error}");
    }

    #[test]
    fn descending_order_keeps_the_primary_key_as_tie_breaker() {
        let query = RowQuery::parse(&params("order=-created_at"), &attempts(), URI).unwrap();
        assert!(query
            .select_page_sql(&attempts())
            .0
            .contains("ORDER BY \"created_at\" DESC, \"id\" ASC"));
        let by_pk = RowQuery::parse(&params("order=-id"), &attempts(), URI).unwrap();
        assert!(by_pk
            .select_page_sql(&attempts())
            .0
            .contains("ORDER BY \"id\" DESC LIMIT"));
    }

    #[test]
    fn limit_is_capped_and_defaults() {
        let query = RowQuery::parse(&params("limit=5000"), &attempts(), URI).unwrap();
        assert_eq!(query.limit, DB_SQL_MAX_LIMIT);
        let query = RowQuery::parse(&[], &attempts(), URI).unwrap();
        assert_eq!((query.offset, query.limit), (0, DB_SQL_DEFAULT_LIMIT));
    }

    #[test]
    fn affinity_coercion_follows_sqlite_rules() {
        assert_eq!(coerce_for_column("INTEGER", "7"), Value::Integer(7));
        assert_eq!(coerce_for_column("bigint", "-3"), Value::Integer(-3));
        assert_eq!(
            coerce_for_column("INTEGER", "seven"),
            Value::Text("seven".into())
        );
        assert_eq!(coerce_for_column("TEXT", "7"), Value::Text("7".into()));
        assert_eq!(
            coerce_for_column("VARCHAR(10)", "7"),
            Value::Text("7".into())
        );
        assert_eq!(coerce_for_column("", "7"), Value::Text("7".into()));
        assert_eq!(coerce_for_column("BLOB", "7"), Value::Text("7".into()));
        assert_eq!(coerce_for_column("REAL", "7"), Value::Real(7.0));
        assert_eq!(coerce_for_column("DOUBLE", "1.5"), Value::Real(1.5));
        assert_eq!(coerce_for_column("NUMERIC", "7"), Value::Integer(7));
        assert_eq!(coerce_for_column("DECIMAL", "1.5"), Value::Real(1.5));
        assert_eq!(coerce_for_column("BOOLEAN", "x"), Value::Text("x".into()));
    }

    #[test]
    fn identifiers_are_quoted_including_embedded_quotes() {
        let shape = TableShape {
            table: "we\"ird".to_string(),
            columns: vec![column("k\"ey", "TEXT", false, None, 1)],
        };
        let query = RowQuery::parse(&params("where=k\"ey=x"), &shape, URI).unwrap();
        assert_eq!(
            query.count_sql(&shape).0,
            "SELECT COUNT(*) FROM \"we\"\"ird\" WHERE \"k\"\"ey\" = ?"
        );
        let pk = shape.primary_key().unwrap();
        assert_eq!(
            select_row_sql(&shape, pk, "x").0,
            "SELECT * FROM \"we\"\"ird\" WHERE \"k\"\"ey\" = ?"
        );
    }

    #[test]
    fn primary_key_detection_distinguishes_single_composite_and_absent() {
        assert_eq!(attempts().primary_key().unwrap().name, "id");
        let composite = TableShape {
            table: "pairs".to_string(),
            columns: vec![
                column("a", "TEXT", false, None, 1),
                column("b", "TEXT", false, None, 2),
            ],
        };
        assert!(composite.primary_key().is_none());
        let absent = TableShape {
            table: "notes".to_string(),
            columns: vec![column("body", "TEXT", false, None, 0)],
        };
        assert!(absent.primary_key().is_none());
        // Without a key and without a requested order, no ORDER BY is invented.
        let query = RowQuery::parse(&[], &absent, URI).unwrap();
        assert!(!query.select_page_sql(&absent).0.contains("ORDER BY"));
    }

    #[test]
    fn write_builders_quote_bind_and_return_the_key() {
        let shape = attempts();
        let columns = vec![("problem".to_string(), Value::Text("two-sum".into()))];
        assert_eq!(
            insert_sql(&shape, &columns),
            (
                "INSERT INTO \"attempts\"(\"problem\") VALUES(?) RETURNING \"id\"".to_string(),
                vec![Value::Text("two-sum".into())]
            )
        );
        assert_eq!(
            insert_sql(&shape, &[]).0,
            "INSERT INTO \"attempts\" DEFAULT VALUES RETURNING \"id\""
        );
        let pk = shape.primary_key().unwrap();
        assert_eq!(
            update_sql(&shape, pk, "7", &columns),
            (
                "UPDATE \"attempts\" SET \"problem\" = ? WHERE \"id\" = ?".to_string(),
                vec![Value::Text("two-sum".into()), Value::Integer(7)]
            )
        );
        assert_eq!(delete_sql(&shape, pk, "7").1, vec![Value::Integer(7)]);
    }

    #[test]
    fn payloads_close_against_the_live_columns() {
        let shape = attempts();
        let payload = |value: serde_json::Value| value.as_object().unwrap().clone();
        let error = payload_columns(
            &shape,
            &payload(serde_json::json!({"nope": 1})),
            PayloadUse::Insert,
        )
        .unwrap_err();
        assert!(
            error.contains("no column `nope`") && error.contains("id, problem, lang, created_at"),
            "{error}"
        );
        let error = payload_columns(
            &shape,
            &payload(serde_json::json!({"ID": 2})),
            PayloadUse::Update,
        )
        .unwrap_err();
        assert!(error.contains("row's address"), "{error}");
        let error = payload_columns(
            &shape,
            &payload(serde_json::json!({"problem": [1]})),
            PayloadUse::Insert,
        )
        .unwrap_err();
        assert!(error.starts_with("payload.problem:"), "{error}");
        assert_eq!(
            payload_columns(
                &shape,
                &payload(serde_json::json!({"PROBLEM": "x", "id": true})),
                PayloadUse::Insert
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn insert_requires_not_null_columns_without_defaults_and_a_non_rowid_key() {
        let shape = attempts();
        let required: Vec<&str> = shape
            .columns
            .iter()
            .filter(|column| shape.required_for_insert(column))
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(required, ["problem"]);
        let text_keyed = TableShape {
            table: "notes".to_string(),
            columns: vec![column("slug", "TEXT", false, None, 1)],
        };
        assert!(text_keyed.required_for_insert(&text_keyed.columns[0]));
    }

    #[tokio::test]
    async fn affordance_lists_live_columns_and_names_only_required_ones_in_the_example() {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(&dir.path().join("t.db")).await.unwrap();
        let store = Store {
            db: &db,
            root: "cairn://p/cairn/t/db".to_string(),
            writable: true,
        };
        let current = CairnResource::ThreadDbTable {
            project: "cairn".to_string(),
            name: "t".to_string(),
            table: "attempts".to_string(),
        };
        let spec = table_affordance(&attempts(), &store, &current);
        let block = render_affordance(&spec);
        assert!(
            block.contains("`id(INTEGER, primary key, generated when omitted)`"),
            "{block}"
        );
        assert!(block.contains("`problem(TEXT, not null)`"), "{block}");
        assert!(
            block.contains("`lang(TEXT, not null, default 'rust')`"),
            "{block}"
        );
        let append = spec
            .actions
            .iter()
            .find(|action| action.mode == "append")
            .unwrap();
        assert_eq!(
            append
                .required
                .iter()
                .map(|key| key.key.as_str())
                .collect::<Vec<_>>(),
            ["problem"]
        );
        assert!(
            append.example.contains("payload:{problem:\"...\"}"),
            "{}",
            append.example
        );
        assert!(spec.actions.iter().any(|action| action.mode == "patch"
            && action.uri_template == "cairn://p/cairn/t/db/attempts/{pk}"));
        assert!(spec.actions.iter().any(|action| action.mode == "delete"));

        let read_only = Store {
            db: &db,
            root: "cairn://db".to_string(),
            writable: false,
        };
        let current = CairnResource::DbTable {
            table: "attempts".to_string(),
        };
        let spec = table_affordance(&attempts(), &read_only, &current);
        assert!(spec.actions.iter().all(|action| action.mode == "read"));
        assert!(render_affordance(&spec).contains("read-only"));
    }

    async fn seeded_store() -> (tempfile::TempDir, LocalDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open(&dir.path().join("t.db")).await.unwrap();
        db.execute_batch(
            "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT NOT NULL, created_at INTEGER);
             INSERT INTO attempts VALUES (1, 'two-sum', 10), (2, 'two-sum', 30), (3, 'three|sum', 20);
             CREATE TABLE pairs(a TEXT, b TEXT, PRIMARY KEY(a, b));
             INSERT INTO pairs VALUES ('x', 'y');
             CREATE TABLE keyed(slug TEXT PRIMARY KEY, body TEXT);
             INSERT INTO keyed VALUES ('a b/c', 'spaced');",
        )
        .await
        .unwrap();
        (dir, db)
    }

    fn table_resource(table: &str) -> CairnResource {
        CairnResource::ThreadDbTable {
            project: "cairn".to_string(),
            name: "t".to_string(),
            table: table.to_string(),
        }
    }

    #[tokio::test]
    async fn collection_reads_filter_order_and_render_row_uris() {
        let (_dir, db) = seeded_store().await;
        let store = Store {
            db: &db,
            root: "cairn://p/cairn/t/db".to_string(),
            writable: true,
        };
        let rendered = produce(
            &store,
            "attempts",
            None,
            &params("where=problem=two-sum&order=-created_at&format=json"),
            &table_resource("attempts"),
            None,
        )
        .await;
        let document: serde_json::Value = serde_json::from_str(&rendered.content).unwrap();
        assert_eq!(document["primaryKey"], "id");
        assert_eq!(document["total"], 2);
        assert_eq!(document["rows"][0]["id"], 2);
        assert_eq!(document["rows"][1]["id"], 1);
        assert_eq!(rendered.total_units, Some(2));

        let markdown = produce(
            &store,
            "attempts",
            None,
            &[],
            &table_resource("attempts"),
            None,
        )
        .await;
        assert!(
            markdown
                .content
                .starts_with("3 rows in cairn://p/cairn/t/db/attempts\n"),
            "{}",
            markdown.content
        );
        assert!(
            markdown
                .content
                .contains("| cairn://p/cairn/t/db/attempts/3 | 3 | three\\|sum | 20 |"),
            "{}",
            markdown.content
        );

        let keyed = produce(&store, "keyed", None, &[], &table_resource("keyed"), None).await;
        assert!(
            keyed
                .content
                .contains("cairn://p/cairn/t/db/keyed/a%20b%2Fc"),
            "{}",
            keyed.content
        );
    }

    #[tokio::test]
    async fn row_reads_coerce_the_key_and_refuse_keyless_tables() {
        let (_dir, db) = seeded_store().await;
        let store = Store {
            db: &db,
            root: "cairn://db".to_string(),
            writable: false,
        };
        let row = produce(
            &store,
            "attempts",
            Some("2"),
            &params("format=json"),
            &CairnResource::DbRow {
                table: "attempts".into(),
                pk: "2".into(),
            },
            None,
        )
        .await;
        let document: serde_json::Value = serde_json::from_str(&row.content).unwrap();
        assert_eq!(document["uri"], "cairn://db/attempts/2");
        assert_eq!(document["row"]["problem"], "two-sum");

        let spaced = produce(
            &store,
            "keyed",
            Some("a b/c"),
            &[],
            &CairnResource::DbRow {
                table: "keyed".into(),
                pk: "a b/c".into(),
            },
            None,
        )
        .await;
        assert!(
            spaced.content.contains("| body | spaced |"),
            "{}",
            spaced.content
        );

        let missing = produce(
            &store,
            "attempts",
            Some("99"),
            &[],
            &CairnResource::DbRow {
                table: "attempts".into(),
                pk: "99".into(),
            },
            None,
        )
        .await;
        assert_eq!(missing.content, "No row at cairn://db/attempts/99");

        let composite = produce(
            &store,
            "pairs",
            Some("x"),
            &[],
            &CairnResource::DbRow {
                table: "pairs".into(),
                pk: "x".into(),
            },
            None,
        )
        .await;
        assert!(
            composite.content.contains("composite (or no) primary key"),
            "{}",
            composite.content
        );
        assert!(
            composite.content.contains("cairn://db?sql="),
            "{}",
            composite.content
        );

        let absent = produce(
            &store,
            "nope",
            None,
            &[],
            &CairnResource::DbTable {
                table: "nope".into(),
            },
            None,
        )
        .await;
        assert!(
            absent.content.starts_with("No table `nope` in cairn://db"),
            "{}",
            absent.content
        );

        let reserved = produce(
            &store,
            "schema",
            None,
            &[],
            &CairnResource::DbTable {
                table: "schema".into(),
            },
            None,
        )
        .await;
        assert!(
            reserved.content.contains("reserved segment"),
            "{}",
            reserved.content
        );
    }
}
