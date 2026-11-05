//! Reading one statement's `EXPLAIN` bytecode for the ways it walks a wide row.
//!
//! Plain `EXPLAIN`, never `EXPLAIN QUERY PLAN`: the pinned engine's query plan
//! omits trigger and foreign-key-action subprograms entirely, which is how
//! cairn/5907's trigger bodies scanned 272,699 rows behind a passing plan pin.
//! The bytecode lists every subprogram after the main program, each starting
//! again at address 0 and ending in a `Halt` whose p4 reads `trigger` or `fk
//! action`.
//!
//! The engine's comment column names what each instruction touches, so the
//! classifier needs little from the schema: `OpenRead` reads `table=<t>, …` or
//! `index=<i>, …` (a foreign-key check writes `=<i>, …`), `Column` reads
//! `r[n]=<t>.<col>`, and only `OpenWrite`, whose comment is `root=N; iDb=0`,
//! has to be resolved by root page.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::storage::DbResult;
use crate::turso::{Connection, Row, Value};

use super::registry::WideRowTable;

/// How many instructions an excerpt may carry.
const EXCERPT_LINES: usize = 12;

/// One row of an `EXPLAIN` listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instr {
    pub addr: i64,
    pub opcode: String,
    pub p1: i64,
    pub p2: i64,
    pub p3: i64,
    pub p4: String,
    pub p5: i64,
    pub comment: String,
}

impl Instr {
    fn from_row(row: &Row) -> DbResult<Self> {
        Ok(Self {
            addr: int(row, 0)?,
            opcode: text(row, 1)?,
            p1: int(row, 2)?,
            p2: int(row, 3)?,
            p3: int(row, 4)?,
            p4: text(row, 5)?,
            p5: int(row, 6)?,
            comment: text(row, 7)?,
        })
    }

    /// `addr opcode p1 p2 p3 comment`, as an excerpt shows it.
    pub fn render(&self) -> String {
        let line = format!(
            "{:>3} {} {} {} {} {}",
            self.addr, self.opcode, self.p1, self.p2, self.p3, self.comment
        );
        line.trim_end().to_string()
    }
}

fn int(row: &Row, idx: usize) -> DbResult<i64> {
    Ok(match row.get_value(idx)? {
        Value::Integer(value) => value,
        Value::Text(value) => value.trim().parse().unwrap_or(0),
        _ => 0,
    })
}

fn text(row: &Row, idx: usize) -> DbResult<String> {
    Ok(match row.get_value(idx)? {
        Value::Text(value) => value,
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Null | Value::Blob(_) => String::new(),
    })
}

/// A statement's bytecode, split into its main program and its subprograms.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    programs: Vec<Vec<Instr>>,
}

impl Listing {
    /// Split a listing wherever the address restarts at 0.
    pub fn new(instrs: impl IntoIterator<Item = Instr>) -> Self {
        let mut programs: Vec<Vec<Instr>> = Vec::new();
        for instr in instrs {
            match programs.last_mut() {
                Some(program) if instr.addr != 0 => program.push(instr),
                _ => programs.push(vec![instr]),
            }
        }
        Self { programs }
    }

    pub fn main(&self) -> &[Instr] {
        self.programs.first().map_or(&[], Vec::as_slice)
    }

    /// Trigger bodies and foreign-key actions, in the order the engine lists them.
    pub fn subprograms(&self) -> &[Vec<Instr>] {
        self.programs.get(1..).unwrap_or(&[])
    }

    /// Every instruction of every program.
    pub fn instrs(&self) -> impl Iterator<Item = &Instr> {
        self.programs.iter().flatten()
    }

    /// The whole listing, one `opcode comment` per line, for an assertion message.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (index, program) in self.programs.iter().enumerate() {
            if index > 0 {
                out.push_str("-- subprogram\n");
            }
            for instr in program {
                out.push_str(&instr.render());
                out.push('\n');
            }
        }
        out
    }
}

/// `EXPLAIN <sql>` on `conn`. Placeholders need no binding: the engine compiles
/// `?` without values.
pub async fn explain(conn: &Connection, sql: &str) -> DbResult<Listing> {
    let mut rows = conn.query(format!("EXPLAIN {sql}"), ()).await?;
    let mut instrs = Vec::new();
    while let Some(row) = rows.next().await? {
        instrs.push(Instr::from_row(&row)?);
    }
    Ok(Listing::new(instrs))
}

/// What an `OpenRead` or `OpenWrite` cursor is positioned over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    Table(String),
    Index { name: String, table: String },
}

impl Object {
    /// The table whose rows this object reaches.
    pub fn table(&self) -> &str {
        match self {
            Self::Table(table) | Self::Index { table, .. } => table,
        }
    }

    /// The object's own name: its table's, or its index's.
    pub fn name(&self) -> &str {
        match self {
            Self::Table(name) | Self::Index { name, .. } => name,
        }
    }
}

/// Where a wide table's columns sit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WideLayout {
    /// Column names by position, as `PRAGMA table_info` numbers them, which is
    /// the position a `Column` instruction's p2 reads.
    pub columns: Vec<String>,
    /// The position of the first blob column: a read at or past it walks the
    /// row's overflow chain.
    pub first_blob: usize,
}

/// The little of the schema the classifier cannot read from the bytecode.
#[derive(Debug, Clone, Default)]
pub struct SchemaView {
    by_root: HashMap<i64, Object>,
    index_table: HashMap<String, String>,
    /// Each unique index and its key column count, for an `OpenWrite` cursor,
    /// whose p4 does not describe its key the way an `OpenRead`'s does.
    unique: HashMap<String, Option<usize>>,
    wide: HashMap<String, WideLayout>,
}

impl SchemaView {
    /// Read the view from `conn`: every table and index from `sqlite_master`,
    /// and the column layout of each `registry` table the database has.
    pub async fn load(conn: &Connection, registry: &[WideRowTable]) -> DbResult<Self> {
        let mut view = Self::default();
        let mut rows = conn
            .query(
                "SELECT type, name, tbl_name, rootpage, coalesce(sql, '') FROM sqlite_master \
                 WHERE type IN ('table', 'index')",
                (),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            let (kind, name, table, root, sql) = (
                text(&row, 0)?,
                text(&row, 1)?,
                text(&row, 2)?,
                int(&row, 3)?,
                text(&row, 4)?,
            );
            let object = if kind == "index" {
                if name.starts_with("sqlite_autoindex_") || is_unique_index_sql(&sql) {
                    view.unique.insert(name.clone(), None);
                }
                view.index_table.insert(name.clone(), table.clone());
                Object::Index { name, table }
            } else {
                Object::Table(name)
            };
            if root != 0 {
                view.by_root.insert(root, object);
            }
        }
        drop(rows);
        let unique: Vec<String> = view.unique.keys().cloned().collect();
        for index in unique {
            let Ok(mut rows) = conn
                .query(format!("PRAGMA index_info(\"{index}\")"), ())
                .await
            else {
                continue;
            };
            let mut columns = 0;
            while rows.next().await?.is_some() {
                columns += 1;
            }
            if columns > 0 {
                view.unique.insert(index, Some(columns));
            }
        }
        for entry in registry {
            let mut rows = conn
                .query(format!("PRAGMA table_info({})", entry.table), ())
                .await?;
            let mut columns = Vec::new();
            while let Some(row) = rows.next().await? {
                let position = usize::try_from(int(&row, 0)?).unwrap_or(0);
                if columns.len() <= position {
                    columns.resize(position + 1, String::new());
                }
                columns[position] = text(&row, 1)?;
            }
            let first_blob = entry
                .blob_columns
                .iter()
                .filter_map(|blob| columns.iter().position(|name| name == blob))
                .min();
            if let Some(first_blob) = first_blob {
                view.wide.insert(
                    entry.table.to_string(),
                    WideLayout {
                        columns,
                        first_blob,
                    },
                );
            }
        }
        Ok(view)
    }

    /// The layout of `table`, when it is a registered wide table here.
    pub fn wide(&self, table: &str) -> Option<&WideLayout> {
        self.wide.get(table)
    }

    /// What sits at root page `root` of this database.
    pub fn object_at_root(&self, root: i64) -> Option<&Object> {
        self.by_root.get(&root)
    }

    #[cfg(test)]
    pub(super) fn with_root_for_test(root: i64, table: &str) -> Self {
        let mut view = Self::default();
        view.by_root.insert(root, Object::Table(table.to_string()));
        view
    }
}

fn is_unique_index_sql(sql: &str) -> bool {
    let mut words = sql.split_whitespace().map(str::to_ascii_uppercase);
    words.next().as_deref() == Some("CREATE") && words.next().as_deref() == Some("UNIQUE")
}

/// The rules, in the order a reader should care about them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Rule {
    /// A wide table walked in full: by its own cursor, or through one of its
    /// indexes while fetching each row. Whatever columns are read, every row's
    /// first page is paid for, which is what 5864's first statement cost.
    WideScan,
    /// A trigger body or foreign-key action walking a real table. A trigger
    /// body is part of the statement that fires it, so it must be keyed to the
    /// row that fired it (docs/database.md).
    TriggerScan,
    /// An index or rowid range over a wide table that reads a column at or past
    /// the first blob. Report-only: a bounded window over `events.data` is real
    /// work, and bytecode cannot show the range's size (5876's shape).
    RangePastBlob,
}

impl Rule {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WideScan => "wide-scan",
            Self::TriggerScan => "trigger-scan",
            Self::RangePastBlob => "range-past-blob",
        }
    }

    /// Whether a finding under this rule fails a run that has not allowlisted it.
    pub const fn gating(self) -> bool {
        !matches!(self, Self::RangePastBlob)
    }
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One flagged shape in one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub rule: Rule,
    pub table: String,
    /// The column read past the blob, for `range-past-blob`.
    pub column: Option<String>,
    /// The index the walk or seek goes through, when it goes through one.
    pub index: Option<String>,
    /// The flagged instructions with two either side, at most twelve lines.
    pub excerpt: String,
}

impl Finding {
    /// `rule on table[.column]`.
    pub fn subject(&self) -> String {
        match &self.column {
            Some(column) => format!("{} on {}.{column}", self.rule, self.table),
            None => format!("{} on {}", self.rule, self.table),
        }
    }
}

/// A range over an index, from a seek the loop steps away from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSeek {
    pub index: String,
    pub table: String,
}

/// The classifier's verdict on one listing.
#[derive(Debug, Clone, Default)]
pub struct Classified {
    pub findings: Vec<Finding>,
    /// Every table the statement opens, directly or through an index, in any
    /// program. What the runner's staleness check calls "seen".
    pub tables: BTreeSet<String>,
    /// Every stepped seek through an index that is not a point read, on any
    /// table, wide or not.
    pub range_seeks: Vec<RangeSeek>,
    /// An `Open` the schema view could not resolve. The caller refreshes the
    /// view and classifies again; a second miss stands.
    pub unresolved: bool,
}

impl Classified {
    fn push(&mut self, finding: Finding) {
        let duplicate = self.findings.iter().any(|seen| {
            seen.rule == finding.rule
                && seen.table == finding.table
                && seen.column == finding.column
        });
        if !duplicate {
            self.findings.push(finding);
        }
    }
}

/// Apply every rule to every program of `listing`.
pub fn classify(listing: &Listing, schema: &SchemaView) -> Classified {
    let mut out = Classified::default();
    for (index, program) in listing.programs.iter().enumerate() {
        classify_program(program, index > 0, schema, &mut out);
    }
    out.findings
        .sort_by(|a, b| (a.rule, &a.table).cmp(&(b.rule, &b.table)));
    out
}

#[derive(Debug, Clone)]
struct Cursor {
    object: Object,
    /// The key columns an index cursor's `k(N, …)` declares, less the rowid.
    key_columns: Option<usize>,
}

/// One instruction that moves an index cursor's walk onto a table row.
struct Fetch {
    from: i64,
    into: i64,
    into_table: Option<String>,
}

/// One `Column` or `ColumnRange` read of a table cursor.
struct ColumnRead {
    at: usize,
    cursor: i64,
    table: String,
    first: usize,
    last: usize,
}

fn classify_program(
    program: &[Instr],
    subprogram: bool,
    schema: &SchemaView,
    out: &mut Classified,
) {
    // Cursor numbers are per program and may be reused within one, so the
    // binding is the one in force where each instruction sits.
    let mut cursors: HashMap<i64, Cursor> = HashMap::new();
    let mut walks: Vec<(usize, i64, Object)> = Vec::new();
    let mut seeks: Vec<(usize, i64, Object, bool)> = Vec::new();
    let mut steps: HashSet<i64> = HashSet::new();
    let mut fetches: Vec<Fetch> = Vec::new();
    let mut rowid_regs: HashMap<i64, i64> = HashMap::new();
    let mut reads: Vec<ColumnRead> = Vec::new();

    let table_of = |cursors: &HashMap<i64, Cursor>, cursor: i64| match cursors.get(&cursor) {
        Some(Cursor {
            object: Object::Table(table),
            ..
        }) => Some(table.clone()),
        _ => None,
    };

    for (at, instr) in program.iter().enumerate() {
        match instr.opcode.as_str() {
            "OpenRead" | "OpenWrite" => match resolve_open(instr, schema) {
                Some(object) => {
                    out.tables.insert(object.table().to_string());
                    cursors.insert(
                        instr.p1,
                        Cursor {
                            object,
                            key_columns: key_columns(&instr.p4),
                        },
                    );
                }
                None => {
                    out.unresolved = true;
                    cursors.remove(&instr.p1);
                }
            },
            "OpenDup" => {
                if let Some(cursor) = cursors.get(&instr.p2).cloned() {
                    cursors.insert(instr.p1, cursor);
                }
            }
            "OpenEphemeral" | "OpenAutoindex" | "SorterOpen" | "OpenPseudo" => {
                cursors.remove(&instr.p1);
            }
            "Rewind" | "Last" => {
                if let Some(cursor) = cursors.get(&instr.p1) {
                    walks.push((at, instr.p1, cursor.object.clone()));
                }
            }
            "SeekGE" | "SeekGT" | "SeekLE" | "SeekLT" => {
                if let Some(cursor) = cursors.get(&instr.p1) {
                    let point = is_point_seek(cursor, program, at, schema);
                    seeks.push((at, instr.p1, cursor.object.clone(), point));
                }
            }
            "Next" | "Prev" if is_reachable(program, at) => {
                steps.insert(instr.p1);
            }
            "DeferredSeek" => fetches.push(Fetch {
                from: instr.p1,
                into: instr.p2,
                into_table: table_of(&cursors, instr.p2),
            }),
            "IdxRowId" => {
                rowid_regs.insert(instr.p2, instr.p1);
            }
            "SeekRowid" | "NotExists" => {
                let reg = if instr.opcode == "SeekRowid" {
                    instr.p2
                } else {
                    instr.p3
                };
                if let Some(&from) = rowid_regs.get(&reg) {
                    fetches.push(Fetch {
                        from,
                        into: instr.p1,
                        into_table: table_of(&cursors, instr.p1),
                    });
                }
            }
            "Column" | "ColumnRange" => {
                if let Some(table) = table_of(&cursors, instr.p1) {
                    let first = usize::try_from(instr.p2).unwrap_or(0);
                    let count = if instr.opcode == "ColumnRange" {
                        instr.p4.trim().parse::<usize>().unwrap_or(1).max(1)
                    } else {
                        1
                    };
                    reads.push(ColumnRead {
                        at,
                        cursor: instr.p1,
                        table,
                        first,
                        last: first + count - 1,
                    });
                }
            }
            _ => {}
        }
    }

    let fetches_row = |from: i64, table: &str| {
        fetches
            .iter()
            .any(|fetch| fetch.from == from && fetch.into_table.as_deref() == Some(table))
    };

    // A `Rewind` or `Last` the loop never steps from reads one row, which is
    // how `max(rowid)` compiles; only a stepped cursor walks the table.
    for (at, cursor, object) in walks.iter().filter(|(_, cursor, _)| steps.contains(cursor)) {
        let table = object.table();
        let index = index_name(object);
        if subprogram {
            out.push(Finding {
                rule: Rule::TriggerScan,
                table: table.to_string(),
                column: None,
                index,
                excerpt: format!(
                    "-- {} subprogram\n{}",
                    subprogram_kind(program),
                    excerpt(program, *at, None)
                ),
            });
            continue;
        }
        if schema.wide(table).is_none() {
            continue;
        }
        let walks_rows = match object {
            Object::Table(_) => true,
            Object::Index { .. } => fetches_row(*cursor, table),
        };
        if walks_rows {
            out.push(Finding {
                rule: Rule::WideScan,
                table: table.to_string(),
                column: None,
                index,
                excerpt: excerpt(program, *at, None),
            });
        }
    }

    for (at, cursor, object, point) in &seeks {
        if *point || !steps.contains(cursor) {
            continue;
        }
        let table = object.table();
        if let Some(index) = index_name(object) {
            let seek = RangeSeek {
                index,
                table: table.to_string(),
            };
            if !out.range_seeks.contains(&seek) {
                out.range_seeks.push(seek);
            }
        }
        let Some(layout) = schema.wide(table) else {
            continue;
        };
        let row_cursors: Vec<i64> = match object {
            Object::Table(_) => vec![*cursor],
            Object::Index { .. } => fetches
                .iter()
                .filter(|fetch| fetch.from == *cursor && fetch.into_table.as_deref() == Some(table))
                .map(|fetch| fetch.into)
                .collect(),
        };
        let past_blob = reads.iter().find(|read| {
            row_cursors.contains(&read.cursor)
                && read.table == table
                && read.last >= layout.first_blob
        });
        if let Some(read) = past_blob {
            let position = read.first.max(layout.first_blob);
            out.push(Finding {
                rule: Rule::RangePastBlob,
                table: table.to_string(),
                column: layout.columns.get(position).cloned(),
                index: index_name(object),
                excerpt: excerpt(program, *at, Some(read.at)),
            });
        }
    }
}

fn index_name(object: &Object) -> Option<String> {
    match object {
        Object::Table(_) => None,
        Object::Index { name, .. } => Some(name.clone()),
    }
}

/// Whether control can reach `program[at]`: it does not sit straight after an
/// unconditional `Goto`, or something jumps to it. `max(rowid)` compiles a
/// `Prev` behind a `Goto` that nothing targets, so its loop never steps. p2 is
/// a register or a column number for many opcodes, so this over-counts jumps,
/// which errs toward reporting.
fn is_reachable(program: &[Instr], at: usize) -> bool {
    let after_goto = at > 0 && program[at - 1].opcode == "Goto";
    let addr = program[at].addr;
    !after_goto
        || program
            .iter()
            .enumerate()
            .any(|(index, instr)| index != at && instr.p2 == addr)
}

/// `trigger` or `fk action`, from the `Halt` that ends a subprogram.
fn subprogram_kind(program: &[Instr]) -> &str {
    program
        .iter()
        .rev()
        .find(|instr| instr.opcode == "Halt" && !instr.p4.is_empty())
        .map_or("unnamed", |halt| halt.p4.as_str())
}

/// What an `OpenRead`/`OpenWrite` opens: from its comment when it names the
/// object, else from its root page.
fn resolve_open(instr: &Instr, schema: &SchemaView) -> Option<Object> {
    let comment = instr.comment.trim();
    if let Some(rest) = comment.strip_prefix("table=") {
        return Some(Object::Table(first_field(rest)));
    }
    if let Some(rest) = comment
        .strip_prefix("index=")
        .or_else(|| comment.strip_prefix('='))
    {
        let name = first_field(rest);
        let table = schema.index_table.get(&name)?.clone();
        return Some(Object::Index { name, table });
    }
    schema.by_root.get(&instr.p2).cloned()
}

fn first_field(rest: &str) -> String {
    rest.split(',')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// `k(N, …)` → the key's columns less the trailing rowid.
fn key_columns(p4: &str) -> Option<usize> {
    let inner = p4.strip_prefix("k(")?;
    let count: usize = inner.split([',', ')']).next()?.trim().parse().ok()?;
    count.checked_sub(1)
}

/// `key=[a..b]` → how many registers the seek key spans.
fn seek_key_len(comment: &str) -> Option<usize> {
    let range = comment.split("key=[").nth(1)?.split(']').next()?;
    let (start, end) = range.split_once("..")?;
    let (start, end): (usize, usize) = (start.trim().parse().ok()?, end.trim().parse().ok()?);
    end.checked_sub(start).map(|span| span + 1)
}

/// A seek is a point read, not a range, when it positions on the whole key of
/// a unique index and the loop ends at the first key past it: `SeekGE` then
/// `IdxGT` over the same registers, which is how an equality on every key
/// column compiles. Anything else, including a rowid range, is a range.
fn is_point_seek(cursor: &Cursor, program: &[Instr], at: usize, schema: &SchemaView) -> bool {
    let Object::Index { name, .. } = &cursor.object else {
        return false;
    };
    let Some(schema_columns) = schema.unique.get(name) else {
        return false;
    };
    let seek = &program[at];
    let full_key = matches!(
        (cursor.key_columns.or(*schema_columns), seek_key_len(&seek.comment)),
        (Some(columns), Some(keys)) if keys >= columns
    );
    let bounded_at_key = program[at + 1..]
        .iter()
        .take(3)
        .any(|next| next.opcode == "IdxGT" && next.p1 == seek.p1 && next.p3 == seek.p3);
    seek.opcode == "SeekGE" && full_key && bounded_at_key
}

fn excerpt(program: &[Instr], at: usize, also: Option<usize>) -> String {
    let start = at.saturating_sub(2);
    let end = (at + 3).min(program.len());
    let mut lines: Vec<&Instr> = program[start..end].iter().collect();
    if let Some(also) = also.filter(|also| *also < start || *also >= end) {
        lines.push(&program[also]);
    }
    lines.truncate(EXCERPT_LINES);
    lines
        .iter()
        .map(|instr| instr.render())
        .collect::<Vec<_>>()
        .join("\n")
}
