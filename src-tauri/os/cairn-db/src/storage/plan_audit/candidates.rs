//! A measurement, not a rule: how many statements seek an index whose leading
//! column is a type or status while binding a column that leads a better one.
//!
//! The planner keeps no statistics, so it can prefer `idx_artifacts_type` to the
//! job index beside it and walk every artifact of that type per job (5643). The
//! same shape cost `messages` (5930) and `events` (5618). The performance thread
//! decides whether it becomes a rule after reading the count, so nothing here
//! gates or reaches a production log: it runs only when the test log is set, and
//! its index catalog is loaded on that path alone.
//!
//! Binding is read from the statement's text, because bytecode does not say
//! which register holds a parameter a column was compared with. A join's two
//! tables can therefore lend each other a column name: this counts candidates,
//! it does not convict.

use std::collections::HashMap;

use crate::storage::{DbResult, RowExt};
use crate::turso::Connection;

use super::Classified;

/// The rule's name in the test log.
pub const LOW_CARDINALITY_SEEK: &str = "low-cardinality-seek";

/// Each index's columns and leading column, and which indexes each column leads.
#[derive(Debug, Default)]
pub struct IndexCatalog {
    columns: HashMap<String, Vec<String>>,
    leading: HashMap<String, String>,
    leads: HashMap<(String, String), Vec<String>>,
}

impl IndexCatalog {
    pub async fn load(conn: &Connection) -> DbResult<Self> {
        let mut names = Vec::new();
        let mut rows = conn
            .query(
                "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index' ORDER BY name",
                (),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            names.push((row.text(0)?, row.text(1)?));
        }
        drop(rows);
        let mut catalog = Self::default();
        for (index, table) in names {
            let mut info = conn
                .query(format!("PRAGMA index_info(\"{index}\")"), ())
                .await?;
            // An expression has no name.
            let mut columns = Vec::new();
            while let Some(row) = info.next().await? {
                columns.push(row.opt_text(2)?);
            }
            if let Some(Some(column)) = columns.first() {
                catalog
                    .leads
                    .entry((table, column.clone()))
                    .or_default()
                    .push(index.clone());
                catalog.leading.insert(index.clone(), column.clone());
            }
            catalog
                .columns
                .insert(index, columns.into_iter().flatten().collect());
        }
        Ok(catalog)
    }
}

/// One seek a better index might serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub table: String,
    /// The index the plan seeks, and the type or status column leading it.
    pub index: String,
    pub leading: String,
    /// The column the statement binds, and the index it leads.
    pub column: String,
    pub better_index: String,
}

fn is_low_cardinality(column: &str) -> bool {
    ["type", "status", "state", "kind"].iter().any(|word| {
        column == *word
            || column
                .strip_suffix(word)
                .is_some_and(|prefix| prefix.ends_with('_'))
    })
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Whether `sql` compares `column` with a parameter or another column, or tests
/// it against an `IN` list, in a form an index on it could serve. `+column`
/// keeps a column out of index selection, so it does not count.
fn binds(sql: &str, column: &str) -> bool {
    let (text, column) = (sql.to_ascii_lowercase(), column.to_ascii_lowercase());
    let bytes = text.as_bytes();
    let skip_space = |mut at: usize| {
        while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
            at += 1;
        }
        at
    };
    text.match_indices(&column).any(|(start, _)| {
        let end = start + column.len();
        if bytes.get(end).copied().is_some_and(is_word) || (start > 0 && is_word(bytes[start - 1]))
        {
            return false;
        }
        let mut name_start = start;
        while name_start > 0 && (is_word(bytes[name_start - 1]) || bytes[name_start - 1] == b'.') {
            name_start -= 1;
        }
        if name_start > 0 && bytes[name_start - 1] == b'+' {
            return false;
        }
        let next = skip_space(end);
        match bytes.get(next) {
            Some(b'=') => {
                let value = skip_space(next + 1);
                bytes.get(value).is_some_and(|byte| {
                    *byte == b'?' || byte.is_ascii_alphabetic() || *byte == b'_'
                })
            }
            Some(b'i') if bytes.get(next + 1) == Some(&b'n') => {
                bytes.get(skip_space(next + 2)) == Some(&b'(')
            }
            _ => false,
        }
    })
}

/// Every stepped, non-point seek in `classified` led by a low-cardinality column
/// where `sql` binds a column leading a different index of the same table.
pub fn low_cardinality_seeks(
    classified: &Classified,
    catalog: &IndexCatalog,
    sql: &str,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for seek in &classified.range_seeks {
        let Some(leading) = catalog.leading.get(&seek.index) else {
            continue;
        };
        if !is_low_cardinality(leading) {
            continue;
        }
        // A bound column in the sought index narrows that seek only with every
        // column before it bound too. Then the type or status column in front of it
        // costs nothing the other index would save.
        let narrowing: Vec<&String> = catalog
            .columns
            .get(&seek.index)
            .map(|columns| {
                columns
                    .iter()
                    .take_while(|column| *column == leading || binds(sql, column))
                    .collect()
            })
            .unwrap_or_default();
        let mut better: Vec<(&String, &String)> = catalog
            .leads
            .iter()
            .filter(|((table, column), _)| {
                *table == seek.table
                    && column != leading
                    && !narrowing.contains(&column)
                    && binds(sql, column)
            })
            .filter_map(|((_, column), indexes)| {
                indexes
                    .iter()
                    .find(|index| **index != seek.index)
                    .map(|index| (column, index))
            })
            .collect();
        better.sort();
        if let Some((column, better_index)) = better.first() {
            candidates.push(Candidate {
                table: seek.table.clone(),
                index: seek.index.clone(),
                leading: leading.clone(),
                column: (*column).clone(),
                better_index: (*better_index).clone(),
            });
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_low_cardinality_column_is_a_type_or_status_by_name() {
        for yes in ["type", "status", "state", "kind", "event_type", "run_state"] {
            assert!(is_low_cardinality(yes), "{yes}");
        }
        for no in ["job_id", "prototype", "estate", "kindness", "statement"] {
            assert!(!is_low_cardinality(no), "{no}");
        }
    }

    #[test]
    fn a_column_is_bound_by_a_parameter_a_column_or_a_list() {
        for sql in [
            "SELECT 1 FROM t WHERE job_id = ?1",
            "SELECT 1 FROM t WHERE job_id=?",
            "SELECT 1 FROM t a WHERE a.job_id = j.id",
            "SELECT 1 FROM t WHERE job_id IN (?, ?)",
            "SELECT 1 FROM t WHERE JOB_ID in(SELECT id FROM u)",
        ] {
            assert!(binds(sql, "job_id"), "{sql}");
        }
        for sql in [
            "SELECT job_id FROM t",
            "SELECT 1 FROM t WHERE job_id = 'x'",
            "SELECT 1 FROM t WHERE job_id IS NULL",
            "SELECT 1 FROM t WHERE my_job_id = ?",
            "SELECT 1 FROM t WHERE job_id2 = ?",
            "SELECT 1 FROM t WHERE +job_id IN (?, ?)",
            "SELECT 1 FROM t a WHERE +a.job_id = ?",
            "SELECT 1 FROM t WHERE job_id INDEXED",
        ] {
            assert!(!binds(sql, "job_id"), "{sql}");
        }
    }
}
