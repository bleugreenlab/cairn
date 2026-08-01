//! Job models for database records (replaces Timeline Node)

use crate::storage::{DbResult, RowExt};

/// Canonical column projection for rows mapped by [`db_job_from_row`].
///
/// Keep this list in exactly the same order as the positional reads in
/// `db_job_from_row`; all jobs queries should import this constant rather than
/// spelling out their own projection.
pub const JOB_COLUMNS: &str = "id, execution_id, recipe_node_id, parent_job_id,
    branch, base_commit, current_session_id, resume_session_id, status, agent_config_id,
    issue_id, project_id, task_description, created_at, updated_at, completed_at,
    parent_tool_use_id, task_index, started_at, model, node_name, base_branch,
    current_turn_id, uri_segment, pack_anchor";

#[derive(Debug, Clone)]
pub struct DbJob {
    pub id: String,
    pub execution_id: Option<String>,
    pub recipe_node_id: Option<String>,
    pub parent_job_id: Option<String>,
    pub branch: Option<String>,
    /// An **archival record** of where this job's branch was last anchored — not
    /// a coordinate, and never the answer to "where does this job's work sit?"
    ///
    /// Ask the store instead. `branch::resolve_current_for_read` resolves a
    /// job's live logical head in roughly 0.4ms, and run placement, the
    /// materialization read authority, and the check impact gate all derive from
    /// it. Three separate surfaces once reached for this row because it looked
    /// like a usable commit id, and each produced a defect that took an issue to
    /// unwind: a builder stranded behind a lineage guard, phantom full-check
    /// waves on zero-delta planners, and a +6k/−3k diff on a +350/−62 branch
    /// (CAIRN-3094 / 3108 / 3150, ended by CAIRN-3224).
    ///
    /// What legitimately remains (CAIRN-3226 weighed and kept each):
    ///
    /// - It seeds [`Self::pack_anchor`] when a child inherits a parent's lineage
    ///   and the parent's own anchor is NULL. Archival wants an archival value.
    /// - It is a *verified* fallback for a job inheriting a branch whose
    ///   bookmark no longer resolves, used only when the store can still produce
    ///   the commit it names.
    /// - The base-advance reconcile records the dest it rebased a branch onto.
    ///   That write is deliberately not a compare-and-swap: nothing downstream
    ///   depends on the ordering, and the CAS that used to guard it never
    ///   provided any.
    ///
    /// Two things are easy to assume and both are false. It is **not** refreshed
    /// for every job: `prepare_job` resolves it live, but requires a
    /// `recipe_node_id`, so sub-agent tasks, ephemeral calls, and standalone
    /// workflows — all inserted with a NULL node by
    /// `insert_child_job_session_run` — keep the value copied from their parent
    /// for their whole life. And it is **not** dropped or renamed despite
    /// naming what it is not: `jobs` is a synced `ProjectScoped` table, and
    /// destructive column changes on synced replica tables break Turso sync
    /// triggers (see the TEAM_TAIL note in `storage::migrations`).
    pub base_commit: Option<String>,
    /// Nearest durable ancestor commit reachable from the project default
    /// branch, captured alongside `base_commit`. NULL
    /// when unresolvable.
    pub pack_anchor: Option<String>,
    pub current_session_id: Option<String>,
    pub resume_session_id: Option<String>,
    pub status: String,
    pub agent_config_id: Option<String>,
    pub issue_id: Option<String>,
    pub project_id: String,
    pub task_description: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<String>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<String>,
    pub node_name: Option<String>,
    pub base_branch: Option<String>,
    pub current_turn_id: Option<String>,
    pub uri_segment: Option<String>,
}

pub fn db_job_from_row(row: &turso::Row) -> DbResult<DbJob> {
    Ok(DbJob {
        id: row.text(0)?,
        execution_id: row.opt_text(1)?,
        recipe_node_id: row.opt_text(2)?,
        parent_job_id: row.opt_text(3)?,
        branch: row.opt_text(4)?,
        base_commit: row.opt_text(5)?,
        current_session_id: row.opt_text(6)?,
        resume_session_id: row.opt_text(7)?,
        status: row.text(8)?,
        agent_config_id: row.opt_text(9)?,
        issue_id: row.opt_text(10)?,
        project_id: row.text(11)?,
        task_description: row.opt_text(12)?,
        created_at: row.i64(13)? as i32,
        updated_at: row.i64(14)? as i32,
        completed_at: row.opt_i64(15)?.map(|value| value as i32),
        parent_tool_use_id: row.opt_text(16)?,
        task_index: row.opt_i64(17)?.map(|value| value as i32),
        started_at: row.opt_i64(18)?.map(|value| value as i32),
        model: row.opt_text(19)?,
        node_name: row.opt_text(20)?,
        base_branch: row.opt_text(21)?,
        current_turn_id: row.opt_text(22)?,
        uri_segment: row.opt_text(23)?,
        pack_anchor: row.opt_text(24)?,
    })
}

/// Load the live (non-cancelled) job for a recipe node within an execution,
/// preferring the newest attempt.
///
/// Restart-node archives a node's prior job as `cancelled` and creates a fresh
/// one (see `execution::advancement::restart_node`), so a single recipe node can
/// own several job rows at once. Every per-node *job* lookup must resolve to the
/// live attempt, never the cancelled archive, or downstream readiness and input
/// resolution read stale state after a restart. This is the one canonical
/// node→job lookup; callers that previously spelled their own
/// `WHERE execution_id = ? AND recipe_node_id = ? LIMIT 1` should route here.
///
/// Cascade reads that key on `status = 'failed'` (e.g. `upstream_failed_conn`)
/// are already safe — a cancelled row never matches — and intentionally stay as
/// they are.
pub async fn load_live_job_by_execution_node_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
) -> DbResult<Option<DbJob>> {
    let sql = format!(
        "SELECT {JOB_COLUMNS}
         FROM jobs
         WHERE execution_id = ?1 AND recipe_node_id = ?2 AND status <> 'cancelled'
         ORDER BY created_at DESC
         LIMIT 1"
    );
    let mut rows = conn.query(&sql, (execution_id, recipe_node_id)).await?;
    rows.next()
        .await?
        .map(|row| db_job_from_row(&row))
        .transpose()
}

#[derive(Debug)]
pub struct NewJob<'a> {
    pub id: &'a str,
    pub execution_id: Option<&'a str>,
    pub recipe_node_id: Option<&'a str>,
    pub parent_job_id: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub base_commit: Option<&'a str>,
    pub pack_anchor: Option<&'a str>,
    pub current_session_id: Option<&'a str>,
    pub resume_session_id: Option<&'a str>,
    pub status: &'a str,
    pub agent_config_id: Option<&'a str>,
    pub issue_id: Option<&'a str>,
    pub project_id: &'a str,
    pub task_description: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub completed_at: Option<i32>,
    pub parent_tool_use_id: Option<&'a str>,
    pub task_index: Option<i32>,
    pub started_at: Option<i32>,
    pub model: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub base_branch: Option<&'a str>,
    pub current_turn_id: Option<&'a str>,
    pub uri_segment: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct UpdateJobChangeset<'a> {
    pub branch: Option<Option<&'a str>>,
    pub base_commit: Option<Option<&'a str>>,
    pub pack_anchor: Option<Option<&'a str>>,
    pub current_session_id: Option<Option<&'a str>>,
    pub resume_session_id: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub updated_at: Option<i32>,
    pub completed_at: Option<Option<i32>>,
    pub started_at: Option<Option<i32>>,
    pub model: Option<Option<&'a str>>,
}

/// A structural fence around the one column that keeps being mistaken for a
/// coordinate.
///
/// The stale-coordinate family was not reached through the Rust field on
/// [`DbJob`] — it was reached through raw SQL. `durable_content.rs` wrote its
/// own `SELECT base_commit FROM jobs`, and `checks_turn_end.rs` selected
/// `j.base_commit` into a coordinate struct. Neither is visible to the compiler,
/// to a type, or to a doc comment, so neither could be prevented by any of them.
///
/// **What this covers.** A string literal — regular or raw, in any case — that
/// names `base_commit` and puts `jobs` in table position. Table position is the
/// token after `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`, or a comma, so a join
/// (`... FROM projects p JOIN jobs j ON ...`), a comma-separated `FROM` list, and
/// a lowercase query are all caught, not just the three shapes that happened to
/// be in the tree when this was written. [`literal_reads_jobs_base_commit`] is a
/// pure function with its own positive and negative fixtures, so the bypasses are
/// proven closed rather than assumed to be.
///
/// **What it deliberately does not cover.** A whole-row projection —
/// `SELECT {JOB_COLUMNS} FROM jobs` into [`DbJob`] — does not name the column,
/// and flagging it would flag every job load in the codebase. That surface is
/// fenced differently and adequately: it lands on [`DbJob::base_commit`], whose
/// documentation states what the value is, and the field access that follows is
/// ordinary Rust the compiler and a reader can both see. This guard exists for
/// the surface neither of those reaches.
///
/// **What it cannot cover.** SQL assembled at runtime from fragments that
/// individually name neither the column nor the table. That is a real limit, not
/// a closed one.
///
/// The site list is file-level, so moving code inside a sanctioned file never
/// churns it.
#[cfg(test)]
mod jobs_base_commit_sql_reach {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    /// Files permitted to name `jobs.base_commit` in SQL, and why.
    ///
    /// - `orchestrator/base_advance.rs` — the reconcile records the dest it
    ///   rebased a branch onto, and snapshots the record to decide whether a
    ///   write is needed. Reader and writer are one code path.
    /// - `execution/jobs/persistence.rs` — writes the record live at branch prep
    ///   (`update_job_coordinate`) and reads it as the archival seed for
    ///   `pack_anchor`.
    /// - `execution/advancement/job_creation.rs` — inserts a DAG job with the
    ///   column NULL; `prepare_job` fills it live.
    /// - `effects/dag/delegation.rs` — reparenting copies the parent's record
    ///   onto a delegated child alongside its branch.
    /// - `storage/migrations.rs` — schema lineage and its fixtures.
    /// - `diff.rs` — a test fixture that seeds a deliberately stale record to
    ///   prove the live derivation ignores it.
    const SANCTIONED_SQL_SITES: &[&str] = &[
        "os/cairn-core/src/diff.rs",
        "os/cairn-core/src/effects/dag/delegation.rs",
        "os/cairn-core/src/execution/advancement/job_creation.rs",
        "os/cairn-core/src/execution/jobs/persistence.rs",
        "os/cairn-core/src/orchestrator/base_advance.rs",
        "os/cairn-db/src/storage/migrations.rs",
    ];

    const SCANNED_ROOTS: &[&str] = &[
        "os/cairn-core/src",
        "os/cairn-db/src",
        "cairn-transport/src",
        "cairn-executor/src",
    ];

    /// Tokens after which the next identifier names a table. The comma is here
    /// because a comma-separated `FROM` list is a join by another spelling, and
    /// requiring a keyword immediately before `jobs` would miss it.
    const TABLE_POSITION: &[&str] = &["from", "join", "into", "update", "table", ","];

    fn src_tauri_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("cairn-db sits two levels below the src-tauri root")
            .to_path_buf()
    }

    /// Whether a raw string's terminator sits at `at`: a quote followed by the
    /// same number of hashes that opened it.
    fn raw_string_ends_at(chars: &[char], at: usize, hashes: usize) -> bool {
        chars[at] == '"' && (1..=hashes).all(|offset| chars.get(at + offset) == Some(&'#'))
    }

    /// Every Rust string literal in `source`, regular and raw alike, with `//`
    /// comment lines dropped first so prose about the column (of which there is a
    /// great deal, deliberately) never registers as a SQL site.
    fn string_literals(source: &str) -> Vec<String> {
        let code = source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let chars: Vec<char> = code.chars().collect();
        let mut literals = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            // A raw string opener is an `r` that does not continue an
            // identifier, then any number of hashes, then a quote.
            let starts_word = i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
            if chars[i] == 'r' && starts_word {
                let mut cursor = i + 1;
                let mut hashes = 0;
                while cursor < chars.len() && chars[cursor] == '#' {
                    hashes += 1;
                    cursor += 1;
                }
                if chars.get(cursor) == Some(&'"') {
                    cursor += 1;
                    let start = cursor;
                    while cursor < chars.len() && !raw_string_ends_at(&chars, cursor, hashes) {
                        cursor += 1;
                    }
                    literals.push(chars[start..cursor.min(chars.len())].iter().collect());
                    i = cursor + 1 + hashes;
                    continue;
                }
            }
            if chars[i] == '"' {
                let mut cursor = i + 1;
                let mut literal = String::new();
                while cursor < chars.len() && chars[cursor] != '"' {
                    if chars[cursor] == '\\' {
                        cursor += 2;
                        continue;
                    }
                    literal.push(chars[cursor]);
                    cursor += 1;
                }
                literals.push(literal);
                i = cursor + 1;
                continue;
            }
            i += 1;
        }
        literals
    }

    /// Lowercased identifier tokens, with commas surviving as tokens of their
    /// own so a `FROM` list can be read positionally.
    fn sql_tokens(literal: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        for ch in literal.chars() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                current.push(ch.to_ascii_lowercase());
                continue;
            }
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            if ch == ',' {
                tokens.push(",".to_string());
            }
        }
        if !current.is_empty() {
            tokens.push(current);
        }
        tokens
    }

    /// Whether one string literal reads or writes `jobs.base_commit`.
    fn literal_reads_jobs_base_commit(literal: &str) -> bool {
        if !literal.to_ascii_lowercase().contains("base_commit") {
            return false;
        }
        let tokens = sql_tokens(literal);
        tokens
            .windows(2)
            .any(|pair| TABLE_POSITION.contains(&pair[0].as_str()) && pair[1] == "jobs")
    }

    fn names_jobs_base_commit_in_sql(source: &str) -> bool {
        string_literals(source)
            .iter()
            .any(|literal| literal_reads_jobs_base_commit(literal))
    }

    /// The guard's own file, which the tree scan skips.
    ///
    /// Its fixtures are deliberately-written examples of exactly the queries it
    /// detects, so scanning itself would report itself forever. Nothing hides
    /// behind this: a real query added to this file would sit directly beside
    /// the rule that would have flagged it, and
    /// [`only_sanctioned_sites_name_jobs_base_commit_in_sql`] asserts that the
    /// skipped file really does still look like a query site — if the fixtures
    /// ever stop being detectable, that is a broken guard, not a clean tree.
    fn guard_source_file() -> &'static str {
        file!()
    }

    fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rust_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// Every shape a jobs-table query can take to expose the column. Each of
    /// these passed the first version of this guard, which recognized only
    /// `FROM jobs`, `INTO jobs`, and `UPDATE jobs` as written.
    #[test]
    fn the_detector_catches_every_shape_of_a_jobs_base_commit_query() {
        for sql in [
            "SELECT base_commit FROM jobs WHERE id = ?1",
            "UPDATE jobs SET base_commit = ?2 WHERE id = ?1",
            "INSERT INTO jobs(id, base_commit) VALUES(?1, ?2)",
            // A join never puts the table in a FROM clause of its own.
            "SELECT j.base_commit FROM projects p JOIN jobs j ON j.project_id = p.id",
            // A comma-separated FROM list is a join by another spelling.
            "SELECT j.base_commit FROM projects p, jobs j WHERE j.project_id = p.id",
            // SQL is case-insensitive and this codebase is not uniformly upper.
            "select base_commit from jobs where id = ?1",
            // A fully qualified column reference.
            "SELECT jobs.base_commit FROM jobs",
            // A subquery reaching the column from an unrelated outer table.
            "SELECT (SELECT base_commit FROM jobs WHERE id = e.job_id) FROM executions e",
        ] {
            assert!(
                literal_reads_jobs_base_commit(sql),
                "this reads jobs.base_commit and was not flagged: {sql}"
            );
        }
    }

    /// A raw string literal is a query like any other.
    #[test]
    fn the_detector_reads_raw_string_literals() {
        let source = "const QUERY: &str = r#\"SELECT j.base_commit FROM projects p \
                      JOIN jobs j ON j.project_id = p.id\"#;";
        assert!(
            names_jobs_base_commit_in_sql(source),
            "a raw string literal must not be a bypass"
        );
    }

    /// The negative half. A guard that flags everything is no more useful than
    /// one that flags nothing.
    #[test]
    fn the_detector_leaves_alone_what_is_not_a_jobs_coordinate_read() {
        for sql in [
            // Other tables have a base_commit of their own.
            "SELECT content_hash, base_commit, tip_commit FROM object_transfers",
            // A rebuild table is not the jobs table.
            "SELECT base_commit FROM jobs_new",
            // Neither is a differently-named one that merely starts the same.
            "SELECT base_commit FROM job_terminals",
            // A whole-row projection does not name the column. See the module
            // doc: this is out of scope on purpose, not by oversight.
            "SELECT id, branch, status, pack_anchor FROM jobs WHERE id = ?1",
        ] {
            assert!(
                !literal_reads_jobs_base_commit(sql),
                "this is not a jobs.base_commit read and was flagged: {sql}"
            );
        }

        // Prose about the column is not a query, whether it sits in a comment or
        // inside a message string — including this guard's own failure text.
        assert!(!names_jobs_base_commit_in_sql(
            "// jobs.base_commit is resolved live from the store, never read from the row\n"
        ));
        assert!(!literal_reads_jobs_base_commit(
            "these files name jobs.base_commit in SQL; resolve it live from the store instead"
        ));
    }

    #[test]
    fn only_sanctioned_sites_name_jobs_base_commit_in_sql() {
        let root = src_tauri_root();
        let mut files = Vec::new();
        for scanned in SCANNED_ROOTS {
            collect_rust_files(&root.join(scanned), &mut files);
        }
        assert!(
            !files.is_empty(),
            "the scan found no Rust sources under {}; the guard is not actually looking at \
             anything",
            root.display()
        );

        // The skip below is only sound while this file's fixtures still read as
        // the queries they are meant to imitate.
        let guard_source =
            std::fs::read_to_string(root.join(guard_source_file())).unwrap_or_else(|error| {
                panic!(
                    "the guard's own source must be readable at {}: {error}",
                    guard_source_file()
                )
            });
        assert!(
            names_jobs_base_commit_in_sql(&guard_source),
            "this guard's own fixtures no longer read as jobs.base_commit queries, so they have \
             stopped testing anything and the skip below hides a real file"
        );

        let observed: BTreeSet<String> = files
            .iter()
            .filter(|path| {
                std::fs::read_to_string(path)
                    .map(|text| names_jobs_base_commit_in_sql(&text))
                    .unwrap_or(false)
            })
            .map(|path| {
                path.strip_prefix(&root)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .filter(|relative| relative != guard_source_file())
            .collect();
        let sanctioned: BTreeSet<String> = SANCTIONED_SQL_SITES
            .iter()
            .map(|site| (*site).to_string())
            .collect();

        let unsanctioned: Vec<&String> = observed.difference(&sanctioned).collect();
        assert!(
            unsanctioned.is_empty(),
            "these files query jobs.base_commit and are not sanctioned to: {unsanctioned:?}\n\
             \n\
             That column is an archival record of where a branch was last anchored, not a \
             coordinate. If you need where a job's work currently sits, resolve it live from the \
             store (`branch::resolve_current_for_read`), the way run placement, the \
             materialization read authority, and the check impact gate all do. Querying the row \
             instead is what produced CAIRN-3094, 3108, and 3150.\n\
             \n\
             If the new site is genuinely archival, add it to SANCTIONED_SQL_SITES with the \
             reason."
        );

        let departed: Vec<&String> = sanctioned.difference(&observed).collect();
        assert!(
            departed.is_empty(),
            "these sanctioned sites no longer query jobs.base_commit: {departed:?}\n\
             Remove them from SANCTIONED_SQL_SITES so the list keeps meaning something."
        );
    }
}
