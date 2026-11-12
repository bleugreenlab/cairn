//! SQL statement scanning and policy shared by database resources.

/// Where a thread's rulings went. Thread migration 0002 drops
/// `_cairn_rulings` once its rows are imported as tenets, so SQL that still
/// names the table is refused with this rather than with a missing-table error
/// that does not say where the decisions live now.
pub(crate) const RULINGS_ARE_TENETS: &str =
    "rulings are project tenets now: propose with an append to cairn:@/tenets";

/// The append that replaces a ruling insert.
pub(crate) const TENET_APPEND_EXAMPLE: &str =
    "write({changes:[{target:\"cairn:@/tenets\",mode:\"append\",payload:{statement:\"...\"}}]})";

/// Refuse SQL that names the retired `_cairn_rulings` table, in any
/// identifier form. A string literal that merely mentions it is not refused.
///
/// Called by the thread store's write policy, its `cairn:~/db` read, and wake
/// condition arming.
pub(crate) fn refuse_retired_rulings(sql: &str) -> Result<(), String> {
    if sql_identifiers(sql)
        .iter()
        .any(|identifier| identifier.eq_ignore_ascii_case("_cairn_rulings"))
    {
        return Err(format!(
            "{RULINGS_ARE_TENETS}, for example `{TENET_APPEND_EXAMPLE}`; read them at cairn:@/tenets"
        ));
    }
    Ok(())
}

/// The table one write statement targets and what it does there, for
/// attributing a SQL write's `db-change` events: `insert`, `update`, or
/// `delete`, with DDL on a table counted as `update`. Recognized forms are
/// `INSERT [OR x] INTO`, `REPLACE INTO`, `UPDATE [OR x]`, `DELETE FROM`, and
/// `CREATE [TEMP] TABLE` / `DROP TABLE` / `ALTER TABLE` with `IF [NOT] EXISTS`.
///
/// `None` for anything else, including WITH-prefixed DML, other DDL (indexes,
/// views, triggers), and a schema-qualified name: a caller treats those as
/// touching any table. A trigger's writes to other tables are never seen here.
pub(crate) fn statement_target(sql: &str) -> Option<(String, &'static str)> {
    let words = sql_identifiers(sql);
    let keyword = |index: usize| words.get(index).map(|word| word.to_ascii_uppercase());
    let is = |index: usize, expected: &str| keyword(index).as_deref() == Some(expected);
    let (index, action) = match keyword(0)?.as_str() {
        "INSERT" | "REPLACE" => {
            let into = if is(1, "OR") { 3 } else { 1 };
            if !is(into, "INTO") {
                return None;
            }
            (into + 1, "insert")
        }
        "UPDATE" => (if is(1, "OR") { 3 } else { 1 }, "update"),
        "DELETE" if is(1, "FROM") => (2, "delete"),
        "CREATE" | "DROP" | "ALTER" => {
            let mut index = 1;
            if is(0, "CREATE") && (is(index, "TEMP") || is(index, "TEMPORARY")) {
                index += 1;
            }
            if !is(index, "TABLE") {
                return None;
            }
            index += 1;
            if is(index, "IF") {
                index += if is(index + 1, "NOT") { 3 } else { 2 };
            }
            (index, "update")
        }
        _ => return None,
    };
    let table = words.get(index)?;
    // `main.t` scans as two words; naming `main` would misattribute the write.
    if ["main", "temp"]
        .iter()
        .any(|schema| table.eq_ignore_ascii_case(schema))
    {
        return None;
    }
    Some((table.clone(), action))
}

/// One table or view of a database's catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogEntry {
    pub name: String,
    /// A view's defining `CREATE VIEW` statement; `None` for a table.
    pub view_sql: Option<String>,
}

/// The tables a read statement reads, spelled as the catalog spells them and
/// sorted: every identifier the SQL names that is a table, and, for each view
/// it names, the tables that view reads, recursively.
///
/// An over-approximation by design. A column or alias that shares a table's
/// name adds that table, which costs a subscriber an extra re-read and never
/// staleness; a table the SQL actually reads is always named by an identifier
/// or reached through a view, so it is never missed. String literals and
/// comments name nothing.
pub(crate) fn referenced_tables(sql: &str, catalog: &[CatalogEntry]) -> Vec<String> {
    let mut tables: Vec<String> = Vec::new();
    let mut expanded_views = std::collections::HashSet::new();
    let mut pending = vec![sql.to_string()];
    while let Some(text) = pending.pop() {
        for identifier in sql_identifiers(&text) {
            let Some(entry) = catalog
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case(&identifier))
            else {
                continue;
            };
            match &entry.view_sql {
                None => {
                    if !tables
                        .iter()
                        .any(|table| table.eq_ignore_ascii_case(&entry.name))
                    {
                        tables.push(entry.name.clone());
                    }
                }
                Some(view) => {
                    if expanded_views.insert(entry.name.to_ascii_lowercase()) {
                        pending.push(view.clone());
                    }
                }
            }
        }
    }
    tables.sort();
    tables
}

/// Validate the single, read-only statement accepted by SQL read projections.
pub(crate) fn validate_read_only_sql(sql: &str) -> Result<(), String> {
    let statements = split_statements(sql);
    if statements.is_empty() {
        return Err("SQL query is empty".to_string());
    }
    if statements.len() != 1 {
        return Err("SQL query must contain exactly one statement".to_string());
    }

    let tokens = sql_tokens(&statements[0]);
    let statement = explain_target(&tokens)?;
    validate_read_tokens(statement)
}

/// Return identifiers named by SQL, including SQLite's quoted identifier forms.
/// String literals and comments are excluded.
pub(crate) fn sql_identifiers(sql: &str) -> Vec<String> {
    let mut chars = sql.char_indices().peekable();
    let mut identifiers = Vec::new();
    while let Some((_, ch)) = chars.next() {
        match ch {
            '\'' => skip_quoted(ch, &mut chars),
            '"' | '`' => identifiers.push(read_quoted_identifier(ch, &mut chars)),
            '[' => identifiers.push(read_bracket_identifier(&mut chars)),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut identifier = String::from(c);
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_alphanumeric() || next == '_' {
                        identifier.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                identifiers.push(identifier);
            }
            _ => {}
        }
    }
    identifiers
}

fn read_quoted_identifier(
    quote: char,
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> String {
    let mut identifier = String::new();
    while let Some((_, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
                identifier.push(quote);
            } else {
                break;
            }
        } else {
            identifier.push(ch);
        }
    }
    identifier
}

fn read_bracket_identifier(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) -> String {
    let mut identifier = String::new();
    for (_, ch) in chars.by_ref() {
        if ch == ']' {
            break;
        }
        identifier.push(ch);
    }
    identifier
}

/// Split SQL at top-level semicolons, ignoring semicolons in quoted values,
/// identifiers, and comments.
pub(crate) fn split_statements(sql: &str) -> Vec<String> {
    let mut chars = sql.char_indices().peekable();
    let mut start = 0;
    let mut statements = Vec::new();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            ';' => {
                push_statement(&mut statements, &sql[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    push_statement(&mut statements, &sql[start..]);
    statements
}

/// How many values a statement binds, numbered the way SQLite numbers its
/// parameters: a bare `?` takes one more than the highest index assigned so far,
/// `?NNN` takes index NNN, and `:name`, `@name` and `$name` take the next index
/// unless the same name was already seen. The count is the highest index.
/// Placeholders inside quoted values, identifiers and comments are not
/// parameters.
///
/// Turso binds a parameter that was not given a value as NULL, so a statement
/// sent with too few values runs rather than failing: `WHERE post = ?` then
/// matches nothing and the write reports success with no rows changed.
pub(crate) fn parameter_count(sql: &str) -> usize {
    fn is_word(ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$' || !ch.is_ascii()
    }
    let mut chars = sql.char_indices().peekable();
    let mut highest = 0usize;
    let mut names: Vec<String> = Vec::new();
    while let Some((_, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            '?' => {
                let mut digits = String::new();
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_digit() {
                        digits.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                highest = match digits.parse::<usize>() {
                    Ok(index) => highest.max(index),
                    Err(_) => highest + 1,
                };
            }
            ':' | '@' | '$' if chars.peek().is_some_and(|(_, next)| is_word(*next)) => {
                let mut name = String::from(ch);
                while let Some((_, next)) = chars.peek().copied() {
                    if is_word(next) {
                        name.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if !names.contains(&name) {
                    names.push(name);
                    highest += 1;
                }
            }
            // A whole word, so a `$` inside an identifier is not a parameter.
            c if is_word(c) => {
                while chars.peek().is_some_and(|(_, next)| is_word(*next)) {
                    chars.next();
                }
            }
            _ => {}
        }
    }
    highest
}

/// The parts of a `CREATE [UNIQUE] INDEX` statement, as SQLite's catalog keeps
/// its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexDefinition {
    pub unique: bool,
    /// Each indexed column or expression as written, with any `COLLATE` or
    /// `ASC`/`DESC` suffix.
    pub terms: Vec<String>,
    /// A partial index's `WHERE` predicate. Whether the planner can use the
    /// index depends on it, so it is never dropped.
    pub predicate: Option<String>,
}

/// Parse a catalog `CREATE INDEX` statement. `None` means the text does not
/// have the expected shape; callers then show the statement itself.
pub(crate) fn parse_create_index(sql: &str) -> Option<IndexDefinition> {
    let tokens = sql_tokens(sql);
    if tokens.first().map(String::as_str) != Some("CREATE") {
        return None;
    }
    let unique = match tokens.get(1).map(String::as_str) {
        Some("UNIQUE") => true,
        Some("INDEX") => false,
        _ => return None,
    };

    // The first parenthesis opens the term list: everything before it is
    // keywords and (possibly quoted) names.
    let mut chars = sql.char_indices().peekable();
    let mut depth = 0usize;
    let mut open = None;
    let mut separators = Vec::new();
    let close = loop {
        let (idx, ch) = chars.next()?;
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            '(' => {
                open.get_or_insert(idx);
                depth += 1;
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    break idx;
                }
            }
            ',' if depth == 1 => separators.push(idx),
            _ => {}
        }
    };
    let open = open?;

    let mut terms = Vec::with_capacity(separators.len() + 1);
    let mut start = open + 1;
    for end in separators.into_iter().chain(std::iter::once(close)) {
        let term = sql[start..end].trim();
        if term.is_empty() {
            return None;
        }
        terms.push(collapse_whitespace(term));
        start = end + 1;
    }

    let rest = sql[close + 1..].trim().trim_end_matches(';').trim_end();
    let predicate = if rest.is_empty() {
        None
    } else {
        let keyword = rest.get(..5)?;
        let after = &rest[5..];
        if !keyword.eq_ignore_ascii_case("WHERE")
            || after
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return None;
        }
        let predicate = after.trim();
        if predicate.is_empty() {
            return None;
        }
        Some(collapse_whitespace(predicate))
    };
    Some(IndexDefinition {
        unique,
        terms,
        predicate,
    })
}

/// Collapse each whitespace run outside quoted text to one space, so a
/// multi-line definition reads on one line with its literals intact.
pub(crate) fn collapse_whitespace(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut closing_quote = None;
    let mut pending_space = false;
    for ch in sql.trim().chars() {
        if let Some(quote) = closing_quote {
            out.push(ch);
            if ch == quote {
                closing_quote = None;
            }
            continue;
        }
        if ch.is_whitespace() {
            pending_space = true;
            continue;
        }
        if std::mem::take(&mut pending_space) {
            out.push(' ');
        }
        closing_quote = match ch {
            '\'' | '"' | '`' => Some(ch),
            '[' => Some(']'),
            _ => None,
        };
        out.push(ch);
    }
    out
}

fn push_statement(statements: &mut Vec<String>, candidate: &str) {
    let candidate = candidate.trim();
    if !candidate.is_empty() {
        statements.push(candidate.to_string());
    }
}

pub(crate) fn sql_tokens(sql: &str) -> Vec<String> {
    let mut chars = sql.char_indices().peekable();
    let mut tokens = Vec::new();
    while let Some((_, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut token = String::from(c);
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_alphanumeric() || next == '_' {
                        token.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(token.to_ascii_uppercase());
            }
            _ => {}
        }
    }
    tokens
}

fn explain_target(tokens: &[String]) -> Result<&[String], String> {
    if tokens.first().map(String::as_str) != Some("EXPLAIN") {
        return Ok(tokens);
    }
    let rest = if tokens.get(1).map(String::as_str) == Some("QUERY")
        && tokens.get(2).map(String::as_str) == Some("PLAN")
    {
        &tokens[3..]
    } else {
        &tokens[1..]
    };
    if rest.is_empty() {
        Err("EXPLAIN requires a statement to explain".to_string())
    } else {
        Ok(rest)
    }
}

fn validate_read_tokens(tokens: &[String]) -> Result<(), String> {
    let first = tokens
        .first()
        .map(String::as_str)
        .ok_or_else(|| "SQL query is empty".to_string())?;
    match first {
        "SELECT" => Ok(()),
        "WITH" => {
            const MUTATING: &[&str] = &[
                "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH",
                "DETACH", "REPLACE", "VACUUM", "REINDEX", "ANALYZE",
            ];
            if let Some(token) = tokens.iter().find(|token| MUTATING.contains(&token.as_str())) {
                Err(format!("SQL query is not read-only: contains {token}"))
            } else {
                Ok(())
            }
        }
        "PRAGMA" => validate_schema_pragma(tokens),
        other => Err(format!(
            "Unsupported SQL statement '{other}'; cairn://db permits SELECT, read-only WITH, EXPLAIN, and schema PRAGMAs"
        )),
    }
}

pub(crate) fn validate_schema_pragma(tokens: &[String]) -> Result<(), String> {
    const ALLOWED: &[&str] = &["TABLE_LIST", "TABLE_INFO", "INDEX_LIST", "INDEX_INFO"];
    match tokens.get(1).map(String::as_str) {
        Some(name) if ALLOWED.contains(&name) => Ok(()),
        Some(name) => Err(format!(
            "Unsupported PRAGMA '{name}'; allowed schema PRAGMAs are table_list, table_info, index_list, and index_info"
        )),
        None => Err("PRAGMA requires a schema pragma name".to_string()),
    }
}

fn skip_quoted(quote: char, chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    while let Some((_, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
            } else {
                break;
            }
        }
    }
}

fn skip_bracket_identifier(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    for (_, ch) in chars.by_ref() {
        if ch == ']' {
            break;
        }
    }
}

fn skip_line_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    for (_, ch) in chars.by_ref() {
        if ch == '\n' {
            break;
        }
    }
}

fn skip_block_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    let mut previous = '\0';
    for (_, ch) in chars.by_ref() {
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_index_parsing_keeps_terms_uniqueness_and_predicate() {
        assert_eq!(
            parse_create_index("CREATE INDEX idx_t_a ON t(a)"),
            Some(IndexDefinition {
                unique: false,
                terms: vec!["a".into()],
                predicate: None,
            })
        );
        assert_eq!(
            parse_create_index(
                "create unique index if not exists \"odd (name\" on [t](\n  a COLLATE NOCASE,\n  b DESC\n)\nwhere state IN ('pending',\n   'two  spaces')"
            ),
            Some(IndexDefinition {
                unique: true,
                terms: vec!["a COLLATE NOCASE".into(), "b DESC".into()],
                predicate: Some("state IN ('pending', 'two  spaces')".into()),
            })
        );
        assert_eq!(
            parse_create_index("CREATE INDEX e ON t(lower(name), coalesce(a, ','))"),
            Some(IndexDefinition {
                unique: false,
                terms: vec!["lower(name)".into(), "coalesce(a, ',')".into()],
                predicate: None,
            })
        );
        for unparseable in [
            "CREATE TABLE t(a)",
            "CREATE INDEX i ON t()",
            "CREATE INDEX i ON t(a",
            "CREATE INDEX i ON t(a) WHEREVER",
            "CREATE INDEX i ON t(a) WHERE",
        ] {
            assert_eq!(parse_create_index(unparseable), None, "{unparseable}");
        }
    }

    #[test]
    fn parameter_count_follows_sqlite_numbering() {
        for (sql, expected) in [
            ("UPDATE posts SET why = ? WHERE post = ?", 2),
            ("SELECT 1", 0),
            ("INSERT INTO t VALUES ('?', \"?\", [?], ?) -- ?", 1),
            ("SELECT ? /* ? ? */ + ?", 2),
            ("SELECT ?3", 3),
            ("SELECT ?2, ?", 3),
            ("SELECT ?1, ?1", 1),
            ("UPDATE t SET a = :v, b = :v WHERE id = :id", 2),
            ("SELECT @a, $b, :c", 3),
            ("SELECT price$ FROM t WHERE x = ?", 1),
            ("SELECT '2020-01-01 10:30'", 0),
        ] {
            assert_eq!(parameter_count(sql), expected, "{sql}");
        }
    }

    #[test]
    fn identifier_scanner_keeps_quoted_names_but_not_values_or_comments() {
        assert_eq!(
            sql_identifiers(
                "DROP TABLE \"_cairn_rulings\"; SELECT '_cairn_value' /* _cairn_comment */"
            ),
            ["DROP", "TABLE", "_cairn_rulings", "SELECT"]
        );
    }

    #[test]
    fn retired_rulings_are_refused_in_every_identifier_form_but_not_as_data() {
        for sql in [
            "SELECT * FROM _cairn_rulings",
            "SELECT * FROM main.\"_CAIRN_RULINGS\"",
            "DELETE FROM [_cairn_rulings]",
        ] {
            assert!(refuse_retired_rulings(sql)
                .unwrap_err()
                .starts_with(RULINGS_ARE_TENETS));
        }
        assert!(refuse_retired_rulings("SELECT '_cairn_rulings' -- _cairn_rulings").is_ok());
        assert!(refuse_retired_rulings("SELECT * FROM _cairn_open_questions").is_ok());
    }

    #[test]
    fn statement_target_attributes_every_write_form() {
        let cases = [
            (
                "INSERT INTO attempts(problem) VALUES (?)",
                Some(("attempts", "insert")),
            ),
            (
                "insert or replace into \"we ird\" values (1)",
                Some(("we ird", "insert")),
            ),
            (
                "REPLACE INTO attempts VALUES (1)",
                Some(("attempts", "insert")),
            ),
            ("UPDATE attempts SET x = 1", Some(("attempts", "update"))),
            (
                "UPDATE OR IGNORE [attempts] SET x = 1",
                Some(("attempts", "update")),
            ),
            (
                "DELETE FROM `attempts` WHERE id = 1",
                Some(("attempts", "delete")),
            ),
            (
                "CREATE TABLE attempts(id INTEGER)",
                Some(("attempts", "update")),
            ),
            (
                "CREATE TEMP TABLE IF NOT EXISTS scratch(id)",
                Some(("scratch", "update")),
            ),
            (
                "DROP TABLE IF EXISTS attempts",
                Some(("attempts", "update")),
            ),
            (
                "ALTER TABLE attempts ADD COLUMN lang TEXT",
                Some(("attempts", "update")),
            ),
            (
                "WITH x AS (SELECT 1) INSERT INTO attempts SELECT * FROM x",
                None,
            ),
            ("CREATE INDEX idx ON attempts(problem)", None),
            ("INSERT INTO main.attempts VALUES (1)", None),
            ("SELECT * FROM attempts", None),
        ];
        for (sql, expected) in cases {
            let got = statement_target(sql);
            assert_eq!(
                got.as_ref()
                    .map(|(table, action)| (table.as_str(), *action)),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn referenced_tables_are_the_named_tables_and_what_named_views_read() {
        let table = |name: &str| CatalogEntry {
            name: name.to_string(),
            view_sql: None,
        };
        let catalog = [
            table("issues"),
            table("comments"),
            table("jobs"),
            table("Labels"),
            CatalogEntry {
                name: "issue_list".to_string(),
                view_sql: Some(
                    "CREATE VIEW issue_list AS SELECT i.*, l.name FROM issues i JOIN labels l"
                        .to_string(),
                ),
            },
            CatalogEntry {
                name: "looped".to_string(),
                view_sql: Some("CREATE VIEW looped AS SELECT * FROM looped, jobs".to_string()),
            },
        ];
        assert_eq!(
            referenced_tables(
                "SELECT c.body FROM \"comments\" c JOIN [ISSUES] i ON i.id = c.issue_id",
                &catalog
            ),
            ["comments", "issues"]
        );
        assert_eq!(
            referenced_tables("SELECT * FROM issue_list", &catalog),
            ["Labels", "issues"]
        );
        assert_eq!(
            referenced_tables("SELECT * FROM looped", &catalog),
            ["jobs"]
        );
        assert_eq!(
            referenced_tables("SELECT 'jobs' -- comments\n, 1", &catalog),
            Vec::<String>::new()
        );
    }

    #[test]
    fn splitting_ignores_semicolons_inside_sql_syntax() {
        assert_eq!(
            split_statements("INSERT INTO x VALUES ('a;b'); -- ;\nSELECT [semi;colon] FROM x;"),
            [
                "INSERT INTO x VALUES ('a;b')",
                "-- ;\nSELECT [semi;colon] FROM x"
            ]
        );
    }

    #[test]
    fn read_policy_uses_the_shared_splitter() {
        assert!(validate_read_only_sql("SELECT ';' /* ; */").is_ok());
        assert_eq!(
            validate_read_only_sql("SELECT 1; SELECT 2").unwrap_err(),
            "SQL query must contain exactly one statement"
        );
    }
}
