//! SQL statement scanning and policy shared by database resources.

/// Where a thread's rulings went. Thread migration 0002 drops
/// `_cairn_rulings` once its rows are imported as tenets, so SQL that still
/// names the table is refused with this rather than with a missing-table error
/// that does not say where the decisions live now.
pub(crate) const RULINGS_ARE_TENETS: &str =
    "rulings are project tenets now: propose with an append to cairn:@/tenets";

/// The append that replaces a ruling insert.
pub(crate) const TENET_APPEND_EXAMPLE: &str = "write({changes:[{target:\"cairn:@/tenets\",mode:\"append\",payload:{question:\"...\",options:[\"recommended\",\"alternative\"]}}]})";

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
