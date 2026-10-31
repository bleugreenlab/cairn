//! The resource layer's read contract, held against its source.
//!
//! Every resource read is a bounded pooled read (`read_resource`, the
//! export's `short_read`, or `LocalDb::read`) whose closure runs database
//! statements and nothing else. A transaction that stays open while its owner
//! waits on jj, the network, a lock, or another database call blocks every
//! checkpoint for as long as that wait lasts, and an out-of-pool one cannot be
//! cut at all: on 10-29 a `cairn:~/rebase` replay held one for thirteen
//! minutes, twice (cairn/5502). The pooled read's parked-read timeout is the
//! runtime backstop; this is the review-time one, so the shape cannot drift
//! back in one call site at a time.
//!
//! A scan, not a parser: comments and literals are blanked, each read call's
//! closure is brace-matched, and the closure is searched for calls that leave
//! the database. A false positive goes on [`ALLOWED`] with its reason.
//!
//! It does not look inside the helpers a closure calls. A `&Connection` helper
//! that opens a thread store or shells out passes this scan; keeping helpers
//! called from a read to statements is a review obligation, and a helper that
//! does more takes its id out of the read and runs after it (as the thread
//! seed's store census does).

use std::path::{Path, PathBuf};

/// Calls a read closure may not make: each one waits on something other than
/// the statement in front of it.
const FORBIDDEN_IN_READ: &[&str] = &[
    "run_db_blocking",
    "spawn_blocking",
    "block_in_place",
    "sleep",
    "Command::",
    "std::fs::",
    "tokio::fs::",
    ".lock()",
    "describe_job_launch",
    "render_live_pr_section",
    "source_conflict_report",
    "apply_pending_search",
    "search_content",
    "display_for",
    "store.get",
    ".get(hash)",
    "reconstruct_events",
    "db_store::",
    "arc_store::",
];

/// `(file suffix, function, reason)` for a read call this scan misjudges.
/// Starts empty; an entry needs a reason a reviewer would accept.
const ALLOWED: &[(&str, &str, &str)] = &[];

fn core_src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// The files whose reads this contract covers.
fn scanned_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    rust_files(&core_src().join("resources"), &mut files);
    files.push(core_src().join("projects/export.rs"));
    files.push(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../cairn-db/src/storage/events/reconstruct.rs"),
    );
    files.retain(|path| !path.ends_with("read_contract_tests.rs"));
    files
}

/// `source` with every comment and string or char literal replaced by spaces
/// (newlines kept), so a token inside one is never mistaken for code and
/// offsets still line up.
fn blank_comments_and_literals(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = bytes.to_vec();
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for byte in &mut out[from..to] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    };
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                let end = source[i..].find('\n').map_or(bytes.len(), |n| i + n);
                blank(&mut out, i, end);
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let (mut depth, mut j) = (1, i + 2);
                while j < bytes.len() && depth > 0 {
                    if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                        depth += 1;
                        j += 2;
                    } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                        depth -= 1;
                        j += 2;
                    } else {
                        j += 1;
                    }
                }
                blank(&mut out, i, j);
                i = j;
            }
            b'r' if (bytes.get(i + 1) == Some(&b'"') || bytes.get(i + 1) == Some(&b'#'))
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')) =>
            {
                let hashes = bytes[i + 1..].iter().take_while(|b| **b == b'#').count();
                if bytes.get(i + 1 + hashes) != Some(&b'"') {
                    i += 1;
                    continue;
                }
                let close = format!("\"{}", "#".repeat(hashes));
                let body = i + 2 + hashes;
                let end = source[body..]
                    .find(&close)
                    .map_or(bytes.len(), |n| body + n + close.len());
                blank(&mut out, i, end);
                i = end;
            }
            b'"' => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j] != b'"' {
                    j += if bytes[j] == b'\\' { 2 } else { 1 };
                }
                blank(&mut out, i, (j + 1).min(bytes.len()));
                i = j + 1;
            }
            b'\'' => {
                // A char literal closes within a few bytes; a lifetime does not.
                let close = if bytes.get(i + 1) == Some(&b'\\') {
                    source[i + 2..].find('\'').map(|n| i + 2 + n)
                } else {
                    source[i + 1..]
                        .char_indices()
                        .nth(1)
                        .filter(|(_, ch)| *ch == '\'')
                        .map(|(n, _)| i + 1 + n)
                };
                match close {
                    Some(end) if end - i <= 10 => {
                        blank(&mut out, i, end + 1);
                        i = end + 1;
                    }
                    _ => i += 1,
                }
            }
            _ => i += 1,
        }
    }
    String::from_utf8(out).expect("blanking keeps the text UTF-8")
}

/// The byte just past the `)` matching the `(` at `open`.
fn matching_paren(code: &str, open: usize) -> usize {
    let mut depth = 0usize;
    for (offset, byte) in code.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return open + offset + 1;
                }
            }
            _ => {}
        }
    }
    code.len()
}

/// The name of the `fn` enclosing `at`, for messages and the allowlist.
fn enclosing_fn(code: &str, at: usize) -> &str {
    code[..at]
        .rfind("fn ")
        .map(|start| {
            let name = &code[start + 3..];
            let end = name
                .find(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
                .unwrap_or(name.len());
            &name[..end]
        })
        .unwrap_or("?")
}

fn line_of(code: &str, at: usize) -> usize {
    code[..at].matches('\n').count() + 1
}

/// Whether `span` passes `db` or `orch` (a nested checkout or orchestrator
/// work) as a call argument, or calls a method on either.
fn nested_database_call(span: &str) -> Option<String> {
    let bytes = span.as_bytes();
    for name in ["db", "orch"] {
        let mut from = 0;
        while let Some(found) = span[from..].find(name) {
            let at = from + found;
            from = at + name.len();
            let before = span[..at].trim_end_matches(['&', '*', ' ', '\n']);
            let after = span[at + name.len()..].trim_start();
            let ident_before = at > 0
                && (bytes[at - 1].is_ascii_alphanumeric()
                    || bytes[at - 1] == b'_'
                    || bytes[at - 1] == b'.');
            let ident_after = span
                .as_bytes()
                .get(at + name.len())
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_');
            if ident_before || ident_after {
                continue;
            }
            let as_argument = (before.ends_with('(') || before.ends_with(','))
                && (after.starts_with(',') || after.starts_with(')'));
            let as_receiver = after.starts_with('.');
            if as_argument || as_receiver {
                return Some(name.to_string());
            }
        }
    }
    None
}

#[test]
fn connect_for_read_is_gone() {
    let mut files = Vec::new();
    rust_files(&core_src(), &mut files);
    let offenders: Vec<String> = files
        .iter()
        .filter(|path| !path.ends_with("read_contract_tests.rs"))
        .filter(|path| {
            blank_comments_and_literals(&std::fs::read_to_string(path).unwrap())
                .contains("connect_for_read(")
        })
        .map(|path| path.display().to_string())
        .collect();
    assert!(
        offenders.is_empty(),
        "connect_for_read is back: {offenders:?}"
    );
}

#[test]
fn resource_reads_take_no_connection_of_their_own() {
    let mut offenders = Vec::new();
    for path in scanned_files() {
        if path.ends_with("reconstruct.rs") {
            continue;
        }
        let code = blank_comments_and_literals(&std::fs::read_to_string(&path).unwrap());
        for (at, _) in code.match_indices(".connect()") {
            offenders.push(format!(
                "{}:{} in {}",
                path.display(),
                line_of(&code, at),
                enclosing_fn(&code, at)
            ));
        }
    }
    assert!(
        offenders.is_empty(),
        "an out-of-pool connection cannot be cut when it parks; read through read_resource: {offenders:#?}"
    );
}

#[test]
fn read_closures_run_statements_only() {
    let mut offenders = Vec::new();
    let mut reads_seen = 0;
    for path in scanned_files() {
        let code = blank_comments_and_literals(&std::fs::read_to_string(&path).unwrap());
        let display = path.display().to_string();
        for call in [".read(", "read_resource(", "short_read("] {
            for (at, _) in code.match_indices(call) {
                if code[..at].trim_end().ends_with("fn") {
                    continue;
                }
                let open = at + call.len() - 1;
                let call_span = &code[open..matching_paren(&code, open)];
                // Only a call that takes a closure is a transaction body, and
                // only the closure is: the call's own `db` argument is not.
                let Some(closure) = call_span.find('|') else {
                    continue;
                };
                let span = &call_span[closure..];
                reads_seen += 1;
                let function = enclosing_fn(&code, at);
                if ALLOWED
                    .iter()
                    .any(|(file, name, _)| display.ends_with(file) && *name == function)
                {
                    continue;
                }
                let mut found: Vec<String> = FORBIDDEN_IN_READ
                    .iter()
                    .filter(|token| span.contains(*token))
                    .map(|token| token.to_string())
                    .collect();
                if let Some(name) = nested_database_call(span) {
                    found.push(format!("a call on or with `{name}`"));
                }
                if !found.is_empty() {
                    offenders.push(format!(
                        "{display}:{} in {function}: {}",
                        line_of(&code, at),
                        found.join(", ")
                    ));
                }
            }
        }
    }
    assert!(
        reads_seen > 40,
        "the scan found only {reads_seen} read closures; is it still looking?"
    );
    assert!(
        offenders.is_empty(),
        "a read closure may run database statements only; do this work after the read \
         returns (cairn/5502): {offenders:#?}"
    );
}

#[test]
fn the_scanner_sees_through_literals_and_finds_nested_calls() {
    let code = blank_comments_and_literals(
        "let a = \"db.read(|c| sleep())\"; // sleep\nlet b = r#\"x\"#; let c = 'x'; fn f<'a>() {}",
    );
    assert!(!code.contains("sleep"), "{code}");
    assert!(code.contains("fn f<'a>"), "{code}");
    assert_eq!(nested_database_call("(|c| load(db, 1))"), Some("db".into()));
    assert_eq!(nested_database_call("(|c| x(&orch))"), Some("orch".into()));
    assert_eq!(
        nested_database_call("(|c| db.query_opt(1))"),
        Some("db".into())
    );
    assert_eq!(
        nested_database_call("(|c| load(conn, routed_db_name))"),
        None
    );
    assert_eq!(nested_database_call("(|c| self.db.thing)"), None);
}
