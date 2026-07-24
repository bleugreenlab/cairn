use cairn_common::protocol::WarmSearchDeclineReason;
use regex::escape;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TranslatedSearch {
    Grep {
        pattern: String,
        globs: Vec<String>,
        output_mode: String,
        case_insensitive: Option<bool>,
        before_context: usize,
        after_context: usize,
        show_line_numbers: bool,
        max_per_file: Option<usize>,
        /// Zero or more search-path arguments within the run worktree. Empty =
        /// whole-worktree search (`path = None` in the old single-path shape).
        paths: Vec<String>,
    },
    Files {
        globs: Vec<String>,
        path: Option<String>,
    },
}

fn type_glob(file_type: &str) -> Option<&'static str> {
    match file_type {
        "rust" => Some("*.rs"),
        "ts" => Some("*.{ts,tsx}"),
        "js" => Some("*.{js,jsx,mjs,cjs}"),
        "py" => Some("*.py"),
        "md" => Some("*.{md,mdx}"),
        "json" => Some("*.json"),
        "yaml" => Some("*.{yaml,yml}"),
        _ => None,
    }
}

/// A translated head-stage search plus its ordered tail of post-filter stages.
/// `post` is empty for a bare `rg`/`grep`; a non-empty `post` is a whitelisted
/// pipeline (`| head`, `| tail`, `| grep -v`, ...) applied to the head stage's
/// output in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TranslatedSearchPipeline {
    pub search: TranslatedSearch,
    pub post: Vec<PostFilter>,
}

/// A pure line-transform tail stage of a translated search pipeline. Every
/// variant is a whitelisted transform the run() layer can reproduce faithfully
/// on the head stage's output stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostFilter {
    /// `head -N` / `head -n N` / bare `head` (coreutils default 10).
    Head(usize),
    /// `tail -N` / `tail -n N` / bare `tail` (coreutils default 10).
    Tail(usize),
    /// `sed -n 'START,ENDp'` / `sed -n 'Np'` line selection.
    Lines { start: usize, end: usize },
    /// `wc -l` line count.
    CountLines,
    /// Locale-independent byte-order `sort` (the agent environment sets `LC_ALL=C`).
    Sort,
    /// Adjacent duplicate removal (`uniq`).
    Uniq,
    /// `grep [-v] [-i] [-F] PATTERN` as a line filter (no `-r`/paths). `pattern`
    /// is already BRE-converted (or `-F`-escaped) to the index dialect.
    Grep {
        pattern: String,
        invert: bool,
        case_insensitive: bool,
    },
}

#[derive(Debug, Clone)]
struct Token {
    text: String,
    has_unquoted_expansion: bool,
}

/// Translate one already-expanded executable invocation. Bash remains the sole
/// parser for shell syntax; this function sees only the program and argv that
/// would have reached the real executable.
pub(crate) fn translate_search_invocation(
    program: &str,
    argv: &[String],
) -> Result<TranslatedSearch, WarmSearchDeclineReason> {
    let program = std::path::Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(program);
    if argv.iter().any(|arg| arg == "-") {
        return Err(WarmSearchDeclineReason::StdinInput);
    }
    let tokens: Vec<Token> = argv
        .iter()
        .cloned()
        .map(|text| Token {
            text,
            has_unquoted_expansion: false,
        })
        .collect();
    match program {
        "rg" => translate_rg(&tokens),
        "grep" => translate_grep(&tokens),
        _ => return Err(WarmSearchDeclineReason::UnsupportedProgram),
    }
    .ok_or_else(|| classify_invocation_decline(argv))
}

/// Whether a translated pattern compiles. An uncompilable pattern is not a
/// translation gap — the invocation's *shape* was understood perfectly — so it
/// is not this module's business to reject. It is a per-caller serving
/// decision: the PATH shim declines so the real binary prints its own
/// diagnostic and exits 2, while the `run` path lets its walk fallback produce
/// Cairn's canonical `Invalid regex pattern '…'` message.
pub(crate) fn pattern_compiles(translated: &TranslatedSearch) -> bool {
    match translated {
        TranslatedSearch::Grep { pattern, .. } => regex::Regex::new(pattern).is_ok(),
        TranslatedSearch::Files { .. } => true,
    }
}

fn classify_invocation_decline(argv: &[String]) -> WarmSearchDeclineReason {
    argv.iter()
        .find(|arg| arg.starts_with('-') && *arg != "--")
        .cloned()
        .map(WarmSearchDeclineReason::UnsupportedFlag)
        .unwrap_or(WarmSearchDeclineReason::UnsupportedInvocation)
}

/// Historical whole-command adapter retained for reconstructed-transcript
/// backtests. Production routing uses [`translate_search_invocation`].
#[allow(dead_code)]
pub(crate) fn translate_search_command(command: &str) -> Option<TranslatedSearchPipeline> {
    translate_search_command_detailed(command).ok()
}

/// Translate a command whose executable identity has already classified it as a
/// search. Unlike the historical adapter, this preserves the coverage gap so the
/// run dispatcher can fail the read explicitly instead of placing it as a build.
pub(crate) fn translate_search_command_detailed(
    command: &str,
) -> Result<TranslatedSearchPipeline, WarmSearchDeclineReason> {
    let stages = split_pipeline(command).ok_or(WarmSearchDeclineReason::UnsupportedInvocation)?;
    let (head, tail) = stages
        .split_first()
        .ok_or(WarmSearchDeclineReason::UnsupportedInvocation)?;
    let tokens = tokenize(head).ok_or(WarmSearchDeclineReason::UnsupportedInvocation)?;
    let first = tokens
        .first()
        .ok_or(WarmSearchDeclineReason::UnsupportedInvocation)?;
    let argv: Vec<String> = tokens[1..].iter().map(|token| token.text.clone()).collect();
    if tokens[1..].iter().any(|token| token.has_unquoted_expansion) {
        return Err(WarmSearchDeclineReason::UnsupportedInvocation);
    }
    let search = translate_search_invocation(&first.text, &argv)?;
    let mut post = Vec::with_capacity(tail.len());
    for stage in tail {
        let tokens = tokenize(stage).ok_or(WarmSearchDeclineReason::UnsupportedInvocation)?;
        post.push(parse_post_filter(&tokens).ok_or_else(|| {
            let detail = tokens
                .iter()
                .find(|token| token.text.starts_with('-'))
                .or_else(|| tokens.first())
                .map(|token| token.text.clone())
                .unwrap_or_else(|| "pipeline stage".to_string());
            WarmSearchDeclineReason::UnsupportedFlag(detail)
        })?);
    }
    Ok(TranslatedSearchPipeline { search, post })
}

/// Split a command on top-level unquoted pipes, preserving each stage's original
/// quoting so `tokenize` can re-parse it. Rejects `||` (logical-or we cannot
/// emulate) and empty stages (a leading, trailing, or doubled pipe). Other shell
/// operators (`;`, `&`, `>`, `<`, `` ` ``, `$`) stay inside their stage and still
/// hard-reject the whole command when `tokenize` sees them.
fn split_pipeline(command: &str) -> Option<Vec<String>> {
    let mut stages = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_single {
            current.push(ch);
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }
        if in_double {
            if ch == '\\' {
                current.push(ch);
                if let Some(next) = chars.next() {
                    current.push(next);
                }
                continue;
            }
            current.push(ch);
            if ch == '"' {
                in_double = false;
            }
            continue;
        }
        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '\\' => {
                current.push(ch);
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '|' => {
                if chars.peek() == Some(&'|') {
                    return None;
                }
                let stage = current.trim().to_string();
                if stage.is_empty() {
                    return None;
                }
                stages.push(stage);
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_single || in_double {
        return None;
    }
    let last = current.trim().to_string();
    if last.is_empty() {
        return None;
    }
    stages.push(last);
    Some(stages)
}

/// Parse one tail stage into a whitelisted [`PostFilter`]. Anything outside the
/// whitelist (`cat`, `sort`, a recursive `grep`, an unknown flag) returns `None`,
/// which fails the whole pipeline translation so the command falls through to a
/// real subprocess unchanged.
fn parse_post_filter(tokens: &[Token]) -> Option<PostFilter> {
    let first = tokens.first()?;
    match first.text.as_str() {
        "head" => parse_head_tail(&tokens[1..], true),
        "tail" => parse_head_tail(&tokens[1..], false),
        "grep" => parse_post_grep(&tokens[1..]),
        "sed" => parse_sed_lines(&tokens[1..]),
        "wc" => (tokens.len() == 2 && tokens[1].text == "-l").then_some(PostFilter::CountLines),
        "sort" => (tokens.len() == 1).then_some(PostFilter::Sort),
        "uniq" => (tokens.len() == 1).then_some(PostFilter::Uniq),
        _ => None,
    }
}

fn parse_sed_lines(tokens: &[Token]) -> Option<PostFilter> {
    if tokens.len() != 2 || tokens[0].text != "-n" {
        return None;
    }
    let expression = tokens[1].text.strip_suffix('p')?;
    let (start, end) = match expression.split_once(',') {
        Some((start, end)) => (parse_usize(start)?, parse_usize(end)?),
        None => {
            let line = parse_usize(expression)?;
            (line, line)
        }
    };
    (start > 0 && end >= start).then_some(PostFilter::Lines { start, end })
}

/// `head`/`tail` accept `-N`, `-n N`, `-nN`, `--lines=N`, `--lines N`; a bare
/// invocation defaults to 10 (coreutils). A positional (reading from a file, not
/// the piped stream) or any other flag returns `None`.
fn parse_head_tail(tokens: &[Token], is_head: bool) -> Option<PostFilter> {
    let mut count: Option<usize> = None;
    let mut index = 0;
    while index < tokens.len() {
        reject_expansion(&tokens[index])?;
        let text = &tokens[index].text;
        if let Some(value) = text.strip_prefix("--lines=") {
            count = Some(parse_usize(value)?);
            index += 1;
            continue;
        }
        if text == "--lines" || text == "-n" {
            let value = take_value(tokens, &mut index)?;
            count = Some(parse_usize(&value)?);
            continue;
        }
        if let Some(value) = text.strip_prefix("-n") {
            count = Some(parse_usize(value)?);
            index += 1;
            continue;
        }
        if let Some(value) = text.strip_prefix('-') {
            if !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()) {
                count = Some(parse_usize(value)?);
                index += 1;
                continue;
            }
        }
        return None;
    }
    let count = count.unwrap_or(10);
    Some(if is_head {
        PostFilter::Head(count)
    } else {
        PostFilter::Tail(count)
    })
}

/// A tail `grep` used as a pure line filter: `-v` (invert), `-i` (case), `-F`
/// (fixed) and exactly one pattern. A recursive `-r`, an output-mode flag, a
/// glob, a path arg, or any other flag returns `None` (a second recursive grep
/// is not a line filter). The pattern is BRE-converted like the head grep unless
/// `-F`.
fn parse_post_grep(tokens: &[Token]) -> Option<PostFilter> {
    let mut invert = false;
    let mut case_insensitive = false;
    let mut fixed = false;
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        reject_expansion(&tokens[index])?;
        let text = &tokens[index].text;
        if text == "--" {
            index += 1;
            while index < tokens.len() {
                reject_expansion(&tokens[index])?;
                positionals.push(tokens[index].text.clone());
                index += 1;
            }
            break;
        }
        if text == "-v" {
            invert = true;
            index += 1;
            continue;
        }
        if text == "-i" || text == "--ignore-case" {
            case_insensitive = true;
            index += 1;
            continue;
        }
        if text == "-F" {
            fixed = true;
            index += 1;
            continue;
        }
        if text.starts_with('-') && text.len() > 2 && !text.starts_with("--") {
            for ch in text[1..].chars() {
                match ch {
                    'v' => invert = true,
                    'i' => case_insensitive = true,
                    'F' => fixed = true,
                    _ => return None,
                }
            }
            index += 1;
            continue;
        }
        if text.starts_with('-') {
            return None;
        }
        positionals.push(text.clone());
        index += 1;
    }
    if positionals.len() != 1 {
        return None;
    }
    let raw = positionals.pop()?;
    let pattern = if fixed {
        escape(&raw)
    } else {
        bre_to_ere(&raw)?
    };
    Some(PostFilter::Grep {
        pattern,
        invert,
        case_insensitive,
    })
}

fn tokenize(command: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut text = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut token_started = false;
    let mut has_unquoted_expansion = false;
    let mut unquoted_index = 0usize;
    let mut chars = command.trim().chars().peekable();

    while let Some(ch) = chars.next() {
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                token_started = true;
                text.push(ch);
            }
            continue;
        }

        if in_double {
            match ch {
                '"' => in_double = false,
                '$' | '`' => return None,
                '\\' => {
                    token_started = true;
                    if let Some(next) = chars.next() {
                        text.push(next);
                    } else {
                        text.push('\\');
                    }
                }
                _ => {
                    token_started = true;
                    text.push(ch);
                }
            }
            continue;
        }

        match ch {
            c if c.is_whitespace() => {
                if token_started {
                    tokens.push(Token {
                        text: std::mem::take(&mut text),
                        has_unquoted_expansion,
                    });
                    token_started = false;
                    has_unquoted_expansion = false;
                    unquoted_index = 0;
                }
            }
            '\'' => {
                token_started = true;
                in_single = true;
            }
            '"' => {
                token_started = true;
                in_double = true;
            }
            '\\' => {
                token_started = true;
                if let Some(next) = chars.next() {
                    text.push(next);
                } else {
                    text.push('\\');
                }
            }
            '|' | ';' | '&' | '>' | '<' | '`' | '$' => return None,
            '*' | '?' | '[' => {
                token_started = true;
                has_unquoted_expansion = true;
                text.push(ch);
                unquoted_index += 1;
            }
            '~' => {
                token_started = true;
                if unquoted_index > 0 {
                    has_unquoted_expansion = true;
                }
                text.push(ch);
                unquoted_index += 1;
            }
            _ => {
                token_started = true;
                text.push(ch);
                unquoted_index += 1;
            }
        }
    }

    if in_single || in_double {
        return None;
    }
    if token_started {
        tokens.push(Token {
            text,
            has_unquoted_expansion,
        });
    }
    Some(tokens)
}

#[derive(Debug, Clone)]
struct ParsedGrepLike {
    pattern: Option<String>,
    globs: Vec<String>,
    output_mode: String,
    case_insensitive: Option<bool>,
    before_context: usize,
    after_context: usize,
    show_line_numbers: bool,
    max_per_file: Option<usize>,
    fixed_strings: bool,
    word_regexp: bool,
}

impl Default for ParsedGrepLike {
    fn default() -> Self {
        Self {
            pattern: None,
            globs: Vec::new(),
            output_mode: "content".to_string(),
            case_insensitive: None,
            before_context: 0,
            after_context: 0,
            show_line_numbers: false,
            max_per_file: None,
            fixed_strings: false,
            word_regexp: false,
        }
    }
}

fn translate_rg(tokens: &[Token]) -> Option<TranslatedSearch> {
    let mut parsed = ParsedGrepLike {
        case_insensitive: Some(false),
        ..ParsedGrepLike::default()
    };
    let mut saw_files = false;
    let mut saw_e = false;
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < tokens.len() {
        reject_expansion(&tokens[index])?;
        let text = &tokens[index].text;
        if text == "--" {
            index += 1;
            while index < tokens.len() {
                reject_expansion(&tokens[index])?;
                positionals.push(tokens[index].text.clone());
                index += 1;
            }
            break;
        }
        if text == "--files" {
            saw_files = true;
            index += 1;
            continue;
        }
        if text == "--smart-case" || text == "-S" {
            return None;
        }
        if text == "-t" || text == "--type" {
            let value = take_value(tokens, &mut index)?;
            parsed.globs.push(type_glob(&value)?.to_string());
            continue;
        }
        if let Some(value) = text.strip_prefix("--glob=") {
            parsed.globs.push(value.to_string());
            index += 1;
            continue;
        }
        if text == "--glob" || text == "-g" {
            let value = take_value(tokens, &mut index)?;
            parsed.globs.push(value);
            continue;
        }
        if let Some(value) = text.strip_prefix("--max-count=") {
            parsed.max_per_file = Some(parse_usize(value)?);
            index += 1;
            continue;
        }
        if text == "--max-count" || text == "-m" {
            let value = take_value(tokens, &mut index)?;
            parsed.max_per_file = Some(parse_usize(&value)?);
            continue;
        }
        if let Some(value) = text.strip_prefix("-m") {
            if !value.is_empty() {
                parsed.max_per_file = Some(parse_usize(value)?);
                index += 1;
                continue;
            }
        }
        if text == "--line-number" || text == "-n" {
            parsed.show_line_numbers = true;
            index += 1;
            continue;
        }
        if text == "--no-line-number" || text == "-N" {
            parsed.show_line_numbers = false;
            index += 1;
            continue;
        }
        if text == "-i" || text == "--ignore-case" {
            parsed.case_insensitive = Some(true);
            index += 1;
            continue;
        }
        if text == "-w" || text == "--word-regexp" {
            parsed.word_regexp = true;
            index += 1;
            continue;
        }
        if text == "-l" {
            parsed.output_mode = "files_with_matches".to_string();
            index += 1;
            continue;
        }
        if text == "-c" {
            parsed.output_mode = "count".to_string();
            index += 1;
            continue;
        }
        if text == "-F" {
            parsed.fixed_strings = true;
            index += 1;
            continue;
        }
        if text == "-e" {
            if saw_e || parsed.pattern.is_some() {
                return None;
            }
            let pattern = take_value(tokens, &mut index)?;
            parsed.pattern = Some(pattern);
            saw_e = true;
            continue;
        }
        if let Some(value) = text.strip_prefix("-e") {
            if value.is_empty() || saw_e || parsed.pattern.is_some() {
                return None;
            }
            parsed.pattern = Some(value.to_string());
            saw_e = true;
            index += 1;
            continue;
        }
        if text == "-A" || text == "-B" || text == "-C" {
            let flag = text.as_str();
            let value = parse_usize(&take_value(tokens, &mut index)?)?;
            apply_context(&mut parsed, flag, value);
            continue;
        }
        if let Some((flag, value)) = short_context_value(text) {
            apply_context(&mut parsed, flag, parse_usize(value)?);
            index += 1;
            continue;
        }
        if text.starts_with('-') {
            return None;
        }
        positionals.push(text.clone());
        index += 1;
    }

    if saw_files {
        if parsed.pattern.is_some()
            || parsed.output_mode != "content"
            || parsed.case_insensitive != Some(false)
            || parsed.before_context != 0
            || parsed.after_context != 0
            || parsed.show_line_numbers
            || parsed.max_per_file.is_some()
            || parsed.fixed_strings
            || positionals.len() > 1
        {
            return None;
        }
        return Some(TranslatedSearch::Files {
            globs: parsed.globs,
            path: positionals.pop(),
        });
    }

    finish_grep(parsed, positionals)
}

fn translate_grep(tokens: &[Token]) -> Option<TranslatedSearch> {
    let mut parsed = ParsedGrepLike {
        case_insensitive: Some(false),
        ..ParsedGrepLike::default()
    };
    let mut extended = false;
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < tokens.len() {
        reject_expansion(&tokens[index])?;
        let text = &tokens[index].text;
        if text == "--" {
            index += 1;
            while index < tokens.len() {
                reject_expansion(&tokens[index])?;
                positionals.push(tokens[index].text.clone());
                index += 1;
            }
            break;
        }
        if let Some(value) = text.strip_prefix("--include=") {
            parsed.globs.push(value.to_string());
            index += 1;
            continue;
        }
        if text == "-r" || text == "-R" {
            index += 1;
            continue;
        }
        if text == "-i" {
            parsed.case_insensitive = Some(true);
            index += 1;
            continue;
        }
        if text == "-n" {
            parsed.show_line_numbers = true;
            index += 1;
            continue;
        }
        if text == "-l" {
            parsed.output_mode = "files_with_matches".to_string();
            index += 1;
            continue;
        }
        if text == "-c" {
            parsed.output_mode = "count".to_string();
            index += 1;
            continue;
        }
        if text == "-E" {
            extended = true;
            index += 1;
            continue;
        }
        if text == "-F" {
            parsed.fixed_strings = true;
            index += 1;
            continue;
        }
        if text.starts_with('-') && text.len() > 2 && !text.starts_with("--") {
            for ch in text[1..].chars() {
                match ch {
                    'r' | 'R' => {}
                    'i' => parsed.case_insensitive = Some(true),
                    'n' => parsed.show_line_numbers = true,
                    'l' => parsed.output_mode = "files_with_matches".to_string(),
                    'c' => parsed.output_mode = "count".to_string(),
                    'E' => extended = true,
                    'F' => parsed.fixed_strings = true,
                    _ => return None,
                }
            }
            index += 1;
            continue;
        }
        if text.starts_with('-') {
            return None;
        }
        positionals.push(text.clone());
        index += 1;
    }

    if positionals.is_empty() {
        return None;
    }
    // First positional is the pattern; every remaining positional is a search
    // path (multi-path is supported). Basic-grep (BRE) patterns are converted to
    // the index's ERE dialect unless `-E` (already ERE) or `-F` (literal).
    let raw_pattern = positionals.remove(0);
    let pattern = if extended || parsed.fixed_strings {
        raw_pattern
    } else {
        bre_to_ere(&raw_pattern)?
    };
    parsed.pattern = Some(pattern);

    finish_grep(parsed, positionals)
}

fn finish_grep(
    mut parsed: ParsedGrepLike,
    mut positionals: Vec<String>,
) -> Option<TranslatedSearch> {
    if parsed.pattern.is_none() {
        if positionals.is_empty() {
            return None;
        }
        parsed.pattern = Some(positionals.remove(0));
    }
    // Whatever positionals remain after the pattern are search paths (empty =
    // whole worktree). Multi-path is supported.
    let mut pattern = parsed.pattern?;
    if parsed.fixed_strings {
        pattern = escape(&pattern);
    }
    if parsed.word_regexp {
        pattern = format!(r"\b(?:{pattern})\b");
    }
    Some(TranslatedSearch::Grep {
        pattern,
        globs: parsed.globs,
        output_mode: parsed.output_mode,
        case_insensitive: parsed.case_insensitive,
        before_context: parsed.before_context,
        after_context: parsed.after_context,
        show_line_numbers: parsed.show_line_numbers,
        max_per_file: parsed.max_per_file,
        paths: positionals,
    })
}

fn reject_expansion(token: &Token) -> Option<()> {
    (!token.has_unquoted_expansion).then_some(())
}

fn take_value(tokens: &[Token], index: &mut usize) -> Option<String> {
    *index += 1;
    let token = tokens.get(*index)?;
    reject_expansion(token)?;
    *index += 1;
    Some(token.text.clone())
}

fn parse_usize(value: &str) -> Option<usize> {
    value.parse::<usize>().ok()
}

fn short_context_value(text: &str) -> Option<(&'static str, &str)> {
    for flag in ["-A", "-B", "-C"] {
        if let Some(value) = text.strip_prefix(flag) {
            if !value.is_empty() {
                return Some((flag, value));
            }
        }
    }
    None
}

fn apply_context(parsed: &mut ParsedGrepLike, flag: &str, value: usize) {
    match flag {
        "-A" => parsed.after_context = value,
        "-B" => parsed.before_context = value,
        "-C" => {
            parsed.before_context = value;
            parsed.after_context = value;
        }
        _ => unreachable!(),
    }
}

/// Convert a POSIX basic-regex (BRE) pattern to the ERE/RE2 dialect the warm
/// index uses. BRE gives `\|`/`\(`/`\)`/`\{`/`\}`/`\+`/`\?` their special meaning
/// and treats the bare forms as literals — the exact inverse of ERE — so toggle
/// each. `.`/`*`/`^`/`$` and bracket expressions mean the same in both and pass
/// through untouched. Returns `None` for constructs RE2 cannot faithfully model
/// (backreferences, and the GNU word-boundary / character-class letter escapes
/// whose semantics diverge) so the caller falls through to a real grep rather
/// than silently changing the match set.
fn bre_to_ere(pattern: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '[' => {
                // A bracket expression means the same in BRE and ERE; copy it
                // verbatim, honoring the POSIX rule that a `]` immediately after
                // `[` or `[^` is a literal member rather than the close.
                out.push('[');
                if chars.peek() == Some(&'^') {
                    out.push(chars.next().unwrap());
                }
                if chars.peek() == Some(&']') {
                    out.push(chars.next().unwrap());
                }
                for c in chars.by_ref() {
                    out.push(c);
                    if c == ']' {
                        break;
                    }
                }
            }
            '\\' => {
                let next = chars.next()?;
                match next {
                    '|' => out.push('|'),
                    '(' => out.push('('),
                    ')' => out.push(')'),
                    '{' => out.push('{'),
                    '}' => out.push('}'),
                    '+' => out.push('+'),
                    '?' => out.push('?'),
                    // Backreferences (`\1`..`\9`), GNU word-boundary escapes
                    // (`\<`/`\>`), and letter escapes (`\b`/`\w`/`\s`/...) mean
                    // something different — or nothing — in RE2 than in GNU BRE.
                    // Bail rather than guess.
                    c if c.is_ascii_alphanumeric() || c == '<' || c == '>' => return None,
                    // Any other escaped punctuation (`\.`, `\*`, `\\`, ...) is a
                    // literal in both dialects; copy it through verbatim.
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
            }
            // Bare ERE metacharacters that are literals in BRE → escape.
            '|' | '(' | ')' | '{' | '}' | '+' | '?' => {
                out.push('\\');
                out.push(ch);
            }
            // `.`, `*`, `^`, `$`, and everything else pass through.
            other => out.push(other),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grep(command: &str) -> TranslatedSearch {
        translate_search_command(command).unwrap().search
    }

    fn invocation(
        program: &str,
        argv: &[&str],
    ) -> Result<TranslatedSearch, WarmSearchDeclineReason> {
        translate_search_invocation(
            program,
            &argv
                .iter()
                .map(|arg| (*arg).to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn expanded_invocation_preserves_spaces_and_multiple_roots() {
        assert!(matches!(
            invocation("rg", &["-n", "two words", "src one", "src two"]),
            Ok(TranslatedSearch::Grep { pattern, paths, show_line_numbers: true, .. })
                if pattern == "two words" && paths == ["src one", "src two"]
        ));
    }

    #[test]
    fn grep_defaults_to_case_sensitive() {
        assert!(matches!(
            invocation("grep", &["needle"]),
            Ok(TranslatedSearch::Grep {
                case_insensitive: Some(false),
                ..
            })
        ));
    }

    #[test]
    fn expanded_invocation_returns_bounded_declines() {
        assert!(matches!(
            invocation("grep", &["needle"]),
            Ok(TranslatedSearch::Grep { paths, .. }) if paths.is_empty()
        ));
        assert!(matches!(
            invocation("grep", &["-r", "needle"]),
            Ok(TranslatedSearch::Grep { paths, .. }) if paths.is_empty()
        ));
        assert_eq!(
            invocation("rg", &["needle", "-"]),
            Err(WarmSearchDeclineReason::StdinInput)
        );
        // An uncompilable pattern still *translates*: the shape was understood,
        // and each caller decides whether to serve it or let its own fallback
        // report the error.
        assert!(!pattern_compiles(&invocation("rg", &["(", "."]).unwrap()));
        assert!(pattern_compiles(
            &invocation("rg", &["needle", "."]).unwrap()
        ));
        assert!(matches!(
            invocation("rg", &["--json", "needle", "."]),
            Err(WarmSearchDeclineReason::UnsupportedFlag(flag)) if flag == "--json"
        ));
    }

    fn pipeline(command: &str) -> TranslatedSearchPipeline {
        translate_search_command(command).unwrap()
    }

    #[test]
    fn rg_plain_pattern_defaults_to_content_case_sensitive_without_line_numbers() {
        assert_eq!(
            grep("rg needle"),
            TranslatedSearch::Grep {
                pattern: "needle".to_string(),
                globs: vec![],
                output_mode: "content".to_string(),
                case_insensitive: Some(false),
                before_context: 0,
                after_context: 0,
                show_line_numbers: false,
                max_per_file: None,
                paths: vec![],
            }
        );
    }

    #[test]
    fn rg_supports_pattern_path_and_flags() {
        assert_eq!(
            grep("rg -i -n -A 2 -B3 -g '*.rs' -g '!target/**' -m 4 -e needle src"),
            TranslatedSearch::Grep {
                pattern: "needle".to_string(),
                globs: vec!["*.rs".to_string(), "!target/**".to_string()],
                output_mode: "content".to_string(),
                case_insensitive: Some(true),
                before_context: 3,
                after_context: 2,
                show_line_numbers: true,
                max_per_file: Some(4),
                paths: vec!["src".to_string()],
            }
        );
    }

    #[test]
    fn rg_supports_output_modes_context_fixed_and_files() {
        assert!(matches!(
            grep("rg -l needle"),
            TranslatedSearch::Grep { output_mode, .. } if output_mode == "files_with_matches"
        ));
        assert!(matches!(
            grep("rg -c needle"),
            TranslatedSearch::Grep { output_mode, .. } if output_mode == "count"
        ));
        assert!(matches!(
            grep("rg -C2 needle"),
            TranslatedSearch::Grep {
                before_context: 2,
                after_context: 2,
                ..
            }
        ));
        assert!(matches!(
            grep("rg -F 'a+b?'"),
            TranslatedSearch::Grep { pattern, .. } if pattern == "a\\+b\\?"
        ));
        assert_eq!(
            grep("rg --files -g '*.rs' src"),
            TranslatedSearch::Files {
                globs: vec!["*.rs".to_string()],
                path: Some("src".to_string()),
            }
        );
    }

    #[test]
    fn grep_recursive_subset_translates() {
        assert_eq!(
            grep("grep -Rinl --include='*.rs' needle src"),
            TranslatedSearch::Grep {
                pattern: "needle".to_string(),
                globs: vec!["*.rs".to_string()],
                output_mode: "files_with_matches".to_string(),
                case_insensitive: Some(true),
                before_context: 0,
                after_context: 0,
                show_line_numbers: true,
                max_per_file: None,
                paths: vec!["src".to_string()],
            }
        );
        assert!(matches!(
            grep("grep -rEc 'a+b' ."),
            TranslatedSearch::Grep { output_mode, pattern, paths, .. }
                if output_mode == "count" && pattern == "a+b" && paths == vec![".".to_string()]
        ));
        assert!(matches!(
            grep("grep -rF 'a+b?' src"),
            TranslatedSearch::Grep { pattern, .. } if pattern == "a\\+b\\?"
        ));
    }

    #[test]
    fn grep_and_rg_accept_multiple_search_paths() {
        assert!(matches!(
            grep("rg needle src tests"),
            TranslatedSearch::Grep { paths, .. }
                if paths == vec!["src".to_string(), "tests".to_string()]
        ));
        assert!(matches!(
            grep("grep -r needle src tests"),
            TranslatedSearch::Grep { paths, .. }
                if paths == vec!["src".to_string(), "tests".to_string()]
        ));
    }

    #[test]
    fn basic_grep_patterns_convert_bre_metacharacters() {
        // Bare ERE metacharacters are literals in BRE.
        for (command, expected) in [
            ("grep -r 'a+b' src", "a\\+b"),
            ("grep -r 'a?b' src", "a\\?b"),
            ("grep -r 'a|b' src", "a\\|b"),
            ("grep -r '(a)' src", "\\(a\\)"),
            // `.` is any-char in both dialects.
            ("grep -r 'a.b' src", "a.b"),
            // BRE escapes toggle to their ERE special meaning.
            ("grep -r 'a\\|b' src", "a|b"),
        ] {
            assert!(
                matches!(
                    grep(command),
                    TranslatedSearch::Grep { ref pattern, .. } if pattern == expected
                ),
                "{command} -> {expected}"
            );
        }
    }

    #[test]
    fn maps_common_rg_types_to_index_globs() {
        assert!(matches!(
            grep("rg --type ts needle src"),
            TranslatedSearch::Grep { globs, paths, .. }
                if globs == ["*.{ts,tsx}".to_string()] && paths == ["src".to_string()]
        ));
    }

    #[test]
    fn bre_to_ere_round_trips_and_bails_on_unfaithful_constructs() {
        assert_eq!(bre_to_ere("a\\(b\\)c").as_deref(), Some("a(b)c"));
        assert_eq!(bre_to_ere("a+b?").as_deref(), Some("a\\+b\\?"));
        assert_eq!(bre_to_ere("a.b*").as_deref(), Some("a.b*"));
        assert_eq!(bre_to_ere("[a|b]+").as_deref(), Some("[a|b]\\+"));
        assert_eq!(bre_to_ere("\\.").as_deref(), Some("\\."));
        // Backreferences and word-boundary / letter escapes cannot be served.
        assert_eq!(bre_to_ere("\\(a\\)\\1"), None);
        assert_eq!(bre_to_ere("\\<word\\>"), None);
        assert_eq!(bre_to_ere("\\bword"), None);
    }

    #[test]
    fn splits_and_parses_post_filter_pipelines() {
        // The full reported command translates head + tail stages.
        let p = pipeline(
            "grep -rn 'chat.stop\\|chat.newSession\\|isBrowserTab\\|isTerminalTab' \
             src/ packages/ui/src/ | grep -v test | head -40",
        );
        assert!(matches!(
            p.search,
            TranslatedSearch::Grep { ref pattern, ref paths, show_line_numbers: true, .. }
                if pattern == "chat.stop|chat.newSession|isBrowserTab|isTerminalTab"
                    && *paths == vec!["src/".to_string(), "packages/ui/src/".to_string()]
        ));
        assert_eq!(
            p.post,
            vec![
                PostFilter::Grep {
                    pattern: "test".to_string(),
                    invert: true,
                    case_insensitive: false,
                },
                PostFilter::Head(40),
            ]
        );
    }

    #[test]
    fn parses_each_post_filter_shape() {
        assert_eq!(pipeline("rg x | head -40").post, vec![PostFilter::Head(40)]);
        assert_eq!(
            pipeline("rg x | head -n 40").post,
            vec![PostFilter::Head(40)]
        );
        assert_eq!(pipeline("rg x | head").post, vec![PostFilter::Head(10)]);
        assert_eq!(pipeline("rg x | tail -5").post, vec![PostFilter::Tail(5)]);
        assert_eq!(
            pipeline("rg x | sed -n '1,40p'").post,
            vec![PostFilter::Lines { start: 1, end: 40 }]
        );
        assert_eq!(pipeline("rg x | wc -l").post, vec![PostFilter::CountLines]);
        assert_eq!(
            pipeline("rg x | sort | uniq").post,
            vec![PostFilter::Sort, PostFilter::Uniq]
        );
        assert_eq!(
            pipeline("rg x | grep -v test").post,
            vec![PostFilter::Grep {
                pattern: "test".to_string(),
                invert: true,
                case_insensitive: false,
            }]
        );
        assert_eq!(
            pipeline("rg x | grep -iv Y").post,
            vec![PostFilter::Grep {
                pattern: "Y".to_string(),
                invert: true,
                case_insensitive: true,
            }]
        );
        assert_eq!(
            pipeline("rg x | grep -F 'a.b'").post,
            vec![PostFilter::Grep {
                pattern: "a\\.b".to_string(),
                invert: false,
                case_insensitive: false,
            }]
        );
    }

    #[test]
    fn rejects_unsupported_or_malformed_pipelines() {
        for command in [
            // Non-whitelisted tail stages.
            "rg x | cat",
            "rg x | wc",
            "rg x | wc -w",
            // A recursive grep is not a line filter.
            "rg x | grep -r y src",
            "rg x | grep -l y",
            // Malformed pipelines.
            "rg x || rg y",
            "rg x |",
            "| rg x",
            "rg x | | head",
            // Head-stage is not a search command.
            "ls | grep x",
        ] {
            assert_eq!(translate_search_command(command), None, "{command}");
        }
    }

    #[test]
    fn rejects_shell_metacharacters_and_expansions() {
        for command in [
            "rg needle | cat",
            "rg needle && cat file",
            "rg needle; cat file",
            "rg $VAR",
            "rg $(pwd)",
            "rg `pwd`",
            "rg needle > out",
            "rg *.rs",
            "rg file?.rs",
            "rg [abc]",
            "rg mid~token",
        ] {
            assert_eq!(translate_search_command(command), None, "{command}");
        }
    }

    #[test]
    fn rejects_non_standalone_or_unsupported_shapes() {
        for command in [
            "git grep needle",
            "bash -lc 'rg needle'",
            "rg -e one -e two",
            "rg --smart-case needle",
            "rg --unknown needle",
            "grep -r --exclude='*.rs' needle src",
            // `grep -e PATTERN` is not translated (a basic-grep `-e` pattern
            // would need BRE conversion the `-e` path does not apply); it falls
            // through to a real subprocess. Only rg's `-e` (ERE) is accepted.
            "grep -r -e 'a+b' src",
            "grep -re needle src",
            // Backreference cannot be faithfully converted from BRE.
            "grep -r '\\(a\\)\\1' src",
        ] {
            assert_eq!(translate_search_command(command), None, "{command}");
        }
    }

    #[test]
    fn accepts_quoted_shell_sensitive_tokens_as_literals() {
        assert!(matches!(
            grep("rg 'literal * pattern'"),
            TranslatedSearch::Grep { pattern, .. } if pattern == "literal * pattern"
        ));
        assert!(matches!(
            grep("rg -g '[[]abc]' needle"),
            TranslatedSearch::Grep { globs, .. } if globs == vec!["[[]abc]".to_string()]
        ));
        assert_eq!(translate_search_command("rg 'unterminated"), None);
    }
}
