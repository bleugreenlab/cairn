//! Translating an `rg` or `grep` argv into the read grep it asks for.
//!
//! The shell has already been read by [`super::shell_words`], so this sees only
//! the program and the argument words that would reach it. Every flag accepted
//! here maps onto a read `?grep=` modifier or folds into the pattern (`-F`,
//! `-w`, basic-regex conversion); any other flag declines naming itself, and
//! `run::read_equivalent` reports it as the reason the item needs a machine.

use regex::escape;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SearchTranslationError {
    /// The search reads stdin, which no read target names.
    StdinInput,
    UnsupportedProgram,
    /// A flag with no read equivalent, as written.
    UnsupportedFlag(String),
    /// A basic-regex construct RE2 cannot express faithfully: a backreference,
    /// or a GNU word-boundary or letter escape.
    UnsupportedPattern,
    /// No pattern, a flag missing its value, or a count that is not a number.
    UnsupportedInvocation,
}

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
        /// Zero or more search-path arguments. Empty searches the whole tree.
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

/// Translate one search invocation: `program` and the argument words the
/// shell would hand it.
pub(crate) fn translate_search_invocation(
    program: &str,
    argv: &[String],
) -> Result<TranslatedSearch, SearchTranslationError> {
    let program = std::path::Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(program);
    if argv.iter().any(|arg| arg == "-") {
        return Err(SearchTranslationError::StdinInput);
    }
    match program {
        "rg" => translate_rg(argv),
        "grep" => translate_grep(argv),
        _ => Err(SearchTranslationError::UnsupportedProgram),
    }
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
            case_insensitive: Some(false),
            before_context: 0,
            after_context: 0,
            show_line_numbers: false,
            max_per_file: None,
            fixed_strings: false,
            word_regexp: false,
        }
    }
}

fn unsupported(flag: &str) -> SearchTranslationError {
    SearchTranslationError::UnsupportedFlag(flag.to_string())
}

fn translate_rg(argv: &[String]) -> Result<TranslatedSearch, SearchTranslationError> {
    let mut parsed = ParsedGrepLike::default();
    let mut saw_files = false;
    let mut positionals = Vec::new();
    let mut index = 0;

    while index < argv.len() {
        let text = argv[index].as_str();
        if text == "--" {
            positionals.extend(argv[index + 1..].iter().cloned());
            break;
        }
        if text == "--files" {
            saw_files = true;
            index += 1;
            continue;
        }
        if text == "-t" || text == "--type" {
            let value = take_value(argv, &mut index)?;
            let glob = type_glob(&value).ok_or_else(|| unsupported(&format!("{text} {value}")))?;
            parsed.globs.push(glob.to_string());
            continue;
        }
        if let Some(value) = text.strip_prefix("--glob=") {
            parsed.globs.push(value.to_string());
            index += 1;
            continue;
        }
        if text == "--glob" || text == "-g" {
            let value = take_value(argv, &mut index)?;
            parsed.globs.push(value);
            continue;
        }
        if let Some(value) = text.strip_prefix("--max-count=") {
            parsed.max_per_file = Some(parse_usize(value)?);
            index += 1;
            continue;
        }
        if text == "--max-count" || text == "-m" {
            let value = take_value(argv, &mut index)?;
            parsed.max_per_file = Some(parse_usize(&value)?);
            continue;
        }
        if let Some(value) = text.strip_prefix("-m").filter(|value| !value.is_empty()) {
            parsed.max_per_file = Some(parse_usize(value)?);
            index += 1;
            continue;
        }
        match text {
            "--line-number" | "-n" => parsed.show_line_numbers = true,
            "--no-line-number" | "-N" => parsed.show_line_numbers = false,
            "-i" | "--ignore-case" => parsed.case_insensitive = Some(true),
            "-w" | "--word-regexp" => parsed.word_regexp = true,
            "-l" | "--files-with-matches" => parsed.output_mode = "files_with_matches".to_string(),
            "-c" | "--count" => parsed.output_mode = "count".to_string(),
            "-F" | "--fixed-strings" => parsed.fixed_strings = true,
            "-e" | "--regexp" => {
                if parsed.pattern.is_some() {
                    return Err(unsupported(text));
                }
                parsed.pattern = Some(take_value(argv, &mut index)?);
                continue;
            }
            "-A" | "-B" | "-C" => {
                let value = parse_usize(&take_value(argv, &mut index)?)?;
                apply_context(&mut parsed, text, value);
                continue;
            }
            _ => {
                if let Some(value) = text.strip_prefix("-e").filter(|value| !value.is_empty()) {
                    if parsed.pattern.is_some() {
                        return Err(unsupported("-e"));
                    }
                    parsed.pattern = Some(value.to_string());
                } else if let Some((flag, value)) = short_context_value(text) {
                    apply_context(&mut parsed, flag, parse_usize(value)?);
                } else if text.starts_with('-') {
                    return Err(unsupported(text));
                } else {
                    positionals.push(text.to_string());
                }
            }
        }
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
            return Err(unsupported("--files"));
        }
        return Ok(TranslatedSearch::Files {
            globs: parsed.globs,
            path: positionals.pop(),
        });
    }

    finish_grep(parsed, positionals)
}

fn translate_grep(argv: &[String]) -> Result<TranslatedSearch, SearchTranslationError> {
    let mut parsed = ParsedGrepLike::default();
    let mut extended = false;
    let mut recursive = false;
    let mut positionals = Vec::new();

    for (index, text) in argv.iter().enumerate() {
        if text == "--" {
            positionals.extend(argv[index + 1..].iter().cloned());
            break;
        }
        if let Some(value) = text.strip_prefix("--include=") {
            parsed.globs.push(value.to_string());
            continue;
        }
        if text == "--recursive" {
            recursive = true;
            continue;
        }
        if let Some(flags) = text
            .strip_prefix('-')
            .filter(|flags| !flags.is_empty() && !flags.starts_with('-'))
        {
            for flag in flags.chars() {
                match flag {
                    'r' | 'R' => recursive = true,
                    'i' => parsed.case_insensitive = Some(true),
                    'n' => parsed.show_line_numbers = true,
                    'l' => parsed.output_mode = "files_with_matches".to_string(),
                    'c' => parsed.output_mode = "count".to_string(),
                    'E' => extended = true,
                    'F' => parsed.fixed_strings = true,
                    'w' => parsed.word_regexp = true,
                    other => return Err(unsupported(&format!("-{other}"))),
                }
            }
            continue;
        }
        if text.starts_with('-') {
            return Err(unsupported(text));
        }
        positionals.push(text.clone());
    }

    if positionals.is_empty() {
        return Err(SearchTranslationError::UnsupportedInvocation);
    }
    // Without `-r` and without a file, grep reads stdin.
    if !recursive && positionals.len() == 1 {
        return Err(SearchTranslationError::StdinInput);
    }
    // The first positional is the pattern; the rest are search paths. Basic
    // grep patterns are converted to the index's ERE dialect unless `-E`
    // (already ERE) or `-F` (literal).
    let raw_pattern = positionals.remove(0);
    let pattern = if extended || parsed.fixed_strings {
        raw_pattern
    } else {
        bre_to_ere(&raw_pattern).ok_or(SearchTranslationError::UnsupportedPattern)?
    };
    parsed.pattern = Some(pattern);

    finish_grep(parsed, positionals)
}

fn finish_grep(
    mut parsed: ParsedGrepLike,
    mut positionals: Vec<String>,
) -> Result<TranslatedSearch, SearchTranslationError> {
    if parsed.pattern.is_none() {
        if positionals.is_empty() {
            return Err(SearchTranslationError::UnsupportedInvocation);
        }
        parsed.pattern = Some(positionals.remove(0));
    }
    let mut pattern = parsed
        .pattern
        .ok_or(SearchTranslationError::UnsupportedInvocation)?;
    if parsed.fixed_strings {
        pattern = escape(&pattern);
    }
    if parsed.word_regexp {
        pattern = format!(r"\b(?:{pattern})\b");
    }
    Ok(TranslatedSearch::Grep {
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

fn take_value(argv: &[String], index: &mut usize) -> Result<String, SearchTranslationError> {
    let value = argv
        .get(*index + 1)
        .cloned()
        .ok_or(SearchTranslationError::UnsupportedInvocation)?;
    *index += 2;
    Ok(value)
}

fn parse_usize(value: &str) -> Result<usize, SearchTranslationError> {
    value
        .parse::<usize>()
        .map_err(|_| SearchTranslationError::UnsupportedInvocation)
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

/// Convert a POSIX basic-regex (BRE) pattern to the ERE/RE2 dialect the read
/// grep uses. BRE gives `\|`/`\(`/`\)`/`\{`/`\}`/`\+`/`\?` their special meaning
/// and treats the bare forms as literals, the exact inverse of ERE, so each is
/// toggled. `.`/`*`/`^`/`$` and bracket expressions mean the same in both and
/// pass through untouched. Returns `None` for constructs RE2 cannot model
/// faithfully (backreferences, and the GNU word-boundary and letter escapes
/// whose semantics diverge), rather than silently changing the match set.
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
                    // something different, or nothing, in RE2 than in GNU BRE.
                    c if c.is_ascii_alphanumeric() || c == '<' || c == '>' => return None,
                    // Any other escaped punctuation (`\.`, `\*`, `\\`, ...) is a
                    // literal in both dialects; copy it through verbatim.
                    other => {
                        out.push('\\');
                        out.push(other);
                    }
                }
            }
            // Bare ERE metacharacters that are literals in BRE are escaped.
            '|' | '(' | ')' | '{' | '}' | '+' | '?' => {
                out.push('\\');
                out.push(ch);
            }
            other => out.push(other),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::handlers::shell_words::{simple_commands, Piece};

    /// The invocation a one-command shell line hands its program.
    fn translate(command: &str) -> Result<TranslatedSearch, SearchTranslationError> {
        let Some(Piece::Command(words)) = simple_commands(command).unwrap().pop() else {
            panic!("{command} is one simple command");
        };
        let words: Vec<String> = words.into_iter().map(|word| word.text).collect();
        translate_search_invocation(&words[0], &words[1..])
    }

    fn grep(command: &str) -> TranslatedSearch {
        translate(command).unwrap()
    }

    #[test]
    fn expanded_invocation_preserves_spaces_and_multiple_roots() {
        assert!(matches!(
            grep("rg -n 'two words' 'src one' 'src two'"),
            TranslatedSearch::Grep { pattern, paths, show_line_numbers: true, .. }
                if pattern == "two words" && paths == ["src one", "src two"]
        ));
    }

    #[test]
    fn grep_defaults_to_case_sensitive() {
        assert!(matches!(
            grep("grep -r needle"),
            TranslatedSearch::Grep {
                case_insensitive: Some(false),
                ..
            }
        ));
    }

    #[test]
    fn declines_name_what_has_no_read_equivalent() {
        // Without `-r` and without a file, grep reads stdin.
        assert_eq!(
            translate("grep needle"),
            Err(SearchTranslationError::StdinInput)
        );
        assert!(matches!(
            grep("grep -r needle"),
            TranslatedSearch::Grep { paths, .. } if paths.is_empty()
        ));
        assert_eq!(
            translate("rg needle -"),
            Err(SearchTranslationError::StdinInput)
        );
        for (command, flag) in [
            ("rg --json needle .", "--json"),
            ("rg -i --json needle", "--json"),
            ("rg -e one -e two", "-e"),
            ("rg --smart-case needle", "--smart-case"),
            ("rg -t java needle", "-t java"),
            ("grep -r --exclude=*.rs needle src", "--exclude=*.rs"),
            ("grep -rv needle src", "-v"),
            ("grep -r -e a+b src", "-e"),
        ] {
            assert_eq!(
                translate(command),
                Err(SearchTranslationError::UnsupportedFlag(flag.to_string())),
                "{command}"
            );
        }
        assert_eq!(
            translate(r"grep -r '\(a\)\1' src"),
            Err(SearchTranslationError::UnsupportedPattern)
        );
        assert_eq!(
            translate("git grep needle"),
            Err(SearchTranslationError::UnsupportedProgram)
        );
        assert_eq!(
            translate("rg -A"),
            Err(SearchTranslationError::UnsupportedInvocation)
        );
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
        assert!(matches!(
            grep("rg -w word"),
            TranslatedSearch::Grep { pattern, .. } if pattern == r"\b(?:word)\b"
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
        // A file argument makes a non-recursive grep a read of that file.
        assert!(matches!(
            grep("grep -n needle README.md"),
            TranslatedSearch::Grep { paths, .. } if paths == ["README.md"]
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
    fn accepts_quoted_shell_sensitive_tokens_as_literals() {
        assert!(matches!(
            grep("rg 'literal * pattern'"),
            TranslatedSearch::Grep { pattern, .. } if pattern == "literal * pattern"
        ));
        assert!(matches!(
            grep("rg -g '[[]abc]' needle"),
            TranslatedSearch::Grep { globs, .. } if globs == vec!["[[]abc]".to_string()]
        ));
    }
}
