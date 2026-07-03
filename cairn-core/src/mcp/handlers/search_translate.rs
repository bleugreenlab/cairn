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
        path: Option<String>,
    },
    Files {
        globs: Vec<String>,
        path: Option<String>,
    },
}

#[derive(Debug, Clone)]
struct Token {
    text: String,
    has_unquoted_expansion: bool,
}

pub(crate) fn translate_search_command(command: &str) -> Option<TranslatedSearch> {
    let tokens = tokenize(command)?;
    let first = tokens.first()?;
    match first.text.as_str() {
        "rg" => translate_rg(&tokens[1..]),
        "grep" => translate_grep(&tokens[1..]),
        _ => None,
    }
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
    path: Option<String>,
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
            path: None,
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
        if text == "--smart-case" || text == "-S" || text == "-t" || text == "--type" {
            return None;
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
        if text == "-i" {
            parsed.case_insensitive = Some(true);
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
    let mut parsed = ParsedGrepLike::default();
    let mut recursive = false;
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
            recursive = true;
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
                    'r' | 'R' => recursive = true,
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

    if !recursive || positionals.is_empty() || positionals.len() > 2 {
        return None;
    }
    parsed.pattern = Some(positionals.remove(0));
    if let Some(path) = positionals.pop() {
        parsed.path = Some(path);
    }

    let pattern = parsed.pattern.as_ref()?;
    if !extended && !parsed.fixed_strings && contains_regex_metachar(pattern) {
        return None;
    }

    finish_grep(parsed, Vec::new())
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
    if positionals.len() > 1 || (parsed.path.is_some() && !positionals.is_empty()) {
        return None;
    }
    if parsed.path.is_none() {
        parsed.path = positionals.pop();
    }
    let mut pattern = parsed.pattern?;
    if parsed.fixed_strings {
        pattern = escape(&pattern);
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
        path: parsed.path,
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

fn contains_regex_metachar(pattern: &str) -> bool {
    pattern.chars().any(|ch| {
        matches!(
            ch,
            '.' | '^' | '$' | '*' | '[' | ']' | '\\' | '+' | '?' | '|' | '(' | ')' | '{' | '}'
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grep(command: &str) -> TranslatedSearch {
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
                path: None,
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
                path: Some("src".to_string()),
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
                path: Some("src".to_string()),
            }
        );
        assert!(matches!(
            grep("grep -rEc 'a+b' ."),
            TranslatedSearch::Grep { output_mode, pattern, path, .. }
                if output_mode == "count" && pattern == "a+b" && path.as_deref() == Some(".")
        ));
        assert!(matches!(
            grep("grep -rF 'a+b?' src"),
            TranslatedSearch::Grep { pattern, .. } if pattern == "a\\+b\\?"
        ));
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
            "rg needle src tests",
            "rg -e one -e two",
            "rg -t rust needle",
            "rg --type rust needle",
            "rg --smart-case needle",
            "rg --unknown needle",
            "grep needle src",
            "grep -r 'a+b' src",
            "grep -r 'a?b' src",
            "grep -r 'a|b' src",
            "grep -r '(a)' src",
            "grep -r 'a.b' src",
            "grep -r needle src tests",
            "grep -r --exclude='*.rs' needle src",
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
