//! ast-grep parse + pattern execution, mapped to owned render-ready rows.
//!
//! All structural queries flow through here: detect the language from a file
//! extension, parse the source into a tree-sitter tree, compile an ast-grep
//! `Pattern`, and collect matches. User-supplied `?ast=` patterns are validated
//! with the fallible [`compile_pattern`] so a malformed pattern returns a
//! friendly error instead of panicking inside tree-sitter.

use std::ops::Range;
use std::path::Path;

use ast_grep_core::matcher::Pattern;
use ast_grep_core::tree_sitter::StrDoc;
use ast_grep_core::{AstGrep, Language};
use ast_grep_language::{LanguageExt, SupportLang};

/// The concrete ast-grep document type for a bundled language.
pub type Doc = StrDoc<SupportLang>;
/// A node in a parsed source tree.
pub type SymbolNode<'r> = ast_grep_core::Node<'r, Doc>;

/// Detect the structural language for a path by file extension. Returns `None`
/// for extensions without a bundled grammar (the read surface treats those as
/// "structural search not available for this file type").
pub fn lang_for_path(path: &Path) -> Option<SupportLang> {
    SupportLang::from_path(path)
}

/// Parse `src` under `lang` into a reusable syntax tree. Parsing is infallible:
/// tree-sitter always produces a tree, inserting error nodes for invalid input.
pub fn parse(src: &str, lang: SupportLang) -> AstGrep<Doc> {
    lang.ast_grep(src)
}

/// One structural match collected into an owned, render-ready shape. Byte
/// offsets come straight from tree-sitter (no UTF-16 conversion).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AstMatch {
    /// 0-based start line.
    pub start_line: usize,
    /// 0-based end line.
    pub end_line: usize,
    /// Byte range of the match within the source.
    pub byte_range: Range<usize>,
    /// The matched node's full source text.
    pub text: String,
}

impl AstMatch {
    /// The first non-empty source line of the match, trimmed — the single-line
    /// snippet used in `path:line:snippet` rows.
    pub fn snippet(&self) -> String {
        first_line(&self.text)
    }
}

/// Compile a user-supplied `?ast=` pattern against `lang`, surfacing a friendly
/// error when the pattern is malformed rather than panicking. ast-grep's
/// `str` matcher builds patterns with the infallible `Pattern::new` (which can
/// panic), so callers that accept untrusted patterns must route through here.
pub fn compile_pattern(pattern: &str, lang: SupportLang) -> Result<Pattern, String> {
    if pattern.trim().is_empty() {
        return Err("empty ast pattern".to_string());
    }
    Pattern::try_new(pattern, lang).map_err(|err| format!("invalid ast pattern: {err}"))
}

/// Run a compiled pattern over a parsed tree, collecting owned match rows.
pub fn run_pattern(ast: &AstGrep<Doc>, pattern: &Pattern) -> Vec<AstMatch> {
    ast.root()
        .find_all(pattern)
        .map(|m| AstMatch {
            start_line: m.start_pos().line(),
            end_line: m.end_pos().line(),
            byte_range: m.range(),
            text: m.text().to_string(),
        })
        .collect()
}

/// Convenience for a single source string: compile + parse + run.
pub fn search_source(src: &str, lang: SupportLang, pattern: &str) -> Result<Vec<AstMatch>, String> {
    let compiled = compile_pattern(pattern, lang)?;
    let ast = parse(src, lang);
    Ok(run_pattern(&ast, &compiled))
}

/// The first non-empty line of `text`, trimmed of surrounding whitespace.
pub fn first_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_language_by_extension() {
        assert_eq!(
            lang_for_path(Path::new("src/lib.rs")),
            Some(SupportLang::Rust)
        );
        assert_eq!(
            lang_for_path(Path::new("a/b.py")),
            Some(SupportLang::Python)
        );
        assert_eq!(lang_for_path(Path::new("main.go")), Some(SupportLang::Go));
        assert_eq!(
            lang_for_path(Path::new("app.ts")),
            Some(SupportLang::TypeScript)
        );
        assert!(lang_for_path(Path::new("notes.unknownext")).is_none());
    }

    #[test]
    fn finds_rust_pattern_matches_with_byte_ranges() {
        let src = "fn alpha() {}\nfn beta() { alpha(); }\n";
        let hits = search_source(src, SupportLang::Rust, "alpha()").unwrap();
        // The call site `alpha()` matches (the declaration `fn alpha()` is a
        // different node shape, so the call expression is the structural hit).
        assert!(!hits.is_empty());
        let call = hits.iter().find(|h| h.start_line == 1).unwrap();
        assert_eq!(&src[call.byte_range.clone()], "alpha()");
        assert_eq!(call.snippet(), "alpha()");
    }

    #[test]
    fn metavariable_pattern_matches_function_items() {
        let src = "fn one() {}\nfn two() {}\n";
        let hits = search_source(src, SupportLang::Rust, "fn $NAME() {}").unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].start_line, 0);
        assert_eq!(hits[1].start_line, 1);
    }

    #[test]
    fn malformed_pattern_reports_error_not_panic() {
        let err = search_source("x = 1", SupportLang::Python, "   ").unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn python_pattern_matches() {
        let src = "def greet(name):\n    return name\n";
        let hits = search_source(src, SupportLang::Python, "def $F($A):\n    $$$BODY").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].start_line, 0);
    }
}
