//! Per-language structural knowledge: which tree-sitter node kinds declare a
//! named symbol and which are call expressions. This is the data that adapts
//! the language-agnostic navigation walk in [`super::nav`] to each grammar.
//!
//! Adding a language is a data addition here, not new plumbing. References work
//! for every bundled grammar without an entry (the walk matches any node whose
//! kind ends in `identifier`); declarations and callers need the kind tables.

use ast_grep_language::SupportLang;

/// The structural node-kind tables for one language.
pub struct LangSpec {
    /// Declaration node kinds that carry a `name` field (functions, types, etc.).
    decls: &'static [&'static str],
    /// Call-expression node kinds.
    calls: &'static [&'static str],
    /// Fields to try, in order, for the callee of a call node.
    callee_fields: &'static [&'static str],
}

impl LangSpec {
    /// Whether `kind` declares a named symbol.
    pub fn is_decl(&self, kind: &str) -> bool {
        self.decls.contains(&kind)
    }

    /// Whether `kind` is a call expression.
    pub fn is_call(&self, kind: &str) -> bool {
        self.calls.contains(&kind)
    }

    /// Fields to probe, in order, for a call node's callee.
    pub fn callee_fields(&self) -> &'static [&'static str] {
        self.callee_fields
    }
}

const RUST: LangSpec = LangSpec {
    decls: &[
        "function_item",
        "function_signature_item",
        "struct_item",
        "enum_item",
        "union_item",
        "trait_item",
        "type_item",
        "const_item",
        "static_item",
        "mod_item",
        "macro_definition",
    ],
    calls: &["call_expression", "macro_invocation"],
    callee_fields: &["function", "macro"],
};

const TS: LangSpec = LangSpec {
    decls: &[
        "function_declaration",
        "generator_function_declaration",
        "class_declaration",
        "abstract_class_declaration",
        "interface_declaration",
        "enum_declaration",
        "type_alias_declaration",
        "method_definition",
        "public_field_definition",
    ],
    calls: &["call_expression"],
    callee_fields: &["function"],
};

const JS: LangSpec = LangSpec {
    decls: &[
        "function_declaration",
        "generator_function_declaration",
        "class_declaration",
        "method_definition",
        "field_definition",
    ],
    calls: &["call_expression"],
    callee_fields: &["function"],
};

const PYTHON: LangSpec = LangSpec {
    decls: &["function_definition", "class_definition"],
    calls: &["call"],
    callee_fields: &["function"],
};

const GO: LangSpec = LangSpec {
    decls: &[
        "function_declaration",
        "method_declaration",
        "type_spec",
    ],
    calls: &["call_expression"],
    callee_fields: &["function"],
};

const EMPTY: LangSpec = LangSpec {
    decls: &[],
    calls: &[],
    callee_fields: &["function"],
};

/// The structural spec for a language. Languages without a first-cut spec fall
/// back to [`EMPTY`] — references still work, declarations/callers return empty.
pub fn spec(lang: SupportLang) -> &'static LangSpec {
    match lang {
        SupportLang::Rust => &RUST,
        SupportLang::TypeScript | SupportLang::Tsx => &TS,
        SupportLang::JavaScript => &JS,
        SupportLang::Python => &PYTHON,
        SupportLang::Go => &GO,
        _ => &EMPTY,
    }
}
