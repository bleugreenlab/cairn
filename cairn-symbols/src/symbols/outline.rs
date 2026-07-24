//! The `?outline` read lens: a file's signature skeleton — top-level items and
//! their direct members — with no bodies. Backed by the vendored
//! `ast_grep_outline::model` types as the output contract; extraction is driven
//! from the same per-language structural knowledge that powers `nav`.
//!
//! Composes with `?glob=` to produce a whole-subtree structural map for cold
//! orientation without reading bodies.

use std::borrow::Cow;
use std::path::Path;

use ast_grep_language::SupportLang;
use ast_grep_outline::model::{
    EntryRole, OutlineEntry, OutlineItem, OutlineMember, SourcePosition, SourceRange, SymbolType,
};

use super::engine::{first_line, lang_for_path, parse, SymbolNode};
use super::render::Rendered;
use super::walk::{build_globset, relative, source_files};

/// Produce an outline for a file or directory `target`, rooted at `root`.
pub fn outline(root: &Path, target: &Path, glob: Option<&str>) -> Rendered {
    if target.is_file() {
        // Single file: rows are path-less (`line:signature  [tags]`). The read
        // header (`=== uri ===`) already carries the path, so repeating it on
        // every row is pure noise.
        let display = relative(root, target).to_string_lossy().into_owned();
        return Rendered::message(outline_file(target).unwrap_or_else(|| {
            format!("no bundled grammar for {display} — outline needs a known file type")
        }));
    }
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(err) => return Rendered::message(err),
        },
        None => None,
    };
    // Multi-file walk: group each file's path-less rows under a single path
    // header line, separated by a blank line, rather than prefixing the path
    // onto every row.
    let mut blocks = Vec::new();
    for (path, _lang) in source_files(root, target, globset.as_ref()) {
        let display = relative(root, &path).to_string_lossy().into_owned();
        if let Some(rendered) = outline_file(&path) {
            if !rendered.is_empty() {
                blocks.push(format!("{display}\n{rendered}"));
            }
        }
    }
    if blocks.is_empty() {
        return Rendered::message("no outline entries");
    }
    Rendered::message(blocks.join("\n\n"))
}

pub fn outline_texts(files: &[(String, String)], glob: Option<&str>) -> Rendered {
    let globset = match glob {
        Some(raw) => match build_globset(raw) {
            Ok(set) => Some(set),
            Err(error) => return Rendered::message(error),
        },
        None => None,
    };
    let mut blocks = Vec::new();
    for (path, src) in files {
        let path_ref = Path::new(path);
        if globset.as_ref().is_some_and(|set| {
            !set.is_match(path_ref) && !path_ref.file_name().is_some_and(|name| set.is_match(name))
        }) {
            continue;
        }
        let rendered = outline_text(src, lang_for_path(path_ref));
        if !rendered.is_empty() {
            blocks.push(format!("{path}\n{rendered}"));
        }
    }
    if blocks.is_empty() {
        Rendered::message("no outline entries")
    } else {
        Rendered::message(blocks.join("\n\n"))
    }
}

fn outline_file(path: &Path) -> Option<String> {
    let lang = lang_for_path(path)?;
    let src = std::fs::read_to_string(path).ok()?;
    Some(outline_text(&src, Some(lang)))
}

/// Outline a single in-memory source as path-less rows (`line:signature  [tags]`).
/// The bytes-based entry the archival/gitcoord read path uses: a recorded
/// single-file outline reconstructs identically from a git blob (the engine
/// only parses text). `None` lang → empty (no bundled grammar).
pub fn outline_text(src: &str, lang: Option<SupportLang>) -> String {
    match lang {
        Some(lang) => render_items(&extract_items(src, lang)),
        None => String::new(),
    }
}

/// Count the file headers in a multi-file outline body. Entry rows are
/// line-numbered (`\d+:…`); every other non-empty line is a file header. Used
/// to report `N matches in M files` for a directory outline.
pub fn file_count(body: &str) -> usize {
    body.lines()
        .filter(|line| !line.is_empty() && !is_row(line))
        .count()
}

/// Whether a body line is an entry row (`\d+:…`) rather than a file header.
fn is_row(line: &str) -> bool {
    let digits = line.bytes().take_while(|b| b.is_ascii_digit()).count();
    digits > 0 && line.as_bytes().get(digits) == Some(&b':')
}

/// Extract top-level items with their direct members from a source string.
fn extract_items(src: &str, lang: SupportLang) -> Vec<OutlineItem<'static>> {
    let ast = parse(src, lang);
    let root = ast.root();
    let mut items = Vec::new();
    for child in root.children() {
        // Some grammars wrap declarations (e.g. an exported TS declaration is an
        // `export_statement` around the real node); unwrap one layer to the
        // inner declaration when the wrapper is not itself a declaration.
        let node = unwrap_decl(&child);
        let Some(symbol_type) = item_symbol_type(node.kind().as_ref()) else {
            continue;
        };
        let exported = is_exported(&child) || is_exported(&node);
        items.push(build_item(&node, symbol_type, exported));
    }
    items
}

fn unwrap_decl<'r>(node: &SymbolNode<'r>) -> SymbolNode<'r> {
    if matches!(
        node.kind().as_ref(),
        "export_statement" | "decorated_definition"
    ) {
        if let Some(inner) = node
            .children()
            .find(|c| item_symbol_type(c.kind().as_ref()).is_some())
        {
            return inner;
        }
    }
    node.clone()
}

fn build_item(node: &SymbolNode, symbol_type: SymbolType, exported: bool) -> OutlineItem<'static> {
    let members = member_list(node)
        .map(|list| {
            list.children()
                .filter_map(|child| build_member(&child))
                .collect()
        })
        .unwrap_or_default();
    OutlineItem {
        entry: entry(node, EntryRole::Item, symbol_type),
        is_import: false,
        is_exported: exported,
        members,
    }
}

fn build_member(node: &SymbolNode) -> Option<OutlineMember<'static>> {
    let symbol_type = member_symbol_type(node.kind().as_ref())?;
    Some(OutlineMember {
        entry: entry(node, EntryRole::Member, symbol_type),
        is_public: is_public(node),
    })
}

fn entry(node: &SymbolNode, role: EntryRole, symbol_type: SymbolType) -> OutlineEntry<'static> {
    OutlineEntry {
        role,
        symbol_type,
        name: Cow::Owned(declared_name(node)),
        range: source_range(node),
        signature: Cow::Owned(first_line(&node.text())),
        ast_kind: Cow::Owned(node.kind().into_owned()),
    }
}

fn source_range(node: &SymbolNode) -> SourceRange {
    let start = node.start_pos();
    let end = node.end_pos();
    SourceRange {
        byte_offset: node.range(),
        start: SourcePosition {
            line: start.line(),
            column: start.column(node),
        },
        end: SourcePosition {
            line: end.line(),
            column: end.column(node),
        },
    }
}

/// The declared name of a node: its `name` field, else the trailing identifier
/// of its `type` field (Rust `impl` blocks), else empty.
fn declared_name(node: &SymbolNode) -> String {
    if let Some(name) = node.field("name") {
        return name.text().into_owned();
    }
    if let Some(target) = node.field("type") {
        let mut last = None;
        for descendant in target.dfs() {
            if descendant.kind().ends_with("identifier") {
                last = Some(descendant.text().into_owned());
            }
        }
        if let Some(name) = last {
            return name;
        }
    }
    String::new()
}

/// The body/member-list child of a container item, if any.
fn member_list<'r>(node: &SymbolNode<'r>) -> Option<SymbolNode<'r>> {
    node.children()
        .find(|child| MEMBER_LIST_KINDS.contains(&child.kind().as_ref()))
        .or_else(|| {
            // Rust structs/impls nest the list one level under the `body` field.
            node.field("body")
                .filter(|body| MEMBER_LIST_KINDS.contains(&body.kind().as_ref()))
        })
}

const MEMBER_LIST_KINDS: &[&str] = &[
    // Rust
    "field_declaration_list",
    "enum_variant_list",
    "declaration_list",
    // TypeScript / JavaScript
    "class_body",
    "interface_body",
    "object_type",
    "enum_body",
    // Python
    "block",
    // Go
    "interface_type",
];

/// Top-level item kinds → outline symbol category. `None` means "not an item".
fn item_symbol_type(kind: &str) -> Option<SymbolType> {
    Some(match kind {
        "function_item"
        | "function_signature_item"
        | "function_declaration"
        | "generator_function_declaration"
        | "function_definition" => SymbolType::Function,
        "method_declaration" => SymbolType::Method,
        "struct_item" | "union_item" => SymbolType::Struct,
        "enum_item" | "enum_declaration" => SymbolType::Enum,
        "trait_item" | "interface_declaration" => SymbolType::Interface,
        "class_declaration" | "abstract_class_declaration" | "class_definition" => {
            SymbolType::Class
        }
        "impl_item" => SymbolType::Class,
        "type_item" | "type_alias_declaration" | "type_declaration" => SymbolType::Interface,
        "const_item" | "static_item" => SymbolType::Constant,
        "mod_item" => SymbolType::Module,
        "macro_definition" => SymbolType::Function,
        _ => return None,
    })
}

/// Direct-member kinds → outline symbol category. `None` means "not a member".
fn member_symbol_type(kind: &str) -> Option<SymbolType> {
    Some(match kind {
        "function_item" | "function_signature_item" | "function_definition" => SymbolType::Method,
        "method_definition" | "method_signature" | "method_elem" => SymbolType::Method,
        "field_declaration"
        | "public_field_definition"
        | "field_definition"
        | "property_signature" => SymbolType::Field,
        "enum_variant" | "enum_member" => SymbolType::EnumMember,
        "const_item" => SymbolType::Constant,
        _ => return None,
    })
}

/// Whether a node's first source line marks it exported/public at the top level
/// (`pub` in Rust, `export` in TS/JS).
fn is_exported(node: &SymbolNode) -> bool {
    let line = first_line(&node.text());
    line.starts_with("pub") || line.starts_with("export")
}

/// Whether a member is syntactically public (`pub` in Rust; TS members are
/// public unless marked `private`/`protected`).
fn is_public(node: &SymbolNode) -> bool {
    let line = first_line(&node.text());
    if line.starts_with("private") || line.starts_with("protected") {
        return false;
    }
    if line.starts_with("pub") {
        return true;
    }
    // Rust function/field/const members are private by default; enum variants
    // and TS/Python members are public by default.
    !matches!(
        node.kind().as_ref(),
        "function_item" | "function_signature_item" | "field_declaration" | "const_item"
    )
}

fn render_items(items: &[OutlineItem]) -> String {
    let mut lines = Vec::new();
    for item in items {
        lines.push(render_entry(
            &item.entry,
            item.is_exported,
            item.is_import,
            0,
        ));
        for member in &item.members {
            lines.push(render_entry(&member.entry, member.is_public, false, 1));
        }
    }
    lines.join("\n")
}

fn render_entry(entry: &OutlineEntry, flagged: bool, is_import: bool, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    let line = entry.range.start.line + 1;
    let mut tags = vec![symbol_type_label(entry.symbol_type).to_string()];
    if is_import {
        tags.push("import".to_string());
    }
    if flagged {
        tags.push(if depth == 0 { "exported" } else { "pub" }.to_string());
    }
    format!("{line}:{indent}{}  [{}]", entry.signature, tags.join(" "))
}

fn symbol_type_label(symbol_type: SymbolType) -> &'static str {
    match symbol_type {
        SymbolType::Function => "function",
        SymbolType::Method => "method",
        SymbolType::Struct => "struct",
        SymbolType::Enum => "enum",
        SymbolType::EnumMember => "variant",
        SymbolType::Interface => "interface",
        SymbolType::Class => "class",
        SymbolType::Field => "field",
        SymbolType::Property => "property",
        SymbolType::Constant => "constant",
        SymbolType::Module => "module",
        SymbolType::TypeParameter => "type",
        _ => "symbol",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_rust_items_and_members() {
        let src = "pub fn alpha() {}\n\npub struct Bar {\n    x: u32,\n    pub y: i32,\n}\n";
        let items = extract_items(src, SupportLang::Rust);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].entry.name, "alpha");
        assert!(items[0].is_exported);
        assert_eq!(items[1].entry.name, "Bar");
        assert_eq!(items[1].members.len(), 2);
        assert_eq!(items[1].members[0].entry.name, "x");
        assert!(!items[1].members[0].is_public);
        assert!(items[1].members[1].is_public);
    }

    #[test]
    fn extracts_rust_impl_methods() {
        let src = "struct Foo;\nimpl Foo {\n    pub fn go(&self) {}\n    fn hidden(&self) {}\n}\n";
        let items = extract_items(src, SupportLang::Rust);
        let impl_item = items
            .iter()
            .find(|i| i.entry.ast_kind == "impl_item")
            .unwrap();
        assert_eq!(impl_item.entry.name, "Foo");
        assert_eq!(impl_item.members.len(), 2);
        assert!(impl_item.members[0].is_public);
        assert!(!impl_item.members[1].is_public);
    }

    #[test]
    fn extracts_typescript_class_members() {
        let src = "export class Circle {\n  radius: number;\n  area() { return 0; }\n}\n";
        let items = extract_items(src, SupportLang::TypeScript);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].entry.name, "Circle");
        assert!(items[0].is_exported);
        let names: Vec<_> = items[0]
            .members
            .iter()
            .map(|m| m.entry.name.as_ref())
            .collect();
        assert!(names.contains(&"radius"));
        assert!(names.contains(&"area"));
    }

    #[test]
    fn extracts_python_class_methods() {
        let src = "class Animal:\n    def speak(self):\n        pass\n    def move(self):\n        pass\n";
        let items = extract_items(src, SupportLang::Python);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].entry.name, "Animal");
        let names: Vec<_> = items[0]
            .members
            .iter()
            .map(|m| m.entry.name.as_ref())
            .collect();
        assert!(names.contains(&"speak"));
        assert!(names.contains(&"move"));
    }

    #[test]
    fn renders_greppable_rows() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "pub fn alpha() {}\n").unwrap();
        let r = outline(dir.path(), &dir.path().join("a.rs"), None);
        // A single-file outline is path-less: the read header carries the path.
        assert_eq!(r.body, "1:pub fn alpha() {}  [function exported]");
        assert!(!r.body.contains("a.rs"));
    }

    #[test]
    fn multi_file_groups_rows_under_path_headers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "pub fn alpha() {}\n").unwrap();
        std::fs::write(dir.path().join("b.rs"), "pub fn beta() {}\n").unwrap();
        let r = outline(dir.path(), dir.path(), Some("**/*.rs"));
        // Each file contributes a bare path header followed by path-less rows.
        assert!(r
            .body
            .contains("a.rs\n1:pub fn alpha() {}  [function exported]"));
        assert!(r
            .body
            .contains("b.rs\n1:pub fn beta() {}  [function exported]"));
        // Two header lines, two rows.
        assert_eq!(file_count(&r.body), 2);
    }

    #[test]
    fn file_count_counts_headers_not_rows() {
        let body = "src/a.rs\n1:fn x()  [function]\n2:fn y()  [function]\n\nsrc/b.rs\n3:fn z()  [function]";
        assert_eq!(file_count(body), 2);
    }
}
