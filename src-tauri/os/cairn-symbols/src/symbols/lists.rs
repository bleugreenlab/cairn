//! The lists in a Rust file whose elements commute: what an ordered-set merge
//! may merge element by element instead of line by line.
//!
//! A registry is a list two branches append to independently. A line merge sees
//! only the lines, so two appends at one list's end are a conflict, and taking
//! both sides' regions whole has closed a list twice (cairn/5192). This module
//! names each list's elements, so a merge can take the union of what each side
//! inserted and still render one list.
//!
//! The allowlist is deliberately conservative:
//! - a comma-separated array literal (`[a, b]`, but never the `[x; N]` repeat);
//! - a macro token tree whose top level is comma-separated and holds no `;`
//!   (the `*_lineage![…]` composers, `vec![…]`, a pin passed as `&[…]` inside a
//!   macro body);
//! - a `use` list (`use a::{b, c}`);
//! - a run of consecutive bodiless `mod name;`, `use`, and `extern crate`
//!   declarations in a file or an inline module body.
//!
//! Functions, impls, structs, and every other item are never elements: two
//! same-named functions both kept would parse and not compile, and an item
//! edited on one side and replaced on the other is not two entries.
//!
//! A comment or attribute attaches to the element it precedes; a comment on the
//! same line after an element stays with that element. Anything else in a file
//! is not a list here, and a merge leaves it to the line merge.

use std::ops::Range;

use ast_grep_language::SupportLang;

use super::engine::{parse, SymbolNode};

/// Which shape a list has, which decides how its elements are separated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListKind {
    /// `[a, b]`.
    Array,
    /// The comma-separated contents of a macro token tree.
    TokenTree,
    /// `use a::{b, c}`.
    UseList,
    /// A run of consecutive module declarations and imports, separated by
    /// whitespace.
    Items,
}

impl ListKind {
    /// Whether elements are separated by commas.
    pub fn comma_separated(self) -> bool {
        !matches!(self, Self::Items)
    }
}

/// One element of a list, as byte ranges into the source it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListElement {
    /// The element with its attached leading comments and attributes, and for
    /// an item its same-line trailing comment. Never includes a separating
    /// comma.
    pub span: Range<usize>,
    /// The element proper: what identifies it.
    pub body: Range<usize>,
    /// For a comma list, a comment on the element's line after its comma.
    pub trailing: Option<Range<usize>>,
}

/// One list in a parsed file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustList {
    pub kind: ListKind,
    /// Byte offset of the opening delimiter; `None` for a declaration run,
    /// which has none.
    pub open: Option<usize>,
    /// Everything between the delimiters; for a declaration run, its first
    /// element's start to its last element's end.
    pub content: Range<usize>,
    pub elements: Vec<ListElement>,
    /// For a comma list, whether the last element is followed by a comma.
    pub trailing_comma: bool,
    /// Comments after the last element and before the closing delimiter, which
    /// belong to no element.
    pub tail: Option<Range<usize>>,
}

/// Every allowlisted list in `src`, outermost first. `None` when the file does
/// not parse cleanly: a tree with error nodes says nothing reliable about where
/// its lists begin and end.
pub fn rust_lists(src: &str) -> Option<Vec<RustList>> {
    let ast = parse(src, SupportLang::Rust);
    let root = ast.root();
    if root.dfs().any(|node| node.is_error() || node.is_missing()) {
        return None;
    }
    let mut lists = Vec::new();
    for node in root.dfs() {
        match node.kind().as_ref() {
            "source_file" => {
                let children: Vec<SymbolNode<'_>> = node.children().collect();
                lists.extend(declaration_runs(src, &children));
            }
            "declaration_list" => {
                if let Some((_, children)) = delimited(&node) {
                    lists.extend(declaration_runs(src, &children));
                }
            }
            "array_expression" => lists.extend(comma_list(src, &node, ListKind::Array)),
            "token_tree" => lists.extend(comma_list(src, &node, ListKind::TokenTree)),
            "use_list" => lists.extend(comma_list(src, &node, ListKind::UseList)),
            _ => {}
        }
    }
    Some(lists)
}

/// The opening delimiter's offset and the children between the delimiters.
fn delimited<'r>(node: &SymbolNode<'r>) -> Option<(usize, Vec<SymbolNode<'r>>)> {
    let children: Vec<SymbolNode<'r>> = node.children().collect();
    let (first, last) = (children.first()?, children.last()?);
    let paired = matches!(
        (first.kind().as_ref(), last.kind().as_ref()),
        ("[", "]") | ("(", ")") | ("{", "}")
    );
    if !paired || children.len() < 2 {
        return None;
    }
    let open = first.range().start;
    Some((open, children[1..children.len() - 1].to_vec()))
}

fn is_comment(node: &SymbolNode<'_>) -> bool {
    matches!(node.kind().as_ref(), "line_comment" | "block_comment")
}

/// A node's byte range, except that a comment's ends before any newline the
/// grammar folded into it (doc comments carry their line terminator).
fn range_of(src: &str, node: &SymbolNode<'_>) -> Range<usize> {
    let range = node.range();
    if !is_comment(node) {
        return range;
    }
    let trimmed = src[range.clone()].trim_end_matches(['\n', '\r']).len();
    range.start..range.start + trimmed
}

fn is_inner_doc(src: &str, node: &SymbolNode<'_>) -> bool {
    let text = &src[node.range()];
    text.starts_with("//!") || text.starts_with("/*!")
}

fn same_line(src: &str, from: usize, to: usize) -> bool {
    !src[from..to].contains('\n')
}

fn comma_list(src: &str, node: &SymbolNode<'_>, kind: ListKind) -> Option<RustList> {
    let (open, children) = delimited(node)?;
    if children.iter().any(|child| child.kind() == ";") {
        return None;
    }
    let content = open + 1..node.range().end - 1;
    let mut elements = Vec::new();
    let mut leading: Option<usize> = None;
    let mut body: Option<Range<usize>> = None;
    let mut trailing_comma = false;
    let mut iter = children.iter().peekable();
    while let Some(child) = iter.next() {
        let range = range_of(src, child);
        if child.kind() == "," {
            let body = body.take()?;
            let trailing = match iter.peek() {
                Some(next) if is_comment(next) && same_line(src, range.end, next.range().start) => {
                    let comment = range_of(src, next);
                    iter.next();
                    Some(comment)
                }
                _ => None,
            };
            elements.push(ListElement {
                span: leading.take().unwrap_or(body.start)..body.end,
                body,
                trailing,
            });
            trailing_comma = true;
        } else if is_comment(child) {
            match body.as_mut() {
                None => {
                    leading.get_or_insert(range.start);
                }
                Some(current) => current.end = range.end,
            }
        } else {
            trailing_comma = false;
            match body.as_mut() {
                None => body = Some(range),
                Some(current) => current.end = range.end,
            }
        }
    }
    if let Some(body) = body {
        elements.push(ListElement {
            span: leading.take().unwrap_or(body.start)..body.end,
            body,
            trailing: None,
        });
        trailing_comma = false;
    }
    let tail = leading.map(|start| {
        let end = children
            .iter()
            .map(|child| range_of(src, child).end)
            .max()
            .unwrap_or(start);
        start..end
    });
    Some(RustList {
        kind,
        open: Some(open),
        content,
        elements,
        trailing_comma,
        tail,
    })
}

/// Whether an item commutes with its neighbours as a registry entry: a module
/// declaration without a body, an import, or an extern crate.
fn is_declaration(node: &SymbolNode<'_>) -> bool {
    match node.kind().as_ref() {
        "use_declaration" | "extern_crate_declaration" => true,
        "mod_item" => !node
            .children()
            .any(|child| child.kind() == "declaration_list"),
        _ => false,
    }
}

fn close_run(run: &mut Vec<ListElement>, runs: &mut Vec<RustList>) {
    if let (Some(first), Some(last)) = (run.first(), run.last()) {
        let content = first.span.start..last.span.end;
        runs.push(RustList {
            kind: ListKind::Items,
            open: None,
            content,
            elements: std::mem::take(run),
            trailing_comma: false,
            tail: None,
        });
    }
}

/// The runs of consecutive declarations among a file's or module body's
/// items. Anything else ends a run, together with the comments before it.
fn declaration_runs(src: &str, children: &[SymbolNode<'_>]) -> Vec<RustList> {
    let mut runs = Vec::new();
    let mut run: Vec<ListElement> = Vec::new();
    let mut leading: Option<usize> = None;
    for child in children {
        let range = range_of(src, child);
        if is_comment(child) && !is_inner_doc(src, child) {
            match run.last_mut() {
                Some(previous)
                    if leading.is_none() && same_line(src, previous.span.end, range.start) =>
                {
                    previous.span.end = range.end;
                }
                _ => {
                    leading.get_or_insert(range.start);
                }
            }
        } else if child.kind() == "attribute_item" {
            leading.get_or_insert(range.start);
        } else if is_declaration(child) {
            run.push(ListElement {
                span: leading.take().unwrap_or(range.start)..range.end,
                body: range,
                trailing: None,
            });
        } else {
            leading = None;
            close_run(&mut run, &mut runs);
        }
    }
    close_run(&mut run, &mut runs);
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts<'a>(src: &'a str, list: &RustList) -> Vec<&'a str> {
        list.elements
            .iter()
            .map(|element| &src[element.span.clone()])
            .collect()
    }

    #[test]
    fn an_array_lists_its_elements_with_their_comments() {
        let src = "fn f() {\n    pin(&[\n        \"a\",\n        // why b\n        \"b\", // note\n    ]);\n}\n";
        let lists = rust_lists(src).unwrap();
        let array = lists
            .iter()
            .find(|list| list.kind == ListKind::Array)
            .unwrap();
        assert_eq!(texts(src, array), vec!["\"a\"", "// why b\n        \"b\""]);
        assert_eq!(&src[array.elements[1].body.clone()], "\"b\"");
        assert_eq!(
            array.elements[1].trailing.clone().map(|range| &src[range]),
            Some("// note")
        );
        assert!(array.trailing_comma);
        assert_eq!(&src[array.open.unwrap()..array.open.unwrap() + 1], "[");
    }

    #[test]
    fn a_macro_composer_is_a_comma_list_and_a_rules_body_is_not() {
        let src = "macro_rules! lineage {\n    () => {\n        &[\n            a!(),\n            // tail\n            b!(),\n        ]\n    };\n}\n";
        let lists = rust_lists(src).unwrap();
        let composer = lists
            .iter()
            .filter(|list| list.kind == ListKind::TokenTree)
            .find(|list| list.elements.len() == 2)
            .expect("the composer's bracketed token tree is a list");
        assert_eq!(
            texts(src, composer),
            vec!["a!()", "// tail\n            b!()"]
        );
        // The rules body `{ () => { … }; }` holds a `;`, so it is not a list.
        assert!(lists
            .iter()
            .filter(|list| list.kind == ListKind::TokenTree)
            .all(|list| !src[list.content.clone()].contains("=>")));
    }

    #[test]
    fn declaration_runs_attach_comments_and_end_at_any_other_item() {
        let src = "//! crate docs\n\n/// first\n#[cfg(test)]\nmod a;\nmod b; // trailing\nuse c::d;\n\n// a section header\nfn c() {}\nmod inline { fn x() {} }\nmod e;\n";
        let lists = rust_lists(src).unwrap();
        let runs: Vec<Vec<&str>> = lists
            .iter()
            .filter(|list| list.kind == ListKind::Items)
            .map(|list| texts(src, list))
            .collect();
        assert_eq!(
            runs,
            vec![
                vec![
                    "/// first\n#[cfg(test)]\nmod a;",
                    "mod b; // trailing",
                    "use c::d;"
                ],
                vec!["mod e;"],
            ]
        );
        assert!(lists
            .iter()
            .filter(|list| list.kind == ListKind::Items)
            .all(|list| list.open.is_none()));
    }

    #[test]
    fn functions_and_impl_bodies_are_never_lists() {
        let src = "fn a() {}\nfn b() {}\nimpl S {\n    fn c() {}\n    fn d() {}\n}\n";
        assert!(rust_lists(src)
            .unwrap()
            .iter()
            .all(|list| list.kind != ListKind::Items));
    }

    #[test]
    fn a_repeat_array_and_an_unparseable_file_are_not_lists() {
        let lists = rust_lists("fn f() { let _ = [0u8; 4]; }\n").unwrap();
        assert!(lists.iter().all(|list| list.kind != ListKind::Array));
        assert_eq!(rust_lists("fn f() { [1, 2]] }\n"), None);
    }

    #[test]
    fn a_use_list_and_a_module_body_run_are_lists() {
        let src = "use a::{b, c};\nmod m {\n    use x;\n\n    use y;\n\n    fn f() {}\n}\n";
        let lists = rust_lists(src).unwrap();
        let uses = lists
            .iter()
            .find(|list| list.kind == ListKind::UseList)
            .unwrap();
        assert_eq!(texts(src, uses), vec!["b", "c"]);
        assert!(!uses.trailing_comma);
        let body = lists
            .iter()
            .find(|list| {
                list.kind == ListKind::Items && src[list.content.clone()].contains("use x")
            })
            .unwrap();
        assert_eq!(texts(src, body), vec!["use x;", "use y;"]);
    }
}
