//! Import-graph extraction: the per-file dependency edges of a worktree.
//!
//! [`nav`](super::nav) answers "where is this name"; this module answers "which
//! file does this file depend on". Both parse with the same bundled grammars and
//! accept the same trade -- structural, not semantic. An edge is recovered in two
//! steps that are deliberately kept apart:
//!
//! 1. [`specifiers`] lifts the raw specifier text out of one file's syntax tree
//!    (`use crate::a::b`, `mod x;`, `import ... from "./y"`). Pure, per-file, and
//!    language-shaped.
//! 2. [`Layout`] resolves a specifier to a canonical worktree-relative path using
//!    the project's own layout -- Cargo crate roots, tsconfig `paths`/`baseUrl`,
//!    and workspace package names -- discovered once per walk.
//!
//! Only edges whose target is a walked source file are emitted, so `use std::..`
//! and a `node_modules` import contribute nothing. That is the point: the graph
//! describes this repository, not its dependency closure.
//!
//! Rust and TypeScript/JavaScript are implemented. Another grammar is a new arm
//! in [`specifiers`] plus a resolution rule; nothing else here is language-aware.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use ast_grep_language::SupportLang;
use ignore::WalkBuilder;

use super::engine::parse;
use super::walk::relative;

/// Extensions a bare TS/JS specifier is probed with, in resolution order.
const MODULE_SUFFIXES: &[&str] = &[
    ".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs", ".d.ts",
];

/// Rust path roots that name the language or the test harness, not this worktree.
const RUST_EXTERNAL_ROOTS: &[&str] = &["std", "core", "alloc", "proc_macro", "test"];

/// One import specifier lifted from a syntax tree, before resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Specifier {
    /// The specifier as written, normalized to a single path (`crate::a::b`,
    /// `./x`). A braced Rust use tree expands to one `Specifier` per leaf.
    pub text: String,
    pub kind: SpecifierKind,
}

/// What syntax produced a specifier, which decides how it resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecifierKind {
    /// A Rust `use` path: `crate::a::b`, `super::x`, `other_crate::y`.
    RustUse,
    /// A Rust `mod x;` declaration with no inline body. It names a sibling
    /// module *file*, which no `use` in this file need mention.
    RustMod,
    /// A JS/TS module specifier: `./x`, `@/lib/y`, `@cairn/ui`.
    Module,
}

/// Lift every import specifier out of one source file.
///
/// A language without an arm returns empty rather than erroring: a worktree is a
/// mix of grammars, and an unmapped one simply contributes no edges.
pub fn specifiers(src: &str, lang: SupportLang) -> Vec<Specifier> {
    match lang {
        SupportLang::Rust => rust_specifiers(src),
        SupportLang::TypeScript | SupportLang::Tsx | SupportLang::JavaScript => {
            module_specifiers(src, lang)
        }
        _ => Vec::new(),
    }
}

fn rust_specifiers(src: &str) -> Vec<Specifier> {
    let ast = parse(src, SupportLang::Rust);
    let mut out = Vec::new();
    for node in ast.root().dfs() {
        match node.kind().as_ref() {
            "use_declaration" => {
                let Some(argument) = node.field("argument") else {
                    continue;
                };
                for path in expand_use_tree(argument.text().as_ref()) {
                    if !path.is_empty() {
                        out.push(Specifier {
                            text: path,
                            kind: SpecifierKind::RustUse,
                        });
                    }
                }
            }
            // `mod x { .. }` is an inline module: no file, no edge. Only the
            // bodyless declaration points at another file.
            "mod_item" if node.field("body").is_none() => {
                if let Some(name) = node.field("name") {
                    out.push(Specifier {
                        text: name.text().to_string(),
                        kind: SpecifierKind::RustMod,
                    });
                }
            }
            _ => {}
        }
    }
    out
}

fn module_specifiers(src: &str, lang: SupportLang) -> Vec<Specifier> {
    let ast = parse(src, lang);
    let mut out = Vec::new();
    for node in ast.root().dfs() {
        let raw = match node.kind().as_ref() {
            // `import x from "m"`, `import "m"`, and `export * from "m"` all
            // carry the module in the same `source` field.
            "import_statement" | "export_statement" => {
                node.field("source").map(|source| source.text().to_string())
            }
            // `import("m")` and `require("m")`: the callee is a bare `import`
            // node or the `require` identifier, and the module is the first
            // string argument.
            "call_expression" => {
                let callee = node.field("function");
                let dynamic = callee.as_ref().is_some_and(|callee| {
                    callee.kind().as_ref() == "import" || callee.text().as_ref() == "require"
                });
                if dynamic {
                    node.field("arguments")
                        .and_then(|args| {
                            args.children()
                                .find(|arg| is_string_node(arg.kind().as_ref()))
                        })
                        .map(|arg| arg.text().to_string())
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(text) = raw.as_deref().and_then(unquote) {
            out.push(Specifier {
                text: text.to_string(),
                kind: SpecifierKind::Module,
            });
        }
    }
    out
}

fn is_string_node(kind: &str) -> bool {
    kind == "string" || kind == "template_string"
}

/// Strip the surrounding quotes from a string literal. A template literal with
/// an interpolation is not a static specifier and is dropped.
fn unquote(text: &str) -> Option<&str> {
    let text = text.trim();
    let quote = text.chars().next()?;
    if !matches!(quote, '"' | '\'' | '`') {
        return None;
    }
    let width = quote.len_utf8();
    let inner = text.get(width..text.len().checked_sub(width)?)?;
    if inner.is_empty() || inner.contains("${") {
        return None;
    }
    Some(inner)
}

/// Expand a Rust use tree into one path per leaf.
///
/// `a::{b, c::{d, self}}` becomes `a::b`, `a::c::d`, `a::c`. Glob and alias
/// tails carry no extra module: `a::*` is `a`, and `a::b as c` is `a::b`.
fn expand_use_tree(text: &str) -> Vec<String> {
    let text = text.trim().trim_end_matches(';').trim();
    let Some(open) = text.find('{') else {
        return vec![normalize_use_path(text)];
    };
    let prefix = text[..open].trim().trim_end_matches(':').trim();
    let Some(close) = matching_brace(text, open) else {
        return vec![normalize_use_path(prefix)];
    };
    let mut out = Vec::new();
    for item in split_top_level(&text[open + 1..close]) {
        for leaf in expand_use_tree(item) {
            out.push(join_use(prefix, &leaf));
        }
    }
    out
}

/// Drop the alias tail and any glob segment, leaving the module path itself.
fn normalize_use_path(text: &str) -> String {
    let text = text.trim();
    let text = text.split(" as ").next().unwrap_or(text).trim();
    text.split("::")
        .map(str::trim)
        .filter(|segment| !segment.is_empty() && *segment != "*")
        .collect::<Vec<_>>()
        .join("::")
}

fn join_use(prefix: &str, leaf: &str) -> String {
    // A `self` leaf inside a brace group names the prefix module itself.
    if leaf.is_empty() || leaf == "self" {
        prefix.to_string()
    } else if prefix.is_empty() {
        leaf.to_string()
    } else {
        format!("{prefix}::{leaf}")
    }
}

fn matching_brace(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, ch) in text.char_indices() {
        if index < open {
            continue;
        }
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(inner: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, ch) in inner.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(&inner[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    out.push(&inner[start..]);
    out.into_iter()
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .collect()
}

/// The worktree-relative path of `path`, with forward slashes on every platform
/// so a stored edge reads the same everywhere.
fn rel_string(root: &Path, path: &Path) -> String {
    relative(root, path).to_string_lossy().replace('\\', "/")
}

/// The directory containing `path`, or `None` when `path` has no parent inside
/// the worktree. A top-level file yields the empty string (the worktree root).
fn parent_dir(path: &str) -> Option<String> {
    match path.rfind('/') {
        Some(index) => Some(path[..index].to_string()),
        None => (!path.is_empty()).then(String::new),
    }
}

fn join_rel(dir: &str, rest: &str) -> String {
    match (dir.is_empty(), rest.is_empty()) {
        (true, _) => rest.to_string(),
        (false, true) => dir.to_string(),
        (false, false) => format!("{dir}/{rest}"),
    }
}

/// Join `spec` onto `dir` and collapse `.` / `..`. Returns `None` when the
/// specifier climbs past the worktree root.
fn normalize_rel(dir: &str, spec: &str) -> Option<String> {
    let mut parts: Vec<&str> = if dir.is_empty() {
        Vec::new()
    } else {
        dir.split('/').collect()
    };
    for segment in spec.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    Some(parts.join("/"))
}

/// The project layout a specifier resolves against: which files exist, where
/// each Cargo crate roots, and which module aliases the TypeScript configs and
/// workspace packages declare. Discovered once per walk, reused for every file.
#[derive(Debug, Default)]
pub struct Layout {
    files: HashSet<String>,
    crates: Vec<RustCrate>,
    /// Alias scopes, most specific directory first, so a nested `tsconfig.json`
    /// wins over the root one for the files it governs.
    scopes: Vec<AliasScope>,
}

#[derive(Debug)]
struct RustCrate {
    /// Underscored crate name -- the form a `use` path spells, regardless of the
    /// hyphens the Cargo package name uses.
    name: String,
    /// Worktree-relative crate source root (the directory holding `lib.rs`).
    src_dir: String,
}

#[derive(Debug)]
struct AliasScope {
    /// Worktree-relative directory this scope governs; empty means the whole
    /// worktree (workspace package names).
    dir: String,
    /// `compilerOptions.baseUrl`, worktree-relative, when the config sets one.
    base_url: Option<String>,
    /// Aliases, longest prefix first.
    aliases: Vec<Alias>,
}

impl AliasScope {
    fn governs(&self, path: &str) -> bool {
        self.dir.is_empty() || path.starts_with(&format!("{}/", self.dir))
    }
}

#[derive(Debug)]
struct Alias {
    /// The pattern with any trailing `/*` removed.
    prefix: String,
    /// Whether the pattern ended in `*` and therefore also covers subpaths.
    wildcard: bool,
    /// Worktree-relative substitution roots, in declaration order.
    targets: Vec<String>,
}

impl Alias {
    /// The paths `spec` could name under this alias, most likely first.
    fn candidates(&self, spec: &str) -> Vec<String> {
        let rest = if spec == self.prefix {
            ""
        } else if self.wildcard {
            match spec
                .strip_prefix(&self.prefix)
                .and_then(|rest| rest.strip_prefix('/'))
            {
                Some(rest) => rest,
                None => return Vec::new(),
            }
        } else {
            return Vec::new();
        };
        self.targets
            .iter()
            .map(|target| join_rel(target, rest))
            .collect()
    }
}

impl Layout {
    /// Discover the layout of `root` given the walked source files.
    ///
    /// The config walk is separate from the source walk because `Cargo.toml`,
    /// `tsconfig.json`, and `package.json` are layout facts rather than graph
    /// nodes -- they are never themselves edges. Both walks honor gitignore, so
    /// `node_modules` and `target` cost nothing.
    pub fn discover(root: &Path, files: &[(PathBuf, SupportLang)]) -> Self {
        let file_set: HashSet<String> = files
            .iter()
            .map(|(path, _)| rel_string(root, path))
            .collect();
        let mut crates: Vec<RustCrate> = Vec::new();
        let mut scopes: Vec<AliasScope> = Vec::new();
        let mut package_aliases: Vec<Alias> = Vec::new();

        for entry in WalkBuilder::new(root).hidden(false).build().flatten() {
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !matches!(name, "Cargo.toml" | "tsconfig.json" | "package.json") {
                continue;
            }
            let Some(dir) = parent_dir(&rel_string(root, path)) else {
                continue;
            };
            let Ok(text) = std::fs::read_to_string(path) else {
                continue;
            };
            match name {
                "Cargo.toml" => {
                    if let Some(crate_name) = cargo_package_name(&text) {
                        crates.push(RustCrate {
                            name: crate_name,
                            src_dir: join_rel(&dir, "src"),
                        });
                    }
                }
                "tsconfig.json" => {
                    if let Some(scope) = tsconfig_scope(&dir, &text) {
                        scopes.push(scope);
                    }
                }
                // The root package.json names the repository, not an importable
                // module; only nested workspace packages become aliases.
                "package.json" if !dir.is_empty() => {
                    if let Some(package) = json_string_field(&text, "name") {
                        package_aliases.push(Alias {
                            prefix: package,
                            wildcard: true,
                            targets: vec![join_rel(&dir, "src"), dir.clone()],
                        });
                    }
                }
                _ => {}
            }
        }

        // Longest crate source root first: a workspace member nested inside
        // another crate's directory must claim its own files.
        crates.sort_by(|a, b| b.src_dir.len().cmp(&a.src_dir.len()));
        for scope in &mut scopes {
            scope
                .aliases
                .sort_by(|a, b| b.prefix.len().cmp(&a.prefix.len()));
        }
        scopes.sort_by(|a, b| b.dir.len().cmp(&a.dir.len()));
        if !package_aliases.is_empty() {
            package_aliases.sort_by(|a, b| b.prefix.len().cmp(&a.prefix.len()));
            scopes.push(AliasScope {
                dir: String::new(),
                base_url: None,
                aliases: package_aliases,
            });
        }
        Self {
            files: file_set,
            crates,
            scopes,
        }
    }
}

/// The `[package] name` of a Cargo manifest, underscored to the form `use`
/// spells. A `name.workspace = true` inheritance line carries no literal name
/// and is skipped.
fn cargo_package_name(text: &str) -> Option<String> {
    let mut in_package = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        let Some(rest) = line.strip_prefix("name") else {
            continue;
        };
        let Some(value) = rest.trim_start().strip_prefix('=') else {
            continue;
        };
        let value = value.trim().trim_matches('"');
        if !value.is_empty() {
            return Some(value.replace('-', "_"));
        }
    }
    None
}

/// The alias scope one `tsconfig.json` declares. Returns `None` when the config
/// carries neither `paths` nor `baseUrl` -- there is nothing to resolve with.
fn tsconfig_scope(dir: &str, text: &str) -> Option<AliasScope> {
    let value: serde_json::Value = serde_json::from_str(&strip_json_comments(text)).ok()?;
    let options = value.get("compilerOptions")?;
    let base_url = options
        .get("baseUrl")
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| normalize_rel(dir, raw));
    // `paths` targets are relative to `baseUrl` when one is set, and to the
    // config's own directory otherwise.
    let alias_root = base_url.clone().unwrap_or_else(|| dir.to_string());
    let mut aliases = Vec::new();
    if let Some(paths) = options.get("paths").and_then(serde_json::Value::as_object) {
        for (pattern, targets) in paths {
            let (prefix, wildcard) = split_pattern(pattern);
            let targets: Vec<String> = targets
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|target| normalize_rel(&alias_root, &split_pattern(target).0))
                .collect();
            if !targets.is_empty() {
                aliases.push(Alias {
                    prefix,
                    wildcard,
                    targets,
                });
            }
        }
    }
    (!aliases.is_empty() || base_url.is_some()).then(|| AliasScope {
        dir: dir.to_string(),
        base_url,
        aliases,
    })
}

/// Split a tsconfig path pattern into its literal head and whether it wildcards.
fn split_pattern(pattern: &str) -> (String, bool) {
    match pattern.strip_suffix('*') {
        Some(head) => (head.trim_end_matches('/').to_string(), true),
        None => (pattern.to_string(), false),
    }
}

fn json_string_field(text: &str, key: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(&strip_json_comments(text)).ok()?;
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// `tsconfig.json` is JSON with comments and trailing commas in practice, which
/// `serde_json` rejects. Strip both so a commented config still yields aliases
/// instead of silently resolving nothing.
fn strip_json_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(ch) = chars.next() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '/' if chars.peek() == Some(&'/') => {
                for next in chars.by_ref() {
                    if next == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut previous = '\0';
                for next in chars.by_ref() {
                    if previous == '*' && next == '/' {
                        break;
                    }
                    previous = next;
                }
            }
            _ => out.push(ch),
        }
    }
    strip_trailing_commas(&out)
}

fn strip_trailing_commas(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut dropped = vec![false; chars.len()];
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in chars.iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            ',' => {
                if chars[index + 1..]
                    .iter()
                    .find(|next| !next.is_whitespace())
                    .is_some_and(|next| matches!(next, '}' | ']'))
                {
                    dropped[index] = true;
                }
            }
            _ => {}
        }
    }
    chars
        .iter()
        .enumerate()
        .filter(|(index, _)| !dropped[*index])
        .map(|(_, ch)| *ch)
        .collect()
}

impl Layout {
    /// Resolve one specifier named by `from` (worktree-relative) to another
    /// worktree-relative source file, or `None` when it leaves this worktree.
    /// A file importing itself is not an edge.
    pub fn resolve(&self, from: &str, specifier: &Specifier) -> Option<String> {
        let resolved = match specifier.kind {
            SpecifierKind::RustUse => self.resolve_rust_use(from, &specifier.text),
            SpecifierKind::RustMod => self.resolve_rust_mod(from, &specifier.text),
            SpecifierKind::Module => self.resolve_module(from, &specifier.text),
        }?;
        (resolved != from).then_some(resolved)
    }

    /// The crate whose source root contains `path`, longest root first.
    fn crate_for(&self, path: &str) -> Option<&RustCrate> {
        self.crates
            .iter()
            .find(|krate| path.starts_with(&format!("{}/", krate.src_dir)))
    }

    /// The directory holding a Rust file's child modules. `lib.rs`, `main.rs`,
    /// and `mod.rs` own their own directory; every other file owns the
    /// directory named after its stem.
    fn rust_module_dir(path: &str) -> Option<String> {
        let parent = parent_dir(path)?;
        let stem = Path::new(path).file_stem()?.to_str()?;
        if matches!(stem, "mod" | "lib" | "main") {
            Some(parent)
        } else {
            Some(join_rel(&parent, stem))
        }
    }

    fn resolve_rust_use(&self, from: &str, path: &str) -> Option<String> {
        let krate = self.crate_for(from)?;
        let segments: Vec<&str> = path
            .split("::")
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .collect();
        let first = *segments.first()?;
        let (base, consumed) = match first {
            "crate" => (krate.src_dir.clone(), 1),
            "self" => (Self::rust_module_dir(from)?, 1),
            "super" => {
                let mut dir = Self::rust_module_dir(from)?;
                let mut consumed = 0;
                while segments.get(consumed).copied() == Some("super") {
                    dir = parent_dir(&dir)?;
                    // `super` never climbs out of its own crate.
                    if dir != krate.src_dir && !dir.starts_with(&format!("{}/", krate.src_dir)) {
                        return None;
                    }
                    consumed += 1;
                }
                (dir, consumed)
            }
            _ if RUST_EXTERNAL_ROOTS.contains(&first) => return None,
            name => {
                let normalized = name.replace('-', "_");
                let target = self.crates.iter().find(|krate| krate.name == normalized)?;
                (target.src_dir.clone(), 1)
            }
        };
        self.resolve_rust_module(&base, &segments[consumed..])
    }

    fn resolve_rust_mod(&self, from: &str, name: &str) -> Option<String> {
        let dir = Self::rust_module_dir(from)?;
        self.rust_module_file(&join_rel(&dir, name))
    }

    /// Walk the module path from longest to shortest: `crate::a::b::Type`
    /// resolves to `a/b.rs` once `a/b/Type.rs` fails, because the tail segments
    /// of a `use` name items inside a module, not deeper modules.
    fn resolve_rust_module(&self, base: &str, rest: &[&str]) -> Option<String> {
        for take in (0..=rest.len()).rev() {
            let mut candidate = base.to_string();
            for segment in &rest[..take] {
                candidate = join_rel(&candidate, segment);
            }
            if take == 0 {
                for entry in ["lib.rs", "main.rs", "mod.rs"] {
                    let file = join_rel(base, entry);
                    if self.files.contains(&file) {
                        return Some(file);
                    }
                }
                continue;
            }
            if let Some(hit) = self.rust_module_file(&candidate) {
                return Some(hit);
            }
        }
        None
    }

    fn rust_module_file(&self, candidate: &str) -> Option<String> {
        for file in [format!("{candidate}.rs"), format!("{candidate}/mod.rs")] {
            if self.files.contains(&file) {
                return Some(file);
            }
        }
        None
    }

    fn resolve_module(&self, from: &str, spec: &str) -> Option<String> {
        if spec.starts_with('.') {
            let dir = parent_dir(from)?;
            return self.resolve_module_file(&normalize_rel(&dir, spec)?);
        }
        for scope in &self.scopes {
            if !scope.governs(from) {
                continue;
            }
            for alias in &scope.aliases {
                for candidate in alias.candidates(spec) {
                    if let Some(hit) = self.resolve_module_file(&candidate) {
                        return Some(hit);
                    }
                }
            }
            if let Some(base_url) = &scope.base_url {
                if let Some(candidate) = normalize_rel(base_url, spec) {
                    if let Some(hit) = self.resolve_module_file(&candidate) {
                        return Some(hit);
                    }
                }
            }
        }
        None
    }

    fn resolve_module_file(&self, base: &str) -> Option<String> {
        if self.files.contains(base) {
            return Some(base.to_string());
        }
        // Under NodeNext a TypeScript file imports its sibling by the extension
        // the emit will have, so `./x.js` addresses `x.ts` on disk.
        for (emitted, authored) in [
            (".js", ".ts"),
            (".jsx", ".tsx"),
            (".mjs", ".mts"),
            (".cjs", ".cts"),
        ] {
            if let Some(stem) = base.strip_suffix(emitted) {
                let authored = format!("{stem}{authored}");
                if self.files.contains(&authored) {
                    return Some(authored);
                }
            }
        }
        for suffix in MODULE_SUFFIXES {
            let candidate = format!("{base}{suffix}");
            if self.files.contains(&candidate) {
                return Some(candidate);
            }
        }
        for suffix in MODULE_SUFFIXES {
            let candidate = format!("{base}/index{suffix}");
            if self.files.contains(&candidate) {
                return Some(candidate);
            }
        }
        None
    }
}

/// One walked source file, measured by the same read its parse used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFile {
    /// Worktree-relative path with forward slashes.
    pub path: String,
    /// The bundled grammar that claimed this extension.
    pub language: SupportLang,
    /// Bytes of the source this walk read. Taken from the read itself rather
    /// than a separate `stat`, which would double the syscalls a whole-tree
    /// walk makes for a number the read already knows.
    pub size_bytes: u64,
    /// Newline-terminated lines. A file that is not valid UTF-8 counts zero
    /// lines and zero bytes rather than being dropped -- it still occupies the
    /// tree, and its shape is simply not something a text parse can report.
    pub line_count: u32,
}

/// A worktree's source inventory together with the import graph over it.
#[derive(Debug, Default)]
pub struct ImportGraph {
    pub files: Vec<SourceFile>,
    /// `(from, to)` worktree-relative pairs, deduplicated and sorted so the
    /// payload is byte-stable for the same tree.
    pub edges: Vec<(String, String)>,
}

/// Walk `root` for source files with a bundled grammar and return both the
/// inventory and its import graph, reading each file exactly once.
///
/// This is the whole surface a code-map producer needs: the walk honors
/// gitignore, the measurements come from the read the parse already performed,
/// and the edges are resolved against the project's own layout.
pub fn import_graph(root: &Path) -> ImportGraph {
    let walked = super::walk::source_files(root, root, None);
    let (files, edges) = collect(root, &walked);
    ImportGraph { files, edges }
}

/// Every import edge among `files`, as `(from, to)` worktree-relative pairs.
///
/// `files` is a walked source set (see [`super::walk`]); a file that cannot be
/// read as UTF-8 contributes no edges rather than failing the whole graph.
pub fn edges(root: &Path, files: &[(PathBuf, SupportLang)]) -> Vec<(String, String)> {
    collect(root, files).1
}

fn collect(
    root: &Path,
    files: &[(PathBuf, SupportLang)],
) -> (Vec<SourceFile>, Vec<(String, String)>) {
    let layout = Layout::discover(root, files);
    let mut inventory = Vec::with_capacity(files.len());
    let mut graph = BTreeSet::new();
    for (path, language) in files {
        let from = rel_string(root, path);
        let source = std::fs::read_to_string(path).ok();
        inventory.push(SourceFile {
            path: from.clone(),
            language: *language,
            size_bytes: source.as_ref().map(|src| src.len() as u64).unwrap_or(0),
            line_count: source
                .as_deref()
                .map(|src| src.lines().count() as u32)
                .unwrap_or(0),
        });
        let Some(source) = source else {
            continue;
        };
        for specifier in specifiers(&source, *language) {
            if let Some(to) = layout.resolve(&from, &specifier) {
                graph.insert((from.clone(), to));
            }
        }
    }
    (inventory, graph.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbols::walk::source_files;

    fn write(root: &Path, rel: &str, contents: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    fn graph(root: &Path) -> Vec<(String, String)> {
        edges(root, &source_files(root, root, None))
    }

    fn targets<'a>(graph: &'a [(String, String)], from: &str) -> Vec<&'a str> {
        graph
            .iter()
            .filter(|(source, _)| source == from)
            .map(|(_, target)| target.as_str())
            .collect()
    }

    #[test]
    fn use_tree_expands_to_one_path_per_leaf() {
        assert_eq!(
            expand_use_tree("crate::a::{b, c::{d, self}}"),
            vec!["crate::a::b", "crate::a::c::d", "crate::a::c"]
        );
        assert_eq!(expand_use_tree("crate::a::*"), vec!["crate::a"]);
        assert_eq!(expand_use_tree("crate::a::B as C"), vec!["crate::a::B"]);
        assert_eq!(
            expand_use_tree("std::fmt::Display"),
            vec!["std::fmt::Display"]
        );
    }

    #[test]
    fn rust_extraction_separates_use_paths_from_module_files() {
        let src = concat!(
            "use crate::parser::Ast;\n",
            "use std::fmt;\n",
            "pub mod parser;\n",
            "mod inline { pub fn noop() {} }\n",
        );
        let found = specifiers(src, SupportLang::Rust);
        assert_eq!(
            found,
            vec![
                Specifier {
                    text: "crate::parser::Ast".into(),
                    kind: SpecifierKind::RustUse
                },
                Specifier {
                    text: "std::fmt".into(),
                    kind: SpecifierKind::RustUse
                },
                Specifier {
                    text: "parser".into(),
                    kind: SpecifierKind::RustMod
                },
            ],
            "an inline `mod` body names no file and must not become a specifier"
        );
    }

    #[test]
    fn typescript_extraction_covers_import_export_and_dynamic_forms() {
        let src = concat!(
            "import { a } from './a';\n",
            "import type { B } from '../b';\n",
            "export * from './c';\n",
            "import './side-effect';\n",
            "const late = await import('./late');\n",
            "const cjs = require('./cjs');\n",
            "const dynamic = await import(`./${name}`);\n",
        );
        let found: Vec<String> = specifiers(src, SupportLang::TypeScript)
            .into_iter()
            .map(|specifier| specifier.text)
            .collect();
        assert_eq!(
            found,
            vec!["./a", "../b", "./c", "./side-effect", "./late", "./cjs"],
            "an interpolated specifier is not statically resolvable and is dropped"
        );
    }

    #[test]
    fn rust_edges_resolve_modules_crates_and_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "crates/alpha/Cargo.toml",
            "[package]\nname = \"alpha-core\"\n",
        );
        write(
            root,
            "crates/beta/Cargo.toml",
            "[package]\nname = \"beta\"\n",
        );
        write(
            root,
            "crates/alpha/src/lib.rs",
            "pub mod parser;\npub mod engine;\nuse beta::helper::run;\nuse std::fmt::Display;\n",
        );
        write(
            root,
            "crates/alpha/src/parser.rs",
            "use crate::engine::Machine;\nuse super::parser::Ast;\npub struct Ast;\n",
        );
        write(
            root,
            "crates/alpha/src/engine/mod.rs",
            "pub struct Machine;\n",
        );
        write(root, "crates/beta/src/lib.rs", "pub mod helper;\n");
        write(root, "crates/beta/src/helper.rs", "pub fn run() {}\n");

        let graph = graph(root);
        assert_eq!(
            targets(&graph, "crates/alpha/src/lib.rs"),
            vec![
                "crates/alpha/src/engine/mod.rs",
                "crates/alpha/src/parser.rs",
                "crates/beta/src/helper.rs",
            ],
            "`mod` declarations reach both file and directory modules; a \
             cross-crate `use` reaches the other crate through its hyphen-free \
             name; `std` leaves the worktree and is not an edge"
        );
        assert_eq!(
            targets(&graph, "crates/alpha/src/parser.rs"),
            vec!["crates/alpha/src/engine/mod.rs"],
            "`crate::` roots at the crate source dir; `super::parser` resolves \
             back to this same file and a self-edge is not a dependency"
        );
    }

    #[test]
    fn rust_use_falls_back_to_the_module_that_declares_the_item() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "Cargo.toml", "[package]\nname = \"solo\"\n");
        write(root, "src/lib.rs", "pub mod a;\n");
        write(root, "src/a/mod.rs", "pub mod b;\n");
        write(root, "src/a/b.rs", "pub struct Deep;\n");
        write(root, "src/user.rs", "use crate::a::b::Deep;\n");

        assert_eq!(
            targets(&graph(root), "src/user.rs"),
            vec!["src/a/b.rs"],
            "the trailing `Deep` names an item inside `a::b`, not a deeper module"
        );
    }

    #[test]
    fn typescript_edges_resolve_relative_index_alias_and_workspace_packages() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "tsconfig.json",
            "{\n  // aliases\n  \"compilerOptions\": {\n    \"paths\": {\n      \"@/*\": [\"./src/*\"],\n    },\n  },\n}\n",
        );
        write(
            root,
            "package.json",
            "{ \"name\": \"root\", \"workspaces\": [\"packages/*\"] }\n",
        );
        write(
            root,
            "packages/ui/package.json",
            "{ \"name\": \"@demo/ui\" }\n",
        );
        write(
            root,
            "packages/ui/src/index.ts",
            "export const Button = 1;\n",
        );
        write(root, "packages/ui/src/theme.ts", "export const dark = 1;\n");
        write(root, "src/util.ts", "export const util = 1;\n");
        write(root, "src/widgets/index.tsx", "export const Widget = 1;\n");
        write(root, "src/lib/tokens.ts", "export const tokens = 1;\n");
        write(
            root,
            "src/app.tsx",
            concat!(
                "import { util } from './util.js';\n",
                "import { Widget } from './widgets';\n",
                "import { tokens } from '@/lib/tokens';\n",
                "import { Button } from '@demo/ui';\n",
                "import { dark } from '@demo/ui/theme';\n",
                "import React from 'react';\n",
            ),
        );

        assert_eq!(
            targets(&graph(root), "src/app.tsx"),
            vec![
                "packages/ui/src/index.ts",
                "packages/ui/src/theme.ts",
                "src/lib/tokens.ts",
                "src/util.ts",
                "src/widgets/index.tsx",
            ],
            "a `.js` specifier addresses the authored `.ts`, a directory resolves \
             through its index, a tsconfig alias and a workspace package name \
             both land in the worktree, and a node_modules import does not"
        );
    }

    #[test]
    fn a_commented_tsconfig_still_yields_its_aliases() {
        let scope = tsconfig_scope(
            "web",
            "{\n  /* block */\n  \"compilerOptions\": {\n    \"baseUrl\": \".\", // here\n    \"paths\": { \"~/*\": [\"./app/*\"] }\n  }\n}",
        )
        .expect("a commented config is the common case, not a parse failure");
        assert_eq!(scope.base_url.as_deref(), Some("web"));
        assert_eq!(scope.aliases[0].prefix, "~");
        assert!(scope.aliases[0].wildcard);
        assert_eq!(scope.aliases[0].targets, vec!["web/app".to_string()]);
    }
}
