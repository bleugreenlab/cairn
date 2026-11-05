//! The checked-in list of gating findings the database accepts for now.
//!
//! Compiled in, so production and the Rust suite read one list: a finding it
//! names logs at INFO and names its issue, which is what lets a day of
//! production logs say "no WARN outside what the allowlist owes". The runner
//! parses the same file on its own to judge a whole test run
//! (`scripts/lib/plan-audit.ts`), and the module's tests hold the two to one
//! shape.

use std::sync::OnceLock;

use serde::Deserialize;

use super::{Rule, LOG_TARGET};

const CHECKED_IN: &str = include_str!("../../../plan-audit-allowlist.toml");

/// What an entry's issue is to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AllowKind {
    /// The issue owes this entry's removal.
    Debt,
    /// The issue records the ruling: a bulk scan by design, or a statement only
    /// test code defines.
    Deliberate,
}

/// One accepted statement. Keyed on its hash rather than its file alone, so an
/// entry cannot mask a later statement at the same call site's file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowEntry {
    /// `Location::file()` verbatim, so no `src-tauri/` prefix.
    pub file: String,
    pub rule: String,
    pub table: String,
    /// The audit's 16-hex statement hash.
    pub statement: String,
    pub kind: AllowKind,
    pub issue: String,
    pub reason: String,
}

impl AllowEntry {
    fn names(&self, file: &str, rule: Rule, table: &str, statement: &str) -> bool {
        self.statement == statement
            && self.file == file
            && self.table == table
            && self.rule == rule.as_str()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    #[serde(default)]
    allow: Vec<AllowEntry>,
}

pub fn parse_allowlist(text: &str) -> Result<Vec<AllowEntry>, String> {
    toml::from_str::<Document>(text)
        .map(|document| document.allow)
        .map_err(|error| error.to_string())
}

/// The compiled-in list. One that will not parse logs one error and reads as
/// empty, so every gating finding stays a WARN rather than going quiet.
pub fn allowlist() -> &'static [AllowEntry] {
    static LIST: OnceLock<Vec<AllowEntry>> = OnceLock::new();
    LIST.get_or_init(|| {
        parse_allowlist(CHECKED_IN).unwrap_or_else(|error| {
            log::error!(
                target: LOG_TARGET,
                "plan audit: plan-audit-allowlist.toml does not parse, so every gating finding \
                 stays a WARN: {error}"
            );
            Vec::new()
        })
    })
}

/// The entry that accepts this finding, if one does.
pub fn entry_for<'a>(
    entries: &'a [AllowEntry],
    file: &str,
    rule: Rule,
    table: &str,
    statement: &str,
) -> Option<&'a AllowEntry> {
    entries
        .iter()
        .find(|entry| entry.names(file, rule, table, statement))
}
