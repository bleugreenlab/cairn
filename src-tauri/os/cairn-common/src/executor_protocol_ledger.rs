//! The executor protocol version, read from the paragraphs that describe it.
//!
//! Every executor protocol version is a published release, and the doc block
//! above `EXECUTOR_PROTOCOL_VERSION` is its ledger: one `/// Version N (cairn/X)
//! …` paragraph per version, beside `cairn/X does not move the number`
//! paragraphs for changes that argue they need none. The constant is derived
//! from that ledger, never authored: it is the highest bound heading in the
//! block, plus one when the block carries a `/// Version NEXT` heading. A
//! branch that changes the protocol writes `/// Version NEXT (cairn/N) …`, and
//! the store binds the number when the branch lands on the default branch,
//! rewriting that one heading to the tip's next version in the landed commit.
//! Two branches both writing NEXT land as consecutive versions in landing order.
//!
//! A heading is a top-level doc line (`///` at column 0) whose text begins
//! `Version N` or `Version NEXT`. Mid-sentence mentions, indented field docs,
//! and the ledger's own `Versions 101–111 …` record are not headings, and a
//! version's digits are read whole, so `Version 13` never matches inside
//! `Version 135`.
//!
//! This file is compiled twice: into the crate, for the landing binder and the
//! structural merge, and into `build.rs` by `#[path]`, which derives the
//! constant. It therefore depends on `std` alone.
//! `scripts/lib/executor-protocol-headings.ts` states the same grammar and the
//! same landing rule for the TypeScript side, and
//! `fixtures/executor-protocol-ledger-cases.json` holds the two to one table.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

/// The constant the ledger describes.
pub const ANCHOR: &str = "EXECUTOR_PROTOCOL_VERSION";

/// The word a heading's version follows.
const HEADING_WORD: &str = "Version ";

/// The provisional spelling of a version the landing has not bound yet.
pub const PROVISIONAL: &str = "NEXT";

/// What a heading declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HeadingVersion {
    Bound(u64),
    Next,
}

/// The version a heading line declares, and the byte range of its version
/// token (the digits, or `NEXT`).
pub fn heading(line: &str) -> Option<(HeadingVersion, Range<usize>)> {
    let doc = line.strip_prefix("///")?;
    let rest = doc
        .trim_start_matches([' ', '\t'])
        .strip_prefix(HEADING_WORD)?;
    let start = line.len() - rest.len();
    if let Some(after) = rest.strip_prefix(PROVISIONAL) {
        let boundary = after
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        return boundary.then(|| (HeadingVersion::Next, start..start + PROVISIONAL.len()));
    }
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let value = rest[..digits].parse().ok()?;
    Some((HeadingVersion::Bound(value), start..start + digits))
}

/// The lines of the doc block directly above `pub const {anchor}`, as indices
/// into `text.lines()`: the contiguous `///` lines ending at the declaration.
/// `None` when the file declares no such constant.
pub fn doc_block(text: &str, anchor: &str) -> Option<Range<usize>> {
    let lines: Vec<&str> = text.lines().collect();
    let declaration = lines.iter().position(|line| declares(line, anchor))?;
    let mut start = declaration;
    while start > 0 && lines[start - 1].trim_start().starts_with("///") {
        start -= 1;
    }
    Some(start..declaration)
}

fn declares(line: &str, anchor: &str) -> bool {
    line.trim_start()
        .strip_prefix("pub const ")
        .and_then(|rest| rest.strip_prefix(anchor))
        .is_some_and(|rest| rest.trim_start().starts_with(':'))
}

/// One heading of the anchor's doc block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeading<'a> {
    /// Index into `text.lines()`.
    pub line_index: usize,
    pub line: &'a str,
    pub version: HeadingVersion,
    /// The version token's byte range within `line`.
    pub token: Range<usize>,
}

/// Every heading in the anchor's doc block, in source order; `None` when the
/// file declares no such constant.
pub fn block_headings<'a>(text: &'a str, anchor: &str) -> Option<Vec<BlockHeading<'a>>> {
    let block = doc_block(text, anchor)?;
    Some(
        text.lines()
            .enumerate()
            .skip(block.start)
            .take(block.len())
            .filter_map(|(line_index, line)| {
                heading(line).map(|(version, token)| BlockHeading {
                    line_index,
                    line,
                    version,
                    token,
                })
            })
            .collect(),
    )
}

/// The paragraphs of the anchor's doc block, each as its lines joined by `\n`:
/// runs of `///` lines separated by blank `///` lines. `None` when the file
/// declares no such constant. The version-paragraphs structural merge settles
/// a conflict inside the block element-wise over these.
pub fn doc_paragraphs(text: &str, anchor: &str) -> Option<Vec<String>> {
    let block = doc_block(text, anchor)?;
    let mut paragraphs = Vec::new();
    let mut pending: Vec<&str> = Vec::new();
    for line in text.lines().skip(block.start).take(block.len()) {
        let blank = line
            .trim_start()
            .strip_prefix("///")
            .is_some_and(|rest| rest.trim().is_empty());
        if blank {
            if !pending.is_empty() {
                paragraphs.push(pending.join("\n"));
                pending.clear();
            }
        } else {
            pending.push(line);
        }
    }
    if !pending.is_empty() {
        paragraphs.push(pending.join("\n"));
    }
    Some(paragraphs)
}

/// The literal a `pub const {anchor}: u32 = N;` declaration carries, and its
/// byte range in `text`. `None` when the declaration is not a literal (the
/// derived form, `= derived::{anchor};`) or is absent.
///
/// While a literal is written, it must read what the ledger derives (the
/// build asserts it), and a landing that binds a heading moves it too.
pub fn literal(text: &str, anchor: &str) -> Option<(Range<usize>, u64)> {
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let start = offset;
        offset += line.len();
        if !declares(line, anchor) {
            continue;
        }
        let equals = line.find('=')?;
        let after = &line[equals + 1..];
        let value = after.trim_start();
        let digits = value.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || !value[digits..].trim_start().starts_with(';') {
            return None;
        }
        let at = start + equals + 1 + (after.len() - value.len());
        return Some((at..at + digits, value[..digits].parse().ok()?));
    }
    None
}

/// The version the ledger describes: the highest bound heading in the anchor's
/// doc block, plus one when the block carries a `Version NEXT` heading.
///
/// Refuses a file with no such constant, a block with no bound heading, and a
/// block with more than one NEXT heading: one change takes one version, and two
/// provisional headings would claim the same one.
pub fn derive_version(text: &str, anchor: &str) -> Result<u64, String> {
    let headings = block_headings(text, anchor)
        .ok_or_else(|| format!("no `pub const {anchor}` declaration to derive"))?;
    let highest = headings
        .iter()
        .filter_map(|heading| match heading.version {
            HeadingVersion::Bound(value) => Some(value),
            HeadingVersion::Next => None,
        })
        .max()
        .ok_or_else(|| {
            format!(
                "the doc block above `{anchor}` has no `/// Version N` heading to derive it from"
            )
        })?;
    match headings
        .iter()
        .filter(|heading| heading.version == HeadingVersion::Next)
        .count()
    {
        0 => Ok(highest),
        1 => Ok(highest + 1),
        many => Err(format!(
            "the doc block above `{anchor}` carries {many} `/// Version NEXT` headings; one \
             change takes one version, so fold them into one paragraph"
        )),
    }
}

/// The heading a landing must bind, and the number it takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadingBinding {
    /// Index into the branch text's `lines()`.
    pub line_index: usize,
    /// What the branch wrote: `NEXT`, or the number it chose by hand.
    pub authored: HeadingVersion,
    /// The destination's version plus one.
    pub bound: u64,
}

impl HeadingBinding {
    /// Whether binding changes the text at all: a hand-numbered heading that
    /// already reads the next version stays as written.
    pub fn rewrites(&self) -> bool {
        self.authored != HeadingVersion::Bound(self.bound)
    }
}

/// The heading lines of a block, by the version they declare.
fn lines_by_version<'a>(
    headings: &[BlockHeading<'a>],
) -> BTreeMap<HeadingVersion, BTreeSet<&'a str>> {
    let mut grouped: BTreeMap<HeadingVersion, BTreeSet<&str>> = BTreeMap::new();
    for heading in headings {
        grouped
            .entry(heading.version)
            .or_default()
            .insert(heading.line);
    }
    grouped
}

/// The landing rule: which heading, if any, the branch authored against the
/// destination it lands on, and the version that heading takes there.
///
/// `branch` must already contain `destination` (the landing replays it there
/// first), so every heading the destination carries is on the branch
/// byte-for-byte unless the branch reworded it. A heading is authored when the
/// branch carries a heading line the destination does not, beyond one
/// replacing each of the destination's lines it no longer carries under that
/// version (a reworded heading line mints nothing). A NEXT heading is always
/// authored. The authored heading, if there is one, takes the destination's
/// version plus one, whatever number the branch chose: a hand-numbered heading
/// that collided with one the destination landed first is rebound, exactly as
/// NEXT is bound.
///
/// Refused: a destination that carries NEXT (nothing unbound may reach the
/// default branch), more than one authored heading (one change takes one
/// version), and a version the branch both reworded and added to (which line
/// is new cannot be told apart). Either side failing to declare the constant
/// fails closed.
pub fn plan_binding(
    branch: &str,
    destination: &str,
    anchor: &str,
) -> Result<Option<HeadingBinding>, String> {
    let dest_headings = block_headings(destination, anchor)
        .ok_or_else(|| format!("the destination declares no `pub const {anchor}`"))?;
    let branch_headings = block_headings(branch, anchor)
        .ok_or_else(|| format!("the branch declares no `pub const {anchor}`"))?;
    if dest_headings
        .iter()
        .any(|heading| heading.version == HeadingVersion::Next)
    {
        return Err(format!(
            "the destination carries an unbound `/// Version NEXT` heading above `{anchor}`; only \
             a landing binds one, so the destination must be repaired first"
        ));
    }
    let destination_version = derive_version(destination, anchor)?;
    // Refuses two NEXT headings on the branch before the per-version count does.
    derive_version(branch, anchor)?;

    let dest_lines = lines_by_version(&dest_headings);
    let branch_lines = lines_by_version(&branch_headings);
    let mut authored: Vec<&BlockHeading<'_>> = Vec::new();
    for (version, lines) in &branch_lines {
        let empty = BTreeSet::new();
        let theirs = dest_lines.get(version).unwrap_or(&empty);
        let added: BTreeSet<&str> = lines.difference(theirs).copied().collect();
        let replaced = theirs.difference(lines).count();
        if added.is_empty() || added.len() <= replaced {
            continue;
        }
        if replaced > 0 {
            return Err(format!(
                "this branch both rewords and adds a `/// Version {}` heading, so which one it \
                 authored cannot be told apart; keep the destination's heading line as it is and \
                 head the branch's own paragraph `/// Version NEXT`",
                render(*version)
            ));
        }
        authored.extend(
            branch_headings
                .iter()
                .filter(|heading| heading.version == *version && added.contains(heading.line)),
        );
    }
    match authored.as_slice() {
        [] => Ok(None),
        [heading] => Ok(Some(HeadingBinding {
            line_index: heading.line_index,
            authored: heading.version,
            bound: destination_version + 1,
        })),
        many => Err(format!(
            "this branch adds {} version headings above `{anchor}` ({}); one change takes one \
             version, so fold them into a single `/// Version NEXT` paragraph",
            many.len(),
            many.iter()
                .map(|heading| format!("`{}`", heading.line.trim()))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn render(version: HeadingVersion) -> String {
    match version {
        HeadingVersion::Bound(value) => value.to_string(),
        HeadingVersion::Next => PROVISIONAL.to_string(),
    }
}

/// The branch text with `binding`'s heading rewritten to its bound version,
/// and the constant's literal with it while the constant is written as one.
/// Every other byte is unchanged.
pub fn apply_binding(
    branch: &str,
    binding: &HeadingBinding,
    anchor: &str,
) -> Result<String, String> {
    let mut bound = rewrite_heading(branch, binding)?;
    if let Some((range, _)) = literal(&bound, anchor) {
        bound.replace_range(range, &binding.bound.to_string());
    }
    Ok(bound)
}

fn rewrite_heading(branch: &str, binding: &HeadingBinding) -> Result<String, String> {
    let mut out = String::with_capacity(branch.len() + 4);
    let mut rewritten = false;
    for (index, line) in branch.split_inclusive('\n').enumerate() {
        if index != binding.line_index {
            out.push_str(line);
            continue;
        }
        let Some((version, token)) = heading(line) else {
            return Err(format!(
                "line {} is not the version heading the binding was planned for",
                index + 1
            ));
        };
        if version != binding.authored {
            return Err(format!(
                "line {} heads `{}`, not the `{}` the binding was planned for",
                index + 1,
                render(version),
                render(binding.authored)
            ));
        }
        out.push_str(&line[..token.start]);
        out.push_str(&binding.bound.to_string());
        out.push_str(&line[token.end..]);
        rewritten = true;
    }
    if !rewritten {
        return Err(format!(
            "the branch has no line {} to bind",
            binding.line_index + 1
        ));
    }
    Ok(out)
}

/// The landed commit's invariant, against the destination it landed on: no
/// NEXT heading; no version number the destination does not head except the
/// one after its own; and the derived version equal to the destination's (the
/// branch changes no protocol) or one above it (it changes one). A reworded
/// historical heading keeps its number, so it is not a new version.
pub fn landed_invariant(landed: &str, destination: &str, anchor: &str) -> Result<(), String> {
    let headings = block_headings(landed, anchor)
        .ok_or_else(|| format!("the landing declares no `pub const {anchor}`"))?;
    if headings
        .iter()
        .any(|heading| heading.version == HeadingVersion::Next)
    {
        return Err(format!(
            "the landing still carries `/// Version NEXT` above `{anchor}`"
        ));
    }
    let dest = derive_version(destination, anchor)?;
    let landed_version = derive_version(landed, anchor)?;
    if let Some((_, written)) = literal(landed, anchor) {
        if written != landed_version {
            return Err(format!(
                "the landing writes `{anchor}` = {written}, but its paragraphs derive \
                 {landed_version}"
            ));
        }
    }
    let dest_versions: BTreeSet<HeadingVersion> = block_headings(destination, anchor)
        .unwrap_or_default()
        .into_iter()
        .map(|heading| heading.version)
        .collect();
    let new_versions: BTreeSet<HeadingVersion> = headings
        .iter()
        .map(|heading| heading.version)
        .filter(|version| !dest_versions.contains(version))
        .collect();
    let next = HeadingVersion::Bound(dest + 1);
    let moved_by_one = landed_version == dest + 1 && new_versions == BTreeSet::from([next]);
    let unmoved = landed_version == dest && new_versions.is_empty();
    if moved_by_one || unmoved {
        return Ok(());
    }
    Err(format!(
        "the landing would derive `{anchor}` = {landed_version} on a destination at {dest}, \
         heading {} version(s) the destination does not; a landing adds at most version {}",
        new_versions.len(),
        dest + 1
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Fixture {
        derivation: Vec<Derivation>,
        cases: Vec<Case>,
    }

    #[derive(serde::Deserialize)]
    struct Derivation {
        name: String,
        source: String,
        version: Option<u64>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Case {
        name: String,
        branch: String,
        dest: String,
        ok: bool,
        binds_to: Option<u64>,
    }

    fn fixture() -> Fixture {
        serde_json::from_str(include_str!(
            "../../../../fixtures/executor-protocol-ledger-cases.json"
        ))
        .expect("the shared ledger fixture parses")
    }

    #[test]
    fn the_derivation_agrees_with_the_typescript_reader_on_every_shared_case() {
        for case in fixture().derivation {
            assert_eq!(
                derive_version(&case.source, ANCHOR).ok(),
                case.version,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn the_landing_rule_agrees_with_the_typescript_check_on_every_shared_case() {
        for case in fixture().cases {
            let planned = plan_binding(&case.branch, &case.dest, ANCHOR);
            assert_eq!(planned.is_ok(), case.ok, "{}: {planned:?}", case.name);
            if let Ok(binding) = planned {
                assert_eq!(binding.map(|b| b.bound), case.binds_to, "{}", case.name);
            }
        }
    }

    #[test]
    fn a_bound_heading_reads_the_next_version_and_the_landing_holds_the_invariant() {
        let dest = "/// Version 140 is main's.\n///\n/// Version 141 adds y.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;\n";
        let branch = "/// Version 140 is main's.\n///\n/// Version 141 adds y.\n///\n/// Version NEXT (cairn/5222) adds x.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;\n";
        assert_eq!(derive_version(branch, ANCHOR), Ok(142));
        let binding = plan_binding(branch, dest, ANCHOR).unwrap().unwrap();
        assert!(binding.rewrites());
        let bound = apply_binding(branch, &binding, ANCHOR).unwrap();
        assert!(bound.contains("/// Version 142 (cairn/5222) adds x.\n"));
        assert_eq!(bound.len(), branch.len() - 1);
        assert_eq!(derive_version(&bound, ANCHOR), Ok(142));
        assert_eq!(landed_invariant(&bound, dest, ANCHOR), Ok(()));
        assert!(
            landed_invariant(branch, dest, ANCHOR).is_err(),
            "NEXT never lands"
        );
        // Binding again against the same destination changes nothing.
        let again = plan_binding(&bound, dest, ANCHOR).unwrap().unwrap();
        assert!(!again.rewrites());
    }

    /// While the constant is written as a literal, it moves with the heading
    /// it is bound to, and a landing whose literal disagrees is refused.
    #[test]
    fn a_written_literal_moves_with_its_heading_and_must_agree() {
        let dest = "/// Version 141 adds y.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 141;\n";
        let branch = "/// Version 141 adds y.\n///\n/// Version 141 adds x.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 141;\n";
        assert_eq!(literal(dest, ANCHOR).map(|(_, value)| value), Some(141));
        assert_eq!(
            literal(
                "pub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;\n",
                ANCHOR
            ),
            None
        );
        let binding = plan_binding(branch, dest, ANCHOR).unwrap().unwrap();
        let bound = apply_binding(branch, &binding, ANCHOR).unwrap();
        assert!(
            bound.ends_with(
                "/// Version 142 adds x.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 142;\n"
            ),
            "{bound}"
        );
        assert_eq!(landed_invariant(&bound, dest, ANCHOR), Ok(()));
        let disagreeing = bound.replace("u32 = 142;", "u32 = 141;");
        assert!(landed_invariant(&disagreeing, dest, ANCHOR)
            .unwrap_err()
            .contains("paragraphs derive 142"));
    }

    #[test]
    fn paragraphs_are_the_blank_separated_runs_of_the_block() {
        let text = "//! crate\n\n/// Version 1 a,\n/// continued.\n///\n/// cairn/2 does not move it.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 1;\n";
        assert_eq!(
            doc_paragraphs(text, ANCHOR).unwrap(),
            vec![
                "/// Version 1 a,\n/// continued.".to_string(),
                "/// cairn/2 does not move it.".to_string()
            ]
        );
    }
}
