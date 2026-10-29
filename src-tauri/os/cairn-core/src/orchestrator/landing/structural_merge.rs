//! The ordered-set merge: settling a line conflict inside a registry list by
//! merging the list's elements rather than its lines.
//!
//! A registry is a list two branches append to independently. A line merge
//! sees two edits touching the same closing bracket and calls it a conflict;
//! taking both sides' conflicted regions whole closes the list twice
//! (cairn/5192). Here each conflicted region is mapped to the smallest list
//! that encloses it, that list's elements are merged as a set, and the line
//! merge is re-run with every merged list stood in for by a placeholder. Only
//! a merge in which every conflicted region was settled this way is returned;
//! anything else stays a textual conflict for an agent.
//!
//! Set semantics, per list:
//! - a base element survives when neither side removed it;
//! - an element either side inserted is kept, anchored after the base element
//!   that precedes it on that side, theirs (already landed) before ours;
//! - an insertion identical on both sides appears once;
//! - an element both sides removed, where the two sides put different things
//!   in its place, is a genuine edit conflict and is refused;
//! - an element one side changed (its comments, attributes, or anything else
//!   but its identity) while the other removed or replaced it is an edit
//!   against a delete or another edit, and is refused;
//! - an element whose identity (its code, whitespace-normalized) is unchanged
//!   but whose attached comments both sides changed differently is refused.
//!
//! Which lists qualify is decided by `cairn_symbols::symbols::lists`.

use std::collections::BTreeSet;
use std::ops::Range;

use cairn_symbols::symbols::lists::{rust_lists, RustList};

/// Merge `ours` into `theirs` from `base`, settling every line conflict inside
/// an allowlisted Rust list element-wise. `None` when the line merge leaves a
/// conflict this cannot settle, or when any version does not parse.
///
/// A line merge that reports no conflict but does not parse is treated as
/// conflicted where the two sides' changes touch: that is the shape of two
/// appends that each moved a list's closing line.
pub(crate) fn ordered_set_merge(base: &str, ours: &str, theirs: &str) -> Option<String> {
    if let Ok(clean) = diffy::merge(base, ours, theirs) {
        if rust_lists(&clean).is_some() {
            return Some(clean);
        }
    }
    let (base_lists, ours_lists, theirs_lists) =
        (rust_lists(base)?, rust_lists(ours)?, rust_lists(theirs)?);
    let ours_hunks = hunks(base, ours);
    let theirs_hunks = hunks(base, theirs);
    let base_lines = LineIndex::new(base);

    let mut chosen: BTreeSet<usize> = BTreeSet::new();
    for interval in conflicted_intervals(&ours_hunks, &theirs_hunks) {
        chosen.insert(smallest_enclosing(&base_lines, &base_lists, &interval)?);
    }
    let chosen: Vec<usize> = chosen.into_iter().collect();
    let nested = chosen.iter().any(|&outer| {
        chosen.iter().any(|&inner| {
            outer != inner && contains(&base_lists[outer].content, &base_lists[inner].content)
        })
    });
    if chosen.is_empty() || nested {
        return None;
    }

    let ours_lines = LineIndex::new(ours);
    let theirs_lines = LineIndex::new(theirs);
    let mut substitutions = Vec::new();
    for &index in &chosen {
        let list = &base_lists[index];
        let ours_list = corresponding(&base_lines, list, &ours_hunks, &ours_lines, &ours_lists)?;
        let theirs_list = corresponding(
            &base_lines,
            list,
            &theirs_hunks,
            &theirs_lines,
            &theirs_lists,
        )?;
        let merged = merge_list((base, list), (ours, ours_list), (theirs, theirs_list))?;
        substitutions.push((
            list.content.clone(),
            ours_list.content.clone(),
            theirs_list.content.clone(),
            merged,
        ));
    }

    let placeholder = |index: usize| format!("/*@@cairn-ordered-set-{index}@@*/");
    if [base, ours, theirs]
        .iter()
        .any(|text| text.contains("@@cairn-ordered-set-"))
    {
        return None;
    }
    let stand_in = |text: &str, ranges: Vec<Range<usize>>| {
        let mut ordered: Vec<(usize, Range<usize>)> = ranges.into_iter().enumerate().collect();
        ordered.sort_by_key(|(_, range)| std::cmp::Reverse(range.start));
        let mut out = text.to_string();
        for (index, range) in ordered {
            out.replace_range(range, &placeholder(index));
        }
        out
    };
    let base_stood = stand_in(
        base,
        substitutions.iter().map(|(b, _, _, _)| b.clone()).collect(),
    );
    let ours_stood = stand_in(
        ours,
        substitutions.iter().map(|(_, o, _, _)| o.clone()).collect(),
    );
    let theirs_stood = stand_in(
        theirs,
        substitutions.iter().map(|(_, _, t, _)| t.clone()).collect(),
    );
    let mut merged = diffy::merge(&base_stood, &ours_stood, &theirs_stood).ok()?;
    for (index, (_, _, _, content)) in substitutions.iter().enumerate() {
        let marker = placeholder(index);
        if merged.matches(&marker).count() != 1 {
            return None;
        }
        merged = merged.replacen(&marker, content, 1);
    }
    rust_lists(&merged).map(|_| merged)
}

/// One changed region of a side against base: `old` in base lines, `new` in the
/// side's lines, both zero-based and half-open. An insertion has an empty `old`
/// whose start is the base line it precedes.
#[derive(Debug, Clone)]
struct Hunk {
    old: Range<usize>,
    new_len: usize,
}

fn hunks(base: &str, side: &str) -> Vec<Hunk> {
    let mut options = diffy::DiffOptions::new();
    options.set_context_len(0);
    options
        .create_patch(base, side)
        .hunks()
        .iter()
        .map(|hunk| {
            let old = hunk.old_range();
            // diffy numbers a non-empty range from 1 and an empty one by the
            // line it follows, as unified diffs do.
            let start = if old.is_empty() {
                old.start()
            } else {
                old.start() - 1
            };
            Hunk {
                old: start..start + old.len(),
                new_len: hunk.new_range().len(),
            }
        })
        .collect()
}

/// The base line ranges where the two sides' changes touch or overlap, which
/// is where a line merge conflicts.
fn conflicted_intervals(ours: &[Hunk], theirs: &[Hunk]) -> Vec<Range<usize>> {
    let mut intervals: Vec<Range<usize>> = Vec::new();
    for o in ours {
        for t in theirs {
            if o.old.start <= t.old.end && t.old.start <= o.old.end {
                intervals.push(o.old.start.min(t.old.start)..o.old.end.max(t.old.end));
            }
        }
    }
    intervals.sort_by_key(|interval| interval.start);
    let mut merged: Vec<Range<usize>> = Vec::new();
    for interval in intervals {
        match merged.last_mut() {
            Some(last) if interval.start <= last.end => last.end = last.end.max(interval.end),
            _ => merged.push(interval),
        }
    }
    merged
}

struct LineIndex {
    starts: Vec<usize>,
}

impl LineIndex {
    fn new(text: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(text.match_indices('\n').map(|(at, _)| at + 1));
        Self { starts }
    }

    fn line_of(&self, offset: usize) -> usize {
        self.starts.partition_point(|&start| start <= offset) - 1
    }

    fn column_of(&self, offset: usize) -> usize {
        offset - self.starts[self.line_of(offset)]
    }
}

fn contains(outer: &Range<usize>, inner: &Range<usize>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

/// The list whose interior holds the whole interval. A delimited list's
/// opening line comes before the interval, which may run up to and include its
/// closing line. A declaration run's lines hold the interval, which may also
/// be an insertion directly after the run.
fn smallest_enclosing(
    lines: &LineIndex,
    lists: &[RustList],
    interval: &Range<usize>,
) -> Option<usize> {
    lists
        .iter()
        .enumerate()
        .filter(|(_, list)| match list.open {
            Some(open) => {
                let close = lines.line_of(list.content.end);
                lines.line_of(open) < interval.start
                    && if interval.is_empty() {
                        interval.start <= close
                    } else {
                        interval.end <= close + 1
                    }
            }
            None => {
                let first = lines.line_of(list.content.start);
                let last = lines.line_of(list.content.end.saturating_sub(1));
                first <= interval.start && interval.end <= last + 1
            }
        })
        .min_by_key(|(_, list)| list.content.len())
        .map(|(index, _)| index)
}

/// Where an unchanged base line sits on a side, or `None` when the side changed
/// it.
fn map_line(line: usize, hunks: &[Hunk]) -> Option<usize> {
    let mut shift: isize = 0;
    for hunk in hunks {
        if hunk.old.contains(&line) {
            return None;
        }
        if hunk.old.start <= line {
            shift += hunk.new_len as isize - hunk.old.len() as isize;
        }
    }
    usize::try_from(line as isize + shift).ok()
}

/// The side's list that opens on the base list's opening line, unchanged, at
/// the same column and of the same kind. A declaration run has no opening
/// line; it is found by the line of its first element, which the side must
/// have left alone.
fn corresponding<'l>(
    base_lines: &LineIndex,
    list: &RustList,
    hunks: &[Hunk],
    side_lines: &LineIndex,
    side_lists: &'l [RustList],
) -> Option<&'l RustList> {
    let Some(open) = list.open else {
        let anchor = list.elements.first()?.body.start;
        let line = map_line(base_lines.line_of(anchor), hunks)?;
        return side_lists.iter().find(|candidate| {
            candidate.open.is_none()
                && candidate.kind == list.kind
                && side_lines.line_of(candidate.content.start) <= line
                && line <= side_lines.line_of(candidate.content.end.saturating_sub(1))
        });
    };
    let line = map_line(base_lines.line_of(open), hunks)?;
    let column = base_lines.column_of(open);
    side_lists.iter().find(|candidate| {
        candidate.kind == list.kind
            && candidate.open.is_some_and(|candidate_open| {
                side_lines.line_of(candidate_open) == line
                    && side_lines.column_of(candidate_open) == column
            })
    })
}

/// One element as it renders: the whitespace before it, its text with attached
/// comments, and for a comma list whatever follows its comma on the same line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Element<'a> {
    key: String,
    lead: &'a str,
    text: &'a str,
    after: &'a str,
}

/// A list's elements and the text from its last element to its closing
/// delimiter.
fn elements<'a>(src: &'a str, list: &RustList) -> Option<(Vec<Element<'a>>, &'a str)> {
    let comma = list.kind.comma_separated();
    let mut cursor = list.content.start;
    let mut out = Vec::with_capacity(list.elements.len());
    for (index, element) in list.elements.iter().enumerate() {
        let lead = src.get(cursor..element.span.start)?;
        let text = &src[element.span.clone()];
        let key = src[element.body.clone()]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let has_comma = comma && (index + 1 < list.elements.len() || list.trailing_comma);
        let after = if has_comma {
            let comma_at = element.body.end + src[element.body.end..].find(',')?;
            cursor = comma_at + 1;
            match &element.trailing {
                Some(trailing) => {
                    cursor = trailing.end;
                    &src[comma_at + 1..trailing.end]
                }
                None => "",
            }
        } else {
            cursor = element.span.end;
            ""
        };
        out.push(Element {
            key,
            lead,
            text,
            after,
        });
    }
    Some((out, src.get(cursor..list.content.end)?))
}

/// For each base element, the side element it survives as; and each side
/// element that is new, with the base element it follows on that side.
struct Alignment {
    kept: Vec<Option<usize>>,
    inserted: Vec<(Option<usize>, usize)>,
}

fn align(base: &[Element<'_>], side: &[Element<'_>]) -> Alignment {
    let (n, m) = (base.len(), side.len());
    let mut lcs = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if base[i].key == side[j].key {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut kept = vec![None; n];
    let mut inserted = Vec::new();
    let (mut i, mut j) = (0, 0);
    let mut anchor: Option<usize> = None;
    while i < n || j < m {
        if i < n && j < m && base[i].key == side[j].key {
            kept[i] = Some(j);
            anchor = Some(i);
            i += 1;
            j += 1;
        } else if j < m && (i == n || lcs[i][j + 1] >= lcs[i + 1][j]) {
            inserted.push((anchor, j));
            j += 1;
        } else {
            i += 1;
        }
    }
    Alignment { kept, inserted }
}

/// The base element each base position falls after on a side: the last base
/// element before it that the side kept.
fn gap_anchor(kept: &[Option<usize>], position: usize) -> Option<usize> {
    (0..position).rev().find(|&i| kept[i].is_some())
}

/// Three-way choice of one piece of text: whichever side changed it, or the
/// common change; `None` when both changed it differently.
fn pick<T: PartialEq + Copy>(base: T, ours: T, theirs: T) -> Option<T> {
    if ours == base || ours == theirs {
        Some(theirs)
    } else if theirs == base {
        Some(ours)
    } else {
        None
    }
}

fn merge_list<'a>(
    (base_src, base_list): (&'a str, &RustList),
    (ours_src, ours_list): (&'a str, &RustList),
    (theirs_src, theirs_list): (&'a str, &RustList),
) -> Option<String> {
    let (base, base_closing) = elements(base_src, base_list)?;
    let (ours, ours_closing) = elements(ours_src, ours_list)?;
    let (theirs, theirs_closing) = elements(theirs_src, theirs_list)?;
    let ours_alignment = align(&base, &ours);
    let theirs_alignment = align(&base, &theirs);
    let insertions_at = |alignment: &Alignment, side: &[Element<'a>], anchor: Option<usize>| {
        alignment
            .inserted
            .iter()
            .filter(|(at, _)| *at == anchor)
            .map(|(_, index)| (side[*index].key.clone(), side[*index].text))
            .collect::<Vec<_>>()
    };

    // An element one side changed and the other removed or re-keyed: keeping
    // either loses the other side's edit.
    for (position, base_element) in base.iter().enumerate() {
        let edited = |kept: Option<usize>, side: &[Element<'a>]| {
            kept.is_some_and(|index| {
                (side[index].text, side[index].after) != (base_element.text, base_element.after)
            })
        };
        let (ours_kept, theirs_kept) = (
            ours_alignment.kept[position],
            theirs_alignment.kept[position],
        );
        if ours_kept.is_some() != theirs_kept.is_some()
            && (edited(ours_kept, &ours) || edited(theirs_kept, &theirs))
        {
            return None;
        }
    }

    // An element both sides removed, with different things put in its place,
    // is two edits of one element, not two independent entries.
    for position in 0..base.len() {
        if ours_alignment.kept[position].is_some() || theirs_alignment.kept[position].is_some() {
            continue;
        }
        let ours_in_place = insertions_at(
            &ours_alignment,
            &ours,
            gap_anchor(&ours_alignment.kept, position),
        );
        let theirs_in_place = insertions_at(
            &theirs_alignment,
            &theirs,
            gap_anchor(&theirs_alignment.kept, position),
        );
        if ours_in_place != theirs_in_place {
            return None;
        }
    }

    let theirs_inserted: Vec<&Element<'a>> = theirs_alignment
        .inserted
        .iter()
        .map(|(_, index)| &theirs[*index])
        .collect();
    let mut result: Vec<Element<'a>> = Vec::new();
    let emit_insertions = |result: &mut Vec<Element<'a>>, anchor: Option<usize>| -> Option<()> {
        for (at, index) in &theirs_alignment.inserted {
            if *at == anchor {
                result.push(theirs[*index].clone());
            }
        }
        for (at, index) in &ours_alignment.inserted {
            if *at != anchor {
                continue;
            }
            let element = &ours[*index];
            match theirs_inserted
                .iter()
                .find(|landed| landed.key == element.key)
            {
                Some(landed) if landed.text == element.text && landed.after == element.after => {}
                Some(_) => return None,
                None => result.push(element.clone()),
            }
        }
        Some(())
    };
    emit_insertions(&mut result, None)?;
    for (position, base_element) in base.iter().enumerate() {
        if let (Some(o), Some(t)) = (
            ours_alignment.kept[position],
            theirs_alignment.kept[position],
        ) {
            let (ours_element, theirs_element) = (&ours[o], &theirs[t]);
            let (text, after) = pick(
                (base_element.text, base_element.after),
                (ours_element.text, ours_element.after),
                (theirs_element.text, theirs_element.after),
            )?;
            result.push(Element {
                key: base_element.key.clone(),
                lead: theirs_element.lead,
                text,
                after,
            });
        }
        emit_insertions(&mut result, Some(position))?;
    }
    let closing = pick(base_closing, ours_closing, theirs_closing)?;

    let comma = base_list.kind.comma_separated();
    let trailing_comma = if theirs.is_empty() {
        ours_list.trailing_comma
    } else {
        theirs_list.trailing_comma
    };
    let mut out = String::new();
    for (index, element) in result.iter().enumerate() {
        out.push_str(element.lead);
        out.push_str(element.text);
        if comma && (index + 1 < result.len() || trailing_comma) {
            out.push(',');
            out.push_str(element.after);
        }
    }
    out.push_str(closing);
    Some(out)
}

/// The version-paragraphs merge: the doc block above `anchor` merged as an
/// ordered set of its blank-`///`-separated paragraphs, and the rest of the
/// file line-merged around it.
///
/// Every branch that changes the executor protocol appends a paragraph to the
/// one block above `EXECUTOR_PROTOCOL_VERSION`, so two of them always touch the
/// same line and a line merge always conflicts. As paragraphs they are two
/// insertions at one place, kept in landing order, the landed side's first.
/// The set rules are the ordered-set merge's, with a paragraph's exact text as
/// its identity: a paragraph one side rewrote and the other removed or
/// rewrote differently is a conflict, and so is any conflict outside the
/// block. `None` leaves the file to the line merge's conflict.
pub(crate) fn version_paragraphs_merge(
    base: &str,
    ours: &str,
    theirs: &str,
    anchor: &str,
) -> Option<String> {
    if let Ok(clean) = diffy::merge(base, ours, theirs) {
        return Some(clean);
    }
    const PLACEHOLDER: &str = "/*@@cairn-version-paragraphs@@*/\n";
    if [base, ours, theirs]
        .iter()
        .any(|text| text.contains("@@cairn-version-paragraphs@@"))
    {
        return None;
    }
    let (base_rest, base_paragraphs, _) = split_doc_block(base, anchor, PLACEHOLDER)?;
    let (ours_rest, ours_paragraphs, _) = split_doc_block(ours, anchor, PLACEHOLDER)?;
    let (theirs_rest, theirs_paragraphs, separator) = split_doc_block(theirs, anchor, PLACEHOLDER)?;
    let rest = diffy::merge(&base_rest, &ours_rest, &theirs_rest).ok()?;
    if rest.matches(PLACEHOLDER).count() != 1 {
        return None;
    }
    let paragraphs = merge_sequence(&base_paragraphs, &ours_paragraphs, &theirs_paragraphs)?;
    let mut block = paragraphs.join(&format!("\n{separator}\n"));
    if !block.is_empty() {
        block.push('\n');
    }
    Some(rest.replacen(PLACEHOLDER, &block, 1))
}

/// `text` with `anchor`'s doc block replaced by `placeholder`, the block's
/// paragraphs, and the blank doc line that separates them (`///` when the
/// block has none).
fn split_doc_block(
    text: &str,
    anchor: &str,
    placeholder: &str,
) -> Option<(String, Vec<String>, String)> {
    use cairn_common::executor_protocol_ledger::{doc_block, doc_paragraphs};
    let block = doc_block(text, anchor)?;
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let separator = lines[block.clone()]
        .iter()
        .map(|line| line.trim_end_matches(['\n', '\r']))
        .find(|line| {
            line.trim_start()
                .strip_prefix("///")
                .is_some_and(|rest| rest.trim().is_empty())
        })
        .unwrap_or("///")
        .to_string();
    let mut rest = lines[..block.start].concat();
    rest.push_str(placeholder);
    rest.push_str(&lines[block.end..].concat());
    Some((rest, doc_paragraphs(text, anchor)?, separator))
}

/// Merge three versions of a sequence as an ordered set with each element's
/// value as its identity: an element survives unless a side removed it, every
/// insertion is kept after the base element it follows (theirs first, an
/// identical insertion once), and an element both sides removed with different
/// things put in its place is refused as two edits of one element.
fn merge_sequence<T: Clone + Eq>(base: &[T], ours: &[T], theirs: &[T]) -> Option<Vec<T>> {
    let ours_alignment = align_values(base, ours);
    let theirs_alignment = align_values(base, theirs);
    let in_place = |alignment: &Alignment, side: &[T], anchor: Option<usize>| -> Vec<T> {
        alignment
            .inserted
            .iter()
            .filter(|(at, _)| *at == anchor)
            .map(|(_, index)| side[*index].clone())
            .collect()
    };
    for position in 0..base.len() {
        if ours_alignment.kept[position].is_some() || theirs_alignment.kept[position].is_some() {
            continue;
        }
        let ours_in_place = in_place(
            &ours_alignment,
            ours,
            gap_anchor(&ours_alignment.kept, position),
        );
        let theirs_in_place = in_place(
            &theirs_alignment,
            theirs,
            gap_anchor(&theirs_alignment.kept, position),
        );
        if ours_in_place != theirs_in_place {
            return None;
        }
    }
    // A base element one side removed while the other kept it is a removal,
    // unless the removing side put something in its place: that is an edit,
    // and it is kept as the edited element by the insertion pass below.
    let mut result = Vec::new();
    let emit = |result: &mut Vec<T>, anchor: Option<usize>| {
        let theirs_here = in_place(&theirs_alignment, theirs, anchor);
        for element in &theirs_here {
            result.push(element.clone());
        }
        for element in in_place(&ours_alignment, ours, anchor) {
            if !theirs_here.contains(&element) {
                result.push(element);
            }
        }
    };
    emit(&mut result, None);
    for (position, element) in base.iter().enumerate() {
        if ours_alignment.kept[position].is_some() && theirs_alignment.kept[position].is_some() {
            result.push(element.clone());
        }
        emit(&mut result, Some(position));
    }
    Some(result)
}

fn align_values<T: Eq>(base: &[T], side: &[T]) -> Alignment {
    let (n, m) = (base.len(), side.len());
    let mut lcs = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if base[i] == side[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut kept = vec![None; n];
    let mut inserted = Vec::new();
    let (mut i, mut j) = (0, 0);
    let mut anchor: Option<usize> = None;
    while i < n || j < m {
        if i < n && j < m && base[i] == side[j] {
            kept[i] = Some(j);
            anchor = Some(i);
            i += 1;
            j += 1;
        } else if j < m && (i == n || lcs[i][j + 1] >= lcs[i + 1][j]) {
            inserted.push((anchor, j));
            j += 1;
        } else {
            i += 1;
        }
    }
    Alignment { kept, inserted }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(text: &str) -> bool {
        cairn_vcs::parse_gate::parse_failure("merged.rs", text).is_none()
    }

    /// The private lineage pin at the moment cairn/5192 broke: the branch
    /// appended one migration name, main had appended three and closed the
    /// array with `],`, and the old union wrote both regions whole.
    const PIN_BASE: &str = "\
#[test]
fn private_migrations_apply_in_order() {
    assert_eq!(
        applied_names(),
        &[
            \"command_outcome_history\",
            \"pr_publish_attempts\",
            \"index_sibling_job_lookup\",
        ]
    );
}
";
    const PIN_OURS: &str = "\
#[test]
fn private_migrations_apply_in_order() {
    assert_eq!(
        applied_names(),
        &[
            \"command_outcome_history\",
            \"pr_publish_attempts\",
            \"index_sibling_job_lookup\",
            // cairn/5113: tenets become yes/no statements.
            \"tenet_statements\",
        ]
    );
}
";
    const PIN_THEIRS: &str = "\
#[test]
fn private_migrations_apply_in_order() {
    assert_eq!(
        applied_names(),
        &[
            \"command_outcome_history\",
            \"pr_publish_attempts\",
            \"index_sibling_job_lookup\",
            \"inference_records\",
            \"object_store_maintenance\",
            \"ingress_events\",
        ],
    );
}
";

    #[test]
    fn the_5192_pin_merges_into_one_parseable_array() {
        assert!(diffy::merge(PIN_BASE, PIN_OURS, PIN_THEIRS).is_err());
        let merged = ordered_set_merge(PIN_BASE, PIN_OURS, PIN_THEIRS).expect("the pin settles");
        assert_eq!(
            merged,
            "\
#[test]
fn private_migrations_apply_in_order() {
    assert_eq!(
        applied_names(),
        &[
            \"command_outcome_history\",
            \"pr_publish_attempts\",
            \"index_sibling_job_lookup\",
            \"inference_records\",
            \"object_store_maintenance\",
            \"ingress_events\",
            // cairn/5113: tenets become yes/no statements.
            \"tenet_statements\",
        ],
    );
}
"
        );
        assert!(parses(&merged));
        assert_eq!(merged.matches("],").count(), 1);
    }

    const LIST_BASE: &str = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n];\n";

    #[test]
    fn different_insertions_at_one_place_are_both_kept_theirs_first() {
        let ours = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"ours\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"theirs\",\n];\n";
        assert_eq!(
            ordered_set_merge(LIST_BASE, ours, theirs).unwrap(),
            "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"theirs\",\n    \"ours\",\n];\n"
        );
    }

    #[test]
    fn an_identical_insertion_on_both_sides_appears_once() {
        let ours =
            "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"same\",\n    \"ours\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"same\",\n    \"theirs\",\n];\n";
        assert_eq!(
            ordered_set_merge(LIST_BASE, ours, theirs).unwrap(),
            "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"same\",\n    \"theirs\",\n    \"ours\",\n];\n"
        );
    }

    #[test]
    fn a_base_duplicate_each_side_deleted_once_leaves_one_copy() {
        let base = "pin!(\n    a!(),\n    dup!(),\n    dup!(),\n    b!(),\n);\n";
        let ours = "pin!(\n    a!(),\n    dup!(),\n    b!(),\n    ours!(),\n);\n";
        let theirs = "pin!(\n    a!(),\n    dup!(),\n    b!(),\n    theirs!(),\n);\n";
        assert_eq!(
            ordered_set_merge(base, ours, theirs).unwrap(),
            "pin!(\n    a!(),\n    dup!(),\n    b!(),\n    theirs!(),\n    ours!(),\n);\n"
        );
    }

    /// A list is located by an opening line both sides left alone; a list
    /// that lives entirely on the conflicted line has none, so the edit stays
    /// the agent's.
    #[test]
    fn a_list_on_its_own_conflicted_line_stays_textual() {
        let base = "use a::{b, c};\n";
        let ours = "use a::{b, c, ours};\n";
        let theirs = "use a::{b, c, theirs};\n";
        assert_eq!(ordered_set_merge(base, ours, theirs), None);
    }

    #[test]
    fn a_multi_line_use_list_merges() {
        let base = "use a::{\n    b,\n    c,\n};\n";
        let ours = "use a::{\n    b,\n    c,\n    ours,\n};\n";
        let theirs = "use a::{\n    b,\n    c,\n    theirs,\n};\n";
        assert_eq!(
            ordered_set_merge(base, ours, theirs).unwrap(),
            "use a::{\n    b,\n    c,\n    theirs,\n    ours,\n};\n"
        );
    }

    #[test]
    fn module_declarations_appended_together_are_both_kept() {
        let base = "//! crate\n\npub mod a;\npub mod b;\n";
        let ours = "//! crate\n\npub mod a;\npub mod b;\npub mod ours;\n";
        let theirs = "//! crate\n\npub mod a;\npub mod b;\n#[cfg(test)]\nmod theirs;\n";
        assert_eq!(
            ordered_set_merge(base, ours, theirs).unwrap(),
            "//! crate\n\npub mod a;\npub mod b;\n#[cfg(test)]\nmod theirs;\npub mod ours;\n"
        );
    }

    #[test]
    fn two_same_named_functions_appended_stay_textual() {
        let base = "pub mod a;\n\nfn existing() {}\n";
        let ours = "pub mod a;\n\nfn existing() {}\n\npub fn helper() -> u8 {\n    1\n}\n";
        let theirs = "pub mod a;\n\nfn existing() {}\n\npub fn helper() -> u8 {\n    2\n}\n";
        assert_eq!(ordered_set_merge(base, ours, theirs), None);
    }

    #[test]
    fn two_methods_added_to_one_impl_stay_textual() {
        let base = "impl S {\n    fn a() {}\n}\n";
        let ours = "impl S {\n    fn a() {}\n    fn b() -> u8 { 1 }\n}\n";
        let theirs = "impl S {\n    fn a() {}\n    fn b() -> u8 { 2 }\n}\n";
        assert_eq!(ordered_set_merge(base, ours, theirs), None);
    }

    #[test]
    fn an_attribute_edit_against_an_edit_of_its_item_stays_textual() {
        let base = "#[derive(Debug)]\npub struct S;\n";
        let ours = "#[derive(Debug, Clone)]\npub struct S;\n";
        let theirs = "#[derive(Debug)]\npub struct S(u8);\n";
        assert_eq!(ordered_set_merge(base, ours, theirs), None);
    }

    #[test]
    fn an_entry_commented_on_one_side_and_renamed_on_the_other_stays_textual() {
        let ours = "const NAMES: &[&str] = &[\n    \"a\",\n    // why b\n    \"b\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b2\",\n];\n";
        assert_eq!(ordered_set_merge(LIST_BASE, ours, theirs), None);
    }

    #[test]
    fn an_entry_commented_on_one_side_and_deleted_on_the_other_stays_textual() {
        let ours = "const NAMES: &[&str] = &[\n    \"a\",\n    // why b\n    \"b\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n];\n";
        assert_eq!(ordered_set_merge(LIST_BASE, ours, theirs), None);
        assert_eq!(ordered_set_merge(LIST_BASE, theirs, ours), None);
    }

    #[test]
    fn a_conflict_inside_a_function_body_stays_textual() {
        let base = "fn f() -> u32 {\n    let x = 1;\n    x\n}\n";
        let ours = "fn f() -> u32 {\n    let x = 2;\n    x\n}\n";
        let theirs = "fn f() -> u32 {\n    let x = 3;\n    x\n}\n";
        assert_eq!(ordered_set_merge(base, ours, theirs), None);
    }

    #[test]
    fn the_same_element_edited_two_ways_stays_textual() {
        let ours = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b-ours\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b-theirs\",\n];\n";
        assert_eq!(ordered_set_merge(LIST_BASE, ours, theirs), None);
    }

    #[test]
    fn one_entry_with_two_different_comments_stays_textual() {
        let ours = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    // why, per ours\n    \"c\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    // why, per theirs\n    \"c\",\n];\n";
        assert_eq!(ordered_set_merge(LIST_BASE, ours, theirs), None);
    }

    const PROTOCOL_BASE: &str = "\
//! protocol

/// Version 156 adds the directive.
///
/// cairn/5288 does not move the number.
pub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;

pub enum Frame {}
";

    /// The specimen of cairn/5116 and cairn/5324 (10-26): two branches each
    /// append a paragraph to the block, which a line merge always conflicts on.
    #[test]
    fn two_appended_version_paragraphs_are_both_kept_the_landed_one_first() {
        let ours = PROTOCOL_BASE.replace(
            "/// cairn/5288 does not move the number.\n",
            "/// cairn/5288 does not move the number.\n///\n/// Version NEXT (cairn/5324) adds ours.\n",
        );
        let theirs = PROTOCOL_BASE
            .replace(
                "/// cairn/5288 does not move the number.\n",
                "/// cairn/5288 does not move the number.\n///\n/// Version 157 (cairn/5116) adds theirs,\n/// over two lines.\n",
            )
            .replace("pub enum Frame {}", "pub enum Frame { Theirs }");
        assert!(diffy::merge(PROTOCOL_BASE, &ours, &theirs).is_err());
        let merged =
            version_paragraphs_merge(PROTOCOL_BASE, &ours, &theirs, "EXECUTOR_PROTOCOL_VERSION")
                .expect("two appended paragraphs settle");
        assert_eq!(
            merged,
            "\
//! protocol

/// Version 156 adds the directive.
///
/// cairn/5288 does not move the number.
///
/// Version 157 (cairn/5116) adds theirs,
/// over two lines.
///
/// Version NEXT (cairn/5324) adds ours.
pub const EXECUTOR_PROTOCOL_VERSION: u32 = derived::EXECUTOR_PROTOCOL_VERSION;

pub enum Frame { Theirs }
"
        );
        assert_eq!(
            cairn_common::executor_protocol_ledger::derive_version(
                &merged,
                "EXECUTOR_PROTOCOL_VERSION"
            ),
            Ok(158)
        );
    }

    #[test]
    fn a_paragraph_rewritten_on_one_side_and_removed_on_the_other_stays_textual() {
        let ours =
            PROTOCOL_BASE.replace("does not move the number.", "does not move it, reworded.");
        let theirs = PROTOCOL_BASE.replace("///\n/// cairn/5288 does not move the number.\n", "");
        assert_eq!(
            version_paragraphs_merge(PROTOCOL_BASE, &ours, &theirs, "EXECUTOR_PROTOCOL_VERSION"),
            None
        );
    }

    #[test]
    fn a_conflict_outside_the_block_stays_textual() {
        let ours = PROTOCOL_BASE
            .replace("pub enum Frame {}", "pub enum Frame { Ours }")
            .replace(
                "/// cairn/5288 does not move the number.\n",
                "/// cairn/5288 does not move the number.\n///\n/// Version NEXT a.\n",
            );
        let theirs = PROTOCOL_BASE
            .replace("pub enum Frame {}", "pub enum Frame { Theirs }")
            .replace(
                "/// cairn/5288 does not move the number.\n",
                "/// cairn/5288 does not move the number.\n///\n/// Version 157 b.\n",
            );
        assert_eq!(
            version_paragraphs_merge(PROTOCOL_BASE, &ours, &theirs, "EXECUTOR_PROTOCOL_VERSION"),
            None
        );
    }

    #[test]
    fn a_clean_line_merge_is_returned_as_is() {
        let ours = "const NAMES: &[&str] = &[\n    \"ours\",\n    \"a\",\n    \"b\",\n];\n";
        let theirs = "const NAMES: &[&str] = &[\n    \"a\",\n    \"b\",\n    \"theirs\",\n];\n";
        assert_eq!(
            ordered_set_merge(LIST_BASE, ours, theirs).unwrap(),
            "const NAMES: &[&str] = &[\n    \"ours\",\n    \"a\",\n    \"b\",\n    \"theirs\",\n];\n"
        );
    }
}
