//! Offline Claude token counting derived from ctok.
#![forbid(unsafe_code)]

use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};
use unicode_general_category::{get_general_category, GeneralCategory as G};
use unicode_normalization::{char::canonical_combining_class, UnicodeNormalization};

const BOW: char = '\u{fdd0}';
const EOW: char = '\u{fdd1}';
const SHIFT: char = '\u{fdd3}';
const CAPS: char = '\u{fdd4}';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Family {
    V4_7,
    V5,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cut {
    pub byte_offset: usize,
    pub tokens: u32,
}
impl Family {
    fn overhead(self) -> u32 {
        if self == Self::V4_7 {
            11
        } else {
            6
        }
    }
}

pub fn count(text: &str, family: Family) -> u32 {
    let tail = if family == Family::V4_7 {
        let n = normalize(text)
            .iter()
            .rev()
            .take_while(|m| m.c == '\n')
            .count();
        frame_tail(n)
    } else {
        0
    };
    family.overhead()
        + model()
            .costs(&marked(text, family))
            .last()
            .copied()
            .unwrap_or(0)
        + tail
}

fn frame_tail(n: usize) -> u32 {
    if n == 0 {
        return 0;
    }
    let run: Vec<Mark> = (0..n + 2).map(|_| Mark { c: '\n', end: 0 }).collect();
    let costs = model().costs(&run);
    let mut best = u32::MAX;
    // The final vocabulary token belongs to the frame. Minimize the prefix that leaves a suffix piece.
    for cut in 0..run.len() {
        let suffix: String = run[cut..].iter().map(|m| m.c).collect();
        if trie_has(&model().trie, &suffix) {
            best = best.min(costs[cut]);
        }
    }
    best
}
fn trie_has(trie: &Trie, s: &str) -> bool {
    let mut node = trie;
    for c in s.chars().rev() {
        let Some(n) = node.next.get(&c) else {
            return false;
        };
        node = n;
    }
    node.end
}

/// Finds the longest source prefix under a content-token budget in one DP pass.
/// Message framing is deliberately excluded from prefix budgets.
pub fn cut(text: &str, budget: u32, family: Family) -> Cut {
    let s = marked(text, family);
    let costs = model().costs(&s);
    let mut out = Cut {
        byte_offset: 0,
        tokens: 0,
    };
    for (i, m) in s.iter().enumerate() {
        if costs[i + 1] <= budget && m.end >= out.byte_offset {
            out = Cut {
                byte_offset: m.end,
                tokens: costs[i + 1],
            }
        }
    }
    out
}

#[derive(Deserialize)]
struct Doc {
    pieces: Vec<String>,
    bytes: Vec<String>,
    contractions: Vec<String>,
}
#[derive(Default)]
struct Trie {
    end: bool,
    next: HashMap<char, Trie>,
}
impl Trie {
    fn add(&mut self, s: &str) {
        let mut n = self;
        for c in s.chars().rev() {
            n = n.next.entry(c).or_default()
        }
        n.end = true
    }
}
struct Model {
    trie: Trie,
    units: HashSet<char>,
    bytes: HashSet<Vec<u8>>,
}
static MODEL: OnceLock<Model> = OnceLock::new();
fn model() -> &'static Model {
    MODEL.get_or_init(|| {
        let d: Doc = serde_json::from_str(include_str!("../data/pieces_v4_7.json")).unwrap();
        let mut trie = Trie::default();
        let mut units = HashSet::new();
        for p in d.pieces {
            let p = parse(&p);
            if p.chars().count() == 1 {
                units.insert(p.chars().next().unwrap());
            }
            trie.add(&p)
        }
        for c in d.contractions {
            trie.add(&(c + &EOW.to_string()))
        }
        Model {
            trie,
            units,
            bytes: d.bytes.into_iter().map(|s| hex(&s)).collect(),
        }
    })
}
impl Model {
    fn unit(&self, c: char) -> u32 {
        if matches!(c, BOW | EOW | SHIFT | CAPS) || self.units.contains(&c) {
            return 1;
        }
        let b = c.to_string().into_bytes();
        let mut dp = vec![u32::MAX; b.len() + 1];
        dp[0] = 0;
        for i in 1..=b.len() {
            for j in 0..i {
                if i - j == 1 || self.bytes.contains(&b[j..i]) {
                    dp[i] = dp[i].min(dp[j] + 1)
                }
            }
        }
        dp[b.len()]
    }
    fn costs(&self, s: &[Mark]) -> Vec<u32> {
        let mut best = vec![0; s.len() + 1];
        for end in 1..=s.len() {
            best[end] = best[end - 1] + self.unit(s[end - 1].c);
            let mut node = self.trie.next.get(&s[end - 1].c);
            if node.is_some_and(|n| n.end) {
                best[end] = best[end].min(best[end - 1] + 1)
            }
            for start in (0..end - 1).rev() {
                node = node.and_then(|n| n.next.get(&s[start].c));
                let Some(n) = node else { break };
                if n.end {
                    best[end] = best[end].min(best[start] + 1)
                }
            }
        }
        best
    }
}
fn parse(s: &str) -> String {
    let mut o = String::new();
    let mut i = 0;
    while i < s.len() {
        let r = &s[i..];
        if r.starts_with("⟨bow⟩") {
            o.push(BOW);
            i += 9
        } else if r.starts_with("⟨eow⟩") {
            o.push(EOW);
            i += 9
        } else if r.starts_with("⟨shift⟩") {
            o.push(SHIFT);
            i += 11
        } else if r.starts_with("⟨caps⟩") {
            o.push(CAPS);
            i += 10
        } else {
            let c = r.chars().next().unwrap();
            o.push(c);
            i += c.len_utf8()
        }
    }
    o
}
fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[derive(Clone, Copy)]
struct Mark {
    c: char,
    end: usize,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Class {
    Word,
    Hard,
    Digit,
    Punct,
    Space,
    Killer,
    Stray,
}

fn normalize(text: &str) -> Vec<Mark> {
    let mut raw = Vec::new();
    for (i, c) in text.char_indices() {
        if matches!(c,'\u{1}'..='\u{8}'|'\u{b}'..='\u{1f}'|'\u{7f}'..='\u{9f}')
            || ('\u{e000}'..='\u{f8ff}').contains(&c)
        {
            continue;
        }
        let c = match c {
            '\0'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}' => ' ',
            _ => c,
        };
        raw.push((c, i + c.len_utf8()));
    }
    // NFC composes within canonical combining sequences. Normalize each sequence
    // separately and attach every produced scalar to the sequence's source end;
    // this keeps offsets monotonic and on original UTF-8 boundaries even when a
    // decomposed sequence contracts to one scalar.
    let mut out = Vec::new();
    let mut cluster: Vec<(char, usize)> = Vec::new();
    let flush = |cluster: &mut Vec<(char, usize)>, out: &mut Vec<Mark>| {
        if let Some(end) = cluster.last().map(|item| item.1) {
            let source: String = cluster.iter().map(|item| item.0).collect();
            out.extend(source.nfc().map(|c| Mark { c, end }));
            cluster.clear();
        }
    };
    for item in raw {
        if canonical_combining_class(item.0) == 0 && !cluster.is_empty() {
            let before: String = cluster.iter().map(|part| part.0).collect();
            let mut with_next = before.clone();
            with_next.push(item.0);
            // Adjacent Hangul Jamo all have ccc=0 but compose under NFC. Keep
            // them in one source span when adding this scalar reduces the
            // normalized scalar count; otherwise a new starter opens a cluster.
            if with_next.nfc().count() >= before.nfc().count() + 1 {
                flush(&mut cluster, &mut out);
            }
        }
        cluster.push(item);
    }
    flush(&mut cluster, &mut out);
    // NFC deliberately does not compose Thai SARA AM's compatibility decomposition.
    let mut i = 0;
    while i + 1 < out.len() {
        if out[i].c == '\u{e4d}' && out[i + 1].c == '\u{e32}' {
            let end = out[i + 1].end;
            out.splice(i..i + 2, [Mark { c: '\u{e33}', end }]);
        } else {
            i += 1;
        }
    }
    out
}

fn marked(text: &str, family: Family) -> Vec<Mark> {
    let raw_head_space = text.starts_with(' ');
    let mut cs = normalize(text);
    if family == Family::V4_7 {
        while cs.last().is_some_and(|m| m.c == '\n') {
            cs.pop();
        }
    } else {
        // v5 absorbs raw trailing ASCII whitespace before folded spaces can join it.
        let stripped = text
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_whitespace())
            .count();
        for _ in 0..stripped {
            cs.pop();
        }
    }
    if family == Family::V4_7
        && raw_head_space
        && cs.first().is_some_and(|m| m.c == ' ')
        && cs.get(1).is_none_or(|m| m.c != ' ')
    {
        cs.remove(0);
    }

    let mut runs: Vec<(Class, Vec<Mark>)> = Vec::new();
    for m in cs {
        let mut class = classify(m.c);
        if class == Class::Word
            && stray_mark(m.c)
            && runs
                .last()
                .is_none_or(|r| !matches!(r.0, Class::Word | Class::Stray))
        {
            class = Class::Stray;
        }
        if runs.last().is_some_and(|r| r.0 == class) {
            runs.last_mut().unwrap().1.push(m);
        } else {
            runs.push((class, vec![m]));
        }
    }
    // HARD is our storage class, not a pretoken class: split letters, numbers, and punctuation.
    let mut split = Vec::new();
    for (cl, body) in runs {
        if cl != Class::Hard || body.len() == 1 {
            split.push((cl, body));
            continue;
        }
        for m in body {
            let k = hard_kind(m.c);
            if split.last().is_some_and(|(c, b): &(Class, Vec<Mark>)| {
                *c == Class::Hard && hard_kind(b[0].c) == k
            }) {
                split.last_mut().unwrap().1.push(m)
            } else {
                split.push((Class::Hard, vec![m]));
            }
        }
    }
    runs = split;
    if runs.is_empty() {
        return if family == Family::V4_7 {
            vec![Mark { c: BOW, end: 0 }]
        } else {
            vec![]
        };
    }

    let border_space = |i: usize, left: bool| -> bool {
        if left {
            if i == 0 {
                return family == Family::V4_7;
            }
            runs[i - 1].0 == Class::Space && runs[i - 1].1.last().is_some_and(|m| m.c == ' ')
        } else {
            runs.get(i + 1).is_some_and(|r| {
                r.0 == Class::Space
                    && r.1.first().is_some_and(|m| m.c == ' ')
                    && r.1.get(1).is_none_or(|m| m.c != ' ')
            })
        }
    };
    let opens_word = |i: usize| {
        runs[i].0 == Class::Punct
            && body_string(&runs[i].1) == "'"
            && runs
                .get(i + 1)
                .is_some_and(|r| matches!(r.0, Class::Word | Class::Stray))
    };
    let first_own = !opens_word(0)
        && (matches!(runs[0].0, Class::Word | Class::Punct | Class::Stray)
            || hard_bow(&runs[0].1)
            || digit_bow(&runs[0].1)
            || runs[0].0 == Class::Space && runs[0].1[0].c == ' ');
    let mut out = Vec::new();
    if family == Family::V4_7 && !first_own {
        push(
            &mut out,
            if opens_word(0) { ' ' } else { BOW },
            runs[0].1[0].end,
        );
    }
    for i in 0..runs.len() {
        let (cl, body) = &runs[i];
        let end = body.last().unwrap().end;
        match cl {
            Class::Word => {
                let fused = i > 0 && runs[i - 1].0 == Class::Stray;
                let contraction = i > 0
                    && runs[i - 1].0 == Class::Punct
                    && body_string(&runs[i - 1].1) == "'"
                    && matches!(
                        body_string(body).as_str(),
                        "s" | "t" | "d" | "m" | "ll" | "re" | "ve"
                    )
                    && (i < 2 || !takes_right_border(&runs[i - 2]));
                let (marker, lower) = mark_case(body, family, fused);
                if let Some(c) = marker {
                    push(&mut out, c, body[0].end);
                }
                if !fused && !contraction {
                    push(&mut out, BOW, body[0].end);
                }
                out.extend(lower);
                push(&mut out, EOW, end);
            }
            Class::Stray => {
                push(&mut out, BOW, body[0].end);
                out.extend(body.iter().copied());
                if runs.get(i + 1).is_none_or(|r| r.0 != Class::Word) {
                    push(&mut out, EOW, end);
                }
            }
            Class::Killer => {
                if border_space(i, true) && (body[0].c as u32) < 0x10000 {
                    push(&mut out, BOW, body[0].end)
                }
                out.extend(body.iter().copied());
                if border_space(i, false) && (body.last().unwrap().c as u32) < 0x10000 {
                    push(&mut out, EOW, end)
                }
            }
            Class::Punct => {
                if border_space(i, true) && !opens_word(i) {
                    push(&mut out, BOW, body[0].end)
                }
                out.extend(body.iter().copied());
                if border_space(i, false) {
                    push(&mut out, EOW, end)
                }
            }
            Class::Hard if hard_bow(body) || hard_eow(body) => {
                if border_space(i, true) && hard_bow(body) {
                    push(&mut out, BOW, body[0].end)
                }
                out.extend(body.iter().copied());
                if border_space(i, false) && hard_eow(body) {
                    push(&mut out, EOW, end)
                }
            }
            Class::Digit | Class::Hard if digit_run(body) => {
                if border_space(i, true) && digit_bow(body) {
                    push(&mut out, BOW, body[0].end)
                }
                out.extend(body.iter().copied());
                if border_space(i, false) && digit_eow(body) {
                    push(&mut out, EOW, end)
                }
            }
            _ => out.extend(body.iter().copied()),
        }
    }
    // eow + one seam space + case markers + bow => eow + case markers + bow
    let mut joined = Vec::new();
    let mut i = 0;
    while i < out.len() {
        if out[i].c == EOW && out.get(i + 1).is_some_and(|m| m.c == ' ') {
            let mut j = i + 2;
            while out.get(j).is_some_and(|m| matches!(m.c, SHIFT | CAPS)) {
                j += 1
            }
            if out.get(j).is_some_and(|m| m.c == BOW) {
                joined.push(out[i]);
                joined.extend_from_slice(&out[i + 2..=j]);
                i = j + 1;
                continue;
            }
        }
        joined.push(out[i]);
        i += 1;
    }
    joined
}
fn push(v: &mut Vec<Mark>, c: char, end: usize) {
    v.push(Mark { c, end })
}
fn body_string(v: &[Mark]) -> String {
    v.iter().map(|m| m.c).collect()
}

fn mark_case(body: &[Mark], _family: Family, head_mark: bool) -> (Option<char>, Vec<Mark>) {
    if head_mark {
        return (None, body.to_vec());
    }
    let s = body_string(body);
    if s.contains('ẞ') {
        return (None, body.to_vec());
    }
    let marker = if body[0].c.is_uppercase()
        && body[1..].iter().all(|m| !m.c.is_uppercase())
        && body[0].c.to_lowercase().ne([body[0].c])
    {
        Some(SHIFT)
    } else {
        None
    };
    if marker.is_none() {
        return (None, body.to_vec());
    }
    let mut lower = Vec::new();
    for m in body {
        for mut c in m.c.to_lowercase() {
            if marker == Some(CAPS) && c == 'ς' {
                c = 'σ'
            }
            lower.push(Mark { c, end: m.end })
        }
    }
    (marker, lower)
}

fn classify(c: char) -> Class {
    if is_killer(c) {
        return Class::Killer;
    }
    let o = c as u32;
    let g = get_general_category(c);
    if matches!(
        g,
        G::SpaceSeparator | G::LineSeparator | G::ParagraphSeparator
    ) || matches!(c, '\t' | '\n' | '\r' | '\x0c' | '\x0b')
    {
        return Class::Space;
    }
    if matches!(o,0x16ee..=0x16f0|0x2160..=0x2188|0x24b6..=0x24e9|0xa6e6..=0xa6ef) {
        return Class::Word;
    }
    if g == G::DecimalNumber && (o < 128 || matches!(o,0x660..=0x669|0x6f0..=0x6f9)) {
        return Class::Digit;
    }
    if o < 128
        && matches!(
            g,
            G::ConnectorPunctuation
                | G::DashPunctuation
                | G::OpenPunctuation
                | G::ClosePunctuation
                | G::InitialPunctuation
                | G::FinalPunctuation
                | G::OtherPunctuation
                | G::MathSymbol
                | G::CurrencySymbol
                | G::ModifierSymbol
                | G::OtherSymbol
        )
    {
        return Class::Punct;
    }
    if "—»«•°„–−£§€…√→（№†└│།·─═█".contains(c) {
        return Class::Punct;
    }
    if o == 0xcf3 {
        return Class::Word;
    }
    if is_letter_or_mark(c) && !hard_cp(o) {
        return Class::Word;
    }
    Class::Hard
}
fn is_letter_or_mark(c: char) -> bool {
    matches!(
        get_general_category(c),
        G::UppercaseLetter
            | G::LowercaseLetter
            | G::TitlecaseLetter
            | G::ModifierLetter
            | G::OtherLetter
            | G::NonspacingMark
            | G::SpacingMark
            | G::EnclosingMark
    )
}
fn hard_cp(o: u32) -> bool {
    o >= 0x10000
        || matches!(o,0x4e00..=0x9fff|0x3400..=0x4dbf|0xf900..=0xfaff|0xac00..=0xd7a3|0x3005..=0x3006|0x6dd..=0x6e0|0x6e9..=0x6ec)
}
fn in_ranges(o: u32, rs: &[(u32, u32)]) -> bool {
    rs.iter().any(|&(a, b)| a <= o && o <= b)
}
fn is_killer(c: char) -> bool {
    let o = c as u32;
    if canonical_combining_class(c) == 9 && c != 'ฺ' {
        return true;
    }
    const EXTRA: &str = "़়਼઼݂݄݆݈॒݀݁݃݅݇݉݊॑॓॔৾૽૾૿଼୕఼಼็่้๊๋์๎່້໊໋໌໎༹༘༙༵༷༾༿့࿆྆྇឴឵៉៊់៌៍៎៏័៑៓᬴꦳᩿᭬៝᩵᩶᩷᩸᩹᩺᩻᩼᭫᭭᭮᭯᭰᭱᭲᭳";
    if EXTRA.contains(c) || matches!(o,0x300..=0x344|0x346..=0x362) {
        return true;
    }
    const R: &[(u32, u32)] = &[
        (0x483, 0x489),
        (0x591, 0x5af),
        (0x658, 0x658),
        (0x6df, 0x6e0),
        (0x6ea, 0x6ec),
        (0x7eb, 0x7f3),
        (0x7fd, 0x7fd),
        (0x859, 0x85b),
        (0x898, 0x89f),
        (0x8ca, 0x8d3),
        (0x8e0, 0x8e1),
        (0x818, 0x819),
        (0x82d, 0x82d),
        (0x8ea, 0x8ef),
        (0x135d, 0x135f),
        (0x180b, 0x180d),
        (0x180f, 0x180f),
        (0x1939, 0x193b),
        (0x1ab0, 0x1abe),
        (0x1ac1, 0x1acb),
        (0x1cd0, 0x1ce8),
        (0x1ced, 0x1ced),
        (0x1cf4, 0x1cf4),
        (0x1cf8, 0x1cf9),
        (0x1be6, 0x1be6),
        (0x1c37, 0x1c37),
        (0x1cf7, 0x1cf7),
        (0x1dc0, 0x1dd2),
        (0x1df5, 0x1dff),
        (0x20d0, 0x20f0),
        (0xa66f, 0xa672),
        (0xa67c, 0xa67d),
        (0x2cef, 0x2cf1),
        (0x302a, 0x302f),
        (0x3099, 0x309a),
        (0xa6f0, 0xa6f1),
        (0xa8e0, 0xa8f1),
        (0xa92b, 0xa92d),
        (0xaabf, 0xaabf),
        (0xaac1, 0xaac1),
        (0xabec, 0xabec),
        (0xfe20, 0xfe2f),
    ];
    in_ranges(o, R)
}
fn stray_mark(c: char) -> bool {
    let o = c as u32;
    !matches!(o, 0x711 | 0x730..=0x73f) && canonical_combining_class(c) != 0 && !is_killer(c)
}
fn ideographic_punct(c: char) -> bool {
    matches!(c as u32, 0x3001..=0x303f)
        && matches!(
            get_general_category(c),
            G::ConnectorPunctuation
                | G::DashPunctuation
                | G::OpenPunctuation
                | G::ClosePunctuation
                | G::InitialPunctuation
                | G::FinalPunctuation
                | G::OtherPunctuation
        )
}
fn marks_punct(c: char) -> bool {
    let o = c as u32;
    if o >= 0x10000 {
        return false;
    }
    let g = get_general_category(c);
    (matches!(
        g,
        G::ConnectorPunctuation
            | G::DashPunctuation
            | G::OpenPunctuation
            | G::ClosePunctuation
            | G::InitialPunctuation
            | G::FinalPunctuation
            | G::OtherPunctuation
            | G::MathSymbol
            | G::CurrencySymbol
            | G::ModifierSymbol
            | G::OtherSymbol
            | G::Format
            | G::Unassigned
    )) && !ideographic_punct(c)
}
fn hard_kind(c: char) -> u8 {
    if marks_punct(c) {
        0
    } else if digit_border(c) {
        1
    } else {
        2
    }
}
fn hard_bow(b: &[Mark]) -> bool {
    b.first()
        .is_some_and(|m| matches!(m.c as u32, 0xfe00..=0xfe0f) || marks_punct(m.c))
}
fn hard_eow(b: &[Mark]) -> bool {
    b.last()
        .is_some_and(|m| matches!(m.c as u32, 0xfe00..=0xfe0f) || marks_punct(m.c))
}
fn digit_border(c: char) -> bool {
    let g = get_general_category(c);
    (c as u32) < 0x10000 && (g == G::OtherNumber || (g == G::DecimalNumber && !c.is_ascii()))
}
fn digit_run(b: &[Mark]) -> bool {
    !b.is_empty()
        && (b
            .iter()
            .all(|m| get_general_category(m.c) == G::DecimalNumber)
            || b.iter()
                .all(|m| get_general_category(m.c) == G::OtherNumber))
}
fn digit_bow(b: &[Mark]) -> bool {
    digit_run(b) && digit_border(b[0].c)
}
fn digit_eow(b: &[Mark]) -> bool {
    digit_run(b) && digit_border(b.last().unwrap().c)
}
fn takes_right_border(r: &(Class, Vec<Mark>)) -> bool {
    r.0 == Class::Punct
        || hard_eow(&r.1)
        || (matches!(r.0, Class::Digit | Class::Hard) && digit_eow(&r.1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct HeldOutRow {
        text: String,
        v47: u32,
        v5: u32,
    }

    #[test]
    fn held_out_corpora_match_pinned_ctok_exactly() {
        for line in include_str!("../tests/fixtures/held_out.jsonl").lines() {
            let row: HeldOutRow = serde_json::from_str(line).expect("valid held-out fixture");
            assert_eq!(count(&row.text, Family::V4_7), row.v47);
            assert_eq!(count(&row.text, Family::V5), row.v5);
        }
    }
    #[test]
    fn upstream_sanity() {
        assert_eq!(count("hello, world", Family::V4_7), 15);
        assert_eq!(count("hello, world", Family::V5), 10)
    }
    #[test]
    fn cut_boundaries() {
        let s = "hello 世界";
        for b in 0..30 {
            let c = cut(s, b, Family::V4_7);
            assert!(s.is_char_boundary(c.byte_offset));
            assert!(c.tokens <= b)
        }
    }
    #[test]
    fn cut_offsets_follow_repeated_and_composed_source_text() {
        assert_eq!(cut("aaaa", u32::MAX, Family::V5).byte_offset, 4);
        let decomposed = "e\u{301}";
        assert_eq!(
            cut(decomposed, u32::MAX, Family::V5).byte_offset,
            decomposed.len()
        );
        let hangul = "\u{1100}\u{1161}";
        assert_eq!(
            normalize(hangul)
                .iter()
                .map(|mark| mark.c)
                .collect::<String>(),
            "가"
        );
        assert_eq!(cut(hangul, u32::MAX, Family::V5).byte_offset, hangul.len());
    }
    #[test]
    fn full_cut() {
        let s = "hello, world";
        let c = cut(s, u32::MAX, Family::V4_7);
        assert_eq!(c.byte_offset, s.len());
        assert_eq!(c.tokens, count(s, Family::V4_7) - 11)
    }
    #[test]
    fn empty_cut() {
        assert_eq!(
            cut("hello", 0, Family::V5),
            Cut {
                byte_offset: 0,
                tokens: 0
            }
        )
    }
}
