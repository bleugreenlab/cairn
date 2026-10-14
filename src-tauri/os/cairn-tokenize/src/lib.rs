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

/// Scraped text can contain the noncharacters this port uses as its structural
/// markers. Read as themselves they would tile as marker atoms, so a literal
/// U+FDD0..U+FDD4 is moved onto a private-use sentinel, out of reach of every
/// vocabulary edge. Input private use is stripped before this runs, so the
/// sentinels cannot collide with text; the byte floor and the classification
/// predicates that read Unicode data restore the original codepoint, and the
/// substitution is in place, so source offsets are untouched.
const LITERAL_MARKERS: std::ops::RangeInclusive<char> = '\u{fdd0}'..='\u{fdd4}';
const LITERAL_SENTINELS: std::ops::RangeInclusive<char> = '\u{e000}'..='\u{e004}';

fn escape_literal(c: char) -> char {
    if !LITERAL_MARKERS.contains(&c) {
        return c;
    }
    char::from_u32(c as u32 - *LITERAL_MARKERS.start() as u32 + *LITERAL_SENTINELS.start() as u32)
        .expect("sentinel is a private-use scalar")
}

fn literal(c: char) -> char {
    if !LITERAL_SENTINELS.contains(&c) {
        return c;
    }
    char::from_u32(c as u32 - *LITERAL_SENTINELS.start() as u32 + *LITERAL_MARKERS.start() as u32)
        .expect("marker is a noncharacter scalar")
}

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
            let mut cs = p.chars();
            // Marker atoms are deliberately not unit characters: a literal marker
            // codepoint restored from its sentinel must reach the byte floor.
            if let (Some(c), None) = (cs.next(), cs.next()) {
                if !matches!(c, BOW | EOW | SHIFT) {
                    units.insert(c);
                }
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
        if matches!(c, BOW | EOW | SHIFT) {
            return 1;
        }
        let c = literal(c);
        if self.units.contains(&c) {
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

/// The whitespace v5's frame absorbs off the end of the content.
const FRAME_STRIP: [char; 6] = [' ', '\t', '\n', '\r', '\u{c}', '\u{b}'];

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

/// The C0 and C1 controls the model reads as nothing at all. TAB and LF keep
/// their own cost, and NUL folds to a space rather than vanishing.
fn is_stripped_control(c: char) -> bool {
    matches!(c,'\u{1}'..='\u{8}'|'\u{b}'..='\u{1f}'|'\u{7f}'..='\u{9f}')
}
/// The space separators the model reads exactly as U+0020. U+3000, TAB and LF
/// are deliberately absent: each has a cost of its own.
fn is_funny_space(c: char) -> bool {
    matches!(
        c,
        '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}'
    )
}

fn normalize(text: &str) -> Vec<Mark> {
    let raw: Vec<(char, usize)> = text
        .char_indices()
        .map(|(i, c)| (c, i + c.len_utf8()))
        .collect();
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
            if with_next.nfc().count() > before.nfc().count() {
                flush(&mut cluster, &mut out);
            }
        }
        cluster.push(item);
    }
    flush(&mut cluster, &mut out);
    // Everything below runs on normalized text, in the order the oracle reads it.
    // Removing a control character before NFC would let it compose the two
    // scalars it stands between, which the oracle keeps apart.
    out.retain(|m| !is_stripped_control(m.c));
    for m in out.iter_mut() {
        if m.c == '\0' {
            m.c = ' ';
        }
    }
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
    // Private use is read as nothing, and its two neighbours join into one word.
    out.retain(|m| !('\u{e000}'..='\u{f8ff}').contains(&m.c));
    for m in out.iter_mut() {
        if is_funny_space(m.c) {
            m.c = ' ';
        }
        // Input private use is gone by now, so the sentinels are free for the
        // marker codepoints the text supplied literally.
        m.c = escape_literal(m.c);
    }
    out
}

fn marked(text: &str, family: Family) -> Vec<Mark> {
    let raw_head_space = text.starts_with(' ');
    // v5's frame absorbs the raw trailing whitespace run, so that run comes off
    // before normalization: a folded no-break space is not free at the end, and a
    // stripped control character inside the run must not take a neighbour's
    // place. v4.7's frame absorbs a normalized trailing newline run instead.
    let mut cs = if family == Family::V4_7 {
        normalize(text)
    } else {
        normalize(text.trim_end_matches(FRAME_STRIP))
    };
    if family == Family::V4_7 {
        while cs.last().is_some_and(|m| m.c == '\n') {
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
            // A variation selector never opens a sub-run: it rides its base's, or
            // a symbol would sever at the selector and lose the marker it is due.
            if split.last().is_some_and(|(c, b): &(Class, Vec<Mark>)| {
                *c == Class::Hard && (is_selector(m.c) || hard_kind(b[0].c) == k)
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
                let (marker, lower) = mark_case(body, fused);
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
            while out.get(j).is_some_and(|m| m.c == SHIFT) {
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

/// Case pairs the measured models know that the host Unicode tables need not:
/// pinned here so a toolchain upgrade cannot move a count.
const NEW_CASE_PAIRS: [(char, char); 2] = [('\u{1c89}', '\u{1c8a}'), ('\u{a7cb}', '\u{264}')];

/// Uncased for the measured models, though Unicode calls `ϴ` a capital: spelled
/// bare it costs one token less than ⟨shift⟩ plus `θ` would. This is not ẞ's
/// blanket block on the whole span — `Θϴ` still takes its ⟨shift⟩ — so the
/// character leaves the cased set instead.
const UNCASED: char = '\u{3f4}';

fn new_case_pair(c: char) -> Option<char> {
    NEW_CASE_PAIRS
        .iter()
        .find(|(upper, _)| *upper == c)
        .map(|(_, lower)| *lower)
}
fn new_cased(c: char) -> bool {
    NEW_CASE_PAIRS
        .iter()
        .any(|(upper, lower)| *upper == c || *lower == c)
}
/// The lowered spelling of the characters the host tables must not decide:
/// `None` leaves the character to `char::to_lowercase`.
fn pinned_lower(c: char) -> Option<char> {
    if c == UNCASED {
        Some(c)
    } else {
        new_case_pair(c)
    }
}
fn is_upper(c: char) -> bool {
    c != UNCASED && (new_case_pair(c).is_some() || c.is_uppercase())
}
/// Whether lowering the character produces a different spelling. A marker in
/// front of an unchanged body would over-count.
fn lowers(c: char) -> bool {
    match pinned_lower(c) {
        Some(lower) => lower != c,
        None => c.to_lowercase().ne([c]),
    }
}

/// LATIN CAPITAL LETTER I WITH DOT ABOVE, whose lowered spelling is two scalars.
/// It is transparent to the title-case test and stays literal in the lowered
/// body, so it blocks a marker only as the span's own head.
const DOTTED: char = 'İ';

fn mark_case(body: &[Mark], head_mark: bool) -> (Option<char>, Vec<Mark>) {
    // ẞ blocks a marker from anywhere in the span; a caseless or unlowerable
    // head blocks it too, and an unattached mark that opened the word has taken
    // the head position already.
    if head_mark || body.iter().any(|m| m.c == 'ẞ') {
        return (None, body.to_vec());
    }
    let head = body[0].c;
    let tail_is_lower = body[1..].iter().all(|m| m.c == DOTTED || !is_upper(m.c));
    let fires = if body.iter().any(|m| m.c == DOTTED) {
        head != DOTTED && is_upper(head) && tail_is_lower
    } else {
        is_upper(head) && tail_is_lower && lowers(head)
    };
    if !fires {
        return (None, body.to_vec());
    }
    let mut lower = Vec::new();
    for (i, m) in body.iter().enumerate() {
        match pinned_lower(m.c) {
            _ if i > 0 && m.c == DOTTED => lower.push(*m),
            Some(c) => lower.push(Mark { c, end: m.end }),
            None => lower.extend(m.c.to_lowercase().map(|c| Mark { c, end: m.end })),
        }
    }
    (Some(SHIFT), lower)
}

fn classify(c: char) -> Class {
    if is_killer(c) {
        return Class::Killer;
    }
    if new_cased(c) {
        return Class::Word;
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
    // Variation selectors are gc=Mn but take no word model: each costs one more
    // than its base, where ⟨bow⟩ and ⟨eow⟩ flanks would read three more.
    if is_selector(c) {
        return Class::Hard;
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
    // The ideographic iteration and closing marks, and the kana repeat, iteration
    // and masu marks, are letters by category but continue the run they follow.
    // The katakana prolonged sound mark and small `ヶ` measured exact as wordy and
    // are deliberately absent.
    o >= 0x10000
        || matches!(o,0x4e00..=0x9fff|0x3400..=0x4dbf|0xf900..=0xfaff|0xac00..=0xd7a3|0x3005..=0x3006|0x3031..=0x3035|0x303b..=0x303c|0x6dd..=0x6e0|0x6e9..=0x6ec)
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
/// The category-`So` members of CJK Symbols and Punctuation. The category test
/// below misses them, and they read the way the block's punctuation does.
const IDEOGRAPHIC_SYMBOLS: &str = "〄〒〓〠〶〷〾〿";

fn ideographic_punct(c: char) -> bool {
    matches!(c as u32, 0x3001..=0x303f)
        && (matches!(
            get_general_category(c),
            G::ConnectorPunctuation
                | G::DashPunctuation
                | G::OpenPunctuation
                | G::ClosePunctuation
                | G::InitialPunctuation
                | G::FinalPunctuation
                | G::OtherPunctuation
        ) || IDEOGRAPHIC_SYMBOLS.contains(c))
}
fn marks_punct(c: char) -> bool {
    let c = literal(c);
    let o = c as u32;
    if o >= 0x10000 {
        return false;
    }
    // A separating mark is punct-kind whatever its category says: it claims the
    // border markers of the run it terminates.
    if is_killer(c) {
        return true;
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
fn is_selector(c: char) -> bool {
    matches!(c as u32, 0xfe00..=0xfe0f)
}
fn hard_bow(b: &[Mark]) -> bool {
    b.first()
        .is_some_and(|m| is_selector(m.c) || marks_punct(m.c))
}
fn hard_eow(b: &[Mark]) -> bool {
    b.last()
        .is_some_and(|m| is_selector(m.c) || marks_punct(m.c))
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

    /// Counts read from upstream ctok at the commit the README pins, as
    /// `(text, v4.7, v5)`. Each table is one classification boundary; a probe is
    /// framed where the marker it earns is visible in the count.
    type Oracle = &'static [(&'static str, u32, u32)];

    fn assert_oracle(table: Oracle) {
        for (text, v47, v5) in table {
            assert_eq!(count(text, Family::V4_7), *v47, "v4.7 {text:?}");
            assert_eq!(count(text, Family::V5), *v5, "v5 {text:?}");
        }
    }

    /// The CJK block's own symbols and its kana repeat, iteration and masu marks
    /// stay inside the run they sit in, where the word model would cost more.
    #[test]
    fn ideographic_symbols_and_iteration_marks_are_markerless() {
        assert_oracle(&[
            ("\u{3004}", 14, 8),
            ("\u{6587}\u{3004}\u{6587}", 16, 10),
            ("a \u{3004} b", 17, 12),
            ("\u{3012}", 14, 8),
            ("\u{6587}\u{3012}\u{6587}", 16, 10),
            ("a \u{3012} b", 17, 12),
            ("\u{3013}", 14, 8),
            ("\u{6587}\u{3013}\u{6587}", 16, 10),
            ("a \u{3013} b", 17, 12),
            ("\u{3020}", 14, 8),
            ("\u{6587}\u{3020}\u{6587}", 16, 10),
            ("a \u{3020} b", 17, 12),
            ("\u{3036}", 14, 8),
            ("\u{6587}\u{3036}\u{6587}", 16, 10),
            ("a \u{3036} b", 17, 12),
            ("\u{3037}", 14, 8),
            ("\u{6587}\u{3037}\u{6587}", 16, 10),
            ("a \u{3037} b", 17, 12),
            ("\u{303e}", 14, 8),
            ("\u{6587}\u{303e}\u{6587}", 16, 10),
            ("a \u{303e} b", 17, 12),
            ("\u{303f}", 14, 8),
            ("\u{6587}\u{303f}\u{6587}", 16, 10),
            ("a \u{303f} b", 17, 12),
            ("\u{3031}", 14, 8),
            ("\u{6587}\u{3031}\u{6587}", 16, 10),
            ("a \u{3031} b", 17, 12),
            ("\u{3032}", 14, 8),
            ("\u{6587}\u{3032}\u{6587}", 16, 10),
            ("a \u{3032} b", 17, 12),
            ("\u{3033}", 14, 8),
            ("\u{6587}\u{3033}\u{6587}", 16, 10),
            ("a \u{3033} b", 17, 12),
            ("\u{3034}", 14, 8),
            ("\u{6587}\u{3034}\u{6587}", 16, 10),
            ("a \u{3034} b", 17, 12),
            ("\u{3035}", 14, 8),
            ("\u{6587}\u{3035}\u{6587}", 16, 10),
            ("a \u{3035} b", 17, 12),
            ("\u{303b}", 14, 8),
            ("\u{6587}\u{303b}\u{6587}", 16, 10),
            ("a \u{303b} b", 17, 12),
            ("\u{303c}", 14, 8),
            ("\u{6587}\u{303c}\u{6587}", 16, 10),
            ("a \u{303c} b", 17, 12),
            // The neighbours measured the other way, which the rule must not take:
            // 々 〆 keep the ideographic reading, ー ヶ 〇 the wordy one.
            ("\u{3005}", 13, 7),
            ("\u{6587}\u{3005}\u{6587}", 15, 9),
            ("\u{3006}", 14, 8),
            ("\u{6587}\u{3006}\u{6587}", 16, 10),
            ("\u{30fc}", 13, 8),
            ("\u{6587}\u{30fc}\u{6587}", 16, 10),
            ("\u{30f6}", 15, 10),
            ("\u{6587}\u{30f6}\u{6587}", 18, 12),
            ("\u{3007}", 14, 8),
            ("\u{6587}\u{3007}\u{6587}", 16, 10),
        ]);
    }

    /// ϴ leaves the cased set, the Unicode 16 pairs are pinned into it, and İ
    /// stays literal in a lowered body.
    #[test]
    fn case_marking_follows_the_measured_cased_set() {
        assert_oracle(&[
            ("\u{3f4}", 15, 10),
            ("\u{398}", 14, 9),
            ("\u{3b8}", 13, 8),
            ("\u{398}\u{3f4}", 16, 11),
            ("\u{3f4}\u{398}", 17, 12),
            ("\u{3f4}a", 15, 10),
            ("a\u{3f4}", 15, 10),
            ("\u{3f4}\u{3f4}", 17, 12),
            ("a \u{3f4} b", 17, 12),
            ("\u{1c89}", 17, 12),
            ("\u{1c8a}", 16, 11),
            ("\u{a7cb}", 16, 11),
            ("\u{264}", 15, 10),
            ("\u{1c89}\u{1c8a}", 20, 15),
            ("\u{1c89}abc", 18, 13),
            ("\u{a7cb}\u{264}", 18, 13),
            ("\u{a7cb}abc", 17, 12),
            ("a \u{1c89} b", 19, 14),
            ("a \u{a7cb} b", 18, 13),
            ("\u{130}", 13, 8),
            ("\u{130}stanbul", 16, 11),
            ("a \u{130} b", 15, 10),
            ("A\u{130}a", 15, 10),
            ("\u{130}\u{130}", 14, 9),
            ("Istanbul", 16, 11),
        ]);
    }

    /// Text can contain the noncharacters this port marks with. Read as markers
    /// they would cost one token each; they cost their own bytes instead.
    #[test]
    fn literal_marker_codepoints_never_reach_the_vocabulary() {
        assert_oracle(&[
            ("\u{fdd0}", 15, 9),
            ("\u{fdd0}\u{fdd0}", 18, 12),
            ("a\u{fdd0}b", 16, 11),
            ("\u{fdd1}", 15, 9),
            ("\u{fdd1}\u{fdd1}", 18, 12),
            ("a\u{fdd1}b", 16, 11),
            ("\u{fdd2}", 15, 9),
            ("\u{fdd2}\u{fdd2}", 18, 12),
            ("a\u{fdd2}b", 16, 11),
            ("\u{fdd3}", 15, 9),
            ("\u{fdd3}\u{fdd3}", 18, 12),
            ("a\u{fdd3}b", 16, 11),
            ("\u{fdd4}", 15, 9),
            ("\u{fdd4}\u{fdd4}", 18, 12),
            ("a\u{fdd4}b", 16, 11),
        ]);
        // The same counts as content: three bytes each, never one marker atom.
        for c in '\u{fdd0}'..='\u{fdd4}' {
            let one = c.to_string();
            let two = format!("{c}{c}");
            let held = format!("a{c}b");
            for (family, single, double, between) in
                [(Family::V4_7, 4, 7, 5), (Family::V5, 3, 6, 5)]
            {
                let content = |text: &str| count(text, family) - family.overhead();
                assert_eq!(content(&one), single, "{c:?} alone");
                assert_eq!(content(&two), double, "{c:?} repeated");
                assert_eq!(content(&held), between, "{c:?} between letters");
            }
        }
    }

    /// Both populations that take a border marker without being punctuation by
    /// category: a mark that separates word runs, and a variation selector.
    #[test]
    fn border_markers_reach_separating_marks_and_selectors() {
        assert_oracle(&[
            ("\u{302a}", 14, 8),
            ("\u{302a} ", 16, 8),
            (" \u{302a}", 14, 10),
            ("\u{302a}\u{302a}", 16, 10),
            ("a\u{302a}b", 15, 10),
            ("\u{fe00}", 14, 8),
            ("\u{fe0f}", 13, 7),
            ("\u{2696}\u{fe0f}", 16, 10),
            ("a\u{fe0f}b", 14, 9),
            ("\u{ff1f}\u{fe0f}", 14, 8),
        ]);
    }

    /// The order the oracle reads its own rewrites in: a control character is
    /// removed after NFC, so it still blocks the composition it stands in; v5's
    /// frame absorbs the raw trailing whitespace run rather than what survives
    /// normalization.
    #[test]
    fn normalization_runs_in_the_measured_order() {
        assert_oracle(&[
            ("a\u{c}\u{301}", 13, 8),
            ("a\u{301}", 12, 7),
            ("a \t\r\n ", 16, 7),
            ("a\u{e4d}\u{e32}", 14, 9),
            ("a\u{e000}\u{301}", 13, 8),
        ]);
    }

    /// One probe for each piece first witnessed at the pinned commit, so an
    /// omission in the imported vocabulary reads as a count.
    #[test]
    fn pieces_first_witnessed_at_this_pin_are_present() {
        assert_oracle(&[
            ("ouou", 14, 9),
            ("verver", 13, 8),
            ("\u{e2d}\u{e07}\u{e2d}\u{e07}", 14, 9),
            ("f\u{e9}v", 13, 8),
            ("househ", 13, 8),
            ("kr\u{f3}l", 12, 7),
            ("k\u{f6}zs\u{e9}g", 12, 7),
            ("throug", 13, 8),
            ("\u{ed}r\u{f3}", 12, 7),
            ("\u{ff0d}", 12, 7),
            ("\u{fffd}", 12, 7),
            ("Popula\u{e7}\u{e3}o", 12, 7),
            ("Tov\u{e1}bbi", 12, 7),
            ("\u{c5}r", 12, 7),
            ("\u{421}\u{43e}\u{431}\u{44b}\u{442}\u{438}\u{44f}", 12, 7),
            ("\u{3042}\u{3042}", 14, 9),
            ("\u{304c}\u{304c}", 13, 8),
            ("././", 13, 7),
            ("\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}", 13, 7),
            ("\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}\u{b7}", 13, 7),
        ]);
    }

    /// The recurrence the reverse trie implements, spelled out over every
    /// segmentation. It holds the tiler still while the vocabulary moves.
    fn exhaustive(s: &[Mark]) -> u32 {
        if s.is_empty() {
            return 0;
        }
        (0..s.len())
            .filter_map(|start| {
                let segment: String = s[start..].iter().map(|m| m.c).collect();
                let cost = if trie_has(&model().trie, &segment) {
                    1
                } else if s.len() - start == 1 {
                    model().unit(s[start].c)
                } else {
                    return None;
                };
                Some(exhaustive(&s[..start]) + cost)
            })
            .min()
            .expect("the one-character floor always tiles")
    }

    #[test]
    fn the_reverse_trie_tiles_as_cheaply_as_exhaustive_search() {
        for text in [
            "hi",
            "a's",
            "Hello",
            "1,234",
            "\u{6587}\u{5b57}",
            "e\u{301}x",
            "\u{fdd0}a",
            "\u{3004}\u{3031}",
        ] {
            for family in [Family::V4_7, Family::V5] {
                let s = marked(text, family);
                assert_eq!(
                    model().costs(&s).last().copied().unwrap_or(0),
                    exhaustive(&s),
                    "{text:?}"
                );
            }
        }
    }

    #[test]
    fn cut_holds_source_boundaries_across_the_changed_classifications() {
        for text in [
            "\u{6587}\u{3004}\u{5b57}",
            "a\u{3031}b",
            "\u{398}\u{3f4}",
            "\u{1c89}abc",
            "a\u{fdd0}b",
            "\u{fdd4}\u{fdd4}",
            "\u{2696}\u{fe0f}!",
            "a\u{302a}b",
        ] {
            for family in [Family::V4_7, Family::V5] {
                let full = cut(text, u32::MAX, family);
                assert_eq!(full.byte_offset, text.len(), "{text:?} whole");
                for budget in 0..=count(text, family) {
                    let c = cut(text, budget, family);
                    assert!(text.is_char_boundary(c.byte_offset), "{text:?} @{budget}");
                    assert!(c.tokens <= budget, "{text:?} @{budget}");
                }
            }
        }
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
