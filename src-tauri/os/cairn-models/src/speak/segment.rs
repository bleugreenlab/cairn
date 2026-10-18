//! The shared segmenter: cuts a text stream into whole requests for a
//! provider that cannot take input as it arrives.
//!
//! Latency and prosody pull in opposite directions. The first segment is cut
//! eagerly, so the first audio starts soon: at the first sentence end, or at a
//! clause boundary (`, ; : —`) once [`FIRST_CLAUSE`] characters are buffered.
//! Later segments are cut at sentence ends, or at a clause boundary once
//! [`LATER_CLAUSE`] characters are buffered, and hard-split at the last
//! whitespace before [`HARD_SPLIT`] characters, well under OpenAI's 4096.
//!
//! A sentence end is `.`, `!`, `?` or `…` (with any closing quotes or
//! brackets) followed by whitespace and then an uppercase letter, a quote, a
//! digit or an opening bracket. So a boundary is confirmed one token late,
//! never early, and decimals (`3.5`), lowercase continuations, and a small
//! list of abbreviations (`Mr.`, `Dr.`, `vs.`, `e.g.` ...) are not ends. A
//! clause boundary likewise needs the whitespace after it, so `3,000` stays
//! whole. [`Segmenter::flush`] and [`Segmenter::finish`] emit whatever is
//! buffered.

/// Characters the first segment needs before a clause boundary cuts it.
pub const FIRST_CLAUSE: usize = 24;
/// Characters a later segment needs before a clause boundary cuts it.
pub const LATER_CLAUSE: usize = 160;
/// The longest a segment may be; one without a boundary inside it is split at
/// its last whitespace.
pub const HARD_SPLIT: usize = 400;
/// Characters past [`HARD_SPLIT`] that must arrive before a hard split
/// mid-stream, so the token confirming a sentence end near the limit is seen.
const LOOKAHEAD: usize = 16;

/// Words whose trailing period does not end a sentence, lowercased.
const ABBREVIATIONS: &[&str] = &[
    "mr", "mrs", "ms", "dr", "st", "vs", "e.g", "i.e", "jr", "sr", "prof",
];

const SENTENCE_ENDS: &[char] = &['.', '!', '?', '…'];
const CLOSERS: &[char] = &['"', '\'', '”', '’', ')', ']', '»'];
const CLAUSES: &[char] = &[',', ';', ':'];
const DASH: char = '—';

/// Cuts pushed text into segments. Pure: no I/O, no clocks.
#[derive(Debug, Default)]
pub struct Segmenter {
    buffer: String,
    emitted: usize,
}

impl Segmenter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `text` and return every segment it completes, in order.
    pub fn push(&mut self, text: &str) -> Vec<String> {
        self.buffer.push_str(text);
        self.cut_all(false)
    }

    /// Emit everything buffered now: as one segment, or several when it is
    /// longer than [`HARD_SPLIT`].
    pub fn flush(&mut self) -> Vec<String> {
        let mut segments = self.cut_all(true);
        let rest = std::mem::take(&mut self.buffer);
        segments.extend(self.emit(&rest));
        segments
    }

    /// The input has ended: emit what remains.
    pub fn finish(&mut self) -> Vec<String> {
        self.flush()
    }

    fn cut_all(&mut self, finishing: bool) -> Vec<String> {
        let mut segments = Vec::new();
        while let Some(cut) = self.next_cut(finishing) {
            let rest = self.buffer.split_off(cut);
            let segment = std::mem::replace(&mut self.buffer, rest);
            self.buffer = self.buffer.trim_start().to_string();
            segments.extend(self.emit(&segment));
        }
        segments
    }

    fn emit(&mut self, segment: &str) -> Option<String> {
        let segment = segment.trim();
        if segment.is_empty() {
            return None;
        }
        self.emitted += 1;
        Some(segment.to_string())
    }

    /// Where the next segment ends (a byte index into the buffer), if the
    /// buffer holds a complete one.
    ///
    /// Only boundaries inside the first [`HARD_SPLIT`] characters count, so
    /// no segment is ever longer than that, however the text was chunked when
    /// it arrived. Without one, the window is split at its last whitespace:
    /// mid-stream once [`LOOKAHEAD`] characters past it have arrived (so a
    /// sentence end near the limit can still be confirmed by what follows,
    /// whatever the chunking), and when `finishing` as soon as it overflows.
    fn next_cut(&self, finishing: bool) -> Option<usize> {
        let clause_floor = if self.emitted == 0 {
            FIRST_CLAUSE
        } else {
            LATER_CLAUSE
        };
        let text = self.buffer.as_str();
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        let leading = chars.iter().take_while(|(_, c)| c.is_whitespace()).count();
        let end_of = |index: usize| chars.get(index).map_or(text.len(), |(byte, _)| *byte);
        let window = leading + HARD_SPLIT;
        for (position, &(_, c)) in chars.iter().enumerate().take(window) {
            let length = position + 1 - leading.min(position + 1);
            if SENTENCE_ENDS.contains(&c) {
                let mut after = position + 1;
                while chars.get(after).is_some_and(|(_, c)| CLOSERS.contains(c)) {
                    after += 1;
                }
                if after <= window && self.confirms_sentence(&chars, position, after) {
                    return Some(end_of(after));
                }
            } else if CLAUSES.contains(&c) && length >= clause_floor {
                if chars
                    .get(position + 1)
                    .is_some_and(|(_, c)| c.is_whitespace())
                {
                    return Some(end_of(position + 1));
                }
            } else if c == DASH && length >= clause_floor && position + 1 < chars.len() {
                return Some(end_of(position + 1));
            }
        }
        let length = chars.len().saturating_sub(leading);
        let overflowing = if finishing {
            length > HARD_SPLIT
        } else {
            length >= HARD_SPLIT + LOOKAHEAD
        };
        if overflowing {
            let split = chars[..window]
                .iter()
                .rposition(|(_, c)| c.is_whitespace())
                .filter(|&index| index > leading)
                .unwrap_or(window);
            return Some(end_of(split));
        }
        None
    }

    /// Whether the sentence-ending character at `position` (closers running
    /// up to `after`) really ends a sentence, judged by what follows.
    fn confirms_sentence(&self, chars: &[(usize, char)], position: usize, after: usize) -> bool {
        if !chars.get(after).is_some_and(|(_, c)| c.is_whitespace()) {
            return false;
        }
        let Some(&(_, next)) = chars[after..].iter().find(|(_, c)| !c.is_whitespace()) else {
            // Only whitespace so far: wait for the next token.
            return false;
        };
        let opens = next.is_uppercase()
            || next.is_ascii_digit()
            || matches!(next, '"' | '\'' | '“' | '‘' | '(' | '[' | '«');
        if !opens {
            return false;
        }
        if chars[position].1 == '.' {
            let word: String = chars[..position]
                .iter()
                .rev()
                .take_while(|(_, c)| !c.is_whitespace())
                .map(|(_, c)| *c)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
                .to_lowercase();
            let word = word.trim_start_matches(['(', '"', '\'', '“', '‘']);
            if ABBREVIATIONS.contains(&word) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(pieces: &[&str]) -> (Vec<String>, Segmenter) {
        let mut segmenter = Segmenter::new();
        let mut out = Vec::new();
        for piece in pieces {
            out.extend(segmenter.push(piece));
        }
        (out, segmenter)
    }

    #[test]
    fn a_sentence_end_is_confirmed_one_token_late() {
        let (out, mut segmenter) = feed(&["I cast Lightning Bolt."]);
        assert!(out.is_empty(), "the next token has not arrived");
        assert!(
            segmenter.push(" ").is_empty(),
            "whitespace alone confirms nothing"
        );
        assert_eq!(segmenter.push("It"), vec!["I cast Lightning Bolt."]);
        assert_eq!(segmenter.finish(), vec!["It"]);
    }

    #[test]
    fn the_first_segment_is_eager_and_later_ones_wait_longer() {
        // Short first sentence: cut at once.
        let (out, _) = feed(&["Hi! Then a long clause, with commas, that keeps going"]);
        assert_eq!(out, vec!["Hi!"]);
        // First clause boundary past 24 characters cuts the first segment.
        let (out, mut segmenter) = feed(&["Well, I think this attacker, if unblocked, deals five"]);
        assert_eq!(out, vec!["Well, I think this attacker,"]);
        // A later segment ignores clause boundaries under 160 characters.
        assert!(segmenter.push(" damage, which is lethal, ").is_empty());
        assert_eq!(
            segmenter.push("so I pass. Next"),
            vec!["if unblocked, deals five damage, which is lethal, so I pass."]
        );
    }

    #[test]
    fn a_later_clause_boundary_cuts_past_160_characters() {
        let mut segmenter = Segmenter::new();
        assert_eq!(segmenter.push("Go. A"), vec!["Go."]);
        let long = "b".repeat(170);
        assert!(segmenter.push(&long).is_empty());
        let out = segmenter.push("; and then more");
        assert_eq!(out, vec![format!("A{long};")]);
    }

    #[test]
    fn decimals_abbreviations_and_lowercase_are_not_ends() {
        let (out, _) =
            feed(&["It costs 3.5 mana. Mr. Smith vs. Dr. Who, e.g. Today. ok then. Done"]);
        assert_eq!(
            out,
            vec![
                "It costs 3.5 mana.",
                "Mr. Smith vs. Dr. Who, e.g. Today. ok then."
            ]
        );
        // `e.g. Today` is not a cut, and `Today. ok` continues in lowercase.
    }

    #[test]
    fn closing_quotes_ride_with_their_sentence_and_quotes_open_the_next() {
        let (out, _) = feed(&["He said \"Pass.\" \"Fine,\" I replied. 4 cards"]);
        assert_eq!(out, vec!["He said \"Pass.\"", "\"Fine,\" I replied."]);
    }

    #[test]
    fn a_clause_boundary_needs_whitespace_after_it() {
        let (out, _) = feed(&["The total is 3,000,000 life points, apparently"]);
        assert_eq!(out, vec!["The total is 3,000,000 life points,"]);
        let (out, _) = feed(&["This is twenty-five chars — and then"]);
        assert_eq!(out, vec!["This is twenty-five chars —"]);
    }

    #[test]
    fn an_unbroken_run_is_hard_split_at_its_last_whitespace() {
        let words = "word ".repeat(100);
        let (out, segmenter) = feed(&[&words]);
        assert_eq!(out.len(), 1);
        assert!(out[0].chars().count() <= HARD_SPLIT);
        assert!(out[0].ends_with("word"));
        assert!(!segmenter.buffer.is_empty());
        let solid = "x".repeat(450);
        let (out, _) = feed(&[&solid]);
        assert_eq!(out[0].chars().count(), HARD_SPLIT);
    }

    #[test]
    fn flush_and_finish_emit_whatever_is_buffered() {
        let mut segmenter = Segmenter::new();
        assert!(segmenter.push("half a tho").is_empty());
        assert_eq!(segmenter.flush(), vec!["half a tho"]);
        assert!(segmenter.flush().is_empty());
        segmenter.push("  ");
        assert!(segmenter.finish().is_empty());
    }

    /// Every segment `text` yields when it arrives in pieces of `size`
    /// characters (or whole, for 0), input end included.
    fn segments(text: &str, size: usize) -> Vec<String> {
        let mut segmenter = Segmenter::new();
        let mut out = Vec::new();
        if size == 0 {
            out.extend(segmenter.push(text));
        } else {
            let chars: Vec<char> = text.chars().collect();
            for piece in chars.chunks(size) {
                out.extend(segmenter.push(&piece.iter().collect::<String>()));
            }
        }
        out.extend(segmenter.finish());
        out
    }

    #[test]
    fn no_segment_exceeds_the_limit_however_the_text_arrives() {
        // Punctuation far past the limit must not stretch a segment to it.
        let late_stop = format!("{}. Next", "word ".repeat(1000));
        let unbroken = "x".repeat(1000);
        let mixed = format!(
            "Opening line. {} Then, finally, the end. {}",
            "lorem ipsum ".repeat(60),
            "tail ".repeat(120)
        );
        for text in [&late_stop, &unbroken, &mixed] {
            let whole = segments(text, 0);
            for size in [1, 3, 7, 64, 401] {
                let pieces = segments(text, size);
                assert_eq!(pieces, whole, "chunks of {size} differ from the whole");
            }
            assert!(
                whole
                    .iter()
                    .all(|segment| segment.chars().count() <= HARD_SPLIT),
                "{:?}",
                whole.iter().map(|s| s.chars().count()).collect::<Vec<_>>()
            );
            let rejoined: String = whole.concat();
            let original: String = text.split_whitespace().collect();
            assert_eq!(rejoined.split_whitespace().collect::<String>(), original);
        }
        // A flush of an overlong buffer splits it too.
        let mut segmenter = Segmenter::new();
        segmenter.push(&"y ".repeat(300));
        assert!(segmenter
            .flush()
            .iter()
            .all(|segment| segment.chars().count() <= HARD_SPLIT));
    }
}
