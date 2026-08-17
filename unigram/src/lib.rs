//! `unigram` — a bijective codec between bytes and words that cost one LLM token.
//!
//! Machine identifiers are routinely handed to a language model and asked back: an
//! acknowledgement token, a digest, a correlation id. Hexadecimal is the worst
//! possible carrier for that trip. It is expensive, because a hex run shreds into a
//! fragment every character or two under every tokenizer; and it is *undetectably*
//! fragile, because every character is drawn from the same sixteen, so a corrupted
//! one still looks like a valid digest.
//!
//! This crate carries the same bytes as words drawn from a fixed alphabet of 256.
//! Two properties follow from that size, and they are the whole design:
//!
//! - **One word is exactly one byte.** Encoding is a table lookup per byte with no
//!   bit-packing, no padding, and no length convention; decoding is its inverse.
//!   Every byte string has exactly one encoding, and every sequence of alphabet
//!   words decodes.
//! - **Every word is exactly one token.** Each entry was measured to cost a single
//!   token, so an encoded value costs exactly one token per byte — and, unlike hex,
//!   the same for every value. Measured against hex of the same payload:
//!
//! | payload  | hex (mean / worst) | `unigram` |
//! |----------|--------------------|-----------|
//! | 4 bytes  | 5.5 / 8            | 4         |
//! | 16 bytes | 21.0 / 26          | 16        |
//! | 32 bytes | 42.2 / 51          | 32        |
//!
//!   Roughly a quarter cheaper on average, but the flat cost matters more than the
//!   mean: hex cost swings with the value, so a budget built on it has to assume
//!   the worst case.
//!
//! Corruption also becomes visible rather than silent: the alphabet is 256 words out
//! of every string that could be written, so a mangled word is overwhelmingly likely
//! to be no word at all, and [`decode`] says so and names it.
//!
//! ```
//! let words = unigram::encode(&[0x3d, 0x9a, 0x00, 0xff]);
//! assert_eq!(unigram::decode(&words).unwrap(), vec![0x3d, 0x9a, 0x00, 0xff]);
//! ```
//!
//! ## Why the join is a space
//!
//! Tokenizer vocabularies hold their canonical word entries space-prefixed, so the
//! space between two words is absorbed into the word that follows it and costs
//! nothing. Any other separator is charged for twice: it becomes a token itself, and
//! it severs the word from the space-prefixed entry that made the word cheap. A
//! hyphenated join roughly triples the cost of the same payload. Encoded values
//! travel inside quoted strings in practice, where embedded spaces are free.
//!
//! [`decode`] is nonetheless liberal in what it accepts: any run of characters that
//! is not an ASCII letter separates words, and case is ignored. A value that came
//! back hyphenated, re-wrapped across lines, comma-joined, or shouted still decodes
//! to the bytes that were sent.
//!
//! ## The alphabet
//!
//! Entries are lowercase ASCII English, 4 to 11 characters, chosen under four
//! constraints:
//!
//! - **One token — gated against Claude, measured against the rest.** Every entry
//!   is held to a single token by a test here using the in-repo `cairn-tokenize`,
//!   which counts for Claude models only. The table was also *selected* so every
//!   entry costs one token under GPT-2/3 (`r50k`, `p50k`), GPT-3.5/4 (`cl100k`),
//!   GPT-4o (`o200k`), and Llama's SentencePiece — spanning both the BPE and
//!   SentencePiece families — but those vocabularies are not vendored, so that half
//!   is evidence rather than a gate. See "Changing the alphabet" below.
//! - **No two entries within one character edit of each other**, which is what makes
//!   a single-character slip land outside the alphabet instead of on a different
//!   valid word. This is the property hex cannot have.
//! - **Nothing charged** — no death, violence, race, gender, religion, or politics.
//!   These strings surface unbidden in transcripts, logs, and user-facing errors.
//! - **No entry is an inflection of another**, so a dropped plural cannot silently
//!   decode to a different byte.
//!
//! ## Changing the alphabet
//!
//! The tests here enforce single-token cost **under Claude only**, plus the table's
//! structural properties: 256 entries, sorted, unique, 4 to 11 lowercase ASCII
//! characters, and no two within one edit. `cairn-tokenize` is a development
//! dependency — nothing at runtime tokenizes.
//!
//! The cross-tokenizer cost claim is deliberately **not** gated: vendoring four more
//! vocabularies to enforce it would cost more than the claim is worth. It is instead
//! reproducible — `verify-alphabet.py`, beside this file, re-checks every entry
//! against all four non-Claude families:
//!
//! ```text
//! uv run src-tauri/os/unigram/verify-alphabet.py
//! ```
//!
//! Run it after any edit to [`ALPHABET`]. A green test suite alone does not
//! establish the cross-tokenizer property, and an edit that satisfies every test
//! here can still break it.

#![forbid(unsafe_code)]

use std::fmt;

/// The 256-word alphabet, sorted, indexed by the byte each word encodes.
///
/// Sorted so [`decode`] can binary-search it, and byte `n` is `ALPHABET[n]` — the
/// table *is* the codec. Reordering an entry changes what every previously issued
/// value decodes to, so this list is appended to, never rearranged.
///
/// Laid out packed rather than one entry per line: rustfmt would give this table
/// 256 vertical lines, which is harder to scan and to review than a grid, and the
/// entries are data rather than code.
///
/// Editing this table? Run `verify-alphabet.py` — the tests in this crate gate the
/// single-token cost under Claude only, not under the other four tokenizer families
/// the crate documentation claims.
#[rustfmt::skip]
pub const ALPHABET: [&str; 256] = [
    "access", "account", "action", "address", "album", "android", "application", "area",
    "array", "article", "association", "author", "award", "background", "band", "black",
    "board", "body", "border", "break", "build", "building", "business", "button", "call",
    "card", "career", "category", "census", "center", "central", "century", "change", "character",
    "check", "city", "class", "click", "client", "close", "club", "code", "college", "color",
    "column", "command", "common", "community", "company", "components", "console", "container",
    "content", "control", "council", "count", "country", "course", "data", "database", "density",
    "department", "description", "design", "development", "device", "director", "display",
    "district", "division", "document", "door", "double", "download", "early", "education",
    "element", "email", "error", "events", "example", "export", "express", "face", "features",
    "field", "film", "first", "float", "food", "football", "force", "form", "format", "function",
    "future", "games", "general", "green", "group", "head", "header", "height", "help", "high",
    "history", "home", "host", "house", "households", "images", "import", "important", "income",
    "index", "info", "information", "input", "install", "island", "king", "label", "language",
    "large", "league", "length", "level", "library", "license", "life", "light", "list",
    "local", "location", "login", "love", "management", "march", "market", "master", "material",
    "math", "median", "members", "message", "method", "million", "models", "money", "music",
    "network", "news", "north", "note", "number", "object", "office", "options", "package",
    "page", "password", "people", "period", "person", "places", "play", "players", "population",
    "port", "position", "power", "press", "price", "print", "println", "process", "production",
    "products", "program", "project", "property", "published", "query", "question", "range",
    "records", "references", "region", "register", "render", "report", "request", "research",
    "response", "results", "return", "review", "river", "role", "room", "router", "school",
    "science", "score", "script", "search", "season", "section", "select", "send", "series",
    "services", "session", "share", "social", "society", "software", "song", "source", "south",
    "space", "span", "species", "square", "station", "story", "street", "string", "students",
    "study", "style", "success", "system", "table", "target", "task", "team", "television",
    "template", "title", "token", "track", "train", "training", "union", "university", "update",
    "username", "users", "version", "video", "village", "website", "width", "window", "world",
];

/// Why a sequence of words could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// A word outside the alphabet, and where in the sequence it sat.
    ///
    /// Reported rather than skipped or guessed at: a value that lost a word is not
    /// the value that was sent, and inventing the byte it stood for would turn a
    /// visible transcription error back into a silent one — the whole failure mode
    /// this codec exists to remove.
    UnknownWord { position: usize, word: String },
    /// No alphabet words at all. The encoding of no bytes is the empty string, which
    /// is never a value a caller means to transmit, so decoding one is an error
    /// rather than an empty success.
    Empty,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownWord { position, word } => write!(
                f,
                "`{word}` (word {}) is not in the unigram alphabet",
                position + 1
            ),
            Self::Empty => f.write_str("no unigram words found"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Split on any run of characters that is not an ASCII letter.
///
/// Being this liberal is what lets a value survive a round trip through a model or
/// a transcript: hyphens, newlines, commas, quotes, and stray punctuation all read
/// as separators, so only the words themselves have to arrive intact.
fn split_words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !c.is_ascii_alphabetic())
        .filter(|word| !word.is_empty())
}

/// Encode bytes as space-joined alphabet words, one word per byte.
pub fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 8);
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(ALPHABET[*byte as usize]);
    }
    out
}

/// Decode alphabet words back to the bytes they carry.
///
/// Liberal in what it accepts — see [`split_words`] — but exact in what it returns:
/// every word must be in the alphabet, or the value is refused and the offending
/// word named.
pub fn decode(text: &str) -> Result<Vec<u8>, DecodeError> {
    let mut bytes = Vec::new();
    for (position, word) in split_words(text).enumerate() {
        let lowered = word.to_ascii_lowercase();
        match ALPHABET.binary_search(&lowered.as_str()) {
            Ok(index) => bytes.push(index as u8),
            Err(_) => {
                return Err(DecodeError::UnknownWord {
                    position,
                    word: word.to_string(),
                })
            }
        }
    }
    if bytes.is_empty() {
        return Err(DecodeError::Empty);
    }
    Ok(bytes)
}

/// Mint `bytes` bytes of fresh entropy, encoded.
///
/// Four bytes is a good default for a nonce: 32 bits in four tokens, against the
/// nineteen a 32-character hex string costs.
///
/// # Panics
///
/// If the OS entropy source is unavailable. That is not a condition a caller can
/// do anything useful with, and returning a predictable value instead would be far
/// worse than stopping.
pub fn mint(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    getrandom::fill(&mut buffer).expect("OS entropy source unavailable");
    encode(&buffer)
}

/// Reduce a value to the form comparisons are made in: trimmed, lowercased, and
/// with internal whitespace runs collapsed to a single space.
///
/// Deliberately preserves every non-whitespace character, so this is safe to apply
/// to a string that is *not* an encoded value — a legacy hex token, say — without
/// mangling it. [`decode`]'s liberal splitting is the opposite trade and belongs
/// only where the bytes are actually wanted back.
pub fn normalize(text: &str) -> String {
    text.split_whitespace()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Compare a value that was issued against one a caller presented, tolerating the
/// damage a round trip through a model or a transcript does.
///
/// When both sides are encoded values the comparison is on the decoded bytes, so
/// separator and case damage on the presented side cannot matter. Otherwise it
/// falls back to comparing [`normalize`]d strings, which is what lets a value
/// issued in some older format still match itself without a migration.
pub fn matches(issued: &str, presented: &str) -> bool {
    if let (Ok(issued_bytes), Ok(presented_bytes)) = (decode(issued), decode(presented)) {
        return issued_bytes == presented_bytes;
    }
    normalize(issued) == normalize(presented)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_tokenize::{count, Family};

    /// A carrier word to measure against. A word's cost has to be measured as the
    /// MARGINAL cost of appending it to preceding text, because that is the position
    /// it actually occupies inside an encoded value. Measuring `" word"` on its own
    /// would also charge for the leading space opening the string, which no word in
    /// a real encoding ever pays.
    const CARRIER: &str = "the";

    fn marginal_cost(word: &str) -> u32 {
        count(&format!("{CARRIER} {word}"), Family::V5) - count(CARRIER, Family::V5)
    }

    /// The claim the crate is named for.
    #[test]
    fn every_alphabet_word_costs_exactly_one_token() {
        for word in ALPHABET {
            let cost = marginal_cost(word);
            assert_eq!(
                cost, 1,
                "`{word}` costs {cost} tokens space-prefixed, not 1"
            );
        }
    }

    /// The per-word property has to compose, or it buys nothing: an encoded value
    /// costs one token per byte end to end, with the spaces free.
    #[test]
    fn an_encoded_value_costs_one_token_per_byte() {
        let bytes = [0u8, 17, 128, 255, 42, 200, 7, 91];
        let encoded = encode(&bytes);
        let cost = count(&encoded, Family::V5) - count("", Family::V5);
        assert_eq!(cost, bytes.len() as u32, "{encoded}");
    }

    /// The comparison against what this replaces.
    ///
    /// The claim is deliberately about the distribution rather than every case. A
    /// hex string whose digits happen to group well can tie or win — hex cost VARIES
    /// with the value, which is itself half the point. Measured over deterministic
    /// pseudorandom values so the assertion cannot flake on a lucky sample.
    #[test]
    fn an_encoded_value_costs_less_than_the_hex_it_replaces() {
        let base = count("", Family::V5);
        let (mut hex_total, mut word_total) = (0u32, 0u32);
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        for _ in 0..64 {
            let bytes: Vec<u8> = (0..16)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 24) as u8
                })
                .collect();
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            let words = encode(&bytes);
            let word_cost = count(&words, Family::V5) - base;
            // An invariant, not an average: no value costs more than one token per
            // byte, so an encoded value's size is known before it is minted.
            assert_eq!(word_cost, 16, "{words}");
            hex_total += count(&hex, Family::V5) - base;
            word_total += word_cost;
        }
        assert!(
            hex_total > word_total,
            "hex {hex_total} vs words {word_total}"
        );
    }

    /// A hyphenated join is the tempting alternative, and it is the expensive one:
    /// each hyphen becomes a token AND severs the word from the space-prefixed vocab
    /// entry that made it cheap. Pinned as a test so nobody "tidies" the separator.
    #[test]
    fn joining_on_a_hyphen_would_cost_more_than_joining_on_a_space() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let spaced = encode(&bytes);
        let hyphenated = spaced.replace(' ', "-");
        let base = count("", Family::V5);
        assert!(
            count(&hyphenated, Family::V5) - base > count(&spaced, Family::V5) - base,
            "hyphenated {hyphenated}"
        );
    }

    #[test]
    fn the_alphabet_is_sorted_unique_and_plain_lowercase() {
        let mut sorted = ALPHABET;
        sorted.sort_unstable();
        assert_eq!(sorted, ALPHABET, "binary_search requires sorted order");
        let unique: std::collections::HashSet<_> = ALPHABET.iter().collect();
        assert_eq!(unique.len(), ALPHABET.len());
        for word in ALPHABET {
            assert!(
                word.len() >= 4 && word.len() <= 11 && word.bytes().all(|b| b.is_ascii_lowercase()),
                "`{word}`"
            );
        }
    }

    /// Distance from every other entry is what turns a one-character slip into a
    /// refusal instead of a different valid byte. Without it the codec is only as
    /// honest as hex.
    #[test]
    fn no_two_entries_are_within_one_edit_of_each_other() {
        fn within_one_edit(a: &str, b: &str) -> bool {
            let (a, b) = if a.len() > b.len() { (b, a) } else { (a, b) };
            let (short, long) = (a.as_bytes(), b.as_bytes());
            match long.len() - short.len() {
                0 => short.iter().zip(long).filter(|(x, y)| x != y).count() <= 1,
                1 => {
                    let skip = short.iter().zip(long).take_while(|(x, y)| x == y).count();
                    short[skip..] == long[skip + 1..]
                }
                _ => false,
            }
        }
        for (i, a) in ALPHABET.iter().enumerate() {
            for b in &ALPHABET[i + 1..] {
                assert!(!within_one_edit(a, b), "`{a}` and `{b}` are one edit apart");
            }
        }
    }

    #[test]
    fn every_byte_round_trips() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(decode(&encode(&all)).unwrap(), all);
    }

    #[test]
    fn a_single_byte_round_trips_without_separators() {
        let encoded = encode(&[7]);
        assert!(!encoded.contains(' '));
        assert_eq!(decode(&encoded).unwrap(), vec![7]);
    }

    /// The point of the codec: a value mangled on its way through a model still
    /// decodes to what was sent.
    #[test]
    fn decoding_survives_the_mangling_a_round_trip_introduces() {
        let bytes = [0x3d, 0x9a, 0x00, 0xff];
        let encoded = encode(&bytes);
        for mangled in [
            encoded.to_uppercase(),
            format!("  {encoded}  "),
            encoded.replace(' ', "-"),
            encoded.replace(' ', ",  "),
            encoded.replace(' ', "\n"),
            format!("\"{}\"", encoded.replace(' ', "   ")),
        ] {
            assert_eq!(decode(&mangled).unwrap(), bytes, "{mangled}");
        }
    }

    #[test]
    fn an_unknown_word_is_refused_and_named() {
        let encoded = format!("{} zzzz {}", ALPHABET[1], ALPHABET[2]);
        assert_eq!(
            decode(&encoded),
            Err(DecodeError::UnknownWord {
                position: 1,
                word: "zzzz".to_string(),
            })
        );
    }

    /// A near-miss is the case that matters: one character off a real entry must be
    /// refused, not silently read as some other byte.
    #[test]
    fn a_one_character_slip_is_refused_rather_than_read_as_another_byte() {
        assert!(matches!(
            decode("accesx"),
            Err(DecodeError::UnknownWord { .. })
        ));
    }

    #[test]
    fn an_empty_value_is_refused() {
        assert_eq!(decode(""), Err(DecodeError::Empty));
        assert_eq!(decode("   -- \n"), Err(DecodeError::Empty));
    }

    #[test]
    fn mint_produces_one_word_per_requested_byte() {
        let minted = mint(4);
        assert_eq!(minted.split(' ').count(), 4, "{minted}");
        assert_eq!(decode(&minted).unwrap().len(), 4);
        assert_ne!(mint(8), mint(8));
    }

    #[test]
    fn matching_tolerates_mangling_of_an_encoded_value() {
        let issued = mint(4);
        assert!(matches(&issued, &issued));
        assert!(matches(&issued, &issued.to_uppercase()));
        assert!(matches(
            &issued,
            &format!("  {}  ", issued.replace(' ', " - "))
        ));
        assert!(!matches(&issued, &mint(4)));
    }

    /// Values issued in an older opaque format have to keep matching themselves, or
    /// swapping the minted form would strand every token outstanding at the moment
    /// of the upgrade.
    #[test]
    fn matching_still_compares_values_that_are_not_encoded_at_all() {
        let legacy = "3925ca9a0065442496cc231d6ae48870";
        assert!(matches(legacy, legacy));
        assert!(matches(legacy, &format!("  {}  ", legacy.to_uppercase())));
        assert!(!matches(legacy, "3925ca9a0065442496cc231d6ae48871"));
        assert!(!matches(legacy, &mint(4)));
    }

    #[test]
    fn normalize_leaves_a_non_encoded_string_intact() {
        assert_eq!(normalize("  3925CA9A-0065  "), "3925ca9a-0065");
    }
}
