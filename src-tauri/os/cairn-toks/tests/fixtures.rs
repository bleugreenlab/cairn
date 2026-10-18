//! Offline tests over the generated fixtures (`fixtures/generate.py`): three
//! tiny self-trained tokenizers, one per family shape Cairn pins (byte-level
//! BPE, the same under NFC, SentencePiece-style BPE), and a refusal fixture.
#![cfg(toks_native)]

use std::path::PathBuf;

use cairn_toks::{Cut, CutStrategy, LoadError, Tokenizer};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    fixtures: std::collections::BTreeMap<String, Fixture>,
}

#[derive(Deserialize)]
struct Fixture {
    file: String,
    probes: Vec<Probe>,
}

#[derive(Deserialize)]
struct Probe {
    text: String,
    ids: Vec<u32>,
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn golden() -> Golden {
    let raw = std::fs::read_to_string(fixtures_dir().join("golden.json")).unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn load(file: &str) -> Tokenizer {
    let bytes = std::fs::read(fixtures_dir().join(file)).unwrap();
    Tokenizer::from_json(&bytes).unwrap_or_else(|error| panic!("{file}: {error}"))
}

fn tokenizers() -> Vec<(String, Tokenizer)> {
    golden()
        .fixtures
        .into_iter()
        .map(|(name, fixture)| (name, load(&fixture.file)))
        .collect()
}

/// A small deterministic generator, so failures reproduce from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Fragments chosen to stress pre-tokenizers, normalizers and added-token
/// matching: words, code, CJK, combining marks (NFC rewrites them), long
/// whitespace runs, emoji, digits, and the fixtures' added tokens themselves.
const FRAGMENTS: &[&str] = &[
    "the",
    " quick",
    " brown",
    "Fox",
    "JUMPS",
    "over",
    " lazy",
    "dog.",
    "\n",
    "\n\n",
    "\t",
    "    ",
    "          ",
    "fn",
    "()",
    " {",
    "}",
    "::",
    "snake_case",
    "CamelCase",
    "12",
    "345",
    "67890",
    "3.14",
    "\u{65e5}\u{672c}\u{8a9e}",
    "\u{6f22}\u{5b57}",
    "\u{30c6}\u{30ad}\u{30b9}\u{30c8}",
    "caf\u{e9}",
    "cafe\u{301}",
    "e\u{301}\u{302}",
    "\u{e9}",
    "\u{1f600}",
    "\u{1f680}\u{1f680}",
    "\u{2726}",
    "\u{3b1}\u{3b2}",
    "don't",
    "WE'VE",
    "'s",
    "<|end|>",
    "<tool_call>",
    "<|end",
    "tool_call>",
    "!",
    "?",
    "...",
    "\"",
    "'",
    " ",
    "\u{a0}",
    "zygomorphic",
    "\r\n",
    "\u{200b}",
    "\u{feff}",
];

fn random_text(rng: &mut Rng, fragments: usize) -> String {
    (0..fragments)
        .map(|_| FRAGMENTS[rng.below(FRAGMENTS.len())])
        .collect()
}

/// The oracle: one encode of the exact prefix, no splitting anywhere.
fn exact_count(tokenizer: &Tokenizer, text: &str) -> u32 {
    tokenizer.encode(text).unwrap().len() as u32
}

fn assert_cut_contract(name: &str, tokenizer: &Tokenizer, text: &str, budget: u32, cut: Cut) {
    assert!(
        text.is_char_boundary(cut.byte_offset),
        "{name}: cut at {} is not a char boundary of {text:?}",
        cut.byte_offset
    );
    assert!(
        cut.tokens <= budget,
        "{name}: {} tokens over budget {budget} for {text:?}",
        cut.tokens
    );
    assert_eq!(
        cut.tokens,
        exact_count(tokenizer, &text[..cut.byte_offset]),
        "{name}: reported tokens disagree with the prefix's count (budget {budget}, text {text:?})"
    );
}

#[test]
fn golden_ids_match_hugging_face() {
    for (name, fixture) in golden().fixtures {
        let tokenizer = load(&fixture.file);
        for probe in &fixture.probes {
            assert_eq!(
                tokenizer.encode(&probe.text).unwrap(),
                probe.ids,
                "{name}: {:?}",
                probe.text
            );
            assert_eq!(
                tokenizer.count(&probe.text).unwrap() as usize,
                probe.ids.len(),
                "{name}: {:?}",
                probe.text
            );
        }
    }
}

#[test]
fn every_fixture_family_has_certified_cuts() {
    for (name, tokenizer) in tokenizers() {
        assert!(tokenizer.has_certified_cuts(), "{name}");
    }
}

#[test]
fn cut_contract_holds_on_both_strategies() {
    for (name, tokenizer) in tokenizers() {
        let mut rng = Rng(0x5eed_0000 ^ name.len() as u64);
        for round in 0..60 {
            // Short texts bisect one region; longer ones span many coarse parts.
            let fragments = if round % 4 == 0 {
                600
            } else {
                1 + rng.below(80)
            };
            let text = random_text(&mut rng, fragments);
            let total = exact_count(&tokenizer, &text);
            let budgets = [
                0,
                1,
                2,
                total / 3,
                total / 2,
                total.saturating_sub(1),
                total,
                total + 5,
            ];
            for budget in budgets {
                for strategy in [CutStrategy::Certified, CutStrategy::TokenBytes] {
                    let cut = tokenizer.cut_with(&text, budget, strategy).unwrap();
                    assert_cut_contract(
                        &format!("{name}/{strategy:?}"),
                        &tokenizer,
                        &text,
                        budget,
                        cut,
                    );
                    if budget >= total {
                        assert_eq!(
                            cut.byte_offset,
                            text.len(),
                            "{name}/{strategy:?}: whole text fits"
                        );
                    }
                }
            }
        }
    }
}

/// The certified cut should land on the longest fitting prefix almost always;
/// a cut far short of it would mean the bisection or the part sums are off.
#[test]
fn certified_cut_is_near_maximal() {
    for (name, tokenizer) in tokenizers() {
        let mut rng = Rng(0xa11ce ^ name.len() as u64);
        for _ in 0..20 {
            let text = random_text(&mut rng, 200);
            let total = exact_count(&tokenizer, &text);
            let budget = total / 2;
            let cut = tokenizer.cut(&text, budget).unwrap();
            let longest = text
                .char_indices()
                .map(|(at, _)| at)
                .chain(std::iter::once(text.len()))
                .filter(|&at| exact_count(&tokenizer, &text[..at]) <= budget)
                .max()
                .unwrap();
            assert!(
                longest - cut.byte_offset <= 16,
                "{name}: cut at {} but {longest} fits budget {budget}",
                cut.byte_offset
            );
        }
    }
}

/// Texts past the pooled scratch size are counted as sums over certified
/// parts; the sum must equal one whole-text encode exactly.
#[test]
fn long_text_part_sums_are_exact() {
    for (name, tokenizer) in tokenizers() {
        let mut rng = Rng(0xb16 ^ name.len() as u64);
        let text = random_text(&mut rng, 60_000);
        assert!(
            text.len() > 200_000,
            "the text must span several 64 KiB parts"
        );
        let whole = exact_count(&tokenizer, &text);
        assert_eq!(tokenizer.count(&text).unwrap(), whole, "{name}");
        for budget in [whole / 5, whole / 2, whole - 1] {
            let cut = tokenizer.cut(&text, budget).unwrap();
            assert_cut_contract(&name, &tokenizer, &text, budget, cut);
            assert!(
                cut.byte_offset > 0,
                "{name}: a long text should yield a non-empty prefix"
            );
        }
    }
}

/// Cheap in-house fuzzing of the encode surface: random code points from
/// ranges that exercise every byte-length class and the normalizers.
#[test]
fn randomized_encode_never_errors() {
    const RANGES: &[(u32, u32)] = &[
        (0x20, 0x7e),
        (0x00, 0x1f),
        (0x80, 0x24f),
        (0x300, 0x36f),
        (0x3040, 0x30ff),
        (0x4e00, 0x9fff),
        (0xac00, 0xd7a3),
        (0x1f300, 0x1faff),
        (0xe000, 0xf8ff),
        (0xfff0, 0xffff),
    ];
    for (name, tokenizer) in tokenizers() {
        let mut rng = Rng(0xf022 ^ name.len() as u64);
        for _ in 0..3000 {
            let len = rng.below(64);
            let text: String = (0..len)
                .filter_map(|_| {
                    let (lo, hi) = RANGES[rng.below(RANGES.len())];
                    char::from_u32(lo + rng.below((hi - lo + 1) as usize) as u32)
                })
                .collect();
            let ids = tokenizer
                .encode(&text)
                .unwrap_or_else(|error| panic!("{name}: {error} on {text:?}"));
            assert_eq!(
                tokenizer.count(&text).unwrap() as usize,
                ids.len(),
                "{name}: {text:?}"
            );
        }
    }
}

#[test]
fn refusal_names_the_unsupported_feature() {
    let bytes = std::fs::read(fixtures_dir().join("refused_dropout.json")).unwrap();
    match Tokenizer::from_json(&bytes) {
        Err(LoadError::Refused { code, what }) => {
            assert_eq!(code, -3, "TOKS_E_UNSUPPORTED");
            assert!(
                what.contains("dropout"),
                "diagnostic names the feature: {what}"
            );
        }
        Err(other) => panic!("expected a refusal, got {other}"),
        Ok(_) => panic!("a dropout tokenizer must not load"),
    }
}

#[test]
fn malformed_input_is_a_format_refusal() {
    match Tokenizer::from_json(b"{\"model\": ") {
        Err(LoadError::Refused { code, .. }) => assert_eq!(code, -2, "TOKS_E_FORMAT"),
        Err(other) => panic!("expected a refusal, got {other}"),
        Ok(_) => panic!("truncated JSON must not load"),
    }
}

#[test]
fn source_digest_is_the_parsed_bytes() {
    use std::fmt::Write;
    let bytes = std::fs::read(fixtures_dir().join("bytelevel.json")).unwrap();
    let tokenizer = Tokenizer::from_json(&bytes).unwrap();
    let reported = tokenizer
        .source_sha256()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    let output = std::process::Command::new("shasum")
        .args(["-a", "256"])
        .arg(fixtures_dir().join("bytelevel.json"))
        .output()
        .or_else(|_| {
            std::process::Command::new("sha256sum")
                .arg(fixtures_dir().join("bytelevel.json"))
                .output()
        })
        .unwrap();
    let expected = String::from_utf8(output.stdout).unwrap();
    assert_eq!(reported, expected.split_whitespace().next().unwrap());
}

#[test]
fn one_tokenizer_serves_many_threads() {
    let tokenizer = std::sync::Arc::new(load("bytelevel_nfc.json"));
    let text = "caf\u{e9} and cafe\u{301} ".repeat(2000);
    let expected = exact_count(&tokenizer, &text);
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let tokenizer = tokenizer.clone();
            let text = text.clone();
            std::thread::spawn(move || {
                for _ in 0..20 {
                    assert_eq!(tokenizer.count(&text).unwrap(), expected);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
}
