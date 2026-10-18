//! What token counting costs on the read-budget path.
//!
//! ```sh
//! cargo bench -p cairn-session --bench token_meter
//! TOKS_BENCH_TOKENIZER=/path/to/tokenizer.json cargo bench -p cairn-session --bench token_meter
//! ```
//!
//! Part one times each meter's `count` and `cut` (at about 60% of the body's
//! tokens) on code, prose and CJK bodies from 4 KB to 1 MB; the cut column as
//! a multiple of count isolates the cost of finding the prefix. Part two runs
//! the renderer's real recompose paths, a single large file read and a batch
//! that forces the byte-ceiling search, through a decorator that counts and
//! times every meter call, and reports the share of each read spent metering.
//! With `TOKS_BENCH_TOKENIZER` set, the toks meter over that file joins both.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
use cairn_common::token_meter::{Cut, TokenMeter};
use cairn_db::storage::render::{assemble, render_segment_bounded};
use cairn_session::token_meters::{CLAUDE_TOKEN_METER, O200K_TOKEN_METER};

struct Toks(cairn_toks::Tokenizer);

impl TokenMeter for Toks {
    fn count(&self, text: &str) -> u32 {
        self.0.count(text).unwrap()
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        let cut = self.0.cut(text, budget).unwrap();
        Cut {
            byte_offset: cut.byte_offset,
            tokens: cut.tokens,
        }
    }
}

/// Counts and times every call it forwards.
struct Metered<'a> {
    inner: &'a dyn TokenMeter,
    counts: AtomicU64,
    cuts: AtomicU64,
    nanos: AtomicU64,
}

impl<'a> Metered<'a> {
    fn new(inner: &'a dyn TokenMeter) -> Self {
        Self {
            inner,
            counts: AtomicU64::new(0),
            cuts: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
        }
    }

    fn timed<T>(&self, calls: &AtomicU64, call: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let out = call();
        self.nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        calls.fetch_add(1, Ordering::Relaxed);
        out
    }
}

impl TokenMeter for Metered<'_> {
    fn count(&self, text: &str) -> u32 {
        self.timed(&self.counts, || self.inner.count(text))
    }

    fn cut(&self, text: &str, budget: u32) -> Cut {
        self.timed(&self.cuts, || self.inner.cut(text, budget))
    }
}

fn body(kind: &str, bytes: usize) -> String {
    let mut out = String::with_capacity(bytes + 256);
    let mut i = 0usize;
    while out.len() < bytes {
        match kind {
            "code" => out.push_str(&format!(
                "{:>6}\t    let value_{i} = compute(&input[{i}..], {i} * 3)?; // step {i} of the pipeline\n",
                i + 1
            )),
            "prose" => out.push_str(&format!(
                "Paragraph {i}: token budgets matter most where context windows are smallest, \
                 so an estimate that drifts by a tenth decides what the model never sees. "
            )),
            _ => out.push_str(&format!(
                "第{i}段：トークン予算は文脈窓が最も小さい場所で最も重要です。中文句子也在这里。\n"
            )),
        }
        i += 1;
    }
    let mut end = bytes.min(out.len());
    while !out.is_char_boundary(end) {
        end -= 1;
    }
    out.truncate(end);
    out
}

fn median(mut samples: Vec<Duration>) -> f64 {
    samples.sort();
    samples[samples.len() / 2].as_secs_f64() * 1e3
}

fn time(iterations: usize, mut call: impl FnMut()) -> f64 {
    call();
    median(
        (0..iterations)
            .map(|_| {
                let start = Instant::now();
                call();
                start.elapsed()
            })
            .collect(),
    )
}

fn iterations(bytes: usize) -> usize {
    if bytes >= 1 << 20 {
        5
    } else if bytes >= 256 << 10 {
        9
    } else {
        31
    }
}

/// One labelled read, run against whichever meter it is handed.
type Read = (String, Box<dyn Fn(&dyn TokenMeter)>);

fn segment(uri: &str, body: &str) -> ReadSegment {
    ReadSegment::text(
        body.to_string(),
        SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line),
    )
}

fn main() {
    let toks = std::env::var_os("TOKS_BENCH_TOKENIZER").map(|path| {
        let bytes = std::fs::read(&path).expect("read TOKS_BENCH_TOKENIZER");
        Toks(cairn_toks::Tokenizer::from_json(&bytes).expect("toks loads the tokenizer"))
    });
    let mut meters: Vec<(&str, &dyn TokenMeter)> = vec![
        ("o200k", &O200K_TOKEN_METER),
        ("claude", &CLAUDE_TOKEN_METER),
    ];
    if let Some(toks) = &toks {
        meters.push(("toks", toks));
    }
    let sizes = [4 << 10, 45 << 10, 256 << 10, 1 << 20];

    println!("## count and cut (median ms)\n");
    // `count` repeats one text, which toks' segment memo replays: the
    // recompose loop's workload. `cold` prefixes each run with a fresh number,
    // so no meter has seen the text before.
    println!("| meter | corpus | bytes | tokens | count | cold count | cut@60% | cut/count |");
    println!("|---|---|---|---|---|---|---|---|");
    for (name, meter) in &meters {
        for kind in ["code", "prose", "cjk"] {
            for &bytes in &sizes {
                let text = body(kind, bytes);
                let tokens = meter.count(&text);
                let budget = tokens * 3 / 5;
                let count = time(iterations(bytes), || {
                    black_box(meter.count(black_box(&text)));
                });
                let mut run = 0u64;
                let cold = time(iterations(bytes), || {
                    run += 1;
                    let fresh = format!("{run} {text}");
                    black_box(meter.count(black_box(&fresh)));
                });
                let cut = time(iterations(bytes), || {
                    black_box(meter.cut(black_box(&text), budget));
                });
                println!(
                    "| {name} | {kind} | {bytes} | {tokens} | {count:.3} | {cold:.3} | {cut:.3} | {:.1}x |",
                    cut / count
                );
            }
        }
    }

    println!("\n## recompose path (median ms per read)\n");
    println!("| meter | read | ms | count calls | cut calls | ms in meter | meter share |");
    println!("|---|---|---|---|---|---|---|");
    for (name, meter) in &meters {
        let reads: Vec<Read> = vec![
            ("1 MB file".into(), {
                let text = body("code", 1 << 20);
                Box::new(move |meter| {
                    black_box(render_segment_bounded(segment("file:big.rs", &text), meter));
                })
            }),
            ("256 KB file".into(), {
                let text = body("prose", 256 << 10);
                Box::new(move |meter| {
                    black_box(render_segment_bounded(
                        segment("file:notes.md", &text),
                        meter,
                    ));
                })
            }),
            ("batch 6 x 45 KB".into(), {
                let texts: Vec<String> = ["code", "prose", "cjk", "code", "prose", "cjk"]
                    .iter()
                    .map(|kind| body(kind, 45 << 10))
                    .collect();
                Box::new(move |meter| {
                    let segments = texts
                        .iter()
                        .enumerate()
                        .map(|(i, text)| segment(&format!("file:part{i}.txt"), text))
                        .collect();
                    black_box(assemble(segments, meter));
                })
            }),
        ];
        for (label, read) in &reads {
            let runs = 5;
            let mut totals = Vec::new();
            let (mut counts, mut cuts, mut in_meter) = (0, 0, 0.0);
            for _ in 0..runs {
                let metered = Metered::new(*meter);
                let start = Instant::now();
                read(&metered);
                totals.push(start.elapsed());
                counts = metered.counts.load(Ordering::Relaxed);
                cuts = metered.cuts.load(Ordering::Relaxed);
                in_meter += metered.nanos.load(Ordering::Relaxed) as f64 / 1e6 / runs as f64;
            }
            let total = median(totals);
            println!(
                "| {name} | {label} | {total:.2} | {counts} | {cuts} | {in_meter:.2} | {:.0}% |",
                100.0 * in_meter / total
            );
        }
    }
}
