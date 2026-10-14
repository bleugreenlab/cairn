//! Compare this port against a corpus of upstream ctok counts.
//!
//! Reads `{"text": ..., "v47": ..., "v5": ...}` rows on stdin, reports the exact
//! agreement rate and mean absolute error per family, and prints the first
//! disagreements so a drift can be read rather than only measured.

use cairn_tokenize::{count, Family};
use serde::Deserialize;
use std::io::{self, BufRead};

/// Enough to see the shape of a disagreement without burying it.
const SHOWN: usize = 20;

#[derive(Deserialize)]
struct Row {
    text: String,
    v47: u32,
    v5: u32,
}
fn main() {
    let mut n = 0u64;
    let mut e47 = 0u64;
    let mut e5 = 0u64;
    let mut abs47 = 0u64;
    let mut abs5 = 0u64;
    let mut shown = 0usize;
    for line in io::stdin().lock().lines() {
        let r: Row = serde_json::from_str(&line.unwrap()).unwrap();
        let a = count(&r.text, Family::V4_7);
        let b = count(&r.text, Family::V5);
        n += 1;
        e47 += u64::from(a == r.v47);
        e5 += u64::from(b == r.v5);
        abs47 += a.abs_diff(r.v47) as u64;
        abs5 += b.abs_diff(r.v5) as u64;
        if (a != r.v47 || b != r.v5) && shown < SHOWN {
            shown += 1;
            let text = &r.text;
            println!(
                "mismatch {text:?}: v4.7 {a} want {}, v5 {b} want {}",
                r.v47, r.v5
            );
            println!("  scalars: {:04x?}", text.chars().collect::<Vec<_>>());
        }
    }
    println!(
        "rows={n} v4.7 exact={e47} ({:.4}%) mae={:.4} v5 exact={e5} ({:.4}%) mae={:.4}",
        100.0 * e47 as f64 / n as f64,
        abs47 as f64 / n as f64,
        100.0 * e5 as f64 / n as f64,
        abs5 as f64 / n as f64
    );
    if e47 < n || e5 < n {
        std::process::exit(1);
    }
}
