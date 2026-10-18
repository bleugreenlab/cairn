//! The fleet check: every tokenizer Cairn pins, fetched at its pinned revision
//! through the verified store, loaded by toks, and compared with the reference
//! counts `fixtures/fleet_golden.py` recorded. Needs the network, so it is
//! ignored by default:
//!
//! ```sh
//! cargo test -p cairn-toks --test fleet -- --ignored --nocapture
//! ```
//!
//! `FLEET_CACHE` reuses a download directory across runs. The printed table is
//! the record a toks upgrade or a new pin is judged by.
#![cfg(toks_native)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use cairn_models::tokenizer_store::{PinnedSource, TokenizerStore};
use cairn_models::types::tokenizers::{pins, TokenizerPin};
use cairn_toks::Tokenizer;
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    probes: Vec<Probe>,
    pins: BTreeMap<String, PinCounts>,
}

#[derive(Deserialize)]
struct Probe {
    text: String,
    repeat: usize,
}

#[derive(Deserialize)]
struct PinCounts {
    counts: Vec<u32>,
}

fn check(pin: &TokenizerPin, store: &TokenizerStore, golden: &Golden) -> Result<String, String> {
    let expected = golden
        .pins
        .get(pin.digest)
        .ok_or("no reference counts: rerun fixtures/fleet_golden.py")?;
    let source = store
        .ensure(pin)
        .map_err(|error| format!("fetch: {error}"))?;
    let tokenizer = match &source {
        PinnedSource::Bytes(bytes) => Tokenizer::from_json(bytes),
        PinnedSource::Dir(snapshot) => Tokenizer::from_dir(snapshot.path()),
    }
    .map_err(|error| format!("toks: {error}"))?;
    if !pin.is_directory() && tokenizer.source_sha256() != pin.digest_bytes() {
        return Err("toks parsed bytes other than the pinned file".into());
    }

    let mut mismatches = Vec::new();
    for (index, (probe, &reference)) in golden.probes.iter().zip(&expected.counts).enumerate() {
        let text = probe.text.repeat(probe.repeat);
        let count = tokenizer.count(&text).map_err(|error| error.to_string())?;
        if count != reference {
            mismatches.push(format!("probe {index}: {count} vs {reference}"));
        }
        // The cut contract on the real file, against a single whole encode.
        for budget in [1, reference / 3, reference.saturating_sub(1)] {
            let cut = tokenizer
                .cut(&text, budget)
                .map_err(|error| error.to_string())?;
            let exact = tokenizer
                .encode(&text[..cut.byte_offset])
                .map_err(|error| error.to_string())?;
            if !text.is_char_boundary(cut.byte_offset)
                || cut.tokens > budget
                || cut.tokens as usize != exact.len()
            {
                mismatches.push(format!(
                    "probe {index}: cut contract broken at budget {budget}"
                ));
            }
        }
    }
    if mismatches.is_empty() {
        Ok(format!(
            "{} probes exact, cuts {}",
            golden.probes.len(),
            if tokenizer.has_certified_cuts() {
                "certified"
            } else {
                "by token bytes"
            }
        ))
    } else {
        Err(mismatches.join("; "))
    }
}

#[test]
#[ignore = "fetches every pinned tokenizer from huggingface.co"]
fn every_pinned_tokenizer_loads_exactly() {
    let golden: Golden = serde_json::from_str(include_str!("fleet_golden.json")).unwrap();
    let cache = std::env::var_os("FLEET_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("cairn-toks-fleet"));
    let store = TokenizerStore::new(cache);

    let mut failed = Vec::new();
    println!("\n| family | repository | size | digest | result |");
    println!("|---|---|---|---|---|");
    for pin in pins() {
        let size: u64 = pin.files.iter().map(|file| file.size).sum();
        let files = if pin.is_directory() {
            format!("{size} B in {} files", pin.files.len())
        } else {
            format!("{size} B")
        };
        let result = check(pin, &store, &golden);
        let shown = match &result {
            Ok(summary) => format!("ok: {summary}"),
            Err(reason) => format!("FAILED: {reason}"),
        };
        println!(
            "| {} | {} | {files} | {}… | {shown} |",
            pin.family,
            pin.repo,
            &pin.digest[..12]
        );
        if result.is_err() {
            failed.push(pin.repo);
        }
    }
    assert!(failed.is_empty(), "pins that are not exact: {failed:?}");
}
