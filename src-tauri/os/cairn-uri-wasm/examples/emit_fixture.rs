//! Prints the parity fixture the WebAssembly build is held to: every
//! `cairn_uri::samples` entry as its canonical URI and the exact strings the
//! native build of each export returns for it, plus addresses that must
//! resolve to nothing.
//!
//! The strings are what the exports return, not a re-serialized value: a
//! `serde_json::Value` sorts object keys while the exports keep declaration
//! order, so a fixture built from values only agrees with the module under a
//! key-order-insensitive comparison. Byte-for-byte strings hold every engine
//! to the same check.
//!
//! `bun run test:uri-wasm` writes this to the build directory and runs
//! `parity.test.ts` against the module built from the same commit.

use cairn_uri::samples::every_resource_variant;
use cairn_uri_wasm::{canonical, label_with, parse, resolve, surface};
use serde_json::json;

/// Parseable shapes the resolver refuses, and strings that are not addresses.
const UNRESOLVED: &[&str] = &[
    "cairn://p/cairn/design-review/procedures",
    "cairn://p/cairn/design-review/task/probe/automations",
    "cairn://p/cairn/design-review/diff",
    "cairn://p/cairn/t/design-review",
    "cairn://nope",
    "https://example.com",
    "",
];

/// Display names the fixture labels every sample with a second time.
const NAMED: &str = r#"{"nodeLabel":"Builder","taskLabel":"Review"}"#;

fn main() {
    let samples: Vec<_> = every_resource_variant()
        .into_iter()
        .map(|sample| {
            let uri = sample.resource.to_uri();
            assert_eq!(
                resolve(&uri).as_ref(),
                Some(&sample.resource),
                "{uri} must resolve natively before the wasm build is held to it"
            );
            json!({
                "uri": uri,
                "kind": sample.resource.kind().slug(),
                "parse": parse(&uri).expect("resolved above"),
                "label": label_with(&uri, None).unwrap().expect("resolved above"),
                "namedLabel": label_with(&uri, Some(NAMED)).unwrap().expect("resolved above"),
                "surface": surface(&uri).expect("resolved above"),
                "canonical": canonical(&uri).expect("resolved above"),
            })
        })
        .collect();
    for uri in UNRESOLVED {
        assert!(resolve(uri).is_none(), "{uri}");
    }
    let fixture = json!({ "named": NAMED, "samples": samples, "unresolved": UNRESOLVED });
    println!("{}", serde_json::to_string_pretty(&fixture).unwrap());
}
