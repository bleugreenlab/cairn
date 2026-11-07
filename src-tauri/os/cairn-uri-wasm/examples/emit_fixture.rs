//! Prints the parity fixture the WebAssembly build is held to: every
//! `cairn_uri::samples` entry as its canonical URI and the JSON the native
//! build resolves it to, plus addresses that must resolve to nothing.
//!
//! `bun run test:uri-wasm` writes this to the build directory and runs
//! `parity.test.ts` against the module built from the same commit.

use cairn_uri::samples::every_resource_variant;
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

fn main() {
    let samples: Vec<_> = every_resource_variant()
        .into_iter()
        .map(|sample| {
            let uri = sample.resource.to_uri();
            let resolved = cairn_uri_wasm::resolve(&uri);
            assert_eq!(
                resolved.as_ref(),
                Some(&sample.resource),
                "{uri} must resolve natively before the wasm build is held to it"
            );
            json!({ "uri": uri, "expected": serde_json::to_value(&sample.resource).unwrap() })
        })
        .collect();
    for uri in UNRESOLVED {
        assert!(cairn_uri_wasm::resolve(uri).is_none(), "{uri}");
    }
    let fixture = json!({ "samples": samples, "unresolved": UNRESOLVED });
    println!("{}", serde_json::to_string_pretty(&fixture).unwrap());
}
