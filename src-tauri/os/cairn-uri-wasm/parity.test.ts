// The WebAssembly build of the URI grammar must agree with the native build on
// every sample: the fixture holds the exact strings native Rust's exports
// return for each canonical URI (`examples/emit_fixture.rs`), and the module
// here is built from the same commit by `bun run test:uri-wasm`.
//
// Comparisons are byte-for-byte on purpose. A structural comparison passes
// whatever order an engine or a serializer puts keys in, and a string
// comparison against a re-serialized fixture fails for the same reason, which
// is what made a browser page report 113 of 206 samples "disagreeing" with a
// build that agreed on every one.
//
// Instantiation is synchronous from inline bytes on purpose: the frontend asks
// for a label at render time, so an async-only init would be a finding.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// The committed module the frontend imports, not the fresh build beside it:
// across architectures only the committed one is bound (by digest) to these
// inputs, and its behaviour is what this holds to the native build.
const dir = join(import.meta.dir, "..", "..", "..", "packages", "ui", "src", "uri-wasm");
const grammar = await import(join(dir, "cairn_uri.js"));
grammar.initSync({ module: readFileSync(join(dir, "cairn_uri_bg.wasm")) });

interface Sample {
  uri: string;
  kind: string;
  parse: string;
  label: string;
  namedLabel: string;
  surface: string;
  canonical: string;
}
interface Fixture {
  named: string;
  samples: Sample[];
  unresolved: string[];
}
const fixture: Fixture = JSON.parse(readFileSync(join(dir, "samples.json"), "utf8"));

describe("cairn-uri-wasm parity with the native grammar", () => {
  test("the fixture carries one sample per resource kind", () => {
    const kinds = new Set(fixture.samples.map((sample) => sample.kind));
    expect(kinds.size).toBe(fixture.samples.length);
    expect(fixture.samples.length).toBeGreaterThan(200);
  });

  test.each(fixture.samples.map((sample) => [sample.uri, sample] as const))("%s", (uri, sample) => {
    expect(grammar.parse(uri)).toBe(sample.parse);
    expect(grammar.label(uri)).toBe(sample.label);
    expect(grammar.label(uri, fixture.named)).toBe(sample.namedLabel);
    expect(grammar.surface(uri)).toBe(sample.surface);
    expect(grammar.canonical(uri)).toBe(sample.canonical);
  });

  test("addresses that name nothing resolve to nothing", () => {
    for (const uri of fixture.unresolved) {
      expect(grammar.parse(uri), uri).toBeUndefined();
      expect(grammar.label(uri), uri).toBeUndefined();
      expect(grammar.surface(uri), uri).toBeUndefined();
      expect(grammar.canonical(uri), uri).toBeUndefined();
    }
  });

  test("a malformed label context throws", () => {
    expect(() => grammar.label("cairn://p/CAIRN/42", "not json")).toThrow();
    expect(() => grammar.label("cairn://p/CAIRN/42", '{"node_label":"x"}')).toThrow();
  });
});
