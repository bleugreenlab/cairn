// The WebAssembly build of the URI grammar must agree with the native build on
// every sample: the fixture is what native Rust resolves each canonical URI to
// (`examples/emit_fixture.rs`), and the module here is built from the same
// commit by `bun run test:uri-wasm`.
//
// Instantiation is synchronous from inline bytes on purpose: the frontend asks
// for a label at render time, so an async-only init would be a finding.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const dir = join(import.meta.dir, "..", "..", "target", "uri-wasm");
const grammar = await import(join(dir, "cairn_uri.js"));
grammar.initSync({ module: readFileSync(join(dir, "cairn_uri_bg.wasm")) });

interface Fixture {
  samples: { uri: string; expected: { kind: string } & Record<string, unknown> }[];
  unresolved: string[];
}
const fixture: Fixture = JSON.parse(readFileSync(join(dir, "samples.json"), "utf8"));

describe("cairn-uri-wasm parity with the native grammar", () => {
  test("the fixture carries one sample per resource kind", () => {
    const kinds = new Set(fixture.samples.map((sample) => sample.expected.kind));
    expect(kinds.size).toBe(fixture.samples.length);
    expect(fixture.samples.length).toBeGreaterThan(200);
  });

  test.each(fixture.samples.map((sample) => [sample.uri, sample]))("%s", (_uri, sample) => {
    const parsed = grammar.parse(sample.uri);
    expect(parsed).toBeString();
    expect(JSON.parse(parsed)).toEqual(sample.expected);
    expect(grammar.to_uri(JSON.stringify(sample.expected))).toBe(sample.uri);
  });

  test("addresses that name nothing resolve to nothing", () => {
    for (const uri of fixture.unresolved) {
      expect(grammar.parse(uri), uri).toBeUndefined();
    }
  });

  test("to_uri refuses JSON that is not a resource", () => {
    expect(() => grammar.to_uri('{"kind":"not-a-kind"}')).toThrow();
  });
});
