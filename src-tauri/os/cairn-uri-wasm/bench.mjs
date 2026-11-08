// Init and per-call cost of the URI grammar module, and its byte-for-byte
// agreement with the native fixture, in whichever JS engine loads this file.
// Under bun: `bun src-tauri/os/cairn-uri-wasm/bench.mjs` after
// `bun run build:uri-wasm`. In a browser: serve `src-tauri/` and open
// `os/cairn-uri-wasm/bench.html`, which prints the same report.

const ROUNDS = 200;

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function perCall(uris, call) {
  const start = performance.now();
  for (let round = 0; round < ROUNDS; round++) {
    for (const uri of uris) call(uri);
  }
  return +(((performance.now() - start) * 1000) / (ROUNDS * uris.length)).toFixed(3);
}

export async function measure(loadModule, bytes, fixture) {
  const uris = fixture.samples.map((sample) => sample.uri);

  // First and only init of the bindings: compile plus instantiate, from bytes,
  // synchronously. It runs before anything else compiles these bytes so no
  // engine-side code cache flatters it; wasm-bindgen caches the instance, so
  // it runs once.
  const grammar = await loadModule();
  const initStart = performance.now();
  grammar.initSync({ module: bytes });
  const initMs = performance.now() - initStart;

  // Synchronous compile alone, repeated: what `new WebAssembly.Module` costs
  // on this engine for these bytes once warm.
  const compiles = [];
  for (let i = 0; i < 10; i++) {
    const start = performance.now();
    new WebAssembly.Module(bytes);
    compiles.push(performance.now() - start);
  }

  const firstStart = performance.now();
  grammar.parse(uris[0]);
  const firstCallMs = performance.now() - firstStart;

  // The same byte-for-byte comparison parity.test.ts makes.
  const disagreeing = fixture.samples
    .filter(
      (sample) =>
        grammar.parse(sample.uri) !== sample.parse ||
        grammar.label(sample.uri) !== sample.label ||
        grammar.label(sample.uri, fixture.named) !== sample.namedLabel ||
        grammar.surface(sample.uri) !== sample.surface,
    )
    .map((sample) => sample.uri);

  return {
    engine: typeof navigator !== "undefined" ? navigator.userAgent : "unknown",
    wasmBytes: bytes.byteLength,
    samples: uris.length,
    agreeing: uris.length - disagreeing.length,
    disagreeing,
    compileMedianMs: +median(compiles).toFixed(3),
    compileFirstMs: +compiles[0].toFixed(3),
    initSyncMs: +initMs.toFixed(3),
    firstParseMs: +firstCallMs.toFixed(4),
    parseUsPerCall: perCall(uris, (uri) => grammar.parse(uri)),
    labelUsPerCall: perCall(uris, (uri) => grammar.label(uri)),
    surfaceUsPerCall: perCall(uris, (uri) => grammar.surface(uri)),
  };
}

if (typeof Bun !== "undefined" && import.meta.main) {
  const { readFileSync } = await import("node:fs");
  const { join } = await import("node:path");
  const dir = join(import.meta.dir, "..", "..", "target", "uri-wasm");
  const bytes = readFileSync(join(dir, "cairn_uri_bg.wasm"));
  const fixture = JSON.parse(readFileSync(join(dir, "samples.json"), "utf8"));
  const report = await measure(() => import(join(dir, "cairn_uri.js")), bytes, fixture);
  console.log(JSON.stringify({ ...report, engine: `bun ${Bun.version}` }, null, 2));
}
