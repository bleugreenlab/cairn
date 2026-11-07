// Init and per-call cost of the URI grammar module, in whichever JS engine
// loads this file. Under bun: `bun src-tauri/os/cairn-uri-wasm/bench.mjs` after
// `bun run build:uri-wasm`. In a browser: serve `src-tauri/` and open
// `os/cairn-uri-wasm/bench.html`, which prints the same report.

const ROUNDS = 200;

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

export async function measure(loadModule, bytes, samples) {
  const uris = samples.map((sample) => sample.uri);
  const jsons = samples.map((sample) => JSON.stringify(sample.expected));

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

  let calls = 0;
  const parseStart = performance.now();
  for (let round = 0; round < ROUNDS; round++) {
    for (const uri of uris) {
      grammar.parse(uri);
      calls++;
    }
  }
  const parseUs = ((performance.now() - parseStart) * 1000) / calls;

  const parseJsonStart = performance.now();
  for (let round = 0; round < ROUNDS; round++) {
    for (const uri of uris) JSON.parse(grammar.parse(uri));
  }
  const parseJsonUs = ((performance.now() - parseJsonStart) * 1000) / calls;

  const toUriStart = performance.now();
  for (let round = 0; round < ROUNDS; round++) {
    for (const json of jsons) grammar.to_uri(json);
  }
  const toUriUs = ((performance.now() - toUriStart) * 1000) / calls;

  return {
    engine: typeof navigator !== "undefined" ? navigator.userAgent : "unknown",
    wasmBytes: bytes.byteLength,
    samples: uris.length,
    compileMedianMs: +median(compiles).toFixed(3),
    compileFirstMs: +compiles[0].toFixed(3),
    initSyncMs: +initMs.toFixed(3),
    firstParseMs: +firstCallMs.toFixed(4),
    parseUsPerCall: +parseUs.toFixed(3),
    parseAndJsonParseUsPerCall: +parseJsonUs.toFixed(3),
    toUriUsPerCall: +toUriUs.toFixed(3),
  };
}

if (typeof Bun !== "undefined" && import.meta.main) {
  const { readFileSync } = await import("node:fs");
  const { join } = await import("node:path");
  const dir = join(import.meta.dir, "..", "..", "target", "uri-wasm");
  const bytes = readFileSync(join(dir, "cairn_uri_bg.wasm"));
  const { samples } = JSON.parse(readFileSync(join(dir, "samples.json"), "utf8"));
  const report = await measure(() => import(join(dir, "cairn_uri.js")), bytes, samples);
  console.log(JSON.stringify({ ...report, engine: `bun ${Bun.version}` }, null, 2));
}
