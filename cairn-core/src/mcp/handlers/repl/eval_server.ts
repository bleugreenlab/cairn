// Cairn stateful REPL eval-server (typescript / bun). The bun-run sibling of
// eval_server.py, with parity semantics.
//
// Reads one JSON request per line on stdin: {"code": "..."}.
// Writes one JSON response per line on a PRIVATE duplicate of stdout:
//   {"type": "success"|"error", "value"?, "note"?, "error"?, "stdout", "stderr"}
//
// A single main-realm namespace persists across requests: script-top-level
// `const`/`let`/`class`/`function` declarations land in the context's global
// lexical environment (the mechanism Node's own REPL relies on), so variables,
// imports (via require / await import), and definitions carry over between `run`
// calls. Because evaluation happens in bun's MAIN realm, `Bun`, `fetch`,
// `console`, and `process` are all real with zero injection.
//
// Framing is protected at the OS file-descriptor level, mirroring eval_server.py:
// the JSON protocol is written to a private duplicate of the original stdout
// (PROTOCOL_FD) and the process's own fd 1 is baselined to /dev/null. During each
// evaluation both fd 1 and fd 2 are redirected into temp capture files and
// restored before the response is written, so a raw fd write, a native addon, or
// an inherited-stdio subprocess lands in the capture instead of corrupting the
// one-response-per-line stream and desynchronizing later sends.

import { dlopen, FFIType, suffix } from "bun:ffi";
import {
  openSync,
  writeSync,
  closeSync,
  readFileSync,
  unlinkSync,
} from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { tmpdir } from "node:os";
import vm from "node:vm";

// libc dup/dup2 for fd-level framing (macOS: libc.dylib / libSystem.B.dylib;
// Linux glibc: libc.so.6).
const libcCandidates = [`libc.${suffix}`, "libSystem.B.dylib", "libc.so.6"];
let libc: ReturnType<typeof dlopen> | null = null;
for (const name of libcCandidates) {
  try {
    libc = dlopen(name, {
      dup: { args: [FFIType.i32], returns: FFIType.i32 },
      dup2: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
    });
    break;
  } catch {
    // try the next candidate
  }
}
if (!libc) {
  // Fail visibly on inherited stderr rather than hanging the host on a REPL that
  // could never frame its output.
  writeSync(2, "cairn-repl: could not load libc for fd-level output framing\n");
  process.exit(1);
}
const { dup, dup2 } = libc.symbols;

// Refs captured at startup so user code that clobbers globals (JSON, Bun.inspect,
// fs.writeSync) cannot break the protocol or value serialization.
const _stringify = JSON.stringify.bind(JSON);
const _parse = JSON.parse.bind(JSON);
const _inspect = Bun.inspect;
const _writeSync = writeSync;

// A private duplicate of the real stdout, reserved for the JSON protocol; fd 1 is
// then baselined to /dev/null so nothing but an explicit protocol write reaches
// the host pipe outside an evaluation window. fd 2 is left inherited (the host
// does not pipe our stderr, so it cannot block) and captured only during eval.
const PROTOCOL_FD = dup(1);
dup2(openSync("/dev/null", "w"), 1);

// TS/JS transpiler. deadCodeElimination MUST be false or pure trailing expression
// statements (`({a:1})`, `null`, `[1,2,3]`) are eliminated to empty output,
// silently destroying value capture.
const transpiler = new Bun.Transpiler({
  loader: "ts",
  deadCodeElimination: false,
});

// require() resolves against the worktree node_modules (cwd = worktree); await
// import() flows through importModuleDynamically below.
(globalThis as Record<string, unknown>).require = createRequire(
  join(process.cwd(), "__cairn_repl__.ts"),
);
const dynImport = (spec: string) => import(spec);

const NOTE_WRAP_DECL =
  "This send was auto-wrapped for top-level await, so `const`/`let`/`class`/" +
  "`function` declared here are scoped to the send and do NOT persist across " +
  "sends. Use bare assignment (`x = await f()`) or `return` a value to carry " +
  "state forward.";

function writeResponse(resp: Record<string, unknown>): void {
  _writeSync(PROTOCOL_FD, _stringify(resp) + "\n");
}

let evalCounter = 0;
interface Capture {
  savedOut: number;
  savedErr: number;
  outFd: number;
  errFd: number;
  outPath: string;
  errPath: string;
}

// Redirect OS fds 1 and 2 into fresh temp files for one evaluation. Mirrors
// eval_server.py's _capture_fds.
function captureFds(): Capture {
  const id = `${process.pid}-${++evalCounter}`;
  const outPath = join(tmpdir(), `cairn-repl-out-${id}`);
  const errPath = join(tmpdir(), `cairn-repl-err-${id}`);
  const outFd = openSync(outPath, "w+");
  const errFd = openSync(errPath, "w+");
  const savedOut = dup(1);
  const savedErr = dup(2);
  dup2(outFd, 1);
  dup2(errFd, 2);
  return { savedOut, savedErr, outFd, errFd, outPath, errPath };
}

// Restore fds 1 and 2 and drain the capture files to text.
function restoreFds(cap: Capture): { stdout: string; stderr: string } {
  dup2(cap.savedOut, 1);
  dup2(cap.savedErr, 2);
  closeSync(cap.savedOut);
  closeSync(cap.savedErr);
  const stdout = readFileSync(cap.outPath, "utf8");
  const stderr = readFileSync(cap.errPath, "utf8");
  closeSync(cap.outFd);
  closeSync(cap.errFd);
  try {
    unlinkSync(cap.outPath);
  } catch {
    // best-effort temp cleanup
  }
  try {
    unlinkSync(cap.errPath);
  } catch {
    // best-effort temp cleanup
  }
  return { stdout, stderr };
}

// A top-level `return` is legal under `new Function` (function-body grammar) but a
// script rejects it at PARSE time — before any statement runs (empirically
// verified: no preceding side effect executes). So catching it lets us safely
// retry the exact code once through the async wrap, which also gives top-level
// await sends a natural way to return a value.
function isTopLevelReturn(e: unknown): boolean {
  return (
    e instanceof SyntaxError &&
    /Return statements are only valid inside functions|Illegal return statement/.test(
      e.message,
    )
  );
}

async function execute(
  code: string,
  js: string,
  wrapped: boolean,
): Promise<Record<string, unknown>> {
  // The async wrap transpiles the ORIGINAL (not the already-transpiled js) so TS
  // syntax and top-level await inside the wrap are handled in one pass, and a
  // trailing `return` yields the send's value.
  let body: string;
  if (wrapped) {
    try {
      body = transpiler.transformSync(`(async () => {\n${code}\n})()`);
    } catch (e) {
      // The wrap itself will not transpile (for example a static import inside
      // the send): a clean error, never a crashed request loop.
      return {
        type: "error",
        error: e instanceof Error ? e.message : String(e),
        stdout: "",
        stderr: "",
      };
    }
  } else {
    body = js;
  }

  const cap = captureFds();
  let resp: Record<string, unknown> | null = null;
  let retryAsReturn = false;
  try {
    let value = vm.runInThisContext(body, {
      filename: "<cairn-repl>",
      importModuleDynamically: dynImport,
    } as vm.RunningScriptOptions);
    // A thenable trailing value is awaited so `fetch(u).then(r => r.json())` as
    // the last expression returns data, not `Promise { ... }`.
    if (value != null && typeof (value as { then?: unknown }).then === "function") {
      value = await value;
    }
    resp = { type: "success" };
    // undefined completion → omit `value` (matches python's None omission); a real
    // value (including null → "null") is serialized with Bun.inspect.
    if (value !== undefined) {
      resp.value = _inspect(value);
    }
  } catch (e) {
    if (!wrapped && isTopLevelReturn(e)) {
      retryAsReturn = true;
    } else {
      resp = {
        type: "error",
        error: e instanceof Error && e.stack ? e.stack : String(e),
      };
    }
  }
  const { stdout, stderr } = restoreFds(cap);

  if (retryAsReturn) {
    // The parse failure ran nothing (capture is empty and discarded); rerun the
    // exact code once through the wrap so its `return` yields a value.
    return execute(code, js, true);
  }

  resp = resp ?? { type: "error", error: "unknown eval failure" };
  resp.stdout = stdout;
  resp.stderr = stderr;
  if (
    wrapped &&
    resp.type === "success" &&
    /\b(const|let|class|function)\b/.test(code)
  ) {
    resp.note = NOTE_WRAP_DECL;
  }
  return resp;
}

async function run(code: string): Promise<Record<string, unknown>> {
  let js: string;
  try {
    js = transpiler.transformSync(code);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    // A top-level `return` combined with top-level await forces module grammar,
    // where the transpiler rejects the return outright. Route to the async wrap
    // (execute transpiles the wrapped original) so the `return` yields a value.
    // (A bare top-level `return` without await transpiles fine and is caught
    // later at script-parse time, then retried through the wrap.)
    if (/Top-level return/i.test(msg)) {
      return execute(code, "", true);
    }
    // A genuine transpile/syntax error ran nothing: no capture, just the message.
    return { type: "error", error: msg, stdout: "", stderr: "" };
  }

  // new Function() is the eager, execution-free parser: it parses immediately with
  // function-body grammar, accepting everything a script accepts (including a
  // top-level `return`, handled at run time above) while rejecting top-level await
  // and static import/export. (vm.Script is LAZY in bun and would not surface
  // these until run time.)
  let wrapped = false;
  try {
    new Function(js);
  } catch {
    if (transpiler.scanImports(code).some((i) => i.kind === "import-statement")) {
      return {
        type: "error",
        error:
          'A static `import` statement is not supported in a REPL send. Use ' +
          '`require("pkg")` (resolves the worktree node_modules) or ' +
          '`await import("pkg")` instead.',
        stdout: "",
        stderr: "",
      };
    }
    // Top-level await (function-body-illegal, script-intended): async-wrap it.
    wrapped = true;
  }

  return execute(code, js, wrapped);
}

process.on("unhandledRejection", (reason) => {
  // Never crash the server (bun's default exit would present to the host as a
  // mysterious "died"); surface the rejection on inherited stderr instead.
  try {
    const text =
      reason instanceof Error && reason.stack ? reason.stack : String(reason);
    _writeSync(2, `cairn-repl unhandledRejection: ${text}\n`);
  } catch {
    // last resort: swallow
  }
});

// One JSON request per line on stdin; one framed response per line out.
for await (const line of console) {
  const trimmed = line.trim();
  if (!trimmed) continue;
  let code: unknown;
  try {
    code = _parse(trimmed).code;
  } catch {
    code = undefined;
  }
  if (typeof code !== "string") {
    writeResponse({
      type: "error",
      error:
        "malformed request: expected a JSON object with a `code` field",
      stdout: "",
      stderr: "",
    });
    continue;
  }
  writeResponse(await run(code));
}
