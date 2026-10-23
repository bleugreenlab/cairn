#!/usr/bin/env bun
/**
 * The codex-contract gate negatives' stdio man-in-the-middle.
 *
 *   bun break_proxy.ts <surface> <real codex> app-server [args...]
 *
 * Runs the REAL binary and relays JSON-RPC lines both ways, rewriting exactly
 * one surface so the case covering it must go red. Everything else passes
 * through untouched, so a negative fails for its own reason only.
 */
import { connect } from "node:net";

const [surface, codex, ...args] = process.argv.slice(2);
const child = Bun.spawn([codex, ...args], {
  stdin: "pipe",
  stdout: "pipe",
  stderr: "inherit",
  env: process.env,
});

type Message = {
  id?: unknown;
  method?: string;
  params?: any;
  result?: any;
  error?: { code?: number; message?: string };
};

const INJECTED_ID = 990001;
const pending = new Map<unknown, string>();
let injected = false;
let clientAnswered = false;
const held: string[] = [];
let heldThreadStart: unknown = undefined;

const toClient = (line: string) => process.stdout.write(`${line}\n`);
const toCodex = (line: string) => {
  child.stdin.write(`${line}\n`);
  child.stdin.flush();
};

function replaceAll(line: string, from: string, to: string): string {
  return line.split(from).join(to);
}

/** A Codex message on its way to Cairn: answer the line(s) to relay. */
function downstream(line: string): string[] {
  let message: Message;
  try {
    message = JSON.parse(line);
  } catch {
    return [line];
  }
  const answering = message.method === undefined ? pending.get(message.id) : undefined;
  if (answering) pending.delete(message.id);
  switch (surface) {
    case "no-active-turn-text":
      return [replaceAll(line, "no active turn", "nothing running")];
    case "interrupt-mismatch-text":
      return [replaceAll(line, " but found ", " / live: ")];
    case "missing-rollout-text":
      return [replaceAll(line, "no rollout found", "rollout unavailable")];
    case "capacity-sentence":
      return [replaceAll(line, "Selected model is at capacity", "The selected model is busy")];
    case "goal-status-renamed": {
      for (const goal of [message.result?.goal, message.params?.goal]) {
        if (goal?.status === "paused") goal.status = "onHold";
      }
      return [JSON.stringify(message)];
    }
    case "will-retry-dropped":
      if (message.method === "error" && message.params) delete message.params.willRetry;
      return [JSON.stringify(message)];
    case "turn-status-renamed":
      if (message.method === "turn/completed" && message.params?.turn?.status === "failed") {
        message.params.turn.status = "errored";
      }
      return [JSON.stringify(message)];
    case "mcp-item-renamed":
      if (message.params?.item?.type === "mcpToolCall") message.params.item.type = "mcpCall";
      return [JSON.stringify(message)];
    case "unhandled-server-request":
      if (message.method === "turn/started" && !injected) {
        injected = true;
        return [
          line,
          JSON.stringify({ id: INJECTED_ID, method: "item/contract/unknownRequest", params: {} }),
        ];
      }
      return [line];
    case "config-warning":
      if (answering === "initialize") {
        return [
          line,
          JSON.stringify({
            method: "configWarning",
            params: { summary: "Unknown feature flag `shell_snapshot` was ignored", details: null },
          }),
        ];
      }
      return [line];
    case "thread-start-awaits-server-request":
      // Withhold the thread/start answer until the client answers a server
      // request, as a Codex that awaited its own token refresh would.
      if (answering === "thread/start" && !clientAnswered) {
        heldThreadStart = message.id;
        held.push(line);
        return [];
      }
      return [line];
    case "refresh-never-sent":
      return message.method === "account/chatgptAuthTokens/refresh" ? [] : [line];
    default:
      return [line];
  }
}

/** A Cairn message on its way to Codex: answer the line(s) to forward. */
function upstream(line: string): string[] {
  let message: Message;
  try {
    message = JSON.parse(line);
  } catch {
    return [line];
  }
  if (message.method && message.id !== undefined) pending.set(message.id, message.method);
  if (!message.method && message.id !== undefined) {
    if (message.id === INJECTED_ID) return [];
    clientAnswered = true;
    if (heldThreadStart !== undefined) {
      for (const heldLine of held.splice(0)) toClient(heldLine);
      heldThreadStart = undefined;
    }
  }
  if (surface === "thread-start-param-rejected" && message.method === "thread/start") {
    pending.delete(message.id);
    toClient(
      JSON.stringify({
        id: message.id,
        error: { code: -32600, message: "Invalid request: unknown field `serviceName`" },
      }),
    );
    return [];
  }
  return [line];
}

if (surface === "egress") {
  const proxy = new URL(process.env.HTTPS_PROXY ?? process.env.https_proxy ?? "http://127.0.0.1:9");
  const socket = connect(Number(proxy.port), proxy.hostname, () => {
    socket.write("CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com:443\r\n\r\n");
  });
  socket.on("data", () => socket.destroy());
  socket.on("error", () => {});
}

async function relay(stream: ReadableStream<Uint8Array>, each: (line: string) => void) {
  const decoder = new TextDecoder();
  let buffered = "";
  for await (const chunk of stream) {
    buffered += decoder.decode(chunk, { stream: true });
    let end = buffered.indexOf("\n");
    while (end >= 0) {
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 1);
      if (line.trim()) each(line);
      end = buffered.indexOf("\n");
    }
  }
  if (buffered.trim()) each(buffered);
}

const fromClient = relay(Bun.stdin.stream(), (line) => {
  for (const out of upstream(line)) toCodex(out);
}).then(() => child.stdin.end());
const fromCodex = relay(child.stdout, (line) => {
  for (const out of downstream(line)) toClient(out);
});

await fromCodex;
void fromClient;
process.exit(await child.exited);
