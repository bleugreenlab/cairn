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
import { startRelay } from "../support/stdio_relay.ts";

const [surface, codex, ...args] = process.argv.slice(2);
// Strip only Cairn's explicit distrust, reproducing the pre-hardening launch.
const forwarded = args.filter((arg, index) =>
  !(surface === "project-trust-removed" &&
    (arg.startsWith("projects=") || (arg === "-c" && args[index + 1]?.startsWith("projects=")))));
const io = startRelay([codex, ...forwarded]);

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

const toClient = io.toClient;

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
  if (["project-trust-removed", "thread-project-trust-removed"].includes(surface) && message.params?.config) {
    delete message.params.config.projects;
    line = JSON.stringify(message);
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

await io.run({ stdout: downstream, stdin: upstream });
