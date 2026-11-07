#!/usr/bin/env bun
/**
 * The codex-contract suite's MCP server: stdio, newline-delimited JSON-RPC.
 *
 * - `echo` answers its `text` argument back.
 * - `hang` never answers, so a case can hold a tool call in flight.
 *
 * Given a path as its first argument, it appends one JSON line per event to
 * that file: `start` and `exit` (stdin closed), every `tools/call` with its
 * full params (`_meta` included), and every `notifications/cancelled`. Each
 * line carries this process's pid, so a case can tell one shim from another.
 * The `start` line is written before anything else, so a case that only asks
 * whether the server ever ran can check that the file exists.
 */
import { appendFileSync } from "node:fs";

const logPath = process.argv[2];
const log = (event: string, fields: Record<string, unknown> = {}) => {
  if (logPath) appendFileSync(logPath, `${JSON.stringify({ event, pid: process.pid, ...fields })}\n`);
};
const write = (message: unknown) => process.stdout.write(`${JSON.stringify(message)}\n`);

const tools = [
  {
    name: "echo",
    description: "Answer the given text back.",
    inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
  },
  {
    name: "hang",
    description: "Never answer.",
    inputSchema: { type: "object", properties: { label: { type: "string" } } },
  },
];

type Message = { id?: unknown; method?: string; params?: Record<string, unknown> };

function answer(message: Message) {
  if (message.id === undefined || message.id === null) {
    if (message.method === "notifications/cancelled") log("cancelled", { params: message.params ?? {} });
    return;
  }
  switch (message.method) {
    case "initialize":
      return write({
        jsonrpc: "2.0",
        id: message.id,
        result: {
          protocolVersion: message.params?.protocolVersion ?? "2025-06-18",
          capabilities: { tools: {} },
          serverInfo: { name: "codex-contract-fixture", version: "1" },
        },
      });
    case "tools/list":
      return write({ jsonrpc: "2.0", id: message.id, result: { tools } });
    case "tools/call": {
      const tool = message.params?.name;
      log("call", { id: message.id, tool, params: message.params ?? {} });
      if (tool === "hang") return;
      const args = (message.params?.arguments ?? {}) as { text?: unknown };
      return write({
        jsonrpc: "2.0",
        id: message.id,
        result: { content: [{ type: "text", text: String(args.text ?? "") }], isError: false },
      });
    }
    case "ping":
      return write({ jsonrpc: "2.0", id: message.id, result: {} });
    default:
      return write({
        jsonrpc: "2.0",
        id: message.id,
        error: { code: -32601, message: `method not found: ${message.method}` },
      });
  }
}

log("start");
let buffered = "";
for await (const chunk of Bun.stdin.stream()) {
  buffered += new TextDecoder().decode(chunk);
  let end = buffered.indexOf("\n");
  while (end >= 0) {
    const line = buffered.slice(0, end).trim();
    buffered = buffered.slice(end + 1);
    if (line) {
      try {
        answer(JSON.parse(line));
      } catch {
        // Not JSON: nothing to answer.
      }
    }
    end = buffered.indexOf("\n");
  }
}
log("exit");
