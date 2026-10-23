#!/usr/bin/env bun
/**
 * The codex-contract suite's MCP server: stdio, newline-delimited JSON-RPC,
 * one `echo` tool that answers its `text` argument back.
 */
const write = (message: unknown) => process.stdout.write(`${JSON.stringify(message)}\n`);

const tools = [
  {
    name: "echo",
    description: "Answer the given text back.",
    inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] },
  },
];

function answer(message: { id?: unknown; method?: string; params?: Record<string, unknown> }) {
  if (message.id === undefined || message.id === null) return;
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
