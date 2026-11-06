#!/usr/bin/env bun
/**
 * The claude-contract gate negatives' stdio man-in-the-middle.
 *
 *   bun break_relay.ts <surface> <real claude> [args...]
 *
 * Runs the REAL binary (its credential descriptor passed through) and relays
 * its stream-json stdout and its stderr, rewriting exactly one surface so the
 * case covering it must go red. Everything else passes through untouched, so
 * a negative fails for its own reason only.
 */
import { connect } from "node:net";
import { startRelay } from "../support/stdio_relay.ts";

const [surface, claude, ...args] = process.argv.slice(2);
const io = startRelay([claude, ...args], { stderr: true, passFds: [3] });

let injected = false;

function rename(object: any, from: string, to: string) {
  if (object && from in object) {
    object[to] = object[from];
    delete object[from];
  }
}

/** A stdout line on its way to Cairn: the line(s) to relay. */
function stdout(line: string): string[] {
  let event: any;
  try {
    event = JSON.parse(line);
  } catch {
    return [line];
  }
  const init = event.type === "system" && event.subtype === "init";
  switch (surface) {
    case "claude-version-renamed":
      if (init) rename(event, "claude_code_version", "claudeCodeVersion");
      break;
    case "core-verbs-dropped":
      if (init) event.tools = (event.tools ?? []).filter((tool: string) => !tool.startsWith("mcp__cairn__"));
      break;
    case "builtin-tool-offered":
      if (init) event.tools = [...(event.tools ?? []), "Bash"];
      break;
    case "is-error-dropped":
      if (event.type === "result") delete event.is_error;
      break;
    case "usage-field-renamed":
      if (event.type === "result") rename(event.usage, "cache_read_input_tokens", "cacheReadInputTokens");
      break;
    case "rate-limit-reset-renamed":
      if (event.type === "rate_limit_event") rename(event.rate_limit_info, "resetsAt", "resets_at");
      break;
    case "rate-limit-status-renamed":
      if (event.type === "rate_limit_event") rename(event.rate_limit_info, "status", "limitStatus");
      break;
    case "control-response-reshaped":
      if (event.type === "control_response") {
        const id = event.request_id ?? event.response?.request_id;
        delete event.request_id;
        if (event.response) {
          delete event.response.request_id;
          event.response.requestId = id;
        }
      }
      break;
    case "tool-result-renamed":
      if (event.type === "user" && Array.isArray(event.message?.content)) {
        for (const block of event.message.content) {
          if (block?.type === "tool_result") block.type = "tool_output";
        }
      }
      break;
    case "unsolicited-control-request":
      if (init && !injected) {
        injected = true;
        return [
          line,
          JSON.stringify({
            type: "control_request",
            request_id: "break-relay-1",
            request: { subtype: "can_use_tool", tool_name: "Bash", input: { command: "true" } },
          }),
        ];
      }
      return [line];
    default:
      return [line];
  }
  return [JSON.stringify(event)];
}

/** A stderr line on its way to Cairn: the line(s) to relay. */
function stderr(line: string): string[] {
  switch (surface) {
    case "resume-miss-text":
      return [line.split("No conversation found with session ID").join("Conversation not located for session ID")];
    case "session-in-use-text":
      return [line.split(" is already in use").join(" is taken")];
    default:
      return [line];
  }
}

if (surface === "egress") {
  const proxy = new URL(process.env.HTTPS_PROXY ?? process.env.https_proxy ?? "http://127.0.0.1:9");
  const socket = connect(Number(proxy.port), proxy.hostname, () => {
    socket.write("CONNECT api.anthropic.com:443 HTTP/1.1\r\nHost: api.anthropic.com:443\r\n\r\n");
  });
  socket.on("data", () => socket.destroy());
  socket.on("error", () => {});
}

await io.run({ stdout, stderr });
