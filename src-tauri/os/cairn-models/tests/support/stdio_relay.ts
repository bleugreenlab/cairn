/**
 * The line-relay core the contract suites' gate negatives share.
 *
 * Runs the REAL binary as a child and relays its stdio line by line, each
 * direction through a rewrite the suite's break script supplies, so exactly
 * one surface can be broken while everything else passes through untouched.
 * Descriptors above 2 the relay inherited (a credential Cairn hands the CLI
 * on fd 3) are passed straight to the child.
 */
import { fstatSync } from "node:fs";

export type Rewrite = (line: string) => string[];

export interface RelayOptions {
  /** Relay the child's stderr line by line too; otherwise it is inherited. */
  stderr?: boolean;
  /** Descriptors to pass through to the child, when the relay holds them. */
  passFds?: number[];
}

export interface Relay {
  /** Write a line to the client (the relay's stdout). */
  toClient(line: string): void;
  /** Write a line to the child's stdin. */
  toChild(line: string): void;
  /** Write a line to the client's stderr. */
  toClientStderr(line: string): void;
  /**
   * Relay until the child closes its output, then exit with its code.
   * `stdout` sees each child stdout line, `stdin` each client line, `stderr`
   * each child stderr line (with `stderr: true`); each answers the lines to
   * pass on. Absent means pass through.
   */
  run(rewrites: { stdout?: Rewrite; stdin?: Rewrite; stderr?: Rewrite }): Promise<never>;
}

function held(fd: number): boolean {
  try {
    fstatSync(fd);
    return true;
  } catch {
    return false;
  }
}

async function lines(stream: ReadableStream<Uint8Array>, each: (line: string) => void) {
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

function flushed(stream: NodeJS.WriteStream): Promise<void> {
  return new Promise((resolve) => stream.write("", () => resolve()));
}

export function startRelay(argv: string[], options: RelayOptions = {}): Relay {
  const extra = (options.passFds ?? []).filter(held);
  const child = Bun.spawn(argv, {
    stdio: ["pipe", "pipe", options.stderr ? "pipe" : "inherit", ...extra],
    env: process.env,
  });
  const toClient = (line: string) => void process.stdout.write(`${line}\n`);
  const toClientStderr = (line: string) => void process.stderr.write(`${line}\n`);
  const toChild = (line: string) => {
    child.stdin.write(`${line}\n`);
    child.stdin.flush();
  };
  const pass: Rewrite = (line) => [line];
  return {
    toClient,
    toChild,
    toClientStderr,
    async run({ stdout = pass, stdin = pass, stderr = pass }) {
      const fromClient = lines(Bun.stdin.stream(), (line) => {
        for (const out of stdin(line)) toChild(out);
      }).then(() => child.stdin.end());
      const fromChild = lines(child.stdout, (line) => {
        for (const out of stdout(line)) toClient(out);
      });
      const fromChildStderr = options.stderr
        ? lines(child.stderr as ReadableStream<Uint8Array>, (line) => {
            for (const out of stderr(line)) toClientStderr(out);
          })
        : Promise.resolve();
      await Promise.all([fromChild, fromChildStderr]);
      void fromClient;
      const code = await child.exited;
      await Promise.all([flushed(process.stdout), flushed(process.stderr)]);
      process.exit(code);
    },
  };
}
