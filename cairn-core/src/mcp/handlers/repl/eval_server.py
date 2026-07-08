#!/usr/bin/env python3
"""Cairn stateful REPL eval-server (python).

Reads one JSON request per line on stdin: {"code": "..."}.
Writes one JSON response per line on a PRIVATE duplicate of stdout:
  {"type": "success"|"error", "value"?, "error"?, "stdout", "stderr"}

A single module-level namespace dict persists across requests, so variables,
imports, and definitions carry over between `run` calls — the whole point of the
stateful REPL.

Framing is protected at the OS file-descriptor level, not just at
`sys.stdout`/`sys.stderr`. The JSON protocol is written to a private duplicate
of the original stdout (`_PROTOCOL_FD`), and the process's own fd 1 is baselined
to /dev/null. During each evaluation both fd 1 and fd 2 are redirected into
temporary capture files and restored before the response is written. So anything
that writes to a raw descriptor — `os.write(1, ...)`, a C extension, or an
uncaptured `subprocess.run([...])` — lands in the capture instead of corrupting
the one-response-per-line protocol stream and desynchronizing later sends.
"""

import ast
import json
import os
import sys
import tempfile
import traceback

# A private duplicate of the real stdout, reserved for the JSON protocol. The
# process's own fd 1 is then baselined to /dev/null so NOTHING except an
# explicit protocol write can reach the host pipe outside an evaluation window;
# during evaluation fd 1 is redirected into a capture file. fd 2 is left
# inherited (the host does not pipe the eval-server's stderr, so it cannot block)
# and is captured only during evaluation.
_PROTOCOL_FD = os.dup(1)
_DEVNULL_FD = os.open(os.devnull, os.O_WRONLY)
os.dup2(_DEVNULL_FD, 1)

# Persistent execution namespace, shared as globals across every request.
_NAMESPACE = {"__name__": "__cairn_repl__", "__builtins__": __builtins__}


def _write_response(response):
    """Write one framed JSON line to the private protocol descriptor."""
    line = (json.dumps(response) + "\n").encode("utf-8", "replace")
    os.write(_PROTOCOL_FD, line)


def _capture_fds():
    """Redirect OS fds 1 and 2 into fresh temp files for the duration of one
    evaluation. Returns the saved descriptors and capture files so
    `_restore_fds` can put everything back."""
    sys.stdout.flush()
    sys.stderr.flush()
    out_file = tempfile.TemporaryFile()
    err_file = tempfile.TemporaryFile()
    saved_out = os.dup(1)
    saved_err = os.dup(2)
    os.dup2(out_file.fileno(), 1)
    os.dup2(err_file.fileno(), 2)
    return saved_out, saved_err, out_file, err_file


def _restore_fds(saved_out, saved_err, out_file, err_file):
    """Flush, restore fds 1 and 2, and drain the capture files to text."""
    sys.stdout.flush()
    sys.stderr.flush()
    os.dup2(saved_out, 1)
    os.dup2(saved_err, 2)
    os.close(saved_out)
    os.close(saved_err)
    out_file.seek(0)
    captured_out = out_file.read().decode("utf-8", "replace")
    err_file.seek(0)
    captured_err = err_file.read().decode("utf-8", "replace")
    out_file.close()
    err_file.close()
    return captured_out, captured_err


def _run(code):
    """Execute one request's code and return the JSON-able response dict."""
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError:
        # A parse failure never ran anything: no captured output, just the error.
        return {
            "type": "error",
            "error": traceback.format_exc(),
            "stdout": "",
            "stderr": "",
        }

    response = {"type": "success"}
    saved_out, saved_err, out_file, err_file = _capture_fds()
    try:
        # When the final statement is a bare expression, exec everything before
        # it and eval that last node so its value can be captured and repr'd —
        # the interactive "last expression is the result" convention. Otherwise
        # exec the whole block with no value.
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last = tree.body.pop()
            if tree.body:
                exec(compile(tree, "<cairn-repl>", "exec"), _NAMESPACE)
            value = eval(
                compile(ast.Expression(last.value), "<cairn-repl>", "eval"),
                _NAMESPACE,
            )
            # None (the value of most statements-as-expressions) is noise; omit
            # `value` entirely so the caller can distinguish "no result".
            if value is not None:
                response["value"] = repr(value)
        else:
            exec(compile(tree, "<cairn-repl>", "exec"), _NAMESPACE)
    except BaseException:  # noqa: BLE001 - surface every failure to the caller
        response = {"type": "error", "error": traceback.format_exc()}
    finally:
        captured_out, captured_err = _restore_fds(saved_out, saved_err, out_file, err_file)

    response["stdout"] = captured_out
    response["stderr"] = captured_err
    return response


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
            code = request["code"]
        except (ValueError, KeyError, TypeError):
            _write_response(
                {
                    "type": "error",
                    "error": "malformed request: expected a JSON object with a `code` field",
                    "stdout": "",
                    "stderr": "",
                }
            )
            continue
        _write_response(_run(code))


if __name__ == "__main__":
    main()
