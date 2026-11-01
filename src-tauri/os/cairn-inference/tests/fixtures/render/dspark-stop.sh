#!/usr/bin/env bash
# Rendered by Cairn for deployment dsv4f: remove every rank's container.
set -uo pipefail
if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi
"$rt" rm -f 'cairn-dsv4f-r0' >/dev/null 2>&1 || true
ssh -o BatchMode=yes '10.20.20.41' 'bash -lc '\''if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi; "$rt" rm -f '\''\'\'''\''cairn-dsv4f-r1'\''\'\'''\'' >/dev/null 2>&1 || true'\''' || echo 'could not reach bginf-sp2 to remove rank 1'
exit 0
