#!/usr/bin/env bash
# Rendered by Cairn for deployment embed: stop its native server.
set -uo pipefail
art="$HOME/.cairn/inference/artifacts"/'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'
run="$HOME/.cairn/inference/run"
name='cairn-embed-r0'
stop_server() {
  if [ ! -f "$run/$name.pid" ]; then return 0; fi
  pid="$(cat "$run/$name.pid")"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    case "$command" in
      ""|*"$art"*)
        kill -TERM "$pid" 2>/dev/null || true
        waited=0
        while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 30 ]; do sleep 1; waited=$((waited + 1)); done
        kill -KILL "$pid" 2>/dev/null || true
        ;;
      *) echo "pid $pid is no longer $name, so it was left running" ;;
    esac
  fi
  rm -f "$run/$name.pid"
}
stop_server
exit 0
