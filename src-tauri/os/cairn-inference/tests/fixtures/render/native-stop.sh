#!/usr/bin/env bash
# Rendered by Cairn for deployment embed: stop its native server.
set -uo pipefail
art="$HOME/.cairn/inference/artifacts"/'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'
run="$HOME/.cairn/inference/run"
name='cairn-embed-r0'
launched_at() { LC_ALL=C ps -p "$1" -o lstart= 2>/dev/null | tr -s ' ' || true; }
stop_server() {
  if [ ! -f "$run/$name.pid" ]; then return 0; fi
  pid="$(sed -n 1p "$run/$name.pid")"
  launched="$(sed -n 2p "$run/$name.pid")"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    if [ -n "$launched" ]; then
      case "$(launched_at "$pid")" in ""|"$launched") ours=yes ;; *) ours=no ;; esac
    else
      case "$(ps -p "$pid" -o command= 2>/dev/null || true)" in ""|*"$art"*) ours=yes ;; *) ours=no ;; esac
    fi
    if [ "$ours" = no ]; then
      echo "pid $pid is no longer $name, so it was left running"
    else
      kill -TERM "$pid" 2>/dev/null || true
      waited=0
      while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge 540 ]; then
          echo "server $name (pid $pid) had not exited 540s after TERM, so it was left running, still holding its memory, rather than killed: killing a process that holds a large wired model can strand that memory until a reboot. Stop it once its requests finish" >&2
          return 1
        fi
        sleep 1
        waited=$((waited + 1))
      done
    fi
  fi
  rm -f "$run/$name.pid"
}
stop_server || exit 1
exit 0
