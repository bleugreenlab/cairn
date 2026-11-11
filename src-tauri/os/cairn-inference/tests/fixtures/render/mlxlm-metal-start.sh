#!/usr/bin/env bash
# Rendered by Cairn from profile qwen3.6-35b-a3b-4bit-mlxlm-metal-x1 for deployment chat: MLX 0.32.2, run natively.
set -euo pipefail
hf="${HF_HOME:-$HOME/.cache/huggingface}"
hf_get() { if [ -z "${HF_TOKEN:-}" ] && [ -s "$hf/token" ]; then HF_TOKEN="$(cat "$hf/token")"; fi; if [ -n "${HF_TOKEN:-}" ]; then curl -fsSL --retry 3 -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") -o "$2" "$1"; else curl -fsSL --retry 3 -o "$2" "$1"; fi; }
f="$hf"/'hub/models--mlx-community--Qwen3.6-35B-A3B-4bit/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/mlx-community/Qwen3.6-35B-A3B-4bit/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json' "$f.part"; [ "$({ printf 'blob %d\0' "$(wc -c <"$f.part")"; cat "$f.part"; } | sha1sum | cut -d ' ' -f 1)" = '8b4b87fc69023e7a224eb6563753aaf3223d8b98' ] || { echo "$f.part does not match its pinned git blob" >&2; exit 1; }; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--mlx-community--Qwen3.6-35B-A3B-4bit/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00004.safetensors'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/mlx-community/Qwen3.6-35B-A3B-4bit/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00004.safetensors' "$f.part"; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--mlx-community--Qwen3.6-35B-A3B-4bit/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00002-of-00004.safetensors'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/mlx-community/Qwen3.6-35B-A3B-4bit/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00002-of-00004.safetensors' "$f.part"; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
mkdir -p "$hf"/'hub/models--mlx-community--Qwen3.6-35B-A3B-4bit'
art="$HOME/.cairn/inference/artifacts"/'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'
run="$HOME/.cairn/inference/run"
name='cairn-chat-r0'
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
if [ ! -f "$art/.complete" ]; then
  rm -rf "$art"
  mkdir -p "$art"
  curl -fsSL --retry 3 -o "$art"/'mlx_lm-0.32.2-py3-none-any.whl' 'https://files.pythonhosted.org/packages/d1/4f/000eefdeb41388782b47e7f39904cf2d9e7e49a60df5e172097c358dba8e/mlx_lm-0.32.2-py3-none-any.whl'
  echo 'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'"  ""$art"/'mlx_lm-0.32.2-py3-none-any.whl' | sha256sum -c --quiet -
  if [ -x "${CAIRN_HOME:-$HOME/.cairn}/bin/uv" ]; then uv="${CAIRN_HOME:-$HOME/.cairn}/bin/uv"; elif command -v uv >/dev/null 2>&1; then uv=uv; else echo "uv is not installed on $(hostname): Cairn provisions it with the executor, so update the executor" >&2; exit 1; fi
  "$uv" venv -q --python '3.12' "$art/env"
  VIRTUAL_ENV="$art/env" "$uv" pip install -q --exclude-newer '2026-10-17T00:00:00Z' "$art"/'mlx_lm-0.32.2-py3-none-any.whl'
  touch "$art/.complete"
fi
stop_server
mkdir -p "$run"
cd "$run"
nohup env -i 'HOME='"$HOME" 'PATH=/usr/bin:/bin:/usr/sbin:/sbin' 'TMPDIR='"${TMPDIR:-/tmp}" 'HF_HOME='"$hf" 'HF_HUB_OFFLINE=1' "$art/env/bin/python" -c 'import sys
import mlx_lm.server as server
served = sys.argv.pop(1)
serve = server.run

def run(host, port, provider, *rest, **named):
    for table in (provider._model_map, provider._adapter_map, provider._draft_model_map):
        table[served] = table['\''default_model'\'']
    provider.load_default()
    return serve(host, port, provider, *rest, **named)

server.run = run
sys.argv[0] = '\''mlx_lm.server'\''
server.main()
' 'Qwen/Qwen3.6-35B-A3B' '--model' "$hf"'/hub/models--mlx-community--Qwen3.6-35B-A3B-4bit/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380' '--host' '0.0.0.0' '--port' '8080' >"$run/$name.log" 2>&1 </dev/null &
pid=$!
echo "$pid" >"$run/$name.pid"
while true; do
  if curl -fsS -o /dev/null --max-time 5 'http://127.0.0.1:8080/v1/models'; then exit 0; fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server $name exited before it was healthy; its last errors:"
    grep -E 'Error|error|Exception' "$run/$name.log" | tail -n 6 || tail -n 10 "$run/$name.log"
    rm -f "$run/$name.pid"
    exit 1
  fi
  sleep 5
done
