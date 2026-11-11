#!/usr/bin/env bash
# Rendered by Cairn from profile qwen3-embedding-4b-q8_0-llamacpp-metal-x1 for deployment embed: llama.cpp b12283, run natively.
set -euo pipefail
hf="${HF_HOME:-$HOME/.cache/huggingface}"
hf_get() { if [ -z "${HF_TOKEN:-}" ] && [ -s "$hf/token" ]; then HF_TOKEN="$(cat "$hf/token")"; fi; if [ -n "${HF_TOKEN:-}" ]; then curl -fsSL --retry 3 -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") -o "$2" "$1"; else curl -fsSL --retry 3 -o "$2" "$1"; fi; }
f="$hf"/'hub/models--Qwen--Qwen3-Embedding-4B-GGUF/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/Qwen/Qwen3-Embedding-4B-GGUF/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf' "$f.part"; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
mkdir -p "$hf"/'hub/models--Qwen--Qwen3-Embedding-4B-GGUF'
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
if [ ! -f "$art/.complete" ]; then
  rm -rf "$art"
  mkdir -p "$art"
  curl -fsSL --retry 3 -o "$art"/'llama-b12283-bin-macos-arm64.tar.gz' 'https://github.com/ggml-org/llama.cpp/releases/download/b12283/llama-b12283-bin-macos-arm64.tar.gz'
  echo 'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'"  ""$art"/'llama-b12283-bin-macos-arm64.tar.gz' | sha256sum -c --quiet -
  tar -xzf "$art"/'llama-b12283-bin-macos-arm64.tar.gz' -C "$art"
  rm -f "$art"/'llama-b12283-bin-macos-arm64.tar.gz'
  touch "$art/.complete"
fi
stop_server
mkdir -p "$run"
cd "$run"
nohup env -i 'HOME='"$HOME" 'PATH=/usr/bin:/bin:/usr/sbin:/sbin' 'TMPDIR='"${TMPDIR:-/tmp}" 'HF_HOME='"$hf" 'HF_HUB_OFFLINE=1' "$art"/'llama-b12283/llama-server' '-m' "$hf"'/hub/models--Qwen--Qwen3-Embedding-4B-GGUF/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf' '--alias' 'Qwen/Qwen3-Embedding-4B' '--host' '0.0.0.0' '--port' '8090' '--embeddings' '--pooling' 'last' >"$run/$name.log" 2>&1 </dev/null &
pid=$!
echo "$pid" >"$run/$name.pid"
while true; do
  if curl -fsS -o /dev/null --max-time 5 'http://127.0.0.1:8090/v1/models'; then exit 0; fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server $name exited before it was healthy; its last errors:"
    grep -E 'Error|error|Exception' "$run/$name.log" | tail -n 6 || tail -n 10 "$run/$name.log"
    rm -f "$run/$name.pid"
    exit 1
  fi
  sleep 5
done
