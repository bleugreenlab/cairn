#!/usr/bin/env bash
# Rendered by Cairn from recipe qwen3-embedding-4b-q8_0-llamacpp-x1 for deployment embed.
set -euo pipefail
# rank 0 on fedora
set -euo pipefail
if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi
hf="${HF_HOME:-$HOME/.cache/huggingface}"
mkdir -p "$hf"
f="$hf"'/hub/models--Qwen--Qwen3-Embedding-4B-GGUF/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; curl -fsSL --retry 3 -o "$f.part" 'https://huggingface.co/Qwen/Qwen3-Embedding-4B-GGUF/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf'; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
"$rt" rm -f 'cairn-embed-r0' >/dev/null 2>&1 || true
if [ "$rt" = podman ]; then extra=(--security-opt label=disable --group-add keep-groups); else extra=(); fi
"$rt" run -d --restart no --name 'cairn-embed-r0' -p 8090:8090 --device /dev/dri "${extra[@]}" -v "$hf":'/models:ro' 'ghcr.io/ggml-org/llama.cpp@sha256:a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8' '-m' '/models/hub/models--Qwen--Qwen3-Embedding-4B-GGUF/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/Qwen3-Embedding-4B-Q8_0.gguf' '--alias' 'Qwen/Qwen3-Embedding-4B' '--host' '0.0.0.0' '--port' '8090' '--embeddings' '--pooling' 'last'
while true; do
  if curl -fsS -o /dev/null --max-time 5 'http://127.0.0.1:8090/v1/models'; then exit 0; fi
  if [ "$("$rt" inspect -f '{{.State.Running}}' 'cairn-embed-r0' 2>/dev/null)" != true ]; then
    echo "container cairn-embed-r0 exited before it was healthy; its last errors:"
    "$rt" logs --tail 400 'cairn-embed-r0' 2>&1 | grep -E 'Error|error|Exception' | tail -n 6 || "$rt" logs --tail 10 'cairn-embed-r0' 2>&1
    exit 1
  fi
  sleep 5
done
