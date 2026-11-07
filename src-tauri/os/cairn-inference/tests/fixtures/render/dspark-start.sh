#!/usr/bin/env bash
# Rendered by Cairn from profile deepseek-v4-flash-vision-exp-dspark-x2 for deployment dsv4f.
set -euo pipefail
# rank 1 on bginf-sp2
ssh -o BatchMode=yes '10.20.20.41' 'bash -ls' <<'CAIRN_RANK_1'
set -euo pipefail
if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi
hf="${HF_HOME:-$HOME/.cache/huggingface}"
hf_get() { if [ -z "${HF_TOKEN:-}" ] && [ -s "$hf/token" ]; then HF_TOKEN="$(cat "$hf/token")"; fi; if [ -n "${HF_TOKEN:-}" ]; then curl -fsSL --retry 3 -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") -o "$2" "$1"; else curl -fsSL --retry 3 -o "$2" "$1"; fi; }
src="$HOME/.cairn/inference/sources/f3d76450f3d76450f3d76450f3d76450f3d76450"
if [ ! -d "$src/.git" ]; then rm -rf "$src.part"; mkdir -p "$src.part"; git -C "$src.part" init -q; git -C "$src.part" fetch -q --depth 1 'https://github.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark' 'f3d76450f3d76450f3d76450f3d76450f3d76450'; git -C "$src.part" checkout -q FETCH_HEAD; mv "$src.part" "$src"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json' "$f.part"; [ "$({ printf 'blob %d\0' "$(wc -c <"$f.part")"; cat "$f.part"; } | sha1sum | cut -d ' ' -f 1)" = '8b4b87fc69023e7a224eb6563753aaf3223d8b98' ] || { echo "$f.part does not match its pinned git blob" >&2; exit 1; }; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/tokenizer.json'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/tokenizer.json' "$f.part"; echo 'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00049.safetensors'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00049.safetensors' "$f.part"; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
mkdir -p "$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp'
"$rt" rm -f 'cairn-dsv4f-r1' >/dev/null 2>&1 || true
if [ "$rt" = docker ]; then gpu=(--gpus all); else gpu=(--device nvidia.com/gpu=all); fi
if [ "$rt" = podman ]; then extra=(--security-opt label=disable --group-add keep-groups); else extra=(); fi
"$rt" run -d --restart no --name 'cairn-dsv4f-r1' --ipc host --network host --shm-size '64gb' "${gpu[@]}" --ulimit 'memlock=-1:-1' "${extra[@]}" -v "$src"/'patches':'/opt/patches:ro' -v "$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp':'/root/.cache/huggingface/hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp:ro' -e 'HEADLESS=1' -e 'NCCL_CUMEM_ENABLE=0' -e 'VLLM_HOST_IP=10.0.1.2' -e 'HF_HUB_OFFLINE=1' --entrypoint '' 'ghcr.io/anemll/dspark-vllm-gx10@sha256:a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8' 'bash' '-c' '/opt/patches/apply.sh && exec vllm serve deepseek-ai/DeepSeek-V4-Flash-Vision-Exp --revision 6821d6ad3681a4b137b066b76094fa82ebd0a380 --served-model-name deepseek-ai/DeepSeek-V4-Flash-Vision-Exp --port 8888 --tensor-parallel-size 2 --nnodes 2 --node-rank 1 --master-addr 10.0.1.1 --speculative-config '\''{"method":"dspark","num_speculative_tokens":5}'\'''
CAIRN_RANK_1
# rank 0 on bginf-sp1
set -euo pipefail
if command -v docker >/dev/null 2>&1; then rt=docker; elif command -v podman >/dev/null 2>&1; then rt=podman; else echo "neither docker nor podman is installed on $(hostname)" >&2; exit 1; fi
hf="${HF_HOME:-$HOME/.cache/huggingface}"
hf_get() { if [ -z "${HF_TOKEN:-}" ] && [ -s "$hf/token" ]; then HF_TOKEN="$(cat "$hf/token")"; fi; if [ -n "${HF_TOKEN:-}" ]; then curl -fsSL --retry 3 -H @<(printf 'Authorization: Bearer %s\n' "$HF_TOKEN") -o "$2" "$1"; else curl -fsSL --retry 3 -o "$2" "$1"; fi; }
src="$HOME/.cairn/inference/sources/f3d76450f3d76450f3d76450f3d76450f3d76450"
if [ ! -d "$src/.git" ]; then rm -rf "$src.part"; mkdir -p "$src.part"; git -C "$src.part" init -q; git -C "$src.part" fetch -q --depth 1 'https://github.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark' 'f3d76450f3d76450f3d76450f3d76450f3d76450'; git -C "$src.part" checkout -q FETCH_HEAD; mv "$src.part" "$src"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/config.json' "$f.part"; [ "$({ printf 'blob %d\0' "$(wc -c <"$f.part")"; cat "$f.part"; } | sha1sum | cut -d ' ' -f 1)" = '8b4b87fc69023e7a224eb6563753aaf3223d8b98' ] || { echo "$f.part does not match its pinned git blob" >&2; exit 1; }; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/tokenizer.json'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/tokenizer.json' "$f.part"; echo 'a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
f="$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp/snapshots/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00049.safetensors'
if [ ! -f "$f" ]; then mkdir -p "$(dirname "$f")"; hf_get 'https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-Vision-Exp/resolve/6821d6ad3681a4b137b066b76094fa82ebd0a380/model-00001-of-00049.safetensors' "$f.part"; echo '0000000000000000000000000000000000000000000000000000000000000000'"  $f.part" | sha256sum -c --quiet -; mv "$f.part" "$f"; fi
mkdir -p "$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp'
"$rt" rm -f 'cairn-dsv4f-r0' >/dev/null 2>&1 || true
if [ "$rt" = docker ]; then gpu=(--gpus all); else gpu=(--device nvidia.com/gpu=all); fi
if [ "$rt" = podman ]; then extra=(--security-opt label=disable --group-add keep-groups); else extra=(); fi
"$rt" run -d --restart no --name 'cairn-dsv4f-r0' --ipc host --network host --shm-size '64gb' "${gpu[@]}" --ulimit 'memlock=-1:-1' "${extra[@]}" -v "$src"/'patches':'/opt/patches:ro' -v "$hf"/'hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp':'/root/.cache/huggingface/hub/models--deepseek-ai--DeepSeek-V4-Flash-Vision-Exp:ro' -e 'NCCL_CUMEM_ENABLE=0' -e 'VLLM_HOST_IP=10.0.1.1' -e 'HF_HUB_OFFLINE=1' --entrypoint '' 'ghcr.io/anemll/dspark-vllm-gx10@sha256:a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8' 'bash' '-c' '/opt/patches/apply.sh && exec vllm serve deepseek-ai/DeepSeek-V4-Flash-Vision-Exp --revision 6821d6ad3681a4b137b066b76094fa82ebd0a380 --served-model-name deepseek-ai/DeepSeek-V4-Flash-Vision-Exp --port 8888 --tensor-parallel-size 2 --nnodes 2 --node-rank 0 --master-addr 10.0.1.1 --speculative-config '\''{"method":"dspark","num_speculative_tokens":5}'\'''
while true; do
  if curl -fsS -o /dev/null --max-time 5 'http://127.0.0.1:8888/v1/models'; then exit 0; fi
  if [ "$("$rt" inspect -f '{{.State.Running}}' 'cairn-dsv4f-r0' 2>/dev/null)" != true ]; then
    echo "container cairn-dsv4f-r0 exited before it was healthy; its last errors:"
    "$rt" logs --tail 400 'cairn-dsv4f-r0' 2>&1 | grep -E 'Error|error|Exception' | tail -n 6 || "$rt" logs --tail 10 'cairn-dsv4f-r0' 2>&1
    exit 1
  fi
  sleep 5
done
