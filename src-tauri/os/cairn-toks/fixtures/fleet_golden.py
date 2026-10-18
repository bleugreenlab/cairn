# /// script
# requires-python = ">=3.10"
# dependencies = ["tokenizers==0.23.2", "tiktoken==0.12.0"]
# ///
"""Reference counts for the fleet test (`tests/fleet.rs`).

Run from this directory with `uv run fleet_golden.py`. For every pin in
`cairn-models-types/src/tokenizers.rs` it downloads the pinned files at the
pinned revision into a scratch directory, verifies their sha256, and records
the reference token count of each probe: Hugging Face tokenizers 0.23.2 with
`add_special_tokens=False` for a tokenizer.json, and for Kimi the reference its
own wrapper uses, tiktoken over `tiktoken.model` with the pattern its
`tokenization_kimi.py` declares (read as text here, never executed). The
probes contain no special-token strings, where the wrapper's handling would
matter. Only counts are committed; the fleet test fails for a pin with no
row, so a new pin cannot skip this step.
"""
import hashlib
import json
import os
import re
import tempfile
import urllib.request

import tiktoken
from tiktoken.load import load_tiktoken_bpe
from tokenizers import Tokenizer

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "tests", "fleet_golden.json")

# (family, repo, revision, digest, files) mirrored from tokenizers.rs.
PINS = [
    ("glm-5", "zai-org/GLM-5.3", "ba32f8c19c8738e2c57c71798c601d477a7bf844",
     "19e773648cb4e65de8660ea6365e10acca112d42a854923df93db4a6f333a82d", ["tokenizer.json"]),
    ("kimi-k3", "moonshotai/Kimi-K3", "f831ab66814297da540d832a5235f8e904f29d06",
     "9daf23e519d7119b3cda6ba28922449503b6d4a0b3749f8c97d173a111490748",
     ["encoding_k3.py", "tiktoken.model", "tokenization_kimi.py", "tokenizer_config.json"]),
    ("kimi-k2.5", "moonshotai/Kimi-K2.5", "4d01dfe0332d63057c186e0b262165819efb6611",
     "c1dde261820d8bb00602430e4f28d154b71cb74308abadaa4f10fd8bea532221",
     ["tiktoken.model", "tokenization_kimi.py", "tokenizer_config.json"]),
    ("deepseek-v4.1", "deepseek-ai/DeepSeek-V4.1-Flash", "314113f8a7dadb388215396ba5fbee2c610a04d6",
     "c90dfa01249db1be4245780a052ede752e1361c612ac6d08e2bdada7d599476b", ["tokenizer.json"]),
    ("deepseek-v4", "deepseek-ai/DeepSeek-V4-Flash", "60d8d70770c6776ff598c94bb586a859a38244f1",
     "8f9f37ca37fdc4f5fd36d5cf4d3b0e8392edb4e894fd10cc0d70b4957c8633cf", ["tokenizer.json"]),
    ("deepseek-v3.2", "deepseek-ai/DeepSeek-V3.2", "a7e62ac04ecb2c0a54d736dc46601c5606cf10a6",
     "cd050be35cae877f8f0aa847f45aa87e23835a56ca32b29b28545597852784e5", ["tokenizer.json"]),
    ("qwen3.8", "Qwen/Qwen3.8-27B", "1d4bf0f2ff6012fd82039f2fa52739d0dd7c60c0",
     "0997f410c57a1f4e53b09e4be8f4a172d90edd9564368fb0847030937229b9f3", ["tokenizer.json"]),
    ("qwen3.8", "unsloth/Qwen3.8-27B-NVFP4", "f0b7c9e722f5565102fff8481c99e4d86ae099c7",
     "06b9509352d2af50381ab2247e083b80d32d5c0aba91c272ca9ff729b6a0e523", ["tokenizer.json"]),
    ("gemma-4", "google/gemma-4-12B-it", "707f0a3b8a3c7ad586ed01e27eafbad8a27dd0f7",
     "cc8d3a0ce36466ccc1278bf987df5f71db1719b9ca6b4118264f45cb627bfe0f", ["tokenizer.json"]),
    ("minimax-m3", "MiniMaxAI/MiniMax-M3", "f0e1c1e04d40177e4673a22097036854f536e9c0",
     "bb1f1626cf01448f1e3b6036d0a061ffc66c91d9046aada14ea23a5441b5ad6e", ["tokenizer.json"]),
    ("mimo-v2.5", "XiaomiMiMo/MiMo-V2.5", "63651580ca774f8504f676040460aed3e1244ac1",
     "633518aad78f9f61bae2ae420d621215754a4424c918b052cd8c22a3b59e99d2", ["tokenizer.json"]),
]

# Each probe is `text` repeated `repeat` times; the long ones cross the meter's
# 64 KiB part size, so its certified-cut sums are checked on real files.
PROBES = [
    {"text": "Hello, world!", "repeat": 1},
    {"text": "The quick brown fox jumps over the lazy dog. ", "repeat": 3},
    {"text": "fn main() {\n    let x: Vec<u32> = (0..10).collect();\n    println!(\"{x:?}\");\n}\n", "repeat": 1},
    {"text": "def count(text):\n\treturn len(text.split())  # tabs and comments\n", "repeat": 1},
    {"text": "日本語のテキストと漢字、中文句子。", "repeat": 1},
    {"text": "café vs café, Ångström vs Ångström — NFC", "repeat": 1},
    {"text": "   leading and   multiple    spaces\t\ttabs\n\n\n  trailing  ", "repeat": 1},
    {"text": "\U0001F600\U0001F680 emoji ✦ αβγ 1234567890 3.14159", "repeat": 1},
    {"text": "don't won't I'LL WE'VE they're it's", "repeat": 1},
    {"text": "SELECT id, name FROM users WHERE active = 1;\n", "repeat": 1},
    {"text": "नमस्ते مرحبا 안녕하세요 Привет", "repeat": 1},
    {"text": "Token budgets matter most where context windows are smallest: 日本語, code `x += 1;`, and café.\n", "repeat": 1500},
]


def fetch(repo, revision, files, digest, root):
    directory = os.path.join(root, digest)
    os.makedirs(directory, exist_ok=True)
    for name in files:
        path = os.path.join(directory, name)
        if not os.path.exists(path):
            urllib.request.urlretrieve(f"https://huggingface.co/{repo}/resolve/{revision}/{name}", path)
    lines = sorted(f"{name}\0{hashlib.sha256(open(os.path.join(directory, name), 'rb').read()).hexdigest()}\n" for name in files)
    actual = (
        hashlib.sha256("".join(lines).encode()).hexdigest()
        if len(files) > 1
        else lines[0].split("\0")[1].strip()
    )
    assert actual == digest, f"{repo}: {actual} is not the pinned {digest}"
    return directory


def kimi_reference(directory):
    source = open(os.path.join(directory, "tokenization_kimi.py"), encoding="utf-8").read()
    body = source[source.index('pat_str = "|".join(['):]
    body = body[: body.index("])")]
    pattern = "|".join(re.findall(r'r"""(.*?)"""', body, re.S))
    ranks = load_tiktoken_bpe(os.path.join(directory, "tiktoken.model"))
    encoding = tiktoken.Encoding(name="kimi", pat_str=pattern, mergeable_ranks=ranks, special_tokens={})
    return lambda text: len(encoding.encode_ordinary(text))


def main():
    root = os.environ.get("FLEET_CACHE") or tempfile.mkdtemp(prefix="cairn-fleet-")
    counts = {}
    for family, repo, revision, digest, files in PINS:
        directory = fetch(repo, revision, files, digest, root)
        if files == ["tokenizer.json"]:
            tokenizer = Tokenizer.from_file(os.path.join(directory, "tokenizer.json"))
            count = lambda text, t=tokenizer: len(t.encode(text, add_special_tokens=False).ids)
        else:
            count = kimi_reference(directory)
        counts[digest] = {
            "family": family,
            "repo": repo,
            "counts": [count(probe["text"] * probe["repeat"]) for probe in PROBES],
        }
        print(family, repo, counts[digest]["counts"])
    with open(OUT, "w", encoding="utf-8") as handle:
        json.dump({"probes": PROBES, "pins": counts}, handle, ensure_ascii=False, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
