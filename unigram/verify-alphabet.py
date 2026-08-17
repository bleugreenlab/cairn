# /// script
# requires-python = ">=3.10"
# dependencies = ["tiktoken", "transformers", "sentencepiece", "protobuf"]
# ///
"""Re-check every ALPHABET entry against the tokenizers `unigram` claims.

The crate's own tests gate single-token cost under Claude only, via the in-repo
`cairn-tokenize`. This script covers the other four families the documentation
claims, whose vocabularies are far too large to vendor for a gate. Run it after
ANY edit to the ALPHABET table in src/lib.rs -- a green `cargo test` does not
establish the cross-tokenizer property.

    uv run src-tauri/os/unigram/verify-alphabet.py

Exits non-zero and names every offending entry if the property no longer holds.
Requires network access on first run to fetch the vocabularies.
"""

import pathlib
import re
import sys

LIB = pathlib.Path(__file__).parent / "src" / "lib.rs"
BPE_FAMILIES = ("o200k_base", "cl100k_base", "p50k_base", "r50k_base")
LLAMA = "hf-internal-testing/llama-tokenizer"
EXPECTED = 256


def alphabet() -> list[str]:
    """Parse the ALPHABET table out of the crate source.

    Read from the source rather than duplicated here on purpose: a copy would
    drift, and a checker that verifies a stale copy is worse than none.
    """
    source = LIB.read_text()
    marker = "pub const ALPHABET: [&str; "
    body = source.split(marker, 1)[1].split("= [", 1)[1].split("];", 1)[0]
    return re.findall(r'"([a-z]+)"', body)


def main() -> int:
    words = alphabet()
    if len(words) != EXPECTED:
        print(f"FAIL: expected {EXPECTED} entries, parsed {len(words)}")
        return 1
    if len(set(words)) != len(words):
        print("FAIL: duplicate entries")
        return 1

    failures: list[tuple[str, str]] = []

    import tiktoken

    for family in BPE_FAMILIES:
        encoding = tiktoken.get_encoding(family)
        # Space-prefixed: that is the position every word occupies inside an
        # encoded value, and the form the vocabularies hold canonically.
        bad = [word for word in words if len(encoding.encode(" " + word)) != 1]
        print(f"{family:14s} {len(words) - len(bad):3d}/{len(words)} one token")
        failures += [(family, word) for word in bad]

    from transformers import AutoTokenizer

    llama = AutoTokenizer.from_pretrained(LLAMA)
    bad = [word for word in words if len(llama.tokenize(" " + word)) != 1]
    print(f"{'llama-sp':14s} {len(words) - len(bad):3d}/{len(words)} one token")
    failures += [("llama-sp", word) for word in bad]

    if failures:
        print("\nFAIL: entries costing more than one token:")
        for family, word in failures:
            print(f"  {family}: {word}")
        return 1

    print(f"\nOK: all {len(words)} entries are one token under every family checked.")
    print("Claude is gated separately by `cargo test -p unigram`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
