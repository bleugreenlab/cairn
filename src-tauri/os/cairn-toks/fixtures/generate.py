# /// script
# requires-python = ">=3.10"
# dependencies = ["tokenizers==0.23.2"]
# ///
"""Generate the offline tokenizer fixtures for cairn-toks' tests.

Run from this directory with `uv run generate.py`. Every fixture is trained here
from the small corpus below, so nothing is downloaded and the files carry no
third-party vocabulary. `golden.json` records the ids Hugging Face tokenizers
0.23.2 (the version toks claims parity with) returns for each probe with
`add_special_tokens=False`, which is the content-token count cairn-toks
measures. Regenerating rewrites every file; commit them together.
"""
import json
import os

from tokenizers import AddedToken, Tokenizer, decoders, models, normalizers, pre_tokenizers, trainers

HERE = os.path.dirname(os.path.abspath(__file__))

CORPUS = [
    "The quick brown fox jumps over the lazy dog.",
    "Token budgets matter most where context windows are smallest.",
    "fn main() { println!(\"hello, world\"); }",
    "def count(text):\n    return len(text.split())\n",
    "SELECT id, name FROM users WHERE active = 1 ORDER BY name;",
    "日本語のテキストと漢字を含む文。",
    "café naïve résumé über straße",
    "snake_case::path/to/thing.rs:42 and CamelCaseIdentifier",
    "   indented    text\twith\ttabs\n\nand blank lines",
    "Numbers 1234567890 and 3.14159 and 2026-10-17.",
    "don't won't I'LL WE'VE they're it's",
    "\U0001F600 \U0001F680 emoji and symbols ✦ αβγ",
] * 8

# Added tokens: one special, one not. Their presence exercises toks' added-token
# matching and widens the certified-cut window cairn-toks derives at load.
ADDED = [
    AddedToken("<|end|>", special=True, normalized=False),
    AddedToken("<tool_call>", special=False, normalized=False),
]

PROBES = [
    "",
    "a",
    "Hello, world!",
    "The quick brown fox jumps over the lazy dog.",
    "fn main() {\n    println!(\"hi\");\n}\n",
    "日本語のテキストと漢字",
    "café vs café — combining marks",
    "   leading and   multiple    spaces\t\ttabs\n\n\n",
    "\U0001F600\U0001F680 emoji 1234567890",
    "don't won't I'LL WE'VE",
    "before <|end|> after <tool_call>{\"a\":1}",
    "unseen words: zygomorphic quixotry xylophonist",
]


def byte_level(nfc: bool) -> Tokenizer:
    tok = Tokenizer(models.BPE())
    if nfc:
        tok.normalizer = normalizers.NFC()
    tok.pre_tokenizer = pre_tokenizers.ByteLevel(add_prefix_space=False, use_regex=True)
    tok.decoder = decoders.ByteLevel()
    trainer = trainers.BpeTrainer(
        vocab_size=600,
        initial_alphabet=pre_tokenizers.ByteLevel.alphabet(),
        show_progress=False,
    )
    tok.train_from_iterator(CORPUS, trainer=trainer)
    tok.add_tokens(ADDED)
    return tok


def metaspace() -> Tokenizer:
    tok = Tokenizer(models.BPE(byte_fallback=True))
    tok.pre_tokenizer = pre_tokenizers.Metaspace(replacement="▁", prepend_scheme="first", split=False)
    tok.decoder = decoders.Sequence(
        [decoders.Replace("▁", " "), decoders.ByteFallback(), decoders.Fuse(), decoders.Strip(" ", 1, 0)]
    )
    byte_tokens = [f"<0x{b:02X}>" for b in range(256)]
    trainer = trainers.BpeTrainer(vocab_size=700, special_tokens=byte_tokens, show_progress=False)
    tok.train_from_iterator(CORPUS, trainer=trainer)
    tok.add_tokens(ADDED)
    return tok


def write(name: str, tok: Tokenizer) -> dict:
    path = os.path.join(HERE, f"{name}.json")
    tok.save(path, pretty=False)
    return {
        "file": f"{name}.json",
        "probes": [{"text": text, "ids": tok.encode(text, add_special_tokens=False).ids} for text in PROBES],
    }


def main() -> None:
    golden = {
        "tokenizers": "0.23.2",
        "fixtures": {
            "bytelevel": write("bytelevel", byte_level(nfc=False)),
            "bytelevel_nfc": write("bytelevel_nfc", byte_level(nfc=True)),
            "metaspace": write("metaspace", metaspace()),
        },
    }

    # A valid tokenizer.json using a feature toks refuses at load: BPE dropout
    # makes encoding random, so no exact count exists. hf itself loads it.
    refused = json.load(open(os.path.join(HERE, "bytelevel.json")))
    refused["model"]["dropout"] = 0.1
    with open(os.path.join(HERE, "refused_dropout.json"), "w") as handle:
        json.dump(refused, handle, ensure_ascii=False, separators=(",", ":"))

    with open(os.path.join(HERE, "golden.json"), "w") as handle:
        json.dump(golden, handle, ensure_ascii=False, indent=1)
        handle.write("\n")


if __name__ == "__main__":
    main()
