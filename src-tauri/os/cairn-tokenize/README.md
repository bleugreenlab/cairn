# cairn-tokenize

An offline Rust port of the v4.7 and v5 counting model from
sanderland/ctok, pinned at commit 70872fd4981dc5990267923235b4114535bb4473.

`data/pieces_v4_7.json` is a reduced projection of that commit's v4.7
vocabulary: the piece strings, the byte-prefix keys and the contraction
suffixes, with upstream's witness metadata (the probe that pins each piece)
omitted. v5 reuses the same measured vocabulary under its own six-token frame
and free whitespace tail, matching upstream; v3 is deliberately not imported.

Two examples carry a repin. `cargo run --example vocab -- <ctok checkout>`
rewrites the reduced file with `--write` and otherwise checks the embedded copy
against a checkout, comparing the piece, byte-prefix and contraction sets
independently and the commit the data claims against the one the checkout is
on. `cargo run --example audit` reads `{"text", "v47", "v5"}` rows of upstream
counts on stdin and reports where this port disagrees.

The implementation and reduced data are derivative works under upstream's MIT
license, retained as LICENSE-ctok. Cairn's surrounding code remains BUSL-1.1.

ctok is a reconstructed estimator, not Anthropic's proprietary tokenizer.
