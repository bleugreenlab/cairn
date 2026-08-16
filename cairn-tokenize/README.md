# cairn-tokenize

An offline Rust port of the v4.7 and v5 counting model from
sanderland/ctok, pinned at commit 005430604a7abc6e08fab3fd77d582251b1800f2.

Only the v4.7 vocabulary piece strings and byte-prefix keys are retained; upstream
witness metadata is intentionally omitted. v5 uses the same measured vocabulary
with its distinct six-token frame and free whitespace tail, matching upstream.
The implementation and reduced data are derivative works under upstream's MIT
license, retained as LICENSE-ctok. Cairn's surrounding code remains BUSL-1.1.

ctok is a reconstructed estimator, not Anthropic's proprietary tokenizer.
