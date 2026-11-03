# Benchmark data notice

`ladder.json` and the anchors in `anchors/` hold Cairn's benchmark levels: per-cell aggregates of public benchmark results, and the abilities, difficulties, subject offsets, residuals and levels fitted jointly from all of them (`bench/neighbours/ladder.py`, cairn/5863).

Because the fitted values derive from every source below together, and some sources are share-alike, this data as a whole is an adaptation licensed under CC-BY-SA-4.0 (`LICENSES/CC-BY-SA-4.0.txt`). Each source's own results keep their own license, which each cell of `ladder.json` names.

Changes made: per-item results were aggregated into solve rates per model, configuration, scaffold and benchmark, then fitted by an additive logit model; no item text is included. Reference item identifiers in `anchors/` are the benchmarks' public task names.

## Sources

- **aims**: AIMS measurement database, AIMS Foundations (aims-foundations/measurement-db). License: CC-BY-SA-4.0 (`LICENSES/CC-BY-SA-4.0.txt`). <https://huggingface.co/datasets/aims-foundations/measurement-db>. Share-alike.
- **arc_agi**: ARC-AGI results, ARC Prize Foundation (huggingface.co/arcprize). License: MIT (`LICENSES/MIT.txt`). <https://huggingface.co/arcprize>.
- **bfcl**: Berkeley Function Calling Leaderboard results, Gorilla project, UC Berkeley (HuanzhiMao/BFCL-Result). License: Apache-2.0 (`LICENSES/Apache-2.0.txt`). <https://github.com/HuanzhiMao/BFCL-Result>.
- **epoch_swebench**: Epoch AI, 'Capabilities & benchmarking', epoch.ai/benchmarks: SWE-bench Verified runs under Epoch's bash agent. License: CC-BY-4.0 (`LICENSES/CC-BY-4.0.txt`). <https://epoch.ai/data/benchmark_data.zip>.
- **hal_matrix**: HAL response matrix, Azam 2026 (razam2/hal-response-matrix), from the Holistic Agent Leaderboard, Kapoor et al. 2026. License: CC-BY-4.0 (`LICENSES/CC-BY-4.0.txt`). <https://huggingface.co/datasets/razam2/hal-response-matrix>.
- **matharena**: MathArena ArXivMath outputs, MathArena, ETH Zurich SRI Lab (huggingface.co/MathArena). License: CC-BY-SA-4.0 (`LICENSES/CC-BY-SA-4.0.txt`). <https://huggingface.co/MathArena>. Share-alike.
- **openhands_index**: OpenHands Index, OpenHands (OpenHands/openhands-index). License: Apache-2.0 (`LICENSES/Apache-2.0.txt`). <https://huggingface.co/datasets/OpenHands/openhands-index>.
- **terminal_bench**: Terminal-Bench 2.0 leaderboard submissions, Harbor Framework (harborframework/terminal-bench-2-leaderboard). License: Apache-2.0 (`LICENSES/Apache-2.0.txt`). <https://huggingface.co/datasets/harborframework/terminal-bench-2-leaderboard>.
- **wild**: WILD, Kensho Technologies (kensho/WILD). License: Apache-2.0 (`LICENSES/Apache-2.0.txt`). <https://huggingface.co/datasets/kensho/WILD>.
