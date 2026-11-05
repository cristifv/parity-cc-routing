# parked/nnmpc/ (not built): the PoC's NNMPC, kept for its rewrite

The user (2026-10-28, via the coordinator): "it should also predict using the NNMPC". The PoC's
NNMPC (transformer predictor + model-predictive controller) is **kept** and rewritten as the
second predictor and planner of the optimization layer, next to the CTMC (PLAN §3.8, §5.9; phase
5c). Nothing here is compiled, linked or tested; the Bazel build never globs `parked/`.

Restored on 2026-10-28 from the verified archive
`/home/cristifg/Repos/parity-archives/parity-poc-pre-rename-2026-10-28/` (phase 1.0 had deleted the
controller and the model files under the earlier plan); every file below matches the archive's
`MANIFEST.sha256` byte for byte (28 files checked).

| Path | What | PoC path |
|---|---|---|
| `controller/nnmpc_controller.{h,cpp}` | The MPC loop (a simulated 15-min staffing loop in the PoC; review 2026-10-22 §5: crashes) | `parity-poc/{include,src}/` |
| `predictor/` | `transformer_predictor`, `pretrain`, `norm_params` (the transformer and its training) | `parity-poc/{include,src}/` |
| `models/nnmpc_transformer.pt`, `models/norm_params.bin` | The committed TorchScript model and its normalization (they don't match the config: reference only; a TorchScript file can carry code, so it is **never loaded** by RE, PLAN §5.9) | `models/` |
| `data/*.csv` | The synthetic data the PoC trained on (`agent_performance.csv` has per-agent rows: never used for routing decisions, AI Act) | `data/` |
| `poc-deps/` | Headers and sources the controller includes, for reading it (agent, data_manager, event_producer, metric, metrics_stream, gamification, kpi, performance_record, site_config, acd, main.cpp) | `parity-poc/{include,src}/` |

**Not restored (D1, decided):** the Erlang code the controller called
(`differential_evolution.*` with `compute_erlang_a`, used at `nnmpc_controller.cpp:439`;
`jde_optimizer.*`; `markov_allocator.*`; `app_config.h` with `use_erlang_a`; `data_generator.h`).
The rewrite replaces that queue update with the CTMC/simulator (PLAN §3.8). They stay in the archive.
`controller/nnmpc_controller.cpp` is the only file in this repository outside `tests/golden/` that
still contains the word; `scripts/ci.sh`'s gate covers `src/`, `model/`, `sim/`.
