# Golden single-queue table (sanity check of the CTMC solver only)

`single_queue_v1.csv` was captured on 2026-10-28, in phase 1.0, **before** the PoC's queueing
formulas were deleted (PLAN §2, D1). It is the oracle's source, nothing else: phase 2's
single-queue birth-death chain must match every row to 1e-6 relative or 1e-12 absolute (CR-027).

Provenance:
- PoC source: `parity-poc/parity-poc/src/differential_evolution.cpp`
  (sha256 `73246124bc50337e32ac0b5bd1d02edc2364fb72fac4ced02540d46e488c3284`) and
  `include/differential_evolution.h` (sha256 `8f4500823aba2cb615966edec80af40b800f769d0cc23e79c5023ea783b8cf46`),
  both in the verified archive `/home/cristifg/Repos/parity-archives/parity-poc-pre-rename-2026-10-28/`.
- A scratch harness compiled only that file (`g++ 15.2.1 -std=c++23 -O2`, no libtorch) and printed
  each row with `%.17g`.
- Grid: N in {1, 2, 5, 10, 25, 50, 100, 250, 500}; offered load rho = lambda/(N mu) in {0.5, 0.8,
  0.95, 1.0, 1.1, 1.5}; AHT in {180, 420} s (mu = 1/AHT); patience in {60, 120, 300} s
  (theta = 1/patience). 324 rows with `source = erlang_a` (the PoC's `erlang_a(N, lambda, mu,
  patience)`), plus 54 rows with `source = erlang_c` (theta = 0, rho < 1: `erlang_c_pw` for
  `p_delay` and `compute_asa` for `asa_s`), because the PoC never has theta = 0. lambda = 0 rows are
  excluded (the PoC returns 9999 s there; the new model returns 0).
- Definitions (stationary): `asa_s` = Lq / lambda (every arrival, abandoners included),
  `abandon` = theta Lq / lambda, `p_delay` = P(n >= N).

Columns: `source,N,lambda,mu,theta,asa_s,abandon,p_delay`.
