#!/usr/bin/env python3
"""Judges one tests/load/run.sh scenario (phase 7, PLAN §7 "Load", §6.1) from the driver's summary,
RE's /metrics at the end and the process samples; writes report.json and prints one line per check.
  judge.py <scenario> <workdir> <targets.json>
Exit 0 when every check passes.
"""
import json
import sys

scenario, w, targets_path = sys.argv[1], sys.argv[2], sys.argv[3]
T = json.load(open(targets_path))
S = json.load(open(f"{w}/summary.json"))
samples = [json.loads(x) for x in open(f"{w}/samples.jsonl") if x.strip()]
metrics = {}
hist = []
inbox = []
for line in open(f"{w}/metrics.txt"):
    if line.startswith("#") or not line.strip():
        continue
    name, val = line.rsplit(" ", 1)
    if name.startswith("parity_cc_routing_offer_decision_seconds_bucket") or name.startswith("parity_cc_routing_writer_inbox_wait_seconds_bucket"):
        le = name.split('le="')[1].split('"')[0]
        (hist if "decision" in name else inbox).append((float("inf") if le == "+Inf" else float(le), float(val)))
    else:
        metrics[name] = float(val)


def hist_p(h, p):
    total = h[-1][1] if h else 0
    for le, n in h:
        if total and n >= p * total:
            return le * 1000
    return None

checks = []


def check(name, ok, value, bound):
    checks.append({"check": name, "ok": bool(ok), "value": value, "bound": bound})


# I9 (PLAN §6.1): the offer decision inside the writer, p99 as its log2 bucket's bound, and the max.
count = hist[-1][1] if hist else 0
p99 = None
for le, n in hist:
    if count and n >= 0.99 * count:
        p99 = le * 1000
        break
dmax = metrics.get("parity_cc_routing_offer_decision_max_seconds", 0) * 1000
# A step-up run looks for the knee: its latency targets are judged per window (below), not overall.
STEP = bool(T.get("stepup"))
if not STEP:
  check("I9 offer decision p99 (ms, bucket bound)", p99 is not None and p99 <= T.get("decision_p99_ms", 5), p99, T.get("decision_p99_ms", 5))
  check("I9 offer decision max (ms)", dmax <= T.get("decision_max_ms", 50), dmax, T.get("decision_max_ms", 50))
check("violations seen by the driver (double offers, lost calls or reservations)", not S["violations"], len(S["violations"]), 0)
check("calls lost (RE no longer holds an unfinished call)", S["calls"]["lost"] == 0, S["calls"]["lost"], 0)
check("sweep matches (an eligible READY agent idled while a call waited: a bug)",
      metrics.get("parity_cc_routing_sweep_matches_total", 0) == 0, metrics.get("parity_cc_routing_sweep_matches_total", 0), 0)
if T.get("all_offered", True) and not STEP:
    check("calls never offered (there were free agents)", S["calls"]["never_offered"] == 0, S["calls"]["never_offered"], 0)
if not STEP:
  check("offers expired (the driver answers every offer)", metrics.get("parity_cc_routing_offers_expired_total", 0) == 0,
        metrics.get("parity_cc_routing_offers_expired_total", 0), 0)
tto = S["time_to_offer_ms"]
if not STEP and T.get("tto_check", True):
  check("time to offer p99 (ms), all", tto["all"].get("p99_ms", 1e9) < T.get("tto_p99_ms", 50), tto["all"].get("p99_ms"), T.get("tto_p99_ms", 50))
if "storm_agents" in T and not STEP:
    st = S["storm_agents"]
    check("storm agents", st == T["storm_agents"], st, T["storm_agents"])
    check("storm length (s)", S["storm_s"] <= T.get("storm_s_max", 21), S["storm_s"], T.get("storm_s_max", 21))
    check("AttachAgent p99 (ms) during the storm", S["attach_ms"].get("p99_ms", 1e9) < T.get("attach_p99_ms", 300),
          S["attach_ms"].get("p99_ms"), T.get("attach_p99_ms", 300))
    check("every storm agent attached and READY", S["set_ready_ms"].get("n", 0) >= st and S["attach_ms"].get("n", 0) >= st,
          {"attached": S["attach_ms"].get("n", 0), "ready": S["set_ready_ms"].get("n", 0)}, st)
    d = tto["during_storm"]
    check("time to offer p99 (ms) during the storm", d.get("n", 0) > 0 and d.get("p99_ms", 1e9) < T.get("tto_p99_ms", 50),
          d.get("p99_ms"), T.get("tto_p99_ms", 50))
if T.get("must_refuse"):
    check("queue_full reached (max_entries filled)", S["refusals"].get("Enqueue:queue_full", 0) > 0,
          S["refusals"].get("Enqueue:queue_full", 0), "> 0")
if "concurrent" in T:
    check("peak concurrent calls", S["peak_concurrent"] >= T["concurrent"], S["peak_concurrent"], T["concurrent"])
rss = max((s["rss_mib"] for s in samples), default=0)
hwm = max((s["hwm_mib"] for s in samples), default=0)
if "rss_mib_max" in T:
    check("RE RSS (MiB, VmHWM)", hwm <= T["rss_mib_max"], round(hwm, 1), T["rss_mib_max"])


def cores(group, t_from, t_to):
    xs = [s for s in samples if t_from <= s["t"] <= t_to]
    if len(xs) < 2:
        return None
    a, b = xs[0], xs[-1]
    ticks = b["cpu_ticks"].get(group, 0) - a["cpu_ticks"].get(group, 0)
    return round(ticks / b["tick_hz"] / (b["t"] - a["t"]), 3)


t0 = S.get("run0_epoch_ms", 0) / 1000 + T.get("warmup_s", 0)
t1 = S.get("arrivals_end_epoch_ms", 0) / 1000
cpu = {g: cores(g, t0, t1) for g in ("all", "pcr-shard", "pcr-opt", "pcr-commit", "pcr-follow", "other")}
opt = {}
ms = [s for s in samples if s.get("metrics") and t0 <= s["t"] <= t1]
if len(ms) >= 2:
    a, b = ms[0]["metrics"], ms[-1]["metrics"]
    dt = ms[-1]["t"] - ms[0]["t"]
    sec = lambda k: b.get(k, 0) - a.get(k, 0)
    opt = {"window_s": round(dt, 1),
           "solve_plus_lever_cores": round((sec("parity_cc_routing_model_solve_seconds_sum") + sec("parity_cc_routing_lever_seconds_sum")) / dt, 3),
           "solves": sec("parity_cc_routing_model_solves_total"), "lever_plans": sec("parity_cc_routing_lever_plans_total"),
           "snapshots_dropped": sec("parity_cc_routing_model_snapshots_dropped_total")}
if "optimizer_cores_max" in T:
    c = cpu.get("pcr-opt")
    check("optimizer threads' CPU (cores) while calls flow", c is not None and c <= T["optimizer_cores_max"], c, T["optimizer_cores_max"])
    check("optimizer ticks ran (solves in the window)", opt.get("solves", 0) >= T.get("min_solves", 1), opt.get("solves", 0), T.get("min_solves", 1))
    check("snapshots dropped (optimizer behind)", opt.get("snapshots_dropped", 1) == 0, opt.get("snapshots_dropped"), 0)
# Step-up windows (storms at rising attach rates, call steps at rising call rates): what each achieved,
# its latencies, I9 from the histogram's change over the window, RE's CPU; the knee is the last window
# that holds every target with at least 90 % of its configured rate achieved.
def scrape_at(t, before):
    xs = [s for s in samples if s.get("metrics", {}).get("decision_buckets")]
    xs = [s for s in xs if (s["t"] <= t if before else s["t"] >= t)]
    if not xs:
        return None
    return xs[-1] if before else xs[0]


win_rows = []
for wd in S.get("windows", []):
    a, b = scrape_at(wd["from_epoch_ms"] / 1000, True), scrape_at(wd["to_epoch_ms"] / 1000, False)
    wp99 = wmax = None
    if a and b:
        ba = {le: n for le, n in a["metrics"]["decision_buckets"]}
        bb = b["metrics"]["decision_buckets"]
        total = bb[-1][1] - ba.get(bb[-1][0], 0)
        for le, n in bb:
            if total > 0 and n - ba.get(le, 0) >= 0.99 * total:
                wp99 = float("inf") if le == "+Inf" else float(le) * 1000
                break
        ma = a["metrics"].get("parity_cc_routing_offer_decision_max_seconds", 0)
        mb = b["metrics"].get("parity_cc_routing_offer_decision_max_seconds", 0)
        wmax = mb * 1000 if mb > ma else None  # the max since start rose in this window (else: below the earlier max)
    is_storm = wd["name"].startswith("storm")
    achieved = wd["attach_rate_achieved"] if is_storm else wd["calls_rate_achieved"]
    tto_w = wd["time_to_offer_ms"]
    ok = (tto_w.get("n", 0) == 0 or tto_w.get("p99_ms", 1e9) < T.get("tto_p99_ms", 50)) and \
         (wp99 is None or wp99 <= T.get("decision_p99_ms", 5)) and (wmax is None or wmax <= T.get("decision_max_ms", 50)) and \
         achieved >= 0.9 * wd["target_rate"]
    if is_storm:
        ok = ok and wd["attach_ms"].get("p99_ms", 1e9) < T.get("attach_p99_ms", 300)
    win_rows.append({"name": wd["name"], "target_rate": wd["target_rate"], "achieved_rate": round(achieved, 1),
                     "offers_per_s": round(wd["offers_rate"], 1), "peak_concurrent": wd["peak_concurrent"],
                     "attach_p99_ms": wd["attach_ms"].get("p99_ms"), "tto_p50_ms": tto_w.get("p50_ms"),
                     "tto_p99_ms": tto_w.get("p99_ms"), "tto_max_ms": tto_w.get("max_ms"),
                     "enqueue_p99_ms": wd["enqueue_ms"].get("p99_ms"), "decision_p99_le_ms": wp99, "decision_max_ms_new": wmax,
                     "re_cores": cores("all", wd["from_epoch_ms"] / 1000, wd["to_epoch_ms"] / 1000),
                     "re_shard_cores": cores("pcr-shard", wd["from_epoch_ms"] / 1000, wd["to_epoch_ms"] / 1000), "ok": ok})
knee = {}
for kind in ("storm", "calls"):
    rows = [r for r in win_rows if r["name"].startswith(kind)]
    passed = []
    for r in rows:
        if not r["ok"]:
            break
        passed.append(r)
    if rows:
        knee[kind] = {"last_ok": passed[-1]["name"] if passed else None,
                      "first_broken": rows[len(passed)]["name"] if len(passed) < len(rows) else None}
loads = [float(s["loadavg"][0]) for s in samples]
report = {
    "scenario": scenario, "pass": all(c["ok"] for c in checks), "checks": checks,
    "driver": {k: v for k, v in S.items() if k != "timeline"},
    "engine": {"decision_p99_le_ms": p99, "decision_max_ms": dmax, "decisions": count,
               "inbox_wait_p99_le_ms": hist_p(inbox, 0.99), "inbox_wait_p999_le_ms": hist_p(inbox, 0.999),
               "inbox_wait_max_ms": metrics.get("parity_cc_routing_writer_inbox_wait_max_seconds", 0) * 1000,
               "periodic_max_ms": metrics.get("parity_cc_routing_writer_periodic_max_seconds", 0) * 1000,
               "periodic_avg_ms": metrics.get("parity_cc_routing_writer_periodic_seconds_sum", 0) * 1000 /
                                  max(1, metrics.get("parity_cc_routing_writer_periodics_total", 0)),
               "rss_mib_max": round(rss, 1), "hwm_mib": round(hwm, 1), "cpu_cores_in_window": cpu, "optimizer": opt,
               **{k.replace("parity_cc_routing_", ""): v for k, v in metrics.items() if k.endswith("_total") and v}},
    "host_loadavg_1m": {"first": loads[0] if loads else None, "max": max(loads) if loads else None},
    "windows": win_rows, "knee": knee,
}
json.dump(report, open(f"{w}/report.json", "w"), indent=2)
for r in win_rows:
    print(("ok   " if r["ok"] else "BROKE") + " " + json.dumps(r) if STEP else "info " + json.dumps(r))
if knee:
    print("knee: " + json.dumps(knee))
for c in checks:
    print(("ok   " if c["ok"] else "FAIL ") + c["check"] + ": " + json.dumps(c["value"]) + " (bound " + json.dumps(c["bound"]) + ")")
print(json.dumps({"scenario": scenario, "pass": report["pass"], "tto": tto["all"], "attach": S.get("attach_ms"),
                  "peak_concurrent": S["peak_concurrent"], "decision_p99_le_ms": p99, "decision_max_ms": dmax,
                  "rss_mib": round(hwm, 1), "inbox_wait_p99_le_ms": report["engine"]["inbox_wait_p99_le_ms"],
                  "inbox_wait_max_ms": report["engine"]["inbox_wait_max_ms"], "periodic_max_ms": report["engine"]["periodic_max_ms"],
                  "periodic_avg_ms": round(report["engine"]["periodic_avg_ms"], 3), "cpu": cpu, "optimizer": opt, "loadavg": report["host_loadavg_1m"]}))
sys.exit(0 if report["pass"] else 1)
