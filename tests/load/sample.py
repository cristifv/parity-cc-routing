#!/usr/bin/env python3
"""Samples one RE process every second for tests/load/run.sh (phase 7): its RSS (VmRSS, VmHWM),
CPU per thread group (pcr-shard-*, pcr-opt-*, pcr-commit, pcr-follow, the rest), and the host's
load average; every 2 s also RE's optimizer counters and I9's histogram from its /metrics (solve and lever seconds,
snapshots, drops). Runs until <stop-file> exists or the process is gone; one JSON line per sample.
  sample.py <pid> <out.jsonl> <stop-file> [<metrics url>]
"""
import json
import os
import re
import sys
import time
import urllib.request

pid, out_path, stop = int(sys.argv[1]), sys.argv[2], sys.argv[3]
url = sys.argv[4] if len(sys.argv) > 4 else ""
WANT = ("parity_cc_routing_model_solve_seconds_sum", "parity_cc_routing_lever_seconds_sum",
        "parity_cc_routing_model_solves_total", "parity_cc_routing_model_snapshots_dropped_total",
        "parity_cc_routing_lever_plans_total", "parity_cc_routing_offers_total",
        "parity_cc_routing_offer_decision_max_seconds", "parity_cc_routing_inbox_refused_total",
        "parity_cc_routing_writer_inbox_wait_max_seconds", "parity_cc_routing_writer_periodic_max_seconds",
        "parity_cc_routing_writer_periodic_seconds_sum", "parity_cc_routing_writer_periodics_total")
tick = os.sysconf("SC_CLK_TCK")


def group(name):
    m = re.match(r"(pcr-shard|pcr-opt|pcr-commit|pcr-follow)", name)
    return m.group(1) if m else "other"


def cpu_by_group():
    totals = {}
    base = f"/proc/{pid}/task"
    for tid in os.listdir(base):
        try:
            with open(f"{base}/{tid}/stat") as f:
                s = f.read()
        except OSError:
            continue
        name = s[s.index("(") + 1:s.rindex(")")]
        fields = s[s.rindex(")") + 2:].split()
        ticks = int(fields[11]) + int(fields[12])  # utime + stime
        g = group(name)
        totals[g] = totals.get(g, 0) + ticks
        totals["all"] = totals.get("all", 0) + ticks
    return totals


def mem():
    r = {}
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(("VmRSS:", "VmHWM:")):
                r[line.split(":")[0]] = int(line.split()[1]) / 1024.0
    return r


def scrape():
    if not url:
        return {}
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            text = r.read().decode()
    except OSError:
        return {}
    got = {}
    buckets = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in WANT:
            got[parts[0]] = float(parts[1])
        elif len(parts) == 2 and parts[0].startswith("parity_cc_routing_offer_decision_seconds_bucket"):
            le = parts[0].split('le="')[1].split('"')[0]
            buckets.append([le, float(parts[1])])
    if buckets:
        got["decision_buckets"] = buckets  # cumulative, since start (I9 per window from two scrapes)
    return got


n = 0
with open(out_path, "w") as out:
    while not os.path.exists(stop):
        try:
            t = time.time()
            m = mem()
            c = cpu_by_group()
            with open("/proc/loadavg") as f:
                la = f.read().split()[:3]
        except (OSError, ValueError):
            break
        rec = {"t": t, "rss_mib": m.get("VmRSS", 0), "hwm_mib": m.get("VmHWM", 0), "cpu_ticks": c, "tick_hz": tick,
               "loadavg": la}
        if n % 2 == 0:
            rec["metrics"] = scrape()
        n += 1
        out.write(json.dumps(rec) + "\n")
        out.flush()
        time.sleep(1.0)
