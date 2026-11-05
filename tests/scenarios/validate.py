#!/usr/bin/env python3
"""Validates routing-scenario.v1 files kept in this repository (regressions, full-scale runs, and
the invariant search's generated scenarios) against parity-contracts' schema and the cross-field
rules contract-check applies to the canonical set (parity-contracts rust/contract-check
scenarios.rs). Stdlib only: the keywords the schema uses (no jsonschema module on this host).

  validate.py <parity-contracts dir> <file-or-dir>... [--name-is-stem]
"""
import json
import os
import re
import sys


def check(schema, root, value, path, errs):
    if "$ref" in schema:
        ref = schema["$ref"]
        assert ref.startswith("#/$defs/"), ref
        return check(root["$defs"][ref[8:]], root, value, path, errs)
    if "oneOf" in schema:
        ok = 0
        for alt in schema["oneOf"]:
            e = []
            check(alt, root, value, path, e)
            ok += not e
        if ok != 1:
            errs.append(f"{path}: matches {ok} of oneOf")
        return
    if "const" in schema and value != schema["const"]:
        errs.append(f"{path}: must be {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errs.append(f"{path}: {value!r} not in enum")
    t = schema.get("type")
    types = {"object": dict, "array": list, "string": str, "boolean": bool}
    if t == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            errs.append(f"{path}: not an integer")
            return
    elif t == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            errs.append(f"{path}: not a number")
            return
    elif t in types:
        if not isinstance(value, types[t]):
            errs.append(f"{path}: not a {t}")
            return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errs.append(f"{path}: below minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errs.append(f"{path}: above maximum {schema['maximum']}")
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            errs.append(f"{path}: not above {schema['exclusiveMinimum']}")
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errs.append(f"{path}: too short")
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errs.append(f"{path}: {value!r} does not match {schema['pattern']}")
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errs.append(f"{path}: fewer than {schema['minItems']} items")
        if schema.get("uniqueItems") and len({json.dumps(v, sort_keys=True) for v in value}) != len(value):
            errs.append(f"{path}: items not unique")
        if "items" in schema:
            for i, v in enumerate(value):
                check(schema["items"], root, v, f"{path}[{i}]", errs)
    if isinstance(value, dict):
        for r in schema.get("required", []):
            if r not in value:
                errs.append(f"{path}: missing {r}")
        props = schema.get("properties", {})
        for k, v in value.items():
            if k in props:
                check(props[k], root, v, f"{path}.{k}", errs)
            elif schema.get("additionalProperties") is False:
                errs.append(f"{path}: unexpected {k}")
            elif isinstance(schema.get("additionalProperties"), dict):
                check(schema["additionalProperties"], root, v, f"{path}.{k}", errs)


def cross_field(doc, shared):
    out = []
    duration = doc["duration_s"]
    tenants, sites, queues = set(), set(), set()
    for t in doc["tenants"]:
        tid = t["id"]
        if tid not in shared:
            out.append(f"tenant {tid} is not a shared test tenant with agents")
        if tid in tenants:
            out.append(f"tenant {tid} twice")
        tenants.add(tid)
        qids = set()
        for q in t["queues"]:
            if q["id"] in qids:
                out.append(f"{tid}: queue {q['id']} twice")
            qids.add(q["id"])
            queues.add((tid, q["id"]))
        nxt = {q["id"]: q["max_wait_action"].get("queue_id") for q in t["queues"]
               if q["max_wait_action"]["action"] == "queue"}
        for start in nxt:
            seen, at = [start], start
            while at in nxt:
                at = nxt[at]
                if at in seen:
                    out.append(f"{tid}: max-wait queue chain loops ({' -> '.join(seen + [at])})")
                    break
                seen.append(at)
            else:
                continue
            break
        for q in t["queues"]:
            a = q["max_wait_action"]
            if a["action"] == "queue" and (a.get("queue_id") not in qids or a.get("queue_id") == q["id"]):
                out.append(f"{tid}: max_wait_action queue {a.get('queue_id')} must be another queue of the tenant")
            if a["action"] == "flow_step" and "flow_step" not in a:
                out.append(f"{tid}: flow_step needs flow_step")
            if sum(m["share"] for m in q.get("priority_mix", [])) > 1 + 1e-9:
                out.append(f"{tid}: priority_mix shares above 1")
        groups = list(t["agents"]) + [e["group"] for e in doc.get("events", [])
                                      if e["kind"] == "mass_login" and e.get("tenant") == tid]
        for g in groups:
            for q in g["queues"]:
                if q not in qids:
                    out.append(f"{tid}: a group names unknown queue {q}")
            ids = len(g.get("ids", []))
            if ids > g["count"]:
                out.append(f"{tid}: more ids than count")
            if ids < g["count"] and "id_prefix" not in g:
                out.append(f"{tid}: a group needs id_prefix")
            if "site" in g:
                sites.add((tid, g["site"]))
    for e in doc.get("events", []):
        k = e["kind"]
        if e["at_s"] > duration:
            out.append(f"event {k} after the end")
        t = e.get("tenant")
        if t is not None and t not in tenants:
            out.append(f"event {k} names undeclared tenant {t}")
        if k in ("mass_login", "mass_logout", "supervisor_pin", "queue_config", "request_flood") and t is None:
            out.append(f"event {k} needs tenant")
        if k == "mass_login" and not isinstance(e.get("group"), dict):
            out.append("mass_login needs group")
        if t is not None and "site" in e and (t, e["site"]) not in sites:
            out.append(f"event {k} names unknown site {e['site']}")
        if k in ("queue_config", "request_flood") and t is not None and (t, e.get("queue")) not in queues:
            out.append(f"event {k} names an unknown queue")
    for kpi in doc["expectations"].get("kpi", []):
        t = kpi.get("tenant")
        if t is not None:
            if t not in tenants:
                out.append(f"kpi names undeclared tenant {t}")
            if "queue" in kpi and (t, kpi["queue"]) not in queues:
                out.append(f"kpi names unknown queue {kpi['queue']}")
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    name_is_stem = "--name-is-stem" in sys.argv
    contracts, targets = args[0], args[1:]
    schema = json.load(open(os.path.join(contracts, "schemas/scenarios/routing-scenario.v1.json")))
    shared = set(json.load(open(os.path.join(contracts, "tenants/test-tenants.v1.json")))["test"]["agents"])
    files = []
    for t in targets:
        if os.path.isdir(t):
            files += sorted(os.path.join(t, f) for f in os.listdir(t) if f.endswith(".json"))
        else:
            files.append(t)
    bad = 0
    for f in files:
        doc = json.load(open(f))
        errs = []
        check(schema, schema, doc, "$", errs)
        if not errs:
            errs = cross_field(doc, shared)
        if name_is_stem and doc.get("name") != os.path.basename(f)[:-5]:
            errs.append("name must equal the file name")
        if errs:
            bad += 1
            print(f"INVALID {f}: " + "; ".join(errs[:5]))
    print(f"routing-scenario.v1: {len(files)} files, {bad} invalid")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
