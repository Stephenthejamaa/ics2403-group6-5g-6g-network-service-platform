"""
================================================================================
PRACTICAL 2 | MILESTONE 9 - Transparency and Reliability (Week 9)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

The application works with ONE logical resource -- "the subscriber database"
(like the UDM in a real 5G core) -- and never with the machines behind it.
Each experiment tests one kind of transparency from the brief:

  [M9-a] ACCESS        one replica speaks a legacy wire format; the application
                       code is unchanged because an adapter translates
  [M9-b] LOCATION      the application's source code contains NO machine names;
                       the resource is relocated and the same code still works
  [M9-c] REPLICATION   1, 2 or 3 copies behind the same API; copies stay identical,
                       even when several writers race on the same key
  [M9-d] MIGRATION     the database moves to another node WHILE clients read and
                       write -- compared with a stop-the-world baseline
  [M9-e] FAILURE       a replica crashes mid-run and later returns; compared with a
                       client that talks to one physical node
  [M9-f] OVERHEAD      what each kind of transparency costs per operation

Run:
    python -m practical2_m9_m10.milestone9_transparency
================================================================================
"""

import sys
import os
import json
import time
import random
import threading
import inspect
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.cluster import Cluster
from common.communication import send_message
from common.fault_tolerance import run_open_loop
from common.transparency import Directory, TransparentClient, NaiveClient

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

REPLICAS = ["edge-1", "edge-2", "ran-1"]          # ran-1 speaks the LEGACY wire format
MIGRATION_SIZES = [2000, 10000, 40000]            # number of subscriber records in the database
WORKLOAD_THREADS, WORKLOAD_INTERVAL_S, HOT_KEYS = 4, 0.010, 1000
FAIL_RATE_HZ, FAIL_STREAM_S, CRASH_AT_S, RESTART_AT_S = 40, 7.0, 1.5, 4.0
STALL_THRESHOLD_MS = 20.0                          # a write slower than this counts as "stalled"
NODE_WORDS = ("edge-", "ran-", "core-", "cloud-")


def log(tag, msg):
    print(f"[{tag}] {msg}")


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[int(q * (len(v) - 1))]


def mean(values):
    return sum(values) / len(values) if values else None


def setup_nodes(cluster):
    for n in cluster.configs:
        send_message(*cluster.addr(n), {"type": "kv_clear"})
        send_message(*cluster.addr(n), {"type": "kv_configure", "legacy_only": n == "ran-1"})


def profile(i):
    return {"plan": "gold" if i % 2 == 0 else "silver", "data_mb": i * 10, "rate": round(i * 0.25, 2),
            "roaming": i % 3 == 0}


# -----------------------------------------------------------------------------
# THE APPLICATION. It talks about subscribers and a logical resource name. It must
# never mention a machine. (Experiment B checks that by scanning this very function.)
# -----------------------------------------------------------------------------
def subscriber_app(client, logical, n=60):
    expected = {}
    for i in range(n):
        key = f"imsi-{i:05d}"
        expected[key] = profile(i)
        client.put(logical, key, expected[key])
    correct, served_by = 0, Counter()
    for key, value in expected.items():
        r = client.get(logical, key)
        correct += int(r["ok"] and r["value"] == value)
        served_by[r["node"]] += 1
    return {"written": n, "read_back_correct": correct, "served_by": dict(served_by)}


# =============================================================================
# [M9-a] ACCESS transparency
# =============================================================================

def experiment_access(cluster):
    log("M9", "A. ACCESS transparency: one replica speaks a LEGACY wire format ...")
    setup_nodes(cluster)
    naive = {}
    for node in REPLICAS:                              # physical addressing: the app must know each node's format
        nc = NaiveClient(cluster, node)
        ok = sum(1 for i in range(100) if nc.put(f"imsi-{i:05d}", profile(i), i + 1)["status"] == "ok")
        naive[node] = {"wire_format": "legacy only" if node == "ran-1" else "native", "writes_ok": ok, "of": 100}
    setup_nodes(cluster)

    d = Directory()
    d.create("udm/access", REPLICAS)
    tc = TransparentClient(cluster, d)
    app = subscriber_app(tc, "udm/access", n=100)
    # verify EVERY physical copy holds the right values (read back through the adapter)
    per_replica = {}
    for node in REPLICAS:
        good = 0
        for i in range(100):
            r = tc.access.get(node, f"imsi-{i:05d}")
            good += int(r.get("found") and r["value"] == profile(i))
        per_replica[node] = {"wire_format": tc.access.format_of(node), "records_correct": good, "of": 100}
    div = tc.divergence("udm/access")
    out = {"physical_addressing": naive, "transparent": {"app": app, "per_replica": per_replica,
                                                           "format_translations": tc.access.translations,
                                                           "divergent_keys": div["divergent_keys"]}}
    log("M9", f"  without transparency: writes ok -> " + ", ".join(f"{n}: {v['writes_ok']}/100" for n, v in naive.items()))
    log("M9", f"  with transparency   : records correct on every copy -> " +
               ", ".join(f"{n}: {v['records_correct']}/100" for n, v in per_replica.items()) +
               f" ({tc.access.translations} format translations, {div['divergent_keys']} divergent keys)")
    return out


# =============================================================================
# [M9-b] LOCATION transparency
# =============================================================================

def experiment_location(cluster):
    log("M9", "B. LOCATION transparency: relocate the database, run the SAME application code ...")
    setup_nodes(cluster)
    src = inspect.getsource(subscriber_app)
    names_in_code = sum(src.count(w) for w in NODE_WORDS)
    d = Directory()
    d.create("udm/profiles", ["edge-2"])
    tc = TransparentClient(cluster, d)
    before = subscriber_app(tc, "udm/profiles")
    mig = tc.migrate_live("udm/profiles", "edge-2", "cloud-1")        # administrator relocates the resource
    after = subscriber_app(tc, "udm/profiles")                          # the very same function, same arguments
    n = 20000
    t0 = time.perf_counter()
    for _ in range(n):
        d.replicas("udm/profiles")
    lookup_us = (time.perf_counter() - t0) / n * 1e6
    out = {"machine_names_in_application_code": names_in_code,
           "before_relocation": before, "relocation": mig, "after_relocation": after,
           "directory_lookup_us": round(lookup_us, 2)}
    log("M9", f"  machine names appearing in the application's source code: {names_in_code}")
    log("M9", f"  before: {before['read_back_correct']}/{before['written']} correct, served by {before['served_by']}")
    log("M9", f"  after : {after['read_back_correct']}/{after['written']} correct, served by {after['served_by']}  "
               f"(same code; directory lookup costs {lookup_us:.1f} microseconds)")
    return out


# =============================================================================
# [M9-c] REPLICATION transparency + [M9-f] overhead
# =============================================================================

def _timed_ops(put_fn, get_fn, n=300):
    puts, gets = [], []
    for i in range(n):
        t = time.perf_counter(); put_fn(f"imsi-{i:05d}", profile(i)); puts.append((time.perf_counter() - t) * 1000)
    for i in range(n):
        t = time.perf_counter(); get_fn(f"imsi-{i:05d}"); gets.append((time.perf_counter() - t) * 1000)
    return puts, gets


def experiment_replication(cluster):
    log("M9", "C. REPLICATION transparency + OVERHEAD: 1, 2, 3 copies behind the same API ...")
    configs = [
        ("direct physical access (no transparency)", None, ["edge-1"]),
        ("transparent, 1 copy (native format)", "t1n", ["edge-1"]),
        ("transparent, 1 copy (legacy format)", "t1l", ["ran-1"]),
        ("transparent, 2 copies", "t2", ["edge-1", "edge-2"]),
        ("transparent, 3 copies", "t3", REPLICAS),
    ]
    # Latency here is tiny (about 0.1 ms) and the whole platform shares one CPU core, so a single
    # pass is noisy. We therefore run every configuration ROUNDS times, interleaved (so a slow
    # moment hits all configurations alike), and report the MEDIAN across rounds.
    ROUNDS = 5
    samples = {label: {"put_mean": [], "put_p95": [], "get_mean": [], "get_p95": []} for label, _, _ in configs}
    last = {}
    for _ in range(ROUNDS):
        for label, key, nodes in configs:
            setup_nodes(cluster)
            if key is None:
                nc, ctr = NaiveClient(cluster, nodes[0]), iter(range(1, 10**9))
                puts, gets = _timed_ops(lambda k, v: nc.put(k, v, next(ctr)), lambda k: nc.get(k))
                msgs_put, msgs_get, div = 1, 1.0, {"divergent_keys": 0}
            else:
                d = Directory()
                d.create("udm/r", nodes)
                tc = TransparentClient(cluster, d)
                tc.access.format_of(nodes[0])                      # discover formats before timing starts
                m0 = tc.messages
                puts, gets = _timed_ops(lambda k, v: tc.put("udm/r", k, v), lambda k: tc.get("udm/r", k))
                total_msgs = tc.messages - m0
                msgs_put, msgs_get = len(nodes), (total_msgs - 300 * len(nodes)) / 300
                div = tc.divergence("udm/r")
            sm = samples[label]
            sm["put_mean"].append(mean(puts)); sm["put_p95"].append(pct(puts, .95))
            sm["get_mean"].append(mean(gets)); sm["get_p95"].append(pct(gets, .95))
            last[label] = (len(nodes), msgs_put, msgs_get, div["divergent_keys"])
    rows = {}
    med = lambda v: sorted(v)[len(v) // 2]
    for label, key, nodes in configs:
        sm, (copies, mp, mg, dv) = samples[label], last[label]
        rows[label] = {"copies": copies, "put_mean_ms": round(med(sm["put_mean"]), 3), "put_p95_ms": round(med(sm["put_p95"]), 3),
                       "get_mean_ms": round(med(sm["get_mean"]), 3), "get_p95_ms": round(med(sm["get_p95"]), 3),
                       "messages_per_put": mp, "messages_per_get": round(mg, 2), "divergent_keys": dv}
        r = rows[label]
        log("M9", f"  {label:44s} put {r['put_mean_ms']:6.3f} ms   get {r['get_mean_ms']:6.3f} ms   "
                   f"msgs/put {r['messages_per_put']}  divergent keys {r['divergent_keys']}   (median of {ROUNDS} rounds)")

    # several writers racing on the SAME key must still leave all copies identical
    setup_nodes(cluster)
    d = Directory()
    d.create("udm/race", REPLICAS)
    tc = TransparentClient(cluster, d)

    def writer(w):
        for j in range(60):
            tc.put("udm/race", f"imsi-{j % 10:05d}", {"writer": w, "round": j})

    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    race = tc.divergence("udm/race")
    log("M9", f"  4 writers racing on 10 shared keys (240 writes): divergent keys across 3 copies = {race['divergent_keys']}")

    base_label = configs[0][0]
    base, t1n, t1l, t3 = rows[base_label], rows[configs[1][0]], rows[configs[2][0]], rows[configs[4][0]]
    overhead = [
        {"mechanism": "Location (directory lookup + client logic)", "put_overhead_ms": round(t1n["put_mean_ms"] - base["put_mean_ms"], 3),
         "get_overhead_ms": round(t1n["get_mean_ms"] - base["get_mean_ms"], 3), "extra_messages_per_put": 0, "extra_messages_per_get": 0,
         "note": "compared with talking to the node directly"},
        {"mechanism": "Access (legacy format translation)", "put_overhead_ms": round(t1l["put_mean_ms"] - t1n["put_mean_ms"], 3),
         "get_overhead_ms": round(t1l["get_mean_ms"] - t1n["get_mean_ms"], 3), "extra_messages_per_put": 0, "extra_messages_per_get": 0,
         "note": "legacy node vs native node, 1 copy each"},
        {"mechanism": "Replication (3 copies instead of 1)", "put_overhead_ms": round(t3["put_mean_ms"] - t1n["put_mean_ms"], 3),
         "get_overhead_ms": round(t3["get_mean_ms"] - t1n["get_mean_ms"], 3), "extra_messages_per_put": 2, "extra_messages_per_get": 0,
         "note": "writes go to every copy; reads still touch only one"},
        {"mechanism": "Failure masking (normal operation)", "put_overhead_ms": 0.0, "get_overhead_ms": 0.0,
         "extra_messages_per_put": 0, "extra_messages_per_get": 0,
         "note": "no cost until a failure happens; then one extra attempt per affected request"},
    ]
    return {"configs": rows, "concurrent_writers_divergent_keys": race["divergent_keys"], "overhead": overhead}


# =============================================================================
# [M9-d] MIGRATION transparency
# =============================================================================

class Workload:
    """Background application traffic: 4 threads, 70% reads / 30% writes, each checking its own results."""

    def __init__(self, tc, logical, preload):
        self.tc, self.logical = tc, logical
        self.records, self._lock = [], threading.Lock()
        self._stop = threading.Event()
        self.threads = [threading.Thread(target=self._run, args=(t,), daemon=True) for t in range(WORKLOAD_THREADS)]
        self.expected = {f"imsi-{i:06d}": preload(i) for i in range(HOT_KEYS)}

    def _run(self, tid):
        rng = random.Random(tid)
        keys = [f"imsi-{i:06d}" for i in range(HOT_KEYS) if i % WORKLOAD_THREADS == tid]
        seq = 0
        while not self._stop.is_set():
            k = rng.choice(keys)
            t_start = time.time()
            t = time.perf_counter()
            if rng.random() < 0.3:
                seq += 1
                v = {"plan": "gold", "writer": tid, "seq": seq}
                r = self.tc.put(self.logical, k, v)
                ok, correct, kind = r["ok"], True, "put"
                if ok:
                    self.expected[k] = v
            else:
                r = self.tc.get(self.logical, k)
                ok, kind = r["ok"], "get"
                correct = bool(ok and r["found"] and r["value"] == self.expected[k])
            with self._lock:
                self.records.append({"t": t_start, "latency_ms": (time.perf_counter() - t) * 1000.0,
                                     "ok": ok, "kind": kind, "correct": correct})
            time.sleep(WORKLOAD_INTERVAL_S)

    def start(self):
        [t.start() for t in self.threads]

    def stop(self):
        self._stop.set()
        [t.join(timeout=5) for t in self.threads]


def migration_case(cluster, n_keys, mode):
    setup_nodes(cluster)
    d = Directory()
    d.create("udm/mig", ["edge-1", "edge-2"])
    tc = TransparentClient(cluster, d)
    preload = lambda i: {"plan": "gold", "data_mb": i}
    items = {f"imsi-{i:06d}": [preload(i), 0] for i in range(n_keys)}
    keys = list(items)
    for node in ("edge-1", "edge-2"):
        for s in range(0, n_keys, 2000):
            send_message(*cluster.addr(node), {"type": "kv_load", "items": {k: items[k] for k in keys[s:s + 2000]}}, timeout=10)
    wl = Workload(tc, "udm/mig", preload)
    wl.start()
    time.sleep(0.4)
    t_a = time.time()
    mig = tc.migrate_live("udm/mig", "edge-2", "core-1") if mode == "live" else \
        tc.migrate_stop_the_world("udm/mig", "edge-2", "core-1")
    t_b = time.time()
    time.sleep(0.4)
    wl.stop()

    recs = wl.records
    during = [r for r in recs if t_a <= r["t"] <= t_b]
    before = [r["latency_ms"] for r in recs if r["t"] < t_a]
    writes_during = [r for r in during if r["kind"] == "put"]
    dst_total = send_message(*cluster.addr("core-1"), {"type": "kv_dump", "limit": 1}, timeout=10)["total"]
    div = tc.divergence("udm/mig")
    return {
        "keys": n_keys, "mode": mode, "migration_seconds": round(t_b - t_a, 3),
        "operations_total": len(recs), "operations_during_migration": len(during),
        "failed_operations": len([r for r in recs if not r["ok"]]),
        "incorrect_reads": len([r for r in recs if r["kind"] == "get" and not r["correct"]]),
        "stalled_writes": len([r for r in writes_during if r["latency_ms"] > STALL_THRESHOLD_MS]),
        "max_write_latency_ms": round(max([r["latency_ms"] for r in writes_during], default=0.0), 2),
        "baseline_mean_latency_ms": round(mean(before), 3) if before else None,
        "p95_latency_ms": round(pct([r["latency_ms"] for r in recs], .95), 3),
        "records_on_new_node": dst_total, "divergent_keys_after": div["divergent_keys"],
        "integrity_ok": dst_total == n_keys and div["divergent_keys"] == 0,
    }


def experiment_migration(cluster):
    log("M9", f"D. MIGRATION transparency: move the database edge-2 -> core-1 under live traffic "
               f"({WORKLOAD_THREADS * 100} ops/s), sizes {MIGRATION_SIZES} ...")
    rows = []
    for n in MIGRATION_SIZES:
        for mode in ("stop_the_world", "live"):
            r = migration_case(cluster, n, mode)
            rows.append(r)
            log("M9", f"  {n:6d} records  {mode:15s} took {r['migration_seconds']:5.2f}s  ops during: {r['operations_during_migration']:4d}  "
                       f"failed {r['failed_operations']}  wrong reads {r['incorrect_reads']}  stalled writes {r['stalled_writes']:3d}  "
                       f"max write latency {r['max_write_latency_ms']:7.1f} ms  integrity {r['integrity_ok']}")
    return rows


# =============================================================================
# [M9-e] FAILURE transparency + reliability
# =============================================================================

def failure_case(cluster, transparent):
    setup_nodes(cluster)
    acked, alock = {}, threading.Lock()
    marks = {}
    if transparent:
        d = Directory()
        d.create("udm/fail", REPLICAS)
        client = TransparentClient(cluster, d)
        put = lambda k, v: client.put("udm/fail", k, v)
        get = lambda k: client.get("udm/fail", k)
    else:
        nc, ctr, clock = NaiveClient(cluster, "edge-1"), iter(range(1, 10**9)), threading.Lock()

        def put(k, v):
            with clock:
                ver = next(ctr)
            try:
                r = nc.put(k, v, ver)
                return {"ok": r.get("status") == "ok", "version": ver}
            except Exception:
                return {"ok": False, "version": None}

        def get(k):
            try:
                r = nc.get(k)
                return {"ok": r.get("status") == "ok", "found": r.get("found", False), "version": r.get("version")}
            except Exception:
                return {"ok": False, "found": False, "version": None}

    for i in range(200):
        r = put(f"imsi-{i:05d}", {"seq": 0})
        acked[f"imsi-{i:05d}"] = r["version"]

    def op(i):
        k = f"imsi-{i % 200:05d}"
        t = time.perf_counter()                      # latency is timed HERE, identically for both kinds of client
        if i % 10 < 3:
            r = put(k, {"seq": i})
            lat = (time.perf_counter() - t) * 1000.0
            if r["ok"]:
                with alock:
                    acked[k] = max(acked.get(k) or 0, r["version"])
            return {"kind": "put", "ok": r["ok"], "lost": False, "stale": False, "latency_ms": lat}
        with alock:
            need = acked.get(k) or 0
        r = get(k)
        lat = (time.perf_counter() - t) * 1000.0
        ok = r["ok"]
        return {"kind": "get", "ok": ok, "latency_ms": lat,
                "lost": bool(ok and not r["found"] and need > 0),
                "stale": bool(ok and r["found"] and (r["version"] or 0) < need)}

    def crash():
        marks["crash"] = time.time()
        cluster.kill("edge-1")

    def restart():
        marks["restart_begin"] = time.time()
        cluster.restart("edge-1")
        marks["restarted"] = time.time()
        if transparent:
            marks["repair"] = client.recover("udm/fail", "edge-1")
        marks["recovered"] = time.time()

    t0, recs = run_open_loop(op, FAIL_RATE_HZ, FAIL_STREAM_S, [(CRASH_AT_S, crash), (RESTART_AT_S, restart)])
    time.sleep(0.6)
    ok_recs = [r for r in recs if r["ok"]]
    restarted_keys = send_message(*cluster.addr("edge-1"), {"type": "kv_dump", "limit": 1})["total"]
    out = {
        "client": "transparent (replicated + failover + repair)" if transparent else "physical (bound to edge-1)",
        "operations": len(recs), "failed_operations": len(recs) - len(ok_recs),
        "availability": round(len(ok_recs) / len(recs), 4),
        "lost_reads": len([r for r in recs if r["lost"]]), "stale_reads": len([r for r in recs if r["stale"]]),
        "p95_latency_ms": round(pct([r["latency_ms"] for r in ok_recs], .95), 3) if ok_recs else None,
        "max_latency_ms": round(max([r["latency_ms"] for r in ok_recs], default=0.0), 3),
        "restart_seconds": round(marks["restarted"] - marks["restart_begin"], 3),
        "keys_on_restarted_node": restarted_keys,
    }
    if transparent:
        div = client.divergence("udm/fail")
        out.update({"repair_seconds": round(marks["repair"]["seconds"], 3), "keys_repaired": marks["repair"]["keys_repaired"],
                    "divergent_keys_after_repair": div["divergent_keys"],
                    "full_redundancy_restored_after_s": round(marks["recovered"] - marks["crash"], 3)})
    else:
        out.update({"repair_seconds": None, "keys_repaired": 0, "divergent_keys_after_repair": None,
                    "full_redundancy_restored_after_s": None})
    return out


def experiment_failure(cluster):
    log("M9", f"E. FAILURE transparency: crash edge-1 at t={CRASH_AT_S}s, restart at t={RESTART_AT_S}s "
               f"({FAIL_RATE_HZ} ops/s for {FAIL_STREAM_S:.0f}s) ...")
    rows = []
    for transparent in (False, True):
        r = failure_case(cluster, transparent)
        rows.append(r)
        log("M9", f"  {r['client']:46s} availability {r['availability']*100:6.2f}%  failed {r['failed_operations']:3d}  "
                   f"LOST reads {r['lost_reads']:3d}  stale reads {r['stale_reads']}  keys on restarted node {r['keys_on_restarted_node']}")
    return rows


def run(write_json=True):
    cluster = Cluster()
    cluster.start_all()
    try:
        access = experiment_access(cluster)
        location = experiment_location(cluster)
        replication = experiment_replication(cluster)
        migration = experiment_migration(cluster)
        failure = experiment_failure(cluster)
    finally:
        cluster.stop_all()
    out = {"parameters": {"replicas": REPLICAS, "legacy_node": "ran-1", "migration_sizes": MIGRATION_SIZES,
                          "workload_threads": WORKLOAD_THREADS, "workload_interval_s": WORKLOAD_INTERVAL_S,
                          "hot_keys": HOT_KEYS, "live_copy_rate_keys_per_s": 25000, "stall_threshold_ms": STALL_THRESHOLD_MS,
                          "failure_rate_hz": FAIL_RATE_HZ, "failure_stream_s": FAIL_STREAM_S,
                          "crash_at_s": CRASH_AT_S, "restart_at_s": RESTART_AT_S},
           "access": access, "location": location, "replication": replication, "migration": migration, "failure": failure}
    if write_json:
        path = os.path.join(RESULTS_DIR, "milestone9_transparency.json")
        with open(path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        log("M9", f"Results saved -> {path}")
    return out


def main():
    run()
    log("M9", "MILESTONE 9 COMPLETE.")


if __name__ == "__main__":
    main()
