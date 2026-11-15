"""
system_models.py
-----------------
[MILESTONE 8 - Distributed System Models]

The five classic distributed-system MODELS, each implemented as a different
policy for deciding WHICH node of the SAME 5-node platform serves each
request. Nothing about the nodes changes -- only the placement logic -- so
any difference in results is caused by the model itself.

  host_based      All processing on ONE central host (cloud-1); clients are
                  "dumb terminals" that only send requests.
  workstation     Every client has its OWN machine (its home edge node) and
                  does all its work there. No sharing, no migration.
  server_based    Dedicated servers per kind of work (client-server): light
                  work (URLLC/mMTC) on the two edge servers, heavy eMBB on
                  ran-1. This is exactly the static "edge-first" placement
                  used since Assignment 1, so it is our BASELINE.
  processor_pool  A central pool manager hands each request to whichever
                  processor has the most SPARE capacity, ignoring where it
                  is (location-blind).
  integrated      Workstation + pool combined (our PROPOSED system): prefer
                  the nearest node, but spill over to a farther one when the
                  near one gets busy (location-AWARE adaptive placement).

Because all nodes run on one laptop, the network distance between tiers is
SIMULATED: each request really sleeps for a tier-dependent round-trip time
(edge 2 ms, RAN 5 ms, core 10 ms, cloud 25 ms) before it is sent. Costs are
ASSUMED prices (documented below), used only to compare models with each other.
"""

import statistics
import threading
import time

from common.communication import send_message
from common.workload_generator import generate_workload

TIER_OF = {"edge-1": "edge", "edge-2": "edge", "ran-1": "ran", "core-1": "core", "cloud-1": "cloud"}
SIM_RTT_MS = {"edge": 2, "ran": 5, "core": 10, "cloud": 25}                      # simulated network distance
COST_PER_VCPU_HOUR = {"edge": 0.060, "ran": 0.045, "core": 0.035, "cloud": 0.025}  # ASSUMED $ per vCPU-hour
TRANSPORT_COST_PER_1K = {"edge": 0.02, "ran": 0.05, "core": 0.10, "cloud": 0.25}   # ASSUMED $ per 1,000 requests
LOAD_PENALTY_MS = 60          # integrated model: how strongly it avoids a busy node
MAX_ADMISSION_WAIT_S = 1.0    # how long a request may queue for capacity before it is rejected
URLLC_SLA_MS = 40             # latency target used in the batch-layer SLA metric

MODEL_NAMES = ["host_based", "workstation", "server_based", "processor_pool", "integrated"]
MODEL_LABELS = {
    "host_based": "Host-based", "workstation": "Workstation", "server_based": "Server-based (baseline)",
    "processor_pool": "Processor pool", "integrated": "Integrated (proposed)",
}


# =============================================================================
# Routers (one per model)
# =============================================================================

class Router:
    name = "base"
    retry_on_failure = False   # may the client try another node if its node has crashed?

    def __init__(self, cluster):
        self.cluster = cluster
        self.caps = {n: (c.cpu_capacity, c.memory_capacity, c.bandwidth_capacity)
                     for n, c in cluster.configs.items()}
        self._lock = threading.Lock()
        self.inflight = {n: [0.0, 0.0, 0.0] for n in cluster.configs}
        self.dead = set()
        self._rr = 0

    # --- what each model must define ---
    def provisioned(self):
        raise NotImplementedError

    def candidates(self, task, client_id):
        """Nodes this model is WILLING to use for the task, best first (called under the lock)."""
        raise NotImplementedError

    # --- shared bookkeeping ---
    def _fits(self, n, t):
        cap, inf = self.caps[n], self.inflight[n]
        return (inf[0] + t["cpu"] <= cap[0] and inf[1] + t["memory"] <= cap[1]
                and inf[2] + t["bandwidth"] <= cap[2])

    def acquire(self, task, client_id):
        """Pick the first acceptable node that currently has room, and reserve room on it."""
        with self._lock:
            for n in self.candidates(task, client_id):
                if n not in self.dead and self._fits(n, task):
                    inf = self.inflight[n]
                    inf[0] += task["cpu"]; inf[1] += task["memory"]; inf[2] += task["bandwidth"]
                    return n
        return None

    def release(self, node, task):
        with self._lock:
            inf = self.inflight[node]
            inf[0] -= task["cpu"]; inf[1] -= task["memory"]; inf[2] -= task["bandwidth"]

    def mark_dead(self, node):
        with self._lock:
            self.dead.add(node)

    def _next_rr(self):
        self._rr += 1
        return self._rr


class HostBased(Router):
    name = "host_based"

    def provisioned(self):
        return ["cloud-1"]

    def candidates(self, task, client_id):
        return ["cloud-1"]


class Workstation(Router):
    name = "workstation"
    EDGES = ["edge-1", "edge-2"]

    def provisioned(self):
        return list(self.EDGES)

    def candidates(self, task, client_id):
        return [self.EDGES[client_id % 2]]        # each client only ever uses its OWN machine


class ServerBased(Router):
    name = "server_based"

    def provisioned(self):
        return ["edge-1", "edge-2", "ran-1"]

    def candidates(self, task, client_id):
        if task["service_class"] == "eMBB":
            return ["ran-1"]
        first = self._next_rr() % 2
        return ["edge-1", "edge-2"] if first == 0 else ["edge-2", "edge-1"]


class ProcessorPool(Router):
    name = "processor_pool"
    retry_on_failure = True

    def provisioned(self):
        return list(self.cluster.configs)

    def candidates(self, task, client_id):
        free = {n: 1.0 - self.inflight[n][0] / self.caps[n][0] for n in self.cluster.configs}
        return sorted(free, key=lambda n: (-free[n], -self.caps[n][0]))   # most spare capacity first


class Integrated(Router):
    name = "integrated"
    retry_on_failure = True

    def provisioned(self):
        return list(self.cluster.configs)

    def candidates(self, task, client_id):
        def score(n):
            tier = TIER_OF[n]
            s = SIM_RTT_MS[tier] + LOAD_PENALTY_MS * (self.inflight[n][0] / self.caps[n][0])
            if task["service_class"] == "URLLC" and tier in ("core", "cloud"):
                s += 1000      # latency-critical work only goes far away if NOTHING nearby has room
            return s
        return sorted(self.cluster.configs, key=score)


ROUTERS = {"host_based": HostBased, "workstation": Workstation, "server_based": ServerBased,
           "processor_pool": ProcessorPool, "integrated": Integrated}


# =============================================================================
# One experiment run
# =============================================================================

def _execute(router, cluster, task, client_id):
    """Returns (status, node, latency_ms) for ONE request, following the model's rules."""
    t0 = time.time()
    attempts = 2 if router.retry_on_failure else 1
    last_node = None
    for _ in range(attempts):
        node = router.acquire(task, client_id)
        waited = 0.0
        while node is None and waited < MAX_ADMISSION_WAIT_S:
            time.sleep(0.005)
            waited += 0.005
            node = router.acquire(task, client_id)
        if node is None:
            return "rejected", last_node, (time.time() - t0) * 1000.0
        last_node = node
        try:
            time.sleep(SIM_RTT_MS[TIER_OF[node]] / 1000.0)       # simulated network distance
            host, port = cluster.addr(node)
            reply = send_message(host, port, {"type": "task_request", "task": task}, timeout=3.0)
            status = reply.get("status", "error")
            router.release(node, task)
            return ("completed" if status == "completed" else "rejected"), node, (time.time() - t0) * 1000.0
        except Exception:
            router.release(node, task)
            router.mark_dead(node)       # learned the hard way that this node is gone
    return "error", last_node, (time.time() - t0) * 1000.0


def run_model(cluster, model: str, n_clients: int, n_tasks: int, seed: int = 8008,
              crash_node: str = None, crash_after_fraction: float = 0.3) -> dict:
    router = ROUTERS[model](cluster)
    tasks = generate_workload(n_tasks, seed=seed)
    results, rlock = [], threading.Lock()
    util_samples = {n: [] for n in cluster.configs}
    stop = threading.Event()
    crashed = {"done": False}

    def sampler():
        while not stop.is_set():
            for n in cluster.configs:
                try:
                    h, p = cluster.addr(n)
                    m = send_message(h, p, {"type": "metrics_request"}, timeout=0.3)
                    util_samples[n].append(m["resource_utilization"]["cpu"])
                except Exception:
                    pass
            time.sleep(0.05)

    def crasher():
        while not stop.is_set():
            with rlock:
                done = len(results)
            if done >= crash_after_fraction * n_tasks:
                cluster.kill(crash_node)
                crashed["done"] = True
                return
            time.sleep(0.005)

    def client(cid):
        for task in tasks[cid::n_clients]:
            status, node, lat = _execute(router, cluster, task, cid)
            with rlock:
                results.append({"status": status, "node": node, "latency_ms": lat,
                                "service_class": task["service_class"]})

    threads = [threading.Thread(target=client, args=(c,)) for c in range(n_clients)]
    aux = [threading.Thread(target=sampler, daemon=True)]
    if crash_node:
        aux.append(threading.Thread(target=crasher, daemon=True))
    t0 = time.time()
    for t in aux + threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.time() - t0
    stop.set()
    for t in aux:
        t.join(timeout=1.0)

    done = [r for r in results if r["status"] == "completed"]
    lat = sorted(r["latency_ms"] for r in done)
    throughput = len(done) / wall if wall > 0 else 0.0
    prov = router.provisioned()
    mean_util = {n: (statistics.mean(v) if v else 0.0) for n, v in util_samples.items()}
    cap_cpu = {n: cluster.configs[n].cpu_capacity for n in cluster.configs}
    util_weighted = (sum(mean_util[n] * cap_cpu[n] for n in prov) / sum(cap_cpu[n] for n in prov)) if prov else 0.0
    xs = [mean_util[n] for n in prov]
    jain = (sum(xs) ** 2 / (len(xs) * sum(x * x for x in xs))) if xs and sum(x * x for x in xs) > 0 else 1.0

    served_by = {}
    mix = {}
    for r in done:
        served_by[r["node"]] = served_by.get(r["node"], 0) + 1
        mix.setdefault(r["service_class"], {})
        mix[r["service_class"]][r["node"]] = mix[r["service_class"]].get(r["node"], 0) + 1

    prov_cost_per_hour = sum(cap_cpu[n] * COST_PER_VCPU_HOUR[TIER_OF[n]] for n in prov)
    transport_per_1k = (sum(TRANSPORT_COST_PER_1K[TIER_OF[n]] * c for n, c in served_by.items()) / len(done)
                        if done else 0.0)
    compute_per_1k = prov_cost_per_hour / (throughput * 3.6) if throughput > 0 else float("inf")

    by_class = {}
    for cls in ("URLLC", "eMBB", "mMTC"):
        vals = [r["latency_ms"] for r in done if r["service_class"] == cls]
        by_class[cls] = round(statistics.mean(vals), 2) if vals else None

    return {
        "model": model, "clients": n_clients, "tasks": n_tasks,
        "completed": len(done),
        "rejected": len([r for r in results if r["status"] == "rejected"]),
        "errors": len([r for r in results if r["status"] == "error"]),
        "success_rate": round(len(done) / len(results), 4) if results else 0.0,
        "throughput_per_s": round(throughput, 2),
        "mean_latency_ms": round(statistics.mean(lat), 2) if lat else None,
        "p95_latency_ms": round(lat[int(0.95 * (len(lat) - 1))], 2) if lat else None,
        "mean_latency_by_class_ms": by_class,
        "mean_cpu_utilization_by_node": {n: round(v, 4) for n, v in mean_util.items()},
        "provisioned_nodes": prov,
        "utilization_of_provisioned_capacity": round(util_weighted, 4),
        "load_balance_jain_index": round(jain, 4),
        "served_by_node": served_by,
        "routing_mix": mix,
        "provisioned_cost_per_hour": round(prov_cost_per_hour, 3),
        "transport_cost_per_1k_requests": round(transport_per_1k, 4),
        "compute_cost_per_1k_requests": round(compute_per_1k, 6) if compute_per_1k != float("inf") else None,
        "total_cost_per_1k_requests": round(compute_per_1k + transport_per_1k, 4) if compute_per_1k != float("inf") else None,
        "crash_node": crash_node, "wall_seconds": round(wall, 3),
    }


# =============================================================================
# Scorecard: which model fits a telecom platform best?
# =============================================================================

# Weights reflect telecom priorities: latency and reliability matter most.
SCORECARD_WEIGHTS = {"latency": 0.25, "throughput": 0.20, "reliability": 0.20,
                     "scalability": 0.15, "cost": 0.10, "utilization": 0.10}
EQUAL_WEIGHTS = {k: 1 / 6 for k in SCORECARD_WEIGHTS}


def _normalise(values: dict, higher_is_better: bool) -> dict:
    lo, hi = min(values.values()), max(values.values())
    if hi == lo:
        return {k: 1.0 for k in values}
    return {k: ((v - lo) / (hi - lo)) if higher_is_better else ((hi - v) / (hi - lo)) for k, v in values.items()}


def build_scorecard(metrics: dict, weights: dict) -> dict:
    """metrics: {metric_name: {model: value}}  ->  {model: weighted score in 0..1}"""
    norm = {
        "latency": _normalise(metrics["latency"], higher_is_better=False),
        "throughput": _normalise(metrics["throughput"], True),
        "reliability": _normalise(metrics["reliability"], True),
        "scalability": _normalise(metrics["scalability"], True),
        "cost": _normalise(metrics["cost"], higher_is_better=False),
        "utilization": _normalise(metrics["utilization"], True),
    }
    return {m: round(sum(weights[k] * norm[k][m] for k in weights), 4) for m in metrics["latency"]}
