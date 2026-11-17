"""
================================================================================
PRACTICAL 1 | MILESTONE 8 - Distributed System Models (Week 8)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M8-a] The same 5-node platform run under FIVE distributed-system models:
         host-based, workstation, server-based, processor pool, integrated.
  [M8-b] Evaluation on the six criteria the brief names:
         LATENCY, THROUGHPUT, RESOURCE UTILIZATION, SCALABILITY,
         RELIABILITY, COST.
           * scalability = how well throughput grows when clients go 10 -> 40
           * reliability = success rate when ONE node crashes mid-run,
                           averaged over all five possible nodes
  [M8-c] A weighted scorecard that answers: which model best fits a
         telecommunications platform? (Section 6 of the brief: baseline
         vs proposed -> server_based is the baseline, integrated the proposal.)

Run:
    python -m practical1_m7_m8.milestone8_system_models
================================================================================
"""

import sys
import os
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.cluster import Cluster
from common.system_models import (MODEL_NAMES, MODEL_LABELS, run_model, build_scorecard,
                                  SCORECARD_WEIGHTS, EQUAL_WEIGHTS)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# (clients, total requests) for the scalability experiment; the middle level is "nominal load"
LOAD_LEVELS = [(10, 150), (20, 300), (40, 600)]
NOMINAL = 1
SEED = 8008


def log(tag, msg):
    print(f"[{tag}] {msg}")


def scalability_runs(cluster):
    log("M8", f"Scalability: every model at {[c for c, _ in LOAD_LEVELS]} concurrent clients "
               f"({[t for _, t in LOAD_LEVELS]} requests) ...")
    runs = {m: [] for m in MODEL_NAMES}
    for m in MODEL_NAMES:
        for clients, tasks in LOAD_LEVELS:
            r = run_model(cluster, m, clients, tasks, seed=SEED)
            runs[m].append(r)
        lo, mid, hi = runs[m]
        log("M8", f"  {MODEL_LABELS[m]:26s} throughput {lo['throughput_per_s']:7.1f} -> {hi['throughput_per_s']:7.1f}/s   "
                   f"mean latency {mid['mean_latency_ms']:6.1f} ms   success@40 clients {hi['success_rate']*100:5.1f}%")
    return runs


def reliability_runs(cluster):
    clients, tasks = LOAD_LEVELS[NOMINAL]
    log("M8", f"Reliability: crash each of the 5 nodes in turn, mid-run, for every model "
               f"({clients} clients, {tasks} requests) ...")
    out = {m: {} for m in MODEL_NAMES}
    for m in MODEL_NAMES:
        for node in cluster.configs:
            r = run_model(cluster, m, clients, tasks, seed=SEED, crash_node=node)
            out[m][node] = {"success_rate": r["success_rate"], "completed": r["completed"],
                            "errors": r["errors"], "rejected": r["rejected"]}
            cluster.restart(node)          # recovery before the next scenario
        rates = [v["success_rate"] for v in out[m].values()]
        worst_node = min(out[m], key=lambda n: out[m][n]["success_rate"])
        log("M8", f"  {MODEL_LABELS[m]:26s} avg success under a node crash {sum(rates)/len(rates)*100:5.1f}%   "
                   f"worst case {min(rates)*100:5.1f}% (when {worst_node} dies)")
    return out


def summarise(runs, reliability):
    metrics = {k: {} for k in ("latency", "throughput", "reliability", "scalability", "cost", "utilization")}
    table = {}
    for m in MODEL_NAMES:
        lo, mid, hi = runs[m][0], runs[m][NOMINAL], runs[m][2]
        rel = [v["success_rate"] for v in reliability[m].values()]
        client_ratio = hi["clients"] / lo["clients"]
        scal_index = (hi["throughput_per_s"] / lo["throughput_per_s"]) / client_ratio * hi["success_rate"]
        table[m] = {
            "model": MODEL_LABELS[m],
            "mean_latency_ms": mid["mean_latency_ms"], "p95_latency_ms": mid["p95_latency_ms"],
            "urllc_latency_ms": mid["mean_latency_by_class_ms"]["URLLC"],
            "throughput_per_s": mid["throughput_per_s"],
            "success_rate_nominal": mid["success_rate"],
            "utilization_of_provisioned_capacity": mid["utilization_of_provisioned_capacity"],
            "load_balance_jain_index": mid["load_balance_jain_index"],
            "scalability_index": round(scal_index, 4),
            "throughput_at_10_clients": lo["throughput_per_s"], "throughput_at_40_clients": hi["throughput_per_s"],
            "success_at_40_clients": hi["success_rate"],
            "reliability_avg_under_node_crash": round(sum(rel) / len(rel), 4),
            "reliability_worst_case": round(min(rel), 4),
            "provisioned_cost_per_hour": mid["provisioned_cost_per_hour"],
            "total_cost_per_1k_requests": mid["total_cost_per_1k_requests"],
            "routing_mix": mid["routing_mix"],
        }
        metrics["latency"][m] = mid["mean_latency_ms"]
        metrics["throughput"][m] = mid["throughput_per_s"]
        metrics["reliability"][m] = sum(rel) / len(rel)
        metrics["scalability"][m] = scal_index
        metrics["cost"][m] = mid["total_cost_per_1k_requests"]
        metrics["utilization"][m] = mid["utilization_of_provisioned_capacity"]
    scores_weighted = build_scorecard(metrics, SCORECARD_WEIGHTS)
    scores_equal = build_scorecard(metrics, EQUAL_WEIGHTS)
    return table, scores_weighted, scores_equal


def run(write_json=True):
    cluster = Cluster()
    cluster.start_all()
    try:
        runs = scalability_runs(cluster)
        reliability = reliability_runs(cluster)
    finally:
        cluster.stop_all()

    table, scores_w, scores_e = summarise(runs, reliability)
    ranking = sorted(scores_w, key=lambda m: -scores_w[m])
    log("M8", "---- NOMINAL-LOAD COMPARISON (20 clients, 300 requests) ----")
    log("M8", f"  {'model':26s}{'latency':>9s}{'thruput':>9s}{'util':>7s}{'scal.':>7s}{'reliab.':>8s}{'$/1k req':>10s}{'score':>7s}")
    for m in ranking:
        t = table[m]
        log("M8", f"  {t['model']:26s}{t['mean_latency_ms']:8.1f}m{t['throughput_per_s']:8.1f}/"
                   f"{t['utilization_of_provisioned_capacity']*100:6.1f}%{t['scalability_index']:7.2f}"
                   f"{t['reliability_avg_under_node_crash']*100:7.1f}%{t['total_cost_per_1k_requests']:10.4f}{scores_w[m]:7.3f}")
    log("M8", f"  Best model for telecom (weighted): {MODEL_LABELS[ranking[0]]}")
    log("M8", f"  Ranking with EQUAL weights (robustness check): "
               f"{[MODEL_LABELS[m] for m in sorted(scores_e, key=lambda m: -scores_e[m])]}")

    out = {
        "parameters": {"load_levels_clients_requests": LOAD_LEVELS, "nominal_level_index": NOMINAL, "seed": SEED,
                       "weights": SCORECARD_WEIGHTS},
        "scalability_runs": runs,
        "reliability_runs": reliability,
        "summary_table": table,
        "scorecard_weighted": scores_w,
        "scorecard_equal_weights": scores_e,
        "ranking_weighted": ranking,
    }
    if write_json:
        path = os.path.join(RESULTS_DIR, "milestone8_system_models.json")
        with open(path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        log("M8", f"Results saved -> {path}")
    return out


def main():
    run()
    log("M8", "MILESTONE 8 COMPLETE.")


if __name__ == "__main__":
    main()
