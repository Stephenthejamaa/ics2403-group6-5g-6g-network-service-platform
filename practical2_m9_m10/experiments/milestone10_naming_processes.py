import json
import os
import sys
import time
import statistics

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from common.cluster import Cluster
from common.naming import (
    NameWorld,
    SCHEMES,
    RESOLVER_CLASSES,
    make_names,
    workload_exchanges,
)
from common.communication import send_message
from common.workload_generator import generate_workload


def measure_resolution(resolver, names):
    """Measure first-pass and repeated lookup latency."""
    first_latencies = []
    first_successes = 0

    resolver.reset_counter()

    for name in names:
        start = time.perf_counter()
        location = resolver.resolve(name)
        first_latencies.append((time.perf_counter() - start) * 1000)
        if location is not None:
            first_successes += 1

    first_exchanges = resolver.exchanges

    repeated_latencies = []
    repeated_successes = 0
    resolver.reset_counter()

    for name in names:
        start = time.perf_counter()
        location = resolver.resolve(name)
        repeated_latencies.append((time.perf_counter() - start) * 1000)
        if location is not None:
            repeated_successes += 1

    repeated_exchanges = resolver.exchanges

    return {
        "lookups": len(names),
        "first_pass_successes": first_successes,
        "first_pass_average_latency_ms": round(
            statistics.mean(first_latencies), 4
        ),
        "first_pass_max_latency_ms": round(max(first_latencies), 4),
        "first_pass_exchanges": first_exchanges,
        "first_pass_exchanges_per_lookup": round(
            first_exchanges / len(names), 4
        ),
        "repeated_successes": repeated_successes,
        "repeated_average_latency_ms": round(
            statistics.mean(repeated_latencies), 4
        ),
        "repeated_exchanges": repeated_exchanges,
        "repeated_exchanges_per_lookup": round(
            repeated_exchanges / len(names), 4
        ),
    }


def test_process_lifecycle(cluster, world):
    """Test leader coordination, then resolve and execute a process."""
    from common.naming import NameServerResolver
    from common.coordination import BullyElection

    # Run the simplified Bully election from lowest to highest priority.
    election_rounds = []
    elected_coordinator = None

    ordered_nodes = sorted(
        cluster.configs,
        key=lambda name: cluster.configs[name].election_id,
    )

    for initiator in ordered_nodes:
        cfg = cluster.configs[initiator]
        peers = cluster.directory(
            only=[name for name in cluster.configs if name != initiator]
        )

        election = BullyElection(
            self_name=initiator,
            self_election_id=cfg.election_id,
            peers=peers,
            send_fn=send_message,
            self_host=cfg.host,
            self_port=cfg.port,
        )

        outcome = election.run_election()
        election_rounds.append(outcome)

        if outcome["declared_leader"] is not None:
            elected_coordinator = outcome["declared_leader"]
            break

    if elected_coordinator is None:
        raise RuntimeError("No coordinator was elected")


    resolver = NameServerResolver(world)

    logical_name = next(iter(world.truth))
    location = resolver.resolve(logical_name)

    if location is None:
        raise RuntimeError("Process test could not resolve its logical name")

    task = generate_workload(1, seed=2026)[0]

    response = send_message(
        *cluster.addr(location),
        {"type": "task_request", "task": task},
        timeout=5.0,
    )

    if response.get("status") != "completed":
        raise RuntimeError(
            f"Process execution failed: {response}"
        )

    table_response = send_message(
        *cluster.addr(location),
        {"type": "process_table_request"},
        timeout=3.0,
    )

    processes = table_response.get("process_table", [])
    process = next(
        (p for p in processes if p["pid"] == response["pid"]),
        None,
    )

    return {
        "elected_coordinator": elected_coordinator,
        "election_rounds": election_rounds,
        "coordination_successful": elected_coordinator == "cloud-1",
        "logical_name": logical_name,
        "resolved_node": location,
        "task_name": task["name"],
        "service_class": task["service_class"],
        "pid": response["pid"],
        "task_response_status": response["status"],
        "process_found_in_table": process is not None,
        "final_process_state": process["state"] if process else None,
        "process_lifecycle_successful": (
            process is not None
            and process["state"] == "TERMINATED"
            and response["status"] == "completed"
        ),
    }


def main():
    cluster = Cluster(packet_loss_rate=0.0)

    results = {
        "milestone": 10,
        "title": "Naming and Distributed Process Management",
        "topology_nodes": 5,
        "naming_schemes": {},
        "scalability": {},
        "process_management": {},
    }

    try:
        print("Starting distributed cluster...")
        cluster.start_all()

        sizes = [10, 50, 100, 250, 500]
        sample_count = 20

        for size in sizes:
            print(f"\nTesting naming with {size} logical names...")

            truth = make_names(size, seed=10)
            world = NameWorld(cluster)
            world.build(truth)

            sample_names = list(truth)[:min(sample_count, size)]

            size_results = {}

            for scheme in SCHEMES:
                resolver_class = RESOLVER_CLASSES[scheme]
                resolver = resolver_class(world)

                print(f"  Measuring {scheme}...")

                size_results[scheme] = measure_resolution(
                    resolver, sample_names
                )

            results["naming_schemes"][str(size)] = size_results

        # Larger theoretical workload comparison.
        print("\nCalculating large-scale communication costs...")

        large_scale = {
            "nodes": 500,
            "clients": 5000,
            "lookups": 1000,
            "resource_moves": 10,
            "dns_hit_rate_assumption": 0.7,
        }

        for scheme in SCHEMES:
            large_scale[scheme] = workload_exchanges(
                scheme,
                nodes=500,
                clients=5000,
                lookups=1000,
                moves=10,
                dns_hit_rate=0.7,
            )

        results["scalability"] = {
            "measured_name_counts": sizes,
            "measured_lookup_sample_per_size": sample_count,
            "large_scale_model": large_scale,
        }

        # Process-management integration test.
        print("\nTesting distributed process management...")

        process_truth = make_names(20, seed=2026)
        process_world = NameWorld(cluster)
        process_world.build(process_truth)

        results["process_management"] = test_process_lifecycle(
            cluster, process_world
        )

        # Overall evaluation.
        naming_passed = all(
            all(
                entry["first_pass_successes"] == entry["lookups"]
                and entry["repeated_successes"] == entry["lookups"]
                for entry in results["naming_schemes"][str(size)].values()
            )
            for size in sizes
        )

        process_passed = results["process_management"][
            "process_lifecycle_successful"
        ]
        coordination_passed = results["process_management"][
            "coordination_successful"
        ]

        results["summary"] = {
            "all_five_naming_schemes_passed": naming_passed,
            "coordination_passed": coordination_passed,
            "process_lifecycle_passed": process_passed,
            "all_tests_passed": (
                naming_passed and coordination_passed and process_passed
            ),
        }

        output_dir = os.path.join(
            os.path.dirname(__file__), "..", "results"
        )
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(
            output_dir, "milestone10_naming_processes.json"
        )

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, default=str)

        print("\nMILESTONE 10 RESULTS")
        print(json.dumps(results["summary"], indent=4))
        print("Process lifecycle:")
        print(json.dumps(results["process_management"], indent=4))
        print("Results saved to:", output_file)

    finally:
        cluster.stop_all()


if __name__ == "__main__":
    main()
