import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from common.cluster import Cluster
from common.naming import NameWorld, NameServerResolver
from common.transparency import TransparentClient
from common.workload_generator import generate_workload
from common.fault_tolerance import FaultInjector, FailureDetector


def main():
    cluster = Cluster(packet_loss_rate=0.0)
    detector = None
    injector = FaultInjector(cluster)

    results = {
        "milestone": 9,
        "title": "Transparency and Reliability",
        "tests": {}
    }

    try:
        print("Starting distributed cluster...")
        cluster.start_all()

        # Create logical resources and register their physical locations.
        service = "/edge/session/service-1"
        failover_service = "/edge/session/failover-1"

        world = NameWorld(cluster)
        world.build({
            service: "edge-1",
            failover_service: "edge-1"
        })

        resolver = NameServerResolver(world)

        replicas = {
            service: ["edge-1", "edge-2"],
            failover_service: ["edge-1", "edge-2"]
        }

        client = TransparentClient(
            cluster,
            resolver,
            replica_sets=replicas,
            timeout=0.8
        )

        tasks = generate_workload(6, seed=2026)
        task1 = tasks[0]
        task2 = tasks[1]

        # TEST 1: Access transparency
        print("Testing access transparency...")
        access_result = client.access(service, task1, mode="single")

        results["tests"]["access_transparency"] = {
            "logical_resource": service,
            "successful": access_result.get("outcome", {}).get("ok", False),
            "response": access_result
        }

        # TEST 2: Location transparency
        print("Testing location transparency...")
        initial_location = client.locate(service)

        results["tests"]["location_transparency"] = {
            "logical_resource": service,
            "resolved_location": initial_location,
            "successful": initial_location == "edge-1"
        }

        # TEST 3: Replication transparency
        print("Testing replication transparency...")
        replication_result = client.access(service, task2, mode="quorum")

        results["tests"]["replication_transparency"] = {
            "replicas": replicas[service],
            "successful": replication_result.get("outcome", {}).get("ok", False),
            "response": replication_result
        }

        # TEST 4: Migration transparency
        print("Testing migration transparency...")
        migration_result = client.migrate(service, "ran-1")
        migrated_location = client.locate(service)

        post_migration_access = client.access(
            service, task1, mode="single"
        )

        results["tests"]["migration_transparency"] = {
            "migration": migration_result,
            "resolved_location_after_migration": migrated_location,
            "access_after_migration": post_migration_access,
            "successful": (
                migrated_location == "ran-1"
                and post_migration_access.get("outcome", {}).get("ok", False)
            )
        }

        # TEST 5: Failure transparency
        print("Testing failure transparency...")

        detector = FailureDetector(
            cluster,
            interval=0.1,
            probe_timeout=0.15,
            misses_to_suspect=3
        )
        detector.start()

        injector.inject("edge-1", "service_down")

        deadline = time.monotonic() + 5.0
        while not detector.is_suspected("edge-1"):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Failure detector did not suspect edge-1 within five seconds"
                )
            time.sleep(0.05)

        failover_result = client.access(
            failover_service, task2, mode="failover"
        )

        results["tests"]["failure_transparency"] = {
            "failed_node": "edge-1",
            "failure_detected": detector.is_suspected("edge-1"),
            "failover_result": failover_result,
            "successful": (
                failover_result.get("outcome", {}).get("ok", False)
                and failover_result.get("outcome", {}).get("node") == "edge-2"
            ),
            "detector_events": detector.events
        }

        injector.clear("edge-1", "service_down")

        # Overall evaluation
        results["summary"] = {
            name: test["successful"]
            for name, test in results["tests"].items()
        }
        results["all_tests_passed"] = all(results["summary"].values())

        output_dir = os.path.join(
            os.path.dirname(__file__), "..", "results"
        )
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(
            output_dir, "milestone9_transparency.json"
        )

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, default=str)

        print("\nMILESTONE 9 RESULTS")
        print(json.dumps(results["summary"], indent=4))
        print("All tests passed:", results["all_tests_passed"])
        print("Results saved to:", output_file)

    finally:
        if detector is not None:
            detector.stop()
        cluster.stop_all()


if __name__ == "__main__":
    main()
