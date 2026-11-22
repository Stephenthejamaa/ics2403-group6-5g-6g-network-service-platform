"""
transparency.py
---------------
[MILESTONE 9] Client-facing transparency layer.

Applications access logical resources without needing to manage their
physical node locations, replica selection, failover, or migration.
"""

from common.fault_tolerance import ReplicatedClient


class TransparentClient:
    """Provides a uniform interface to logically named distributed resources."""

    def __init__(self, cluster, resolver, replica_sets=None,
                 detector=None, timeout=0.25):
        self.cluster = cluster
        self.resolver = resolver
        self.replica_sets = dict(replica_sets or {})
        self.detector = detector
        self.timeout = timeout

    def locate(self, logical_name):
        """Resolve a logical resource name to its current physical node."""
        return self.resolver.resolve(logical_name)

    def access(self, logical_name, task, mode="single"):
        """
        Access a resource using only its logical name.

        mode may be:
          single   - use one resolved node
          failover - try replicas until one succeeds
          quorum   - require a majority agreement
        """
        if mode not in ("single", "failover", "quorum"):
            raise ValueError("mode must be single, failover, or quorum")

        location = self.locate(logical_name)
        if location is None:
            return {
                "ok": False,
                "logical_resource": logical_name,
                "error": "resource_not_found",
                "mode": mode,
            }

        replicas = list(self.replica_sets.get(logical_name, []))

        # The resolver's current location is always the first candidate.
        replicas = [location] + [
            node for node in replicas if node != location
        ]

        client = ReplicatedClient(
            self.cluster,
            replicas,
            detector=self.detector,
            mode=mode,
            timeout=self.timeout,
        )

        outcome = client.request(task)

        return {
            "logical_resource": logical_name,
            "mode": mode,
            "resolved": True,
            "outcome": outcome,
        }

    def migrate(self, logical_name, destination):
        """
        Move a resource and update its naming information.

        The logical name remains unchanged. DNS-style cached answers may
        remain stale until their configured TTL expires.
        """
        if logical_name not in self.resolver.world.truth:
            raise KeyError(f"Unknown logical resource: {logical_name}")

        if destination not in self.cluster.configs:
            raise ValueError(f"Unknown destination node: {destination}")

        old_location = self.resolver.world.truth[logical_name]

        if old_location == destination:
            return {
                "logical_resource": logical_name,
                "old_location": old_location,
                "new_location": destination,
                "updated": False,
                "reason": "already_at_destination",
                "update_exchanges": 0,
            }

        # Move the physical resource, then update the naming scheme.
        self.resolver.world.relocate([logical_name], destination)
        exchanges = self.resolver.update(logical_name, destination)

        # Keep the configured replica list consistent with the migration.
        if logical_name in self.replica_sets:
            previous = self.replica_sets[logical_name]
            self.replica_sets[logical_name] = [destination] + [
                node for node in previous
                if node not in (old_location, destination)
            ]

        return {
            "logical_resource": logical_name,
            "old_location": old_location,
            "new_location": destination,
            "updated": True,
            "update_exchanges": exchanges,
            "resolved_location_after_update": self.locate(logical_name),
        }
