"""
naming.py
----------
[MILESTONE 10 - Naming and Distributed Process Management]

A NAME is what you call a resource ("/edge/z1/session/17"); a LOCATION is the
node it currently lives on. Name RESOLUTION turns one into the other:

        Name  ->  Name Resolution  ->  Resource Location

The brief lists five ways to do that; each is implemented here over the real
platform (real messages to real node processes), so their costs can be measured:

  StaticMapResolver       every client keeps a full copy of the table (a "hosts file").
                          Fastest possible lookup, but goes stale the moment anything moves.
  BroadcastResolver       shout "who hosts this name?" to EVERY node; the owner answers.
                          Needs no table at all, but costs one message per node.
  NameServerResolver      ONE central server holds the whole table. Simple and always
                          current, but a single point of failure and a bottleneck.
  PrefixTableResolver     the name space is split into ZONES by prefix (/edge, /ran, ...),
                          each served by a different node; the client holds a tiny prefix
                          table and goes straight to the right zone server.
  DnsStyleResolver        like the prefix table, but through a ROOT server that answers with
                          a REFERRAL ("ask that zone server"), plus caching with a
                          time-to-live (TTL) -- how the real Internet DNS works.

The truth of where a resource lives is the `hosted` set kept by each node itself;
the tables above are only COPIES of that truth, which is what makes them able to go stale.
"""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_message

ZONES = {"/edge": "edge-1", "/ran": "ran-1", "/core": "core-1", "/cloud": "cloud-1", "/proc": "core-1"}
ROOT = "core-1"
CENTRAL = "core-1"
TIER_NODES = {"edge": ["edge-1", "edge-2"], "ran": ["ran-1"], "core": ["core-1"], "cloud": ["cloud-1"]}
TIER_WEIGHTS = {"edge": 0.4, "ran": 0.2, "core": 0.2, "cloud": 0.2}
BULK = 2000


def prefix_of(name: str) -> str:
    return "/" + name.split("/")[1]


def make_names(n: int, seed: int = 10) -> dict:
    """n hierarchical names -> the node that hosts each, e.g. /edge/z3/session/41 -> edge-2."""
    rng = random.Random(seed)
    tiers, weights = list(TIER_WEIGHTS), list(TIER_WEIGHTS.values())
    services = ["session", "qos", "mobility", "billing", "analytics"]
    out = {}
    for i in range(n):
        tier = rng.choices(tiers, weights)[0]
        nodes = TIER_NODES[tier]
        out[f"/{tier}/z{rng.randint(1, 8)}/{rng.choice(services)}/{i}"] = nodes[i % len(nodes)]
    return out


# =============================================================================
# The world: ground truth + the tables installed on the nodes
# =============================================================================

class NameWorld:
    def __init__(self, cluster):
        self.cluster = cluster
        self.truth = {}

    def _send(self, node, msg, timeout=10.0):
        return send_message(*self.cluster.addr(node), msg, timeout=timeout)

    def _bulk(self, node, table, entries):
        items = list(entries.items())
        for s in range(0, len(items), BULK):
            self._send(node, {"type": "name_register", "table": table, "entries": dict(items[s:s + BULK])})

    def build(self, truth: dict):
        self.truth = dict(truth)
        for n in self.cluster.configs:
            self._send(n, {"type": "name_reset"})
        by_host, zone_entries = {}, {}
        for name, node in truth.items():
            by_host.setdefault(node, []).append(name)
            zone_entries.setdefault(ZONES[prefix_of(name)], {})[name] = node
        for node, names in by_host.items():
            for s in range(0, len(names), BULK):
                self._send(node, {"type": "name_host", "add": names[s:s + BULK]})
        self._bulk(CENTRAL, "central", truth)
        for server, entries in zone_entries.items():
            self._bulk(server, "zone", entries)
        self._send(ROOT, {"type": "name_set_referrals", "referrals": dict(ZONES)})

    def relocate(self, names, dst):
        """A resource PHYSICALLY moves. Only the truth changes; name servers are NOT told yet."""
        by_old = {}
        for name in names:
            by_old.setdefault(self.truth[name], []).append(name)
            self.truth[name] = dst
        for old, ns in by_old.items():
            self._send(old, {"type": "name_host", "remove": ns})
        self._send(dst, {"type": "name_host", "add": list(names)})


# =============================================================================
# Resolvers
# =============================================================================

class Resolver:
    scheme = "base"
    label = "base"

    def __init__(self, world):
        self.world, self.cluster = world, world.cluster
        self.exchanges = 0
        self._lock = threading.Lock()

    def _ex(self, node, msg, timeout=0.5):
        with self._lock:
            self.exchanges += 1
        return send_message(*self.cluster.addr(node), msg, timeout=timeout)

    def reset_counter(self):
        with self._lock:
            self.exchanges = 0

    def resolve(self, name):
        raise NotImplementedError

    def update(self, name, new_node) -> int:
        """Tell this scheme's tables that `name` now lives on `new_node`. Returns exchanges used."""
        raise NotImplementedError


class StaticMapResolver(Resolver):
    scheme, label = "static_map", "Static map"

    def __init__(self, world, n_clients=10):
        super().__init__(world)
        self.table = dict(world.truth)
        self.n_clients = n_clients

    def resolve(self, name):
        return self.table.get(name)

    def update(self, name, new_node):
        self.table[name] = new_node
        return self.n_clients            # every client's copy must be redistributed


class BroadcastResolver(Resolver):
    scheme, label = "broadcast", "Broadcast"

    def __init__(self, world):
        super().__init__(world)
        self._pool = ThreadPoolExecutor(max_workers=8)

    def resolve(self, name):
        def ask(node):
            try:
                r = self._ex(node, {"type": "name_query", "name": name}, timeout=0.3)
                return node if r.get("hosted") else None
            except Exception:
                return None
        answers = list(self._pool.map(ask, list(self.cluster.configs)))   # wait for every reply
        for found in answers:
            if found:
                return found
        return None

    def update(self, name, new_node):
        return 0                          # there is no table to update


class NameServerResolver(Resolver):
    scheme, label = "name_server", "Central name server"

    def resolve(self, name):
        try:
            r = self._ex(CENTRAL, {"type": "name_lookup", "table": "central", "name": name})
            return r["location"] if r["status"] == "found" else None
        except Exception:
            return None

    def register(self, name, node):
        self._ex(CENTRAL, {"type": "name_register", "table": "central", "entries": {name: node}})

    def unregister(self, name):
        self._ex(CENTRAL, {"type": "name_unregister", "table": "central", "names": [name]})

    def update(self, name, new_node):
        self.register(name, new_node)
        return 1


class PrefixTableResolver(Resolver):
    scheme, label = "prefix_table", "Prefix table (zones)"

    def __init__(self, world):
        super().__init__(world)
        self.prefix_table = dict(ZONES)

    def _zone_server(self, name):
        return self.prefix_table[prefix_of(name)]

    def resolve(self, name):
        try:
            r = self._ex(self._zone_server(name), {"type": "name_lookup", "table": "zone", "name": name})
            return r["location"] if r["status"] == "found" else None
        except Exception:
            return None

    def register(self, name, node):
        self._ex(self._zone_server(name), {"type": "name_register", "table": "zone", "entries": {name: node}})

    def unregister(self, name):
        self._ex(self._zone_server(name), {"type": "name_unregister", "table": "zone", "names": [name]})

    def update(self, name, new_node):
        self.register(name, new_node)
        return 1


class DnsStyleResolver(PrefixTableResolver):
    scheme, label = "dns_style", "DNS-style (root + cache)"

    def __init__(self, world, ttl_s=30.0):
        super().__init__(world)
        self.ttl_s = ttl_s
        self.ns_cache = {}               # prefix -> zone server  (like cached NS records)
        self.answer_cache = {}           # name -> (location, expires_at)
        self.cache_hits = self.lookups = 0

    def resolve(self, name):
        self.lookups += 1
        hit = self.answer_cache.get(name)
        if hit and time.time() < hit[1]:
            self.cache_hits += 1
            return hit[0]
        try:
            server = self.ns_cache.get(prefix_of(name))
            if server is None:
                r = self._ex(ROOT, {"type": "name_lookup", "table": "zone", "name": name})
                if r["status"] == "found":
                    loc = r["location"]
                    self.answer_cache[name] = (loc, time.time() + self.ttl_s)
                    return loc
                if r["status"] != "referral":
                    return None
                server = r["server"]
                self.ns_cache[r["prefix"]] = server
            r = self._ex(server, {"type": "name_lookup", "table": "zone", "name": name})
            if r["status"] != "found":
                return None
            self.answer_cache[name] = (r["location"], time.time() + self.ttl_s)
            return r["location"]
        except Exception:
            return None

    def update(self, name, new_node):
        self.register(name, new_node)    # the authoritative server learns at once; cached copies expire by TTL
        return 1


SCHEMES = ["static_map", "broadcast", "name_server", "prefix_table", "dns_style"]
RESOLVER_CLASSES = {"static_map": StaticMapResolver, "broadcast": BroadcastResolver, "name_server": NameServerResolver,
                    "prefix_table": PrefixTableResolver, "dns_style": DnsStyleResolver}


def exchanges_at_scale(scheme: str, n_nodes: int, dns_hit_rate: float = 0.7) -> float:
    """Request/reply exchanges one lookup costs in a system of n_nodes nodes (formula, validated live at n=5)."""
    return {"static_map": 0.0, "broadcast": float(n_nodes), "name_server": 1.0, "prefix_table": 1.0,
            "dns_style": (1 - dns_hit_rate) * 1.0}[scheme]


def workload_exchanges(scheme: str, nodes: int = 500, clients: int = 5000, lookups: int = 1000,
                       moves: int = 10, dns_hit_rate: float = 0.7) -> float:
    """
    Total request/reply exchanges a scheme needs for `lookups` lookups AND `moves` resource moves in a system
    of `nodes` nodes and `clients` clients. This is the scalability measure: it includes the cost of KEEPING
    THE NAMES CURRENT, which is what a lookups-only count hides (a static map costs nothing per lookup but
    must push every move to every client).
    """
    return {"static_map": moves * clients,
            "broadcast": lookups * nodes,
            "name_server": lookups + moves,
            "prefix_table": lookups + moves,
            "dns_style": lookups * (1 - dns_hit_rate) + moves}[scheme]