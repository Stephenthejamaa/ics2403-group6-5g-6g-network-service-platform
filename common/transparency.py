"""
transparency.py
----------------
[MILESTONE 9 - Transparency and Reliability]

"Transparency" means the application deals with a LOGICAL resource
("the subscriber database") and never with the PHYSICAL machines behind it.
The brief names five kinds; each is a different thing being hidden:

  ACCESS        hide HOW a node is accessed. One node speaks a legacy wire format;
                the AccessLayer translates, so the application's code is identical.
  LOCATION      hide WHERE the resource is. The application never names a node; a
                Directory maps the logical name to wherever it lives right now.
  REPLICATION   hide that there are SEVERAL copies. Writes go to every copy,
                reads come from one; the application sees a single resource.
  MIGRATION     hide that the resource MOVED. Live migration copies the data to a
                new node while the application keeps reading and writing.
  FAILURE       hide that a node DIED. The client skips the dead copy, and when the
                node returns, its data is REPAIRED from a healthy copy.

Consistency rule used throughout: every stored value carries a VERSION number and a
node only accepts a write whose version is NEWER than what it holds. That one rule
makes writes safe to repeat and lets repair/migration copy old data without ever
overwriting newer data (last-writer-wins by version). The TransparentClient is the
single sequencer that hands out versions.
"""

import itertools
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_message
from common.node import _encode_legacy, _decode_legacy

TIER_ORDER = {"edge-1": 0, "edge-2": 0, "ran-1": 1, "core-1": 2, "cloud-1": 3}   # nearest first
COPY_CHUNK = 250     # keys per copy step during repair / migration
LIVE_COPY_PAUSE_S = 0.010   # live migration is THROTTLED: 250 keys per 10 ms = ~25,000 keys/s, so it never starves clients


# =============================================================================
# Physical addressing (NO transparency) -- the baseline the experiments compare against
# =============================================================================

class NaiveClient:
    """The application names a specific node and must speak that node's own format."""

    def __init__(self, cluster, node, timeout=0.5):
        self.cluster, self.node, self.timeout = cluster, node, timeout

    def put(self, key, value, version):
        h, p = self.cluster.addr(self.node)
        return send_message(h, p, {"type": "kv_put", "key": key, "value": value, "version": version},
                            timeout=self.timeout)

    def get(self, key):
        h, p = self.cluster.addr(self.node)
        return send_message(h, p, {"type": "kv_get", "key": key}, timeout=self.timeout)


# =============================================================================
# ACCESS transparency
# =============================================================================

class AccessLayer:
    """Asks each node which wire format it speaks (once) and translates to/from it."""

    def __init__(self, cluster, timeout=0.5):
        self.cluster, self.timeout = cluster, timeout
        self._fmt = {}
        self.translations = 0

    def format_of(self, node):
        f = self._fmt.get(node)
        if f is None:
            h, p = self.cluster.addr(node)
            caps = send_message(h, p, {"type": "kv_capabilities"}, timeout=self.timeout)
            f = "native" if "native" in caps["formats"] else "legacy"
            self._fmt[node] = f
        return f

    def forget(self, node):
        self._fmt.pop(node, None)

    def put(self, node, key, value, version):
        h, p = self.cluster.addr(node)
        if self.format_of(node) == "native":
            return send_message(h, p, {"type": "kv_put", "key": key, "value": value, "version": version},
                                timeout=self.timeout)
        self.translations += 1
        return send_message(h, p, {"type": "kv_put_legacy", "key": key, "payload": _encode_legacy(value),
                                   "version": version}, timeout=self.timeout)

    def get(self, node, key):
        h, p = self.cluster.addr(node)
        if self.format_of(node) == "native":
            return send_message(h, p, {"type": "kv_get", "key": key}, timeout=self.timeout)
        self.translations += 1
        r = send_message(h, p, {"type": "kv_get_legacy", "key": key}, timeout=self.timeout)
        if r.get("found"):
            r["value"] = _decode_legacy(r["payload"])
        return r


# =============================================================================
# LOCATION transparency: the Directory (logical name -> current physical replicas)
# =============================================================================

class Directory:
    """logical resource -> {node: state}.  States: active | joining | recovering | down."""

    def __init__(self):
        self._lock = threading.RLock()
        self._res = {}
        self.lookups = 0

    def create(self, logical, nodes):
        with self._lock:
            self._res[logical] = {n: "active" for n in nodes}

    def replicas(self, logical, states=("active",)):
        with self._lock:
            self.lookups += 1
            nodes = [n for n, st in self._res[logical].items() if st in states]
        return sorted(nodes, key=lambda n: (TIER_ORDER[n], n))      # nearest first

    def snapshot(self, logical):
        with self._lock:
            return dict(self._res[logical])

    def set_state(self, logical, node, state):
        with self._lock:
            self._res[logical][node] = state

    def add(self, logical, node, state):
        with self._lock:
            self._res[logical][node] = state

    def remove(self, logical, node):
        with self._lock:
            self._res[logical].pop(node, None)

    def mark_down(self, logical, node):
        with self._lock:
            if self._res[logical].get(node) in ("active", "joining", "recovering"):
                self._res[logical][node] = "down"


# =============================================================================
# The transparent client (what the application actually uses)
# =============================================================================

class TransparentClient:
    def __init__(self, cluster, directory, timeout=0.5):
        self.cluster, self.dir, self.timeout = cluster, directory, timeout
        self.access = AccessLayer(cluster, timeout)
        self._versions = itertools.count(1)
        self._vlock = threading.Lock()
        self._mlock = threading.Lock()
        self.messages = 0
        self.write_gate = threading.Event()      # only closed by the stop-the-world migration BASELINE
        self.write_gate.set()
        self._pool = ThreadPoolExecutor(max_workers=16)

    # ---- bookkeeping ----
    def _next_version(self):
        with self._vlock:
            return next(self._versions)

    def _count(self, n=1):
        with self._mlock:
            self.messages += n

    def _mgmt(self, node, msg, timeout=5.0):
        h, p = self.cluster.addr(node)
        self._count()
        return send_message(h, p, msg, timeout=timeout)

    # ---- the application-facing API: logical names only ----
    def put(self, logical, key, value):
        t0 = time.perf_counter()
        if not self.write_gate.is_set():
            self.write_gate.wait(timeout=15)
        targets = self.dir.replicas(logical, ("active", "joining", "recovering"))
        version = self._next_version()

        def one(node):
            try:
                r = self.access.put(node, key, value, version)
                return node, r.get("status") == "ok"
            except Exception:
                return node, False

        results = [one(targets[0])] if len(targets) == 1 else list(self._pool.map(one, targets))
        self._count(len(targets))
        acked = [n for n, ok in results if ok]
        failed = [n for n, ok in results if not ok]
        for n in failed:
            self.dir.mark_down(logical, n)           # FAILURE transparency: stop using it, carry on
        ok = len(acked) >= 1
        return {"ok": ok, "version": version if ok else None, "acked": acked, "failed": failed,
                "latency_ms": (time.perf_counter() - t0) * 1000.0}

    def get(self, logical, key):
        t0 = time.perf_counter()
        for node in self.dir.replicas(logical, ("active",)):
            try:
                r = self.access.get(node, key)
                self._count()
                if r.get("status") != "ok":
                    continue
                return {"ok": True, "found": r["found"], "value": r.get("value"), "version": r.get("version"),
                        "node": node, "latency_ms": (time.perf_counter() - t0) * 1000.0}
            except Exception:
                self._count()
                self.dir.mark_down(logical, node)    # try the next copy
        return {"ok": False, "found": False, "value": None, "version": None, "node": None,
                "latency_ms": (time.perf_counter() - t0) * 1000.0}

    # ---- copying data between replicas (used by repair and migration) ----
    def _copy_all(self, src, dst, pause_s=0.0):
        offset, applied, total = 0, 0, 0
        while True:
            r = self._mgmt(src, {"type": "kv_dump", "offset": offset, "limit": COPY_CHUNK})
            total = r["total"]
            if r["items"]:
                applied += self._mgmt(dst, {"type": "kv_load", "items": r["items"]})["applied"]
            offset += COPY_CHUNK
            if offset >= total:
                return total, applied
            if pause_s:
                time.sleep(pause_s)

    def recover(self, logical, node):
        """FAILURE transparency, second half: bring a restarted node back up to date, then re-admit it."""
        t0 = time.time()
        self.access.forget(node)
        self.dir.set_state(logical, node, "recovering")       # receives new writes, but is not read from yet
        src = next(n for n in self.dir.replicas(logical, ("active",)) if n != node)
        total, applied = self._copy_all(src, node)
        self.dir.set_state(logical, node, "active")
        return {"seconds": time.time() - t0, "keys": total, "keys_repaired": applied, "source": src}

    def migrate_live(self, logical, src, dst):
        """MIGRATION transparency: move a replica src -> dst while reads and writes continue."""
        t0 = time.time()
        self.dir.add(logical, dst, "joining")         # from now on every write ALSO goes to dst
        total, applied = self._copy_all(src, dst, pause_s=LIVE_COPY_PAUSE_S)   # copy history, throttled; the version rule protects newer writes
        self.dir.set_state(logical, dst, "active")
        self.dir.remove(logical, src)
        try:
            self._mgmt(src, {"type": "kv_clear"})
        except Exception:
            pass
        return {"seconds": time.time() - t0, "keys": total, "keys_applied": applied}

    def migrate_stop_the_world(self, logical, src, dst):
        """BASELINE: freeze all writes, copy, switch, unfreeze. Correct, but writes stall for the whole copy."""
        t0 = time.time()
        self.write_gate.clear()
        try:
            time.sleep(0.02)                          # let writes already in flight finish
            total, applied = self._copy_all(src, dst)
            self.dir.add(logical, dst, "active")
            self.dir.remove(logical, src)
            try:
                self._mgmt(src, {"type": "kv_clear"})
            except Exception:
                pass
        finally:
            self.write_gate.set()
        return {"seconds": time.time() - t0, "keys": total, "keys_applied": applied}

    def divergence(self, logical):
        """How many keys differ between the copies? (0 = every replica holds identical data)"""
        dumps = {}
        for node, st in self.dir.snapshot(logical).items():
            if st == "down":
                continue
            try:
                dumps[node] = self._mgmt(node, {"type": "kv_dump"})["items"]
            except Exception:
                continue
        keys = set().union(*[set(d) for d in dumps.values()]) if dumps else set()
        divergent = 0
        for k in keys:
            versions = {d[k][1] if k in d else None for d in dumps.values()}
            if len(versions) > 1:
                divergent += 1
        return {"keys": len(keys), "divergent_keys": divergent, "replicas_compared": sorted(dumps)}
