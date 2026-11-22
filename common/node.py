"""
node.py
-------
[MILESTONE 1 - Distributed OS Foundation]
[MILESTONE 2 - Distributed Processing and Performance] (execution + metrics)
[MILESTONE 3 - Distributed Architecture] (service decomposition metadata)
[MILESTONE 4 - Distributed Algorithms and Coordination] (Lamport clock + Bully election)
[MILESTONE 5 - Distributed Transactions] (2PC participant: prepare/commit/abort)
[MILESTONE 6 - Concurrency and Deadlock Management] (per-node lock manager)
[MILESTONE 7 - Fault Tolerance and Recovery] (fault-injection hooks + service health probe)
[MILESTONE 9 - Transparency] (a versioned key-value "subscriber store" with two wire formats)
[MILESTONE 10 - Naming and Process Management] (name tables, hosted-resource set, long-running processes)

Defines Node, the fundamental unit of the Distributed 5G/6G Network
Service Platform. Each Node:

  - Runs as its OWN operating-system process (multiprocessing.Process),
    demonstrating real process-level distribution rather than a single
    program simulating multiple "virtual" nodes in-memory.
  - Owns a ResourceManager (CPU/memory/bandwidth) and a ProcessManager
    (process creation + scheduling).
  - Hosts a MessageServer (TCP) so other nodes / the orchestrator can
    submit work to it -> the basic inter-node communication mechanism.
  - Simulates task EXECUTION by holding resources for a duration
    proportional to the task's work_units (this is what lets Milestone
    2 measure throughput, latency and utilization meaningfully).
  - Tracks lightweight metrics (tasks completed, tasks dropped/failed,
    per-task latency) used directly by Milestone 2's performance
    monitor.

Node types in Theme 1 (Distributed 5G/6G Network Service Platform):
    EDGE, RAN (radio/access network), CORE, CLOUD
"""

import time
import random
import threading
from multiprocessing import Process as OSProcess

from common.resource_manager import ResourceManager
from common.process_manager import ProcessManager, Process, ProcessState
from common.communication import MessageServer, send_message
from common.coordination import LamportClock
from common.lock_manager import LockManager
from common.integrity import task_checksum


# ---- [M9] LEGACY wire format: "field=t:value;field=t:value" (t = s/i/f/b type tag) ----
def _encode_legacy(value: dict) -> str:
    parts = []
    for k, v in value.items():
        if isinstance(v, bool):
            parts.append(f"{k}=b:{1 if v else 0}")
        elif isinstance(v, int):
            parts.append(f"{k}=i:{v}")
        elif isinstance(v, float):
            parts.append(f"{k}=f:{v!r}")
        else:
            parts.append(f"{k}=s:{v}")
    return ";".join(parts)


def _decode_legacy(payload: str) -> dict:
    out = {}
    for part in filter(None, payload.split(";")):
        k, rest = part.split("=", 1)
        t, v = rest.split(":", 1)
        out[k] = {"b": lambda x: x == "1", "i": int, "f": float, "s": str}[t](v)
    return out


class Node:
    def __init__(self, name: str, node_type: str, host: str, port: int,
                 cpu_capacity: float, memory_capacity: float, bandwidth_capacity: float,
                 scheduling_policy: str = "priority", packet_loss_rate: float = 0.0,
                 election_id: int = 0, services: tuple = ()):
        self.name = name
        self.node_type = node_type  # EDGE | RAN | CORE | CLOUD
        self.host = host
        self.port = port
        self.resource_manager = ResourceManager(cpu_capacity, memory_capacity, bandwidth_capacity)
        self.process_manager = ProcessManager(name, self.resource_manager, policy=scheduling_policy)
        self.packet_loss_rate = packet_loss_rate  # simulated link unreliability (Milestone 2)
        self.election_id = election_id            # [M4] rank for Bully leader election
        self.services = services                   # [M3] microservices this node hosts
        self.lamport_clock = LamportClock()         # [M4] logical clock, ticks on every message
        self.current_leader = None                  # [M4] last-known elected coordinator
        self.lock_manager = LockManager()            # [M6] per-node resource lock table
        self.transaction_log = {}                     # [M5] tx_id -> {"state":..., "reservation":...}
        self.kv = {}                                    # [M9] key -> (value, version): the replicated subscriber store
        self.kv_legacy_only = False                      # [M9] True = this node only speaks the LEGACY wire format
        self.kv_lock = threading.Lock()
        self.hosted = set()                              # [M10] names of resources physically hosted HERE (the truth)
        self.name_tables = {"central": {}, "zone": {}}   # [M10] name -> location tables this node serves as a name server
        self.referrals = {}                              # [M10] prefix -> zone-server node (used when this node is the root)
        self.running_procs = {}                          # [M10] name -> Process (long-running managed processes)
        self.fault_mode = "none"                       # [M7] none|omission|delay|service_down|byzantine
        self.fault_params = {}                          # [M7] parameters for the active fault
        self.metrics = {
            "tasks_received": 0,
            "tasks_completed": 0,
            "tasks_dropped": 0,
            "tasks_rejected_no_resources": 0,
            "task_latencies_ms": [],
        }
        self._server = None

    # ---------- message handling ----------

    def _handle_message(self, message: dict) -> dict:
        msg_type = message.get("type")

        # [M4] Every inbound message advances this node's Lamport clock per
        # Lamport's rule (local_clock = max(local, received) + 1), giving a
        # consistent logical event order across the whole distributed system.
        self.lamport_clock.update(message.get("lamport_ts", 0))

        # [M7] Fault-injection CONTROL messages are exempt from the faults they
        # control -- otherwise a node with an active fault could never be told to stop.
        if msg_type == "fault_inject":
            self.fault_mode = message.get("mode", "none")
            self.fault_params = message.get("params", {})
            return {"status": "ok", "node": self.name, "fault_mode": self.fault_mode}
        if msg_type == "fault_clear":
            self.fault_mode, self.fault_params = "none", {}
            return {"status": "ok", "node": self.name, "fault_mode": "none"}

        # [M7] Apply an active fault to every OTHER message (including heartbeats,
        # so the failure detector genuinely experiences the fault).
        if self.fault_mode == "omission":
            # Receives the message but never answers in time (an OMISSION failure).
            if random.random() < self.fault_params.get("probability", 1.0):
                time.sleep(self.fault_params.get("hold_s", 3.0))
                return {"status": "omitted", "node": self.name}
        elif self.fault_mode == "delay":
            # Slow / degraded link (a COMMUNICATION failure).
            time.sleep(self.fault_params.get("delay_ms", 400) / 1000.0)

        # [M7] Service-level health probe: distinguishes "node is alive" from
        # "the SERVICE on this node is actually working" (a SERVICE failure).
        if msg_type == "service_health":
            ok = self.fault_mode != "service_down"
            return {"status": "ok" if ok else "service_unavailable", "node": self.name}

        if msg_type == "ping":
            return {"status": "ok", "node": self.name, "node_type": self.node_type,
                    "services": list(self.services), "lamport_ts": self.lamport_clock.value}

        if msg_type == "task_request":
            return self._handle_task_request(message)

        if msg_type == "metrics_request":
            return self._metrics_snapshot()

        if msg_type == "process_table_request":
            return {"status": "ok", "process_table": self.process_manager.process_table()}

        if msg_type == "service_info":
            return {"status": "ok", "node": self.name, "node_type": self.node_type,
                    "services": list(self.services)}

        # [M4] Bully leader election: a peer with a lower election_id is
        # asking "are you alive and higher-ranked?" -- since it only
        # contacts higher-ranked peers, replying at all means "yes, back off".
        if msg_type == "election":
            return {"status": "ok", "node": self.name, "election_id": self.election_id,
                    "lamport_ts": self.lamport_clock.value}

        # [M4] A peer has won an election and is announcing itself as leader.
        if msg_type == "coordinator":
            self.current_leader = message.get("leader")
            return {"status": "ack", "node": self.name, "acknowledged_leader": self.current_leader,
                    "lamport_ts": self.lamport_clock.value}

        if msg_type == "leader_query":
            return {"status": "ok", "node": self.name, "current_leader": self.current_leader,
                    "lamport_ts": self.lamport_clock.value}

        # ---- [MILESTONE 5] Two-Phase Commit participant side ----
        if msg_type == "tx_prepare":
            return self._handle_tx_prepare(message)
        if msg_type == "tx_commit":
            return self._handle_tx_commit(message)
        if msg_type == "tx_abort":
            return self._handle_tx_abort(message)
        if msg_type == "tx_release":
            return self._handle_tx_release(message)

        # ---- [MILESTONE 6] Distributed lock manager ----
        if msg_type == "lock_acquire":
            result = self.lock_manager.acquire(message["resource"], message["tx_id"],
                                                timeout=message.get("timeout", 15.0))
            return {"status": result, "node": self.name}
        if msg_type == "lock_release":
            self.lock_manager.release(message["resource"], message["tx_id"])
            return {"status": "released", "node": self.name}
        if msg_type == "lock_state_request":
            return {"status": "ok", "node": self.name, **self.lock_manager.snapshot()}
        if msg_type == "lock_mark_victim":
            self.lock_manager.mark_victim(message["tx_id"])
            return {"status": "marked", "node": self.name}

        ext = self._handle_ext(msg_type, message)
        if ext is not None:
            return ext

        return {"status": "error", "error": f"unknown message type '{msg_type}'"}

    # ---------- [MILESTONES 9 & 10] key-value store, naming, managed processes ----------

    def _handle_ext(self, msg_type: str, m: dict):
        # ---- [M9] key-value subscriber store (data plane: two wire formats) ----
        if msg_type == "kv_configure":
            self.kv_legacy_only = bool(m.get("legacy_only", False))
            return {"status": "ok", "node": self.name, "legacy_only": self.kv_legacy_only}
        if msg_type == "kv_capabilities":
            return {"status": "ok", "node": self.name,
                    "formats": ["legacy"] if self.kv_legacy_only else ["native", "legacy"]}
        if msg_type in ("kv_put", "kv_put_legacy"):
            legacy = msg_type == "kv_put_legacy"
            if self.kv_legacy_only and not legacy:
                return {"status": "unsupported_format", "node": self.name}
            value = _decode_legacy(m["payload"]) if legacy else m["value"]
            with self.kv_lock:
                cur = self.kv.get(m["key"])
                applied = cur is None or m["version"] > cur[1]
                if applied:
                    self.kv[m["key"]] = (value, m["version"])
            return {"status": "ok", "node": self.name, "applied": applied}
        if msg_type in ("kv_get", "kv_get_legacy"):
            legacy = msg_type == "kv_get_legacy"
            if self.kv_legacy_only and not legacy:
                return {"status": "unsupported_format", "node": self.name}
            with self.kv_lock:
                cur = self.kv.get(m["key"])
            if cur is None:
                return {"status": "ok", "node": self.name, "found": False}
            out = {"status": "ok", "node": self.name, "found": True, "version": cur[1]}
            if legacy:
                out["payload"] = _encode_legacy(cur[0])
            else:
                out["value"] = cur[0]
            return out
        # Management plane (replication / repair / migration): uniform on every node.
        if msg_type == "kv_dump":
            with self.kv_lock:
                keys = sorted(self.kv)
                chunk = keys[m.get("offset", 0): m.get("offset", 0) + m["limit"]] if "limit" in m else keys
                items = {k: [self.kv[k][0], self.kv[k][1]] for k in chunk}
            return {"status": "ok", "node": self.name, "items": items, "total": len(keys)}
        if msg_type == "kv_load":
            applied = 0
            with self.kv_lock:
                for k, (v, ver) in m["items"].items():
                    cur = self.kv.get(k)
                    if cur is None or ver > cur[1]:
                        self.kv[k] = (v, ver)
                        applied += 1
            return {"status": "ok", "node": self.name, "applied": applied}
        if msg_type == "kv_clear":
            with self.kv_lock:
                self.kv.clear()
            return {"status": "ok", "node": self.name}

        # ---- [M10] naming ----
        if msg_type == "name_host":
            self.hosted.update(m.get("add", []))
            self.hosted.difference_update(m.get("remove", []))
            return {"status": "ok", "node": self.name, "hosted": len(self.hosted)}
        if msg_type == "name_query":                       # used by BROADCAST resolution
            return {"status": "ok", "node": self.name, "hosted": m["name"] in self.hosted}
        if msg_type == "name_register":
            self.name_tables[m["table"]].update(m["entries"])
            return {"status": "ok", "node": self.name, "size": len(self.name_tables[m["table"]])}
        if msg_type == "name_unregister":
            for n in m["names"]:
                self.name_tables[m["table"]].pop(n, None)
            return {"status": "ok", "node": self.name}
        if msg_type == "name_lookup":                       # a NAME SERVER answering a lookup
            tbl = self.name_tables[m["table"]]
            if m["name"] in tbl:
                return {"status": "found", "node": self.name, "location": tbl[m["name"]]}
            best = None
            for prefix in self.referrals:                   # root server: longest-prefix referral
                if m["name"].startswith(prefix) and (best is None or len(prefix) > len(best)):
                    best = prefix
            if best is not None:
                return {"status": "referral", "node": self.name, "server": self.referrals[best], "prefix": best}
            return {"status": "not_found", "node": self.name}
        if msg_type == "name_set_referrals":
            self.referrals = dict(m["referrals"])
            return {"status": "ok", "node": self.name}
        if msg_type == "name_reset":
            self.hosted.clear()
            self.name_tables = {"central": {}, "zone": {}}
            self.referrals = {}
            return {"status": "ok", "node": self.name}

        # ---- [M10] long-running managed processes ----
        if msg_type == "proc_start":
            spec, reserved_tx = m["spec"], m.get("reserved_tx")
            if reserved_tx is not None:
                entry = self.transaction_log.get(reserved_tx)
                if entry is None or entry["state"] != "committed":
                    return {"status": "rejected", "node": self.name, "reason": "reservation_not_committed"}
            proc = Process(name=m["name"], service_class=spec.get("service_class", "eMBB"),
                           cpu_required=spec["cpu"], memory_required=spec["memory"],
                           bandwidth_required=spec["bandwidth"], work_units=spec.get("work_units", 0))
            if reserved_tx is None:
                if not self.resource_manager.allocate(str(proc.pid), proc.cpu_required,
                                                      proc.memory_required, proc.bandwidth_required):
                    return {"status": "rejected", "node": self.name, "reason": "insufficient_resources"}
            proc.state = ProcessState.RUNNING
            proc.started_at = time.time()
            proc.reserved_tx = reserved_tx
            self.process_manager.all_processes[proc.pid] = proc
            self.running_procs[m["name"]] = proc
            return {"status": "started", "node": self.name, "pid": proc.pid}
        if msg_type == "proc_terminate":
            proc = self.running_procs.pop(m["name"], None)
            if proc is None:
                return {"status": "not_found", "node": self.name}
            if getattr(proc, "reserved_tx", None) is None:      # group-reserved processes are freed by tx_release
                self.resource_manager.release(str(proc.pid), proc.cpu_required,
                                              proc.memory_required, proc.bandwidth_required)
            proc.state = ProcessState.TERMINATED
            proc.finished_at = time.time()
            return {"status": "terminated", "node": self.name}
        if msg_type == "proc_status":
            return {"status": "running" if m["name"] in self.running_procs else "unknown", "node": self.name}
        if msg_type == "proc_list":
            return {"status": "ok", "node": self.name, "running": sorted(self.running_procs)}
        return None

    # ---------- [MILESTONE 5] Two-Phase Commit participant handlers ----------

    def _handle_tx_prepare(self, message: dict) -> dict:
        """
        PREPARE: "could you do this reservation, if asked?" We check
        (via ResourceManager.can_allocate) but do NOT actually allocate
        yet -- that only happens on tx_commit. This is what makes the
        transaction ATOMIC: nothing real changes until every participant
        has already agreed it CAN.
        """
        tx_id = message["tx_id"]
        r = message["reservation"]
        can_do = self.resource_manager.can_allocate(r["cpu"], r["memory"], r["bandwidth"])
        self.transaction_log[tx_id] = {
            "state": "prepared" if can_do else "vote_abort", "reservation": r,
        }
        return {"status": "ok", "node": self.name, "vote": "commit" if can_do else "abort"}

    def _handle_tx_commit(self, message: dict) -> dict:
        """COMMIT: the coordinator confirmed every participant voted commit -> actually apply it now."""
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id)
        if entry is None or entry["state"] != "prepared":
            return {"status": "error", "node": self.name, "error": "no such prepared transaction"}
        r = entry["reservation"]
        ok = self.resource_manager.allocate(tx_id, r["cpu"], r["memory"], r["bandwidth"])
        entry["state"] = "committed" if ok else "commit_failed"
        return {"status": "committed" if ok else "commit_failed", "node": self.name}

    def _handle_tx_abort(self, message: dict) -> dict:
        """ABORT: at least one participant said no (or was unreachable) -> everyone discards the reservation."""
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id, {"state": "aborted", "reservation": {}})
        entry["state"] = "aborted"
        self.transaction_log[tx_id] = entry
        return {"status": "aborted", "node": self.name}

    def _handle_tx_release(self, message: dict) -> dict:
        """
        RELEASE (session teardown): frees resources held by a previously
        COMMITTED transaction. Not part of the formal 2PC vote -- this
        models the end of a session's lifetime (establish -> use ->
        teardown), so a stream of transactions doesn't permanently
        accumulate resource usage forever.
        """
        tx_id = message["tx_id"]
        entry = self.transaction_log.get(tx_id)
        if entry is None or entry["state"] != "committed":
            return {"status": "error", "node": self.name, "error": "no committed transaction to release"}
        r = entry["reservation"]
        self.resource_manager.release(tx_id, r["cpu"], r["memory"], r["bandwidth"])
        entry["state"] = "released"
        return {"status": "released", "node": self.name}

    def _handle_task_request(self, message: dict) -> dict:
        t_recv = time.time()
        self.metrics["tasks_received"] += 1

        # Simulated packet loss on this node's inbound link (Milestone 2 reliability metric)
        if random.random() < self.packet_loss_rate:
            self.metrics["tasks_dropped"] += 1
            return {"status": "dropped", "node": self.name}

        task = message["task"]
        if self.fault_mode == "service_down":
            return {"status": "service_unavailable", "node": self.name}
        proc = self.process_manager.create_process(
            name=task["name"], service_class=task.get("service_class", "eMBB"),
            cpu=task["cpu"], memory=task["memory"], bandwidth=task["bandwidth"],
            work_units=task["work_units"],
        )

        proc, allocated = self.process_manager.dispatch_next()
        if not allocated:
            self.metrics["tasks_rejected_no_resources"] += 1
            return {"status": "rejected", "reason": "insufficient_resources", "node": self.name,
                     "pid": proc.pid if proc else None}

        # Simulate execution time: work_units translated into real (short) sleep,
        # scaled down so the whole distributed system runs quickly for grading/demo.
        exec_seconds = proc.work_units / 1000.0
        time.sleep(exec_seconds)

        self.process_manager.terminate(proc)
        self.metrics["tasks_completed"] += 1
        latency_ms = (time.time() - t_recv) * 1000.0
        self.metrics["task_latencies_ms"].append(latency_ms)

        result = task_checksum(task)
        if self.fault_mode == "byzantine":
            result ^= 0xDEADBEEF   # [M7] a Byzantine node answers confidently, but WRONGLY
        return {
            "status": "completed", "node": self.name, "node_type": self.node_type,
            "pid": proc.pid, "server_latency_ms": round(latency_ms, 3), "result": result,
        }

    def _metrics_snapshot(self) -> dict:
        lat = self.metrics["task_latencies_ms"]
        return {
            "status": "ok",
            "node": self.name,
            "node_type": self.node_type,
            "tasks_received": self.metrics["tasks_received"],
            "tasks_completed": self.metrics["tasks_completed"],
            "tasks_dropped": self.metrics["tasks_dropped"],
            "tasks_rejected_no_resources": self.metrics["tasks_rejected_no_resources"],
            "avg_latency_ms": round(sum(lat) / len(lat), 3) if lat else 0.0,
            "resource_utilization": self.resource_manager.utilization(),
            "current_leader": self.current_leader,
            "lamport_ts": self.lamport_clock.value,
            "transactions_seen": len(self.transaction_log),
            "lock_state": self.lock_manager.snapshot(),
        }

    # ---------- lifecycle ----------

    def run_forever(self):
        """Entry point when the node is launched as its own OS process."""
        self._server = MessageServer(self.host, self.port, self._handle_message)
        self._server.start()
        while True:
            time.sleep(1)


def _node_entry(config):
    """
    Module-level target function for multiprocessing.Process.

    Windows (and any platform using the 'spawn' start method) pickles
    the target + its arguments to hand off to the new process. `config`
    is a plain NodeConfig (no locks, fully picklable); the actual Node
    -- which owns a ResourceManager with a threading.Lock -- is built
    HERE, fresh, inside the child process, after the handoff. This is
    what makes node startup work identically on Linux, macOS and Windows.
    """
    node = Node(
        config.name, config.node_type, config.host, config.port,
        config.cpu_capacity, config.memory_capacity, config.bandwidth_capacity,
        scheduling_policy=config.scheduling_policy, packet_loss_rate=config.packet_loss_rate,
        election_id=config.election_id, services=config.services,
    )
    node.run_forever()


def start_node_process(config) -> OSProcess:
    """Launches a node described by `config` (a NodeConfig) as an independent OS process."""
    p = OSProcess(target=_node_entry, args=(config,), name=config.name, daemon=True)
    p.start()
    return p


def wait_until_up(host: str, port: int, timeout: float = 10.0) -> bool:
    """Polls a node with 'ping' until it responds or timeout elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            reply = send_message(host, port, {"type": "ping"}, timeout=1.0)
            if reply.get("status") == "ok":
                return True
        except Exception:
            time.sleep(0.1)
    return False
