"""
cluster.py
-----------
[MILESTONE 7 / 8 - shared experiment harness]

A small helper that owns the OS processes of the 5-node platform so an
experiment can START everything, KILL a node (a real crash: the OS
process is terminated), RESTART it (a real recovery: a brand-new
process on the same port, with empty state), and cleanly STOP
everything at the end.

Why this exists: Milestones 1-6 launched nodes and simply let them die
when the script exited. Fault tolerance needs the opposite -- nodes
that die and come back on purpose, several times, inside one run -- and
the experiment script must never leave stray node processes holding
ports after it finishes (which would make the NEXT script fail with
"port already in use").
"""

import time

from common.node import start_node_process, wait_until_up
from common.topology import build_nodes


class Cluster:
    def __init__(self, packet_loss_rate: float = 0.0):
        self.configs = {c.name: c for c in build_nodes(packet_loss_rate=packet_loss_rate)}
        self.procs = {}

    # ---- addressing helpers -------------------------------------------------
    def addr(self, name):
        c = self.configs[name]
        return (c.host, c.port)

    def directory(self, only=None):
        """name -> {host, port, election_id} (the shape BullyElection expects)."""
        names = only if only is not None else self.configs.keys()
        return {n: {"host": self.configs[n].host, "port": self.configs[n].port,
                    "election_id": self.configs[n].election_id} for n in names}

    # ---- lifecycle ----------------------------------------------------------
    def start_all(self):
        for name, cfg in self.configs.items():
            self.procs[name] = start_node_process(cfg)
        for name, cfg in self.configs.items():
            if not wait_until_up(cfg.host, cfg.port, timeout=15):
                raise RuntimeError(f"node {name} failed to start")

    def kill(self, name):
        """A genuine crash: terminate the node's OS process (no goodbye, no cleanup)."""
        p = self.procs.get(name)
        if p is not None and p.is_alive():
            p.terminate()
            p.join(timeout=3)

    def restart(self, name) -> float:
        """Recovery: start a NEW process for the node. Returns seconds until it answers pings."""
        t0 = time.time()
        self.kill(name)  # make sure the old one is really gone and the port is free
        cfg = self.configs[name]
        self.procs[name] = start_node_process(cfg)
        wait_until_up(cfg.host, cfg.port, timeout=15)
        return time.time() - t0

    def is_process_alive(self, name) -> bool:
        p = self.procs.get(name)
        return bool(p and p.is_alive())

    def stop_all(self):
        for name in list(self.procs):
            self.kill(name)
        time.sleep(0.2)
