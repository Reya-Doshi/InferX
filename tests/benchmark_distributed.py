# tests/benchmark_distributed.py
"""
InferX Distributed Control Plane Performance Benchmark.

Evaluates leader election settle times and state replication latencies.
"""

import asyncio
import time

from inferx.distributed.manager import ClusterManager
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def run_distributed_benchmark() -> None:
    # Setup 3 nodes
    mgr1 = ClusterManager(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
        peers=[
            {"node_id": "node-2", "host": "127.0.0.1", "port": 0},
            {"node_id": "node-3", "host": "127.0.0.1", "port": 0},
        ],
    )

    mgr2 = ClusterManager(
        node_id="node-2",
        host="127.0.0.1",
        port=0,
        peers=[
            {"node_id": "node-1", "host": "127.0.0.1", "port": 0},
            {"node_id": "node-3", "host": "127.0.0.1", "port": 0},
        ],
    )

    mgr3 = ClusterManager(
        node_id="node-3",
        host="127.0.0.1",
        port=0,
        peers=[
            {"node_id": "node-1", "host": "127.0.0.1", "port": 0},
            {"node_id": "node-2", "host": "127.0.0.1", "port": 0},
        ],
    )

    # Start servers
    await mgr1.start()
    await mgr2.start()
    await mgr3.start()

    # Link ports
    mgr1.election.peers = [
        {"node_id": "node-2", "host": "127.0.0.1", "port": mgr2.port},
        {"node_id": "node-3", "host": "127.0.0.1", "port": mgr3.port},
    ]
    mgr2.election.peers = [
        {"node_id": "node-1", "host": "127.0.0.1", "port": mgr1.port},
        {"node_id": "node-3", "host": "127.0.0.1", "port": mgr3.port},
    ]
    mgr3.election.peers = [
        {"node_id": "node-1", "host": "127.0.0.1", "port": mgr1.port},
        {"node_id": "node-2", "host": "127.0.0.1", "port": mgr2.port},
    ]

    # Make campaign timeouts extremely fast for benchmarking and stagger them to avoid split votes
    mgr1.election.election_min_ms = 15
    mgr1.election.election_max_ms = 30

    mgr2.election.election_min_ms = 80
    mgr2.election.election_max_ms = 100

    mgr3.election.election_min_ms = 150
    mgr3.election.election_max_ms = 180

    for m in [mgr1, mgr2, mgr3]:
        m.election.heartbeat_ms = 5

    # Start gossip/elections
    await mgr1.start_services()
    await mgr2.start_services()
    await mgr3.start_services()

    print("\n" + "=" * 70)
    print("INFERX DISTRIBUTED CONTROL PLANE PERFORMANCE BENCHMARK")
    print("=" * 70)

    # --- BENCHMARK 1: Leader Election Time ---
    t_start = time.perf_counter()

    # Poll until leader is elected
    leader_node_id = None
    for _ in range(50):
        await asyncio.sleep(0.01)
        leaders = [
            mgr1.election.get_leader(),
            mgr2.election.get_leader(),
            mgr3.election.get_leader(),
        ]
        if leaders[0] and leaders[0] == leaders[1] == leaders[2]:
            leader_node_id = leaders[0]
            break

    election_time_ms = (time.perf_counter() - t_start) * 1000.0
    print(f"Leader Elected Node ID   : {leader_node_id}")
    print(f"Election Settle Duration : {election_time_ms:.2f} ms")
    print("-" * 70)

    # --- BENCHMARK 2: Replication Latency ---
    # Find leader manager
    leader_mgr = None
    followers = []
    for m in [mgr1, mgr2, mgr3]:
        if m.election.state == "LEADER":
            leader_mgr = m
        else:
            followers.append(m)

    if not leader_mgr:
        print("Benchmark Error: No leader elected.")
        await mgr1.stop()
        await mgr2.stop()
        await mgr3.stop()
        return

    peers = [{"node_id": f.node_id, "host": f.host, "port": f.port} for f in followers]

    replication_count = 100
    t_start = time.perf_counter()

    for idx in range(replication_count):
        await leader_mgr.state_manager.replicate_to_followers(
            peers, f"key_{idx}", f"val_{idx}"
        )

    duration = time.perf_counter() - t_start
    avg_replication_ms = (duration / replication_count) * 1000.0

    print(f"Total Replications Run   : {replication_count}")
    print(
        f"Average Replication Time : {avg_replication_ms:.3f} ms (Leader -> 2 Followers)"
    )
    print("=" * 70 + "\n")

    # Stop all
    await mgr1.stop()
    await mgr2.stop()
    await mgr3.stop()


if __name__ == "__main__":
    asyncio.run(run_distributed_benchmark())
