# examples/multi_node.py
import asyncio
from inferx.distributed.manager import ClusterManager


async def run_multi_node_example() -> None:
    print("=" * 60)
    print("INFERX MULTI-NODE CLUSTER BOOTSTRAP EXAMPLE")
    print("=" * 60)

    # Configure Node 1 (Seed Node)
    mgr1 = ClusterManager(
        node_id="node-1",
        host="127.0.0.1",
        port=19201,
        peers=[{"node_id": "node-2", "host": "127.0.0.1", "port": 19202}],
        hosted_models=["llama"],
    )

    # Configure Node 2
    mgr2 = ClusterManager(
        node_id="node-2",
        host="127.0.0.1",
        port=19202,
        peers=[{"node_id": "node-1", "host": "127.0.0.1", "port": 19201}],
        hosted_models=["gpt"],
    )

    # Start RPC servers
    print("Starting Cluster RPC listeners...")
    await mgr1.start()
    await mgr2.start()

    # Trigger Node 2 joining Node 1's cluster registry
    print("Node 2 joining cluster registry at Node 1...")
    await mgr2.join_cluster(mgr1.host, mgr1.port)

    # Activate Gossip and Consensus Elections
    print("Activating background monitoring loops...")
    await mgr1.start_services()
    await mgr2.start_services()

    # Stagger election timeouts to force Node 1 to become leader
    mgr1.election.election_min_ms = 40
    mgr1.election.election_max_ms = 80
    mgr2.election.election_min_ms = 150
    mgr2.election.election_max_ms = 200

    # Wait for consensus election to settle
    print("Waiting for leader election to complete...")
    await asyncio.sleep(0.5)

    # Query leader status
    leader_id = mgr2.election.get_leader()
    print(f"Cluster Leader elected  : {leader_id}")

    # Print membership statuses
    active_nodes = mgr1.registry.get_active_nodes()
    print("Active Cluster Nodes Registry:")
    for node in active_nodes:
        print(
            f"  - Node {node.node_id} ({node.host}:{node.port}) Status: {node.status.name} Models: {node.hosted_models}"
        )

    # Graceful shutdown
    print("Shutting down cluster nodes...")
    await mgr1.stop()
    await mgr2.stop()
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_multi_node_example())
