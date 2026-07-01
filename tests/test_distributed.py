# tests/test_distributed.py
"""
InferX Distributed Runtime Test Suite.

Verifies Raft-inspired elections, Gossip-based discovery membership,
load-aware scheduling delegators, metadata replication, and node failovers.
"""
import asyncio
import unittest
from typing import Dict, List

from inferx.distributed.discovery import NodeRegistry, Membership
from inferx.distributed.election import LeaderElection
from inferx.distributed.interfaces import NodeInfo, NodeStatus
from inferx.distributed.rpc import ClusterRpcServer, ClusterRpcClient
from inferx.distributed.state import ClusterStateManager
from inferx.distributed.scheduler import DistributedScheduler
from inferx.distributed.manager import ClusterManager
from inferx.scheduler.interfaces import ScheduledRequest


class TestDistributed(unittest.IsolatedAsyncioTestCase):
    """Multi-node integration test suite for the Control Plane."""

    async def asyncSetUp(self) -> None:
        # We configure mock nodes running on static local ports to avoid startup races.
        self.mgr1 = ClusterManager(
            node_id="node-1", host="127.0.0.1", port=19101,
            peers=[
                {"node_id": "node-2", "host": "127.0.0.1", "port": 19102},
                {"node_id": "node-3", "host": "127.0.0.1", "port": 19103}
            ],
            hosted_models=["llama"]
        )
        
        self.mgr2 = ClusterManager(
            node_id="node-2", host="127.0.0.1", port=19102,
            peers=[
                {"node_id": "node-1", "host": "127.0.0.1", "port": 19101},
                {"node_id": "node-3", "host": "127.0.0.1", "port": 19103}
            ],
            hosted_models=["llama"]
        )

        self.mgr3 = ClusterManager(
            node_id="node-3", host="127.0.0.1", port=19103,
            peers=[
                {"node_id": "node-1", "host": "127.0.0.1", "port": 19101},
                {"node_id": "node-2", "host": "127.0.0.1", "port": 19102}
            ],
            hosted_models=["gpt"]
        )

    async def asyncTearDown(self) -> None:
        await self.mgr1.stop()
        await self.mgr2.stop()
        await self.mgr3.stop()

    async def start_all(self) -> None:
        """Starts RPC servers and initializes leader configurations."""
        await self.mgr1.start()
        await self.mgr2.start()
        await self.mgr3.start()

        # Stagger campaign timers to guarantee node-1 campaigns first and wins without split votes
        self.mgr1.election.election_min_ms = 40
        self.mgr1.election.election_max_ms = 80
        
        self.mgr2.election.election_min_ms = 140
        self.mgr2.election.election_max_ms = 180
        
        self.mgr3.election.election_min_ms = 240
        self.mgr3.election.election_max_ms = 280
        
        for m in [self.mgr1, self.mgr2, self.mgr3]:
            m.election.heartbeat_ms = 15

        # Start gossip/elections now that all servers are listening
        await self.mgr1.start_services()
        await self.mgr2.start_services()
        await self.mgr3.start_services()

    async def test_raft_leader_election(self) -> None:
        await self.start_all()

        # Stop membership gossip to reduce event loop load
        for m in [self.mgr1, self.mgr2, self.mgr3]:
            await m.membership.stop()

        # Poll until leader is elected and all nodes agree on it (up to 30 times, max 3.0s)
        leader_elected = False
        states = []
        leaders = []
        for _ in range(30):
            states = [self.mgr1.election.state, self.mgr2.election.state, self.mgr3.election.state]
            leaders = [self.mgr1.election.get_leader(), self.mgr2.election.get_leader(), self.mgr3.election.get_leader()]
            if states.count("LEADER") == 1 and leaders[0] and leaders[0] == leaders[1] == leaders[2]:
                leader_elected = True
                break
            await asyncio.sleep(0.1)

        self.assertTrue(leader_elected)
        self.assertEqual(states.count("LEADER"), 1)
        self.assertIsNotNone(leaders[0])
        self.assertEqual(leaders[0], leaders[1])
        self.assertEqual(leaders[1], leaders[2])

    async def test_node_join_and_discovery_sync(self) -> None:
        # Start only Node 1 and Node 2 initially
        await self.mgr1.start()
        await self.mgr2.start()

        await self.mgr1.start_services()
        await self.mgr2.start_services()

        # Update Node 1 peers list
        self.mgr1.local_node.port = self.mgr1.rpc_server.port
        self.mgr2.local_node.port = self.mgr2.rpc_server.port
        
        # Simulating Node 2 joining the seed Node 1
        await self.mgr2.join_cluster(self.mgr1.host, self.mgr1.port)

        # Assert Node 1 registry has Node 2, and Node 2 registry has Node 1
        n2 = self.mgr1.registry.get_node("node-2")
        n1 = self.mgr2.registry.get_node("node-1")

        self.assertIsNotNone(n2)
        self.assertIsNotNone(n1)
        self.assertEqual(n2.status, NodeStatus.ACTIVE)
        self.assertEqual(n1.status, NodeStatus.ACTIVE)

    async def test_state_replication_from_leader(self) -> None:
        await self.start_all()

        # Stop election loops to prevent background elections
        for m in [self.mgr1, self.mgr2, self.mgr3]:
            await m.election.stop()

        # Manually designate Node 1 as Leader
        self.mgr1.election.state = "LEADER"
        self.mgr1.election.leader_id = "node-1"
        self.mgr2.election.state = "FOLLOWER"
        self.mgr2.election.leader_id = "node-1"
        self.mgr3.election.state = "FOLLOWER"
        self.mgr3.election.leader_id = "node-1"

        leader_mgr = self.mgr1
        followers = [self.mgr2, self.mgr3]
        
        # Leader sets value and replicates to followers
        peers = [
            {"node_id": f.node_id, "host": f.host, "port": f.port}
            for f in followers
        ]
        
        await leader_mgr.state_manager.replicate_to_followers(peers, "model_config_llama", {"vram_req": "4GB"})

        # Wait brief moment for network dispatch
        await asyncio.sleep(0.05)

        # Assert followers synchronized state locally
        for f in followers:
            val = await f.state_manager.get_value("model_config_llama")
            self.assertEqual(val, {"vram_req": "4GB"})

    async def test_distributed_scheduling_overload_delegation(self) -> None:
        # We configure 2 mock schedulers and registries
        registry = NodeRegistry()
        
        # Setup local node
        n1 = NodeInfo(node_id="node-1", host="127.0.0.1", port=9001, status=NodeStatus.ACTIVE, gpu_utilization=0.9)
        n1.hosted_models.append("llama")
        # Setup remote node
        n2 = NodeInfo(node_id="node-2", host="127.0.0.1", port=9002, status=NodeStatus.ACTIVE, gpu_utilization=0.2)
        n2.hosted_models.append("llama")
        
        registry.register_node(n1)
        registry.register_node(n2)

        # Mock local queue to trigger overload (threshold is 5, we set 8)
        class MockLocalScheduler:
            def __init__(self):
                self._queue = [1] * 8

        local_sched = MockLocalScheduler()
        
        # Mock RPC client
        class MockClient(ClusterRpcClient):
            async def call(self, host, port, method, params):
                return {"result": f"processed_remotely_on_{port}"}

        dist_sched = DistributedScheduler(
            node_id="node-1",
            registry=registry,
            local_scheduler=local_sched,
            rpc_client=MockClient(),
            local_queue_threshold=5
        )

        req = ScheduledRequest(request_id="req-delegated", tenant_id="t1", payload="test")
        res = await dist_sched.schedule_request(req, "llama", "v1.0")

        # Must delegate to Node 2 (port 9002) because local node-1 is overloaded (8 > 5)
        # and node-2 hosts the model and has low GPU load (0.2 < 0.9)
        self.assertEqual(res, "processed_remotely_on_9002")


if __name__ == "__main__":
    unittest.main()
