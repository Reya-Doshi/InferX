# inferx/distributed/manager.py
"""
InferX Cluster Manager.

Integrates RPC server, leader election, gossip membership, and replication states.
Handles cluster joins, leaves, and migrations.
"""
import asyncio
from typing import Any, Dict, List, Optional

from inferx.distributed.discovery import NodeRegistry, Membership
from inferx.distributed.election import LeaderElection
from inferx.distributed.interfaces import NodeInfo, NodeStatus
from inferx.distributed.rpc import ClusterRpcServer, ClusterRpcClient
from inferx.distributed.state import ClusterStateManager
from inferx.utils.logging import get_logger

logger = get_logger("distributed.manager")


class ClusterManager:
    """
    Control Plane Coordinator managing cluster node life cycle events.
    """
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[Dict[str, Any]],
        vram_capacity: int = 16 * 1024 * 1024 * 1024,
        hosted_models: Optional[List[str]] = None,
        security_token: str = "cluster-secret-key"
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.vram_capacity = vram_capacity
        self.hosted_models = hosted_models or []
        self.security_token = security_token

        # Instantiations
        self.registry = NodeRegistry()
        self.rpc_client = ClusterRpcClient(security_token=security_token)
        
        self.rpc_server = ClusterRpcServer(host=host, port=port, security_token=security_token)
        self.election = LeaderElection(node_id, peers, self.rpc_client)
        self.membership = Membership(node_id, self.registry, self.rpc_client)
        self.state_manager = ClusterStateManager(node_id, self.rpc_client)

        # Register local node definition
        self.local_node = NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            status=NodeStatus.ACTIVE,
            vram_capacity=vram_capacity,
            vram_used=0,
            hosted_models=self.hosted_models
        )
        self.registry.register_node(self.local_node)

        # Bind RPC Server handlers
        self.rpc_server.register_handler("request_vote", self.election.handle_request_vote)
        self.rpc_server.register_handler("heartbeat", self.election.handle_heartbeat)
        self.rpc_server.register_handler("ping", self.membership.handle_ping)
        self.rpc_server.register_handler("replicate_state", self.state_manager.handle_replicate_state)
        self.rpc_server.register_handler("join_node", self.handle_join_node)

    async def start(self) -> None:
        """Starts RPC listener."""
        await self.rpc_server.start()
        self.port = self.rpc_server.port
        
        # Update local node with actual server port
        self.local_node.port = self.rpc_server.port
        self.registry.register_node(self.local_node)
        logger.info(f"Cluster Coordinator Node {self.node_id} server active.", component="cluster_manager")

    async def start_services(self) -> None:
        """Starts Gossip membership and Leader election loops after all nodes are listening."""
        await self.membership.start()
        await self.election.start()
        logger.info(f"Cluster Coordinator Node {self.node_id} background loops active.", component="cluster_manager")

    async def stop(self) -> None:
        """Gracefully stops all cluster node activities."""
        await self.election.stop()
        await self.membership.stop()
        await self.rpc_server.stop()
        logger.info(f"Cluster Coordinator Node {self.node_id} successfully stopped.", component="cluster_manager")

    async def join_cluster(self, seed_host: str, seed_port: int) -> None:
        """
        Contacts a seed node to join an active cluster.
        
        Fetches active nodes and merges registries.
        """
        logger.info(f"Node {self.node_id} attempting to join cluster via seed {seed_host}:{seed_port}...", component="cluster_manager")
        
        try:
            res = await self.rpc_client.call(
                seed_host, seed_port,
                "join_node",
                {"node_info": self.local_node.model_dump()}
            )
            
            # Re-register returned cluster nodes
            nodes_data = res.get("active_nodes", [])
            for nd in nodes_data:
                node = NodeInfo(**nd)
                self.registry.register_node(node)
                
            logger.info(f"Node {self.node_id} successfully joined cluster via seed.", component="cluster_manager")
            
        except Exception as e:
            logger.error(f"Failed to join cluster via seed: {e}", exc_info=True, component="cluster_manager")
            raise

    async def handle_join_node(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Processes cluster join requests from new nodes."""
        node_data = params.get("node_info", {})
        if not node_data:
            return {"active_nodes": []}

        new_node = NodeInfo(**node_data)
        self.registry.register_node(new_node)
        self.registry.update_heartbeat(new_node.node_id)

        # Replicate join details across all current peers in background
        async def replicate_join_gossip():
            peers = [p for p in self.registry.get_active_nodes() if p.node_id not in [self.node_id, new_node.node_id]]
            for p in peers:
                try:
                    await self.rpc_client.call(
                        p.host, p.port,
                        "ping",
                        {"node_info": new_node.model_dump()}
                    )
                except Exception:
                    pass

        asyncio.create_task(replicate_join_gossip())

        # Return active nodes list to the joining node
        active_nodes = [node.model_dump() for node in self.registry.get_all_nodes()]
        return {"active_nodes": active_nodes}
