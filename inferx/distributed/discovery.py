# inferx/distributed/discovery.py
"""
InferX Service Discovery & Membership.

Manages Node registries and executes Gossip-based heartbeat cycles to track
live nodes and detect failures.
"""
import asyncio
import random
import time
from typing import Any, Dict, List, Optional

from inferx.distributed.interfaces import NodeInfo, NodeStatus
from inferx.distributed.rpc import ClusterRpcClient
from inferx.utils.logging import get_logger

logger = get_logger("distributed.discovery")


class NodeRegistry:
    """
    Thread-safe inventory of active nodes and metadata.
    """
    def __init__(self) -> None:
        self._nodes: Dict[str, NodeInfo] = {}
        self._last_update: Dict[str, float] = {}
        self._lock = threading_lock()

    def register_node(self, node: NodeInfo) -> None:
        """Adds or updates a node definition."""
        with self._lock:
            self._nodes[node.node_id] = node
            self._last_update[node.node_id] = time.perf_counter()
            logger.info(f"Registered Node: {node.node_id} ({node.host}:{node.port}) Status: {node.status}", component="discovery")

    def remove_node(self, node_id: str) -> None:
        """Removes node registration details."""
        with self._lock:
            self._nodes.pop(node_id, None)
            self._last_update.pop(node_id, None)

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        with self._lock:
            return self._nodes.get(node_id)

    def get_active_nodes(self) -> List[NodeInfo]:
        """Returns all nodes not in the DOWN status."""
        with self._lock:
            return [node for node in self._nodes.values() if node.status != NodeStatus.DOWN]

    def get_all_nodes(self) -> List[NodeInfo]:
        with self._lock:
            return list(self._nodes.values())

    def update_heartbeat(self, node_id: str) -> None:
        """Resets the last-seen check timer for a node."""
        with self._lock:
            self._last_update[node_id] = time.perf_counter()

    def check_failures(self, timeout_sec: float) -> List[str]:
        """
        Scans nodes and marks them DOWN if they exceed the timeout.
        
        Returns:
            A list of failed node_ids.
        """
        now = time.perf_counter()
        failed = []
        
        with self._lock:
            for node_id, last_time in list(self._last_update.items()):
                node = self._nodes.get(node_id)
                if node and node.status != NodeStatus.DOWN:
                    if now - last_time > timeout_sec:
                        node.status = NodeStatus.DOWN
                        failed.append(node_id)
                        logger.warning(
                            f"Node {node_id} heartbeat timeout exceeded. Marked status as DOWN.",
                            component="discovery"
                        )
        return failed


class Membership:
    """
    Gossip membership protocol orchestrator.
    """
    def __init__(
        self,
        node_id: str,
        registry: NodeRegistry,
        rpc_client: Optional[ClusterRpcClient] = None,
        heartbeat_interval_sec: float = 0.5,
        failure_timeout_sec: float = 2.0
    ) -> None:
        self.node_id = node_id
        self.registry = registry
        self.rpc_client = rpc_client or ClusterRpcClient()
        
        self.heartbeat_interval = heartbeat_interval_sec
        self.failure_timeout = failure_timeout_sec
        
        self._is_active = False
        self._lock = asyncio.Lock()
        
        self._gossip_task: Optional[asyncio.Task[None]] = None
        self._failure_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Starts Gossip heartbeats and failure detector loops."""
        async with self._lock:
            if self._is_active:
                return
            self._is_active = True
            
            self._gossip_task = asyncio.create_task(self._gossip_loop())
            self._failure_task = asyncio.create_task(self._failure_loop())

    async def stop(self) -> None:
        """Stops Gossip membership loops."""
        async with self._lock:
            self._is_active = False
            
            if self._gossip_task:
                self._gossip_task.cancel()
                self._gossip_task = None
            if self._failure_task:
                self._failure_task.cancel()
                self._failure_task = None

    async def handle_ping(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Receives node gossip update and resets failure timer."""
        node_data = params.get("node_info", {})
        if node_data:
            node_info = NodeInfo(**node_data)
            # Re-register node status details
            self.registry.register_node(node_info)
            self.registry.update_heartbeat(node_info.node_id)
            
        return {"success": True}

    async def _gossip_loop(self) -> None:
        """Periodically pings random active peers to disperse status."""
        while self._is_active:
            await asyncio.sleep(self.heartbeat_interval)
            
            # Fetch own node stats
            my_node = self.registry.get_node(self.node_id)
            if not my_node:
                continue

            # Pick a random active peer
            peers = [n for n in self.registry.get_active_nodes() if n.node_id != self.node_id]
            if not peers:
                continue
                
            peer = random.choice(peers)
            
            try:
                # Ping peer with node metadata
                await self.rpc_client.call(
                    peer.host, peer.port,
                    "ping",
                    {"node_info": my_node.model_dump()}
                )
            except Exception:
                pass

    async def _failure_loop(self) -> None:
        """Checks for node heartbeat timeout breaches."""
        while self._is_active:
            await asyncio.sleep(self.heartbeat_interval)
            # Evaluate timeouts
            self.registry.check_failures(self.failure_timeout)


# Helper function to obtain lock primitives dynamically
def threading_lock() -> Any:
    import threading
    return threading.Lock()
