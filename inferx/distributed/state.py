# inferx/distributed/state.py
"""
InferX Cluster State Manager.

Coordinates metadata replication, model configuration registry syncs,
and follower states synchronization.
"""

import asyncio
from typing import Any, Dict, List, Optional

from inferx.distributed.rpc import ClusterRpcClient
from inferx.utils.logging import get_logger

logger = get_logger("distributed.state")


class ClusterStateManager:
    """
    Manages and replicates global cluster configuration states.
    """

    def __init__(
        self, node_id: str, rpc_client: Optional[ClusterRpcClient] = None
    ) -> None:
        self.node_id = node_id
        self.rpc_client = rpc_client or ClusterRpcClient()

        # Local cluster state metadata store
        self._state: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def get_value(self, key: str) -> Any:
        async with self._lock:
            return self._state.get(key)

    async def set_value(self, key: str, value: Any) -> None:
        async with self._lock:
            self._state[key] = value

    async def replicate_to_followers(
        self, peers: List[Dict[str, Any]], key: str, value: Any
    ) -> None:
        """
        Replicates a configuration key/value pair to all followers.

        Executes RPC replication updates in parallel.
        """
        async with self._lock:
            self._state[key] = value

        async def replicate_peer(peer: Dict[str, Any]) -> None:
            try:
                await self.rpc_client.call(
                    peer["host"],
                    peer["port"],
                    "replicate_state",
                    {"key": key, "value": value},
                )
            except Exception as e:
                logger.warning(
                    f"Replication failed to peer {peer['node_id']}: {e}",
                    component="state_manager",
                )

        tasks = [asyncio.create_task(replicate_peer(p)) for p in peers]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def handle_replicate_state(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Receives replicated configuration updates from the Leader."""
        key = params.get("key", "")
        value = params.get("value")

        if key:
            async with self._lock:
                self._state[key] = value
                logger.info(
                    f"Synchronized replicated state: {key} (Value: {value})",
                    component="state_manager",
                )

        return {"success": True}
