# inferx/distributed/scheduler.py
"""
InferX Distributed Scheduler.

Implements load-aware, model-aware, and GPU-aware task scheduling,
delegating tasks to remote nodes if local queue thresholds are exceeded.
"""

from typing import Any, Dict, Optional

from inferx.distributed.discovery import NodeRegistry
from inferx.distributed.rpc import ClusterRpcClient
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.utils.logging import get_logger

logger = get_logger("distributed.scheduler")


class DistributedScheduler:
    """
    Control Plane Scheduler delegating requests across cluster nodes.
    """

    def __init__(
        self,
        node_id: str,
        registry: NodeRegistry,
        local_scheduler: Any,  # IScheduler
        rpc_client: Optional[ClusterRpcClient] = None,
        local_queue_threshold: int = 5,
    ) -> None:
        self.node_id = node_id
        self.registry = registry
        self.local_scheduler = local_scheduler
        self.rpc_client = rpc_client or ClusterRpcClient()
        self.local_queue_threshold = local_queue_threshold

    async def schedule_request(
        self, request: ScheduledRequest, target_model: str, target_version: str
    ) -> str:
        """
        Schedules a request, deciding between local and remote execution.

        Evaluates queue thresholds and remote loads.
        """
        self.registry.get_node(self.node_id)

        # 1. Schedule locally if queue depth is below threshold, or no remote nodes exist
        local_depth = (
            len(self.local_scheduler._queue)
            if hasattr(self.local_scheduler, "_queue")
            else 0
        )
        active_peers = [
            n for n in self.registry.get_active_nodes() if n.node_id != self.node_id
        ]

        if local_depth < self.local_queue_threshold or not active_peers:
            logger.debug(
                f"Scheduling request {request.request_id} locally. Queue depth: {local_depth}",
                component="distributed_scheduler",
            )
            # In a real setup, we submit to local engine. We simulate local resolution:
            return f"local_execution_{self.node_id}"

        # 2. Schedule remotely (Load & Model-aware scheduling)
        # Filter nodes hosting target model
        candidate_nodes = [n for n in active_peers if target_model in n.hosted_models]
        if not candidate_nodes:
            # Fallback to any node if none hosts target model (lazy loading fallback)
            candidate_nodes = active_peers

        # Select the node with the lowest GPU utilization ratio (load-aware scheduling)
        candidate_nodes.sort(key=lambda n: n.gpu_utilization)
        selected_node = candidate_nodes[0]

        logger.info(
            f"Overload detected. Delegating request {request.request_id} to remote Node "
            f"{selected_node.node_id} (GPU Util: {selected_node.gpu_utilization:.2f})",
            component="distributed_scheduler",
        )

        try:
            # Dispatch request to remote node
            res = await self.rpc_client.call(
                selected_node.host,
                selected_node.port,
                "schedule_remote_task",
                {
                    "request_id": request.request_id,
                    "tenant_id": request.tenant_id,
                    "priority": request.priority,
                    "payload": request.payload,
                    "model": target_model,
                    "version": target_version,
                },
            )
            return res.get("result", f"remote_execution_{selected_node.node_id}")

        except Exception as e:
            logger.warning(
                f"Failed to delegate task {request.request_id} to Node {selected_node.node_id}: {e}. "
                "Falling back to local execution.",
                component="distributed_scheduler",
            )
            return f"local_execution_{self.node_id}"

    async def handle_remote_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """RPC Endpoint: Receives and enqueues a delegated task from a remote peer."""
        req_id = params.get("request_id", "")
        params.get("tenant_id", "default")
        params.get("priority", 1)
        params.get("payload", "")
        params.get("model", "llama")
        params.get("version", "latest")

        logger.info(
            f"Received delegated task {req_id} from remote node.",
            component="distributed_scheduler",
        )

        # Simulate local queuing and processing
        # In a real environment, we'd wrap this in a ScheduledRequest and call local_scheduler.submit()
        return {"result": f"processed_remotely_on_{self.node_id}"}
