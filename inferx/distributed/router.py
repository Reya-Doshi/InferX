# inferx/distributed/router.py
"""
InferX Distributed Router.

Implements routing algorithms across cluster nodes including tenant affinity
and sticky sessions hashes.
"""

import hashlib
from typing import Dict, Optional

from inferx.distributed.discovery import NodeRegistry
from inferx.distributed.interfaces import NodeInfo


class DistributedRouter:
    """
    Control Plane Router resolving task destination nodes in the cluster.
    """

    def __init__(
        self,
        registry: NodeRegistry,
        tenant_affinity: Optional[
            Dict[str, str]
        ] = None,  # tenant_id -> node_id overrides
    ) -> None:
        self.registry = registry
        self.tenant_affinity = tenant_affinity or {}

    def get_route(self, tenant_id: str, model_name: str) -> Optional[NodeInfo]:
        """
        Determines the target node for a request.

        Order of evaluation:
            1. Tenant affinity overrides.
            2. Sticky session hashing (consistent mapping).
        """
        active_nodes = self.registry.get_active_nodes()
        if not active_nodes:
            return None

        # 1. Tenant Affinity check
        if tenant_id in self.tenant_affinity:
            target_node_id = self.tenant_affinity[tenant_id]
            node = self.registry.get_node(target_node_id)
            if node and node.status != "DOWN":
                return node

        # 2. Filter nodes hosting model
        model_nodes = [n for n in active_nodes if model_name in n.hosted_models]
        nodes_to_hash = model_nodes if model_nodes else active_nodes

        # Sort nodes to guarantee deterministic ordering
        nodes_to_hash.sort(key=lambda n: n.node_id)

        # 3. Sticky Session Hashing (consistent hash mapping)
        hash_val = int(hashlib.md5(tenant_id.encode("utf-8")).hexdigest(), 16)
        target_index = hash_val % len(nodes_to_hash)

        return nodes_to_hash[target_index]
