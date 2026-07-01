# inferx/gateway/router.py
"""
InferX Gateway Router.

Implements percentage-based Canary/Blue-Green routing, tenant-specific model routing,
and version mapping rules.
"""

import random
from typing import Dict, Optional, Tuple

from inferx.gateway.interfaces import IGatewayRouter, GatewayRequestContext
from inferx.utils.logging import get_logger

logger = get_logger("gateway.router")


class GatewayRouter(IGatewayRouter):
    """
    Evaluates request context attributes against routing rules to resolve target models.
    """

    def __init__(
        self,
        canary_weights: Optional[Dict[str, Dict[str, float]]] = None,
        tenant_routes: Optional[Dict[str, Tuple[str, str]]] = None,
    ) -> None:
        # Maps model_name -> version -> percentage_weight (summing to 1.0)
        self.canary_weights = canary_weights or {}
        # Maps tenant_id -> (model_name, version)
        self.tenant_routes = tenant_routes or {}

    def route(self, context: GatewayRequestContext) -> Tuple[str, str]:
        """
        Resolves routing targets.

        Evaluation order:
            1. Tenant specific overrides.
            2. Canary weight checks.
            3. Default request target details.
        """
        # 1. Tenant Routing overrides
        if context.tenant_id in self.tenant_routes:
            target_model, target_version = self.tenant_routes[context.tenant_id]
            logger.info(
                f"Routing request {context.request_id} via Tenant Route override: "
                f"{context.tenant_id} -> {target_model}:{target_version}",
                component="gateway_router",
            )
            return target_model, target_version

        # 2. Canary Weight checks
        model = context.model_name
        if model in self.canary_weights:
            weights = self.canary_weights[model]
            versions = list(weights.keys())
            probabilities = list(weights.values())

            # Weighted random selection
            selected_version = random.choices(versions, weights=probabilities, k=1)[0]
            logger.info(
                f"Routing request {context.request_id} via Canary Weights: "
                f"{model} -> {selected_version}",
                component="gateway_router",
            )
            return model, selected_version

        # 3. Default routing
        return context.model_name, context.version
