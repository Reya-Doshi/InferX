# inferx/distributed/interfaces.py
"""
InferX Distributed Runtime Interfaces.

Defines cluster membership representations, leader election state machines,
and RPC communication boundaries.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class NodeStatus(str, Enum):
    """Lifecycle states of cluster nodes."""

    JOINING = "JOINING"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    DOWN = "DOWN"


class NodeInfo(BaseModel):
    """
    Metadata representation of a node's capabilities, load, and hosted models.
    """

    node_id: str
    host: str
    port: int
    status: NodeStatus = Field(default=NodeStatus.JOINING)
    vram_capacity: int = Field(default=0, ge=0)
    vram_used: int = Field(default=0, ge=0)
    cpu_utilization: float = Field(default=0.0, ge=0.0, le=1.0)
    gpu_utilization: float = Field(default=0.0, ge=0.0, le=1.0)
    hosted_models: List[str] = Field(default_factory=list)

    model_config = {"arbitrary_types_allowed": True}


class IRpcClient(ABC):
    """Abstract interface defining cross-node RPC clients."""

    @abstractmethod
    async def call(
        self, host: str, port: int, method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Dispatches an RPC request to a target remote node."""
        pass


class IRpcServer(ABC):
    """Abstract interface defining the RPC server listener."""

    @abstractmethod
    async def start(self) -> None:
        """Starts listening for remote RPC commands."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stops the RPC server."""
        pass


class ILeaderElection(ABC):
    """Abstract interface defining leader selection coordinators."""

    @abstractmethod
    async def start_election(self) -> None:
        """Triggers candidate campaign loops."""
        pass

    @abstractmethod
    def get_leader(self) -> Optional[str]:
        """Returns the node_id of the current cluster leader, if known."""
        pass
