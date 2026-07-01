# inferx/admission/interfaces.py
"""
InferX Admission Controller Interfaces.

Declares the structural interfaces and models for rate limiters,
load shedders, and the primary IAdmissionController coordinator.
"""
from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel, Field

from inferx.scheduler.interfaces import ScheduledRequest


class AdmissionVerdict(BaseModel):
    """
    Data model representing the decision outcome of a request admission check.
    
    Contains execution status codes and retry recommendations.
    """
    admitted: bool
    error_code: Optional[str] = None
    status_code: int = Field(default=200, ge=100, le=599)  # HTTP Status code equivalent
    retry_after_sec: float = Field(default=0.0, ge=0.0)

    model_config = {"frozen": True}


class IAdmissionController(ABC):
    """Abstract interface defining the gatekeeper logic of the inference pipeline."""

    @abstractmethod
    async def admit(self, request: ScheduledRequest) -> AdmissionVerdict:
        """
        Asynchronously evaluates a request against rate limits and system load.
        
        Args:
            request: The ScheduledRequest to assess.
            
        Returns:
            An AdmissionVerdict indicating approval or rejection criteria.
        """
        pass
