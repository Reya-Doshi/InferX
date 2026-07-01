# inferx/model/interfaces.py
"""
InferX Model Runtime Interfaces.

Defines model metadata configurations, execution contracts, and tokenizer abstractions.
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from pydantic import BaseModel, Field


class ModelMetadata(BaseModel):
    """
    Data model representing model versioning, memory sizing, and fallbacks.
    """
    model_name: str
    version: str
    backend_type: str  # pytorch, onnx, tensorrt, vllm
    estimated_vram_bytes: int = Field(default=0, ge=0)
    fallback_model_name: Optional[str] = None
    fallback_version: Optional[str] = None

    model_config = {"frozen": True}


class IModelInstance(ABC):
    """Interface representing an active loaded model instance in memory."""

    @abstractmethod
    async def predict(self, tokens: List[int]) -> List[int]:
        """Runs tensor inference, returning output token IDs."""
        pass

    @abstractmethod
    def get_metadata(self) -> ModelMetadata:
        """Returns the metadata configuration associated with this instance."""
        pass


class ITokenizer(ABC):
    """Abstract interface defining sequence-to-token text tokenization."""

    @abstractmethod
    def encode(self, text: str) -> List[int]:
        """Encodes raw text into a list of integer token IDs."""
        pass

    @abstractmethod
    def decode(self, tokens: List[int]) -> str:
        """Decodes token IDs back into readable text."""
        pass
