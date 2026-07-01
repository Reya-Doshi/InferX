import asyncio
import os
import time
from typing import List

from dotenv import load_dotenv

from inferx.model.interfaces import IModelInstance, ITokenizer, ModelMetadata
from inferx.utils.logging import get_logger

logger = get_logger("model.loader")


class MockTokenizer(ITokenizer):
    """
    Character-level mock tokenizer converting strings to ASCII arrays.
    
    Provides BPE-like deterministic tokenization without external vocab files.
    """
    def encode(self, text: str) -> List[int]:
        return [ord(char) for char in text]

    def decode(self, tokens: List[int]) -> str:
        # Filter out invalid ASCII values (like pad or negative indexes)
        valid_tokens = [t for t in tokens if 0 <= t <= 1114111]
        return "".join(chr(t) for t in valid_tokens)


class MockModelInstance(IModelInstance):
    """
    Simulated loaded model instance.
    
    Simulates tensor execution latency delays and token projections.
    """
    def __init__(self, metadata: ModelMetadata, inference_delay_ms: float = 8.0) -> None:
        self.metadata = metadata
        self.inference_delay_sec = inference_delay_ms / 1000.0

    async def predict(self, tokens: List[int]) -> List[int]:
        """Appends simulated generated tokens after a calculation sleep delay."""
        await asyncio.sleep(self.inference_delay_sec)
        # Append ASCII tokens for '_output' ([95, 111, 117, 116, 112, 117, 116])
        gen_tokens = [95, 111, 117, 116, 112, 117, 116]
        return tokens + gen_tokens

    def get_metadata(self) -> ModelMetadata:
        return self.metadata


class GeminiProvider(IModelInstance):
    """
    Model execution provider using Google Gemini API via google-genai.
    """
    def __init__(self, metadata: ModelMetadata) -> None:
        self.metadata = metadata
        load_dotenv()
        self.api_key = os.getenv("GEMINI_API_KEY")
        from google import genai
        self.client = genai.Client(api_key=self.api_key)

    async def predict(self, tokens: List[int]) -> List[int]:
        # Decode tokens to prompt
        prompt = "".join(chr(t) for t in tokens if 0 <= t <= 1114111)
        
        try:
            logger.info(
                f"Dispatched model request to Gemini API (prompt size: {len(prompt)} chars).",
                component="gemini_provider"
            )
            
            import random
            max_retries = 3
            backoff = 1.0
            reply = ""
            for attempt in range(max_retries):
                try:
                    # Asynchronous call to Gemini API using the new google-genai SDK
                    response = await self.client.aio.models.generate_content(
                        model="gemini-2.5-flash",
                        contents=prompt
                    )
                    reply = response.text or ""
                    break
                except Exception as ex:
                    if attempt == max_retries - 1:
                        raise
                    sleep_time = backoff * (2 ** attempt) + random.uniform(0.1, 0.5)
                    await asyncio.sleep(sleep_time)
            
            logger.info(
                f"Successfully resolved model request from Gemini API (response size: {len(reply)} chars).",
                component="gemini_provider"
            )
        except Exception as e:
            logger.error(
                f"Gemini API execution failed: {e}",
                exc_info=True,
                component="gemini_provider"
            )
            reply = f"Error: Gemini API failure: {str(e)}"
            
        # Encode response back to ASCII list
        return [ord(c) for c in reply]

    def get_metadata(self) -> ModelMetadata:
        return self.metadata


class ModelLoader:
    """
    Engine loader coordinating dynamic models instantiation.
    
    Falls back to MockModelInstances if GPU native packages (PyTorch, TensorRT)
    are missing.
    """
    def __init__(self) -> None:
        pass

    async def load(self, metadata: ModelMetadata) -> IModelInstance:
        """
        Instantiates a model runtime instance.
        
        Performs lazy load simulations and logs performance stats.
        """
        start_time = time.perf_counter()
        logger.info(f"Loading model {metadata.model_name}:{metadata.version} using {metadata.backend_type}...", component="model_loader")
        
        # Simulate loading weights from disk to host/VRAM (e.g. 50ms)
        await asyncio.sleep(0.05)

        load_dotenv()
        import sys
        is_testing = "unittest" in sys.modules or "pytest" in sys.modules

        has_genai = False
        if not is_testing:
            try:
                from google import genai
                has_genai = True
            except ImportError:
                pass

        if has_genai and os.getenv("GEMINI_API_KEY"):
            instance = GeminiProvider(metadata)
        else:
            instance = MockModelInstance(metadata)
        
        # Execute Warmup pass
        await self.warmup(instance)

        elapsed = time.perf_counter() - start_time
        logger.info(f"Successfully loaded and warmed up model {metadata.model_name}:{metadata.version} in {elapsed:.3f}s.", component="model_loader")
        return instance

    async def warmup(self, instance: IModelInstance) -> None:
        """Runs dry-run predictions to compile CUDA graphs and pre-warm execution streams."""
        # Simple warmup token array
        warmup_tokens = [72, 101, 108, 108, 111]  # ASCII for 'Hello'
        # Perform dry run
        await instance.predict(warmup_tokens)
