# examples/streaming.py
import asyncio
import random
from typing import AsyncGenerator


async def token_generator(prompt: str) -> AsyncGenerator[str, None]:
    """Simulates a model runtime stream emitting tokens sequentially."""
    tokens = ["This", " is", " a", " streaming", " inference", " response", " from", " InferX", " model", " engine."]
    print(f"[Model Runtime] Processing prompt: '{prompt}'")
    for token in tokens:
        # Simulate time to compute next token (time-to-first-token vs inter-token latency)
        await asyncio.sleep(random.uniform(0.02, 0.05))
        yield token


async def run_streaming_example() -> None:
    print("=" * 60)
    print("INFERX STREAMING INFERENCE EXAMPLE")
    print("=" * 60)

    prompt = "Explain cloud-native AI scheduling"
    print("Initiating streaming call...")
    
    stream = token_generator(prompt)
    
    # Consume generated tokens as they arrive
    print("Client receiving tokens stream:")
    print("  ", end="", flush=True)
    async for token in stream:
        print(token, end="", flush=True)
        
    print("\n\nStream complete.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_streaming_example())
