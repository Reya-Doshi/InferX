# examples/single_node.py
import asyncio
import uuid
from typing import Any, Dict
from inferx.core.event_bus import EventBus
from inferx.scheduler.admission import AdmissionController
from inferx.scheduler.batcher import DynamicBatcher
from inferx.scheduler.interfaces import ScheduledRequest


async def run_single_node_example() -> None:
    print("=" * 60)
    print("INFERX SINGLE NODE DEPLOYMENT EXAMPLE")
    print("=" * 60)

    # Initialize core Event Bus and Admission components
    event_bus = EventBus()
    admission = AdmissionController(event_bus=event_bus, max_queue_depth=100)
    batcher = DynamicBatcher(event_bus=event_bus, max_batch_size=8, timeout_ms=20.0)

    # Subscribe to batch completion events to simulate model execution
    def on_batch_ready(event: Dict[str, Any]) -> None:
        batch_id = event.get("batch_id")
        requests = event.get("requests", [])
        print(
            f"[Model Runtime] Processing batch {batch_id} (Size: {len(requests)} requests)"
        )
        for req in requests:
            print(f"  - Request {req['request_id']} executed on GPU stream 0")

    event_bus.subscribe("batch_ready", on_batch_ready)

    # Simulate submitting requests
    print("Submitting 5 requests to the admission controller...")
    for i in range(5):
        req = ScheduledRequest(
            request_id=f"req-{i}",
            model_name="llama-primary:v1.0",
            payload={"prompt": f"Hello world {i}"},
            priority=1,
            deadline_ms=500.0,
            token_count=16,
            tenant_id="default",
        )
        # Add to admission queue
        await admission.submit(req)

    # Wait for dynamic batcher timeout to compile and dispatch the batch
    print("Waiting for batch timer to expire...")
    await asyncio.sleep(0.1)
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_single_node_example())
