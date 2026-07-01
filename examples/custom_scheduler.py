# examples/custom_scheduler.py
import asyncio
from typing import List, Optional
from inferx.scheduler.interfaces import ISchedulingPolicy, ScheduledRequest


class LifoSchedulingPolicy(ISchedulingPolicy):
    """Custom scheduling policy that implements a LIFO (Last-In-First-Out) sorting order."""

    def __init__(self) -> None:
        self._queue: List[ScheduledRequest] = []

    def push(self, request: ScheduledRequest) -> None:
        self._queue.append(request)
        print(f"[LIFO Policy] Pushed request: {request.request_id}")

    def pop(self) -> Optional[ScheduledRequest]:
        if not self._queue:
            return None
        req = self._queue.pop()  # Pop the last element (LIFO)
        print(f"[LIFO Policy] Popped request LIFO target: {req.request_id}")
        return req

    def size(self) -> int:
        return len(self._queue)


async def run_custom_scheduler_example() -> None:
    print("=" * 60)
    print("INFERX CUSTOM SCHEDULER POLICY EXAMPLE")
    print("=" * 60)

    # Initialize our custom policy
    policy = LifoSchedulingPolicy()

    # Push requests
    print("Pushing requests req-A, req-B, req-C sequentially...")
    policy.push(
        ScheduledRequest(
            request_id="req-A", tenant_id="default", priority=1, payload="data-A"
        )
    )
    policy.push(
        ScheduledRequest(
            request_id="req-B", tenant_id="default", priority=1, payload="data-B"
        )
    )
    policy.push(
        ScheduledRequest(
            request_id="req-C", tenant_id="default", priority=1, payload="data-C"
        )
    )

    # Pop requests and assert LIFO ordering (C, then B, then A)
    print("\nPopping requests sequentially:")
    r1 = policy.pop()
    r2 = policy.pop()
    r3 = policy.pop()

    print("\nOrder Verification:")
    print(f"  - First Popped : {r1.request_id if r1 else 'None'} (Expected: req-C)")
    print(f"  - Second Popped: {r2.request_id if r2 else 'None'} (Expected: req-B)")
    print(f"  - Third Popped : {r3.request_id if r3 else 'None'} (Expected: req-A)")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_custom_scheduler_example())
