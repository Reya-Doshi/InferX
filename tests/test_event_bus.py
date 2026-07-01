# tests/test_event_bus.py
"""
InferX Event Bus Test Suite.

Verifies async publishing, wildcard subscriptions, PriorityQueue sorting,
historical event replays, and Dead Letter Queue (DLQ) overflow redirection.
"""

import asyncio
import unittest
from datetime import datetime, timezone

from inferx.event_bus.bus import EventBus
from inferx.event_bus.dlq import DeadLetterQueue
from inferx.event_bus.envelope import EventEnvelope
from inferx.event_bus.events import RequestReceived, BatchCreated, WorkerFailed


class TestEventBus(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the InferX Event Bus."""

    async def asyncSetUp(self) -> None:
        self.dlq = DeadLetterQueue()
        self.bus = EventBus(dlq=self.dlq, queue_capacity=10)

    async def test_publish_subscribe_flow(self) -> None:
        sub_id = self.bus.subscribe("RequestReceived")
        queue = self.bus.get_queue(sub_id)

        # Build payload and envelope
        payload = RequestReceived(
            request_id="req-111",
            model_name="llama-3-8b",
            tenant_id="customer-a",
            payload_size_bytes=1024,
        )
        envelope = EventEnvelope.create_from_payload(payload, priority=2)

        # Publish
        await self.bus.publish(envelope)

        # Verify reception
        self.assertEqual(queue.qsize(), 1)
        received = await queue.get()

        self.assertEqual(received.event_type, "RequestReceived")
        self.assertEqual(received.payload.request_id, "req-111")
        self.assertEqual(received.priority, 2)

    async def test_wildcard_subscription(self) -> None:
        # Wildcard subscriber receives all published events
        sub_id = self.bus.subscribe("*")
        queue = self.bus.get_queue(sub_id)

        e1 = EventEnvelope.create_from_payload(
            RequestReceived(
                request_id="req-222",
                model_name="llama",
                tenant_id="t",
                payload_size_bytes=1,
            )
        )
        e2 = EventEnvelope.create_from_payload(
            BatchCreated(batch_id="batch-123", batch_size=4, model_name="llama")
        )

        await self.bus.publish(e1)
        await self.bus.publish(e2)

        self.assertEqual(queue.qsize(), 2)

        r1 = await queue.get()
        r2 = await queue.get()

        self.assertEqual(r1.event_type, "RequestReceived")
        self.assertEqual(r2.event_type, "BatchCreated")

    async def test_priority_queue_sorting(self) -> None:
        # Use prioritized queue channel
        sub_id = self.bus.subscribe("RequestReceived", priority_queue=True)
        queue = self.bus.get_queue(sub_id)

        # Create envelopes with different priorities
        # Note: High priority value must be dequeued first
        payload = RequestReceived(
            request_id="req", model_name="m", tenant_id="t", payload_size_bytes=1
        )

        low_priority = EventEnvelope.create_from_payload(payload, priority=1)
        high_priority = EventEnvelope.create_from_payload(payload, priority=5)
        medium_priority = EventEnvelope.create_from_payload(payload, priority=3)

        # Publish in non-sequential order
        await self.bus.publish(low_priority)
        await self.bus.publish(high_priority)
        await self.bus.publish(medium_priority)

        self.assertEqual(queue.qsize(), 3)

        # Retrieve and verify correct priority sorting (5 -> 3 -> 1)
        first = await queue.get()
        second = await queue.get()
        third = await queue.get()

        self.assertEqual(first.priority, 5)
        self.assertEqual(second.priority, 3)
        self.assertEqual(third.priority, 1)

    async def test_historical_event_replay(self) -> None:
        start_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)

        # Publish some events
        payload = BatchCreated(batch_id="b-1", batch_size=2, model_name="m")
        e1 = EventEnvelope.create_from_payload(payload)
        await self.bus.publish(e1)

        # Wait briefly to ensure timestamp difference
        await asyncio.sleep(0.01)

        e2 = EventEnvelope.create_from_payload(payload)
        await self.bus.publish(e2)

        # Create new subscriber and replay events
        sub_id = self.bus.subscribe("BatchCreated")
        queue = self.bus.get_queue(sub_id)

        self.assertEqual(queue.qsize(), 0)

        # Replay
        await self.bus.replay(start_ns, sub_id)

        self.assertEqual(queue.qsize(), 2)

    async def test_dlq_queue_overflow_redirection(self) -> None:
        # Setup small queue capacity to trigger overflow
        overflow_bus = EventBus(dlq=self.dlq, queue_capacity=1)

        sub_id = overflow_bus.subscribe("WorkerFailed")
        queue = overflow_bus.get_queue(sub_id)

        payload = WorkerFailed(worker_id="w-1", gpu_id=0, exit_code=1, reason="crashed")
        e1 = EventEnvelope.create_from_payload(payload)
        e2 = EventEnvelope.create_from_payload(payload)

        # First publish fits in queue
        await overflow_bus.publish(e1)
        self.assertEqual(queue.qsize(), 1)

        # Second publish overflows queue and should route to DLQ
        await overflow_bus.publish(e2)

        # Await a brief interval to allow background DLQ routing task to run
        await asyncio.sleep(0.1)

        failed_events = await self.dlq.get_failed_events()
        self.assertEqual(len(failed_events), 1)

        failed_envelope, reason, exc = failed_events[0]
        self.assertEqual(failed_envelope.event_type, "WorkerFailed")
        self.assertIn("Queue capacity overflow", reason)


if __name__ == "__main__":
    unittest.main()
