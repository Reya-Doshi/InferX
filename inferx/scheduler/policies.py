# inferx/scheduler/policies.py
"""
InferX Scheduling Policies.

Implements concrete sorting and queue management strategies:
FIFO, Priority Queue, Earliest Deadline First (EDF), Deficit Round Robin (DRR),
Priority Aging, and Adaptive Schedulers.
"""
from collections import deque
import heapq
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from inferx.scheduler.interfaces import ISchedulingPolicy, ScheduledRequest


class FIFOPolicy(ISchedulingPolicy):
    """Strict chronological First-In-First-Out queue (O(1) operations)."""
    def __init__(self) -> None:
        self._queue: deque[ScheduledRequest] = deque()

    def push(self, request: ScheduledRequest) -> None:
        self._queue.append(request)

    def pop(self) -> Optional[ScheduledRequest]:
        if not self._queue:
            return None
        return self._queue.popleft()

    def size(self) -> int:
        return len(self._queue)


class PriorityQueuePolicy(ISchedulingPolicy):
    """
    Heap-backed Priority Queue (O(log N) operations).
    
    Orders tasks based on priority (highest values first) and resolves ties
    chronologically using enqueue timestamps.
    """
    def __init__(self) -> None:
        self._heap: List[Tuple[int, int, int, ScheduledRequest]] = []
        self._counter = 0  # Prevents heap comparison falling through to ScheduledRequest

    def push(self, request: ScheduledRequest) -> None:
        # Pydantic validates non-negative priorities. We negate priority for max-heap behavior.
        priority_key = -request.priority
        heapq.heappush(self._heap, (priority_key, request.enqueue_timestamp_ns, self._counter, request))
        self._counter += 1

    def pop(self) -> Optional[ScheduledRequest]:
        if not self._heap:
            return None
        _, _, _, request = heapq.heappop(self._heap)
        return request

    def size(self) -> int:
        return len(self._heap)


class DeadlinePolicy(ISchedulingPolicy):
    """
    Earliest Deadline First (EDF) scheduler (O(log N) operations).
    
    Prioritizes requests with the closest absolute deadline (enqueue time + latency limit).
    """
    def __init__(self) -> None:
        self._heap: List[Tuple[int, int, int, ScheduledRequest]] = []
        self._counter = 0

    def push(self, request: ScheduledRequest) -> None:
        heapq.heappush(self._heap, (request.deadline_ns, request.enqueue_timestamp_ns, self._counter, request))
        self._counter += 1

    def pop(self) -> Optional[ScheduledRequest]:
        if not self._heap:
            return None
        _, _, _, request = heapq.heappop(self._heap)
        return request

    def size(self) -> int:
        return len(self._heap)


class WeightedFairQueuePolicy(ISchedulingPolicy):
    """
    Deficit Round Robin (DRR) scheduling policy (O(1) amortized operations).
    
    Allocates queue execution bandwidth fairly across tenants based on configured weights.
    """
    def __init__(self, tenant_weights: Optional[Dict[str, int]] = None, default_weight: int = 1) -> None:
        self.tenant_weights = tenant_weights or {}
        self.default_weight = default_weight
        
        # Deque for each active tenant
        self._queues: Dict[str, deque[ScheduledRequest]] = {}
        # Deficit counter for each active tenant
        self._deficits: Dict[str, int] = {}
        
        # Active tenant list for round-robin rotation
        self._active_tenants: List[str] = []
        self._current_index = 0
        self._total_size = 0

    def push(self, request: ScheduledRequest) -> None:
        tenant_id = request.tenant_id
        if tenant_id not in self._queues:
            self._queues[tenant_id] = deque()
            self._deficits[tenant_id] = 0
            self._active_tenants.append(tenant_id)

        self._queues[tenant_id].append(request)
        self._total_size += 1

    def pop(self) -> Optional[ScheduledRequest]:
        if self._total_size == 0:
            return None

        # Clean up empty queues from active rotation list
        self._active_tenants = [t for t in self._active_tenants if self._queues[t]]
        if not self._active_tenants:
            self._total_size = 0
            return None

        while True:
            if self._current_index >= len(self._active_tenants):
                self._current_index = 0

            tenant_id = self._active_tenants[self._current_index]
            queue = self._queues[tenant_id]

            # Accumulate deficit credit for this round if it was exhausted
            if self._deficits[tenant_id] <= 0:
                weight = self.tenant_weights.get(tenant_id, self.default_weight)
                self._deficits[tenant_id] += weight

            # DRR logic: Check if we have deficit capacity to pop a request
            if self._deficits[tenant_id] >= 1 and queue:
                self._deficits[tenant_id] -= 1
                self._total_size -= 1
                request = queue.popleft()
                
                # Rotate index if deficit is exhausted or queue becomes empty
                if self._deficits[tenant_id] == 0 or not queue:
                    self._deficits[tenant_id] = 0
                    self._current_index += 1
                return request
            else:
                # Rotate index to next tenant
                self._deficits[tenant_id] = 0
                self._current_index += 1

    def size(self) -> int:
        return self._total_size


class PriorityAgingPolicy(ISchedulingPolicy):
    """
    Priority Queue with dynamic starvation prevention (O(log N) pops, O(N) aging).
    
    Increases the aged priority key of tasks as they sit in the queue.
    """
    def __init__(self, aging_rate_per_sec: float = 1.0) -> None:
        self.aging_rate = aging_rate_per_sec
        self._heap: List[Tuple[float, int, int, ScheduledRequest]] = []
        self._counter = 0

    def push(self, request: ScheduledRequest) -> None:
        # Initialize aged priority with base priority
        request.aged_priority = float(request.priority)
        heapq.heappush(self._heap, (-request.aged_priority, request.enqueue_timestamp_ns, self._counter, request))
        self._counter += 1

    def pop(self) -> Optional[ScheduledRequest]:
        if not self._heap:
            return None
        _, _, _, request = heapq.heappop(self._heap)
        return request

    def size(self) -> int:
        return len(self._heap)

    def age_requests(self) -> None:
        """
        Recalculates aged priorities and heapifies the backing list.
        
        Should be executed periodically (off the hot path) to prevent starvation.
        """
        if not self._heap:
            return
        
        now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
        new_heap = []
        
        for _, timestamp_ns, counter, request in self._heap:
            elapsed_sec = (now_ns - timestamp_ns) / 1e9
            # Increase priority over time
            request.aged_priority = request.priority + (self.aging_rate * elapsed_sec)
            
            new_heap.append((-request.aged_priority, timestamp_ns, counter, request))
            
        heapq.heapify(new_heap)
        self._heap = new_heap


class AdaptivePolicy(ISchedulingPolicy):
    """
    Dynamic scheduler that adjusts scheduling policies based on system load.
    
    Under low load, acts as a strict priority queue. Under high load
    (queue length exceeds threshold), activates priority aging with increased rates
    to prevent starvation.
    """
    def __init__(
        self,
        base_policy: PriorityAgingPolicy,
        congestion_threshold: int = 100,
        boosted_aging_rate: float = 5.0
    ) -> None:
        self.policy = base_policy
        self.congestion_threshold = congestion_threshold
        self.boosted_aging_rate = boosted_aging_rate
        self.normal_aging_rate = base_policy.aging_rate

    def push(self, request: ScheduledRequest) -> None:
        self.policy.push(request)
        self._adapt_parameters()

    def pop(self) -> Optional[ScheduledRequest]:
        request = self.policy.pop()
        self._adapt_parameters()
        return request

    def size(self) -> int:
        return self.policy.size()

    def age_requests(self) -> None:
        self.policy.age_requests()

    def _adapt_parameters(self) -> None:
        """Adjusts the aging rate based on queue depth."""
        qsize = self.policy.size()
        if qsize >= self.congestion_threshold:
            # Under heavy load, accelerate aging rate to flush old requests
            self.policy.aging_rate = self.boosted_aging_rate
        else:
            self.policy.aging_rate = self.normal_aging_rate
