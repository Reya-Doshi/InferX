# inferx/admission/limiter.py
"""
InferX Rate Limiters.

Implements sub-microsecond Token Bucket and Leaky Bucket algorithms
with per-tenant isolation locks.
"""
import time
import threading
from typing import Dict, Optional


class BucketState:
    """Represents the internal state of a single Token Bucket instance."""
    def __init__(self, capacity: float, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.Lock()


class TokenBucketLimiter:
    """
    High-performance Token Bucket rate limiter.
    
    Refills are computed mathematically on-demand to avoid background task overhead.
    Supports global and per-tenant configurations.
    """
    def __init__(
        self,
        global_capacity: float,
        global_refill_rate: float,
        tenant_configs: Optional[Dict[str, tuple[float, float]]] = None
    ) -> None:
        self.global_capacity = global_capacity
        self.global_refill_rate = global_refill_rate
        self.tenant_configs = tenant_configs or {}
        
        self._states: Dict[str, BucketState] = {}
        self._map_lock = threading.Lock()

    def consume(self, tenant_id: str) -> bool:
        """
        Consumes a token for a tenant and the global bucket.
        
        Returns:
            True if admitted, False if rate-limited.
        """
        # 1. Check Tenant Limit
        tenant_state = self._get_or_create_state(tenant_id)
        if not self._consume_bucket(tenant_state):
            return False

        # 2. Check Global Limit
        global_state = self._get_or_create_state("global")
        if not self._consume_bucket(global_state):
            # Rollback tenant token if global limit fails to prevent resource leaks
            self._refund_bucket(tenant_state)
            return False

        return True

    def _get_or_create_state(self, key: str) -> BucketState:
        """Retrieves or registers a BucketState struct for a given key."""
        with self._map_lock:
            state = self._states.get(key)
            if state is None:
                if key == "global":
                    state = BucketState(self.global_capacity, self.global_refill_rate)
                else:
                    # Retrieve tenant config or fall back to global capacity limits
                    capacity, refill = self.tenant_configs.get(key, (self.global_capacity, self.global_refill_rate))
                    state = BucketState(capacity, refill)
                self._states[key] = state
            return state

    def _consume_bucket(self, state: BucketState) -> bool:
        """Atomic check and consumption of a token in a bucket state."""
        with state.lock:
            now = time.time()
            elapsed = now - state.last_refill
            
            # Refill tokens mathematically
            state.tokens = min(state.capacity, state.tokens + (elapsed * state.refill_rate))
            state.last_refill = now

            if state.tokens >= 1.0:
                state.tokens -= 1.0
                return True
            return False

    def _refund_bucket(self, state: BucketState) -> None:
        """Refunds a token back to the bucket during transaction rollbacks."""
        with state.lock:
            state.tokens = min(state.capacity, state.tokens + 1.0)


class LeakyBucketLimiter:
    """
    Leaky Bucket limiter used to smooth traffic bursts.
    
    Limits the departure interval of admitted tasks.
    """
    def __init__(self, capacity: int, leak_interval_sec: float) -> None:
        self.capacity = capacity
        self.leak_interval = leak_interval_sec
        self.last_leak_time = time.time()
        self.current_water = 0
        self.lock = threading.Lock()

    def consume(self) -> bool:
        """Consumes space in the leaky bucket queue."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_leak_time
            
            # Leak requests based on elapsed time intervals
            leaks = int(elapsed / self.leak_interval)
            if leaks > 0:
                self.current_water = max(0, self.current_water - leaks)
                self.last_leak_time += leaks * self.leak_interval

            if self.current_water < self.capacity:
                self.current_water += 1
                return True
            return False
