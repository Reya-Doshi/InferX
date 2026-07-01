# inferx/admission/manager.py
"""
InferX Admission Manager.

Coordinates rate limiters, load shedders, and circuit breakers in a non-blocking
gatekeeper pipeline, registering metrics and return verdicts.
"""
import time
from typing import Optional

from inferx.admission.interfaces import AdmissionVerdict, IAdmissionController
from inferx.admission.limiter import TokenBucketLimiter
from inferx.admission.metrics import AdmissionMetrics
from inferx.admission.shedder import LoadShedder, CircuitBreaker
from inferx.core.context import RuntimeContext
from inferx.scheduler.interfaces import IScheduler, ScheduledRequest


class AdmissionManager(IAdmissionController):
    """
    Primary gatekeeper protecting the runtime from overload states.
    
    Sequentially checks circuit breaker status, tenant rate limits,
    and priority-aware hardware load limits.
    """
    def __init__(
        self,
        context: RuntimeContext,
        limiter: TokenBucketLimiter,
        shedder: LoadShedder,
        circuit_breaker: CircuitBreaker,
        metrics: Optional[AdmissionMetrics] = None,
        scheduler: Optional[IScheduler] = None
    ) -> None:
        self.context = context
        self.limiter = limiter
        self.shedder = shedder
        self.circuit_breaker = circuit_breaker
        self.metrics = metrics or AdmissionMetrics()
        self.scheduler = scheduler

    async def admit(self, request: ScheduledRequest) -> AdmissionVerdict:
        """
        Evaluates the request.
        
        Decision steps:
            1. Check Circuit Breaker (Trips fail fast with 503).
            2. Check Token Bucket limits (Global/Tenant breaches fail with 429).
            3. Check System resource congestion (Shedding low priorities with 503).
        """
        start_ns = time.perf_counter_ns()
        admitted = False
        status_code = 200
        error_code = None
        retry_after = 0.0

        try:
            # 1. Circuit Breaker Check
            if not self.circuit_breaker.allow_request():
                status_code = 503
                error_code = "CIRCUIT_BREAKER_OPEN"
                retry_after = self.circuit_breaker.cooldown_sec
                return AdmissionVerdict(
                    admitted=False,
                    error_code=error_code,
                    status_code=status_code,
                    retry_after_sec=retry_after
                )

            # 2. Rate Limiting Check
            if not self.limiter.consume(request.tenant_id):
                status_code = 429
                error_code = "RATE_LIMITED"
                # Suggest a 1-second backoff for rate limits
                retry_after = 1.0
                return AdmissionVerdict(
                    admitted=False,
                    error_code=error_code,
                    status_code=status_code,
                    retry_after_sec=retry_after
                )

            # 3. Load Shedding Check
            if self.shedder.should_shed(request.priority, self.context):
                status_code = 503
                error_code = "LOAD_SHEDDING"
                
                # Estimate retry backoff based on queue occupancy
                qsize = self.scheduler.size() if self.scheduler else 0
                retry_after = max(0.1, qsize * 0.01)  # 10ms per enqueued request
                return AdmissionVerdict(
                    admitted=False,
                    error_code=error_code,
                    status_code=status_code,
                    retry_after_sec=retry_after
                )

            # 4. Admission Approved
            admitted = True
            return AdmissionVerdict(admitted=True, status_code=200)

        finally:
            elapsed_ns = time.perf_counter_ns() - start_ns
            self.metrics.record_decision(
                admitted=admitted,
                status_code=status_code,
                latency_ns=elapsed_ns,
                retry_after=retry_after
            )
