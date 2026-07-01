# tests/test_core.py
"""
InferX Runtime Core Test Suite.

Contains async unit and integration tests verifying Pydantic configs, DI providers,
state machine transitions, ContextVars telemetry logging, and health reports.
"""
import asyncio
import io
import logging
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock
from typing import Any

from inferx.core.bootstrap import bootstrap_core, DIContainer, SingletonProvider, FactoryProvider
from inferx.core.config import RuntimeConfig, AsyncYAMLConfigLoader
from inferx.core.context import RuntimeContext, RuntimeState
from inferx.core.health import HealthManager, HealthReport
from inferx.core.lifecycle import RuntimeLifecycle
from inferx.core.supervisor import RuntimeSupervisor
from inferx.errors.taxonomy import (
    ConfigurationError,
    DependencyInjectionError,
    StateTransitionError
)
from inferx.interfaces.core import (
    IConfigLoader,
    IHealthManager,
    IRuntimeLifecycle,
    IRuntimeSupervisor
)
from inferx.utils.logging import JSONFormatter, telemetry_context, get_logger


class TestConfigValidation(unittest.IsolatedAsyncioTestCase):
    """Verifies async Pydantic configuration validations."""

    def setUp(self) -> None:
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml", mode="w")
        
    def tearDown(self) -> None:
        os.unlink(self.temp_file.name)

    async def test_default_config_parsing(self) -> None:
        self.temp_file.write("""
log_level: "DEBUG"
gateway:
  port: 9000
""")
        self.temp_file.close()

        loader = AsyncYAMLConfigLoader(self.temp_file.name)
        config = await loader.load()

        self.assertEqual(config.log_level, "DEBUG")
        self.assertEqual(config.gateway.port, 9000)
        self.assertEqual(config.gateway.host, "0.0.0.0")

    async def test_invalid_port_validation(self) -> None:
        self.temp_file.write("""
gateway:
  port: 80
""")
        self.temp_file.close()

        loader = AsyncYAMLConfigLoader(self.temp_file.name)
        with self.assertRaises(ConfigurationError):
            await loader.load()

    async def test_invalid_vram_watermark_validation(self) -> None:
        self.temp_file.write("""
admission:
  vram_high_watermark: 1.5
""")
        self.temp_file.close()

        loader = AsyncYAMLConfigLoader(self.temp_file.name)
        with self.assertRaises(ConfigurationError):
            await loader.load()


class TestDIContainer(unittest.TestCase):
    """Verifies registration constraints and provider-based resolution of DI registries."""

    def setUp(self) -> None:
        self.container = DIContainer()

    def test_singleton_provider(self) -> None:
        class TestInterface:
            pass

        class TestImplementation(TestInterface):
            pass

        # Singleton provider should return the same instance
        self.container.register(TestInterface, SingletonProvider(TestImplementation))
        
        inst1 = self.container.resolve(TestInterface)
        inst2 = self.container.resolve(TestInterface)
        self.assertIs(inst1, inst2)

    def test_factory_provider(self) -> None:
        class TestInterface:
            pass

        class TestImplementation(TestInterface):
            pass

        # Factory provider should return a fresh instance every time
        self.container.register(TestInterface, FactoryProvider(TestImplementation))
        
        inst1 = self.container.resolve(TestInterface)
        inst2 = self.container.resolve(TestInterface)
        self.assertIsNot(inst1, inst2)


class TestStateTransitions(unittest.IsolatedAsyncioTestCase):
    """Verifies runtime state transitions and listener notifications."""

    async def test_valid_transitions(self) -> None:
        ctx = RuntimeContext()
        self.assertEqual(ctx.state, RuntimeState.INITIALIZING)

        await ctx.transition_to(RuntimeState.READY)
        self.assertEqual(ctx.state, RuntimeState.READY)

        await ctx.transition_to(RuntimeState.RUNNING)
        self.assertEqual(ctx.state, RuntimeState.RUNNING)

    async def test_invalid_transition_raises(self) -> None:
        ctx = RuntimeContext()
        with self.assertRaises(StateTransitionError):
            # INITIALIZING -> STOPPED is illegal
            await ctx.transition_to(RuntimeState.STOPPED)

    async def test_listeners_notified(self) -> None:
        ctx = RuntimeContext()
        notified = False

        async def mock_listener(old: RuntimeState, new: RuntimeState) -> None:
            nonlocal notified
            if old == RuntimeState.INITIALIZING and new == RuntimeState.READY:
                notified = True

        ctx.register_state_listener(mock_listener)
        await ctx.transition_to(RuntimeState.READY)
        self.assertTrue(notified)


class TestTelemetryLogging(unittest.TestCase):
    """Verifies that ContextVars propagate key parameters directly into JSON log structures."""

    def test_contextvars_harvesting(self) -> None:
        # Setup log capture stream
        log_capture = io.StringIO()
        formatter = JSONFormatter()
        
        logger = logging.getLogger("inferx.test_telemetry")
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Set task context variables
        token = telemetry_context.set({
            "request_id": "test-req-999",
            "model_name": "llama-3-8b",
            "tenant_id": "enterprise-omega"
        })

        try:
            logger.info("Verifying telemetry keys in structured log output.")
        finally:
            telemetry_context.reset(token)

        # Parse output JSON log line
        log_line = log_capture.getvalue().strip()
        log_dict = import_json(log_line)

        self.assertEqual(log_dict["request_id"], "test-req-999")
        self.assertEqual(log_dict["model_name"], "llama-3-8b")
        self.assertEqual(log_dict["tenant_id"], "enterprise-omega")


class TestHealthManager(unittest.IsolatedAsyncioTestCase):
    """Verifies comprehensive HealthReport layout output."""

    async def test_complete_health_report(self) -> None:
        manager = HealthManager(vram_watermark=0.90)
        
        async def check_ok() -> bool:
            return True

        manager.register_provider("mock_worker", check_ok, domain="workers")
        report = await manager.evaluate_health()

        self.assertIsInstance(report, HealthReport)
        self.assertEqual(report.status, "READY")
        self.assertEqual(report.workers["mock_worker"], "HEALTHY")
        
        # Verify placeholders exist
        self.assertIn("scheduler_loop", report.scheduler)
        self.assertIn("batcher_aggregator", report.batcher)


class TestRuntimeLifecycle(unittest.IsolatedAsyncioTestCase):
    """Verifies async lifecycle transitions during graceful shutdowns."""

    async def test_graceful_shutdown_transitions(self) -> None:
        context = RuntimeContext()
        supervisor = AsyncMock(spec=IRuntimeSupervisor)
        lifecycle = RuntimeLifecycle(context, supervisor)

        # Transition context to RUNNING state manually for testing
        await context.transition_to(RuntimeState.READY)
        await context.transition_to(RuntimeState.RUNNING)
        
        # Trigger graceful shutdown
        await lifecycle.shutdown(signal_num=15)
        
        self.assertEqual(context.state, RuntimeState.STOPPED)
        supervisor.stop.assert_awaited_once()


def import_json(json_str: str) -> dict[str, Any]:
    """Helper to parse captured log structures."""
    import json
    return json.loads(json_str)


if __name__ == "__main__":
    unittest.main()
