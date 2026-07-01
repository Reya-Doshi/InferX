# inferx/core/lifecycle.py
"""
InferX Runtime Lifecycle & Signal Coordinator.

Implements sequential state machine transitions during server boot, execution, and graceful shutdowns.
"""
import asyncio
import signal
import sys
from typing import Any, Optional

from inferx.core.context import RuntimeContext, RuntimeState
from inferx.interfaces.core import IRuntimeLifecycle, IRuntimeSupervisor
from inferx.utils.logging import get_logger

logger = get_logger("lifecycle")


class RuntimeLifecycle(IRuntimeLifecycle):
    """
    Coordinates state transitions and teardown logic for runtime components.
    
    Verifies valid lifecycle changes and manages OS signal handlers.
    """
    def __init__(
        self,
        context: RuntimeContext,
        supervisor: IRuntimeSupervisor
    ) -> None:
        self.context = context
        self.supervisor = supervisor
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()

    async def run(self) -> None:
        """Transitions state to READY and RUNNING, waiting for shutdown signals."""
        try:
            # 1. Transition from INITIALIZING -> READY
            await self.context.transition_to(RuntimeState.READY)
            
            # 2. Transition from READY -> RUNNING
            await self.context.transition_to(RuntimeState.RUNNING)
            
            logger.info("InferX Runtime Core running. Press Ctrl+C to terminate.", component="lifecycle")
            
            # Register OS signal handlers
            self._register_signal_handlers()

            # Block until shutdown completes
            await self._shutdown_event.wait()
            logger.info("Lifecycle loop exiting.", component="lifecycle")
            
        except Exception as e:
            logger.fatal(f"Failed during lifecycle execution loop: {e}", exc_info=True, component="lifecycle")
            await self.context.transition_to(RuntimeState.FAILED)
            raise e

    async def shutdown(self, signal_num: int) -> None:
        """
        Coordinates the state transition sequence during graceful shutdowns.
        
        Transitions: RUNNING/DEGRADED -> DRAINING -> SHUTTING_DOWN -> STOPPED.
        """
        async with self._shutdown_lock:
            # Prevent double teardowns
            if self.context.state in [
                RuntimeState.DRAINING,
                RuntimeState.SHUTTING_DOWN,
                RuntimeState.STOPPED,
                RuntimeState.FAILED
            ]:
                return

            logger.fatal(
                f"Shutdown signal {signal_num} intercepted. Initiating graceful teardown...",
                component="lifecycle"
            )
            
            try:
                # 1. Transition to DRAINING
                await self.context.transition_to(RuntimeState.DRAINING)

                # Stop the background supervisor and worker processes
                try:
                    await self.supervisor.stop()
                except Exception as e:
                    logger.error(f"Error during supervisor shutdown: {e}", exc_info=True, component="lifecycle")

                # 2. Drain active connections
                drain_timeout = 5.0
                start_time = asyncio.get_event_loop().time()
                
                while self.context.active_requests > 0:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= drain_timeout:
                        logger.warning(
                            f"Drain timeout reached. Force closing {self.context.active_requests} remaining connections.",
                            component="lifecycle"
                        )
                        break
                    
                    logger.info(
                        f"Waiting for {self.context.active_requests} active requests to complete... "
                        f"({elapsed:.1f}s elapsed)",
                        component="lifecycle"
                    )
                    await asyncio.sleep(0.5)

                # 3. Transition to SHUTTING_DOWN
                await self.context.transition_to(RuntimeState.SHUTTING_DOWN)
                
                # Perform any additional resource cleanups here (e.g. unmapping shared memory)
                
                # 4. Transition to STOPPED
                await self.context.transition_to(RuntimeState.STOPPED)
                logger.info("Graceful shutdown sequence complete.", component="lifecycle")
                
            except Exception as e:
                logger.error(f"Shutdown sequence encountered failure: {e}", exc_info=True, component="lifecycle")
                try:
                    await self.context.transition_to(RuntimeState.FAILED)
                except Exception:
                    pass
            finally:
                self._shutdown_event.set()

    def is_running(self) -> bool:
        """Checks if the context state is RUNNING or DEGRADED."""
        return self.context.state in [RuntimeState.RUNNING, RuntimeState.DEGRADED]

    def _register_signal_handlers(self) -> None:
        """Registers OS signals to trigger the shutdown loop."""
        loop = asyncio.get_running_loop()
        
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(
                        sig,
                        lambda s=sig: asyncio.create_task(self.shutdown(s))
                    )
                except ValueError:
                    pass
        else:
            def handle_windows_signal(signum: int, frame: Any) -> None:
                logger.warning("Windows signal intercepted.", component="lifecycle")
                try:
                    loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(self.shutdown(signum))
                    )
                except Exception as e:
                    logger.error(f"Failed to post shutdown task: {e}", component="lifecycle")

            signal.signal(signal.SIGINT, handle_windows_signal)
            signal.signal(signal.SIGTERM, handle_windows_signal)
