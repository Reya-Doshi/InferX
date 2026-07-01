# inferx/main.py
"""
InferX Main Server Executable.

Entry point of the inference runtime process. Parses options, builds
dependencies, and initiates the core event loop.
"""

import argparse
import asyncio
import os
import sys

from inferx.core.bootstrap import bootstrap_core
from inferx.core.context import RuntimeContext, RuntimeState
from inferx.interfaces.core import IRuntimeLifecycle, IRuntimeSupervisor
from inferx.utils.logging import get_logger

# Configure a fallback console logger for errors during bootstrap
import logging

logging.basicConfig(level=logging.INFO)
bootstrap_logger = logging.getLogger("inferx.bootstrap")


async def main_async(config_path: str) -> None:
    """Asynchronous entry point that initiates startup and enters lifecycle loops."""
    # 1. Bootstrap DI Container & Logging
    container = await bootstrap_core(config_path)

    # Retrieve resolved logger
    logger = get_logger("main")
    logger.info("Initializing runtime components...", component="main")

    context = container.resolve(RuntimeContext)
    supervisor = container.resolve(IRuntimeSupervisor)
    lifecycle = container.resolve(IRuntimeLifecycle)

    from inferx.gateway.manager import GatewayManager

    gateway_manager = container.resolve(GatewayManager)

    try:
        # 2. Start Supervisor (launches worker subprocesses)
        await supervisor.start()

        # Start Gateway Manager
        await gateway_manager.start()

        # 3. Enter main Lifecycle loop (waits for signals)
        await lifecycle.run()

    except asyncio.CancelledError:
        logger.info("Main loop task cancelled.", component="main")
    except Exception as e:
        logger.fatal(
            f"Uncaught exception in main loop: {e}", exc_info=True, component="main"
        )
        # Direct context state transition to FAILED
        try:
            await context.transition_to(RuntimeState.FAILED)
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            await gateway_manager.stop()
        except Exception:
            pass


def main() -> None:
    """Synchronous process wrapper for parsing command-line parameters."""
    parser = argparse.ArgumentParser(
        description="InferX high-throughput AI Inference Runtime."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/default_config.yaml",
        help="Path to the system YAML configuration file.",
    )
    args = parser.parse_args()

    # Verify config file exists
    if not os.path.exists(args.config):
        sys.stderr.write(f"FATAL: Configuration file not found at: {args.config}\n")
        sys.exit(1)

    try:
        # Execute the main event loop
        asyncio.run(main_async(args.config))
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        sys.stderr.write(f"FATAL: Application failed during bootstrap: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
