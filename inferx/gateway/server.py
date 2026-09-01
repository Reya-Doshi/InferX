# inferx/gateway/server.py
"""
InferX Gateway Server CLI Entry Point.
"""

import argparse
import asyncio
import os
import sys

from deploy.render.start_gateway import run_server


def main() -> None:
    """CLI Entrypoint for InferX server."""
    parser = argparse.ArgumentParser(description="InferX AI Inference Gateway")
    parser.add_argument("command", nargs="?", default="serve", choices=["serve", "start"], help="Command to run")
    parser.add_argument("--port", type=int, default=10000, help="Port to bind gateway server (default: 10000)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host address to bind gateway server")
    parser.add_argument("--config", type=str, default="config/default.yaml", help="Path to config file")

    args = parser.parse_args()

    os.environ["PORT"] = str(args.port)
    os.environ["HOST"] = str(args.host)

    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("InferX server stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error starting InferX server: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
