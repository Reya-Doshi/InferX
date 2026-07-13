# deploy/render/start_gateway.py
import asyncio
import os
import logging
from inferx.gateway.manager import GatewayManager
from inferx.gateway.protocols import RestAdapter, WebSocketAdapter
from inferx.gateway.metrics import GatewayMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inferx.render.gateway")


async def run_server() -> None:
    # Render binds the application port to the PORT environment variable
    port = int(os.environ.get("PORT", 10000))
    host = "0.0.0.0"

    logger.info(f"Bootstrapping InferX Gateway on Render: {host}:{port}...")

    # Initialize gateway protocols and adapter layers
    from inferx.admission.limiter import TokenBucketLimiter
    from inferx.admission.manager import AdmissionManager
    from inferx.admission.shedder import (
        BackpressureController,
        LoadShedder,
        CircuitBreaker,
    )
    from inferx.core.context import RuntimeContext
    from inferx.gateway.router import GatewayRouter
    from inferx.gateway.middleware import MiddlewarePipeline

    async def mock_predict(model_name: str, version: str, prompt: str) -> str:
        from dotenv import load_dotenv

        load_dotenv()
        api_key = os.getenv("GEMINI_API_KEY")
        if api_key:
            try:
                from google import genai

                client = genai.Client(api_key=api_key)
                response = await client.aio.models.generate_content(
                    model="gemini-2.5-flash", contents=prompt
                )
                return response.text or ""
            except Exception as e:
                logger.error(f"Error calling Gemini API: {e}", exc_info=True)
                return f"Error calling Gemini API: {e}"
        return f"processed_{model_name}_{version}_{prompt}"

    context = RuntimeContext()
    limiter = TokenBucketLimiter(100.0, 100.0)
    backpressure = BackpressureController()
    shedder = LoadShedder(backpressure)
    circuit_breaker = CircuitBreaker()
    admission = AdmissionManager(context, limiter, shedder, circuit_breaker)

    pipeline = MiddlewarePipeline(
        admission_manager=admission, allowed_api_keys=["sk-valid-key"]
    )
    router = GatewayRouter()

    metrics = GatewayMetrics()
    ws_adapter = WebSocketAdapter(pipeline, router, mock_predict)
    rest_adapter = RestAdapter(pipeline, router, mock_predict, ws_adapter)

    # Initialize gateway manager
    manager = GatewayManager(
        host=host,
        port=port,
        rest_adapter=rest_adapter,
        ws_adapter=ws_adapter,
        metrics=metrics,
    )

    # Start listening to socket connections
    await manager.start()
    logger.info("Gateway server successfully bound and listening.")

    # Keep server running until shutdown signal is received
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Stopping server...")
        await manager.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logger.info("Server terminated by user.")
