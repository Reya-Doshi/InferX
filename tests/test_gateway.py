# tests/test_gateway.py
"""
InferX Gateway Test Suite.

Verifies HTTP/1.1 endpoints, auth validation middleware, size limits,
canary routing distribution, SSE token streaming, WebSocket handshake/frames,
and Admission Controller rate-limit blocks.
"""

import asyncio
import json
import unittest

from inferx.admission.limiter import TokenBucketLimiter
from inferx.admission.manager import AdmissionManager
from inferx.admission.shedder import BackpressureController, LoadShedder, CircuitBreaker
from inferx.core.context import RuntimeContext
from inferx.gateway.router import GatewayRouter
from inferx.gateway.middleware import MiddlewarePipeline
from inferx.gateway.protocols import RestAdapter, WebSocketAdapter
from inferx.gateway.manager import GatewayManager


# Mock prediction function to simulate engine processing
async def mock_predict(model_name: str, version: str, prompt: str) -> str:
    return f"processed_{model_name}_{version}_{prompt}"


class TestGateway(unittest.IsolatedAsyncioTestCase):
    """Integration test suite for the InferX Gateway Server."""

    async def asyncSetUp(self) -> None:
        self.context = RuntimeContext()
        self.context.update_telemetry(vram_util=0.5, cpu_util=0.4, avg_queue_lat=0.0)

        # High limit to bypass rate limits
        self.limiter = TokenBucketLimiter(100.0, 100.0)
        self.backpressure = BackpressureController()
        self.shedder = LoadShedder(self.backpressure)
        self.circuit_breaker = CircuitBreaker()
        self.admission = AdmissionManager(
            self.context, self.limiter, self.shedder, self.circuit_breaker
        )

        # Setup middleware and router (canary routing: 50% v1.0, 50% v2.0)
        self.pipeline = MiddlewarePipeline(
            admission_manager=self.admission,
            allowed_api_keys=["sk-valid-key"],
            max_request_size_bytes=1000,
        )
        self.router = GatewayRouter(
            canary_weights={"llama": {"v1.0": 0.5, "v2.0": 0.5}}
        )

        # Setup adapters and manager on random port
        self.ws_adapter = WebSocketAdapter(self.pipeline, self.router, mock_predict)
        self.rest_adapter = RestAdapter(
            self.pipeline, self.router, mock_predict, self.ws_adapter
        )

        self.manager = GatewayManager(
            host="127.0.0.1",
            port=0,  # OS binds to random free port
            rest_adapter=self.rest_adapter,
            ws_adapter=self.ws_adapter,
        )
        await self.manager.start()

    async def asyncTearDown(self) -> None:
        await self.manager.stop()

    async def test_rest_predict_success(self) -> None:
        # Simulate simple HTTP/1.1 POST /predict request
        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        payload_dict = {"prompt": "hello_world", "model": "llama"}
        body = json.dumps(payload_dict)

        headers = (
            "POST /predict HTTP/1.1\r\n"
            f"Host: {self.manager.host}:{self.manager.port}\r\n"
            "Content-Type: application/json\r\n"
            "Authorization: Bearer sk-valid-key\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        )

        writer.write(headers.encode("utf-8") + body.encode("utf-8"))
        await writer.drain()

        # Read response bytes
        resp_bytes = await reader.read(4096)
        writer.close()
        await writer.wait_closed()

        resp_str = resp_bytes.decode("utf-8")

        # Verify status code
        self.assertIn("HTTP/1.1 200 OK", resp_str)
        self.assertIn("application/json", resp_str)

        # Extract JSON body
        body_json = resp_str.split("\r\n\r\n")[-1]
        data = json.loads(body_json)

        self.assertEqual(data["model"], "llama")
        # Canary router resolves model to either v1.0 or v2.0
        self.assertIn(data["version"], ["v1.0", "v2.0"])
        self.assertIn("processed_llama_", data["response"])

    async def test_rest_auth_failure(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        headers = (
            "POST /predict HTTP/1.1\r\n"
            "Authorization: Bearer sk-invalid-key\r\n"
            "Content-Length: 0\r\n\r\n"
        )
        writer.write(headers.encode("utf-8"))
        await writer.drain()

        resp_bytes = await reader.read(4096)
        writer.close()
        await writer.wait_closed()

        resp_str = resp_bytes.decode("utf-8")

        self.assertIn("HTTP/1.1 401 Unauthorized", resp_str)
        self.assertIn("Unauthorized", resp_str)

    async def test_payload_size_limit(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        # Payload size exceeds 1000 bytes limit
        body = "x" * 2000
        headers = (
            "POST /predict HTTP/1.1\r\n"
            "Authorization: Bearer sk-valid-key\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        )
        writer.write(headers.encode("utf-8") + body.encode("utf-8"))
        await writer.drain()

        resp_bytes = await reader.read(4096)
        writer.close()
        await writer.wait_closed()

        resp_str = resp_bytes.decode("utf-8")
        self.assertIn("HTTP/1.1 413 Payload Too Large", resp_str)

    async def test_sse_streaming_response(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        payload_dict = {"prompt": "hello streams", "model": "llama", "stream": True}
        body = json.dumps(payload_dict)

        headers = (
            "POST /predict HTTP/1.1\r\n"
            "Authorization: Bearer sk-valid-key\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        )

        writer.write(headers.encode("utf-8") + body.encode("utf-8"))
        await writer.drain()

        # Read streaming chunks until EOF
        resp_bytes = bytearray()
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break
            resp_bytes.extend(chunk)

        writer.close()
        await writer.wait_closed()

        resp_str = resp_bytes.decode("utf-8")

        # Verify streaming headers
        self.assertIn("text/event-stream", resp_str)
        self.assertIn("Transfer-Encoding: chunked", resp_str)
        # Verify SSE token formats
        self.assertIn("data: {", resp_str)
        self.assertIn("data: [DONE]", resp_str)

    async def test_websocket_connection_and_predict(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        sec_key = "dGhlIHNhbXBsZSBub25jZQ=="
        headers = (
            "GET /predict HTTP/1.1\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {sec_key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        writer.write(headers.encode("utf-8"))
        await writer.drain()

        # Read handshake response
        resp_bytes = await reader.read(4096)
        resp_str = resp_bytes.decode("utf-8")

        self.assertIn("HTTP/1.1 101 Switching Protocols", resp_str)
        self.assertIn("Upgrade: websocket", resp_str)

        # Send unmasked WebSocket Text Frame containing request JSON
        req_payload = {
            "api_key": "sk-valid-key",
            "prompt": "ws_test_prompt",
            "model": "llama",
        }
        payload_str = json.dumps(req_payload)
        payload_bytes = payload_str.encode("utf-8")

        # Construct WebSocket frame (FIN=1, Opcode=1, Mask=0)
        frame = bytearray()
        frame.append(0x81)
        frame.append(len(payload_bytes))
        frame.extend(payload_bytes)

        writer.write(bytes(frame))
        await writer.drain()

        # Read response frame from server
        resp_frame = await reader.readexactly(2)
        opcode = resp_frame[0] & 0x0F
        length = resp_frame[1] & 0x7F

        payload_resp = await reader.readexactly(length)
        writer.close()
        await writer.wait_closed()

        self.assertEqual(opcode, 0x01)  # Text Frame
        data = json.loads(payload_resp.decode("utf-8"))
        self.assertIn("processed_llama_", data["response"])

    async def test_admission_controller_block(self) -> None:
        # Trip the circuit breaker to force Admission Manager blocks (yields 503)
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()

        reader, writer = await asyncio.open_connection(
            self.manager.host, self.manager.port
        )

        payload_dict = {"prompt": "blocked", "model": "llama"}
        body = json.dumps(payload_dict)
        headers = (
            "POST /predict HTTP/1.1\r\n"
            "Authorization: Bearer sk-valid-key\r\n"
            f"Content-Length: {len(body)}\r\n\r\n"
        )
        writer.write(headers.encode("utf-8") + body.encode("utf-8"))
        await writer.drain()

        resp_bytes = await reader.read(4096)
        writer.close()
        await writer.wait_closed()

        resp_str = resp_bytes.decode("utf-8")

        self.assertIn("HTTP/1.1 503 Service Unavailable", resp_str)
        self.assertIn("CIRCUIT_BREAKER_OPEN", resp_str)


if __name__ == "__main__":
    unittest.main()
