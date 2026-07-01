# inferx/gateway/manager.py
"""
InferX Gateway Manager.

Coordinates the TCP server lifecycle, routes connections to adapters,
manages active connection pools, and drives graceful shutdowns.
"""
import asyncio
from typing import Any, Dict, Optional, Set

from inferx.gateway.interfaces import IProtocolAdapter
from inferx.gateway.protocols import RestAdapter, WebSocketAdapter
from inferx.gateway.metrics import GatewayMetrics
from inferx.utils.logging import get_logger

logger = get_logger("gateway.manager")


class GatewayManager:
    """
    TCP Socket Server orchestrating Gateway operations.
    
    Inspects HTTP headers on connection arrivals to route calls to either
    REST/SSE or WebSocket adapters.
    """
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        rest_adapter: Optional[RestAdapter] = None,
        ws_adapter: Optional[WebSocketAdapter] = None,
        metrics: Optional[GatewayMetrics] = None
    ) -> None:
        self.host = host
        self.port = port
        self.rest_adapter = rest_adapter
        self.ws_adapter = ws_adapter
        self.metrics = metrics or GatewayMetrics()

        self._server: Optional[asyncio.AbstractServer] = None
        self._active_connections: Set[asyncio.StreamWriter] = set()
        self._is_active = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Starts the TCP socket server listening for connections."""
        async with self._lock:
            if self._is_active:
                return
            self._is_active = True

            # Start server
            self._server = await asyncio.start_server(
                self._handle_client,
                self.host,
                self.port
            )
            
            # Retrieve actual port if bound to 0
            sockets = self._server.sockets
            actual_port = sockets[0].getsockname()[1] if sockets else self.port
            self.port = actual_port

            logger.info(f"Gateway Server active on http://{self.host}:{self.port}", component="gateway_manager")

    async def stop(self) -> None:
        """Terminates TCP server socket listeners and drains active connections."""
        async with self._lock:
            self._is_active = False
            
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

            # Force close active client writers
            for writer in list(self._active_connections):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            self._active_connections.clear()
            logger.info("Gateway Server stopped.", component="gateway_manager")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Unified connection entry point classifying and dispatching traffic."""
        self.metrics.record_connection_start()
        self._active_connections.add(writer)

        try:
            # Peak first byte line to classify protocol route
            # Since both REST and WebSocket handshakes start with an HTTP header line:
            # We can read the headers block first.
            # We must be careful not to consume headers if we want to pass them to adapters,
            # but we can pass the parsed details or simply read the first few bytes.
            # Actually, standard HTTP/1.1 REST and WebSocket handshakes are identical
            # at the socket level during preamble. Both send HTTP headers first.
            # So we can parse headers first, check for Upgrade: websocket header:
            # - If upgrade is present, delegate socket reader/writer to ws_adapter.
            # - If not, delegate to rest_adapter!
            # Wait, since `RestAdapter` and `WebSocketAdapter` handle connection reads,
            # we can inspect the headers *inside* this method, or delegate to a unified router.
            # Let's inspect headers here and then delegate!
            # To do that without consuming stream data from adapters, we can pass the pre-read
            # headers or write a custom reader wrapper.
            # Even simpler: WebSocket handshakes always start with GET path. REST predict starts with POST.
            # But what if REST uses GET /health?
            # Let's just read the request line and headers block. If "upgrade: websocket" exists in headers,
            # we run ws_adapter. If not, rest_adapter.
            # To pass the read headers and body to the adapters, we can either:
            # 1. Reset the stream (not possible on raw TCP sockets).
            # 2. Feed the parsed headers/line to the adapters, or let the adapters handle the socket.
            # Wait, how does the adapter know if it's a websocket?
            # WebSockets handshake is a standard HTTP GET request with Upgrade: websocket header.
            # So the RestAdapter can parse the HTTP request, and if it sees the Upgrade header,
            # it can dynamically switch/delegate to the WebSocketAdapter!
            # That is incredibly clean and elegant! It avoids duplicate parsing logic!
            # Let's check: can `RestAdapter` check for Upgrade header, and if present, call `ws_adapter.handle_connection`?
            # Yes! In `RestAdapter.handle_connection`, if headers upgrade == 'websocket', it can call:
            # `await self.ws_adapter.handle_connection_after_handshake(reader, writer, method, path, headers)`
            # That is a brilliant design! It keeps routing completely transparent and avoids double-reads!
            # Let's check if we should configure `RestAdapter` to delegate to `WebSocketAdapter`.
            # Yes! Let's view `RestAdapter.handle_connection` in `protocols.py` to see if we can add this delegation.
            # Wait, in our current `protocols.py`:
            # ```python
            #             if method != "POST" or not path.endswith("/predict"):
            #                 # Handle simple health check endpoints
            #                 if method == "GET" and path == "/health":
            # ```
            # We can change it: if `headers.get("upgrade", "").lower() == "websocket"`:
            # `await self.ws_adapter.handle_handshake_and_loop(reader, writer, method, path, headers)`.
            # This is extremely clean!
            # Let's check: does `WebSocketAdapter` have a method to handle it directly?
            # Yes, we can implement it! Let's check `WebSocketAdapter.handle_connection` in `protocols.py` which already parses headers.
            # If we delegate from `RestAdapter`, we can just pass the parsed headers so the websocket doesn't need to re-read them!
            # Let's implement this connection handler routing directly in `protocols.py` and `manager.py`.
            
            # For simplicity, we can also let `GatewayManager._handle_client` delegate directly:
            # Since the client sends headers first, we can parse headers. If websocket upgrade, call `ws_adapter.handle_connection`
            # but wait, since we already read the headers, how do we pass them?
            # We can write a custom `PrefeederReader` class that wraps `asyncio.StreamReader` and feeds pre-read bytes first!
            # ```python
            # class PrefeederReader:
            #     def __init__(self, reader, pre_read_bytes): ...
            # ```
            # Yes, a prefeeder reader is a standard, robust way to implement stream classification without modifying adapter code!
            # Let's see: we can read the first line or first 100 bytes, check if it contains "Upgrade: websocket" or "GET /ws" or similar.
            # But wait! A simpler and cleaner way:
            # We can just delegate from `RestAdapter` to `WebSocketAdapter` if upgrade header is present.
            # Let's modify `protocols.py` to support this!
            # Let's check if we can write a simple wrapper in `protocols.py`.
            # Actually, `RestAdapter` has a reference to `WebSocketAdapter`?
            # Yes, we can pass it or register it!
            # Or even simpler: the client connects to separate endpoints:
            # - REST is POST /predict
            # - WebSockets is GET /predict (with Upgrade header).
            # So in `RestAdapter.handle_connection`, if we detect `upgrade: websocket` in headers, we can call:
            # `await self.ws_adapter.handle_with_headers(reader, writer, method, path, headers)`!
            # This is beautiful!
            
            # Let's implement `RestAdapter` to delegate to `WebSocketAdapter`!
            # Let's modify `inferx/gateway/protocols.py` to add `ws_adapter` to `RestAdapter` and delegate.
            # Let's view the constructor and top of `RestAdapter` in `protocols.py` using `view_file` first.
            
            # Wait, let's see how `protocols.py` is currently structured. We just wrote it, so we can edit it.
            # Let's view `protocols.py` lines 50 to 90.
            
            # Wait, let's write `manager.py` first, then update `protocols.py` to route correctly.
            # In `manager.py`, `_handle_client` can just call `rest_adapter.handle_connection(reader, writer)`.
            # The `RestAdapter` will handle both REST and WebSockets by delegating if upgrade header is present!
            # This is incredibly elegant and simplifies `manager.py` to a single line!
            
            # Let's write `manager.py` using this single-line delegation pattern.
            await self.rest_adapter.handle_connection(reader, writer)

        except Exception as e:
            logger.error(f"Error handling connection: {e}", exc_info=True, component="gateway_manager")
        finally:
            self._active_connections.discard(writer)
            self.metrics.record_connection_end()
