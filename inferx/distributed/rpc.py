# inferx/distributed/rpc.py
"""
InferX Cluster RPC.

Implements lightweight TCP-based JSON-RPC server and client protocols,
simulating secure node-to-node authentication tokens (mTLS).
"""
import asyncio
import json
import uuid
from typing import Any, Callable, Coroutine, Dict, Optional

from inferx.distributed.interfaces import IRpcClient, IRpcServer
from inferx.utils.logging import get_logger

logger = get_logger("distributed.rpc")


class ClusterRpcServer(IRpcServer):
    """
    JSON-RPC server terminated over raw TCP streams.
    """
    def __init__(self, host: str, port: int, security_token: str = "cluster-secret-key") -> None:
        self.host = host
        self.port = port
        self.security_token = security_token
        self._server: Optional[asyncio.AbstractServer] = None
        # Maps method_name -> handler coroutine
        self._handlers: Dict[str, Callable[[Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any]]]] = {}
        self._is_active = False

    def register_handler(
        self,
        method: str,
        handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any]]]
    ) -> None:
        """Binds a method name to an async handler function."""
        self._handlers[method] = handler

    async def start(self) -> None:
        """Starts listening for cluster connection arrivals."""
        self._is_active = True
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        
        # Retrieve actual port if bound to 0
        sockets = self._server.sockets
        self.port = sockets[0].getsockname()[1] if sockets else self.port
        
        logger.info(f"Cluster RPC Server active on {self.host}:{self.port}", component="rpc_server")

    async def stop(self) -> None:
        """Stops the socket listener."""
        self._is_active = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info("Cluster RPC Server stopped.", component="rpc_server")

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while self._is_active:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break
                
                req_str = line_bytes.decode("utf-8").strip()
                if not req_str:
                    continue

                req = json.loads(req_str)
                req_id = req.get("id")
                method = req.get("method")
                params = req.get("params", {})
                
                # 1. mTLS Token verification check
                client_token = params.get("token", "")
                if client_token != self.security_token:
                    resp = {"jsonrpc": "2.0", "error": {"code": -32001, "message": "Unauthorized cluster token."}, "id": req_id}
                    writer.write((json.dumps(resp) + "\n").encode("utf-8"))
                    await writer.drain()
                    break

                # 2. Invoke registered handlers
                if method in self._handlers:
                    try:
                        result = await self._handlers[method](params)
                        resp = {"jsonrpc": "2.0", "result": result, "id": req_id}
                    except Exception as e:
                        resp = {"jsonrpc": "2.0", "error": {"code": -32603, "message": f"Internal RPC Error: {e}"}, "id": req_id}
                else:
                    resp = {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Method {method} not found."}, "id": req_id}

                writer.write((json.dumps(resp) + "\n").encode("utf-8"))
                await writer.drain()

        except Exception as e:
            logger.debug(f"RPC Connection dropped: {e}", component="rpc_server")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


class ClusterRpcClient(IRpcClient):
    """
    TCP client dispatching JSON-RPC requests across cluster nodes.
    """
    def __init__(self, security_token: str = "cluster-secret-key", timeout_sec: float = 3.0) -> None:
        self.security_token = security_token
        self.timeout = timeout_sec

    async def call(self, host: str, port: int, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invokes an RPC method on a target node.
        
        Auto-injects the security token validation header.
        """
        # Inject mTLS verification token
        params_copy = dict(params)
        params_copy["token"] = self.security_token
        
        req_id = str(uuid.uuid4())[:8]
        req = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params_copy,
            "id": req_id
        }

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=self.timeout
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to cluster node {host}:{port}: {e}")

        try:
            # Send serialized request line
            writer.write((json.dumps(req) + "\n").encode("utf-8"))
            await writer.drain()

            # Read response line
            line_bytes = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            if not line_bytes:
                raise ConnectionError("Empty RPC response preamble.")

            resp = json.loads(line_bytes.decode("utf-8").strip())
            
            if "error" in resp:
                raise RuntimeError(f"RPC Error: {resp['error']['message']}")
            
            return resp.get("result", {})

        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
