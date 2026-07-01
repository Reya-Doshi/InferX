# api/index.py
import json
from typing import Any, Dict


def app(environ: Dict[str, Any], start_response: Any) -> Any:
    """WSGI handler for Vercel Serverless Function deployment."""
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET")

    # Routing rules
    if path == "/health" or path == "/healthz":
        response_body = {"status": "healthy", "service": "inferx-serverless"}
        status = "200 OK"
    elif path == "/v1/chat/completions" and method == "POST":
        response_body = {
            "id": "chatcmpl-vercel",
            "object": "chat.completion",
            "model": "llama-primary:v1.0",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello! This is a serverless completion response from InferX running on Vercel.",
                    },
                    "finish_reason": "stop",
                }
            ],
        }
        status = "200 OK"
    else:
        response_body = {
            "message": "Welcome to InferX Serverless Gateway. Use POST /v1/chat/completions to send requests."
        }
        status = "200 OK"

    # Encode response body
    response_data = json.dumps(response_body).encode("utf-8")

    # Headers
    response_headers = [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(response_data))),
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Headers", "*"),
        ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
    ]

    # Handle HTTP OPTIONS for preflight requests
    if method == "OPTIONS":
        status = "200 OK"
        response_headers[1] = ("Content-Length", "0")
        start_response(status, response_headers)
        return [b""]

    start_response(status, response_headers)
    return [response_data]
