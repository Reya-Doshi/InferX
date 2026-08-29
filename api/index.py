# api/index.py
import json
import os
from typing import Any, Dict
from dotenv import load_dotenv

load_dotenv()


def app(environ: Dict[str, Any], start_response: Any) -> Any:
    """WSGI handler for Vercel Serverless Function deployment."""
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET")

    # Routing rules
    if path == "/health" or path == "/healthz":
        response_body = {"status": "healthy", "service": "inferx-serverless"}
        status = "200 OK"
    elif path == "/v1/chat/completions" and method == "POST":
        # Read request body from WSGI stream
        try:
            request_body_size = int(environ.get("CONTENT_LENGTH", 0))
        except (ValueError, TypeError):
            request_body_size = 0

        request_body_bytes = (
            environ["wsgi.input"].read(request_body_size)
            if request_body_size > 0
            else b""
        )
        prompt = ""
        try:
            body_dict = (
                json.loads(request_body_bytes.decode("utf-8"))
                if request_body_bytes
                else {}
            )
            # Extract prompt from OpenAI-style messages or standard prompt field
            if (
                "messages" in body_dict
                and isinstance(body_dict["messages"], list)
                and len(body_dict["messages"]) > 0
            ):
                prompt = body_dict["messages"][-1].get("content", "")
            else:
                prompt = body_dict.get("prompt", "")
        except Exception:
            body_dict = {}

        api_key = os.getenv("GEMINI_API_KEY")
        content_text = ""
        provider = "mock"

        if api_key and prompt:
            try:
                from google import genai

                client = genai.Client(api_key=api_key)
                response = client.models.generate_content(
                    model="gemini-2.5-flash", contents=prompt
                )
                content_text = response.text or ""
                provider = "gemini"
            except Exception as e:
                content_text = f"Error calling Gemini API: {str(e)}"
        elif not api_key:
            content_text = (
                "GEMINI_API_KEY environment variable is missing on Vercel. "
                "Add GEMINI_API_KEY in your Vercel Project Settings -> Environment Variables to enable live AI responses."
            )
        else:
            content_text = (
                "No prompt found in request body. Please send a 'prompt' field or OpenAI 'messages' array."
            )

        response_body = {
            "id": "chatcmpl-vercel",
            "object": "chat.completion",
            "model": "gemini-2.5-flash" if provider == "gemini" else "inferx-mock",
            "provider": provider,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content_text,
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
