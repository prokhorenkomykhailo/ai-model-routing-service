#!/usr/bin/env python3
"""
Local mock server for provider plug-in smoke tests.

Exposes:
  - OpenAI-compatible: POST /v1/chat/completions
  - HuggingFace-like: POST /models/<model>

It returns deterministic JSON payloads that satisfy our Step 1/3 parsers so we can
validate provider routing and Kafka plumbing without real external API keys.
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Tuple


def _read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    if length > 0:
        raw = handler.rfile.read(length)
    else:
        # Some clients (e.g., OkHttp) may use chunked transfer encoding.
        transfer_encoding = (handler.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in transfer_encoding:
            chunks: list[bytes] = []
            while True:
                line = handler.rfile.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    size = int(line.split(b";", 1)[0], 16)
                except Exception:
                    break
                if size == 0:
                    # trailing CRLF + optional trailers
                    handler.rfile.readline()
                    break
                chunks.append(handler.rfile.read(size))
                # CRLF after each chunk
                handler.rfile.read(2)
            raw = b"".join(chunks) if chunks else b"{}"
        else:
            raw = b"{}"
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return {}


def _detect_prompt(payload: Dict[str, Any], path: str) -> str:
    if path.startswith("/v1/chat/completions"):
        msgs = payload.get("messages") or []
        if isinstance(msgs, list) and msgs:
            last = msgs[-1]
            if isinstance(last, dict):
                return str(last.get("content") or "")
        return ""
    return str(payload.get("inputs") or "")


def _is_step1_clustering(prompt: str) -> bool:
    p = (prompt or "").lower()
    # Be conservative: Step 3 prompts can mention "clusters" and "message_ids" (as input context),
    # but Step 1 uniquely asks for draft titles and a "clusters" array output.
    return (
        "group messages into topic clusters" in p
        and "draft_title" in p
        and "\"clusters\"" in p
    )


def _step1_response() -> str:
    return json.dumps(
        {
            "clusters": [
                {
                    "cluster_id": "cluster_001",
                    "message_ids": [1, 2, 3, 4, 5],
                    "draft_title": "Mock Campaign Planning",
                    "participants": ["Devon", "Sam"],
                    "channel": "#campaign-briefs",
                    "thread_id": "thread_001",
                },
                {
                    "cluster_id": "cluster_002",
                    "message_ids": [6, 7, 8],
                    "draft_title": "Mock Client Follow-up",
                    "participants": ["Leah"],
                    "channel": "#client-communications",
                    "thread_id": "thread_004",
                },
                {
                    "cluster_id": "cluster_003",
                    "message_ids": [9, 10],
                    "draft_title": "Mock Legal Review",
                    "participants": ["Jordan"],
                    "channel": "#project-updates",
                    "thread_id": "thread_007",
                },
            ]
        },
        indent=2,
    )


def _step3_response() -> str:
    return json.dumps(
        {
            "title": "Mock Topic Metadata Title",
            "external_party": "Mock External Party",
            "priority": "high",
            "suggested_action": "Review and confirm next steps with the team.",
            "situation": "A request was raised and needs confirmation.",
            "impact": "Without confirmation, the work may be delayed.",
            "proposed_solution": "Confirm ownership and timeline, then proceed.",
            "decision_needed": "Confirm whether we approve and who owns the next step.",
            "participants": ["Devon (U001)", "Sam (U002)"],
            "action_items": [
                {
                    "task": "Confirm next steps",
                    "owner": "Devon (U001)",
                    "status": "open",
                    "due_date": None,
                }
            ],
            "tags": ["mock", "demo"],
        },
        indent=2,
    )


def _build_response(prompt: str) -> str:
    return _step1_response() if _is_step1_clustering(prompt) else _step3_response()


def _ok_json(handler: BaseHTTPRequestHandler, payload: Any) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        payload = _read_json(self)
        prompt = _detect_prompt(payload, self.path)
        content = _build_response(prompt)

        if self.path.startswith("/v1/chat/completions"):
            model = payload.get("model") or "mock-openai"
            resp = {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 200, "total_tokens": 300},
            }
            return _ok_json(self, resp)

        if self.path.startswith("/models/"):
            # HF hosted inference style response
            return _ok_json(self, [{"generated_text": content}])

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Tuple[Any, ...]) -> None:  # noqa: A002
        # Keep logs quiet; tests read only the generated outputs.
        return


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9001)
    args = ap.parse_args()

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"mock_ai_server listening on :{args.port}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
