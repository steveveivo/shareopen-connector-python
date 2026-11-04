"""
ACP transport (Python) — JSON-RPC over SSE (WorkBuddy-style).

Flow: POST /acp/connect → sessionToken; GET /acp/events → SSE stream;
POST /acp → JSON-RPC (newSession / prompt / cancelRun).  Pure stdlib for the
JSON-RPC POSTs; SSE reads via urllib streaming.
"""

import json
import time
import urllib.request
from typing import Any, Optional


class AcpTransport:
    def __init__(self, config: dict):
        self.config = config
        self.endpoint = config.get("acp_endpoint") or f"{config.get('host')}/acp"
        self.session_token = None
        self.connection_id = None
        self._rpc_id = 0
        self._handlers: dict[str, list] = {}
        self._sse_thread = None

    # ---- event bus ----

    def on(self, event: str, handler) -> None:
        self._handlers.setdefault(event, []).append(handler)

    def off(self, event: str, handler) -> None:
        try:
            self._handlers.get(event, []).remove(handler)
        except ValueError:
            pass

    def _emit(self, event: str, *args) -> None:
        for h in self._handlers.get(event, []):
            try:
                h(*args) if args else h()
            except Exception as e:  # noqa: BLE001
                print(f"[acp-transport] handler error: {e}")

    # ---- lifecycle ----

    async def connect(self) -> None:
        import asyncio

        def do_connect():
            req = urllib.request.Request(
                f"{self.endpoint}/connect",
                data=json.dumps(
                    {
                        "agentId": self.config.get("agent_id"),
                        "token": self.config.get("token"),
                    }
                ).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())

        data = await asyncio.to_thread(do_connect)
        self.session_token = data.get("sessionToken") or data.get("token")
        self.connection_id = data.get("connectionId")
        self._start_sse()
        self._emit("open")

    def _start_sse(self) -> None:
        import threading
        import urllib.parse

        def reader():
            url = f"{self.endpoint}/events?token={urllib.parse.quote(str(self.session_token or ''))}"
            try:
                with urllib.request.urlopen(url, timeout=None) as resp:
                    for line in iter(resp.readline, b""):
                        text = line.decode(errors="replace").rstrip("\n")
                        if text.startswith("data:"):
                            self._on_sse_data(text[5:].strip())
            except Exception as e:  # noqa: BLE001
                self._emit("error", e)

        self._sse_thread = threading.Thread(target=reader, daemon=True)
        self._sse_thread.start()

    def _on_sse_data(self, data: str) -> None:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return
        etype = payload.get("type") or "message"
        if etype in ("stream_chunk", "streamChunk"):
            self._emit(
                "message",
                {
                    "type": "stream_chunk",
                    "content": payload.get("delta") or payload.get("content"),
                    "message_id": payload.get("messageId"),
                    "is_final": False,
                },
            )
        elif etype in ("stream_final", "message"):
            self._emit(
                "message",
                {
                    "type": "agent_message",
                    "content": payload.get("content") or payload.get("text"),
                    "conversation_id": payload.get("conversationId"),
                    "message_id": payload.get("messageId"),
                    "is_final": True,
                },
            )
        else:
            self._emit("message", {"type": etype, **payload})

    # ---- JSON-RPC ----

    async def rpc(self, method: str, params: dict) -> Any:
        import asyncio

        self._rpc_id += 1
        body = json.dumps(
            {"jsonrpc": "2.0", "id": self._rpc_id, "method": method, "params": params}
        ).encode()

        def do():
            req = urllib.request.Request(
                self.endpoint,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.session_token}",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())

        return await asyncio.to_thread(do)

    # ---- outbound frame builders ----

    async def send_chat(self, frame: dict) -> Any:
        return await self.rpc(
            "prompt",
            {
                "conversationId": frame.get("conversation_id"),
                "content": frame.get("content"),
                "messageId": frame.get("message_id"),
            },
        )

    async def send_stream_chunk(self, frame: dict) -> Any:
        if not frame.get("is_final"):
            return None
        return await self.send_chat(frame)

    async def send_typing(self, frame: dict) -> Any:
        return None

    async def send_command_register(self, frame: dict) -> Any:
        return None

    async def new_session(self, params: Optional[dict] = None) -> Any:
        return await self.rpc("newSession", params or {})

    async def cancel_run(self, params: Optional[dict] = None) -> Any:
        return await self.rpc("cancelRun", params or {})

    async def disconnect(self) -> None:
        try:
            await self.rpc("disconnect", {})
        except Exception:  # noqa: BLE001
            pass
        self.session_token = None
        self._emit("close", 1000)
