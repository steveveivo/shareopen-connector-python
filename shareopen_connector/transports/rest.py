"""
REST transport (Python) — HMAC-signed webhook into /api/connectors/:id/inbound.

Cloud agents (Manus / Coze / generic): the agent platform pushes events back
into ShareOpen over this signed endpoint.  Pure stdlib (urllib + hmac).
"""

import hashlib
import hmac
import json
import secrets
import time
import urllib.request
from typing import Any


class RestTransport:
    def __init__(self, config: dict):
        self.config = config
        self._handlers: dict[str, list] = {}

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
                print(f"[rest-transport] handler error: {e}")

    # ---- signing ----

    def sign(self, raw_body: str) -> dict:
        ts = str(int(time.time() * 1000))
        nonce = secrets.token_hex(12)
        key = (self.config.get("hmac_key") or "").encode()
        msg = f"{ts}:{nonce}:{raw_body}".encode()
        sig = hmac.new(key, msg, hashlib.sha256).hexdigest()
        return {"signature": sig, "timestamp": ts, "nonce": nonce}

    # ---- inbound push ----

    async def push_inbound(self, frame: dict) -> dict:
        connector_id = self.config.get("connector_id")
        if not connector_id:
            raise ValueError("REST transport requires connector_id")

        body = json.dumps({**frame, "proto": "so-connector-1"}).encode()
        s = self.sign(body.decode())
        url = f"{self.config.get('host')}/api/connectors/{connector_id}/inbound"
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Signature": f"hmac-sha256={s['signature']}",
                "X-Timestamp": s["timestamp"],
                "X-Nonce": s["nonce"],
            },
        )
        import asyncio

        def do():
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode()
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"raw": raw}

        result = await asyncio.to_thread(do)
        return result

    # ---- outbound frame builders ----

    async def send_chat(self, frame: dict) -> Any:
        return await self.push_inbound(
            {
                "frame": "chat",
                "event_id": frame.get("event_id") or f"evt_{secrets.token_hex(8)}",
                "conversation_id": frame.get("conversation_id"),
                "receiver_id": frame.get("receiver_id"),
                "receiver_type": frame.get("receiver_type", "human"),
                "text": frame.get("content"),
                "attachment": (frame.get("attachments") or [None])[0]
                if frame.get("attachments")
                else None,
            }
        )

    async def send_stream_chunk(self, frame: dict) -> Any:
        if not frame.get("is_final"):
            return None  # no streaming channel over REST in v1
        return await self.push_inbound(
            {
                "frame": "stream_final",
                "event_id": frame.get("message_id"),
                "conversation_id": frame.get("conversation_id"),
                "text": frame.get("content"),
            }
        )

    async def send_typing(self, frame: dict) -> Any:
        return await self.push_inbound(
            {"frame": "typing", "event_id": f"evt_typing_{int(time.time()*1000)}"}
        )

    # [align 2026-11-17] REST 无 progress 通道 v1 — no-op (WS 侧车才用 progress 帧)。
    async def send_progress(self, frame: dict) -> Any:
        return None

    async def send_command_register(self, frame: dict) -> Any:
        return None  # slash menus are a WS/ACP feature

    # ---- lifecycle no-ops (stateless) ----

    async def connect(self) -> None:
        self._emit("open")

    async def disconnect(self) -> None:
        self._emit("close", 1000)
