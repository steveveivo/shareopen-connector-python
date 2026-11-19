"""
WS transport (Python) — websockets client against /ws/agent.

Protocol: PROTOCOL.md §1/§3/§6.  Auth via `?token=`, JSON frames, 30s server
heartbeat.  Uses the `websockets` library (optional dep).
"""

import json
from typing import Any

PROTOCOL_ERRORS = 0  # keep simple; no strict version gate in v1 client


class WsTransport:
    def __init__(self, config: dict):
        self.config = config
        self._ws = None
        self._closed_by_user = False
        self._heartbeat_task = None
        self._handlers: dict[str, list] = {}

    # ---- event bus (shared shape with Node EventEmitter) ----

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
                if args:
                    h(*args)
                else:
                    h()
            except Exception as e:  # noqa: BLE001
                print(f"[ws-transport] handler error: {e}")

    # ---- lifecycle ----

    @property
    def url(self) -> str:
        from urllib.parse import quote
        base = self.config.get("host", "http://localhost:3021").replace("http", "ws", 1)
        token = quote(self.config.get("token", ""), safe="")
        return f"{base}/ws/agent?token={token}"

    async def connect(self) -> None:
        import websockets  # optional dep

        self._closed_by_user = False
        await self._open(websockets)

    async def _open(self, websockets) -> None:
        self._ws = await websockets.connect(self.url, ping_interval=None)
        self._heartbeat_task = _spawn_heartbeat(self)
        self._emit("open")
        async for raw in self._ws:
            self._on_message(raw)
        # Connection closed.
        self._emit("close", 1000)
        if not self._closed_by_user and self.config.get("auto_reconnect", True):
            import asyncio
            await asyncio.sleep(self.config.get("reconnect_delay_s", 5.0))
            await self._open(websockets)

    def _on_message(self, raw) -> None:
        try:
            frame = json.loads(raw if isinstance(raw, str) else raw.decode())
        except Exception:  # noqa: BLE001
            return
        if frame.get("type") == "ping":
            self._send_raw({"type": "pong"})
            return
        self._emit("message", frame)

    def _send_raw(self, obj: dict) -> None:
        if self._ws is None:
            return
        import asyncio

        asyncio.ensure_future(self._ws.send(json.dumps(obj)))

    def _spawn_heartbeat(self) -> None:
        import asyncio

        async def beat():
            while not self._closed_by_user:
                await asyncio.sleep(self.config.get("heartbeat_ms", 25000) / 1000)
                if self._ws and getattr(self._ws, "open", True):
                    self._send_raw({"type": "ping"})

        task = asyncio.ensure_future(beat())
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
        self._heartbeat_task = task

    async def disconnect(self) -> None:
        self._closed_by_user = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001
                pass
            self._ws = None

    # ---- outbound frame builders ----

    async def send_chat(self, frame: dict) -> Any:
        obj = {
            "type": "agent_message",
            "conversation_id": frame.get("conversation_id"),
            "content": frame.get("content"),
        }
        if frame.get("attachments"):
            obj["attachments"] = frame["attachments"]
        if frame.get("message_id"):
            obj["message_id"] = frame["message_id"]
        if frame.get("is_final"):
            obj["is_final"] = True
        self._send_raw(obj)

    async def send_stream_chunk(self, frame: dict) -> Any:
        obj = {
            "type": "stream_chunk",
            "conversation_id": frame.get("conversation_id"),
            # [align 2026-11-17] 双字段: 后端 host/原生侧车契约收 `chunk`, SDK 侧发 `content`。
            # 两字段同帧发出, 后端 handleStreamingChunk 取 msg.chunk ?? msg.content —— 新旧后端都通。
            "chunk": frame.get("chunk") if frame.get("chunk") is not None else frame.get("content"),
            "content": frame.get("content"),
            "message_id": frame.get("message_id"),
        }
        if frame.get("is_final"):
            obj["is_final"] = True
        self._send_raw(obj)

    # [align 2026-11-17] 工具执行进度帧: 后端 handleProgressUpdate 契约
    # {conversation_id, progress, status} (progress 必须 truthy 否则被丢弃)。
    async def send_progress(self, frame: dict) -> Any:
        obj = {
            "type": "progress_update",
            "conversation_id": frame.get("conversation_id"),
            "progress": frame.get("progress"),
            "status": frame.get("status") or "in_progress",
        }
        self._send_raw(obj)

    async def send_typing(self, frame: dict) -> Any:
        obj = {"type": "agent_typing", "conversation_id": frame.get("conversation_id")}
        if frame.get("on") is False:
            obj["on"] = False
        self._send_raw(obj)

    async def send_command_register(self, frame: dict) -> Any:
        self._send_raw({"type": "command_register", "command": frame.get("command")})


def _spawn_heartbeat(transport) -> None:
    """Stand-alone heartbeat spawner (kept at module level for clarity)."""
    import asyncio

    async def beat():
        while True:
            await asyncio.sleep(transport.config.get("heartbeat_ms", 25000) / 1000)
            if getattr(transport, "_closed_by_user", True):
                return
            ws = getattr(transport, "_ws", None)
            if ws and getattr(ws, "open", True):
                transport._send_raw({"type": "ping"})

    return asyncio.ensure_future(beat())
