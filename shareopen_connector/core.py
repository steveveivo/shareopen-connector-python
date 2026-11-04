"""
ShareOpen Connector — unified interface + event dispatch.

Language-agnostic contract (PROTOCOL.md §4):
    connect / disconnect / send / stream / typing / on / register_command

The connector wraps one transport (ws / rest / acp) and re-emits its frames as
unified events: message / reaction / component / command / open / close / error.
"""

import asyncio
from typing import Any, Awaitable, Callable, Optional, Union

from .transports.ws import WsTransport
from .transports.rest import RestTransport
from .transports.acp import AcpTransport

Handler = Callable[[dict], Union[None, Awaitable[None]]]
PROTO = "so-connector-1"


class ShareOpenConnector:
    """One agent's ShareOpen connection, regardless of transport."""

    def __init__(
        self,
        transport: str,
        host: str,
        token: str = "",
        connector_id: str = "",
        hmac_key: str = "",
        acp_endpoint: str = "",
        agent_id: str = "",
        heartbeat_ms: int = 25000,
        auto_reconnect: bool = True,
    ):
        self.config = {
            "transport": transport,
            "host": host,
            "token": token,
            "connector_id": connector_id,
            "hmac_key": hmac_key,
            "acp_endpoint": acp_endpoint,
            "agent_id": agent_id,
            "heartbeat_ms": heartbeat_ms,
            "auto_reconnect": auto_reconnect,
        }
        self._handlers: dict[str, list[Handler]] = {}

        if transport == "ws":
            self.transport = WsTransport(self.config)
        elif transport == "rest":
            self.transport = RestTransport(self.config)
        elif transport == "acp":
            self.transport = AcpTransport(self.config)
        else:
            raise ValueError(f"Unknown transport: {transport}")

        # Wire transport frames into unified events.
        self.transport.on("open", lambda *_: self._emit("open"))
        self.transport.on("close", self._on_close)
        self.transport.on("error", self._on_error)
        self.transport.on("message", self._dispatch)

    # ---- lifecycle ----

    async def connect(self) -> None:
        if hasattr(self.transport, "connect"):
            await self.transport.connect()
        # REST/ACP are stateless per-call; connect is a no-op for them.

    async def disconnect(self) -> None:
        if hasattr(self.transport, "disconnect"):
            await self.transport.disconnect()

    # ---- outbound ----

    async def send(self, conversation_id: str, text: str, **opts: Any) -> Any:
        return await self.transport.send_chat(
            {
                "conversation_id": conversation_id,
                "content": text,
                "attachments": opts.get("attachments"),
                "message_id": opts.get("message_id"),
                "receiver_id": opts.get("receiver_id"),
                "receiver_type": opts.get("receiver_type", "human"),
            }
        )

    async def stream(self, conversation_id: str, chunks, **opts: Any) -> None:
        """Consume an async (or sync) iterable of chunks; emit stream_chunk
        frames, then one final chat frame carrying the complete text."""
        message_id = opts.get("message_id") or f"msg_local_{int(__import__('time').time()*1000)}"
        full = ""
        async for chunk in self._aiter(chunks):
            full += chunk
            await self.transport.send_stream_chunk(
                {
                    "conversation_id": conversation_id,
                    "content": chunk,
                    "message_id": message_id,
                    "is_final": False,
                }
            )
        await self.transport.send_chat(
            {
                "conversation_id": conversation_id,
                "content": full,
                "message_id": message_id,
                "is_final": True,
                "receiver_id": opts.get("receiver_id"),
                "receiver_type": opts.get("receiver_type", "human"),
            }
        )

    async def typing(self, conversation_id: str, on: bool = True) -> Any:
        return await self.transport.send_typing(
            {"conversation_id": conversation_id, "on": on}
        )

    async def register_command(
        self, name: str, description: str, parameters: Optional[list] = None
    ) -> Any:
        return await self.transport.send_command_register(
            {"command": {"name": name, "description": description, "parameters": parameters or []}}
        )

    # ---- inbound events ----

    def on(self, event: str, handler: Handler) -> "ShareOpenConnector":
        self._handlers.setdefault(event, []).append(handler)
        return self

    def off(self, event: str, handler: Handler) -> "ShareOpenConnector":
        self._handlers.get(event, []).remove(handler)
        return self

    # ---- internals ----

    async def _emit(self, event: str, payload: Any = None) -> None:
        for h in self._handlers.get(event, []):
            try:
                res = h(payload or {})
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:  # noqa: BLE001
                print(f"[connector] handler error ({event}): {e}")

    def _on_close(self, *a) -> None:
        asyncio.ensure_future(self._emit("close", a[0] if a else None))

    def _on_error(self, err) -> None:
        asyncio.ensure_future(self._emit("error", err))

    def _dispatch(self, frame: dict) -> None:
        t = frame.get("type") or frame.get("frame")
        asyncio.ensure_future(self._dispatch_frame(t, frame))

    async def _dispatch_frame(self, t: str, frame: dict) -> None:
        if t in ("user_message", "text_message"):
            await self._emit("message", {
                "conversation_id": frame.get("conversation_id"),
                "user_id": frame.get("user_id"),
                "message_id": frame.get("message_id"),
                "text": frame.get("content") or frame.get("text") or "",
                "attachments": frame.get("attachments"),
                # [P0 2026-11-03] 群信封块 + L2 envelope (嵌套帧 data.group / data.envelope)
                "group": frame.get("group"),
                "envelope": frame.get("envelope"),
            })
        elif t == "message":
            # 后端 /ws/agent 在线推送的嵌套形态: {type:'message', data:{...}}
            # 把 data.group / data.envelope 也穿透进来 (与 flat 形态对齐).
            d = frame.get("data") or {}
            await self._emit("message", {
                "conversation_id": d.get("conversation_id") or frame.get("conversation_id"),
                "user_id": d.get("user_id") or frame.get("user_id"),
                "message_id": d.get("message_id") or frame.get("message_id"),
                "text": d.get("message") or d.get("content") or frame.get("content") or frame.get("text") or "",
                "media": d.get("media"),
                "group": d.get("group") or frame.get("group"),
                "envelope": d.get("envelope") or frame.get("envelope"),
            })
        elif t in ("reaction_added", "reaction_removed"):
            await self._emit("reaction", {
                "kind": t,
                "message_id": frame.get("message_id"),
                "conversation_id": frame.get("conversation_id"),
                "emoji": frame.get("emoji"),
                "reacter_id": frame.get("reacter_id"),
                "catchup": frame.get("catchup"),
            })
        elif t == "component_callback":
            await self._emit("component", {
                "invocation_id": frame.get("invocation_id"),
                "component": frame.get("component"),
                "payload": frame.get("payload"),
            })
        elif t == "command_invocation":
            await self._emit("command", {
                "invocation_id": frame.get("invocation_id"),
                "command": frame.get("command_name"),
                "args": frame.get("args"),
            })
        elif t == "stream_chunk":
            await self._emit("stream_chunk", frame)
        # 'pong' and unknown frames are ignored at this level.

    @staticmethod
    async def _aiter(chunks):
        """Accept an async iterator OR a plain iterable of strings."""
        try:
            it = chunks.__aiter__()
            while True:
                yield await it.__anext__()
        except AttributeError:
            for c in chunks:
                yield c
        except StopAsyncIteration:
            return
