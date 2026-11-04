#!/usr/bin/env python3
"""
P0 acceptance demo — Python SDK, WS transport, against local 3021.

Run:
    pip install websockets
    SO_URL=http://localhost:3021 SO_TOKEN=<agent_token> python examples/demo.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shareopen_connector import ShareOpenConnector  # noqa: E402


async def main() -> None:
    host = os.environ.get("SO_URL", "http://localhost:3021")
    token = os.environ.get("SO_TOKEN", "")
    if not token:
        print("Set SO_TOKEN to a ShareOpen agent token.", file=sys.stderr)
        sys.exit(1)

    connector = ShareOpenConnector(transport="ws", host=host, token=token)

    async def on_message(m: dict) -> None:
        conv = m.get("conversation_id")
        text = m.get("text")
        if not conv or not text:
            return
        print(f"[user] {text}")

        await connector.typing(conv)

        reply = f'You said: "{text}"\n\n(This is the ShareOpen Python SDK demo, P0.)'

        async def chunks():
            for i in range(0, len(reply), 20):
                yield reply[i : i + 20]
                await asyncio.sleep(0.06)

        await connector.stream(conv, chunks())
        print(f"[agent] streamed reply → {conv}")

    connector.on("open", lambda *_: print("[demo] connected to", host))
    connector.on("close", lambda code: print("[demo] closed", code))
    connector.on("error", lambda e: print("[demo] error:", e))
    connector.on("message", on_message)

    await connector.connect()
    print("[demo] waiting for messages… Ctrl-C to exit")
    try:
        await asyncio.Future()  # run until interrupted
    except KeyboardInterrupt:
        print("\n[demo] shutting down")
        await connector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
