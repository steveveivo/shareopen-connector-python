# shareopen-connector (Python)

ShareOpen Connector SDK — Python 版。统一三 transport（WS / REST / ACP）的连接器抽象。

## 安装

```bash
pip install websockets   # 仅 WS transport 需要
```

REST / ACP 只用标准库（`urllib` / `http.client`），零第三方依赖。

## 用法（WS transport，对着本地 3021）

```python
from shareopen_connector import ShareOpenConnector

connector = ShareOpenConnector(
    transport="ws",
    host="http://localhost:3021",
    token="<agent_token>",
)
connector.on("message", lambda m: print("user:", m["text"], "conv:", m["conversation_id"]))

async def main():
    await connector.connect()
    conv = "conv_..."
    await connector.typing(conv)
    await connector.send(conv, "hello from python SDK")

import asyncio
asyncio.run(main())
```

## 结构

```
shareopen_connector/
├── __init__.py        # ShareOpenConnector + 三 transport 导出
├── core.py            # 统一接口 + 事件分发
└── transports/
    ├── ws.py          # websockets 客户端（/ws/agent?token=）
    ├── rest.py        # HMAC 签名 webhook（/api/connectors/:id/inbound）
    └── acp.py         # JSON-RPC over SSE（/acp）
```

协议规范见仓库根 `sdk/PROTOCOL.md`。
