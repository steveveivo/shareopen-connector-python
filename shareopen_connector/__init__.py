"""
ShareOpen Connector SDK — Python.

Unified transport abstraction (WS / REST / ACP) for connecting third-party
agents to ShareOpen.  Mirrors the Node SDK (`sdk/node`).  Protocol spec:
`sdk/PROTOCOL.md`.
"""

from .core import ShareOpenConnector
from .transports.ws import WsTransport
from .transports.rest import RestTransport
from .transports.acp import AcpTransport

__all__ = [
    "ShareOpenConnector",
    "WsTransport",
    "RestTransport",
    "AcpTransport",
]
