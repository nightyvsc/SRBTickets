"""
Message envelope definition for all ZeroMQ communication in SRB.

Every message between components uses this JSON envelope format:
{
    "msg_type": "<MsgType>",
    "request_id": "<uuid4>",
    "client_id": "<string>",
    "timestamp": "<ISO8601>",
    "payload": { ... }
}
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class MsgType(str, Enum):
    """All supported message types in the SRB system."""

    def __str__(self) -> str:
        return self.value

    # Client → GT (sync)
    QUERY = "QUERY"
    RESERVE = "RESERVE"
    MODIFY = "MODIFY"

    # Client → GT (async)
    CANCEL = "CANCEL"

    # GT → Cancel service
    CANCEL_REQUEST = "CANCEL_REQUEST"

    # Cancel service → Client (via PUB)
    CANCEL_RESULT = "CANCEL_RESULT"

    # Responses
    RESPONSE = "RESPONSE"
    ERROR = "ERROR"

    # RMC ↔ GT heartbeat / LRU
    READY = "READY"
    HEARTBEAT = "HEARTBEAT"

    # GT internal → persistence (via RMC or cancel)
    DB_QUERY = "DB_QUERY"
    DB_EXECUTE = "DB_EXECUTE"
    DB_RESPONSE = "DB_RESPONSE"


@dataclass
class Envelope:
    """Standard message envelope used across all SRB services."""

    msg_type: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    client_id: str = ""
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize envelope to JSON string."""
        return json.dumps(asdict(self))

    def to_bytes(self) -> bytes:
        """Serialize envelope to bytes for ZeroMQ transport."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: str | bytes) -> Envelope:
        """Deserialize envelope from JSON string or bytes."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        d = json.loads(data)
        return cls(**d)


def make_envelope(
    msg_type: str | MsgType,
    payload: dict[str, Any] | None = None,
    client_id: str = "",
    request_id: str | None = None,
) -> Envelope:
    """Convenience factory for creating message envelopes."""
    return Envelope(
        msg_type=str(msg_type),
        request_id=request_id or str(uuid.uuid4()),
        client_id=client_id,
        payload=payload or {},
    )


def parse_envelope(data: bytes | str) -> Envelope:
    """Parse raw ZeroMQ frame into an Envelope."""
    return Envelope.from_json(data)
