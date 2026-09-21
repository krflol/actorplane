"""Immutable event envelope metadata and bounded tracing options."""
from __future__ import annotations

from dataclasses import dataclass
import math
import weakref
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .authoring import ActorRef, World


def _u64(value: Any, name: str, *, positive: bool = False, optional: bool = False) -> None:
    if optional and value is None:
        return
    minimum = 1 if positive else 0
    if type(value) is not int or not minimum <= value < (1 << 64):
        raise ValueError(f"{name} must be a {'positive ' if positive else ''}u64")


def _actor_handle(value: Any, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    valid = (
        type(value) is tuple
        and len(value) == 3
        and type(value[0]) is int
        and 0 < value[0] < (1 << 64)
        and type(value[1]) is int
        and 0 <= value[1] < (1 << 32)
        and type(value[2]) is int
        and 0 < value[2] < (1 << 64)
    )
    if not valid:
        raise TypeError(f"{name} must be a (world, slot, generation) tuple")


def _port_handle(value: Any, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if type(value) is not tuple or len(value) != 4:
        raise TypeError(f"{name} must be a four-field port tuple")
    _actor_handle(value[:3], name)
    if type(value[3]) is not int or not 0 <= value[3] < (1 << 16):
        raise ValueError(f"{name} port index must be a u16")


def _utf8(value: str, name: str, limit: int) -> bytes:
    if type(value) is not str:
        raise TypeError(f"{name} must be str")
    if len(value) > limit:
        raise ValueError(f"{name} exceeds {limit} UTF-8 bytes")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{name} contains invalid Unicode") from exc
    if len(encoded) > limit:
        raise ValueError(f"{name} exceeds {limit} UTF-8 bytes")
    return encoded


@dataclass(frozen=True)
class TraceContext:
    trace_id: bytes
    span_id: bytes
    sampled: bool = False
    baggage: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if type(self.trace_id) is not bytes or len(self.trace_id) != 16 or not any(self.trace_id):
            raise ValueError("trace_id must be nonzero bytes16")
        if type(self.span_id) is not bytes or len(self.span_id) != 8 or not any(self.span_id):
            raise ValueError("span_id must be nonzero bytes8")
        if type(self.sampled) is not bool:
            raise TypeError("sampled must be bool")
        if type(self.baggage) is not tuple or len(self.baggage) > 8:
            raise TypeError("baggage must be a tuple of at most eight pairs")
        seen: set[str] = set()
        total = 0
        for pair in self.baggage:
            if type(pair) is not tuple or len(pair) != 2:
                raise TypeError("baggage entries must be exact two-tuples")
            key, value = pair
            key_bytes = _utf8(key, "baggage key", 64)
            if not key or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for char in key):
                raise ValueError("baggage key contains unsupported characters")
            value_bytes = _utf8(value, "baggage value", 256)
            if key in seen:
                raise ValueError("duplicate baggage key")
            seen.add(key)
            total += len(key_bytes) + len(value_bytes)
        if total > 1024:
            raise ValueError("baggage exceeds 1024 UTF-8 bytes")


@dataclass(frozen=True)
class MessageOptions:
    timeout: float | None = None
    correlation_id: int | None = None
    causation_id: int | None = None
    trace: TraceContext | None = None

    def __post_init__(self) -> None:
        if self.timeout is not None:
            if type(self.timeout) not in (int, float) or not 0 <= self.timeout <= 86400:
                raise ValueError("timeout must be between zero and 86400 seconds")
            if type(self.timeout) is float and not math.isfinite(self.timeout):
                raise ValueError("timeout must be finite")
        _u64(self.correlation_id, "correlation_id", positive=True, optional=True)
        _u64(self.causation_id, "causation_id", positive=True, optional=True)
        if self.trace is not None and type(self.trace) is not TraceContext:
            raise TypeError("trace must be TraceContext")

    def _native(self, source: tuple[int, int, int] | None = None) -> tuple:
        _actor_handle(source, "source", optional=True)
        timeout_ms = None if self.timeout is None else math.ceil(self.timeout * 1000)
        trace = None
        if self.trace is not None:
            trace = (
                self.trace.trace_id,
                self.trace.span_id,
                self.trace.sampled,
                self.trace.baggage,
            )
        return source, timeout_ms, self.correlation_id, self.causation_id, trace


@dataclass(frozen=True)
class Envelope:
    event_id: int
    schema_kind: str
    schema_id: int
    schema_version: int
    source: ActorRef | None
    destination: ActorRef
    dispatcher: tuple[int, int, int, int] | None
    destination_port: tuple[int, int, int, int] | None
    owner: ActorRef
    enqueued_at_ns: int
    deadline_ns: int | None = None
    correlation_id: int | None = None
    causation_id: int | None = None
    trace: TraceContext | None = None

    def __post_init__(self) -> None:
        _u64(self.event_id, "event_id", positive=True)
        if type(self.schema_kind) is not str or self.schema_kind not in ("pulse", "snapshot", "record", "structured"):
            raise ValueError("unknown schema_kind")
        if type(self.schema_id) is not int or not 0 <= self.schema_id < (1 << 32):
            raise ValueError("schema_id must be u32")
        if type(self.schema_version) is not int or not 0 <= self.schema_version < (1 << 32):
            raise ValueError("schema_version must be u32")
        _u64(self.enqueued_at_ns, "enqueued_at_ns")
        _u64(self.deadline_ns, "deadline_ns", optional=True)
        _u64(self.correlation_id, "correlation_id", positive=True, optional=True)
        _u64(self.causation_id, "causation_id", positive=True, optional=True)
        _port_handle(self.dispatcher, "dispatcher", optional=True)
        _port_handle(self.destination_port, "destination_port", optional=True)
        from .authoring import ActorRef
        if not isinstance(self.destination, ActorRef) or not isinstance(self.owner, ActorRef):
            raise TypeError("destination and owner must be ActorRef")
        if self.source is not None and not isinstance(self.source, ActorRef):
            raise TypeError("source must be ActorRef or None")
        _actor_handle(self.destination.handle, "destination")
        _actor_handle(self.owner.handle, "owner")
        if self.source is not None:
            _actor_handle(self.source.handle, "source")
        if self.trace is not None and type(self.trace) is not TraceContext:
            raise TypeError("trace must be TraceContext")

    @classmethod
    def _from_native(cls, data: dict[str, Any], world: World) -> Envelope:
        from .authoring import ActorRef

        world_ref = weakref.ref(world)

        def rebuild(value: Any) -> ActorRef | None:
            return None if value is None else ActorRef(*tuple(value), _world=world_ref)

        raw_trace = data.get("trace")
        trace = None
        if raw_trace is not None:
            trace = TraceContext(
                bytes(raw_trace["trace_id"]),
                bytes(raw_trace["span_id"]),
                raw_trace.get("sampled", False),
                tuple(tuple(pair) for pair in raw_trace.get("baggage", ())),
            )
        return cls(
            event_id=data["event_id"],
            schema_kind=data["schema_kind"],
            schema_id=data["schema_id"],
            schema_version=data["schema_version"],
            source=rebuild(data.get("source")),
            destination=rebuild(data["destination"]),
            dispatcher=data.get("dispatcher"),
            destination_port=data.get("destination_port"),
            owner=rebuild(data["owner"]),
            enqueued_at_ns=data["enqueued_at_ns"],
            deadline_ns=data.get("deadline_ns"),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            trace=trace,
        )
