"""Bounded actor authoring over the Rust World; callbacks run on one driver."""
from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import math
import os
import threading
import time
import weakref
from enum import Enum
from typing import Any, ClassVar, TYPE_CHECKING
if TYPE_CHECKING:
    from .components import LinkToken, Port
from .failures import Failure, TraceFrame, capture_failure
from .envelopes import Envelope, MessageOptions, TraceContext
from .schemas import CodecError, Event, SchemaPlan, event

MAX_RECORD_ELEMENTS = 4096
I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1


class AdmissionError(RuntimeError):
    """Native admission failed; admission does not imply handler completion."""


class QueueFull(AdmissionError):
    pass


class HandlerFailed(RuntimeError):
    pass


class AffinityError(RuntimeError):
    pass


class LifecycleError(RuntimeError):
    pass


class FailurePolicy(str, Enum):
    STOP_ACTOR = "stop_actor"
    STOP_WORLD = "stop_world"
    CONTINUE = "continue"


class OperationError(RuntimeError):
    pass


class PublicationOutcome(str, Enum):
    PENDING = "Pending"
    ROUTED = "Routed"
    CANCELLED = "Cancelled"
    EXPIRED = "Expired"
    FANOUT_LIMIT = "FanoutLimit"
    SNAPSHOT_FULL = "SnapshotFull"


class PublicationStatus(str, Enum):
    STAGED = "Staged"
    QUEUED = "Queued"
    ROUTING = "Routing"
    ROUTED = "Routed"
    CANCELLED = "Cancelled"
    EXPIRED = "Expired"
    FANOUT_LIMIT = "FanoutLimit"
    SNAPSHOT_FULL = "SnapshotFull"


@dataclass(frozen=True)
class DeliveryReport:
    admitted: int
    rejected: int
    matched: int
    staged: int
    outcome: PublicationOutcome


@dataclass(frozen=True)
class PublicationTicket:
    """Nonblocking native publication status and bounded terminal report."""
    handle: Any
    _world: Any = field(repr=False, compare=False)
    _pid: int = field(default_factory=os.getpid, repr=False, compare=False)

    def _check_pid(self):
        if os.getpid() != self._pid:
            raise LifecycleError("publication ticket belongs to another process")

    def _runtime(self):
        self._check_pid()
        world = self._world()
        if world is None:
            raise LifecycleError("publication's World no longer exists")
        world._check_process()
        return world

    @property
    def id(self) -> int:
        self._check_pid()
        return int(self.handle.identifier())

    @property
    def source(self) -> ActorRef:
        self._check_pid()
        world = self._world()
        return ActorRef(
            *self.handle.source(),
            _world=weakref.ref(world) if world is not None else None,
        )

    def status(self) -> PublicationStatus:
        self._check_pid()
        return PublicationStatus(self.handle.status())

    def report(self) -> DeliveryReport | None:
        self._check_pid()
        data = self.handle.report()
        if data is None:
            return None
        try:
            return DeliveryReport(
                admitted=int(data["admitted"]),
                rejected=int(data["rejected"]),
                matched=int(data["matched"]),
                staged=int(data["staged"]),
                outcome=PublicationOutcome(data["outcome"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AdmissionError("malformed native publication report") from exc


@dataclass(frozen=True)
class OperationOutcome:
    state: str
    value: Any = None
    message: str = ""
    envelope: Envelope | None = None


@dataclass(frozen=True)
class OperationHandle:
    """Nonblocking view of a bounded owner-scoped result reservation."""
    handle: tuple[int, int, int]
    _world: Any = field(repr=False, compare=False)

    def _runtime(self):
        world = self._world()
        if world is None:
            raise LifecycleError("operation's World no longer exists")
        world._check_process()
        return world

    def poll(self) -> OperationOutcome | None:
        world = self._runtime()
        return world._operation_outcome(world._native.operation_status(self.handle))

    def take(self) -> OperationOutcome | None:
        """Consume a terminal result and release its retained slot/payload."""
        world = self._runtime()
        result = world._native.take_operation(self.handle)
        return None if result is None else world._operation_outcome(result)

    def cancel(self) -> bool:
        return self._runtime()._native.cancel_operation(self.handle)

    def result(self):
        outcome = self.take()
        if outcome is None:
            raise OperationError("operation is still pending; drive the World before reading its result")
        if outcome.state == "Completed":
            return outcome.value
        if outcome.state == "TimedOut":
            raise TimeoutError("request timed out; an in-flight external effect may still have occurred")
        raise OperationError(f"{outcome.state}: {outcome.message}")


def _invoke_sync(callback, *args):
    result = callback(*args)
    if inspect.isawaitable(result) or inspect.isasyncgen(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("callbacks must complete synchronously; use events for completion")
    return result


@event("actorplane.Pulse")
class Pulse(Event):
    value: int


@event("actorplane.CountSnapshot")
class CountSnapshot(Event):
    count: int
    total: int


def _int64(value: Any, path: str) -> int:
    if type(value) is not int or not I64_MIN <= value <= I64_MAX:
        raise CodecError(f"{path}: expected signed 64-bit int, got {type(value).__name__}")
    return value


def _encode(value: Event, max_bytes: int = 32764) -> list[int] | bytes:
    if type(value) is Pulse:
        return [_int64(value.value, "actorplane.Pulse.value")]
    if type(value) is CountSnapshot:
        count = _int64(value.count, "actorplane.CountSnapshot.count")
        if count < 0:
            raise CodecError("CountSnapshot.count must be nonnegative")
        return [count, _int64(value.total, "actorplane.CountSnapshot.total")]
    plan = getattr(type(value), "__actorplane_schema__", None)
    if not isinstance(plan, SchemaPlan) or plan.typ is not type(value):
        raise CodecError("payload type must be declared with @event")
    return plan.encode(value, max_bytes=max_bytes)


def handles(event_type):
    if not isinstance(event_type, type) or not hasattr(event_type, "__actorplane_schema__"):
        raise TypeError("@handles requires an @event type")

    def decorate(fn):
        if inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn):
            raise TypeError("coroutine handlers are not supported; return results through events")
        previous = tuple(getattr(fn, "__actorplane_handles__", ()))
        if event_type in previous:
            raise TypeError(f"duplicate handler for {event_type.__name__}")
        fn.__actorplane_handles__ = (*previous, event_type)
        return fn
    return decorate


@dataclass(frozen=True)
class ActorRef:
    world_id: int
    slot: int
    generation: int
    _world: Any = field(default=None, repr=False, compare=False, hash=False)

    @property
    def handle(self) -> tuple[int, int, int]:
        return self.world_id, self.slot, self.generation

    def send(self, value: Event, *, options: MessageOptions | None = None) -> int:
        world = self._world() if self._world else None
        if world is None:
            raise LifecycleError("actor reference's World no longer exists")
        return world.send(self, value, options=options)


class Actor:
    __actorplane_handlers__: ClassVar[dict[type, str]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        methods = {}
        for base in reversed(cls.__mro__):
            methods.update(vars(base))
        handlers = {}
        identities = set()
        for name, method in methods.items():
            declarations = getattr(method, "__actorplane_handles__", None)
            if declarations is None:
                # An ordinary Python override retains the inherited endpoint.
                for base in cls.__mro__[1:]:
                    inherited = vars(base).get(name)
                    if inherited is not None and hasattr(inherited, "__actorplane_handles__"):
                        declarations = inherited.__actorplane_handles__
                        break
            for typ in declarations or ():
                if not callable(method) or inspect.iscoroutinefunction(method) or inspect.isasyncgenfunction(method):
                    raise TypeError(f"{name}: handlers must be synchronous callables")
                identity = typ._event_name, typ._event_version
                if identity in identities:
                    raise TypeError(f"duplicate handler for {identity}")
                identities.add(identity)
                handlers[typ] = name
        for hook in ("configure", "on_start", "on_stop", "on_failure"):
            if inspect.iscoroutinefunction(getattr(cls, hook)) or inspect.isasyncgenfunction(getattr(cls, hook)):
                raise TypeError(f"coroutine {hook} hooks are not supported")
        cls.__actorplane_handlers__ = handlers

    def configure(self, ctx: Context) -> None:
        pass

    def on_start(self, ctx: Context) -> None:
        pass

    def on_stop(self, ctx: Context) -> None:
        pass

    def on_failure(self, failure: Failure, ctx: Context) -> None:
        raise HandlerFailed(f"unhandled child failure: {failure.exception_type}")


def _milliseconds(seconds: float, *, positive: bool = False) -> int:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError("duration must be a number of seconds")
    if not math.isfinite(seconds) or seconds < 0 or seconds > 86400:
        raise ValueError("duration must be finite and between zero and 24 hours")
    result = math.ceil(seconds * 1000)
    if positive and result == 0:
        raise ValueError("period must be positive")
    return result


def _native_options(options: MessageOptions | None, source: ActorRef | None = None):
    if options is not None and type(options) is not MessageOptions:
        raise TypeError("options must be MessageOptions")
    if options is None and source is None:
        return None
    return (options or MessageOptions())._native(source.handle if source else None)


class Context:
    def __init__(
        self, world: World, ref: ActorRef,
        operation: tuple[int, int, int] | None = None,
        envelope: Envelope | None = None, claim_token: int | None = None,
    ):
        self.world, self.ref = world, ref
        self.operation = operation
        self._envelope = envelope
        self._claim_token = claim_token

    @property
    def envelope(self) -> Envelope | None:
        return self._envelope

    @property
    def actor(self) -> ActorRef:
        return self.ref

    def _owner(self) -> None:
        self.world._check_driver()
        if self.world._native.state(self.ref.handle) not in ("Starting", "Active"):
            raise LifecycleError("owner is stopped; it cannot create new work")

    def send(self, target: ActorRef, value: Event, *, options: MessageOptions | None = None) -> int:
        self._owner()
        if options is not None and type(options) is not MessageOptions:
            raise TypeError("options must be MessageOptions")
        return self.world.send(target, value, options=options, _source=self.ref, _claim_token=self._claim_token)

    def subscribe(self, source, target: ActorRef) -> int | LinkToken:
        self._owner()
        from .components import Port
        if isinstance(source, Port):
            return self.world._subscribe_port(self.ref, source, target)
        source_handle = source.handle if isinstance(source, ActorRef) else source
        return self.world._native.subscribe(source_handle, target.handle)

    def link(self, source: Port, target: Port) -> LinkToken:
        self._owner()
        return self.world._link(self.ref, source, target)

    def after(self, delay: float, value: Event, target: ActorRef | None = None, *, options: MessageOptions | None = None) -> None:
        self._owner()
        if options is not None and type(options) is not MessageOptions:
            raise TypeError("options must be MessageOptions")
        kind, values, schema = self.world._encode_event(value)
        native_options = _native_options(options, self.ref)
        self.world._native.after(self.ref.handle, (target or self.ref).handle,
                                 _milliseconds(delay), kind, values, schema, native_options, self._claim_token)

    def native_counter(self, interval: float, window: float, target: ActorRef | None = None):
        """Create a fresh owned Rust source, window counter and summary sink."""
        self._owner()
        return self.world._native.counter(self.ref.handle,
            _milliseconds(interval, positive=True), _milliseconds(window, positive=True),
            target.handle if target else None)

    def request(self, target: ActorRef, value: Event, *, timeout: float = 1.0, options: MessageOptions | None = None) -> OperationHandle:
        self._owner()
        if options is not None and type(options) is not MessageOptions:
            raise TypeError("options must be MessageOptions")
        return self.world.request(self.ref, target, value, timeout=timeout, options=options, _claim_token=self._claim_token)

    def spawn(self, actor_type: type[Actor], *, failure_policy: FailurePolicy | None = None) -> ActorRef:
        self._owner()
        return self.world.spawn(actor_type, parent=self.ref, failure_policy=failure_policy)

    def reply(self, value: Event, *, options: MessageOptions | None = None) -> bool:
        """Complete this admitted request; a late reply returns False."""
        self.world._check_driver()
        if options is not None and type(options) is not MessageOptions:
            raise TypeError("options must be MessageOptions")
        if self.operation is None:
            raise LifecycleError("this delivery is not a request")
        kind, values, schema = self.world._encode_event(value)
        native_options = _native_options(options, self.ref)
        return self.world._native.reply(self.operation, self.ref.handle, kind, values, schema, native_options)


@dataclass(frozen=True)
class _Codec:
    typ: type
    kind: str
    schema: int
    plan: SchemaPlan

    @property
    def key(self) -> tuple[str, int]:
        return self.kind, self.schema

    def decode(self, values: list[int] | bytes):
        if self.kind in ("pulse", "snapshot"):
            return self.typ(*values)
        return self.plan.decode(values)


@dataclass
class _Registration:
    ref: ActorRef
    typ: type[Actor]
    handlers: dict[tuple[str, int], tuple[_Codec, str]]
    instance: Actor | None = None
    started: bool = False
    parent: ActorRef | None = None
    failure_policy: FailurePolicy = FailurePolicy.STOP_ACTOR
    config: tuple = ()
    components: tuple = ()
    ports: dict = field(default_factory=dict)


class World:
    def __init__(self, *, max_schemas: int = 128,
                 max_services: int = 64, max_service_leases: int = 1024,
                 max_leases_per_actor: int = 16,
                 max_publications: int = 256,
                 max_publications_per_source: int = 32,
                 max_publication_fanout: int = 4096,
                 max_routing_snapshot_entries: int = 16384,
                 routing_batch_size: int = 32,
                 failure_policy: FailurePolicy = FailurePolicy.STOP_ACTOR, **kwargs):
        from . import _native
        if _native is None:
            raise ImportError("build the actorplane native extension with maturin develop")
        if type(max_schemas) is not int or not 1 <= max_schemas <= 65536:
            raise ValueError("max_schemas must be in 1..65536")
        if not isinstance(failure_policy, FailurePolicy):
            raise ValueError("failure_policy must be a FailurePolicy")
        for name, maximum in (("cpu_workers", 64), ("max_cpu_jobs", 4096)):
            if name in kwargs and (type(kwargs[name]) is not int or not 1 <= kwargs[name] <= maximum):
                raise ValueError(f"{name} must be an integer in 1..{maximum}")
        for name, value, maximum in (("max_services", max_services, 4096),
                                     ("max_service_leases", max_service_leases, 65536),
                                     ("max_leases_per_actor", max_leases_per_actor, 1024),
                                     ("max_publications", max_publications, 65536),
                                     ("max_publications_per_source", max_publications_per_source, 4096),
                                     ("max_publication_fanout", max_publication_fanout, 65536),
                                     ("max_routing_snapshot_entries", max_routing_snapshot_entries, 1048576),
                                     ("routing_batch_size", routing_batch_size, 4096)):
            if type(value) is not int or not 0 <= value <= maximum:
                raise ValueError(f"{name} must be an integer in 0..{maximum}")
        self._native = _native.NativeWorld(max_schemas=max_schemas,
                                           max_services=max_services,
                                           max_service_leases=max_service_leases,
                                           max_leases_per_actor=max_leases_per_actor,
                                           max_publications=max_publications,
                                           max_publications_per_source=max_publications_per_source,
                                           max_publication_fanout=max_publication_fanout,
                                           max_routing_snapshot_entries=max_routing_snapshot_entries,
                                           routing_batch_size=routing_batch_size,
                                           **kwargs)
        self._max_event_bytes = kwargs.get("max_event_bytes", 32768)
        self._actors: dict[tuple[int, int, int], _Registration] = {}
        self._schemas: dict[tuple[str, int], _Codec] = {}
        self._codec_index: dict[tuple[str, int], _Codec] = {}
        self._schema_lock = threading.RLock()
        self._driver_lock = threading.RLock()
        self._max_schemas = max_schemas
        self._failure_policy = failure_policy
        self._closed = self._stop_requested = self._dispatching = self._running = False
        self._driver: int | None = None
        self._pid = os.getpid()
        self._world_id = self._native.snapshot()["id"]
        self._last_report = None
        self._draining = False
        self._drain_deadline: float | None = None
        self._current_claim: int | None = None

    def _check_process(self):
        if os.getpid() != self._pid:
            raise LifecycleError("World cannot be used after fork")

    def _clock(self) -> float:
        """Monotonic clock seam used by lifecycle and shutdown deadlines."""
        return time.monotonic()

    def _idle_wait(self, milliseconds: int) -> None:
        """Wait for native activity while the foreground driver is idle."""
        self._native.wait(milliseconds)

    def _idle_delay(self, deadline: float | None = None) -> int:
        """Return a bounded wait, respecting the nearest driver deadline."""
        if self._drain_deadline is not None:
            deadline = (self._drain_deadline if deadline is None
                        else min(deadline, self._drain_deadline))
        if deadline is None:
            return 1000
        remaining = max(0.0, deadline - self._clock())
        return min(1000, math.ceil(remaining * 1000))

    def _check_driver(self):
        self._check_process()
        current = threading.get_ident()
        with self._driver_lock:
            if self._driver is None:
                self._driver = current
            if self._driver != current:
                raise AffinityError("World callbacks and lifecycle belong to its foreground driver thread")

    def _check_open(self):
        self._check_process()
        if self._closed or self._stop_requested:
            raise LifecycleError("World is closed or stopping")

    def _register(self, typ: type) -> _Codec:
        with self._schema_lock:
            return self._register_locked(typ)

    def _register_locked(self, typ: type) -> _Codec:
        return self._register_many_locked((typ,))[0]

    def _register_many_locked(self, types) -> list[_Codec]:
        if self._closed:
            raise LifecycleError("World is closed")
        pending = {}
        keys = []
        for typ in types:
            plan = getattr(typ, "__actorplane_schema__", None)
            if not isinstance(plan, SchemaPlan) or plan.typ is not typ:
                raise CodecError("event type must be declared with @event")
            key = plan.descriptor[1:3]
            keys.append(key)
            prior_type = self._schemas[key].typ if key in self._schemas else pending.get(key)
            if prior_type is not None and prior_type is not typ:
                raise CodecError(f"schema identity {key} already belongs to another event class")
            if key not in self._schemas:
                pending[key] = typ
        if len(self._schemas) + len(pending) > self._max_schemas:
            raise CodecError("World schema registration limit exceeded")
        native_types = [typ for typ in pending.values() if typ not in (Pulse, CountSnapshot)]
        try:
            ids = iter(self._native.register_schemas(tuple(typ.__actorplane_schema__.descriptor for typ in native_types))
                       if native_types else ())
        except (ValueError, TypeError, OverflowError) as exc:
            raise CodecError(str(exc)) from exc
        for key, typ in pending.items():
            kind = "pulse" if typ is Pulse else "snapshot" if typ is CountSnapshot else "structured"
            codec = _Codec(typ, kind, next(ids) if kind == "structured" else 0, typ.__actorplane_schema__)
            self._schemas[key] = codec
            self._codec_index[codec.key] = codec
        return [self._schemas[key] for key in keys]

    def _encode_event(self, value: Event) -> tuple[str, list[int] | bytes, int]:
        codec = self._register(type(value))
        values = (codec.plan.encode(value, max_bytes=max(0, self._max_event_bytes - 4))
                  if codec.kind == "structured" else _encode(value))
        return codec.kind, values, codec.schema

    def spawn(self, actor_type: type[Actor], *, parent: ActorRef | None = None,
              failure_policy: FailurePolicy | None = None) -> ActorRef:
        # Registration and the first driver bind are serialized. A concurrently
        # binding driver must see the entire registration or reject its thread.
        with self._driver_lock:
            return self._spawn(actor_type, parent=parent, failure_policy=failure_policy)

    def _spawn(self, actor_type: type[Actor], *, parent: ActorRef | None,
               failure_policy: FailurePolicy | None) -> ActorRef:
        self._check_open()
        if self._driver is not None:
            self._check_driver()
        if not isinstance(actor_type, type) or not issubclass(actor_type, Actor):
            raise TypeError("spawn expects an Actor class")
        from .components import declarations, interfaces, port_declarations
        components = declarations(actor_type)
        port_specs = port_declarations(actor_type)
        contracts = interfaces(actor_type)
        self._validate_composition(actor_type)
        if parent is not None:
            if not isinstance(parent, ActorRef) or parent.world_id != self._world_id:
                raise AdmissionError("parent reference belongs to another World")
            if parent.handle not in self._actors:
                raise LifecycleError("parent actor is stale or stopped")
        policy = self._failure_policy if failure_policy is None else failure_policy
        if not isinstance(policy, FailurePolicy):
            raise ValueError("failure_policy must be a FailurePolicy")
        with self._schema_lock:
            h = self._native.allocate(True, parent.handle if parent else None)
            try:
                declarations = actor_type.__actorplane_handlers__
                codecs = self._register_many_locked(tuple(declarations) + tuple(p[2] for p in port_specs))
                handlers = {codec.key: (codec, method) for codec, method in zip(codecs, declarations.values())}
                self._register_contract(h, actor_type, port_specs, contracts)
            except BaseException:
                self._native.stop(h)
                self._native.finish_python(h)
                raise
        ref = ActorRef(*h, _world=weakref.ref(self))
        self._actors[h] = _Registration(ref, actor_type, handlers, parent=parent,
                                        failure_policy=policy, components=components)
        self._actors[h].ports = self._bind_ports(ref, port_specs)
        return ref

    def _validate_composition(self, root):
        from .components import Component, declarations
        count = 0

        def visit(typ, path):
            nonlocal count
            count += 1
            if count > 1024 or len(path) > 32 or typ in path:
                raise ValueError("component composition is recursive or exceeds its bounds")
            for _, descriptor in declarations(typ):
                config = dict(descriptor.config)
                native_kind = getattr(descriptor.factory, "__actorplane_native_kind__", None)
                if native_kind:
                    expected = {"pulse_source": {"interval"}, "window_counter": {"window"}, "snapshot_sink": set()}
                    if native_kind not in expected or set(config) != expected[native_kind]:
                        raise ValueError("invalid native component configuration fields")
                    for duration in config.values():
                        _milliseconds(duration, positive=True)
                elif issubclass(descriptor.factory, Component):
                    inspect.signature(descriptor.factory).bind(**config)
                    visit(descriptor.factory, (*path, typ))

        visit(root, ())

    def _register_contract(self, handle, typ, ports, contracts):
        self._native.register_component(handle, self._contract_descriptor(typ.__module__ + "." + typ.__qualname__, ports, contracts))

    def _contract_descriptor(self, name, ports, contracts):
        def encoded_port(name, direction, schema):
            codec = self._register(schema)
            return name, direction, codec.kind, codec.schema
        native_ports = tuple(encoded_port(*port) for port in ports)
        native_interfaces = tuple((contract.name, contract.version, tuple(
            encoded_port(name, direction, schema)
            for direction, items in (("input", contract.inputs), ("output", contract.outputs))
            for name, schema in items)) for contract in contracts)
        return name, 1, native_ports, native_interfaces

    def _bind_ports(self, ref, ports):
        from .components import Port
        return {name: Port(ref, name, schema, index, direction)
                for index, (name, direction, schema) in enumerate(ports)}

    def _link(self, owner, source, target):
        from .components import LinkToken, Port
        self._check_driver()
        if not isinstance(source, Port) or not isinstance(target, Port):
            raise TypeError("link requires an output port and an input port")
        if source.direction != "output" or target.direction != "input":
            raise TypeError("link requires an output port and an input port")
        if source.schema is not target.schema:
            raise CodecError("link port schemas differ")
        try:
            identifier = self._native.link(owner.handle, source.handle, target.handle)
        except RuntimeError as error:
            raise AdmissionError(str(error)) from error
        return LinkToken(identifier, owner)

    def _subscribe_port(self, owner, source, target):
        if not isinstance(target, ActorRef) or target.world_id != self._world_id:
            raise AdmissionError("subscription target belongs to another World")
        registration = self._actors.get(target.handle)
        if registration is None:
            raise LifecycleError("subscription target is stale or stopped")
        matches = [port for port in registration.ports.values()
                   if port.direction == "input" and port.schema is source.schema]
        if len(matches) != 1:
            raise CodecError("subscription target needs a matching declared handler")
        return self._link(owner, source, matches[0])

    def _send_port(self, port, value, *, options=None):
        self._check_open()
        if port.direction != "input" or type(value) is not port.schema:
            raise CodecError("input port requires its exact declared event type")
        kind, values, schema = self._encode_event(value)
        try:
            native_options = _native_options(options)
            claim = self._current_claim if self._driver == threading.get_ident() else None
            return self._native.send_port(port.handle, kind, values, schema, native_options, claim)
        except RuntimeError as error:
            raise (QueueFull if "QueueFull" in str(error) else AdmissionError)(str(error)) from error

    def _publish_port(self, port, value, *, options=None):
        self._check_driver()
        if self._current_claim is None:
            self._check_open()
        if port.direction != "output" or type(value) is not port.schema:
            raise CodecError("output port requires its exact declared event type")
        kind, values, schema = self._encode_event(value)
        try:
            native_options = _native_options(options, port.owner)
            native_ticket = self._native.publish_port(
                port.handle, kind, values, schema, self._current_claim, native_options
            )
        except RuntimeError as error:
            raise AdmissionError(str(error)) from error
        return PublicationTicket(native_ticket, weakref.ref(self))

    def send(self, ref: ActorRef, value: Event, *, options: MessageOptions | None = None, _source=None, _claim_token=None) -> int:
        self._check_open()
        if options is not None and type(options) is not MessageOptions:
            raise TypeError("options must be MessageOptions")
        if not isinstance(ref, ActorRef):
            raise TypeError("send requires an ActorRef")
        if ref.world_id != self._world_id:
            raise AdmissionError("CrossWorld: reference belongs to another World")
        kind, values, schema = self._encode_event(value)
        try:
            native_options = _native_options(options, _source)
            return self._native.send(ref.handle, kind, values, schema, native_options, _claim_token)
        except RuntimeError as exc:
            if "ActorNotReady" in str(exc):
                raise LifecycleError(str(exc)) from exc
            error_type = QueueFull if "QueueFull" in str(exc) else AdmissionError
            raise error_type(str(exc)) from exc

    _send = send

    def request(self, owner: ActorRef, target: ActorRef, value: Event, *, timeout: float = 1.0, options: MessageOptions | None = None, _claim_token=None) -> OperationHandle:
        self._check_open()
        if not isinstance(owner, ActorRef) or not isinstance(target, ActorRef):
            raise TypeError("request requires owner and target ActorRefs")
        kind, values, schema = self._encode_event(value)
        try:
            native_options = _native_options(options, owner)
            identifier = self._native.request(owner.handle, target.handle, kind, values, schema, _milliseconds(timeout), native_options, _claim_token)
        except RuntimeError as exc:
            raise AdmissionError(str(exc)) from exc
        return OperationHandle(identifier, weakref.ref(self))

    def _operation_outcome(self, data: dict) -> OperationOutcome | None:
        state = data["state"]
        if state == "Pending":
            return None
        if state == "Completed":
            codec = self._codec_index.get((data["kind"], data["schema"]))
            if codec is None:
                raise CodecError("operation result has an unregistered schema")
            envelope = Envelope._from_native(data["envelope"], self) if data.get("envelope") else None
            return OperationOutcome(state, codec.decode(data["values"]), envelope=envelope)
        envelope = Envelope._from_native(data["envelope"], self) if data.get("envelope") else None
        return OperationOutcome(state, message=data.get("message", ""), envelope=envelope)

    def _publication_report(self, data: dict) -> DeliveryReport:
        try:
            return DeliveryReport(
                admitted=int(data["admitted"]),
                rejected=int(data["rejected"]),
                matched=int(data["matched"]),
                staged=int(data["staged"]),
                outcome=PublicationOutcome(data["outcome"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AdmissionError("malformed native publication report") from exc

    def _cleanup(self) -> None:
        """Release registrations after fencing, never inside a callback."""
        errors = []
        for key, registration in self._ready_registrations("cleanup"):
            registration = self._actors.pop(key, None)
            if registration is None:
                continue
            try:
                if registration.instance is not None:
                    self._dispatching = True
                    _invoke_sync(registration.instance.on_stop, Context(self, registration.ref))
            except BaseException as exc:
                self._report_exception(registration.ref, "Stop", "on_stop", exc, "StopActor")
                errors.append(exc)
            finally:
                self._dispatching = False
                self._native.finish_python(key)
        if errors:
            raise HandlerFailed(f"on_stop failed: {type(errors[0]).__name__}") from errors[0]

    def _ready_registrations(self, phase: str):
        """Yield cutoff-bounded, generation-fenced registrations by phase."""
        cutoff = self._native.ready_cutoff()
        cursor = None
        descending = phase == "cleanup"
        while True:
            item = self._native.ready_next(phase, cursor, cutoff)
            if item is None:
                return
            order, key = item
            registration = self._actors.get(key)
            if registration is None:
                cursor = order
                continue
            yield key, registration
            cursor = None if descending else order

    def _start_pending(self) -> None:
        for key, registration in self._ready_registrations("start"):
            if key not in self._actors or registration.started or self._native.state(key) != "Starting":
                continue
            self._dispatching = True
            phase, handler = "Construction", "__init__"
            try:
                self._construct_components(registration)
                ctx = Context(self, registration.ref)
                phase, handler = "Configure", "configure"
                _invoke_sync(registration.instance.configure, ctx)
                if self._native.state(key) == "Starting":
                    phase, handler = "Start", "on_start"
                    _invoke_sync(registration.instance.on_start, ctx)
                if self._native.state(key) == "Starting":
                    self._activate_components(registration)
                    self._native.activate(key)
                    registration.started = True
            except BaseException as original:
                self._report_exception(registration.ref, phase, handler, original, "StopActor")
                self._dispatching = False
                try:
                    self._cleanup()
                except BaseException as cleanup_error:
                    original.add_note(f"Additional cleanup error: {str(cleanup_error)[:1024]}")
                if not isinstance(original, Exception) or registration.parent is None:
                    raise
            finally:
                self._dispatching = False

    def _construct_components(self, registration):
        from types import MappingProxyType
        from .components import BoundComponent, port_declarations
        registration.instance = registration.typ(**dict(registration.config))
        object.__setattr__(registration.instance, "__actorplane_bound_ports__", MappingProxyType(registration.ports))
        bound = {}
        object.__setattr__(registration.instance, "__actorplane_components__", MappingProxyType(bound))
        for name, descriptor in registration.components:
            kind = getattr(descriptor.factory, "__actorplane_native_kind__", None)
            if kind:
                config = dict(descriptor.config)
                period = next(iter(config.values()), 0)
                native = self._native.prepare_component(registration.ref.handle, kind, _milliseconds(period))
                ref = ActorRef(*native.owner, _world=weakref.ref(self))
                ports = self._bind_ports(ref, port_declarations(descriptor.factory))
                bound[name] = BoundComponent(ref, ports, native)
            else:
                ref = self._spawn(descriptor.factory, parent=registration.ref,
                                  failure_policy=registration.failure_policy)
                child = self._actors[ref.handle]
                child.config = descriptor.config
                bound[name] = BoundComponent(ref, child.ports)
                try:
                    self._construct_components(child)
                except BaseException as error:
                    self._report_exception(child.ref, "Construction", "__init__", error, "StopActor")
                    raise
            for required in descriptor.requires:
                requirement = self._contract_descriptor("requirement", (), (required,))
                for dependency in descriptor.depends_on:
                    try:
                        self._native.require_interface(bound[dependency].owner.handle, requirement)
                        break
                    except RuntimeError as error:
                        if str(error) != "InterfaceMismatch":
                            raise
                else:
                    raise LifecycleError(f"native contract lacks required interface {required.name}")
        # All proxies exist before callbacks. Starting descendants remain behind
        # the root's admission fence until every required hook has succeeded.

    def _activate_components(self, registration):
        for proxy in registration.instance.__actorplane_components__.values():
            child = self._actors.get(proxy.owner.handle)
            if child is not None:
                if self._native.state(child.ref.handle) != "Starting":
                    raise LifecycleError("required component stopped before startup")
                context = Context(self, child.ref)
                phase, hook = "Configure", "configure"
                try:
                    _invoke_sync(child.instance.configure, context)
                    if self._native.state(child.ref.handle) == "Starting":
                        phase, hook = "Start", "on_start"
                        _invoke_sync(child.instance.on_start, context)
                except BaseException as error:
                    self._report_exception(child.ref, phase, hook, error, "StopActor")
                    raise
                if self._native.state(child.ref.handle) != "Starting":
                    raise LifecycleError("required component stopped during startup")
                self._activate_components(child)
                self._native.activate(child.ref.handle)
                child.started = True
            else:
                self._native.activate(proxy.owner.handle)

    def _report_exception(self, reference, phase, handler, exc, action,
                          event_id=None, schema=None, operation=None):
        phase, handler, exception_type, frames, truncated = capture_failure(exc, phase, handler)
        return self._native.report_failure(reference.handle, phase, handler,
            exception_type, frames, action, event_id, schema, operation, truncated)

    def _failure_record(self, data, coalesced=0):
        actor = data.get("actor")
        actor_ref = None
        if actor is not None:
            actor_ref = ActorRef(*actor, _world=weakref.ref(self))
        operation = data.get("operation")
        frames = tuple(TraceFrame(*frame) for frame in data.get("frames", ()))
        return Failure(data.get("sequence", 0), data.get("elapsed_ns", 0), actor_ref,
                       data.get("event_id"), data.get("schema"), operation,
                       data.get("phase", "Handler"), data.get("handler", ""),
                       data.get("exception_type", "builtins.Exception"), frames,
                       bool(data.get("truncated", False)), data.get("action", "StopActor"),
                       int(data.get("coalesced", coalesced)))

    def _pump_failures(self):
        processed = 0
        for key, registration in self._ready_registrations("failure"):
            if registration.instance is None:
                continue
            claimed = self._native.claim_failure(key)
            if claimed is None:
                continue
            token, data, coalesced = claimed
            failure = self._failure_record(data, coalesced)
            self._dispatching = True
            try:
                action = _invoke_sync(registration.instance.on_failure, failure, Context(self, registration.ref))
                if action is not None and not isinstance(action, FailurePolicy):
                    raise TypeError("on_failure must return a FailurePolicy or None")
            except BaseException as exc:
                try:
                    self._report_exception(registration.ref, "Supervisor", "on_failure", exc, "StopWorld")
                finally:
                    self._native.finish_failure(token)
                self.request_stop()
                self._dispatching = False
                try:
                    self._cleanup()
                except BaseException as cleanup_error:
                    exc.add_note(f"Additional cleanup error: {type(cleanup_error).__name__}")
                if not isinstance(exc, Exception):
                    raise
                raise HandlerFailed(f"{registration.typ.__name__} supervisor failed: {type(exc).__name__}") from exc
            else:
                self._native.finish_failure(token, action.value if action else None)
                processed += 1
                if action is FailurePolicy.STOP_WORLD:
                    self.request_stop()
                    break
            finally:
                self._dispatching = False
        return processed

    def _pump(self) -> int:
        # Admit control work before lifecycle callbacks can produce new reports.
        # Failures raised by this pump remain queued for a subsequent pump.
        processed = self._pump_failures()
        self._cleanup()
        self._start_pending()
        for key, registration in self._ready_registrations("delivery"):
            if key not in self._actors:
                continue
            claimed = self._native.claim(key)
            if claimed is None:
                continue
            token, kind, values, schema, _event_id, operation = claimed
            self._dispatching = True
            self._current_claim = token
            callback = None
            name = ""
            try:
                envelope_data = self._native.claim_metadata(token)
                envelope = Envelope._from_native(envelope_data, self) if envelope_data else None
                callback = registration.handlers.get((kind, schema))
                if callback is None:
                    raise CodecError(f"{registration.typ.__name__}: no handler for {(kind, schema)}")
                codec, name = callback
                value = codec.decode(values)
                _invoke_sync(getattr(registration.instance, name), value, Context(self, registration.ref, operation, envelope, token))
            except BaseException as exc:
                policy = registration.failure_policy
                action = "StopActor" if (not isinstance(exc, Exception) or policy is FailurePolicy.STOP_ACTOR) else ("StopWorld" if policy is FailurePolicy.STOP_WORLD else "Continue")
                self._report_exception(registration.ref, "Handler", name if callback else "", exc,
                                       action, _event_id, schema, operation)
                self._native.finish(token, False)
                if policy is FailurePolicy.STOP_WORLD:
                    self.request_stop()
                self._dispatching = False
                if policy is not FailurePolicy.CONTINUE or not isinstance(exc, Exception):
                    try:
                        self._cleanup()
                    except BaseException as cleanup_error:
                        exc.add_note(f"Additional cleanup error: {str(cleanup_error)[:1024]}")
                if not isinstance(exc, Exception):
                    raise
                if policy is FailurePolicy.CONTINUE or (registration.parent is not None and policy is not FailurePolicy.STOP_WORLD):
                    processed += 1
                    continue
                raise HandlerFailed(f"{registration.typ.__name__} handler failed: {type(exc).__name__}") from exc
            else:
                self._native.finish(token, True)
                processed += 1
            finally:
                self._dispatching = False
                self._current_claim = None
        self._cleanup()
        return processed

    def step(self) -> int:
        self._check_driver()
        if self._dispatching or self._running:
            raise AffinityError("World dispatch is not reentrant")
        if self._closed:
            raise LifecycleError("World is closed")
        return self._pump()

    def run(self, duration: float | None = None) -> None:
        self._check_driver()
        if self._running or self._dispatching:
            raise AffinityError("World dispatch is not reentrant")
        if self._closed:
            raise LifecycleError("World is closed")
        if duration is not None:
            _milliseconds(duration)
        deadline = None if duration is None else self._clock() + duration
        self._running = True
        try:
            self._pump()
            while self._actors and (not self._stop_requested or self._draining):
                if deadline is not None and self._clock() >= deadline:
                    break
                if self._pump() == 0 and self._actors:
                    self._idle_wait(self._idle_delay(deadline))
            self._cleanup()
        finally:
            self._running = False

    def run_for(self, duration: float) -> None:
        self.run(duration)

    def stop(self, ref: ActorRef, *, mode: str = "cancel", deadline: float = 1.0) -> dict:
        self._check_driver()
        if not isinstance(ref, ActorRef):
            raise TypeError("stop requires an ActorRef")
        if mode not in ("cancel", "drain"):
            raise ValueError("stop mode must be 'cancel' or 'drain'")
        try:
            report = (self._native.drain(ref.handle, _milliseconds(deadline)) if mode == "drain"
                      else self._native.stop(ref.handle))
        except RuntimeError as exc:
            raise AdmissionError(str(exc)) from exc
        if not self._dispatching:
            self._cleanup()
        return report

    def request_stop(self, *, mode: str = "cancel", deadline: float = 1.0) -> None:
        self._check_process()
        if mode not in ("cancel", "drain"):
            raise ValueError("stop mode must be 'cancel' or 'drain'")
        delay_ms = _milliseconds(deadline)
        if mode == "drain" and not (self._stop_requested and not self._draining):
            self._draining = True
            end = self._clock() + delay_ms / 1000
            self._drain_deadline = min(self._drain_deadline, end) if self._drain_deadline is not None else end
            self._native.drain_all(delay_ms)
        else:
            self._draining = False
            self._native.cancel_all()
        self._stop_requested = True

    def inspect(self) -> dict:
        self._check_process()
        result = self._native.snapshot()
        result["python_registrations"] = len(self._actors)
        result["schemas"] = len(self._schemas)
        return result

    def stop_report(self, ref: ActorRef | None = None) -> dict:
        """Read current shutdown work and lifetime failures without changing state."""
        self._check_process()
        if ref is not None and not isinstance(ref, ActorRef):
            raise TypeError("stop_report requires an ActorRef or None")
        report = self._native.stop_report(ref.handle if ref else None)
        if ref is None and self._closed:
            report["timed_out"] |= self._last_report["timed_out"]
        return report

    def diagnostics(self, *, after_sequence: int = 0, limit: int = 100) -> dict:
        """Read the bounded native diagnostic ring without invoking Python callbacks."""
        self._check_process()
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError("after_sequence must be a nonnegative integer")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        return self._native.diagnostics(after_sequence, limit)

    def close(self, timeout: float = 1.0, *, mode: str | None = None) -> dict:
        self._check_driver()
        if self._dispatching or self._running:
            raise AffinityError("close cannot block inside dispatch; use request_stop")
        if self._closed:
            return self._last_report
        timeout_ms = _milliseconds(timeout)
        had_python_work = bool(self._actors)
        discarded_before = self._native.snapshot()["metrics"]["discarded"]
        if mode is None:
            mode = "drain" if self._draining else "cancel"
        end = self._clock() + timeout_ms / 1000
        if self._drain_deadline is not None:
            end = min(end, self._drain_deadline)
        self.request_stop(mode=mode, deadline=max(0.0, end - self._clock()))
        error = None
        try:
            if self._draining:
                while self._actors and self._clock() < end:
                    if self._pump() == 0 and self._actors:
                        self._idle_wait(self._idle_delay(end))
        except BaseException as exc:
            error = exc
        finally:
            self._native.close(max(0, math.floor((end - self._clock()) * 1000)))
            try:
                self._cleanup()
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note(f"Additional cleanup error: {str(cleanup_error)[:1024]}")
            finally:
                self._last_report = self._native.close(0)
                self._last_report["discarded"] = (
                    self._native.snapshot()["metrics"]["discarded"] - discarded_before)
                # Python hooks are cooperative. Record an overrun after they
                # return; a deadline never means that executing code was killed.
                self._last_report["timed_out"] |= had_python_work and self._clock() >= end
                with self._schema_lock:
                    self._schemas.clear()
                    self._codec_index.clear()
                    self._closed = True
        if error is not None:
            raise error
        return self._last_report

    def __enter__(self) -> World:
        self._check_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if exc is None:
            self.close()
        else:
            try:
                self.close()
            except BaseException as cleanup_error:
                exc.add_note(f"Additional close error: {str(cleanup_error)[:1024]}")
