"""Bounded native TCP listener bindings.

TCP is intentionally unavailable on :class:`TestWorld`: real sockets are
external nondeterministic I/O and are not driven by its virtual clock.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from .authoring import ActorRef, Event, World, _milliseconds, event
from .schemas import Length


@event("actorplane.TcpFrame")
class TcpFrame(Event):
    data: Annotated[bytes, Length(max_length=65536)]


def _world_for(ref: ActorRef, world: World) -> None:
    if not isinstance(ref, ActorRef) or ref.world_id != world._world_id:
        raise ValueError("ActorRef belongs to another World")


def _reject_test_world(world: World) -> None:
    try:
        from .testing import TestWorld
    except ImportError:
        TestWorld = ()
    if isinstance(world, TestWorld):
        raise RuntimeError("TCP is unavailable on TestWorld")


@dataclass(frozen=True)
class TcpListener:
    """A native listener owned by one actor scope."""

    owner: ActorRef
    address: tuple[str, int]
    _world: Any
    _native: Any

    def _runtime(self) -> World:
        world = self._world()
        if world is None:
            raise RuntimeError("listener World no longer exists")
        world._check_process()
        return world

    def stats(self) -> dict:
        world = self._runtime()
        return dict(self._native.stats())

    def connections(self) -> tuple[ActorRef, ...]:
        world = self._runtime()
        return tuple(ActorRef(*handle, _world=self._world) for handle in self._native.connections())

    def close(self, mode: str = "cancel", deadline: float = 1.0) -> dict:
        world = self._runtime()
        world._check_driver()
        if mode not in ("cancel", "drain"):
            raise ValueError("mode must be 'cancel' or 'drain'")
        _milliseconds(deadline)
        return world.stop(self.owner, mode=mode, deadline=deadline)


def listen(
    world: World,
    owner: ActorRef,
    target: ActorRef,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_connections: int = 16,
    max_frame_bytes: int = 16384,
    read_buffer_bytes: int = 1048576,
    write_buffer_bytes: int = 1048576,
    read_timeout: float = 5.0,
    request_timeout: float = 1.0,
    write_timeout: float = 5.0,
) -> TcpListener:
    """Bind a bounded native listener and route frames to ``target``."""

    if not isinstance(world, World):
        raise TypeError("world must be a World")
    _reject_test_world(world)
    world._check_driver()
    world._check_open()
    _world_for(owner, world)
    _world_for(target, world)
    if type(host) is not str or not host:
        raise ValueError("host must be a nonempty string")
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("port must be an integer in 0..65535")
    if type(max_connections) is not int or not 1 <= max_connections <= 1024:
        raise ValueError("max_connections must be in 1..1024")
    if type(max_frame_bytes) is not int or not 1 <= max_frame_bytes <= 65536:
        raise ValueError("max_frame_bytes must be in 1..65536")
    for name, value in (("read_buffer_bytes", read_buffer_bytes), ("write_buffer_bytes", write_buffer_bytes)):
        if type(value) is not int or not 1 <= value <= 64 << 20:
            raise ValueError(f"{name} must be a positive bounded integer")
    durations = tuple(_milliseconds(value, positive=True) for value in (read_timeout, request_timeout, write_timeout))
    world._register(TcpFrame)
    native = world._native.tcp_listen(
        owner.handle, target.handle, host, port, max_connections, max_frame_bytes,
        read_buffer_bytes, write_buffer_bytes, *durations,
    )
    address = tuple(native.address)
    native_owner = native.owner
    listener_owner = ActorRef(*native_owner, _world=world_ref(world))
    return TcpListener(listener_owner, (str(address[0]), int(address[1])), world_ref(world), native)


def world_ref(world: World):
    import weakref
    return weakref.ref(world)


def native_echo(world: World, parent: ActorRef | None = None) -> ActorRef:
    """Create the bounded native TCP echo component."""

    if not isinstance(world, World):
        raise TypeError("world must be a World")
    _reject_test_world(world)
    world._check_driver()
    world._check_open()
    if parent is not None:
        _world_for(parent, world)
    world._register(TcpFrame)
    handle = world._native.tcp_echo(parent.handle if parent else None)
    return ActorRef(*handle, _world=world_ref(world))
