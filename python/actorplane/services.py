"""Explicit, bounded service registration and lease API."""
from __future__ import annotations

from dataclasses import dataclass, field
import weakref
from typing import Any

from .authoring import ActorRef, OperationHandle, World, _milliseconds, _native_options
from .components import Interface, Port


def _world(world: World, ref: ActorRef) -> None:
    if not isinstance(ref, ActorRef) or ref.world_id != world._world_id:
        raise ValueError("ActorRef belongs to another World")


def _descriptor(world: World, contract: Interface) -> tuple:
    if not isinstance(contract, Interface):
        raise TypeError("contract must be an Interface")

    def encoded(name, direction, typ):
        codec = world._register(typ)
        return name, direction, codec.kind, codec.schema

    ports = tuple(encoded(name, "input", typ) for name, typ in contract.inputs) + tuple(
        encoded(name, "output", typ) for name, typ in contract.outputs
    )
    return (contract.name, contract.version, ports, ((contract.name, contract.version, ports),))


@dataclass(frozen=True)
class ServiceRef:
    name: str
    target: ActorRef
    contract: Interface
    _world: Any = field(repr=False, compare=False, hash=False)

    def acquire(self, owner: ActorRef) -> "ServiceLease":
        world = self._world()
        if world is None:
            raise RuntimeError("service World no longer exists")
        world._check_process()
        world._check_driver()
        world._check_open()
        return acquire(world, owner, self.name, self.contract, expected_target=self.target)


@dataclass(frozen=True)
class ServiceLease:
    scope: ActorRef
    owner: ActorRef
    service: ActorRef
    contract: Interface
    _world: Any = field(repr=False, compare=False, hash=False)

    def _runtime(self) -> World:
        world = self._world()
        if world is None:
            raise RuntimeError("service World no longer exists")
        world._check_process()
        return world

    def request(self, port: str, event, *, timeout: float = 1.0, options=None) -> OperationHandle:
        world = self._runtime()
        world._check_driver()
        world._check_open()
        declared = dict(self.contract.inputs)
        if port not in declared or type(event) is not declared[port]:
            raise TypeError("event does not match the service input contract")
        world._register(type(event))
        kind, values, schema = world._encode_event(event)
        return OperationHandle(
            world._native.service_request(self.scope.handle, self.owner.handle, self.service.handle, port, kind, values, schema,
                                          _milliseconds(timeout), _native_options(options, self.scope), world._current_claim),
            weakref.ref(world),
        )

    def link(self, output: str, target: Port):
        world = self._runtime()
        world._check_driver()
        world._check_open()
        if not isinstance(target, Port):
            raise TypeError("service link target must be a Port")
        if target.owner.world_id != world._world_id:
            raise ValueError("ActorRef belongs to another World")
        declared = dict(self.contract.outputs)
        if output not in declared or target.direction != "input" or target.schema is not declared[output]:
            raise TypeError("target does not match the service output contract")
        identifier = world._native.service_link(self.scope.handle, self.owner.handle, self.service.handle, output, target.handle)
        from .components import LinkToken
        return LinkToken(identifier, self.scope)

    def release(self) -> bool:
        world = self._runtime()
        world._check_driver()
        return bool(world._native.release_service(self.scope.handle, self.owner.handle, self.service.handle))


def register(world: World, name: str, target: ActorRef, contract: Interface) -> ServiceRef:
    world._check_process()
    world._check_driver()
    world._check_open()
    _world(world, target)
    if type(name) is not str or not name or len(name.encode("utf-8")) > 128:
        raise ValueError("service name must be a nonempty string")
    descriptor = _descriptor(world, contract)
    world._native.register_service(name, target.handle, descriptor)
    return ServiceRef(name, target, contract, weakref.ref(world))


def acquire(world: World, owner: ActorRef, name: str, contract: Interface,
            *, expected_target: ActorRef | None = None) -> ServiceLease:
    world._check_process()
    world._check_driver()
    world._check_open()
    _world(world, owner)
    if type(name) is not str or not name or len(name.encode("utf-8")) > 128:
        raise ValueError("service name must be a nonempty string")
    if expected_target is not None:
        _world(world, expected_target)
    descriptor = _descriptor(world, contract)
    scope, service = world._native.acquire_service(
        owner.handle, name, descriptor, expected_target.handle if expected_target else None)
    return ServiceLease(ActorRef(*scope, _world=weakref.ref(world)), owner,
                        ActorRef(*service, _world=weakref.ref(world)), contract,
                        weakref.ref(world))
