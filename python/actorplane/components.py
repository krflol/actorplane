"""Immutable component declarations and typed, non-owning port references."""
from __future__ import annotations
from dataclasses import dataclass, field
import math
from types import MappingProxyType
from typing import Any
from .authoring import Actor, ActorRef, LifecycleError
from .schemas import SchemaPlan


def _name(value):
    if type(value) is not str or not value or len(value) > 128 or len(value.encode()) > 128:
        raise ValueError("contract names must be 1..128 UTF-8 bytes")
    return value


def _event(typ):
    plan = getattr(typ, "__actorplane_schema__", None)
    if not isinstance(plan, SchemaPlan) or plan.typ is not typ:
        raise TypeError("ports require an @event class")
    return typ


@dataclass(frozen=True)
class Interface:
    """FIFO admission, reject-on-full, cancel-unclaimed-on-owner-stop contract."""
    name: str
    version: int = 1
    inputs: tuple[tuple[str, type], ...] = ()
    outputs: tuple[tuple[str, type], ...] = ()

    def __post_init__(self):
        _name(self.name)
        if type(self.version) is not int or not 1 <= self.version < 1 << 32:
            raise ValueError("interface version must be a positive uint32")
        seen = set()
        for ports in (self.inputs, self.outputs):
            if type(ports) is not tuple or len(ports) > 64:
                raise TypeError("interface ports must be a bounded tuple")
            for item in ports:
                if type(item) is not tuple or len(item) != 2:
                    raise TypeError("interface port is a (name, event_type) pair")
                name, typ = item
                _name(name)
                _event(typ)
                if name in seen:
                    raise ValueError("duplicate interface port name")
                seen.add(name)
        if len(seen) > 64:
            raise ValueError("at most 64 interface ports")
        object.__setattr__(self, "inputs", tuple(sorted(self.inputs, key=lambda port: port[0])))
        object.__setattr__(self, "outputs", tuple(sorted(self.outputs, key=lambda port: port[0])))


class Component(Actor):
    """Fresh Python state; all callbacks run on the World driver thread."""
    implements: tuple[Interface, ...] = ()


@dataclass(frozen=True, eq=False)
class ComponentDescriptor:
    factory: type
    config: tuple[tuple[str, Any], ...]
    depends_on: tuple[str, ...] = ()
    requires: tuple[Interface, ...] = ()
    name: str = field(default="", init=False)

    def __set_name__(self, owner, name):
        _name(name)
        if self.name and self.name != name:
            raise TypeError("one descriptor cannot name two components")
        object.__setattr__(self, "name", name)

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        try:
            return instance.__actorplane_components__[self.name]
        except (AttributeError, KeyError):
            raise LifecycleError("component is not bound until startup configuration") from None

    def __set__(self, instance, value):
        raise AttributeError("component declarations are immutable")


@dataclass(frozen=True, eq=False)
class Output:
    event_type: type
    name: str = field(default="", init=False)

    def __post_init__(self):
        _event(self.event_type)

    def __set_name__(self, owner, name):
        _name(name)
        if self.name and self.name != name:
            raise TypeError("one descriptor cannot name two output ports")
        object.__setattr__(self, "name", name)

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        try:
            return instance.__actorplane_bound_ports__[self.name]
        except (AttributeError, KeyError):
            raise LifecycleError("output is not bound until startup configuration") from None

    def __set__(self, instance, value):
        raise AttributeError("output declarations are immutable")


def _freeze(value, depth, nodes):
    nodes[0] += 1
    if depth > 8 or nodes[0] > 1024:
        raise ValueError("component configuration exceeds depth/node limits")
    if type(value) in (str, bytes):
        if len(value) > 4096 or (type(value) is str and len(value.encode()) > 4096):
            raise ValueError("component configuration string/buffer exceeds 4096 bytes")
    elif type(value) is int:
        if not -(1 << 63) <= value < 1 << 64:
            raise ValueError("component configuration integer exceeds 64-bit range")
    elif type(value) is float:
        if not math.isfinite(value):
            raise ValueError("component configuration float must be finite")
    elif type(value) is tuple:
        if len(value) > 1024:
            raise ValueError("component configuration tuple is too long")
        return tuple(_freeze(item, depth + 1, nodes) for item in value)
    elif value is not None and type(value) is not bool:
        raise TypeError("component configuration requires immutable primitives or tuples")
    return value


def component(factory: type, *, depends_on=(), requires=(), **config):
    if not isinstance(factory, type) or not (issubclass(factory, Component) or getattr(factory, "__actorplane_native_kind__", None)):
        raise TypeError("component requires a Component class or native component factory")
    if len(config) > 64:
        raise ValueError("at most 64 component configuration fields")
    if type(depends_on) is not tuple or len(depends_on) > 64:
        raise TypeError("depends_on must be a bounded tuple of component names")
    for name in depends_on:
        _name(name)
    if len(set(depends_on)) != len(depends_on):
        raise ValueError("duplicate component dependency")
    if type(requires) is not tuple or len(requires) > 16 or any(type(i) is not Interface for i in requires):
        raise TypeError("requires must be a bounded tuple of Interface contracts")
    nodes = [0]
    frozen = tuple((_name(key), _freeze(value, 0, nodes)) for key, value in sorted(config.items()))
    return ComponentDescriptor(factory, frozen, depends_on, requires)


def _members(cls):
    members = {}
    for base in reversed(cls.__mro__):
        members.update(vars(base))
    return members


def outputs(cls):
    return tuple((n, v) for n, v in _members(cls).items() if isinstance(v, Output))


def port_declarations(cls):
    if getattr(cls, "__actorplane_native_kind__", None):
        return cls.__actorplane_ports__
    ports = tuple((name, "input", typ) for typ, name in cls.__actorplane_handlers__.items())
    ports += tuple((name, "output", declaration.event_type) for name, declaration in outputs(cls))
    if len(ports) > 64 or len({p[0] for p in ports}) != len(ports):
        raise ValueError("ports require unique names and at most 64 entries")
    return ports


def interfaces(cls):
    contracts = getattr(cls, "implements", ())
    if type(contracts) is not tuple or len(contracts) > 16 or any(type(i) is not Interface for i in contracts):
        raise TypeError("implements must be a bounded tuple of Interface contracts")
    if len({(i.name, i.version) for i in contracts}) != len(contracts):
        raise ValueError("duplicate implemented interface")
    ports = port_declarations(cls)
    for contract in contracts:
        for direction, items in (("input", contract.inputs), ("output", contract.outputs)):
            for name, typ in items:
                if (name, direction, typ) not in ports:
                    raise ValueError(f"interface {contract.name} requires matching port {name}")
    return contracts


def declarations(cls):
    found = {n: v for n, v in _members(cls).items() if isinstance(v, ComponentDescriptor)}
    if len(found) > 64 or len({id(d) for d in found.values()}) != len(found):
        raise ValueError("at most 64 distinct component declarations")
    visiting, done, ordered = set(), set(), []

    def visit(name):
        if name in visiting:
            raise ValueError("component dependency cycle")
        if name in done:
            return
        visiting.add(name)
        descriptor = found[name]
        interfaces(descriptor.factory)
        available = []
        for dependency in descriptor.depends_on:
            if dependency not in found:
                raise ValueError(f"missing component dependency {dependency}")
            visit(dependency)
            available.extend(interfaces(found[dependency].factory))
        for required in descriptor.requires:
            if required not in available:
                raise ValueError(f"missing required interface {required.name} v{required.version}")
        visiting.remove(name)
        done.add(name)
        ordered.append((name, descriptor))

    for name in found:
        visit(name)
    return tuple(ordered)


@dataclass(frozen=True)
class Port:
    owner: ActorRef
    name: str
    schema: type
    index: int
    direction: str

    @property
    def handle(self):
        return (*self.owner.handle, self.index)

    def _runtime(self):
        world = self.owner._world() if self.owner._world else None
        if world is None:
            raise LifecycleError("port's World no longer exists")
        return world

    def send(self, event, *, options=None):
        return self._runtime()._send_port(self, event, options=options)

    def emit(self, event, *, options=None):
        return self._runtime()._publish_port(self, event, options=options)


@dataclass(frozen=True)
class LinkToken:
    identifier: int
    owner: ActorRef

    def cancel(self):
        world = self.owner._world() if self.owner._world else None
        if world is None:
            return False
        world._check_process()
        try:
            world._native.unsubscribe(self.identifier)
        except RuntimeError as error:
            if str(error) == "NotFound":
                return False
            raise
        return True


@dataclass(frozen=True)
class BoundComponent:
    owner: ActorRef
    ports: Any = field(default_factory=tuple)
    _native: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        object.__setattr__(self, "ports", MappingProxyType(dict(self.ports)))

    def __getattr__(self, name):
        try:
            return self.ports[name]
        except KeyError:
            raise AttributeError(name) from None

    def stats(self):
        if self._native is None:
            raise LifecycleError("Python components expose state through messages")
        return self._native.stats()
