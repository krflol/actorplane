"""Compiled event schemas and a bounded, canonical binary representation."""
from __future__ import annotations

from collections import ChainMap
from dataclasses import InitVar, dataclass, fields, is_dataclass
from enum import Enum
import math
import struct
import sys
import types
from typing import Annotated, Any, Union, get_args, get_origin, get_type_hints

MAX_BYTES = 1 << 20
MAX_NODES = 4096
CANONICAL_NAN = 0x7FF8000000000000


class CodecError(ValueError):
    """A schema or value violates its declared conversion contract."""


class Event:
    """Optional base for classes declared with @event."""


@dataclass(frozen=True)
class IntRange:
    min: int
    max: int


@dataclass(frozen=True)
class FloatPolicy:
    allow_non_finite: bool = False


@dataclass(frozen=True)
class Length:
    max_length: int


def _error(path, reason):
    # Paths and type names are metadata, never payload values.
    return CodecError(f"{path[:256]}: {reason[:256]}")


def _name(value, limit, path):
    if type(value) is not str or not value or len(value) > limit:
        raise _error(path, f"expected name of 1..{limit} UTF-8 bytes")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        raise _error(path, "expected valid UTF-8 name") from None
    if size > limit:
        raise _error(path, f"name exceeds {limit} UTF-8 bytes")
    return value


@dataclass(frozen=True)
class _Node:
    kind: str
    typ: Any = None
    args: tuple = ()
    minimum: int | None = None
    maximum: int | None = None
    limit: int | None = None
    allow_non_finite: bool = False
    enum_names: tuple[str, ...] = ()
    descriptor: tuple = ()


def _compile(typ, path, depth=0):
    if depth > 16:
        raise _error(path, "schema depth exceeds 16")
    metadata = []
    while get_origin(typ) is Annotated:
        base, *extra = get_args(typ)
        typ = base
        metadata.extend(extra)
    origin, args = get_origin(typ), get_args(typ)
    allowed = (IntRange if typ is int else FloatPolicy if typ is float else
               Length if typ in (str, bytes) or origin is tuple else None)
    if len(metadata) > 1 or any(type(item) is not allowed for item in metadata):
        raise _error(path, "unsupported, duplicate, or inapplicable annotation metadata")
    meta = metadata[0] if metadata else None
    if origin in (Union, types.UnionType):
        present = [item for item in args if item is not type(None)]
        if len(args) != 2 or len(present) != 1:
            raise _error(path, "expected Optional[T]; general unions are unsupported")
        child = _compile(present[0], path, depth + 1)
        return _Node("optional", args=(child,), descriptor=("optional", child.descriptor))
    if origin is tuple:
        if len(args) != 2 or args[1] is not Ellipsis:
            raise _error(path, "expected variable tuple[T, ...]")
        limit = meta.max_length if meta else 4096
        if type(limit) is not int or not 0 <= limit <= 4096:
            raise _error(path, "sequence length must be in 0..4096")
        child = _compile(args[0], path + "[]", depth + 1)
        return _Node("sequence", args=(child,), limit=limit,
                     descriptor=("sequence", child.descriptor, limit))
    if typ is bool:
        return _Node("bool", typ=bool, descriptor=("bool",))
    if typ is int:
        bounds = meta or IntRange(-(1 << 63), (1 << 63) - 1)
        lower, upper = bounds.min, bounds.max
        if (type(lower) is not int or type(upper) is not int or lower > upper or
                lower < -(1 << 63) or upper >= 1 << 64 or
                (lower < 0 and upper >= 1 << 63)):
            raise _error(path, "integer range must fit signed or unsigned 64-bit storage")
        kind = "uint" if upper >= 1 << 63 else "int"
        return _Node(kind, typ=int, minimum=lower, maximum=upper,
                     descriptor=(kind, lower, upper))
    if typ is float:
        allow = meta.allow_non_finite if meta else False
        if type(allow) is not bool:
            raise _error(path, "float policy must be bool")
        return _Node("float", typ=float, allow_non_finite=allow,
                     descriptor=("float", allow))
    if typ in (str, bytes):
        limit = meta.max_length if meta else 4096
        if type(limit) is not int or not 0 <= limit <= MAX_BYTES:
            raise _error(path, "field byte limit must be in 0..1048576")
        kind = "string" if typ is str else "bytes"
        return _Node(kind, typ=typ, limit=limit, descriptor=(kind, limit))
    if isinstance(typ, type) and issubclass(typ, Enum):
        members = typ.__members__
        if not 1 <= len(members) <= 256 or len(members) != len(typ):
            raise _error(path, "enum requires 1..256 distinct symbols without aliases")
        name = _name(f"{typ.__module__}.{typ.__qualname__}", 256, path)
        names = tuple(sorted(_name(name, 256, path) for name in members))
        return _Node("enum", typ=typ, enum_names=names,
                     descriptor=("enum", name, names))
    plan = getattr(typ, "__actorplane_schema__", None) if isinstance(typ, type) else None
    if isinstance(plan, SchemaPlan) and plan.typ is typ:
        return plan._root
    raise _error(path, "unsupported annotation; expected a declared native field type")


def _validate_tree(root, path):
    nodes = 0
    identities = {}

    def visit(node, depth, path):
        nonlocal nodes
        nodes += 1
        if nodes > 1024 or depth > 16:
            raise _error(path, "schema exceeds 1024 nodes or depth 16")
        if node.kind == "record":
            key = node.descriptor[1:3]
            if key in identities and identities[key] != node.descriptor:
                raise _error(path, "nested schema identity collision")
            identities[key] = node.descriptor
            for name, child in node.args:
                visit(child, depth + 1, path + "." + name)
        elif node.kind in ("optional", "sequence"):
            visit(node.args[0], depth + 1, path + "[]")

    visit(root, 0, path)


class _Writer:
    def __init__(self, limit):
        self.data = bytearray()
        self.limit = limit
        self.nodes = 0

    def require(self, size, path):
        if size > self.limit - len(self.data):
            raise _error(path, f"encoded event exceeds {self.limit} bytes")

    def add(self, data, path):
        self.require(len(data), path)
        self.data.extend(data)

    def node(self, path):
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise _error(path, "value exceeds 4096 nodes")


def _put(writer, node, value, path):
    writer.node(path)
    kind = node.kind
    actual = type(value).__name__[:128]
    if kind in ("int", "uint"):
        if type(value) is not int or not node.minimum <= value <= node.maximum:
            raise _error(path, f"expected integer in {node.minimum}..{node.maximum}; got {actual}")
        writer.add(struct.pack("<q" if kind == "int" else "<Q", value), path)
    elif kind == "bool":
        if type(value) is not bool:
            raise _error(path, f"expected bool; got {actual}")
        writer.add(bytes((value,)), path)
    elif kind == "float":
        if type(value) is not float:
            raise _error(path, f"expected float64; got {actual}")
        if not node.allow_non_finite and not math.isfinite(value):
            raise _error(path, "expected finite float64")
        encoded = (struct.pack("<Q", CANONICAL_NAN) if math.isnan(value)
                   else struct.pack("<d", value))
        writer.add(encoded, path)
    elif kind in ("string", "bytes"):
        writer.require(4, path)
        limit = min(node.limit, writer.limit - len(writer.data) - 4)
        if kind == "string":
            if type(value) is not str:
                raise _error(path, f"expected UTF-8 string; got {actual}")
            # Bound source characters before UTF-8 conversion. Temporary bytes
            # are at most four times the configured remaining field budget.
            if len(value) > limit:
                raise _error(path, f"string exceeds {limit} UTF-8 bytes")
            try:
                raw = value.encode("utf-8")
            except UnicodeError:
                raise _error(path, "expected valid UTF-8 string") from None
        else:
            if type(value) not in (bytes, bytearray, memoryview):
                raise _error(path, f"expected bytes or a byte buffer; got {actual}")
            try:
                size = value.nbytes if type(value) is memoryview else len(value)
                if size > limit:
                    raise _error(path, f"buffer exceeds {limit} bytes")
                raw = bytes(value)
            except (ValueError, BufferError) as exc:
                if isinstance(exc, CodecError):
                    raise
                raise _error(path, "expected readable byte buffer") from None
        if len(raw) > limit:
            raise _error(path, f"field exceeds {limit} bytes")
        writer.add(struct.pack("<I", len(raw)), path)
        writer.add(raw, path)
    elif kind == "enum":
        if type(value) is not node.typ:
            raise _error(path, f"expected enum {node.typ.__name__[:128]}; got {actual}")
        if value.name not in node.enum_names:
            raise _error(path, "expected a declared enum symbol")
        writer.add(struct.pack("<I", node.enum_names.index(value.name)), path)
    elif kind == "optional":
        writer.add(b"\0" if value is None else b"\1", path)
        if value is not None:
            _put(writer, node.args[0], value, path)
    elif kind == "sequence":
        if type(value) not in (tuple, list) or len(value) > node.limit:
            raise _error(path, f"expected sequence of at most {node.limit} items; got {actual}")
        if len(value) > MAX_NODES - writer.nodes:
            raise _error(path, "sequence exceeds remaining value-node budget")
        writer.add(struct.pack("<I", len(value)), path)
        for index, item in enumerate(value):
            _put(writer, node.args[0], item, f"{path}[{index}]")
    elif kind == "record":
        if type(value) is not node.typ:
            raise _error(path, f"expected event {node.typ.__name__[:128]}; got {actual}")
        for name, child in node.args:
            _put(writer, child, getattr(value, name), path + "." + name)


class _Reader:
    def __init__(self, data):
        self.data = data
        self.position = 0
        self.nodes = 0

    def take(self, count, path):
        if count > len(self.data) - self.position:
            raise _error(path, "truncated canonical value")
        start = self.position
        self.position += count
        return self.data[start:self.position]

    def node(self, path):
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise _error(path, "value exceeds 4096 nodes")


def _get(reader, node, path):
    reader.node(path)
    kind = node.kind
    if kind in ("int", "uint"):
        value = struct.unpack("<q" if kind == "int" else "<Q", reader.take(8, path))[0]
        if not node.minimum <= value <= node.maximum:
            raise _error(path, f"expected integer in {node.minimum}..{node.maximum}")
        return value
    if kind == "bool":
        value = reader.take(1, path)[0]
        if value > 1:
            raise _error(path, "expected canonical bool tag 0 or 1")
        return bool(value)
    if kind == "float":
        raw = reader.take(8, path)
        value = struct.unpack("<d", raw)[0]
        if not node.allow_non_finite and not math.isfinite(value):
            raise _error(path, "expected finite float64")
        if math.isnan(value) and struct.unpack("<Q", raw)[0] != CANONICAL_NAN:
            raise _error(path, "expected canonical NaN")
        return value
    if kind in ("string", "bytes"):
        size = struct.unpack("<I", reader.take(4, path))[0]
        if size > node.limit:
            raise _error(path, f"field exceeds {node.limit} bytes")
        raw = reader.take(size, path)
        if kind == "bytes":
            return raw
        try:
            return raw.decode("utf-8")
        except UnicodeError:
            raise _error(path, "expected valid UTF-8 string") from None
    if kind == "enum":
        index = struct.unpack("<I", reader.take(4, path))[0]
        if index >= len(node.enum_names):
            raise _error(path, "expected registered enum symbol index")
        return node.typ[node.enum_names[index]]
    if kind == "optional":
        present = reader.take(1, path)[0]
        if present > 1:
            raise _error(path, "expected optional tag 0 or 1")
        return _get(reader, node.args[0], path) if present else None
    if kind == "sequence":
        size = struct.unpack("<I", reader.take(4, path))[0]
        if size > node.limit or size > MAX_NODES - reader.nodes:
            raise _error(path, "sequence exceeds field or remaining value-node limit")
        return tuple(_get(reader, node.args[0], f"{path}[{i}]") for i in range(size))
    if kind == "record":
        return node.typ(**{name: _get(reader, child, path + "." + name)
                           for name, child in node.args})
    raise _error(path, "unsupported compiled node")


@dataclass(frozen=True)
class SchemaPlan:
    typ: type
    descriptor: tuple
    _root: _Node

    def encode(self, value, max_bytes=32768):
        if type(max_bytes) is not int or not 0 <= max_bytes <= MAX_BYTES:
            raise ValueError("max_bytes must be in 0..1048576")
        writer = _Writer(max_bytes)
        _put(writer, self._root, value, self.descriptor[1])
        return bytes(writer.data)

    def decode(self, data):
        if type(data) is not bytes or len(data) > MAX_BYTES:
            raise _error(self.descriptor[1], "expected canonical bytes within 1048576-byte limit")
        reader = _Reader(data)
        value = _get(reader, self._root, self.descriptor[1])
        if reader.position != len(data):
            raise _error(self.descriptor[1], "unexpected trailing encoded fields")
        return value


def event(name: str, version: int = 1):
    """Compile a frozen declaration without a runtime or global class registry."""
    _name(name, 256, "event")
    if type(version) is not int or not 1 <= version < 1 << 32:
        raise _error(name, "version must be an integer in 1..4294967295")

    def decorate(cls):
        if not is_dataclass(cls) or "__dataclass_fields__" not in vars(cls):
            cls = dataclass(frozen=True)(cls)
        elif not cls.__dataclass_params__.frozen:
            raise _error(name, "event dataclass must be frozen")
        frame = sys._getframe(1)
        try:
            localns = ChainMap({cls.__name__: cls}, vars(cls), frame.f_locals)
            hints = get_type_hints(cls, localns=localns, include_extras=True)
        except (NameError, TypeError) as exc:
            raise _error(name, "unresolved or recursive field annotation") from exc
        finally:
            del frame
        declarations = fields(cls)
        if any(isinstance(annotation, InitVar) or annotation is InitVar for annotation in hints.values()):
            raise _error(name, "InitVar fields are not part of a reconstructible event schema")
        if len(declarations) > 256:
            raise _error(name, "event exceeds 256 fields")
        layout = []
        for field in declarations:
            path = name + "." + field.name
            _name(field.name, 128, path)
            if not field.init:
                raise _error(path, "event field must participate in construction")
            layout.append((field.name, _compile(hints[field.name], path, 1)))
        descriptor = ("record", name, version, tuple((key, node.descriptor) for key, node in layout))
        root = _Node("record", typ=cls, args=tuple(layout), descriptor=descriptor)
        _validate_tree(root, name)
        cls.__actorplane_schema__ = SchemaPlan(cls, descriptor, root)
        cls._event_name, cls._event_version = name, version
        return cls
    return decorate
