import math
from enum import Enum
from typing import Annotated, Optional
import pytest
from actorplane.schemas import event, Event, CodecError, IntRange, Length, FloatPolicy

@event("schema.Basic", 1)
class Basic(Event):
    count: Annotated[int, IntRange(-5, 20)]
    label: str
    values: tuple[int, ...]

def test_descriptor_and_roundtrip_snapshot():
    plan = Basic.__actorplane_schema__
    assert plan.descriptor[0:3] == ("record", "schema.Basic", 1)
    original = Basic(4, "ok", [1, 2])
    encoded = plan.encode(original)
    decoded = plan.decode(encoded)
    assert decoded == Basic(4, "ok", (1, 2))

def test_strict_bounds_and_size():
    with pytest.raises(CodecError): Basic(True, "ok", ()).__class__.__actorplane_schema__.encode(Basic(True, "ok", ()))
    with pytest.raises(CodecError): Basic.__actorplane_schema__.encode(Basic(99, "ok", ()))
    with pytest.raises(CodecError): Basic(1, "x" * 5000, ()).__class__.__actorplane_schema__.encode(Basic(1, "x" * 5000, ()))

def test_optional_enum_float_and_nested_record():
    class Color(Enum): BLUE=1; RED=2
    @event("schema.Nested", 1)
    class Nested(Event):
        color: Color
        maybe: Optional[int]
        amount: Annotated[float, FloatPolicy(True)]
    value = Nested(Color.RED, None, float("nan"))
    decoded = Nested.__actorplane_schema__.decode(Nested.__actorplane_schema__.encode(value))
    assert decoded.color is Color.RED and decoded.maybe is None and math.isnan(decoded.amount)

def test_unsupported_annotation_rejected():
    with pytest.raises(CodecError):
        @event("schema.Bad", 1)
        class Bad(Event):
            value: dict

def test_nested_event_and_exact_enum_type_decode():
    class Shade(Enum): LIGHT = 1; DARK = 2
    @event("schema.Inner", 1)
    class Inner(Event): shade: Shade
    @event("schema.Outer", 1)
    class Outer(Event): inner: Inner
    value = Outer(Inner(Shade.DARK))
    decoded = Outer.__actorplane_schema__.decode(Outer.__actorplane_schema__.encode(value))
    assert type(decoded.inner) is Inner and decoded.inner.shade is Shade.DARK

def test_decode_rejects_noncanonical_bool_and_trailing_bytes():
    @event("schema.Flag", 1)
    class Flag(Event): enabled: bool
    plan = Flag.__actorplane_schema__
    with pytest.raises(CodecError): plan.decode(b"\x02")
    with pytest.raises(CodecError): plan.decode(b"\x00\x00")
