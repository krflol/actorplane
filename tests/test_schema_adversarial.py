import struct
from typing import Annotated, Optional
import pytest
from actorplane.schemas import event, Event, CodecError, IntRange, FloatPolicy, Length

def test_metadata_wrong_type_duplicate_and_unknown_rejected():
    with pytest.raises(CodecError):
        @event("adv.bad", 1)
        class Bad(Event): value: Annotated[int, IntRange(0, 1), IntRange(0, 2)]
    with pytest.raises(CodecError):
        @event("adv.badfloat", 1)
        class BadFloat(Event): value: Annotated[int, FloatPolicy()]
    with pytest.raises(CodecError):
        @event("adv.unknown", 1)
        class Unknown(Event): value: Annotated[int, object()]

def test_version_and_integer_extremes():
    with pytest.raises(CodecError): event("adv.zero", 0)(type("Zero", (Event,), {"value": int}))
    @event("adv.uint", 1)
    class Unsigned(Event): value: Annotated[int, IntRange(0, (1 << 64) - 1)]
    plan = Unsigned.__actorplane_schema__
    assert plan.decode(plan.encode(Unsigned((1 << 64) - 1))).value == (1 << 64) - 1

def test_nested_depth_and_nodes_are_bounded():
    current = int
    for i in range(18):
        current = tuple[current, ...]
    with pytest.raises(CodecError):
        event("adv.deep", 1)(type("Deep", (Event,), {"__annotations__": {"value": current}}))

def test_exact_event_class_is_used_without_global_name_collision():
    @event("adv.same", 1)
    class First(Event): value: int
    @event("adv.same", 1)
    class Second(Event): value: int
    first = First.__actorplane_schema__
    assert type(first.decode(first.encode(First(4)))) is First
    assert type(Second.__actorplane_schema__.decode(Second.__actorplane_schema__.encode(Second(5)))) is Second

def test_decode_rejects_malformed_optional_nan_and_utf8():
    @event("adv.decode", 1)
    class Values(Event): maybe: Optional[int]; amount: float; text: str
    plan = Values.__actorplane_schema__
    with pytest.raises(CodecError): plan.decode(b"\x02")
    with pytest.raises(CodecError, match="amount"):
        plan.decode(b"\x00" + struct.pack("<d", float("inf")) + struct.pack("<I", 0))
    with pytest.raises(CodecError, match="text"):
        plan.decode(b"\x00" + struct.pack("<dI", 1.0, 1) + b"\xff")
    @event("adv.nan")
    class Nonfinite: value: Annotated[float, FloatPolicy(True)]
    with pytest.raises(CodecError, match="NaN"):
        Nonfinite.__actorplane_schema__.decode(struct.pack("<Q", 0x7ff8000000000001))

def test_memoryview_and_utf8_lengths_are_checked_before_copy():
    @event("adv.bytes", 1)
    class Blob(Event): data: Annotated[bytes, Length(4)]; text: Annotated[str, Length(4)]
    plan = Blob.__actorplane_schema__
    with pytest.raises(CodecError): plan.encode(Blob(memoryview(b"12345"), "ok"))
    # len(view) is one element; nbytes is eight. Bounds apply to bytes.
    with pytest.raises(CodecError): plan.encode(Blob(memoryview(b"12345678").cast("Q"), "ok"))
    with pytest.raises(CodecError): plan.encode(Blob(memoryview(b"1234"), "ééééé"))

def test_mutable_nested_values_are_snapshotted():
    @event("adv.nested", 1)
    class Nested(Event): values: tuple[tuple[int, ...], ...]
    values = [[1, 2]]
    # Nested mutable lists are accepted only through the outer sequence and are
    # copied into the immutable encoded representation before caller mutation.
    encoded = Nested.__actorplane_schema__.encode(Nested(values))
    values[0].append(9)
    assert Nested.__actorplane_schema__.decode(encoded).values == ((1, 2),)


def test_schema_and_value_node_limits_agree_at_exact_boundaries():
    leaf = event("adv.Fields")(type("Fields", (), {"__annotations__": {f"v{i}": bool for i in range(254)}}))
    # root + four record nodes + four times 254 primitive nodes = 1021.
    base = {f"branch{i}": leaf for i in range(4)}
    good = event("adv.Exact")(type("Exact", (), {"__annotations__": {**base, "x": bool, "y": bool, "z": bool}}))
    with pytest.raises(CodecError, match="1024"):
        event("adv.Over")(type("Over", (), {"__annotations__": {**base, "x": bool, "y": bool, "z": bool, "over": bool}}))
    @event("adv.Many")
    class Many: values: tuple[bool, ...]
    plan = Many.__actorplane_schema__
    assert len(plan.decode(plan.encode(Many((True,) * 4094))).values) == 4094
    with pytest.raises(CodecError): plan.encode(Many((True,) * 4095))
    with pytest.raises(CodecError): plan.decode(struct.pack("<I", 4095) + b"\1" * 4095)


def test_local_postponed_annotations_and_rejected_recursive_type():
    namespace = {"__name__": __name__, "event": event, "Annotated": Annotated, "Length": Length}
    exec("from __future__ import annotations\ndef declare():\n    @event('adv.Local')\n    class Local:\n        name: Annotated[str, Length(8)]\n    @event('adv.LocalParent')\n    class Parent:\n        child: Local\n    return Local, Parent\n", namespace)
    local, parent = namespace["declare"]()
    value = parent(local("ok"))
    assert parent.__actorplane_schema__.decode(parent.__actorplane_schema__.encode(value)) == value
    with pytest.raises(CodecError):
        @event("adv.Recursive")
        class Recursive: child: "Recursive"


def test_initvar_and_undeclared_flag_symbols_have_codec_errors():
    from dataclasses import InitVar
    from enum import Flag
    with pytest.raises(CodecError, match="InitVar"):
        @event("adv.Init")
        class Bad: extra: InitVar[int]
    class Bits(Flag):
        FIRST = 1
        SECOND = 2
    @event("adv.Flag")
    class FlagEvent: bits: Bits
    with pytest.raises(CodecError, match="bits"):
        FlagEvent.__actorplane_schema__.encode(FlagEvent(Bits.FIRST | Bits.SECOND))
