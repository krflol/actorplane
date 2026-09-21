import math
from enum import Enum
from typing import Annotated, Optional

import pytest

from actorplane import Actor, CodecError, Event, World, handles, _native
from actorplane.schemas import FloatPolicy, IntRange, Length, event


class Kind(Enum):
    ALPHA = 1
    OMEGA = 2


@event("integration.Child", 1)
class Child(Event):
    text: str
    values: tuple[int, ...]


@event("integration.Rich", 1)
class Rich(Event):
    signed: Annotated[int, IntRange(-4, 4)]
    unsigned: Annotated[int, IntRange(0, (1 << 64) - 1)]
    ratio: Annotated[float, FloatPolicy(True)]
    enabled: bool
    name: Annotated[str, Length(32)]
    blob: Annotated[bytes, Length(32)]
    kind: Kind
    maybe: Optional[int]
    nested: tuple[int, ...]


@event("integration.Container", 1)
class Container(Event):
    records: tuple[Child, ...]
    rich: Rich
    tail: bool


def sample(blob=b"sample"):
    return Rich(-2, (1 << 64) - 1, -0.0, True, "name", blob, Kind.OMEGA, None, (1, 2))


def test_rich_schema_roundtrip_and_mutation_snapshot():
    value = Rich(-2, 7, float("nan"), True, "ok", bytearray(b"bytes"), Kind.OMEGA, None, [1, 2])
    encoded = Rich.__actorplane_schema__.encode(value)
    decoded = Rich.__actorplane_schema__.decode(encoded)
    assert decoded.enabled and decoded.kind is Kind.OMEGA and decoded.maybe is None
    assert math.isnan(decoded.ratio) and decoded.nested == (1, 2)
    mutable = bytearray(b"abc")
    snap = Rich(1, 2, 0.5, False, "x", mutable, Kind.ALPHA, 3, [4])
    encoded = Rich.__actorplane_schema__.encode(snap)
    mutable[:] = b"zzz"
    assert Rich.__actorplane_schema__.decode(encoded).blob == b"abc"


def test_rich_record_request_reply_and_after():
    seen, references = [], {}

    class Sink(Actor):
        @handles(Container)
        def receive(self, event, ctx):
            seen.append(event)
            if ctx.operation is not None:
                assert ctx.reply(event)

        def on_start(self, ctx):
            ctx.after(.01, Container((Child("timer", (3,)),), sample(), False))

    value = Container([Child("request", [4, 5])], sample(bytearray(b"original")), True)

    with World() as world:
        ref = world.spawn(Sink)
        owner = world.spawn(Actor)
        world.step()
        result = world.request(owner, ref, value)
        value.records[0].values.append(9)
        value.rich.blob[:] = b"modified"
        world.step()
        reply = result.result()
        assert reply.records == (Child("request", (4, 5)),)
        assert reply.rich.blob == b"original"
        assert reply.rich.unsigned == (1 << 64) - 1
        assert math.copysign(1, reply.rich.ratio) == -1
        world.run_for(.1)
        assert any(record.records[0].text == "timer" for record in seen)
        assert world.inspect()["native_schemas"] == 1


def test_failed_schema_registration_does_not_consume_world_capacity():
    @event("integration.First")
    class First: value: bool
    @event("integration.Second")
    class Second: value: str
    class TooMany(Actor):
        @handles(First)
        def first(self, event, ctx): pass
        @handles(Second)
        def second(self, event, ctx): pass
    class One(Actor):
        @handles(First)
        def first(self, event, ctx): pass
    with World(max_schemas=1) as world:
        with pytest.raises(CodecError): world.spawn(TooMany)
        assert world.inspect()["schemas"] == 0
        assert world.inspect()["native_schemas"] == 0
        assert world.inspect()["python_registrations"] == 0
        world.spawn(One)
        assert world.inspect()["native_schemas"] == 1


def test_nested_identity_conflict_is_rejected_atomically_at_world_registration():
    @event("integration.Shared")
    class Shared: value: bool
    @event("integration.Shared")
    class Conflict: value: str
    @event("integration.OwnerA")
    class OwnerA: nested: Shared
    @event("integration.OwnerB")
    class OwnerB: nested: Conflict
    class A(Actor):
        @handles(OwnerA)
        def consume(self, event, ctx): pass
    class B(Actor):
        @handles(OwnerB)
        def consume(self, event, ctx): pass
    with World(max_schemas=2) as world:
        world.spawn(A)
        with pytest.raises(CodecError, match="collision"): world.spawn(B)
        assert world.inspect()["schemas"] == world.inspect()["native_schemas"] == 1
        assert world.inspect()["python_registrations"] == 1


def test_native_schema_validation_rejects_malformed_data_before_admission():
    world = _native.NativeWorld()
    try:
        actor = world.allocate(False)
        world.activate(actor)
        descriptor = ("record", "Raw.Flag", 1, (("flag", ("bool",)),))
        schema, = world.register_schemas((descriptor,))
        assert world.register_schemas((descriptor,)) == [schema]
        for malformed in (b"", b"\x02", b"\x00\x01"):
            with pytest.raises(ValueError): world.send(actor, "structured", malformed, schema)
        with pytest.raises(ValueError): world.send(actor, "structured", b"\x01", schema + 1)
        assert world.snapshot()["metrics"]["admitted"] == 0
        assert world.snapshot()["retained_payload_bytes"] == 0
        world.send(actor, "structured", b"\x01", schema)
        token, kind, value, observed_schema, *_ = world.claim(actor)
        assert (kind, value, observed_schema) == ("structured", b"\x01", schema)
        world.finish(token)
    finally:
        world.close()


def test_native_timer_delivers_structured_record_while_python_holds_gil():
    seen = []
    class Sink(Actor):
        def on_start(self, ctx): ctx.after(.01, sample())
        @handles(Rich)
        def consume(self, event, ctx): seen.append(event)
    with World() as world:
        world.spawn(Sink)
        world.step()
        _native._hold_gil(100)
        assert seen == []
        assert world.inspect()["metrics"]["admitted"] == 1
        world.step()
        assert seen == [sample()]


def test_every_truncation_and_trailing_byte_is_rejected_by_both_codecs():
    value = Container((Child("prefix", (4, 5)), Child("suffix", ())), sample(), True)
    plan = Container.__actorplane_schema__
    encoded = plan.encode(value)
    native = _native.NativeWorld()
    try:
        schema, = native.register_schemas((plan.descriptor,))
        actor = native.allocate(False)
        native.activate(actor)
        for index in range(len(encoded)):
            with pytest.raises(CodecError): plan.decode(encoded[:index])
            with pytest.raises(ValueError): native.send(actor, "structured", encoded[:index], schema)
        with pytest.raises(CodecError): plan.decode(encoded + b"\0")
        with pytest.raises(ValueError): native.send(actor, "structured", encoded + b"\0", schema)
        assert native.snapshot()["metrics"]["admitted"] == 0
        native.send(actor, "structured", encoded, schema)
        claim = native.claim(actor)
        assert plan.decode(claim[2]) == value
        native.finish(claim[0])
    finally:
        native.close()
