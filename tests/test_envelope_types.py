import pytest
from actorplane.envelopes import TraceContext, MessageOptions, Envelope

def test_trace_context_strict_bounds_and_options_native():
    trace = TraceContext(b"1"*16,b"2"*8,True,(("x-y","ok"),))
    assert MessageOptions(.001, 1, 2, trace)._native((1,2,3)) [1] == 1
    with pytest.raises(ValueError): TraceContext(b"\0"*16,b"2"*8)
    with pytest.raises(TypeError): TraceContext(b"1"*16,b"2"*8,1)
    with pytest.raises(ValueError): TraceContext(b"1"*16,b"2"*8,False,(("bad key","x"),))

def test_options_reject_bool_nonfinite_and_duplicate_baggage():
    with pytest.raises(ValueError): MessageOptions(True)
    with pytest.raises(ValueError): MessageOptions(float("inf"))
    with pytest.raises(ValueError): TraceContext(b"1"*16,b"2"*8,baggage=(("x","a"),("x","b")))
    with pytest.raises(TypeError): TraceContext(b"1"*16,b"2"*8,baggage=[("x","a")])
    with pytest.raises(ValueError): MessageOptions(10**1000)

def test_envelope_is_frozen_and_native_snapshot_rebuilds_refs():
    from actorplane import World, Actor
    with World() as world:
        a=world.spawn(Actor); world.step()
        value={"event_id":1,"schema_kind":"record","schema_id":1,"schema_version":1,"source":None,"destination":a.handle,"dispatcher":None,"destination_port":None,"owner":a.handle,"enqueued_at_ns":1}
        envelope=Envelope._from_native(value,world)
        assert envelope.destination.handle == a.handle
        with pytest.raises(Exception): envelope.event_id=2
    with pytest.raises(ValueError): MessageOptions(correlation_id=0)


def test_envelope_integer_handles_and_unicode_bounds_are_strict():
    from dataclasses import replace
    from actorplane import ActorRef

    reference = ActorRef(1, 0, 1)
    envelope = Envelope(1, "pulse", 0, 1, None, reference, None, None, reference, 0)
    for changes in (
        {"schema_id": 2**32}, {"schema_version": 2**32},
        {"event_id": 2**64}, {"event_id": True},
        {"source": ActorRef(1, -1, 1)},
        {"destination_port": (1, 0, 1, 2**16)},
    ):
        with pytest.raises((ValueError, TypeError)):
            replace(envelope, **changes)
    with pytest.raises(ValueError):
        TraceContext(b"a" * 16, b"b" * 8, baggage=(("key", "\ud800"),))
    with pytest.raises(ValueError):
        TraceContext(b"a" * 16, b"b" * 8, baggage=(("key", "é" * 129),))
