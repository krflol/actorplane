import pytest
from actorplane import Event, Pulse, CountSnapshot, CodecError, Actor, handles, event
from actorplane.authoring import _encode, MAX_RECORD_ELEMENTS
from dataclasses import dataclass
import struct

@event("numbers", 2)
class Numbers(Event):
    first: int
    rest: tuple[int, ...]

def test_event_is_frozen_and_values_are_encoded():
    value = Numbers(4, (1, 2))
    assert _encode(value) == struct.pack("<qIqq", 4, 2, 1, 2)
    with pytest.raises(Exception): value.first = 9

def test_bool_and_overflow_are_rejected():
    with pytest.raises(CodecError): _encode(Numbers(True, ()))
    with pytest.raises(CodecError): _encode(Numbers(1 << 63, ()))

def test_sequence_bound_is_rejected():
    with pytest.raises(CodecError): _encode(Numbers(1, tuple(range(MAX_RECORD_ELEMENTS + 1))))

def test_duplicate_and_async_handlers_rejected():
    class A(Actor):
        @handles(Pulse)
        def on_pulse(self, ctx, value): pass
    assert Pulse in A.__actorplane_handlers__
    with pytest.raises(TypeError):
        class Bad(Actor):
            @handles(Pulse)
            def a(self, ctx, value): pass
            @handles(Pulse)
            def b(self, ctx, value): pass

    with pytest.raises(TypeError):
        class Async(Actor):
            @handles(Pulse)
            async def a(self, ctx, value): pass
