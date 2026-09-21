import pytest
from actorplane import World, Actor, Pulse, MessageOptions, TraceContext, handles

def test_context_send_forwards_envelope_metadata_and_readonly_snapshot():
    seen = []
    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)
            with pytest.raises(AttributeError): ctx.envelope = None
    with World() as world:
        source = world.spawn(Actor); target = world.spawn(Target); world.step()
        options = MessageOptions(timeout=.2, correlation_id=7, causation_id=8,
                                 trace=TraceContext(b"1" * 16, b"2" * 8))
        world.send(target, Pulse(3), options=options, _source=source)
        world.step()
    assert seen and seen[0].source.handle == source.handle
    assert seen[0].destination.handle == target.handle
    assert seen[0].correlation_id == 7 and seen[0].causation_id == 8
    assert seen[0].trace.trace_id == b"1" * 16

def test_zero_timeout_is_rejected_at_admission():
    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    with World() as world:
        source = world.spawn(Actor); target = world.spawn(Target); world.step()
        with pytest.raises(RuntimeError):
            world.send(target, Pulse(1), options=MessageOptions(timeout=0), _source=source)
