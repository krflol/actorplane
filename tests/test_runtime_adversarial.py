"""Adversarial public-facade checks for ownership, lifecycle, and codec fences."""
import asyncio
import threading
import time
import pytest

pytest.importorskip("actorplane._native")
from actorplane import World, Actor, Pulse, event, handles, CodecError, HandlerFailed

def test_cross_world_and_stale_stop_are_rejected():
    class A(Actor): pass
    with World() as first, World() as second:
        ref = first.spawn(A); first.step()
        with pytest.raises(RuntimeError): second.stop(ref)
        first.stop(ref)
        replacement = first.spawn(A)
        first.step()
        assert replacement.slot == ref.slot and replacement.generation != ref.generation
        with pytest.raises(RuntimeError): first.stop(ref)

def test_constructor_and_start_failures_release_python_registrations():
    class ConstructorFailure(Actor):
        def __init__(self): raise ValueError("constructor marker")
    world = World()
    world.spawn(ConstructorFailure)
    with pytest.raises(ValueError, match="constructor marker"): world.step()
    assert world.inspect()["python_registrations"] == 0
    world.close()

def test_constructor_runs_on_first_driver_thread():
    threads = []
    class A(Actor):
        def __init__(self): threads.append(threading.get_ident())
    world = World(); world.spawn(A)
    main = threading.get_ident(); world.step(); assert threads == [main]
    world.close()

def test_stop_other_actor_defers_on_stop_until_callback_finishes():
    order = []
    class B(Actor):
        def on_stop(self, ctx): order.append("B.stop")
    class A(Actor):
        def on_stop(self, ctx):
            order.append("A.start-stop")
            world.stop(b_ref)
            order.append("A.end-stop")
    world = World(); a_ref = world.spawn(A); b_ref = world.spawn(B); world.step()
    world.stop(a_ref); assert order == ["A.start-stop", "A.end-stop", "B.stop"]
    world.close()

def test_returned_coroutine_is_rejected_and_async_generator_cannot_register():
    class BadReturn(Actor):
        @handles(Pulse)
        def handler(self, event, ctx):
            async def later(): return None
            return later()
    world = World(); ref = world.spawn(BadReturn); world.step(); ref.send(Pulse(1))
    with pytest.raises(HandlerFailed): world.step()
    world.close()
    with pytest.raises(TypeError):
        class AsyncGenerator(Actor):
            @handles(Pulse)
            async def handler(self, event, ctx):
                yield event.value

def test_inherited_handler_override_has_one_dispatch_target():
    seen = []
    class Base(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): seen.append("base")
    class Child(Base):
        def pulse(self, event, ctx): seen.append("child")
    world = World(); ref = world.spawn(Child); world.step(); ref.send(Pulse(3)); world.step()
    assert seen == ["child"]
    world.close()

def test_self_send_then_request_stop_prevents_later_claims():
    seen = []
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(event.value)
            if event.value == 1:
                ctx.send(ctx.ref, Pulse(2)); ctx.world.request_stop()
    world = World(mailbox_capacity=4); ref = world.spawn(A); world.step(); ref.send(Pulse(1)); world.step(); world.step()
    assert seen == [1]
    world.close()

def test_oversized_record_is_rejected_before_native_admission():
    @event("adversarial.large", 1)
    class Large: values: tuple[int, ...]
    class A(Actor):
        @handles(Large)
        def large(self, event, ctx): pass
    world = World(); ref = world.spawn(A); world.step()
    with pytest.raises(CodecError): ref.send(Large(tuple(range(4097))))
    world.close()

def test_concurrent_external_sends_and_close_remain_bounded():
    class A(Actor): pass
    world = World(mailbox_capacity=2); ref = world.spawn(A); world.step(); errors = []
    def producer():
        for _ in range(100):
            try: ref.send(Pulse(1))
            except RuntimeError as exc: errors.append(exc)
    threads = [threading.Thread(target=producer) for _ in range(3)]
    for t in threads: t.start()
    time.sleep(.002); world.close()
    for t in threads: t.join()
    assert len(errors) <= 300
