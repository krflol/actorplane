"""Drain-mode lifecycle fences and FIFO delivery."""
import pytest

pytest.importorskip("actorplane._native")
from actorplane import World, Actor, Pulse, handles, AdmissionError, AffinityError

def test_drain_runs_admitted_python_messages_fifo_before_on_stop():
    seen = []
    class A(Actor):
        def on_stop(self, ctx): seen.append("stop")
        @handles(Pulse)
        def pulse(self, event, ctx): seen.append(event.value)
    with World() as world:
        ref = world.spawn(A); world.step()
        ref.send(Pulse(1)); ref.send(Pulse(2))
        world.stop(ref, mode="drain", deadline=.5)
        world.run_for(.5)
        assert seen == [1, 2, "stop"]

def test_drain_rejects_new_sends_after_fence():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    with World() as world:
        ref = world.spawn(A); world.step(); ref.send(Pulse(1))
        world.request_stop(mode="drain", deadline=.2)
        with pytest.raises((AdmissionError, RuntimeError)):
            ref.send(Pulse(2))

def test_drain_close_inside_handler_is_reentrant_error():
    errors = []
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            with pytest.raises(AffinityError) as caught:
                world.close(mode="drain", timeout=.1)
            errors.append(caught.value)
    with World() as world:
        ref = world.spawn(A); world.step(); ref.send(Pulse(1)); world.step()
    assert errors

def test_drain_deadline_is_bounded_and_does_not_extend_per_owner():
    class Slow(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            import time
            time.sleep(.08)
    import time
    world = World()
    try:
        ref = world.spawn(Slow); world.step(); ref.send(Pulse(1))
        started = time.monotonic()
        report = world.close(mode="drain", timeout=.01)
        elapsed = time.monotonic() - started
        assert report is not None
        assert elapsed < .5
        assert world.inspect()["python_registrations"] == 0
    finally:
        world.close()


def test_close_reports_discarded_deliveries_from_its_entire_cancellation():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    world = World()
    ref = world.spawn(A)
    world.step()
    for value in range(3):
        ref.send(Pulse(value))
    report = world.close()
    assert report["discarded"] == 3
    assert world.close() == report
    assert world.inspect()["metrics"]["discarded"] == 3


def test_prior_stop_discards_are_not_counted_again_by_close():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    world = World()
    ref = world.spawn(A)
    world.step()
    ref.send(Pulse(1))
    assert world.stop(ref)["discarded"] == 1
    assert world.close()["discarded"] == 0
    assert world.inspect()["metrics"]["discarded"] == 1


def test_native_drain_deadline_expires_while_python_holds_gil():
    from actorplane import _native
    if not hasattr(_native, "_hold_gil"):
        pytest.skip("development extension requires test-support")
    world = World()
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    ref = world.spawn(A)
    world.step()
    ref.send(Pulse(1))
    world.request_stop(mode="drain", deadline=.01)
    _native._hold_gil(100)
    snapshot = world.inspect()
    assert snapshot["metrics"]["drain_timeouts"] == 1
    assert snapshot["metrics"]["discarded"] == 1
    assert snapshot["python_registrations"] == 1
    report = world.close()
    assert report["native_done"] and report["python_done"]
    assert report["timed_out"]
