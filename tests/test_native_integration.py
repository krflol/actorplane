"""Black-box integration checks for the built PyO3 extension."""
import threading, time
import pytest

pytest.importorskip("actorplane._native")
from actorplane import World, Actor, Pulse, CountSnapshot, event, handles, QueueFull, AdmissionError
from actorplane.authoring import LifecycleError, HandlerFailed, AffinityError, CodecError

@event("test.record", 1)
class Record:
    number: int
    values: tuple[int, ...]
    other: tuple[int, ...]

def _capture(fn, failures):
    try: fn()
    except Exception as exc: failures.append(exc)

def test_actor_creation_is_deferred_and_hooks_once():
    seen = []
    class A(Actor):
        def __init__(self): seen.append("construct")
        def on_start(self, ctx): seen.append("start")
        def on_stop(self, ctx): seen.append("stop")
    with World() as world:
        ref = world.spawn(A)
        assert seen == []
        world.step()
        assert seen == ["construct", "start"]
        world.stop(ref); world.stop(ref)
        assert seen == ["construct", "start", "stop"]

def test_pending_send_is_rejected_and_affinity_binds_on_first_driver_call():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    with World() as world:
        ref = world.spawn(A)
        with pytest.raises(LifecycleError): ref.send(Pulse(1))
        world.step()
        failures = []
        t = threading.Thread(target=lambda: _capture(world.step, failures)); t.start(); t.join()
        assert failures and isinstance(failures[0], AffinityError)

def test_self_send_is_queued_and_nested_driver_calls_rejected():
    seen = []
    class A(Actor):
        @handles(Pulse)
        def got(self, event, ctx):
            seen.append(event.value)
            if event.value == 1:
                ctx.send(ctx.ref, Pulse(2))
                with pytest.raises(AffinityError): world.step()
    with World(mailbox_capacity=8) as world:
        ref = world.spawn(A); world.step(); ref.send(Pulse(1)); world.step()
        assert seen == [1]
        world.step(); assert seen == [1, 2]

def test_handler_failure_stops_actor_and_on_stop_runs_once():
    stopped = []
    class Broken(Actor):
        def on_stop(self, ctx): stopped.append(True)
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("boom")
    with World() as world:
        ref = world.spawn(Broken); world.step(); ref.send(Pulse(1))
        with pytest.raises(HandlerFailed): world.step()
        world.step(); world.stop(ref); world.stop(ref)
    assert stopped == [True]

def test_record_payload_is_snapshot_and_decodes_multiple_tuples():
    received = []
    class A(Actor):
        @handles(Record)
        def got(self, event, ctx): received.append((event.number, event.values, event.other))
    with World() as world:
        ref = world.spawn(A); world.step()
        values = [1, 2]; other = [3]
        ref.send(Record(7, values, other)); values.append(99); other.clear()
        world.step()
    assert received == [(7, (1, 2), (3,))]

def test_schema_collision_is_rejected():
    @event("collision", 1)
    class One: value: int
    @event("collision", 1)
    class Two: value: tuple[int, ...]
    class A(Actor):
        @handles(One)
        def got(self, event, ctx): pass
    with World() as world:
        ref = world.spawn(A); world.step(); ref.send(One(1))
        with pytest.raises(CodecError): ref.send(Two((1,)))

def test_held_gil_probe_reports_native_progress():
    import actorplane._native as native
    probe = getattr(native, "_held_gil_probe", None)
    if probe is None: pytest.skip("test-support extension is not enabled")
    report = probe(120)
    assert report["progress_in_hold"] > 0
    assert report["native_stopped_in_hold"] is True
    assert report["bridge_entries_during_hold"] == 0
    assert report["hold_start_ns"] <= report["first_progress_ns"] <= report["last_progress_ns"] < report["hold_end_ns"]

def test_python_callbacks_resume_after_real_gil_hold():
    import actorplane._native as native
    hold = getattr(native, "_hold_gil", None)
    if hold is None: pytest.skip("GIL hold helper is not enabled")
    callbacks = []
    counters = []
    class CounterSink(Actor):
        def on_start(self, ctx):
            self.counter = ctx.native_counter(.002, .01, target=ctx.actor)
            counters.append(self.counter)
        @handles(CountSnapshot)
        def snapshot(self, event, ctx): callbacks.append(event.total)
    with World() as world:
        ref = world.spawn(CounterSink); world.run_for(.02)
        before = len(callbacks)
        native_before = counters[0].stats()
        hold(100)
        assert len(callbacks) == before
        during = counters[0].stats()
        assert during["processed"] > native_before["processed"]
        assert during["sink_received"] > native_before["sink_received"]
        world.run_for(.1)
        assert len(callbacks) > before

def test_saturated_mailbox_rejects_immediately():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
    with World(mailbox_capacity=1) as world:
        ref = world.spawn(A); world.step(); ref.send(Pulse(1))
        with pytest.raises((QueueFull, AdmissionError)):
            ref.send(Pulse(2))

def test_startup_failure_rolls_back_native_counter_and_preserves_error():
    class Failing(Actor):
        def on_start(self, ctx):
            ctx.native_counter(.01, .05, target=ctx.actor)
            raise ValueError("startup marker")
    world = World()
    world.spawn(Failing)
    with pytest.raises(ValueError, match="startup marker"):
        world.step()
    deadline = time.monotonic() + 1
    while world.inspect().get("active_tasks", 0) and time.monotonic() < deadline:
        time.sleep(.01)
    assert world.inspect().get("active_tasks", 0) == 0
    world.close()

def test_on_stop_failure_still_cleans_other_actors():
    stopped = []
    class Bad(Actor):
        def on_stop(self, ctx):
            stopped.append("bad")
            raise ValueError("stop marker")
    class Good(Actor):
        def on_stop(self, ctx): stopped.append("good")
    world = World()
    try:
        world.spawn(Bad); world.spawn(Good); world.step(); world.request_stop()
    finally:
        with pytest.raises(HandlerFailed): world.close()
    assert sorted(stopped) == ["bad", "good"]

def test_closed_world_rejects_new_work():
    world = World(); ref = world.spawn(Actor); world.close()
    with pytest.raises(Exception): ref.send(Pulse(1))

def test_unsupported_sequence_is_rejected_at_decoration():
    with pytest.raises(CodecError):
        @event("bad.sequence", 1)
        class Bad:
            labels: tuple[dict, ...]

def test_schema_budget_and_retired_actor_capacity():
    @event("budget.one", 1)
    class One: value: int
    @event("budget.two", 1)
    class Two: value: int
    class Sink(Actor):
        @handles(One)
        def one(self, event, ctx): pass
        @handles(Two)
        def two(self, event, ctx): pass
    with World(max_schemas=1, max_actors=1) as world:
        with pytest.raises((CodecError, AdmissionError)):
            world.spawn(Sink)
        class OneSink(Actor):
            @handles(One)
            def one(self, event, ctx): pass
        ref = world.spawn(OneSink); world.step(); ref.send(One(1))
        world.stop(ref)
        replacement = world.spawn(OneSink)
        world.step()
        assert replacement.generation != ref.generation
