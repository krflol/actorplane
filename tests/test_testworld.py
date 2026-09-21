import threading

import pytest

from actorplane import Actor, CountSnapshot, MessageOptions, Pulse, event, handles, native
from actorplane.authoring import AffinityError, QueueFull
from actorplane.components import component
from actorplane.testing import TestStepLimit as _TestStepLimit, TestWorld


def test_timer_waits_for_explicit_virtual_advance():
    seen = []

    class Timer(Actor):
        def on_start(self, ctx):
            ctx.after(0.01, Pulse(5))

        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append((event.value, ctx.envelope.enqueued_at_ns))

    world = TestWorld()
    world.spawn(Timer)
    world.run_until_idle()
    assert seen == []
    world.advance(0.01)
    assert len(seen) == 1
    assert seen[0][0] == 5
    assert seen[0][1] == 10_000_000
    world.close()


def test_native_responder_completion_preserves_result_metadata():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    responder = world.native_responder(Pulse)
    options = MessageOptions(correlation_id=41)
    operation = world.request(owner, responder, Pulse(7), options=options)
    world.run_until_idle()
    assert operation.poll() is None
    assert world.complete(operation, Pulse(9))
    world.run_until_idle()
    result = operation.take()
    assert result.value == Pulse(9)
    assert result.envelope is not None
    assert result.envelope.correlation_id == 41
    world.close()


def test_virtual_timeout_rejects_late_completion_and_releases_payload():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    responder = world.native_responder(Pulse)
    operation = world.request(owner, responder, Pulse(3), timeout=0.01)
    world.advance(0.01)
    assert operation.poll().state == "TimedOut"
    assert not world.complete(operation, Pulse(4))
    world.advance(0.001)
    assert world.inspect()["retained_payload_bytes"] == 0
    world.close()


def test_native_components_progress_only_after_virtual_advance():
    class Pipeline(Actor):
        source = component(native.PulseSource, interval=0.002)
        counter = component(native.WindowCounter, window=0.01)
        sink = component(native.SnapshotSink)

        def configure(self, ctx):
            ctx.link(self.source.pulses, self.counter.input)
            ctx.link(self.counter.snapshots, self.sink.input)

    world = TestWorld()
    world.spawn(Pipeline)
    world.run_until_idle()
    world.advance(0.05)
    assert world.inspect()["metrics"]["admitted"] > 0
    world.close()


def test_creator_thread_guard_happens_before_mutation():
    world = TestWorld()
    errors = []

    class Empty(Actor):
        pass

    def worker():
        try:
            world.spawn(Empty)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert errors and isinstance(errors[0], RuntimeError)
    assert world.inspect()["python_registrations"] == 0
    world.close()


def test_reentrant_advance_rejected_before_clock_change():
    observed = []
    holder = {}

    class Reentrant(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            before = holder["world"].now()
            with pytest.raises(AffinityError):
                holder["world"].advance(1.0)
            observed.append(holder["world"].now() == before)

    world = TestWorld()
    holder["world"] = world
    ref = world.spawn(Reentrant)
    world.step()
    ref.send(Pulse(1))
    world.step()
    assert observed == [True]
    world.close()


def test_self_send_cycle_hits_bound_and_close_remains_possible():
    holder = {}

    class Cycle(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            ctx.send(ctx.actor, Pulse(event.value + 1))

    world = TestWorld(max_steps=8)
    holder["ref"] = world.spawn(Cycle)
    world.step()
    holder["ref"].send(Pulse(1))
    with pytest.raises(_TestStepLimit):
        world.run_until_idle()
    world.close()


def test_saturated_mailbox_can_drain_without_wall_clock_wait():
    class Slow(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            pass

    world = TestWorld(mailbox_capacity=1)
    ref = world.spawn(Slow)
    world.step()
    ref.send(Pulse(1))
    with pytest.raises(QueueFull):
        ref.send(Pulse(2))
    report = world.close(mode="drain")
    assert report["native_done"]


def test_idle_pump_does_not_move_virtual_clock_and_python_expiry_can_be_held():
    seen = []

    class Timed(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(event.value)

    world = TestWorld()
    ref = world.spawn(Timed)
    world.step()
    assert world.now() == 0
    world.run_until_idle()
    assert world.now() == 0
    ref.send(Pulse(1), options=MessageOptions(timeout=0.01))
    world.advance(0.01, dispatch_python=False)
    assert seen == []
    world.run_until_idle()
    assert seen == []
    world.close()


def test_virtual_deadlines_remain_relative_after_large_clock_jump():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    world.advance(3600)
    responder = world.native_responder(Pulse)
    operation = world.request(owner, responder, Pulse(1), timeout=0.001)
    world.run_until_idle()
    assert world.complete(operation, Pulse(2))
    world.run_until_idle()
    result = operation.take()
    assert result.envelope is not None
    assert result.envelope.deadline_ns == 3_600_001_000_000
    timeout = world.request(owner, responder, Pulse(3), timeout=0.01)
    world.advance(0.009)
    assert timeout.poll() is None
    world.advance(0.001)
    outcome = timeout.poll()
    assert outcome is not None and outcome.state == "TimedOut"
    world.close()


def test_controlled_cancel_releases_native_operation_payload():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    responder = world.native_responder(Pulse)
    operation = world.request(owner, responder, Pulse(4), timeout=1)
    world.run_until_idle()
    assert world.pending_native()
    assert operation.cancel()
    world.advance(0.001)
    assert operation.take().state == "Cancelled"
    assert not world.pending_native()
    assert world.inspect()["retained_payload_bytes"] == 0
    world.close()


def test_native_pipeline_snapshots_are_replayable_in_virtual_time():
    def run_once():
        snapshots = []

        class Pipeline(Actor):
            source = component(native.PulseSource, interval=0.01)
            counter = component(native.WindowCounter, window=0.01)

            @handles(CountSnapshot)
            def snapshot(self, event, ctx):
                snapshots.append((event.count, event.total, ctx.envelope.enqueued_at_ns))

            def configure(self, ctx):
                ctx.link(self.source.pulses, self.counter.input)
                ctx.subscribe(self.counter.snapshots, ctx.actor)

        world = TestWorld()
        world.spawn(Pipeline)
        world.step()
        for _ in range(10):
            world.advance(0.01)
        world.close(mode="drain")
        return snapshots

    traces = [run_once() for _ in range(20)]
    assert traces[0]
    assert all(trace == traces[0] for trace in traces[1:])
    assert sum(count for count, _, _ in traces[0]) == 10
    assert sum(total for _, total, _ in traces[0]) == 10


def test_stopped_responder_generation_rejects_old_completion():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    old = world.native_responder(Pulse)
    operation = world.request(owner, old, Pulse(1), timeout=1)
    world.run_until_idle()
    world.stop(old)
    world.advance(0.001)
    assert operation.poll().state == "TargetStopped"
    assert not world.complete(operation, Pulse(9))
    replacement = world.native_responder(Pulse)
    assert replacement.slot == old.slot
    assert replacement.generation != old.generation
    fresh = world.request(owner, replacement, Pulse(2), timeout=1)
    world.run_until_idle()
    assert world.complete(fresh, Pulse(9))
    world.run_until_idle()
    assert fresh.take().value == Pulse(9)
    world.close()


def test_drain_with_pending_controlled_operation_uses_virtual_deadline():
    class Owner(Actor):
        pass

    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    responder = world.native_responder(Pulse)
    world.request(owner, responder, Pulse(2), timeout=1)
    world.run_until_idle()
    report = world.close(timeout=0.01, mode="drain")
    assert report["native_done"]
    assert report["timed_out"]
    assert report["python_done"]
    assert world.inspect()["retained_payload_bytes"] == 0
    assert not world.pending_native()


@pytest.mark.parametrize("control", ["step", "advance", "run_until_idle", "close"])
def test_wrong_thread_control_rejection_does_not_poison_creator(control):
    world = TestWorld()
    errors = []

    def worker():
        try:
            if control == "step":
                world.step()
            elif control == "advance":
                world.advance(0.001)
            elif control == "run_until_idle":
                world.run_until_idle()
            else:
                world.close()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert errors and isinstance(errors[0], RuntimeError)
    world.run_until_idle()
    world.close()


def test_virtual_advance_rounding_and_rejected_inputs_do_not_dispatch():
    started = []

    class Pending(Actor):
        def on_start(self, ctx):
            started.append(True)

    with TestWorld() as world:
        world.spawn(Pending)
        for invalid in (-1, float("nan"), float("inf"), 86401):
            with pytest.raises(ValueError):
                world.advance(invalid)
        with pytest.raises(ValueError):
            world.advance(1, max_steps=0)
        assert world.now() == 0 and not started
        world.advance(0.0004)
        assert world.now() == 0.001 and started == [True]
        world.advance(0.0006)
        assert world.now() == 0.002


def test_controlled_structured_replies_share_codecs_and_fence_foreign_operations():
    @event("testing.Response")
    class Response:
        text: str
        values: tuple[int, ...]

    with TestWorld() as world, TestWorld() as other:
        owner = world.spawn(Actor)
        world.step()
        responder = world.native_responder(Response)
        # A second input type must not collide with the responder descriptor.
        pulse_responder = world.native_responder(Pulse)
        operation = world.request(owner, responder, Response("input", (1,)))
        world.run_until_idle()
        assert operation.poll() is None
        with pytest.raises(ValueError, match="another World"):
            other.complete(operation, Response("foreign", (2,)))
        assert other.inspect()["retained_payload_bytes"] == 0
        assert world.complete(operation, Response("result", (3, 4)))
        world.run_until_idle()
        assert operation.take().value == Response("result", (3, 4))
        pulse = world.request(owner, pulse_responder, Pulse(1))
        world.run_until_idle()
        assert world.complete(pulse, Pulse(2))
        world.run_until_idle()
        assert pulse.take().value == Pulse(2)
        assert not world.pending_native()
        assert world.inspect()["retained_payload_bytes"] == 0
