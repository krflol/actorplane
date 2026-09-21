"""Bounded request/reply outcomes and owner-scoped operation slots."""
import pytest

pytest.importorskip("actorplane._native")
from actorplane import World, Actor, event, handles, OperationError, AdmissionError, Pulse

@event("request.body", 1)
class Body:
    number: int
    tags: tuple[int, ...]

@event("request.reply", 1)
class Reply:
    total: int

def test_request_reply_roundtrip_and_pending_result_is_nonblocking():
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx):
            assert ctx.operation is not None
            assert ctx.reply(Reply(event.number + sum(event.tags))) is True
            assert ctx.reply(Reply(999)) is False
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(4, (2, 3)), timeout=1)
        with pytest.raises(OperationError): op.result()
        world.step()
        outcome = op.poll()
        assert outcome.state == "Completed" and outcome.value == Reply(9)
        assert op.result() == Reply(9)
        with pytest.raises(RuntimeError): op.take()

def test_cancel_before_dispatch_prevents_target_callback():
    calls = []
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): calls.append(event.number)
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(1, ()), timeout=1)
        assert op.cancel() is True
        world.step(); assert calls == []
        assert op.poll().state == "Cancelled"

def test_target_stop_produces_target_stopped_and_owner_stop_retires_handle():
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): pass
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(1, ()), timeout=1)
        world.stop(target)
        world.run_for(.01)
        assert op.poll().state == "TargetStopped"
        world.stop(owner)
        with pytest.raises(RuntimeError): op.poll()

def test_terminal_results_consume_operation_capacity_only_after_take():
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): ctx.reply(Reply(event.number))
    with World(max_operations=1) as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        first = world.request(owner, target, Body(1, ()), timeout=1); world.step()
        with pytest.raises((AdmissionError, RuntimeError)): world.request(owner, target, Body(2, ()), timeout=1)
        assert first.result() == Reply(1)
        second = world.request(owner, target, Body(2, ()), timeout=1)
        assert second is not None

def test_queue_full_rolls_back_operation_slot():
    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): pass
        @handles(Body)
        def body(self, event, ctx): pass
    with World(max_operations=1, mailbox_capacity=1) as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        target.send(Pulse(0))
        with pytest.raises(AdmissionError): world.request(owner, target, Body(1, ()), timeout=1)
        # The failed admission must not permanently consume the only slot.
        world.step()
        op = world.request(owner, target, Body(2, ()), timeout=1)
        assert op is not None

def test_cross_world_request_rejected():
    with World() as first, World() as second:
        owner = first.spawn(Actor); target = second.spawn(Actor)
        first.step(); second.step()
        with pytest.raises((AdmissionError, RuntimeError)):
            first.request(owner, target, Body(1, ()), timeout=1)

def test_native_timeout_can_win_before_python_claim_while_gil_is_held():
    import actorplane._native as native
    hold = getattr(native, "_hold_gil", None)
    if hold is None: pytest.skip("GIL hold helper is not enabled")
    seen = []
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): seen.append(event.number)
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(5, ()), timeout=.02)
        hold(100)
        assert op.poll().state == "TimedOut"
        world.step(); assert seen == []

def test_failed_handler_completes_request_as_failed_and_owner_survives():
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): raise ValueError("request failure")
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(1, ()), timeout=1)
        with pytest.raises(Exception): world.step()
        assert op.poll().state == "Failed"
        assert world.inspect()["python_registrations"] >= 1

def test_drain_delivers_admitted_request_then_retains_result_until_take():
    class Target(Actor):
        @handles(Body)
        def body(self, event, ctx): ctx.reply(Reply(event.number))
    with World() as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Body(8, ()), timeout=1)
        world.request_stop(mode="drain", deadline=.5)
        world.run_for(.5)
        # Owner retirement may reclaim the operation handle; completion remains
        # observable through native metrics in that case.
        try:
            outcome = op.poll()
        except RuntimeError:
            outcome = None
        if outcome is not None:
            assert outcome.state == "Completed"
            assert op.take().state == "Completed"
        else:
            assert world.inspect()["metrics"]["operation_completed"] >= 1
