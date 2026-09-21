"""Native control diagnostics stay useful when business queues are saturated."""
import pytest

from actorplane import Actor, Pulse, QueueFull, World, FailurePolicy, handles
from actorplane import _native


def test_bounded_history_reports_gaps_and_can_be_read_after_close():
    class Sink(Actor):
        @handles(Pulse)
        def receive(self, event, ctx):
            pass

    world = World(diagnostic_capacity=3, mailbox_capacity=1)
    try:
        target = world.spawn(Sink)
        world.step()
        target.send(Pulse(1))
        for _ in range(8):
            with pytest.raises(QueueFull):
                target.send(Pulse(2))
        history = world.diagnostics(limit=3)
        assert len(history["entries"]) == 3
        assert history["dropped"] >= 5
        assert history["gap"]["first_available"] == history["entries"][0]["sequence"]
        assert {entry["code"] for entry in history["entries"]} == {"QueueFull"}
        assert all(entry["actor"] == target.handle for entry in history["entries"])
        stamps = [entry["elapsed_ns"] for entry in history["entries"]]
        assert stamps == sorted(stamps)
        cursor = history["entries"][-1]["sequence"]
        assert not world.diagnostics(after_sequence=cursor, limit=3)["entries"]
        # An untrusted cursor must not panic or poison the native mutex.
        assert not world.diagnostics(after_sequence=2**64 - 1, limit=3)["entries"]
        with pytest.raises(RuntimeError):
            world.diagnostics(limit=4)
        assert world.inspect()["metrics"]["rejected"] == 8
    finally:
        world.close()
    assert world.diagnostics(limit=3)["entries"]


def test_timeout_diagnostic_is_recorded_during_held_gil():
    if not hasattr(_native, "_hold_gil"):
        pytest.skip("development extension requires test-support")
    with World(diagnostic_capacity=4) as world:
        class Target(Actor):
            @handles(Pulse)
            def pulse(self, event, ctx): pass
        owner = world.spawn(Target)
        target = world.spawn(Target)
        world.step()
        operation = world.request(owner, target, Pulse(1), timeout=.01)
        _native._hold_gil(100)
        assert operation.poll().state == "TimedOut"
        records = world.diagnostics(limit=4)["entries"]
        assert any(record["code"] == "OperationTimedOut" for record in records)
        assert any(record["operation"] == operation.handle for record in records)


def test_continue_policy_records_ordinary_delivery_failure():
    class Broken(Actor):
        @handles(Pulse)
        def receive(self, event, ctx):
            raise ValueError("payload text should not appear in diagnostics")

    with World(failure_policy=FailurePolicy.CONTINUE) as world:
        actor = world.spawn(Broken)
        world.step()
        actor.send(Pulse(42))
        assert world.step() == 1
        failures = [entry for entry in world.diagnostics()["entries"]
                    if entry["code"] == "HandlerFailed"]
        assert len(failures) == 1
        assert failures[0]["actor"] == actor.handle
        assert "payload" not in str(failures)
