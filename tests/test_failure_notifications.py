"""Structured failure delivery and supervisor policy tests."""
import pytest
from actorplane import World, Actor, Pulse, Failure, FailurePolicy, HandlerFailed, QueueFull, handles

def test_child_failure_is_queued_for_parent_supervisor_on_next_pump():
    refs, failures = {}, []
    class Child(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("child failure")
    class Parent(Actor):
        def on_start(self, ctx): refs["child"] = ctx.spawn(Child)
        def on_failure(self, failure, ctx): failures.append(failure)
    with World() as world:
        world.spawn(Parent); world.step(); world.step(); refs["child"].send(Pulse(1))
        world.step(); assert failures == []
        world.step(); assert len(failures) == 1
        assert isinstance(failures[0], Failure)
        assert failures[0].phase == "Handler"
        assert failures[0].exception_type.endswith("ValueError")

def test_supervisor_control_delivery_is_not_blocked_by_full_business_mailbox():
    refs, failures, order = {}, [], []
    class Child(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise RuntimeError("full mailbox")
    class Parent(Actor):
        def on_start(self, ctx): refs["child"] = ctx.spawn(Child)
        @handles(Pulse)
        def business(self, event, ctx): order.append("business")
        def on_failure(self, failure, ctx):
            failures.append(failure)
            order.append("failure")
    with World(mailbox_capacity=1) as world:
        parent = world.spawn(Parent); world.step(); world.step()
        refs["child"].send(Pulse(2)); world.step()
        parent.send(Pulse(1))
        with pytest.raises(QueueFull): parent.send(Pulse(2))
        assert world.step() == 2
        assert len(failures) == 1
        assert order == ["failure", "business"]

def test_supervisor_returning_stop_world_fences_world():
    refs = {}
    class Child(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("fatal child")
    class Parent(Actor):
        def on_start(self, ctx): refs["child"] = ctx.spawn(Child)
        def on_failure(self, failure, ctx): return FailurePolicy.STOP_WORLD
    with World() as world:
        world.spawn(Parent); world.step(); world.step(); refs["child"].send(Pulse(1)); world.step(); world.step()
        assert world.inspect()["python_registrations"] == 0

def test_supervisor_exception_stops_world_and_preserves_handler_failure():
    refs = {}
    class Child(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("child")
    class Parent(Actor):
        def on_start(self, ctx): refs["child"] = ctx.spawn(Child)
        def on_failure(self, failure, ctx): raise RuntimeError("supervisor broken")
    world = World()
    world.spawn(Parent); world.step(); world.step(); refs["child"].send(Pulse(1)); world.step()
    with pytest.raises(HandlerFailed, match="supervisor"):
        world.step()
    assert world.inspect()["python_registrations"] == 0
    world.close()


@pytest.mark.parametrize("hook,phase", [("__init__", "Construction"),
                                      ("configure", "Configure"), ("on_start", "Start")])
def test_startup_failure_records_exact_lifecycle_phase(hook, phase):
    def fail(*args): raise ValueError("private startup data")
    broken = type("Broken", (Actor,), {hook: fail})
    world = World()
    reference = world.spawn(broken)
    try:
        with pytest.raises(ValueError): world.step()
        failures = [entry["failure"] for entry in world.diagnostics()["entries"] if entry["failure"]]
        assert len(failures) == 1
        assert failures[0]["phase"] == phase
        assert failures[0]["actor"] == reference.handle
        assert failures[0]["handler"] == hook
        assert "private startup data" not in str(failures)
    finally:
        world.close()


def test_cleanup_failure_is_reported_before_python_registration_is_retired():
    class Broken(Actor):
        def on_stop(self, ctx): raise ValueError("private cleanup data")
    world = World()
    reference = world.spawn(Broken)
    world.step()
    with pytest.raises(HandlerFailed): world.stop(reference)
    failures = [entry["failure"] for entry in world.diagnostics()["entries"] if entry["failure"]]
    assert len(failures) == 1
    assert failures[0]["phase"] == "Stop"
    assert world.inspect()["python_registrations"] == 0
    assert world.close()["python_done"]


def test_unhandled_child_failure_escalates_on_next_pump():
    refs = {}
    class Child(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError()
    class Parent(Actor):
        def on_start(self, ctx): refs["child"] = ctx.spawn(Child)
    world = World()
    world.spawn(Parent); world.step(); world.step()
    refs["child"].send(Pulse(1)); world.step()
    with pytest.raises(HandlerFailed, match="supervisor"): world.step()
    assert world.inspect()["python_registrations"] == 0
    assert world.inspect()["metrics"]["failures"] == 2
    world.close()


def test_native_failure_is_reported_while_gil_is_held_and_delivered_afterward():
    from actorplane import _native
    if not hasattr(_native, "_hold_gil"):
        pytest.skip("test-support extension required")
    refs, seen = {}, []
    class Parent(Actor):
        def on_start(self, ctx): refs["pipeline"] = ctx.native_counter(1, 1)
        def on_failure(self, failure, ctx): seen.append(failure)
    with World() as world:
        world.spawn(Parent); world.step()
        pipeline = refs["pipeline"]
        event_id = world._native.send(pipeline.counter, "record", [1], 42)
        _native._hold_gil(150)
        assert seen == []
        assert world.inspect()["metrics"]["failures"] == 1
        assert world.inspect()["active_tasks"] == 0
        world.step()
        assert len(seen) == 1
        assert seen[0].event_id == event_id
        assert seen[0].actor.handle == pipeline.owner
        assert seen[0].exception_type == "InvalidPulseOrOverflow"


def test_hostile_exception_string_does_not_replace_original_failure():
    class Hostile(Exception):
        def __str__(self): raise AssertionError("exception string must not be called")
    class Broken(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise Hostile()
    with World() as world:
        reference = world.spawn(Broken); world.step(); reference.send(Pulse(1))
        with pytest.raises(HandlerFailed) as error: world.step()
        assert isinstance(error.value.__cause__, Hostile)


def test_child_startup_failure_notification_waits_for_next_pump():
    seen = []
    class Broken(Actor):
        def configure(self, ctx): raise ValueError()
    class Parent(Actor):
        def on_start(self, ctx): ctx.spawn(Broken)
        def on_failure(self, failure, ctx): seen.append(failure.phase)
    with World() as world:
        world.spawn(Parent)
        world.step()
        world.step()
        assert seen == []
        world.step()
        assert seen == ["Configure"]
