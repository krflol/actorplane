import pytest
from actorplane import (World, Actor, Pulse, FailurePolicy, HandlerFailed,
                        event, handles)

def test_failure_policy_is_explicit_and_continue_preserves_actor():
    seen = []
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            if event.value == 1: raise ValueError("recoverable")
            seen.append(event.value)
    with World(failure_policy=FailurePolicy.CONTINUE) as world:
        ref = world.spawn(A); world.step(); ref.send(Pulse(1)); ref.send(Pulse(2))
        world.run_for(.01)
        assert world.inspect()["python_registrations"] == 1
        assert seen == [2]

def test_continue_failed_request_is_terminal_while_actor_survives():
    from actorplane import event
    @event("supervision.request", 1)
    class Request: value: int
    @event("supervision.reply", 1)
    class Reply: value: int
    class Target(Actor):
        @handles(Request)
        def request(self, event, ctx): raise ValueError("request failed")
    with World(failure_policy=FailurePolicy.CONTINUE) as world:
        owner = world.spawn(Actor); target = world.spawn(Target); world.step()
        op = world.request(owner, target, Request(1), timeout=1); world.step()
        assert op.poll().state == "Failed"
        assert world.inspect()["python_registrations"] == 2

def test_child_failure_stops_its_subtree_but_supervisor_and_sibling_survive():
    refs = {}
    class Child(Actor):
        def on_start(self, ctx): refs["grandchild"] = ctx.spawn(Sibling)
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("child")
    class Sibling(Actor):
        def on_start(self, ctx): refs.setdefault("sibling_started", True)
        @handles(Pulse)
        def pulse(self, event, ctx): refs["sibling_seen"] = event.value
    class Root(Actor):
        def on_start(self, ctx):
            refs["child"] = ctx.spawn(Child)
            refs["sibling"] = ctx.spawn(Sibling)
        def on_failure(self, failure, ctx):
            refs["failure"] = failure
    with World() as world:
        root = world.spawn(Root); world.step(); world.step(); world.step()
        refs["child"].send(Pulse(1))
        world.step()
        assert world.inspect()["python_registrations"] >= 1
        with pytest.raises(RuntimeError): refs["grandchild"].send(Pulse(2))
        refs["sibling"].send(Pulse(3)); world.step()
        assert refs["sibling_seen"] == 3
        world.stop(refs["sibling"])
        world.stop(root)

def test_stop_world_policy_fences_siblings():
    class Failing(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise ValueError("fatal")
    class Other(Actor): pass
    with World(failure_policy=FailurePolicy.STOP_WORLD) as world:
        bad = world.spawn(Failing); other = world.spawn(Other); world.step(); bad.send(Pulse(1))
        with pytest.raises(HandlerFailed): world.step()
        assert world.inspect()["python_registrations"] == 0

def test_base_exception_is_never_resumable():
    class Fatal(Actor):
        @handles(Pulse)
        def fail(self, event, ctx): raise KeyboardInterrupt()
    with World(failure_policy=FailurePolicy.CONTINUE) as world:
        ref = world.spawn(Fatal); world.step(); ref.send(Pulse(1))
        with pytest.raises(KeyboardInterrupt): world.step()
        assert world.inspect()["python_registrations"] == 0

def test_startup_failure_is_fatal_even_under_continue_and_cleans_registration():
    class Broken(Actor):
        def on_start(self, ctx): raise ValueError("startup")
    world = World(failure_policy=FailurePolicy.CONTINUE); world.spawn(Broken)
    with pytest.raises(ValueError, match="startup"): world.step()
    assert world.inspect()["python_registrations"] == 0
    world.close()

def test_actor_policy_overrides_world_policy():
    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx): raise ValueError("actor policy")
    with World(failure_policy=FailurePolicy.STOP_WORLD) as world:
        ref = world.spawn(A, failure_policy=FailurePolicy.CONTINUE); world.step(); ref.send(Pulse(1)); world.step()
        assert world.inspect()["python_registrations"] == 1
