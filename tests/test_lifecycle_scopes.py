"""Lifecycle-scope probes for staged children and cross-thread fencing."""
import threading
import pytest

pytest.importorskip("actorplane._native")
from actorplane import World, Actor, Pulse, LifecycleError

def test_child_startup_is_staged_after_parent_start():
    order = []
    class Child(Actor):
        def on_start(self, ctx): order.append("child.start")
    class Parent(Actor):
        def on_start(self, ctx):
            order.append("parent.start.begin")
            ctx.spawn(Child)
            order.append("parent.start.end")
    with World() as world:
        world.spawn(Parent); world.step(); assert order == ["parent.start.begin", "parent.start.end"]
        world.step(); assert order == ["parent.start.begin", "parent.start.end", "child.start"]

def test_failed_parent_start_does_not_leave_staged_child_registration():
    order = []
    class Child(Actor):
        def on_start(self, ctx): order.append("child.start")
    class Parent(Actor):
        def on_start(self, ctx):
            ctx.spawn(Child)
            raise ValueError("parent startup")
    world = World(); world.spawn(Parent)
    with pytest.raises(ValueError, match="parent startup"): world.step()
    assert order == []
    assert world.inspect()["python_registrations"] == 0
    world.close()

def test_on_stop_cannot_send_new_work():
    errors = []
    class A(Actor):
        def on_stop(self, ctx):
            try: ctx.send(ctx.ref, Pulse(1))
            except Exception as exc: errors.append(exc)
    with World() as world:
        ref = world.spawn(A); world.step(); world.stop(ref); world.step()
    assert errors and isinstance(errors[0], LifecycleError)

def test_request_stop_is_safe_from_external_thread_and_driver_finishes_cleanup():
    stopped = []
    class A(Actor):
        def on_stop(self, ctx): stopped.append(True)
    world = World(); world.spawn(A); world.step()
    thread = threading.Thread(target=world.request_stop)
    thread.start(); thread.join()
    world.step()
    assert stopped == [True]
    world.close()


def test_owned_children_cleanup_before_parent():
    order = []
    class Child(Actor):
        def on_stop(self, ctx): order.append("child")
    class Parent(Actor):
        def on_start(self, ctx): ctx.spawn(Child)
        def on_stop(self, ctx): order.append("parent")
    with World() as world:
        root = world.spawn(Parent)
        world.step(); world.step()
        world.stop(root)
        assert order == ["child", "parent"]


def test_cancel_all_fences_native_registration_before_python_cleanup():
    world = World()
    world.spawn(Actor)
    world.step()
    world.request_stop()
    with pytest.raises(RuntimeError):
        world._native.allocate(True, None)
    assert world.inspect()["python_registrations"] == 1
    world.step()
    assert world.inspect()["python_registrations"] == 0
    world.close()
