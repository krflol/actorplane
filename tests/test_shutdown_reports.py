"""Shutdown reports distinguish live work from bounded lifetime error evidence."""
import time

import pytest

from actorplane import Actor, HandlerFailed, Pulse, World, handles


def test_stop_report_is_read_only_and_operations_in_scope_are_not_double_counted():
    with World() as world:
        class Accept(Actor):
            @handles(Pulse)
            def pulse(self, event, ctx): pass
        parent = world.spawn(Accept)
        child = world.spawn(Accept, parent=parent)
        world.step()
        operation = world.request(parent, child, Pulse(1))
        report = world.stop_report(parent)
        assert report["outstanding_operations"] == 1
        assert report["queued"] == 1
        assert report["python_pending"] == 2
        assert report["discarded"] == 0
        assert not report["native_done"]
        assert operation.poll() is None
        world.request_stop()
        report = world.stop_report()
        assert report["outstanding_operations"] == 0
        assert report["retained_operations"] == 1
        assert report["native_done"] and not report["python_done"]
    report = world.stop_report()
    assert report["retained_operations"] == 0
    assert report["python_pending"] == 0


def test_cleanup_error_is_retained_with_zero_diagnostic_capacity():
    class Broken(Actor):
        def on_stop(self, ctx):
            raise ValueError("private cleanup message")

    world = World(diagnostic_capacity=0)
    ref = world.spawn(Broken)
    world.step()
    with pytest.raises(HandlerFailed):
        world.close()
    report = world.close()
    assert report["native_done"] and report["python_done"]
    assert report["errors"] == 1
    assert report["last_error"]["actor"] == ref.handle
    assert report["last_error"]["phase"] == "Stop"
    assert "private cleanup message" not in str(report)
    assert world.stop_report()["errors"] == 1


def test_cooperative_cleanup_deadline_overrun_is_reported_after_hook_returns():
    seen = []

    class Slow(Actor):
        def on_stop(self, ctx):
            time.sleep(.03)
            seen.append("returned")

    world = World()
    world.spawn(Slow)
    world.step()
    report = world.close(timeout=.001)
    assert seen == ["returned"]
    assert report["timed_out"]
    assert report["native_done"] and report["python_done"]
    assert world.close() == report
    assert world.stop_report()["timed_out"]
