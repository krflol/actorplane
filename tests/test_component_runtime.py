"""Runtime acceptance tests for Python-authored native components."""
import threading
import time
import pytest
from actorplane import World, Actor, Component, Pulse, CountSnapshot, Interface, Output, component, handles
from actorplane import native

def test_native_pipeline_progresses_while_python_driver_is_paused():
    captured = {}
    class Monitor(Actor):
        source = component(native.PulseSource, interval=.002)
        counter = component(native.WindowCounter, window=.01, depends_on=("source",))
        sink = component(native.SnapshotSink, depends_on=("counter",))
        def configure(self, ctx):
            captured.update(source=self.source, counter=self.counter, sink=self.sink)
            ctx.link(self.source.pulses, self.counter.input)
            ctx.link(self.counter.snapshots, self.sink.input)
            ctx.subscribe(self.counter.snapshots, ctx.actor)
        @handles(CountSnapshot)
        def snapshot(self, event, ctx): captured["python"] = captured.get("python", 0) + 1
    with World() as world:
        world.spawn(Monitor); world.step(); before = captured["counter"].stats()
        time.sleep(.06)
        after = captured["counter"].stats()
        assert after["processed"] > before["processed"]
        assert captured.get("python", 0) == 0
        world.step(); assert captured.get("python", 0) > 0

def test_component_owner_state_isolated_and_driver_affine():
    proxies = []; results = []
    class Worker(Component):
        @handles(Pulse)
        def pulse(self, event, ctx):
            self.total = getattr(self, "total", 0) + event.value
            results.append((ctx.ref.handle, self.total, threading.get_ident()))
    class Parent(Actor):
        worker = component(Worker)
        def configure(self, ctx): proxies.append(self.worker)
    with World() as world:
        a = world.spawn(Parent); b = world.spawn(Parent); world.step()
        proxies[0].pulse.send(Pulse(3)); proxies[1].pulse.send(Pulse(7)); world.step()
        assert [(x[0], x[1]) for x in results] == [(proxies[0].owner.handle, 3), (proxies[1].owner.handle, 7)]
        proxies[0].pulse.send(Pulse(2)); world.step()
        assert results[-1][1] == 5
        assert {x[2] for x in results} == {threading.get_ident()}

def test_invalid_link_and_required_interface_fail_before_activation():
    required = Interface("missing", 1)
    class Monitor(Actor):
        source = component(native.PulseSource, interval=.1, requires=(required,))
        def configure(self, ctx): ctx.link(self.source.pulses, ctx.actor)
    world = World()
    with pytest.raises((TypeError, ValueError)):
        world.spawn(Monitor)
    assert world.inspect()["python_registrations"] == 0
    world.close()

def test_component_startup_failure_rolls_back_native_tasks():
    class Broken(Actor):
        source = component(native.PulseSource, interval=.01)
        def configure(self, ctx): raise ValueError("component startup")
    world = World(); world.spawn(Broken)
    with pytest.raises(ValueError, match="component startup"): world.step()
    world.close()
    assert world.inspect()["active_tasks"] == 0

def test_component_configuration_is_immutable_and_bounded():
    interface = Interface("metrics", 1, outputs=(("snapshot", CountSnapshot),))
    descriptor = component(native.WindowCounter, window=.01, requires=(interface,))
    with pytest.raises(Exception): descriptor.config = ()
    with pytest.raises((TypeError, ValueError)):
        component(native.WindowCounter, window=.01, payload=[1])
