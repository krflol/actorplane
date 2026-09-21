"""Boundary and lifecycle regressions for reusable components."""
from dataclasses import FrozenInstanceError
import pytest
from actorplane import (Actor, Component, CountSnapshot, Pulse, World, Output,
                        Interface, component, handles, native, AdmissionError,
                        CodecError, AffinityError, LifecycleError,
                        PublicationStatus, PublicationOutcome)
from actorplane import _native


def test_startup_output_is_staged_until_all_hooks_succeed():
    seen, reports = [], []
    class Producer(Component):
        output = Output(Pulse)
        def on_start(self, ctx):
            ticket = self.output.emit(Pulse(4))
            reports.append((ticket, ticket.status()))
            assert not seen
    class Parent(Actor):
        source = component(Producer)
        def configure(self, ctx):
            ctx.subscribe(self.source.output, ctx.actor)
        def on_start(self, ctx):
            assert not seen
        @handles(Pulse)
        def input(self, event, ctx):
            seen.append(event.value)
    with World() as world:
        world.spawn(Parent)
        world.step()
        assert reports[0][1] is PublicationStatus.STAGED
        for _ in range(4):
            world.step()
            if seen:
                break
        assert seen == [4]
        assert reports[0][0].report().admitted == 1
        assert reports[0][0].report().outcome is PublicationOutcome.ROUTED
        assert world.inspect()["retained_payload_bytes"] == 0
        assert all(a["staged_entries"] == 0 for a in world.inspect()["actors"])


def test_failed_component_start_discards_staged_outputs_and_cleans_child_first():
    hooks, seen, tickets = [], [], []
    class Producer(Component):
        output = Output(Pulse)
        def on_start(self, ctx):
            tickets.append(self.output.emit(Pulse(4)))
            raise ValueError("failed after staging")
        def on_stop(self, ctx): hooks.append("child")
    class Parent(Actor):
        source = component(Producer)
        def configure(self, ctx): ctx.subscribe(self.source.output, ctx.actor)
        @handles(Pulse)
        def input(self, event, ctx): seen.append(event.value)
        def on_stop(self, ctx): hooks.append("parent")
    world = World()
    world.spawn(Parent)
    with pytest.raises(ValueError, match="failed after staging"):
        world.step()
    report = world.close()
    assert hooks == ["child", "parent"]
    assert not seen and report["native_done"] and report["python_done"]
    assert tickets[0].status() is PublicationStatus.CANCELLED
    assert tickets[0].report().outcome is PublicationOutcome.CANCELLED
    assert world.inspect()["retained_payload_bytes"] == 0
    assert world.inspect()["metrics"]["discarded"] == 1


@pytest.mark.parametrize("native_sink", [False, True])
def test_python_and_native_sink_share_contract_and_drain_completions(native_sink):
    observed, proxies = [], {}
    class PythonSink(Component):
        implements = native.SnapshotSink.implements
        @handles(CountSnapshot)
        def input(self, event, ctx): observed.append(event.total)
    class Producer(Component):
        output = Output(CountSnapshot)
        @handles(Pulse)
        def input(self, event, ctx): self.output.emit(CountSnapshot(1, event.value))
    class Parent(Actor):
        sink = component(native.SnapshotSink if native_sink else PythonSink)
        source = component(Producer, depends_on=("sink",), requires=native.SnapshotSink.implements)
        def configure(self, ctx):
            proxies.update(source=self.source, sink=self.sink)
            ctx.link(self.source.output, self.sink.input)
    world = World()
    world.spawn(Parent)
    world.step()
    proxies["source"].input.send(Pulse(5))
    proxies["source"].input.send(Pulse(6))
    report = world.close(mode="drain", timeout=1)
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert world.inspect()["retained_payload_bytes"] == 0
    if native_sink:
        assert proxies["sink"].stats()["sink_received"] == 2
    else:
        assert observed == [5, 6]


def test_incompatible_interfaces_rejected_natively_before_ingress():
    class First(Component):
        implements = (Interface("tests.conflict", inputs=(("input", Pulse),)),)
        @handles(Pulse)
        def input(self, event, ctx): pass
    class Second(Component):
        implements = (Interface("tests.conflict", inputs=(("input", CountSnapshot),)),)
        @handles(CountSnapshot)
        def input(self, event, ctx): pass
    with World() as world:
        world.spawn(First)
        with pytest.raises(RuntimeError, match="InterfaceMismatch"):
            world.spawn(Second)
        assert world.inspect()["python_registrations"] == 1


def test_dependencies_and_native_configuration_validate_before_any_allocation():
    class Cycle(Actor):
        a = component(native.SnapshotSink, depends_on=("b",))
        b = component(native.SnapshotSink, depends_on=("a",))
    class BadConfiguration(Actor):
        source = component(native.PulseSource, interval=0)
    with World() as world:
        for typ in (Cycle, BadConfiguration):
            with pytest.raises(ValueError): world.spawn(typ)
        assert world.inspect()["actors"] == []


def test_ports_enforce_type_owner_generation_and_link_token_lifetime():
    proxies, seen = {}, []
    class Source(Component): output = Output(Pulse)
    class Parent(Actor):
        source = component(Source)
        def configure(self, ctx):
            proxies.update(source=self.source, token=ctx.subscribe(self.source.output, ctx.actor))
        @handles(Pulse)
        def input(self, event, ctx): seen.append(event.value)
    with World(max_actors=2) as world:
        ref = world.spawn(Parent)
        world.step()
        port = proxies["source"].output
        with pytest.raises(CodecError): port.emit(CountSnapshot(1, 3))
        port.emit(Pulse(7))
        assert proxies["token"].cancel()
        assert not proxies["token"].cancel()
        world.step()
        assert seen == []
        world.stop(ref)
        replacement = world.spawn(Parent)
        world.step()
        assert replacement.generation != ref.generation
        with pytest.raises(AdmissionError): port.emit(Pulse(8))
        with pytest.raises(FrozenInstanceError): proxies["source"].owner = ref


def test_native_links_make_progress_while_gil_is_held():
    if not hasattr(_native, "_hold_gil"):
        pytest.skip("requires test-support development extension")
    proxies = {}
    class Parent(Actor):
        source = component(native.PulseSource, interval=.001)
        counter = component(native.WindowCounter, window=.005)
        sink = component(native.SnapshotSink)
        def configure(self, ctx):
            proxies.update(sink=self.sink)
            ctx.link(self.source.pulses, self.counter.input)
            ctx.link(self.counter.snapshots, self.sink.input)
    with World() as world:
        world.spawn(Parent)
        world.step()
        before = proxies["sink"].stats()["sink_received"]
        _native._hold_gil(120)
        assert proxies["sink"].stats()["sink_received"] > before
        assert len(world.inspect()["links"]) == 2


def test_staging_is_bounded_and_failed_start_releases_every_payload():
    class Source(Component):
        output = Output(Pulse)
        def on_start(self, ctx):
            self.output.emit(Pulse(1))
            self.output.emit(Pulse(2))
    class Parent(Actor): source = component(Source)
    world = World(mailbox_capacity=1)
    world.spawn(Parent)
    with pytest.raises(AdmissionError, match="QueueFull"): world.step()
    world.close()
    assert world.inspect()["retained_payload_bytes"] == 0


def test_undeclared_input_rejected_before_queue_admission():
    with World() as world:
        target = world.spawn(Actor)
        world.step()
        with pytest.raises(AdmissionError, match="SchemaMismatch"): target.send(Pulse(1))
        assert world.inspect()["metrics"]["admitted"] == 0

def test_required_interface_is_checked_against_native_instance_not_marker_duck_type():
    fake = Interface("tests.not_implemented")
    class Pretender(native.PulseSource):
        implements = (fake,)
    class Parent(Actor):
        source = component(Pretender, interval=.1)
        sink = component(native.SnapshotSink, depends_on=("source",), requires=(fake,))
    world = World()
    world.spawn(Parent)
    with pytest.raises(LifecycleError, match="native contract lacks"):
        world.step()
    assert world.close()["native_done"]
    assert world.inspect()["subscriptions"] == 0


def test_native_component_preparation_failure_cleans_previous_children():
    class Parent(Actor):
        first = component(native.SnapshotSink)
        second = component(native.SnapshotSink)
    world = World(max_tasks=1)
    world.spawn(Parent)
    with pytest.raises(RuntimeError, match="native task limit"):
        world.step()
    report = world.close()
    assert report["native_done"] and report["python_done"]
    assert world.inspect()["python_registrations"] == 0
    assert all(a["state"] == "Stopped" for a in world.inspect()["actors"])

def test_component_stopped_during_parent_start_cannot_begin_startup_hooks():
    callbacks = []
    class Worker(Component):
        def configure(self, ctx): callbacks.append("configure")
        def on_start(self, ctx): callbacks.append("start")
        def on_stop(self, ctx): callbacks.append("stop")
    class Parent(Actor):
        worker = component(Worker)
        def on_start(self, ctx): ctx.world.stop(self.worker.owner)
    world = World()
    world.spawn(Parent)
    with pytest.raises(LifecycleError, match="stopped before startup"):
        world.step()
    assert callbacks == ["stop"]
    assert world.close()["python_done"]
