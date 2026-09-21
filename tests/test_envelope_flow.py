"""Envelope propagation, retention, and expiry through the public API."""
import threading

import pytest

from actorplane import (
    Actor, Component, MessageOptions, Output, Pulse, TraceContext, World,
    component, event, handles, _native,
)
from actorplane.testing import TestWorld

TRACE = TraceContext(b"a" * 16, b"b" * 8, True, (("tenant", "blue"),))


def test_context_send_inherits_metadata_without_extending_deadline():
    seen = []

    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)

    class Source(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)
            ctx.send(target, Pulse(2), options=MessageOptions(timeout=20))

    with World() as world:
        source, target = world.spawn(Source), world.spawn(Target)
        world.step()
        source.send(Pulse(1), options=MessageOptions(timeout=1, correlation_id=17, trace=TRACE))
        world.step()
        world.step()
    parent, child = seen
    assert parent.source is None
    assert child.source == source and child.destination == target
    assert child.event_id != parent.event_id
    assert child.correlation_id == parent.correlation_id == 17
    assert child.causation_id == parent.event_id
    assert child.deadline_ns == parent.deadline_ns
    assert child.trace == TRACE


def test_after_inherits_metadata_after_original_claim_finishes():
    seen = []

    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)

    class Source(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)
            ctx.after(.005, Pulse(2), target=target)

    with World() as world:
        source, target = world.spawn(Source), world.spawn(Target)
        world.step()
        source.send(Pulse(1), options=MessageOptions(timeout=1, trace=TRACE))
        world.run_for(.1)
        assert world.inspect()["retained_payload_bytes"] == 0
    parent, timer = seen
    assert timer.source == source
    assert timer.correlation_id == timer.causation_id == parent.event_id
    assert timer.deadline_ns == parent.deadline_ns
    assert timer.trace == TRACE


def test_reply_after_handler_retains_native_request_context_until_terminal():
    saved = []

    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            saved.append(ctx)

    with World() as world:
        owner, target = world.spawn(Actor), world.spawn(Target)
        world.step()
        operation = world.request(owner, target, Pulse(1), options=MessageOptions(trace=TRACE))
        world.step()
        request = saved[0].envelope
        assert operation.poll() is None
        assert request.correlation_id == request.event_id
        assert world.inspect()["retained_payload_bytes"] == 43
        assert saved[0].reply(Pulse(9))
        result = operation.take()
        assert result.value == Pulse(9)
        assert result.envelope.source == target and result.envelope.destination == owner
        assert result.envelope.correlation_id == result.envelope.causation_id == request.event_id
        assert result.envelope.deadline_ns == request.deadline_ns
        assert result.envelope.trace == TRACE
        assert world.inspect()["retained_payload_bytes"] == 0


def test_context_request_inherits_metadata_into_reply():
    contexts, outcomes = [], []

    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            contexts.append(ctx.envelope)
            assert ctx.reply(Pulse(3))

    class Source(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            contexts.append(ctx.envelope)
            outcomes.append(ctx.request(target, Pulse(1), timeout=20))

    with World() as world:
        source, target = world.spawn(Source), world.spawn(Target)
        world.step()
        source.send(Pulse(0), options=MessageOptions(timeout=1, correlation_id=41, trace=TRACE))
        world.step()
        world.step()
        parent, request = contexts
        result = outcomes[0].take()
        assert result.value == Pulse(3)
        assert request.source == source
        assert request.causation_id == parent.event_id
        assert result.envelope.causation_id == request.event_id
        assert request.correlation_id == result.envelope.correlation_id == 41
        assert parent.deadline_ns == request.deadline_ns == result.envelope.deadline_ns
        assert result.envelope.trace == request.trace == TRACE


def test_queued_and_timer_payloads_expire_while_gil_is_held():
    if not hasattr(_native, "_hold_gil"):
        pytest.skip("test-support extension unavailable")
    seen = []

    class Target(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(event)

    class Source(Actor):
        def on_start(self, ctx):
            ctx.after(10, Pulse(2), target, options=MessageOptions(timeout=.03, trace=TRACE))

    with World() as world:
        target = world.spawn(Target)
        world.spawn(Source)
        world.step()
        event_id = target.send(Pulse(1), options=MessageOptions(timeout=.03, correlation_id=77))
        _native._hold_gil(150)
        snapshot = world.inspect()
        assert snapshot["metrics"]["expired"] == 1
        assert snapshot["retained_payload_bytes"] == snapshot["active_tasks"] == 0
        entry = next(e for e in world.diagnostics()["entries"] if e["code"] == "DeadlineExpired")
        assert entry["event_id"] == event_id and entry["correlation_id"] == 77
        world.step()
        assert seen == []


def test_output_fanout_has_unique_ids_and_exact_port_provenance():
    seen, parents, ports = [], [], {}

    class Source(Component):
        output = Output(Pulse)

        @handles(Pulse)
        def pulse(self, event, ctx):
            parents.append(ctx.envelope)
            self.output.emit(event)

    class Sink(Component):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(ctx.envelope)

    class Parent(Actor):
        source = component(Source)
        first = component(Sink)
        second = component(Sink)

        def configure(self, ctx):
            ports.update(source=self.source, first=self.first, second=self.second)
            ctx.link(self.source.output, self.first.pulse)
            ctx.link(self.source.output, self.second.pulse)

    with TestWorld() as world:
        world.spawn(Parent)
        world.step()
        ports["source"].pulse.send(Pulse(1), options=MessageOptions(correlation_id=81, trace=TRACE))
        world.run_until_idle()
    assert len(seen) == 2
    assert seen[0].event_id != seen[1].event_id
    assert {e.destination_port for e in seen} == {ports["first"].pulse.handle, ports["second"].pulse.handle}
    for envelope in seen:
        assert envelope.dispatcher == ports["source"].output.handle
        assert envelope.source == ports["source"].owner
        assert envelope.causation_id == parents[0].event_id
        assert envelope.correlation_id == 81 and envelope.trace == TRACE


def test_port_send_distinguishes_external_thread_from_current_handler():
    seen, ports, thread_errors = {}, {}, []

    class Sink(Component):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen[event.value] = ctx.envelope

    class Parent(Actor):
        sink = component(Sink)

        def configure(self, ctx):
            ports["input"] = self.sink.pulse

    class Source(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            ports["input"].send(Pulse(2), options=MessageOptions(timeout=20))

            def send_external():
                try:
                    ports["input"].send(Pulse(3), options=MessageOptions(correlation_id=93))
                except BaseException as error:
                    thread_errors.append(error)

            thread = threading.Thread(target=send_external)
            thread.start()
            thread.join(1)
            assert not thread.is_alive()

    with World() as world:
        world.spawn(Parent)
        source = world.spawn(Source)
        world.step()
        ports["input"].send(Pulse(1), options=MessageOptions(correlation_id=91))
        source.send(Pulse(0), options=MessageOptions(correlation_id=92, trace=TRACE))
        for _ in range(4):
            world.step()
    assert not thread_errors
    assert seen[1].source is seen[3].source is None
    assert seen[2].source == source and seen[2].correlation_id == 92
    assert seen[2].trace == TRACE
    assert seen[3].correlation_id == 93 and seen[3].trace is None


def test_structured_envelope_reports_registered_schema_version():
    seen = []

    @event("envelope.structured", 4)
    class Structured:
        value: str

    class Target(Actor):
        @handles(Structured)
        def receive(self, event, ctx):
            seen.append(ctx.envelope)

    with World() as world:
        target = world.spawn(Target)
        world.step()
        target.send(Structured("test"))
        world.step()
    assert seen[0].schema_kind == "structured"
    assert seen[0].schema_version == 4
    assert seen[0].destination_port[:3] == target.handle
    assert seen[0].owner == target
