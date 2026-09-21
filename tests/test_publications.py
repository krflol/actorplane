import gc

import pytest
import actorplane.authoring as authoring

from actorplane import (
    Actor, AdmissionError, Component, Output, PublicationOutcome,
    PublicationStatus, Pulse, World, component, handles,
)
from actorplane.testing import TestWorld


def test_ticket_queued_then_routed_without_emit_pumping():
    captured = {}
    startup = []
    class Producer(Component):
        output = Output(Pulse)
        def on_start(self, ctx):
            ticket = self.output.emit(Pulse(1))
            assert ticket.status() is PublicationStatus.STAGED
            startup.append(ticket)
    class Sink(Actor):
        source = component(Producer)
        def configure(self, ctx):
            captured["output"] = self.source.output
            ctx.subscribe(self.source.output, ctx.actor)
        @handles(Pulse)
        def input(self, event, ctx):
            pass
    world = TestWorld()
    world.spawn(Sink)
    world.step()
    ticket = captured["output"].emit(Pulse(7))
    assert ticket.status() is PublicationStatus.QUEUED
    assert ticket.report() is None
    world.run_until_idle()
    assert startup[0].report() is not None
    assert ticket.status() is PublicationStatus.ROUTED
    assert ticket.report().outcome is PublicationOutcome.ROUTED
    world.close()


def test_publication_quota_counts_retained_tickets_until_drop():
    captured = {}
    class Producer(Component):
        output = Output(Pulse)
    class Owner(Actor):
        producer = component(Producer)
        def configure(self, ctx):
            captured["output"] = self.producer.output
    world = TestWorld(max_publications=2, max_publications_per_source=2)
    world.spawn(Owner)
    world.step()
    tickets = [captured["output"].emit(Pulse(1)), captured["output"].emit(Pulse(2))]
    with pytest.raises(AdmissionError):
        captured["output"].emit(Pulse(3))
    assert world.inspect()["publications_pending"] == 2
    world.run_until_idle()
    assert all(ticket.report() is not None for ticket in tickets)
    assert world.inspect()["publications_retained"] == 2
    with pytest.raises(AdmissionError):
        captured["output"].emit(Pulse(3))
    del tickets[:]
    gc.collect()
    assert world.inspect()["publications_retained"] == 0
    replacement = captured["output"].emit(Pulse(4))
    assert replacement.status() is PublicationStatus.QUEUED
    world.close()


def test_fanout_limit_is_terminal_outcome():
    captured = {}
    class Producer(Component):
        output = Output(Pulse)
    class Sink(Component):
        @handles(Pulse)
        def input(self, event, ctx):
            pass
    class Owner(Actor):
        producer = component(Producer)
        left = component(Sink)
        right = component(Sink)
        def configure(self, ctx):
            captured["output"] = self.producer.output
            ctx.link(self.producer.output, self.left.input)
            ctx.link(self.producer.output, self.right.input)
    world = TestWorld(max_publication_fanout=1)
    world.spawn(Owner)
    world.step()
    ticket = captured["output"].emit(Pulse(9))
    world.run_until_idle()
    assert ticket.report().outcome is PublicationOutcome.FANOUT_LIMIT
    world.close()


def test_ticket_identity_and_report_survive_world_disposal():
    captured = {}
    class Producer(Component):
        output = Output(Pulse)
    class Owner(Actor):
        producer = component(Producer)
        def configure(self, ctx):
            captured["output"] = self.producer.output
    world = TestWorld()
    world.spawn(Owner)
    world.step()
    ticket = captured["output"].emit(Pulse(4))
    world.run_until_idle()
    world.close()
    del world
    gc.collect()
    assert ticket.status() is PublicationStatus.ROUTED
    assert ticket.report().outcome is PublicationOutcome.ROUTED
    assert ticket.source.world_id > 0


def test_snapshot_and_stop_report_expose_publication_fields():
    captured = {}
    class Producer(Component):
        output = Output(Pulse)
    class Owner(Actor):
        producer = component(Producer)
        def configure(self, ctx):
            captured["output"] = self.producer.output
    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    routed = captured["output"].emit(Pulse(1))
    world.run_until_idle()
    assert routed.report() is not None
    ticket = captured["output"].emit(Pulse(2))
    snapshot = world.inspect()
    assert {"publications_pending", "publications_retained", "routing_snapshot_entries"} <= snapshot.keys()
    assert snapshot["publications_pending"] == 1
    assert snapshot["publications_retained"] == 1
    assert snapshot["routing_snapshot_entries"] == 0
    report = world.stop(owner)
    assert {"outstanding_publications", "retained_publications"} <= report.keys()
    world.run_until_idle()
    assert ticket.report().outcome is PublicationOutcome.CANCELLED
    assert world.stop_report()["outstanding_publications"] == 0
    assert world.stop_report()["retained_publications"] == 2
    world.close()


def test_ticket_getters_fence_process_before_native_access(monkeypatch):
    captured = {}
    class Producer(Component):
        output = Output(Pulse)
    class Owner(Actor):
        producer = component(Producer)
        def configure(self, ctx):
            captured["output"] = self.producer.output
    world = World()
    world.spawn(Owner)
    world.step()
    ticket = captured["output"].emit(Pulse(3))
    world.close()
    del world
    gc.collect()
    assert ticket.report() is not None
    monkeypatch.setattr(authoring.os, "getpid", lambda: ticket._pid + 1)
    with pytest.raises(RuntimeError, match="another process"):
        ticket.status()
    with pytest.raises(RuntimeError, match="another process"):
        ticket.report()
    with pytest.raises(RuntimeError, match="another process"):
        _ = ticket.id
    with pytest.raises(RuntimeError, match="another process"):
        _ = ticket.source
