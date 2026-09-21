"""Observe publication ingress and routing with an explicitly pumped World."""
from actorplane import (
    Actor, Component, Output, PublicationOutcome, PublicationStatus, Pulse,
    component, handles,
)
from actorplane.testing import TestWorld


received = []
ports = {}


class Producer(Component):
    output = Output(Pulse)


class Consumer(Component):
    @handles(Pulse)
    def input(self, event, ctx):
        received.append(event.value)


class Pipeline(Actor):
    producer = component(Producer)
    left = component(Consumer)
    right = component(Consumer)

    def configure(self, ctx):
        ctx.link(self.producer.output, self.left.input)
        ctx.link(self.producer.output, self.right.input)
        ports["output"] = self.producer.output


with TestWorld(routing_batch_size=1) as world:
    world.spawn(Pipeline)
    world.step()  # Construct, configure, and activate the components.
    ticket = ports["output"].emit(Pulse(7))
    assert ticket.status() is PublicationStatus.QUEUED
    assert ticket.report() is None
    assert received == []
    print(f"ticket {ticket.id}: {ticket.status().value}")

    # Real World routes on native workers; TestWorld advances only when pumped.
    world.run_until_idle()
    report = ticket.report()
    assert report is not None and report.outcome is PublicationOutcome.ROUTED
    assert (report.matched, report.admitted, report.rejected) == (2, 2, 0)
    # Routing admission alone does not prove handler execution. Check it here.
    assert received == [7, 7]
    assert world.inspect()["retained_payload_bytes"] == 0
    print(f"{report.outcome.value}: {report.admitted}/{report.matched} admitted")

# Terminal reports retain metadata, survive close, and consume a ticket slot
# until their handles are dropped. They never retain the event payload.
assert ticket.report() == report
print(f"handlers observed: {received}")
