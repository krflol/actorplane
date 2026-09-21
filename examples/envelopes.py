"""Trace a forwarded message and a request using native envelope snapshots."""

from actorplane import Actor, MessageOptions, Pulse, TraceContext, World, handles


def main():
    seen = []
    references = {}

    class Forward(Actor):
        @handles(Pulse)
        def receive(self, event, ctx):
            seen.append(ctx.envelope)
            ctx.send(references["sink"], Pulse(event.value + 1))

    class Sink(Actor):
        @handles(Pulse)
        def receive(self, event, ctx):
            seen.append(ctx.envelope)

    class Echo(Actor):
        @handles(Pulse)
        def receive(self, event, ctx):
            assert ctx.reply(event)

    trace = TraceContext(b"t" * 16, b"s" * 8, baggage=(("example", "envelopes"),))
    options = MessageOptions(timeout=1, correlation_id=42, trace=trace)
    with World() as world:
        forward = world.spawn(Forward)
        references["sink"] = world.spawn(Sink)
        echo = world.spawn(Echo)
        world.step()
        event_id = forward.send(Pulse(5), options=options)
        world.step()
        world.step()
        parent, child = seen
        assert parent.event_id == event_id
        assert child.source == forward
        assert child.causation_id == parent.event_id
        assert child.correlation_id == parent.correlation_id == 42
        assert child.deadline_ns == parent.deadline_ns
        assert child.trace == trace

        request = world.request(forward, echo, Pulse(7), options=options)
        world.step()
        outcome = request.take()
        assert outcome.state == "Completed" and outcome.value == Pulse(7)
        assert outcome.envelope.source == echo
        assert outcome.envelope.correlation_id == 42
        assert outcome.envelope.trace == trace
        print(f"forwarded: {parent.event_id} -> {child.event_id}; correlation=42")
        print(f"reply: {outcome.envelope.event_id}; trace baggage={trace.baggage}")
        report = world.close(mode="drain")
        assert report["native_done"] and report["python_done"]
        assert world.inspect()["retained_payload_bytes"] == 0


if __name__ == "__main__":
    main()
