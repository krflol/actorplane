"""Exercise real timers and controlled native replies without wall-clock waits."""

from actorplane import Actor, MessageOptions, Pulse, handles
from actorplane.testing import TestWorld


def main() -> None:
    seen = []

    class Timer(Actor):
        def on_start(self, ctx):
            ctx.after(0.01, Pulse(1))

        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append((event.value, ctx.envelope.enqueued_at_ns))

    with TestWorld(max_tasks=8, max_operations=8) as world:
        owner = world.spawn(Timer)
        world.run_until_idle()
        assert world.now() == 0 and seen == []
        world.advance(0.01)
        assert seen == [(1, 10_000_000)]

        responder = world.native_responder(Pulse)
        operation = world.request(owner, responder, Pulse(2), timeout=0.1,
                                  options=MessageOptions(correlation_id=42))
        world.run_until_idle()
        assert operation.poll() is None and world.pending_native()
        assert world.complete(operation, Pulse(3))
        world.run_until_idle()
        result = operation.take()
        assert result.value == Pulse(3)
        assert result.envelope.correlation_id == 42
        assert world.now() == 0.01
        report = world.close(mode="drain")
        assert report["native_done"] and report["python_done"]
        assert not report["timed_out"]
        assert world.inspect()["retained_payload_bytes"] == 0
    print("virtual timer at 10 ms; controlled reply completed; retained bytes=0")


if __name__ == "__main__":
    main()
