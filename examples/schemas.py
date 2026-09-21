"""Nested typed records in request/reply and a Rust-owned timer."""
from enum import Enum
from typing import Annotated

from actorplane import Actor, IntRange, Length, World, event, handles


class Status(Enum):
    READY = "ready"
    WAITING = "waiting"


@event("example.Device")
class Device:
    identifier: Annotated[int, IntRange(0, (1 << 64) - 1)]
    label: Annotated[str, Length(64)]


@event("example.Reading")
class Reading:
    device: Device
    samples: Annotated[tuple[float, ...], Length(16)]
    status: Status
    attachment: Annotated[bytes, Length(64)]
    note: str | None


def main():
    observed = []
    reading = Reading(Device((1 << 64) - 1, "sensor"), (1.5, 2.5), Status.READY, b"native", None)

    class Recorder(Actor):
        def on_start(self, ctx):
            ctx.after(.01, reading)

        @handles(Reading)
        def consume(self, value, ctx):
            observed.append(value)
            if ctx.operation is not None:
                assert ctx.reply(value)

    with World() as world:
        owner, target = world.spawn(Actor), world.spawn(Recorder)
        world.step()
        result = world.request(owner, target, reading)
        world.step()
        assert result.result() == reading
        world.run_for(.1)
        assert observed == [reading, reading]
        print(f"{reading.device.label}: {len(observed)} structured deliveries, samples={reading.samples}")
    assert world.inspect()["retained_payload_bytes"] == 0


if __name__ == "__main__":
    main()
