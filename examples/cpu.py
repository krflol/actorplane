"""Finite native CPU operation example."""

from actorplane import Actor, CountSnapshot, Pulse, World
from actorplane.cpu import native_sum_squares, stats


class Owner(Actor):
    pass


with World(cpu_workers=2, max_cpu_jobs=32) as world:
    owner = world.spawn(Owner)
    world.step()
    calculator = native_sum_squares(world, parent=owner)
    operation = world.request(owner, calculator, Pulse(7), timeout=1.0)
    while operation.poll() is None:
        world.run_for(0.005)
    result = operation.result()
    assert isinstance(result, CountSnapshot)
    assert result.count == 7 and result.total == 140
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert world.inspect()["retained_payload_bytes"] == 0
    assert stats(world)["queued"] == stats(world)["running"] == 0
    print("sum_squares(7)=140; native CPU completion; retained bytes=0")
