"""Two actors independently lease one World-owned native CPU service."""
from actorplane import Actor, CountSnapshot, Interface, Pulse, World
from actorplane.cpu import native_sum_squares
from actorplane.services import register


def calculate(world, lease, n):
    operation = lease.request("requests", Pulse(n), timeout=2)
    for _ in range(400):
        if operation.poll() is not None:
            break
        world.run_for(.005)
    assert operation.result() == CountSnapshot(n, n * (n + 1) * (2 * n + 1) // 6)


with World() as world:
    first, second = world.spawn(Actor), world.spawn(Actor)
    world.step()
    service = register(world, "calculator", native_sum_squares(world),
                       Interface("actorplane.CpuSumSquares", inputs=(("requests", Pulse),)))
    left, right = service.acquire(first), service.acquire(second)
    calculate(world, left, 7)
    world.stop(first)
    assert world.inspect()["services"][0]["leases"] == 1
    calculate(world, right, 8)
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert world.inspect()["retained_payload_bytes"] == 0
    assert world.inspect()["service_leases"] == []
    print("shared CPU service; one holder stopped; other completed; retained bytes=0")
