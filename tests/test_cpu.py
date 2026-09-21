import pytest

from actorplane import Actor, CountSnapshot, Pulse, World
from actorplane.cpu import native_sum_squares, stats
from actorplane.testing import TestWorld


def _drive(world, operation):
    for _ in range(200):
        if operation.poll() is not None:
            return operation
        world.run_for(0.005)
    raise AssertionError("CPU operation did not complete")


@pytest.mark.parametrize("n", [0, 1, 7, 1_000_000])
def test_native_sum_squares_exact_result_and_stats(n):
    class Owner(Actor):
        pass

    world = World(cpu_workers=2, max_cpu_jobs=32)
    owner = world.spawn(Owner)
    world.run_for(0.01)
    target = native_sum_squares(world, parent=owner)
    operation = world.request(owner, target, Pulse(n), timeout=1)
    _drive(world, operation)
    result = operation.result()
    assert isinstance(result, CountSnapshot)
    assert (result.count, result.total) == (n, n * (n + 1) * (2 * n + 1) // 6)
    report = world.close()
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert world.inspect()["retained_payload_bytes"] == 0
    counters = stats(world)
    assert counters["submitted"] == counters["completed"] == 1
    assert counters["queued"] == counters["running"] == 0


def test_cpu_configuration_and_virtual_world_rejection():
    class Owner(Actor):
        pass

    with pytest.raises((ValueError, RuntimeError)):
        World(cpu_workers=0)
    with pytest.raises((ValueError, RuntimeError)):
        World(max_cpu_jobs=0)
    with pytest.raises((ValueError, RuntimeError, TypeError)):
        World(cpu_workers=True)
    with pytest.raises((ValueError, RuntimeError, TypeError)):
        World(max_cpu_jobs=True)
    with pytest.raises((ValueError, RuntimeError, TypeError)):
        World(cpu_workers=65)
    with pytest.raises((ValueError, RuntimeError, TypeError)):
        World(max_cpu_jobs=4097)
    world = TestWorld()
    owner = world.spawn(Owner)
    world.step()
    with pytest.raises(RuntimeError, match="TestWorld"):
        native_sum_squares(world, parent=owner)
    world.close()


def test_cpu_parent_stop_cancels_operation_and_rejects_cross_world_parent():
    class Owner(Actor):
        pass

    first = World(cpu_workers=1, max_cpu_jobs=1)
    second = World()
    owner = first.spawn(Owner)
    requester = first.spawn(Owner)
    other = second.spawn(Owner)
    first.run_for(0.01)
    second.run_for(0.01)
    with pytest.raises(ValueError, match="another World"):
        native_sum_squares(first, parent=other)
    target = native_sum_squares(first, parent=owner)
    operation = first.request(requester, target, Pulse(1_000_000), timeout=1)
    first.stop(owner)
    _drive(first, operation)
    outcome = operation.take()
    assert outcome.state in {"TargetStopped", "Completed"}
    if outcome.state == "Completed":
        assert outcome.value == CountSnapshot(1_000_000, 333_333_833_333_500_000)
    report = first.close()
    assert report["native_done"] and report["python_done"]
    assert first.inspect()["retained_payload_bytes"] == 0
    second.close()


@pytest.mark.parametrize("n", [-1, 1_000_001])
def test_cpu_input_range_fails_with_bounded_diagnostic_and_cleanup(n):
    with World() as world:
        owner = world.spawn(Actor)
        world.step()
        target = native_sum_squares(world)
        operation = world.request(owner, target, Pulse(n), timeout=1)
        _drive(world, operation)
        assert operation.take().state in {"Failed", "TargetStopped"}
        failures = [entry["failure"] for entry in world.diagnostics()["entries"] if entry["failure"]]
        assert len(failures) == 1
        assert failures[0]["exception_type"] == "CpuInputRange"
        report = world.close()
        assert report["native_done"] and report["python_done"]
        assert world.inspect()["retained_payload_bytes"] == 0
