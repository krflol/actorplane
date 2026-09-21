"""Native networking evidence while the calling thread actually holds the GIL."""
import pytest
from actorplane import Actor, World, _native
from actorplane.tcp import listen, native_echo


def test_native_tcp_round_trips_and_stops_inside_held_gil_interval():
    probe = getattr(_native, "_held_gil_tcp_probe", None)
    if probe is None:
        pytest.skip("requires test-support extension")
    with World() as world:
        owner = world.spawn(Actor)
        world.step()
        echo = native_echo(world, owner)
        listener = listen(world, owner, echo)
        report = probe(world._native, listener._native)
        assert report["progress_in_hold"] == 8
        assert report["hold_start_ns"] <= report["first_progress_ns"] <= report["last_progress_ns"] < report["hold_end_ns"]
        assert report["native_stopped_in_hold"]
        assert report["bridge_entries_during_hold"] == 0
        assert report["read_buffer_bytes"] == report["write_buffer_bytes"] == 0
        assert listener.stats()["frames_written"] == 8
        assert listener.connections() == ()
        closed = world.close()
        assert closed["native_done"] and closed["python_done"]
        assert world.inspect()["retained_payload_bytes"] == 0
