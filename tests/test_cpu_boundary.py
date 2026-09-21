import actorplane._native as native
import pytest


def test_cpu_workers_continue_while_python_thread_holds_gil():
    if not hasattr(native, "_cpu_gil_probe"):
        pytest.skip("requires test-support extension")
    report = native._cpu_gil_probe(120)
    assert report["completed"] == report["expected"] == 8
    assert report["native_stopped_in_hold"]
    assert report["bridge_entries_during_hold"] == 0
    assert report["native_done"] and report["retained_payload_bytes"] == 0
    assert report["hold_start_ns"] <= report["first_progress_ns"] <= report["last_progress_ns"] < report["hold_end_ns"]
