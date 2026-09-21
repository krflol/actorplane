"""Shutdown preserves active Python-owned leases until the owner finishes them."""

from actorplane import _native as native


def _world():
    return native.NativeWorld(
        max_actors=8,
        mailbox_capacity=4,
        mailbox_bytes=4096,
        native_payload_budget=4096,
        max_subscriptions=8,
        max_event_bytes=1024,
        max_tasks=2,
        max_operations=8,
        diagnostic_capacity=8,
    )


def test_close_reports_active_business_lease_until_finish():
    world = _world()
    actor = world.allocate(False, None)
    world.activate(actor)
    world.send(actor, "pulse", [1])
    claim = world.claim(actor)
    assert claim is not None
    token = claim[0]

    report = world.close(timeout_ms=1)
    assert not report["native_done"]
    assert report["in_flight"] >= 1
    assert report["delivery_in_flight"] == 1
    assert report["control_in_flight"] == 0
    assert report["native_tasks"] == 0

    world.finish(token, True)
    report = world.close(timeout_ms=1)
    assert report["native_done"]


def test_close_reports_active_failure_lease_until_finish():
    world = _world()
    parent = world.allocate(False, None)
    child = world.allocate(False, parent)
    world.activate(parent)
    world.activate(child)
    world.report_failure(child, "Handler", "test", "Error", [], "Continue")
    claim = world.claim_failure(parent)
    assert claim is not None
    token = claim[0]

    report = world.close(timeout_ms=1)
    assert not report["native_done"]
    assert report["in_flight"] >= 1
    assert report["delivery_in_flight"] == 0
    assert report["control_in_flight"] == 1
    assert report["errors"] == 1
    assert report["last_error"]["actor"] == child

    world.finish_failure(token)
    report = world.close(timeout_ms=1)
    assert report["native_done"]
