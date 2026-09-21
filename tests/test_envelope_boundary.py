"""Raw PyO3 envelope option validation and rollback boundaries."""

import pytest

native = pytest.importorskip("actorplane._native")


def _world():
    return native.NativeWorld(
        max_actors=4,
        mailbox_capacity=4,
        mailbox_bytes=4096,
        native_payload_budget=4096,
        max_subscriptions=4,
        max_event_bytes=1024,
        max_tasks=1,
        max_operations=4,
        diagnostic_capacity=4,
    )


def test_options_reject_index_objects_and_bool_ids_without_retaining_payload():
    world = _world()
    target = world.allocate(False, None)
    world.activate(target)

    class IndexOnly:
        def __index__(self):
            return 7

    for options in [
        (None, None, IndexOnly(), None, None),
        (None, None, True, None, None),
        (None, None, None, False, None),
    ]:
        with pytest.raises(ValueError):
            world.send(target, "pulse", [1], options=options)
    assert world.snapshot()["retained_payload_bytes"] == 0
    world.close()


def test_options_reject_huge_text_and_malformed_trace_before_admission():
    world = _world()
    target = world.allocate(False, None)
    world.activate(target)
    huge = ("k", "x" * 257)
    with pytest.raises(ValueError):
        world.send(target, "pulse", [1], options=(None, None, None, None, (b"a" * 16, b"b" * 8, True, (huge,))))
    with pytest.raises(ValueError):
        world.send(target, "pulse", [1], options=(None, None, None, None))
    with pytest.raises(ValueError):
        world.send(target, "pulse", [1], options=(None, None, None, None, (b"a" * 15, b"b" * 8, True, ())))
    assert world.snapshot()["retained_payload_bytes"] == 0
    world.close()


def test_valid_options_are_merged_and_visible_in_claim_metadata():
    world = _world()
    target = world.allocate(False, None)
    world.activate(target)
    options = (None, 1000, 17, 9, (b"a" * 16, b"b" * 8, True, (("tenant", "native"),)))
    event_id = world.send(target, "pulse", [1], options=options)
    claim = world.claim(target)
    assert claim is not None
    assert claim[0] == event_id
    metadata = world.claim_metadata(event_id)
    assert metadata["event_id"] == event_id
    assert metadata["correlation_id"] == 17
    assert metadata["causation_id"] == 9
    assert metadata["trace"]["baggage"] == (("tenant", "native"),)
    world.finish(event_id)
    world.close()
