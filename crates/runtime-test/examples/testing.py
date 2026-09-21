"""Deterministic Python facade example; it never sleeps or advances wall time."""

from actorplane._native import NativeWorld


def main() -> None:
    world = NativeWorld(virtual_time=True, max_tasks=8, max_operations=8)
    owner = world.allocate(python=False)
    target = world.allocate(python=False)
    world.activate(owner)
    world.activate(target)

    world.after(owner, target, 10, "pulse", 1)
    world.test_advance(0, 10000)  # arm the real timer task
    world.test_advance(10, 10000)
    timer_event = world.claim(target)
    assert timer_event is not None
    world.finish(timer_event[0], True)

    responder = world.test_responder("pulse", 0)
    operation = world.request(owner, responder, "pulse", 2, timeout_ms=100)
    world.test_advance(0, 10000)
    assert operation in world.test_pending()
    assert world.test_complete(operation, "pulse", 3)
    world.test_advance(0, 10000)

    report = world.close(1000)
    assert report["native_done"]
    print("virtual timer and controlled reply completed")


if __name__ == "__main__":
    main()
