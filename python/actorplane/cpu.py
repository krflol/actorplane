"""Bounded native CPU operations.

The computation runs in the native runtime and never invokes Python from its
worker.  ``TestWorld`` is intentionally rejected because this operation uses
the production CPU scheduler rather than the virtual-time backend.
"""

from __future__ import annotations

import weakref
from typing import Any

from .authoring import ActorRef, CountSnapshot, Pulse, World


def _reject_test_world(world: World) -> None:
    try:
        from .testing import TestWorld
    except ImportError:
        TestWorld = ()
    if isinstance(world, TestWorld):
        raise RuntimeError("native CPU operations are unavailable on TestWorld")


def native_sum_squares(world: World, parent: ActorRef | None = None) -> ActorRef:
    """Create an active native actor computing ``1² + ... + n²`` for Pulse n."""

    if not isinstance(world, World):
        raise TypeError("world must be a World")
    _reject_test_world(world)
    world._check_driver()
    world._check_open()
    if parent is not None:
        if not isinstance(parent, ActorRef) or parent.world_id != world._world_id:
            raise ValueError("parent belongs to another World")
        parent_handle = parent.handle
    else:
        parent_handle = None
    world._register(Pulse)
    world._register(CountSnapshot)
    handle = world._native.cpu_sum_squares(parent_handle)
    return ActorRef(*handle, _world=weakref.ref(world))


def stats(world: World) -> dict[str, Any]:
    """Return the bounded native CPU scheduler counters."""

    if not isinstance(world, World):
        raise TypeError("world must be a World")
    world._check_process()
    return dict(world.inspect()["cpu"])
