"""Black-box coverage for the native Python readiness cursors."""

import pytest

from actorplane import Actor, World, Pulse, handles


def test_later_idle_actor_enqueued_by_earlier_handler_runs_same_pump():
    seen = []
    refs = {}

    class First(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("first", event.value))
            refs["later"].send(Pulse(2))

    class Later(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("later", event.value))

    with World() as world:
        refs["first"] = world.spawn(First)
        refs["later"] = world.spawn(Later)
        world.step()
        refs["first"].send(Pulse(1))
        assert world.step() == 2
        assert seen == [("first", 1), ("later", 2)]


def test_send_to_earlier_actor_from_later_actor_waits_for_next_pump():
    seen = []
    refs = {}

    class First(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("first", event.value))

    class Later(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("later", event.value))
            ctx.send(refs["first"], Pulse(2))

    with World() as world:
        refs["first"] = world.spawn(First)
        refs["later"] = world.spawn(Later)
        world.step()
        refs["first"].send(Pulse(1))
        refs["later"].send(Pulse(3))
        assert world.step() == 2
        assert seen == [("first", 1), ("later", 3)]
        assert world.step() == 1
        assert seen == [("first", 1), ("later", 3), ("first", 2)]


def test_reused_slot_gets_new_registration_order():
    seen = []

    class A(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("a", event.value))

    class B(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("b", event.value))

    class C(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            seen.append(("c", event.value))

    with World() as world:
        old = world.spawn(A)
        b = world.spawn(B)
        world.step()
        world.stop(old)
        replacement = world.spawn(C)
        assert replacement.slot == old.slot
        assert replacement.generation != old.generation
        world.step()
        b.send(Pulse(2))
        replacement.send(Pulse(3))
        world.step()
        assert seen == [("b", 2), ("c", 3)]


def test_handler_spawn_is_deferred_until_the_next_start_phase():
    events = []
    refs = {}

    class Child(Actor):
        def on_start(self, ctx):
            events.append("child-start")

    class Parent(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            events.append("parent-event")
            refs["child"] = ctx.spawn(Child)

    with World() as world:
        parent = world.spawn(Parent)
        world.step()
        parent.send(Pulse(1))
        assert world.step() == 1
        assert events == ["parent-event"]
        assert world.step() == 0
        assert events == ["parent-event", "child-start"]


def test_stopping_later_actor_before_claim_skips_business_and_cleans_once():
    events = []
    refs = {}

    class First(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            events.append("first")
            world.stop(refs["later"])

    class Later(Actor):
        @handles(Pulse)
        def pulse(self, event, ctx):
            events.append("later-event")

        def on_stop(self, ctx):
            events.append("later-stop")

    world = World()
    try:
        refs["first"] = world.spawn(First)
        refs["later"] = world.spawn(Later)
        world.step()
        refs["first"].send(Pulse(1))
        refs["later"].send(Pulse(2))
        assert world.step() == 1
        assert events == ["first", "later-stop"]
    finally:
        world.close()


def test_cleanup_hook_fencing_sibling_is_revisited_and_runs_once():
    events = []
    refs = {}

    class First(Actor):
        def on_stop(self, ctx):
            events.append("first-stop")
            world.stop(refs["sibling"])

    class Sibling(Actor):
        def on_stop(self, ctx):
            events.append("sibling-stop")

    world = World()
    try:
        refs["first"] = world.spawn(First)
        refs["sibling"] = world.spawn(Sibling)
        world.step()
        world.stop(refs["first"])
        assert events == ["first-stop", "sibling-stop"]
        assert events.count("sibling-stop") == 1
    finally:
        world.close()


def test_cleanup_cutoff_excludes_registration_created_by_stop_hook():
    events = []
    refs = {}

    class New(Actor):
        def on_start(self, ctx):
            events.append("new-start")

    class First(Actor):
        def on_stop(self, ctx):
            events.append("first-stop")
            refs["new"] = world.spawn(New)
            world.stop(refs["new"])

    world = World()
    try:
        refs["first"] = world.spawn(First)
        world.step()
        world.stop(refs["first"])
        assert events == ["first-stop"]
        assert "new" in refs
        assert world.inspect()["python_registrations"] == 1
        assert world._native.state(refs["new"].handle) == "Stopping"
        world.step()
        assert world.inspect()["python_registrations"] == 0
        assert world._native.state(refs["new"].handle) == "Stopped"
        assert events == ["first-stop"]
    finally:
        world.close()


class _CountingNative:
    def __init__(self, native):
        self._native = native
        self.calls = {"state": 0, "claim": 0, "claim_failure": 0,
                      "ready_cutoff": 0, "ready_next": 0}

    def __getattr__(self, name):
        return getattr(self._native, name)

    def state(self, *args):
        self.calls["state"] += 1
        return self._native.state(*args)

    def claim(self, *args):
        self.calls["claim"] += 1
        return self._native.claim(*args)

    def claim_failure(self, *args):
        self.calls["claim_failure"] += 1
        return self._native.claim_failure(*args)

    def ready_cutoff(self, *args):
        self.calls["ready_cutoff"] += 1
        return self._native.ready_cutoff(*args)

    def ready_next(self, *args):
        self.calls["ready_next"] += 1
        return self._native.ready_next(*args)


@pytest.mark.parametrize("count", [0, 1, 1024])
def test_large_idle_world_uses_readiness_queries_without_actor_scans(count):
    class Idle(Actor):
        pass

    world = World(max_actors=1024)
    try:
        for _ in range(count):
            world.spawn(Idle)
        world.step()
        proxy = _CountingNative(world._native)
        world._native = proxy
        assert world.step() == 0
        assert proxy.calls["state"] == 0
        assert proxy.calls["claim"] == 0
        assert proxy.calls["claim_failure"] == 0
        assert 1 <= proxy.calls["ready_cutoff"] <= 10
        assert 1 <= proxy.calls["ready_next"] <= 10
    finally:
        world.close()
