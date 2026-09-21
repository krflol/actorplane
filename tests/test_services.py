"""Service ownership across the native registry and public Python facade."""
import gc
import threading
import weakref
from dataclasses import FrozenInstanceError

import pytest

from actorplane import (
    Actor, ActorRef, AffinityError, CountSnapshot, Interface, MessageOptions,
    Pulse, TraceContext, World, handles, PublicationOutcome,
)
from actorplane.components import Port
from actorplane.cpu import native_sum_squares
from actorplane.services import acquire, register
from actorplane.testing import TestWorld


CONTRACT = Interface("tests.Shared", inputs=(("requests", Pulse),),
                     outputs=(("updates", CountSnapshot),))
CPU_CONTRACT = Interface("actorplane.CpuSumSquares", inputs=(("requests", Pulse),))


def endpoint(world, *, python=False, parent=None):
    """A manually driven native service, so cancellation ordering is deterministic."""
    world._register(Pulse)
    world._register(CountSnapshot)
    reference = ActorRef(*world._native.allocate(python, parent.handle if parent else None),
                         _world=weakref.ref(world))
    ports = (("requests", "input", "pulse", 0), ("updates", "output", "snapshot", 0))
    world._native.register_component(reference.handle,
                                     ("tests.Service", 1, ports, ((CONTRACT.name, 1, ports),)))
    world._native.activate(reference.handle)
    return reference


def owners(world):
    first, second = world.spawn(Actor), world.spawn(Actor)
    world.step()
    return first, second


def finish(world, target, value):
    claim = world._native.claim(target.handle)
    assert claim is not None
    token, _, _, _, _, operation = claim
    metadata = world._native.claim_metadata(token)
    assert world._native.reply(operation, target.handle, "snapshot", [value, value], 0)
    world._native.finish(token)
    return metadata


def test_two_owners_share_cpu_service_and_stop_independently():
    with World() as world:
        first, second = owners(world)
        target = native_sum_squares(world)
        service = register(world, "calculator", target, CPU_CONTRACT)
        left, right = service.acquire(first), service.acquire(second)
        for lease, value in ((left, 7), (right, 8)):
            operation = lease.request("requests", Pulse(value))
            for _ in range(200):
                if operation.poll() is not None:
                    break
                world.run_for(.005)
            assert operation.result() == CountSnapshot(value, value * (value + 1) * (2 * value + 1) // 6)
        world.stop(first)
        assert world._native.state(target.handle) == "Active"
        assert world.inspect()["services"][0]["leases"] == 1
        assert not left.release()
        operation = right.request("requests", Pulse(3))
        for _ in range(200):
            if operation.poll() is not None:
                break
            world.run_for(.005)
        assert operation.result() == CountSnapshot(3, 14)
        report = world.close()
        assert report["native_done"] and report["python_done"] and not report["timed_out"]
        assert world.inspect()["services"] == world.inspect()["service_leases"] == []
        assert world.inspect()["retained_payload_bytes"] == 0


def test_lease_release_fences_queued_requests_and_slot_reuse():
    with TestWorld() as world:
        owner, _ = owners(world)
        target = endpoint(world)
        service = register(world, "shared", target, CONTRACT)
        lease = service.acquire(owner)
        operation = lease.request("requests", Pulse(1))
        assert lease.release()
        assert not lease.release()
        assert world._native.claim(target.handle) is None
        with pytest.raises(RuntimeError, match="Stale"):
            operation.poll()
        replacement = service.acquire(owner)
        assert replacement.scope.slot == lease.scope.slot
        assert replacement.scope.generation != lease.scope.generation
        assert not lease.release()
        with pytest.raises((RuntimeError, ValueError), match="[Ss]tale"):
            lease.request("requests", Pulse(2))
        valid = replacement.request("requests", Pulse(3))
        finish(world, target, 3)
        assert valid.result() == CountSnapshot(3, 3)
        assert world.inspect()["retained_payload_bytes"] == 0


def test_service_stop_revokes_leases_and_old_service_ref_cannot_rebind():
    with TestWorld() as world:
        first, second = owners(world)
        target = endpoint(world)
        service = register(world, "shared", target, CONTRACT)
        leases = [service.acquire(first), service.acquire(second)]
        world.stop(target)
        assert world.inspect()["services"] == world.inspect()["service_leases"] == []
        assert all(not lease.release() for lease in leases)
        assert all(world._native.state(owner.handle) == "Active" for owner in (first, second))
        replacement = endpoint(world)
        register(world, "shared", replacement, CONTRACT)
        with pytest.raises((RuntimeError, ValueError), match="[Ss]tale|[Ss]top|replaced"):
            service.acquire(first)
        lease = acquire(world, first, "shared", CONTRACT)
        assert lease.service == replacement


def test_caps_are_global_per_owner_and_reusable_after_release():
    with TestWorld(max_services=2, max_service_leases=2, max_leases_per_actor=1) as world:
        first, second = owners(world)
        third = world.spawn(Actor)
        world.step()
        left = register(world, "left", endpoint(world), CONTRACT)
        right = register(world, "right", endpoint(world), CONTRACT)
        a, b = left.acquire(first), right.acquire(second)
        before = len(world.inspect()["actors"])
        for service, owner in ((left, third), (right, first)):
            with pytest.raises(RuntimeError, match="LimitExceeded"):
                service.acquire(owner)
        assert len(world.inspect()["actors"]) == before
        assert a.release()
        c = right.acquire(third)
        assert len(world.inspect()["service_leases"]) == 2
        assert b.release() and c.release()


@pytest.mark.parametrize("argument,maximum", [
    ("max_services", 4096), ("max_service_leases", 65536), ("max_leases_per_actor", 1024),
])
def test_service_configuration_is_strict_and_zero_disables_capacity(argument, maximum):
    for invalid in (True, -1, maximum + 1, 1.5):
        with pytest.raises((TypeError, ValueError)):
            World(**{argument: invalid})
    with TestWorld(**{argument: 0}) as world:
        owner, _ = owners(world)
        target = endpoint(world)
        if argument == "max_services":
            with pytest.raises(RuntimeError, match="LimitExceeded"):
                register(world, "shared", target, CONTRACT)
        else:
            service = register(world, "shared", target, CONTRACT)
            with pytest.raises(RuntimeError, match="LimitExceeded"):
                service.acquire(owner)
        assert world.inspect()["service_leases"] == []


def test_contract_port_type_and_cross_world_rejections_do_not_admit():
    with TestWorld() as world, TestWorld() as other:
        owner, _ = owners(world)
        foreign, _ = owners(other)
        target = endpoint(world)
        service = register(world, "shared", target, CONTRACT)
        lease = service.acquire(owner)
        before = world.inspect()["metrics"]["admitted"]
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            lease.request("requests", CountSnapshot(1, 2))
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            lease.request("updates", CountSnapshot(1, 2))
        with pytest.raises((TypeError, ValueError, RuntimeError)):
            lease.request("missing", Pulse(1))
        with pytest.raises(ValueError, match="another World"):
            service.acquire(foreign)
        with pytest.raises(ValueError, match="another World"):
            lease.link("updates", Port(foreign, "input", CountSnapshot, 0, "input"))
        with pytest.raises(RuntimeError, match="InterfaceMismatch"):
            acquire(world, owner, "shared", Interface(CONTRACT.name, 2, CONTRACT.inputs, CONTRACT.outputs))
        assert world.inspect()["metrics"]["admitted"] == before
        assert world.inspect()["retained_payload_bytes"] == 0
        with pytest.raises(FrozenInstanceError):
            lease.owner = foreign


def test_handler_request_preserves_trace_and_port_metadata():
    captured = {}
    class Owner(Actor):
        @handles(Pulse)
        def input(self, event, ctx):
            captured["event"] = ctx.envelope.event_id
            captured["operation"] = captured["lease"].request("requests", event, timeout=1)

    with TestWorld() as world:
        target = endpoint(world)
        service = register(world, "shared", target, CONTRACT)
        owner = world.spawn(Owner)
        world.step()
        captured["lease"] = service.acquire(owner)
        trace = TraceContext(b"a" * 16, b"b" * 8, baggage=(("test", "service"),))
        owner.send(Pulse(4), options=MessageOptions(timeout=.5, correlation_id=77, trace=trace))
        world.step()
        metadata = finish(world, target, 4)
        assert metadata["source"] == captured["lease"].scope.handle
        assert metadata["destination_port"] == (*target.handle, 0)
        assert metadata["causation_id"] == captured["event"]
        assert metadata["correlation_id"] == 77
        assert metadata["deadline_ns"] == 500_000_000
        outcome = captured["operation"].take()
        assert outcome.value == CountSnapshot(4, 4)
        assert outcome.envelope.trace == trace


def test_service_output_routes_are_owned_by_lease_and_fence_queued_delivery():
    seen = []
    class Sink(Actor):
        @handles(CountSnapshot)
        def input(self, event, ctx):
            seen.append((ctx.actor, event.total))

    with TestWorld() as world:
        first, second = world.spawn(Sink), world.spawn(Sink)
        world.step()
        target = endpoint(world)
        service = register(world, "shared", target, CONTRACT)
        left, right = service.acquire(first), service.acquire(second)
        for lease, holder in ((left, first), (right, second)):
            raw = world._native.port(holder.handle, "input")
            lease.link("updates", Port(holder, "input", CountSnapshot, raw[-1], "input"))
        output = Port(target, "updates", CountSnapshot, 1, "output")
        publication = output.emit(CountSnapshot(1, 9))
        world.run_until_idle()
        report = publication.report()
        assert report is not None
        assert report.admitted == 2
        assert report.outcome is PublicationOutcome.ROUTED
        assert seen == [(first, 9), (second, 9)]
        assert left.release()
        followup = output.emit(CountSnapshot(1, 10))
        world.run_until_idle()
        world.step()
        assert followup.report() is not None
        assert seen == [(first, 9), (second, 9), (second, 10)]
        assert len(world.inspect()["links"]) == 1
        assert world.inspect()["links"][0]["owner"] == right.scope.handle
        assert world.inspect()["retained_payload_bytes"] == 0


def test_dropping_lease_token_does_not_release_actor_owned_resource():
    with TestWorld() as world:
        owner, _ = owners(world)
        service = register(world, "shared", endpoint(world), CONTRACT)
        lease = service.acquire(owner)
        scope = lease.scope
        del lease
        gc.collect()
        assert world._native.state(scope.handle) == "Active"
        assert len(world.inspect()["service_leases"]) == 1
        world.stop(owner)
        assert world.inspect()["service_leases"] == []


def test_service_calls_respect_driver_affinity():
    with World() as world:
        owner, _ = owners(world)
        service = register(world, "shared", endpoint(world), CONTRACT)
        lease = service.acquire(owner)
        errors = []
        def worker():
            for action in (lambda: service.acquire(owner), lease.release,
                           lambda: lease.request("requests", Pulse(1))):
                try:
                    action()
                except Exception as error:
                    errors.append(error)
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert len(errors) == 3 and all(isinstance(error, AffinityError) for error in errors)
        assert len(world.inspect()["service_leases"]) == 1


def test_service_can_be_acquired_and_linked_during_actor_configuration():
    captured, seen = {}, []
    class Owner(Actor):
        def configure(self, ctx):
            lease = captured["service"].acquire(ctx.actor)
            captured["lease"] = lease
            raw = ctx.world._native.port(ctx.actor.handle, "input")
            lease.link("updates", Port(ctx.actor, "input", CountSnapshot, raw[-1], "input"))
            with pytest.raises(RuntimeError, match="ActorNotReady"):
                lease.request("requests", Pulse(1))

        @handles(CountSnapshot)
        def input(self, event, ctx):
            seen.append(event.total)

    with TestWorld() as world:
        target = endpoint(world)
        captured["service"] = register(world, "shared", target, CONTRACT)
        owner = world.spawn(Owner)
        world.step()
        assert captured["lease"].owner == owner
        Port(target, "updates", CountSnapshot, 1, "output").emit(CountSnapshot(1, 23))
        world.step()
        assert seen == [23]
        operation = captured["lease"].request("requests", Pulse(5))
        finish(world, target, 5)
        assert operation.result() == CountSnapshot(5, 5)


def test_release_after_world_close_is_idempotent_and_new_work_rejected():
    world = TestWorld()
    owner, _ = owners(world)
    service = register(world, "shared", endpoint(world), CONTRACT)
    lease = service.acquire(owner)
    world.close()
    assert not lease.release()
    with pytest.raises(RuntimeError, match="closed"):
        lease.request("requests", Pulse(1))
    with pytest.raises(RuntimeError, match="closed"):
        service.acquire(owner)
