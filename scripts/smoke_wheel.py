"""Install a release wheel into a clean environment and exercise its native path."""
from pathlib import Path
import subprocess
import sys
import zipfile


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    wheels = sorted((root / "dist").glob("*.whl"), key=lambda p: p.stat().st_mtime)
    if not wheels:
        raise SystemExit("Build a release wheel in dist first")
    with zipfile.ZipFile(wheels[-1]) as archive:
        license_files = [name for name in archive.namelist()
                         if ".dist-info/" in name and name.endswith("LICENSE")]
        assert license_files, "MIT license file is missing from the wheel"
        assert any(b"MIT License" in archive.read(name) for name in license_files)
    env = root / "target" / "wheel-smoke"
    if not env.resolve().is_relative_to((root / "target").resolve()):
        raise SystemExit("wheel smoke environment resolves outside the build directory")
    subprocess.run(["uv", "venv", "--clear", "--python", sys.executable, str(env)], check=True)
    executable = env / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    subprocess.run(["uv", "pip", "install", "--python", str(executable), str(wheels[-1])], check=True)
    code = '''
import time
from actorplane import Actor, Component, CountSnapshot, FailurePolicy, IntRange, MessageOptions, Output, Pulse, TraceContext, World, component, event, handles, native
from typing import Annotated
from actorplane import _native
assert not hasattr(_native, "_held_gil_probe"), "test hook leaked into release wheel"
assert not hasattr(_native, "_hold_gil"), "test hook leaked into release wheel"
assert not hasattr(_native, "_held_gil_tcp_probe"), "TCP test hook leaked into release wheel"
assert not hasattr(_native, "_cpu_gil_probe"), "CPU test hook leaked into release wheel"
seen = []
class Monitor(Actor):
    def on_start(self, ctx):
        self.counter = ctx.native_counter(.002, .02, target=ctx.actor)
    @handles(CountSnapshot)
    def receive(self, event, ctx):
        seen.append(event.count)
with World() as world:
    world.spawn(Monitor)
    world.run_for(.2)
    assert seen and sum(seen) > 0
    report = world.close()
    assert report["native_done"] and report["python_done"]
    assert world.inspect()["retained_payload_bytes"] == 0
class Echo(Actor):
    @handles(Pulse)
    def receive(self, event, ctx):
        assert ctx.envelope.correlation_id == 123
        assert ctx.envelope.trace.baggage == (("smoke", "wheel"),)
        assert ctx.reply(Pulse(event.value + 1))
with World(max_operations=1, diagnostic_capacity=8) as world:
    owner = world.spawn(Actor)
    target = world.spawn(Echo)
    world.step()
    trace = TraceContext(b"a" * 16, b"b" * 8, baggage=(("smoke", "wheel"),))
    result = world.request(owner, target, Pulse(41), options=MessageOptions(correlation_id=123, trace=trace))
    world.step()
    outcome = result.take()
    assert outcome.value == Pulse(42)
    assert outcome.envelope.correlation_id == 123
    assert outcome.envelope.trace == trace
    assert outcome.envelope.source == target
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"]
    assert not report["timed_out"]
    assert world.inspect()["operation_pending"] == 0
    assert world.inspect()["operation_retained"] == 0
    assert world.inspect()["retained_payload_bytes"] == 0
    world.diagnostics(limit=8)
references, failures = {}, []
class Worker(Actor):
    @handles(Pulse)
    def fail(self, event, ctx):
        raise ValueError("private failure message")
class Supervisor(Actor):
    def on_start(self, ctx):
        references["worker"] = ctx.spawn(Worker)
    def on_failure(self, failure, ctx):
        failures.append(failure)
        return FailurePolicy.STOP_WORLD
with World() as world:
    world.spawn(Supervisor)
    world.step()
    world.step()
    references["worker"].send(Pulse(1))
    world.step()
    assert not failures, "supervisor callback ran inline"
    world.step()
    assert len(failures) == 1
    assert failures[0].phase == "Handler"
    assert failures[0].exception_type == "builtins.ValueError"
    assert failures[0].handler == "fail"
    entries = world.diagnostics()["entries"]
    assert any(entry["failure"] for entry in entries)
    assert "private failure message" not in str(entries)
    report = world.close()
    assert report["native_done"] and report["python_done"]
    assert world.inspect()["metrics"]["notifications_delivered"] == 1
    assert report["errors"] == 1
    assert report["last_error"]["phase"] == "Handler"
    status = world.stop_report()
    assert status["errors"] == 1
    assert status["outstanding_operations"] == 0
    assert status["retained_operations"] == 0
    assert status["python_pending"] == 0
    assert status["native_tasks"] == 0
@event("smoke.Detail")
class Detail:
    identifier: Annotated[int, IntRange(0, (1 << 64) - 1)]
    label: str
@event("smoke.Rich")
class Rich:
    detail: Detail
    data: bytes
    values: tuple[float, ...]
    note: str | None
class RichEcho(Actor):
    @handles(Rich)
    def echo(self, event, ctx):
        assert ctx.reply(event)
with World() as world:
    owner, target = world.spawn(Actor), world.spawn(RichEcho)
    world.step()
    buffer = bytearray(b"before")
    pending = world.request(owner, target, Rich(Detail((1 << 64) - 1, "nested"), buffer, [1.25, -0.0], None))
    buffer[:] = b"after!"
    world.step()
    result = pending.result()
    assert result == Rich(Detail((1 << 64) - 1, "nested"), b"before", (1.25, -0.0), None)
    assert world.inspect()["native_schemas"] == 1
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"]
    assert world.inspect()["retained_payload_bytes"] == 0
proxies = {}
class ComponentProducer(Component):
    output = Output(CountSnapshot)
    @handles(Pulse)
    def input(self, event, ctx):
        self.output.emit(CountSnapshot(1, event.value))
class ComponentParent(Actor):
    sink = component(native.SnapshotSink)
    source = component(ComponentProducer, depends_on=("sink",), requires=native.SnapshotSink.implements)
    def configure(self, ctx):
        proxies.update(source=self.source, sink=self.sink)
        ctx.link(self.source.output, self.sink.input)
with World() as world:
    world.spawn(ComponentParent)
    world.step()
    proxies["source"].input.send(Pulse(42))
    assert len(world.inspect()["links"]) == 1
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert proxies["sink"].stats()["sink_received"] == 1
    assert world.inspect()["retained_payload_bytes"] == 0
from actorplane.testing import TestWorld
virtual_seen = []
class VirtualTimer(Actor):
    def on_start(self, ctx):
        ctx.after(.01, Pulse(7))
    @handles(Pulse)
    def receive(self, event, ctx):
        virtual_seen.append((event.value, ctx.envelope.enqueued_at_ns))
with TestWorld() as world:
    owner = world.spawn(VirtualTimer)
    world.run_until_idle()
    assert world.now() == 0 and not virtual_seen
    world.advance(.01)
    assert virtual_seen == [(7, 10_000_000)]
    responder = world.native_responder(Pulse)
    operation = world.request(owner, responder, Pulse(1), timeout=.01)
    world.run_until_idle()
    assert world.pending_native()
    assert world.complete(operation, Pulse(2))
    world.run_until_idle()
    assert operation.take().value == Pulse(2)
    expired = world.request(owner, responder, Pulse(3), timeout=.01)
    world.advance(.01)
    assert expired.take().state == "TimedOut"
    assert not world.complete(expired, Pulse(4))
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"]
    assert world.inspect()["retained_payload_bytes"] == 0
import socket
from actorplane.tcp import listen, native_echo
with World() as world:
    owner = world.spawn(Actor)
    world.step()
    echo = native_echo(world, owner)
    listener = listen(world, owner, echo)
    with socket.create_connection(listener.address, timeout=2) as client:
        client.sendall(b"\\x00\\x00\\x00\\x04wire")
        response = bytearray()
        while len(response) < 8:
            part = client.recv(8 - len(response))
            assert part, "early TCP EOF"
            response.extend(part)
        assert response == b"\\x00\\x00\\x00\\x04wire"
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"]
    assert listener.stats()["frames_written"] == 1
    assert not listener.connections()
    assert listener.stats()["read_buffer_bytes"] == listener.stats()["write_buffer_bytes"] == 0
    assert world.inspect()["retained_payload_bytes"] == 0
from actorplane.cpu import native_sum_squares, stats as cpu_stats
with World(cpu_workers=1, max_cpu_jobs=4) as world:
    owner = world.spawn(Actor)
    world.step()
    target = native_sum_squares(world, owner)
    operation = world.request(owner, target, Pulse(10000), timeout=1)
    while operation.poll() is None:
        world.run_for(.005)
    assert operation.result() == CountSnapshot(10000, 333383335000)
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert cpu_stats(world)["completed"] == 1
    assert cpu_stats(world)["queued"] == cpu_stats(world)["running"] == 0
    assert world.inspect()["retained_payload_bytes"] == 0
from actorplane import Interface
from actorplane.services import register
with World() as world:
    first, second = world.spawn(Actor), world.spawn(Actor)
    world.step()
    service = register(world, "calculator", native_sum_squares(world),
                       Interface("actorplane.CpuSumSquares", inputs=(("requests", Pulse),)))
    left, right = service.acquire(first), service.acquire(second)
    world.stop(first)
    assert not left.release()
    assert world.inspect()["services"][0]["leases"] == 1
    operation = right.request("requests", Pulse(7))
    for _ in range(200):
        if operation.poll() is not None:
            break
        world.run_for(.005)
    assert operation.result() == CountSnapshot(7, 140)
    report = world.close(mode="drain")
    assert report["native_done"] and report["python_done"] and not report["timed_out"]
    assert world.inspect()["services"] == world.inspect()["service_leases"] == []
    assert world.inspect()["retained_payload_bytes"] == 0
publication_output = {}
class PublicationProducer(Component):
    output = Output(Pulse)
class PublicationOwner(Actor):
    producer = component(PublicationProducer)
    def configure(self, ctx):
        publication_output["port"] = self.producer.output
with TestWorld(max_publications=2, max_publications_per_source=2) as world:
    world.spawn(PublicationOwner)
    world.step()
    ticket = publication_output["port"].emit(Pulse(5))
    assert ticket.status().value == "Queued"
    assert ticket.report() is None
    world.run_until_idle()
    assert ticket.status().value == "Routed"
    assert ticket.report().outcome.value == "Routed"
    assert world.inspect()["metrics"]["publication_submitted"] >= 1
print("Clean release wheel: MIT, native progress, schemas, components, envelopes, requests, drain, supervision, TestWorld, TCP, CPU, services and hook exclusion passed")
'''
    subprocess.run([str(executable), "-I", "-c", code], cwd=env, check=True)


if __name__ == "__main__":
    main()
