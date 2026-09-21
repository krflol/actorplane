import socket
import struct
import threading
import time

import pytest

from actorplane import Actor, Pulse, World, handles
from actorplane.tcp import TcpFrame, TcpListener, listen, native_echo
from actorplane.testing import TestWorld


def _frame(data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + data


def _read_frame(sock: socket.socket) -> bytes:
    header = bytearray()
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        assert chunk, "unexpected EOF while reading frame header"
        header.extend(chunk)
    size = struct.unpack(">I", header)[0]
    body = bytearray()
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        assert chunk, "unexpected EOF while reading frame body"
        body.extend(chunk)
    return bytes(body)


def _pump(world, predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        world.run_for(0.005)
    assert predicate()


def test_native_echo_matches_python_request_reply_contract():
    class Owner(Actor):
        pass

    class PythonEcho(Actor):
        @handles(TcpFrame)
        def echo(self, event, ctx):
            assert ctx.reply(event)

    world = World()
    owner = world.spawn(Owner)
    python_echo = world.spawn(PythonEcho)
    world.run_for(0.01)
    rust_echo = native_echo(world, parent=owner)
    python_request = world.request(owner, python_echo, TcpFrame(b"python"))
    native_request = world.request(owner, rust_echo, TcpFrame(b"native"))
    _pump(world, lambda: python_request.poll() is not None and native_request.poll() is not None)
    assert python_request.result().data == b"python"
    assert native_request.result().data == b"native"
    world.close()


@pytest.mark.parametrize("target_kind", ["python", "native"])
def test_tcp_socket_roundtrip_and_listener_owner_scope(target_kind):
    class Parent(Actor):
        pass

    world = World()
    parent = world.spawn(Parent)
    world.run_for(0.01)
    if target_kind == "native":
        echo = native_echo(world, parent=parent)
    else:
        class PythonEcho(Actor):
            @handles(TcpFrame)
            def echo(self, event, ctx):
                assert ctx.reply(event)
        echo = world.spawn(PythonEcho, parent=parent)
        world.run_for(0.01)
    listener = listen(world, parent, echo, port=0)
    assert isinstance(listener, TcpListener)
    assert listener.owner != parent
    errors = []

    def client():
        try:
            with socket.create_connection(listener.address, timeout=2) as sock:
                sock.settimeout(2)
                sock.sendall(_frame(b"socket\x00frame"))
                assert _read_frame(sock) == b"socket\x00frame"
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=client)
    thread.start()
    while thread.is_alive():
        world.run_for(0.005)
        thread.join(timeout=0.005)
    assert not thread.is_alive()
    assert not errors
    _pump(world, lambda: listener.stats()["frames_written"] >= 1)
    report = listener.close()
    _pump(world, lambda: world.stop_report(listener.owner)["native_tasks"] == 0)
    assert world.stop_report(listener.owner)["native_tasks"] == 0
    assert world.inspect()["actors"]
    assert world.inspect()["python_registrations"] == (1 if target_kind == "native" else 2)
    fresh = world.request(parent, echo, TcpFrame(b"after-listener"))
    _pump(world, lambda: fresh.poll() is not None)
    assert fresh.result().data == b"after-listener"
    world.close()


def test_tcp_rejects_testworld_and_cross_world_references_before_bind():
    test_world = TestWorld()

    class Owner(Actor):
        pass

    owner = test_world.spawn(Owner)
    test_world.step()
    with pytest.raises(RuntimeError, match="TestWorld"):
        native_echo(test_world)
    with pytest.raises(RuntimeError, match="TestWorld"):
        listen(test_world, owner, owner)
    test_world.close()

    first = World()
    second = World()
    first_owner = first.spawn(Owner)
    second_owner = second.spawn(Owner)
    first.run_for(0.01)
    second.run_for(0.01)
    with pytest.raises(ValueError, match="another World"):
        listen(first, first_owner, second_owner)
    first.close()
    second.close()


def test_listener_cancel_closes_pending_connection_and_releases_resources():
    class Parent(Actor):
        pass

    world = World()
    parent = world.spawn(Parent)
    world.run_for(0.01)
    echo = native_echo(world, parent=parent)
    listener = listen(world, parent, echo, port=0)
    sock = socket.create_connection(listener.address, timeout=2)
    sock.settimeout(2)
    _pump(world, lambda: bool(listener.connections()))
    sock.sendall(struct.pack(">I", 4) + b"xy")
    _pump(world, lambda: listener.stats()["read_buffer_bytes"] == 4)
    listener.close(mode="cancel")
    _pump(world, lambda: not listener.connections())
    assert listener.stats()["cancelled_connections"] >= 1 or listener.stats()["closed_connections"] >= 1
    sock.close()
    assert world.inspect()["retained_payload_bytes"] == 0
    world.close()


def test_tcp_invalid_configuration_rejects_before_bind():
    class Parent(Actor):
        pass

    world = World(max_event_bytes=1024)
    parent = world.spawn(Parent)
    world.run_for(0.01)
    echo = native_echo(world, parent=parent)
    before = len(world.inspect()["actors"])
    invalid = (
        {"max_connections": True},
        {"max_connections": 0},
        {"port": True},
        {"host": "localhost"},
        {"max_frame_bytes": 0},
        {"max_frame_bytes": 2000},
    )
    for kwargs in invalid:
        with pytest.raises((ValueError, RuntimeError)):
            listen(world, parent, echo, **kwargs)
    assert len(world.inspect()["actors"]) == before
    world.close()


def test_stalled_python_queue_rejects_overload_while_native_tcp_continues():
    seen = []

    class PythonEcho(Actor):
        @handles(TcpFrame)
        def echo(self, event, ctx):
            seen.append(event.data)
            ctx.reply(event)

    def wait_without_dispatch(predicate):
        deadline = time.monotonic() + 2
        while not predicate():
            assert time.monotonic() < deadline
            time.sleep(0.001)

    with World(mailbox_capacity=1) as world:
        parent = world.spawn(Actor)
        target = world.spawn(PythonEcho)
        world.step()
        native = native_echo(world, parent)
        stalled = listen(world, parent, target, request_timeout=2)
        fast = listen(world, parent, native)
        with socket.create_connection(stalled.address, timeout=2) as first, \
                socket.create_connection(stalled.address, timeout=2) as second, \
                socket.create_connection(fast.address, timeout=2) as independent:
            first.sendall(_frame(b"queued"))
            wait_without_dispatch(lambda: stalled.stats()["requests_admitted"] == 1)
            second.sendall(_frame(b"rejected"))
            wait_without_dispatch(lambda: stalled.stats()["admission_rejections"] == 1)
            independent.sendall(_frame(b"native-still-runs"))
            assert _read_frame(independent) == b"native-still-runs"
            assert seen == []
            world.step()
            assert _read_frame(first) == b"queued"
            assert seen == [b"queued"]
        report = world.close()
        assert report["native_done"] and report["python_done"]
        assert world.inspect()["retained_payload_bytes"] == 0
