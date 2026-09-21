"""Native TCP exchange, an active Python request, and timer-driven cancellation."""
import socket
import struct
import threading

from actorplane import Actor, World, event, handles
from actorplane.tcp import TcpFrame, listen, native_echo


@event("example.CloseConnection")
class CloseConnection:
    reason: str


def read_exact(sock, length):
    result = bytearray()
    while len(result) < length:
        part = sock.recv(length - len(result))
        if not part:
            raise EOFError("connection closed")
        result.extend(part)
    return bytes(result)


def main():
    observations, errors = [], []

    class Workflow(Actor):
        @handles(TcpFrame)
        def pending(self, request, ctx):
            assert request.data == b"wait-for-cancellation"
            observations.append("request admitted")
            # Leave this request pending. Its scoped timer will cancel the owner.
            ctx.after(0.02, CloseConnection("workflow finished"))

        @handles(CloseConnection)
        def finish(self, event, ctx):
            assert ctx.world.stop_report()["outstanding_operations"] == 1
            observations.append("timer cancelled owner")
            ctx.world.request_stop()

    with World() as world:
        owner = world.spawn(Workflow)
        world.step()
        echo = native_echo(world, owner)
        native_listener = listen(world, owner, echo)
        policy_listener = listen(world, owner, owner)

        def client():
            try:
                with socket.create_connection(native_listener.address, timeout=2) as sock:
                    data = b"native\x00roundtrip"
                    sock.sendall(struct.pack(">I", len(data)) + data)
                    size = struct.unpack(">I", read_exact(sock, 4))[0]
                    assert size == len(data) and read_exact(sock, size) == data
                with socket.create_connection(policy_listener.address, timeout=2) as sock:
                    data = b"wait-for-cancellation"
                    sock.sendall(struct.pack(">I", len(data)) + data)
                    try:
                        assert sock.recv(1) == b""
                    except ConnectionResetError:
                        pass
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=client)
        worker.start()
        world.run_for(2)
        report = world.close()
        worker.join(timeout=3)
        assert not worker.is_alive() and not errors, errors
        assert observations == ["request admitted", "timer cancelled owner"]
        assert native_listener.stats()["frames_written"] == 1
        assert policy_listener.stats()["requests_admitted"] == 1
        assert report["native_done"] and report["python_done"]
        assert report["outstanding_operations"] == report["retained_operations"] == 0
        assert world.inspect()["retained_payload_bytes"] == 0
        assert native_listener.connections() == policy_listener.connections() == ()
        assert native_listener.stats()["read_buffer_bytes"] == policy_listener.stats()["read_buffer_bytes"] == 0
    print("native roundtrip=1; pending request cancelled by scoped timer; sockets and bytes released")


if __name__ == "__main__":
    main()
