"""Real driver wait integration; virtual time has a separate explicit quantum."""
from threading import Event, Thread

from actorplane import Actor, Pulse, World, handles


class WaitProbe:
    def __init__(self, native):
        self.native = native
        self.entered = Event()
        self.delays = []

    def __getattr__(self, name):
        return getattr(self.native, name)

    def wait(self, milliseconds):
        self.delays.append(milliseconds)
        self.entered.set()
        return self.native.wait(milliseconds)


def test_real_idle_driver_uses_remaining_deadline_instead_of_millisecond_polling():
    with World() as world:
        world.spawn(Actor)
        probe = WaitProbe(world._native)
        world._native = probe
        world.run_for(0.04)
        assert probe.delays
        # Conversion rounds up to milliseconds, including float clock error.
        assert 1 < probe.delays[0] <= 41
        assert len(probe.delays) <= 2


def test_detached_wait_allows_another_python_thread_to_deliver_and_stop_driver():
    seen, errors = [], []

    class Receiver(Actor):
        @handles(Pulse)
        def receive(self, value, ctx):
            seen.append(value.value)
            world.request_stop()

    with World() as world:
        target = world.spawn(Receiver)
        probe = WaitProbe(world._native)
        world._native = probe

        def send_when_idle():
            try:
                assert probe.entered.wait(2)
                target.send(Pulse(17))
            except BaseException as exc:
                errors.append(exc)

        sender = Thread(target=send_when_idle)
        sender.start()
        world.run_for(3)
        sender.join(2)
        assert not sender.is_alive()
        assert errors == []
        assert seen == [17]
        assert probe.delays == [1000]
        assert world.inspect()["python_registrations"] == 0


def test_native_stop_wakes_idle_driver_without_waiting_again_after_cleanup():
    with World() as world:
        world.spawn(Actor)
        probe = WaitProbe(world._native)
        world._native = probe
        errors = []

        def stop_when_idle():
            try:
                assert probe.entered.wait(2)
                world.request_stop()
            except BaseException as exc:
                errors.append(exc)

        stopper = Thread(target=stop_when_idle)
        stopper.start()
        world.run_for(3)
        stopper.join(2)
        assert not stopper.is_alive()
        assert errors == []
        assert probe.delays == [1000]
        assert world.inspect()["python_registrations"] == 0
