"""Deterministic, virtual-time testing over the production authoring path.

``TestWorld`` uses the same Python registration, codec, lifecycle, failure,
and request code as :class:`actorplane.World`.  The native bridge supplies a
paused clock and explicit completion controls; this module does not emulate
native queues in Python.
"""

from __future__ import annotations

import math
import threading
import weakref
from dataclasses import dataclass
from typing import Any
from contextlib import contextmanager

from .authoring import ActorRef, AffinityError, LifecycleError, OperationHandle, World


class TestStepLimit(RuntimeError):
    """The deterministic pump exceeded its configured work budget."""


@dataclass(frozen=True)
class PendingNative:
    """An operation tuple retained by the deterministic native backend."""

    operation: tuple[int, int, int]


class TestWorld(World):
    """A single-controller World with virtual native time.

    Time advances only through :meth:`advance` or shutdown's shared deadline.
    :meth:`run_until_idle` executes ready work at the current instant. The native
    scheduler has no workers or automatic time advances. Application hooks
    remain cooperative: a blocking hook needs an external wall-clock watchdog.
    This harness does not
    promise replay determinism for arbitrary external I/O or multiple
    controllers.
    """

    __test__ = False

    def __init__(self, *, max_steps: int = 100_000, **kwargs: Any) -> None:
        if type(max_steps) is not int or not 1 <= max_steps <= 1_000_000:
            raise ValueError("max_steps must be an integer in 1..1000000")
        self._test_thread = threading.get_ident()
        self._test_max_steps = max_steps
        self._test_remaining = None
        self._test_native_work = 0
        kwargs["virtual_time"] = True
        super().__init__(**kwargs)

    def _check_test_thread(self) -> None:
        if threading.get_ident() != self._test_thread:
            raise AffinityError("TestWorld controls belong to its creating thread")

    def _check_process(self) -> None:
        super()._check_process()
        self._check_test_thread()

    def _clock(self) -> float:
        return self._native.elapsed_ns() / 1_000_000_000

    def _idle_wait(self, milliseconds: int) -> None:
        report = self._native.test_advance(milliseconds, self._available())
        self._charge(max(1, report["polls"]))
        if not report["idle"]:
            raise TestStepLimit("TestWorld native work exhausted the poll budget")

    def _idle_delay(self, deadline: float | None = None) -> int:
        return min(1, super()._idle_delay(deadline))

    def _assert_control(self, *, allow_closed=False):
        self._check_driver()
        if self._dispatching or self._running:
            raise AffinityError("World dispatch is not reentrant")
        if self._closed and not allow_closed:
            raise LifecycleError("World is closed")

    @contextmanager
    def _budget(self, requested=None):
        limit = self._test_max_steps if requested is None else requested
        if type(limit) is not int or not 1 <= limit <= 1_000_000:
            raise ValueError("max_steps must be an integer in 1..1000000")
        outer = self._test_remaining is None
        if outer:
            self._test_remaining = limit
        try:
            yield
        finally:
            if outer:
                self._test_remaining = None

    def _available(self):
        if self._test_remaining is None or self._test_remaining <= 0:
            raise TestStepLimit("TestWorld exhausted its work budget")
        return self._test_remaining

    def _charge(self, work):
        self._test_remaining -= work
        if self._test_remaining < 0:
            raise TestStepLimit("TestWorld exhausted its work budget")

    @staticmethod
    def _seconds(value: float) -> float:
        if type(value) not in (int, float) or isinstance(value, bool):
            raise TypeError("virtual time must be a number")
        value = float(value)
        if not math.isfinite(value) or not 0 <= value <= 86400:
            raise ValueError("virtual time must be finite and within 0..86400 seconds")
        return value

    def now(self) -> float:
        """Return the virtual monotonic clock in seconds."""

        self._check_process()
        return self._native.elapsed_ns() / 1_000_000_000

    def _native_pump(self) -> dict:
        result = self._native.test_pump(self._available())
        self._charge(result["polls"])
        if not result["idle"]:
            raise TestStepLimit("TestWorld native work exhausted the poll budget")
        return result

    def _pump(self) -> int:
        result_before = self._native_pump()
        processed = super()._pump()
        self._charge(max(1, processed))
        result_after = self._native_pump()
        self._test_native_work = result_before["polls"] + result_after["polls"]
        return processed

    def step(self) -> int:
        """Pump native work before and after one normal Python driver step."""

        self._assert_control()
        with self._budget():
            return super().step()

    def run_until_idle(self, max_steps: int | None = None) -> int:
        """Pump until Python and native work are both idle."""

        self._assert_control()
        with self._budget(max_steps):
            total = 0
            while True:
                python_work = self._pump()
                total += python_work
                if python_work == 0 and self._test_native_work == 0:
                    return total

    def advance(self, seconds: float, *, max_steps: int | None = None,
                dispatch_python: bool = True) -> int:
        """Jump time, rounding up to milliseconds, then run ready work.

        Set ``dispatch_python=False`` to drive native work and expiry while
        leaving Python callbacks stalled. Missed periodic ticks are skipped.
        """

        self._assert_control()
        seconds = self._seconds(seconds)
        if type(dispatch_python) is not bool:
            raise TypeError("dispatch_python must be a bool")
        with self._budget(max_steps):
            if dispatch_python:
                processed = self.run_until_idle()
            else:
                self._native_pump()
                processed = 0
            report = self._native.test_advance(math.ceil(seconds * 1000), self._available())
            self._charge(report["polls"])
            if not report["idle"]:
                raise TestStepLimit("TestWorld native work exhausted the poll budget")
            return processed + (self.run_until_idle() if dispatch_python else 0)

    def run_for(self, duration: float) -> None:
        self.advance(duration)

    def run(self, duration: float | None = None) -> None:
        self._check_test_thread()
        if duration is None:
            self.run_until_idle()
        else:
            self.advance(duration)

    def native_responder(self, event_type: type, parent: ActorRef | None = None) -> ActorRef:
        """Create an active native test responder for explicit request replies."""

        self._check_test_thread()
        self._check_open()
        codec = self._register(event_type)
        if parent is not None:
            if not isinstance(parent, ActorRef) or parent.world_id != self._world_id:
                raise ValueError("parent belongs to another World")
            parent_handle = parent.handle
        else:
            parent_handle = None
        handle = self._native.test_responder(codec.kind, codec.schema, parent_handle)
        return ActorRef(*handle, _world=weakref.ref(self))

    def pending_native(self) -> tuple[PendingNative, ...]:
        """Return immutable operation identities awaiting explicit completion."""

        self._check_process()
        return tuple(PendingNative(tuple(item)) for item in self._native.test_pending())

    def complete(self, operation: OperationHandle, event: Any) -> bool:
        """Queue one encoded reply; pump again to observe the terminal result.

        True means the controlled channel accepted it. Cancellation or expiry
        before the next pump can still win the operation's terminal outcome.
        """

        self._check_test_thread()
        if not isinstance(operation, OperationHandle):
            raise TypeError("complete requires an OperationHandle")
        if operation._runtime() is not self:
            raise ValueError("operation belongs to another World")
        kind, values, schema = self._encode_event(event)
        return bool(self._native.test_complete(operation.handle, kind, values, schema))

    def close(self, timeout: float = 1.0, *, mode: str | None = None) -> dict:
        self._assert_control(allow_closed=True)
        with self._budget():
            return super().close(timeout, mode=mode)
