# TestWorld and shared virtual time

The Rust `actorplane-test` crate and Python `actorplane.testing.TestWorld` drive
the production core, SDK, codecs, queues, and lifecycle on a single creating
thread. They provide explicit time jumps and controlled native replies. Live
Worlds continue to use the system monotonic clock and native worker threads.

## Clock and scheduler

The core owns a concrete `Clock`: system time or a shared manual `Instant`.
Deadline admission, queued/staged expiry, operations, drain, envelope timestamps,
failures, and diagnostics read that clock. Manual advancement checks overflow
before mutation. The core contains no Tokio or Python dependency and invokes no
user clock callbacks while holding its state lock.

The native `test-runtime` feature creates a paused current-thread Tokio runtime.
It has no worker or periodic control thread. Explicit pumps apply core
maintenance and drive the same tracked tasks used by live Worlds. A park hook
wakes the driver, allowing the timer driver to be polled without automatic time
advance; two idle observations with no intervening owned-task polls establish
quiescence. A post-pump check detects divergence between the core and Tokio clocks.
This behavior is covered against the pinned Tokio version, including timers that
remain pending across repeated idle pumps.

Tokio's paused clock normally advances automatically when idle. Its `advance`
operation jumps time and does not itself guarantee that all newly ready timers
have been processed. Those semantics require the explicit pump around each jump.
See Tokio's [advance documentation](https://docs.rs/tokio/1.53.1/tokio/time/fn.advance.html).

An advance first drains currently ready work, then moves the clock, expires core
deadlines, and pumps newly ready work. Periodic SDK components retain their
normal missed-tick `Skip` behavior: a 100 ms jump does not simulate ten 10 ms
ticks. Advance in 10 ms increments when the test needs each tick. The lower-level
`NativeRuntime::advance_virtual` only performs the jump and following pump;
the Rust/Python TestWorld facades perform the preceding pump.

## Python controls

```python
from actorplane import Actor, Pulse
from actorplane.testing import TestWorld

with TestWorld(max_steps=10_000) as world:
    owner = world.spawn(Actor)
    responder = world.native_responder(Pulse)
    world.run_until_idle()
    operation = world.request(owner, responder, Pulse(1), timeout=0.01)
    world.run_until_idle()
    assert world.complete(operation, Pulse(2))
    world.run_until_idle()
    assert operation.take().value == Pulse(2)
    assert world.now() == 0
```

`step()` drives native work around one normal foreground Python step.
`run_until_idle(max_steps=None)` runs ready work at the current instant.
`advance(seconds, max_steps=None, dispatch_python=True)` advances by a finite
nonnegative duration of at most 24 hours, rounded up to milliseconds. `run_for`
uses the same jump semantics; `run()` with no duration runs until idle.

`advance(..., dispatch_python=False)` pumps native work and expires messages
while Python callbacks remain stalled. This is useful for queue and deadline
tests; it is separate from the live runtime's held-GIL independence tests.
`now()` returns virtual seconds since World construction. `close(mode="drain")`
uses the same shared virtual deadline through normal authoring cleanup.

Controls reject calls from another thread, recursive dispatch, or a closed
World before driving work. Native controls also reject use inside an existing
Tokio runtime. The private bridge rejects real-time waits for a virtual World.
The Python implementation subclasses `World` and changes its clock and waiting
seams; registration, dispatch, ownership, codecs, and cleanup remain shared.

## Controlled native replies

`native_responder(event_type, parent=None)` creates an actual SDK component.
Requests reserve normal operation slots, and the responder runs a bounded
deferred job holding the native input. `pending_native()` returns sorted,
immutable operation identities awaiting an external reply.

`complete(operation, event)` encodes through the normal schema path and queues
a held native result. A true return acknowledges that controlled channel's
acceptance; a following pump establishes the operation outcome. Expiry or
cancellation can still win before that pump. Duplicate, terminal, or retired
completions return false. A foreign World operation is rejected.

The controlled registry has a fixed limit. Each responder also obeys its SDK
local job limit and the runtime's global task permits. Input and queued response
payloads consume native byte budgets; cancellation and factory-drop cleanup
remove pending registry entries. Rust consumers must drop a taken `HeldPayload`
before expecting retained bytes to return to zero.

## Ordering and limits

Subscriptions and activity registrations iterate in stable numeric order.
Rust `send_batch` admits in caller slice order without intervening dispatch and
returns one admission result per entry. It is a test ingress convenience, not
atomic fan-out or a production routing scheduler; its input/output vector size
belongs to the caller. Native mailbox and byte limits still apply per admission.

`max_steps` is between 1 and 1,000,000. Rust pumps count owned native task polls;
Python also charges delivered foreground work and at least one unit per driver
turn. The Python limit is checked at normal turn boundaries, so one turn can
process work across multiple bounded actor registrations. Nested controls share
the outer allowance. A non-idle pump exhausting the allowance reports an error
(`TestStepLimit` in Python); callers can still cancel and close the World.

These are cooperative work bounds, not preemption. A native future poll, hook,
destructor, or Python callback that blocks cannot be interrupted by virtual
time. Virtual `timed_out` reports describe logical deadlines, not elapsed wall
time. Run potentially blocking tests under an external wall-clock watchdog; CI
jobs have explicit timeouts. Untracked Tokio tasks and arbitrary application
allocations are outside the owned-work accounting contract.

Repeatability is scoped to explicit single-controller admission and the pinned
scheduler. The regression suite compares full summary/timestamp traces across
20 fresh source/counter Worlds and independently checks stable fan-out event IDs.
This is not a replay guarantee for arbitrary external I/O, simultaneous timer
tie ordering across scheduler versions, or multithreaded production execution.

Run [the Python example](../../examples/testing.py) or
`cargo run --locked -p actorplane-test --example virtual_pipeline`. The latter
drives the real native source, window counter, and sink, then drains them and
checks zero active tasks and retained payloads.
