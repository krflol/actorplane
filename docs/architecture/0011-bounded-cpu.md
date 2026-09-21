# Bounded native CPU work

CPU operations run Rust closures on Tokio's blocking pool, separate from the
asynchronous workers that drive sockets, timers, and component hooks. The runtime
configures an explicit blocking-thread ceiling and separately limits admitted
CPU jobs. No Python callable or interpreter-owned input enters this path.

The SDK method `NativeContext::defer_cpu_reply(input, factory)` accepts an
owned `HeldPayload` and `JobContext` and returns a normal request result. The
component owns the operation task; the requester owns its terminal result slot.
The original deadline, correlation, trace context, and operation generation
remain authoritative. The computation holds no mutable component reference.

## Admission and execution

`CpuConfig { workers, max_jobs }` is supplied to
`NativeRuntime::new_with_cpu_config(world, max_tasks, config)`. Defaults are two
CPU workers and 32 admitted jobs. Valid ranges are 1..64 and 1..4096 respectively.
`NativeRuntime::new` uses these defaults. Python exposes the same controls as
`World(cpu_workers=2, max_cpu_jobs=32)`.

Each admission reserves a CPU job slot, a component job slot, a global native
task slot, an owner task lease, and the retained input bytes. The ordinary SDK
input and callback-effect limits also apply. Admission failure returns a bounded
error immediately. A handler can handle overload explicitly or let its normal
failure policy apply. There is no unbounded pending-sender workaround.

Admitted jobs wait for a CPU worker in bounded native tasks. They check request
and owner cancellation while queued. A worker reservation moves with the closure
into the blocking executor and stays held through its completion and cleanup.
The runtime's native task limit continues to include CPU work even if the
asynchronous task monitoring its result is aborted during shutdown.

## Cancellation and shutdown

Queued cancellation drops input and closure without invoking the computation.
Before invoking a computation, the executor rechecks cancellation. After that
claim, running code must cooperate by checking `JobContext::is_cancelled()`.
That check includes the deadline, component lifecycle, and operation status.
Stopping an owner fences results; late results cannot address a reused slot.
An already terminal/retired request at deferred submission is a benign no-op,
matching asynchronous deferred replies.

Drain allows admitted CPU requests to finish under the shared shutdown deadline.
Cancellation and deadline escalation cannot forcibly kill running Rust code.
Task and input reservations remain visible until it returns, including after
`NativeRuntime::close` times out. A later close can report that work has completed
while retaining the historical timeout flag.

These limits follow the [Tokio blocking-task contract](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html):
started blocking tasks cannot be aborted, and CPU concurrency needs an explicit
bound. This implementation makes those bounds and unfinished work observable.
It does not make arbitrary blocking code preemptible.

## Payload and memory obligations

The input is native, immutable, and charged to the World. Completed output goes
through ordinary schema, event-byte, and terminal-result budget checks; an
oversized result becomes the existing bounded failure outcome. CPU execution
does not bypass mailbox or operation capacity.

`JobContext::reserve_buffer(bytes)` reserves the shared World byte budget before
application scratch allocation. Keep its permit with the allocation.
`reserve_output(bytes)` retains one grow-only reservation in the job context;
call it before output allocation. That reservation survives the factory return
and stays held through result admission or discard. Repeated calls grow to the
largest request rather than appending reservations. A failed growth preserves
the previous charge, and the requested size must fit the event-byte cap.
Core result admission briefly overlaps this reservation with its own payload
charge, so applications must provision for that conservative overlap.
Captured closures, private Rust allocations, and application-created pools remain
the component author's responsibility; the SDK is not an allocator or a process
RSS limit. Large outputs need a reservation before allocation or a bounded
streaming protocol. The supplied computation returns a fixed-size native value.

## Concrete computation

`NativeRuntime::prepare_sum_squares(parent)` prepares a component for activation.
Python's `actorplane.cpu.native_sum_squares(world, parent=None)` creates and
activates the same component. Its named/versioned interface accepts `Pulse(n)`
for `0 <= n <= 1_000_000` and replies with
`CountSnapshot(count=n, total=1*1 + ... + n*n)`. It iterates in Rust, checks
cancellation every 1,024 iterations, and uses checked integer accumulation.
Out-of-range input is a bounded component failure. It has no dynamic output
buffer and no hidden Python fallback.

The real CPU executor is unavailable in `TestWorld`; virtual tests can use its
controlled native responder to test their application protocol. They do not
simulate physical CPU work or claim deterministic blocking-thread scheduling.

## Observability and evidence

`NativeRuntime::cpu_stats()` and Python `world.inspect()["cpu"]` expose worker
and admission limits, queued/running gauges, and cumulative submitted, completed,
cancelled, panicked, and rejected counts. Submitted counts admitted CPU jobs,
including jobs canceled while waiting; rejected counts exhausted CPU admission
capacity, while other SDK admission failures follow ordinary error reporting.
Queued includes brief CPU reservations during SDK preparation. The gauges and
counters are sampled separately, not as one atomic snapshot.

`cpu_monitor()` returns a read-only handle retained by the Python adapter after
closing its scheduler. It owns only CPU counters/semaphores, so inspection can
still observe eventual retirement without retaining a World, payload, or thread.
CPU completion records closure/result
processing, not durable effects or peer acknowledgement. The request outcome is
the authority for whether an individual request completed successfully.

The [native example](../../crates/runtime-native/examples/cpu.rs) and
[Python example](../../examples/cpu.py) check an exact result and cleanup.
[CPU regressions](../../crates/runtime-native/tests/cpu.rs) cover admission caps,
queued cancellation/deadlines while a worker is occupied, independent timer
progress, metadata, panic containment, output limits, and truthful incomplete
shutdown. [Submission races](../../crates/runtime-native/tests/sdk_cancel_submission.rs)
force cancellation immediately before deferred submission. These are bounded
regressions; they do not establish throughput, tail latency, or sustained-load
fairness guarantees.

[Retirement tests](../../crates/runtime-native/tests/cpu_retirement.rs) check
running cancellation followed by reuse of the same operation slot, drain reply
completion, and virtual-runtime rejection. Unit tests drop unpolled and
worker-reserved jobs with panicking captured destructors and verify the resources
remain accounted during destruction. The
[held-GIL probe](../../tests/test_cpu_boundary.py) submits eight native
computations after the measured hold starts, validates their results, and stops
the native component before that hold ends, with zero adapter bridge entries.
