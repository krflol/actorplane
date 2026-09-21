# Drain, requests, and bounded diagnostics

Status: implemented development contract, September 20, 2026. This extends
[the native boundary](0001-native-boundary.md). It does not close every gate in
[the readiness audit](readiness-audit.md).

## Drain and ownership

Drain changes an owner and its descendants to `Quiescing` under the World lock.
New sends, tasks, children, and subscriptions are rejected. Existing claims and
queued work may finish. A task lease permits bounded completion publication,
including a final partial counter window. Sources and untriggered timers stop
creating work. The counter drains its input before the sink retires.

One native control task maintains deadlines independently of business task
permits and Python. A repeated drain keeps the earliest deadline. At expiry,
remaining work is fenced and queued deliveries are discarded. An invocation
already executing retains its lease until it returns. Native completion,
Python cleanup, in-flight work, discarded deliveries, and drain timeout are
reported separately. Python `close()` cannot preempt a blocking callback.

Python cleanup runs on the driver after fencing, with children before parents.
Cleanup can fence another actor without recursively invoking its hook. The
sweep is bounded by the registrations present when cleanup begins.

## Request reservations

An accepted request first reserves an operation slot and then attempts mailbox
and payload admission under the World lock. Failed admission releases the
reservation. Actor and operation handles both carry World, slot, and generation.

Reply, timeout, cancellation, failure, and owner/target stop compete for one
terminal transition. A reply checks the deadline at that transition; a claim
checks it before invoking the handler. A late reply cannot change the winner
or a replacement generation. Timeout does not prove that an in-flight external
effect never occurred.

Completed result payloads share the native retained-byte budget. Oversized or
unaffordable results become a bounded failure in the reserved slot. Retained
terminal results count toward `max_operations` until consumed or their owner
fully retires. `poll()` observes; `take()` and `result()` consume. These methods
never block the foreground driver waiting for completion.

## Failures and diagnostics

The default Python policy stops the failing actor's subtree. Root handler errors
raise `HandlerFailed`; child failures are delivered to a parent `on_failure` hook on
the next foreground pump; the hook may return `STOP_ACTOR`, `STOP_WORLD`,
`CONTINUE`, or `None`. One control notification is pending per child, repeated
failures coalesce around the first bounded record, and control delivery keeps
progress when a business mailbox is full. Startup errors, cleanup errors, and
`BaseException` are not resumable. Automatic restart is not implemented.

Native diagnostics occupy a separate bounded ring with monotonic sequence
numbers and timestamps. Reads report overwritten history and gaps; callers
advance from the last returned entry's sequence when paging. Structured failure
records include bounded phase, handler, qualified exception type, actor/event/
operation identities, action, and up to eight copied traceback frames. Supervisor
delivery additionally carries the notification's coalesced count. Exception
text, arguments, locals, and source lines are excluded. A
zero-capacity ring still supports supervisor notifications. Native panic output
to stderr remains outside structured-report privacy guarantees.

The current implementation favors a single auditable World lock. Operation
counts and ownership lookups are indexed; maintenance and subscription routing
still scan bounded registries. Threaded race tests and repeated multi-World
tests provide regression evidence, not a formal model-checking proof or a
production throughput guarantee.
