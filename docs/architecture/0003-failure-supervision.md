# Failure supervision and bounded reports

Failure handling has two separate paths. A failed Python delivery first reaches
the native lifecycle fence and is recorded as a bounded structured failure.
When the failed actor has a parent, the native runtime queues one control
notification for that parent. The notification is claimed by the foreground
driver on a later pump, so a failed handler never invokes a supervisor inline.
At most one notification is pending per child. Repeated failures coalesce and
retain the first failure record together with a bounded coalesced count. One
driver pump may process one control notification and one ordinary business
delivery for an actor, preserving control progress under a full business
mailbox.

The default `Actor.on_failure()` escalates an unhandled child failure to
`STOP_WORLD`. A custom hook may return `FailurePolicy.STOP_ACTOR`,
`STOP_WORLD`, `CONTINUE`, or `None`. `STOP_WORLD` fences the World even when
the failed child has already retired. Child actions cannot resurrect an actor fenced by native
cancellation. `STOP_ACTOR` cancels the child subtree, `STOP_WORLD` fences the
World, and `CONTINUE` consumes the failed delivery while keeping a live actor
eligible for later messages. A supervisor hook exception reports a
`Supervisor` failure and fences the World. Ordinary exceptions raise
`HandlerFailed` with the original as cause; fatal `BaseException` instances
propagate directly. Startup, construction, configuration, and
stop-hook failures are never resumable.

Failure records contain sequence and elapsed time, optional actor/event/schema
and operation identities, phase, handler, qualified exception type, action,
coalesced count, and copied traceback metadata. Handler and qualified exception
type strings are bounded to 128 UTF-8 bytes; file names to 256 and function
names to 128. At most eight outer-to-inner traceback frames are retained after
a bounded 256-frame walk. The capture path does not call `str(exception)` and
does not retain exception arguments, locals, source lines, traceback objects,
or frame objects. A diagnostic capacity of zero still permits supervisor
notifications; it only disables diagnostic-ring retention.

Failure control leases are separate from business leases. A pending control
notification prevents failed-owner slot reuse until it is claimed or its parent
is stopped. Reports and native cleanup remain bounded. Native panic polling,
factory boundaries, and task guards protect the runtime boundary; panic
destructors, process aborts, and the operating system's native panic hook are
outside this guarantee. Structured reporting therefore does not claim global
privacy for native panic text, which may still be written to stderr.
