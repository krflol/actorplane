# Shutdown reports and ownership accounting

Shutdown reports are read-only snapshots unless produced by a mutating stop or
close operation. `World.stop_report(ref=None)` reports a subtree or the whole
World without requesting cancellation and therefore always has `discarded ==
0`. An initial report returned by `stop()` is captured before Python cleanup;
call `stop_report()` afterward to refresh it.

Reports include queued deliveries, `delivery_in_flight`,
`control_in_flight`, `native_tasks`, `python_pending`,
`pending_notifications`, `outstanding_operations`, `retained_operations`,
`errors`, `last_error`, `in_flight`, `native_done`, `python_done`,
`discarded`, and `timed_out`. `in_flight` is the sum of delivery, control, and
native task work. Outstanding operations count unique pending operations whose
owner or target lies in the reported subtree; retained operations are terminal
results still owned by that scope. `native_done` may be true while Python
registrations or terminal Python-visible results remain.

Error totals and the latest bounded failure sample roll from stopped children
into their parent. World totals survive root slot retirement and generation
reuse. `last_error` is one latest sample, not a complete error history; the
separate bounded diagnostic ring provides sequence paging and may drop older
entries.

Diagnostic capacity zero disables the history ring, but preserves this one
bounded shutdown sample and reserved supervisor notifications. A subtree counts
each pending operation once even when both ends belong to it.

Close waits only within its native deadline budget. It never kills an executing
Python callback or a non-cooperative native task. A deadline timeout is sticky
in later reports, while Python cleanup can still finish afterward. Native work
may therefore be done even when Python references or terminal operation results
remain retained.

Python `close()` runs cleanup hooks cooperatively and records a deadline overrun
after a hook returns. Native shutdown timeouts persist in later World reports;
actor-scoped reports retain their drain timeout state. Full-stop accounting does
not depend on a scheduler handle still being present: retained core task leases
remain the source of truth after native shutdown returns.
