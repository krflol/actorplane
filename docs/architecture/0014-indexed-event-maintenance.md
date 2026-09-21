# Indexed event maintenance

Queued-event expiry previously visited every actor and every queued or staged
entry on each maintenance call. Drain discovery scanned all actors twice more,
and allocation counted live actors across the entire registry. Idle maintenance
therefore grew with unrelated actor occupancy even when nothing was due.

## Decision

Each actor holds exact reference counts for its queued and staged deadlines in
an ordered map. A World index contains at most one entry per actor: its earliest
deadline, keyed by the full World/slot/generation identity. Admission adds the
deadline only after validation succeeds. Claim, expiry, startup flush, and stop
remove deadline references eagerly. Claimed deliveries leave this queue index;
pending requests retain their separate operation deadline until terminalization.
There are no canceled deadline tombstones that accumulate with submission history.

Maintenance removes only due actors from the World index, scans those actors'
bounded queues and staging buffers, and reinstalls each remaining earliest
deadline. Actors are processed in slot order, with FIFO order preserved within
each queue. A deadline can occur more than once, so claiming one delivery does
not remove another delivery's expiry. Queue and operation expiry still record a
request timeout only once.

Separate slot-ordered maps hold actors with staged output and active drain roots.
Activation checks effective lifecycle only for actors with staged output. Drain
maintenance checks the indexed roots and their owned trees. Stops remove these
memberships before retirement, and successful allocation/finalization update an
exact live-actor counter. All changes occur under the existing World state mutex.

When a parent drain absorbs a child already draining, the merged scope now keeps
the earliest deadline. Previously, the parent's deadline could silently extend
the child's deadline. Repeated drain requests can shorten the shared deadline;
they cannot extend it. Held tasks still prevent a truthful stop report from
claiming completion after timeout.

Index storage is bounded by existing actor and mailbox capacities. The changes
add no Python callbacks, payload copies, worker tasks, or public API parameters.
Service lease scopes use the same allocation and retirement paths.

## Evidence and limits

Focused regressions cover equal and out-of-order deadlines, claim versus expiry,
failed admission, stale actor generations, staged output, and merged drain
deadlines. Internal checks reconstruct metadata from authoritative actor state;
an independent seeded queue model compares lifecycle, admission, FIFO delivery,
expiry, and retained payload accounting.

The Python-free `maintenance_scale` example measures empty maintenance, future
queued deadlines, and allocation at increasing actor occupancy. See the
[measurement method](../validation/benchmark-method.md) for local before/after
results and qualifications.

Each affected queue is still scanned when its earliest deadline is due, and a
maintenance pass can process all due actors under one mutex. Active drain checks
still traverse their owned trees. Staging checks scale with staged sources, and
World inspection and shutdown enumerate the state they report or close.
Publication uses bounded tickets and routing batches. Its first batch still
copies the selected snapshot and performs route-index lookup under the World
mutex; subsequent attempts are bounded by the configured batch size and
revalidate each target. The subsequent [Python readiness index](0015-python-readiness.md)
removes Python registration scans, and [targeted activity](0016-targeted-native-activity.md)
replaces native broadcasts. Sustained stress and hosted platform validation
remain separate work.
