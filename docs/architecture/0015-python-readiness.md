# Native readiness for the Python driver

The Python driver previously checked every registration during failure,
cleanup, startup, business, and final cleanup phases. Idle actors incurred
repeated bridge calls. The idle driver then slept for one millisecond, even
when no Python work could run.

## Decision

The core maintains four ordered sets of Python endpoint hints: Start, Failure,
Delivery, and Cleanup. Membership changes under the same World mutex as queue,
lifecycle, and control-lease mutations. Failure readiness uses an exact count
of unclaimed child notifications; coalescing an existing notification does not
increase that count. Claim and discard decrement it. Native endpoints never
occupy these sets.

An immutable allocation order identifies each registration independently of
its reusable actor slot. Python registration is serialized, so allocation order
preserves its existing dictionary order. The counter rejects overflow before
allocation mutates any state. Retirement eagerly removes readiness memberships;
a replacement gets a new order and generation.

Start identifies Python endpoints whose own lifecycle is Starting. Component
children can still initialize while their parent is Starting; effective
lifecycle fences their business admission until the enclosing startup succeeds.
Failure and Delivery require effective Active/Quiescing state and no held
delivery or control lease. Delivery means a nonempty queue, which may still
contain expired, canceled, or unsubscribed entries. Claim remains authoritative
and discards invalid work. Cleanup identifies fenced Python registrations until
`finish_python`; it does not wait for native tasks that can outlive Python cleanup.

The driver captures an allocation cutoff once per phase and queries one hint
at a time. Start, Failure, and Delivery advance in registration order. If an
earlier handler sends to an idle later actor, that later actor can still run in
the same pump. Sends to an actor already passed wait for another pump. Newly
registered actors cannot enter the current phase. A fixed ready-list snapshot
would change these behaviors.

Cleanup repeatedly selects the latest eligible registration below its cutoff.
Children were registered after parents, so cleanup retains descendant-first
ordering. A hook that stops an existing sibling makes that sibling eligible in
the same sweep. Registrations created by the hook stay outside that sweep.
The driver retains one business claim and at most one failure claim per actor
per pump, with failure control before startup/business. Queries neither claim
work nor invoke callbacks; no backlog of Python payload objects is constructed.

A condition variable waits on the same state mutex that protects the readiness
predicate. Adding readiness notifies it, so a delivery arriving between an
empty query and the wait cannot be lost. The bridge detaches from Python while
waiting. Real driver waits are capped at one second and shortened by run/drain
deadlines. Driver loops recheck whether registrations remain after a pump;
cleanup of the last actor must not be followed by another wait. `TestWorld`
retains its explicit one-millisecond idle advance quantum and never uses this
wall-clock wait.

## Evidence and limits

Core tests check ordering/cutoffs, held claims, coalesced failures, ancestor
startup fences, stop/reuse, cleanup, timed waits, and wakeups. Internal checks
reconstruct all memberships and notification counts from actor state through
expiry, services, and repeated retirement. Python tests exercise callback
mutations, same-pump ordering, reused slots, cleanup cutoffs, fixed idle bridge
work, detached waits, and deadline-aware shutdown. Existing virtual-time,
failure, component, TCP, CPU, and held-GIL tests run through this driver.

The local [measurement method](../validation/benchmark-method.md) records idle
steps before and after the change. Empty-World overhead increases; the sample
does not establish loaded throughput or tail latency.

This change targets Python dispatch. The subsequent
[targeted activity change](0016-targeted-native-activity.md) replaces native SDK
broadcasts; native control/timer paths retain periodic checks. A pump can process every eligible registration below
its cutoff, bounded by the World registration limit; it is not a wall-clock
preemption guarantee. Python callbacks remain cooperative. Publication routing
now uses bounded tickets and batches; the Python readiness driver observes the
resulting endpoint hints and does not perform routing itself. First-snapshot and
route-index work remains proportional to selected fan-out under the World
mutex, while later attempts are batch-bounded. Sustained stress and hosted
platform validation remain work toward infrastructure readiness; see
[publication tickets](0017-publication-tickets.md).
