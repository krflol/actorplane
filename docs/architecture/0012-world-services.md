# World services and leases

The core service registry gives a native root actor a stable name and lets
starting or active actors acquire bounded child scopes for using that service. The registry
is implemented in [`crates/runtime-core/src/services.rs`](../../crates/runtime-core/src/services.rs).
The Python facade is in [`python/actorplane/services.py`](../../python/actorplane/services.py); the executable
shared-service example is [`examples/services.py`](../../examples/services.py),
using the native computation component in
[`crates/runtime-native/src/computation.rs`](../../crates/runtime-native/src/computation.rs).
The [Rust example](../../crates/runtime-native/examples/shared_service.rs) exercises
the same ownership without Python.

## Registration and acquisition

`World::register_service(name, target, required)` accepts only an active,
root-level native actor whose registered component publishes the exact named
and versioned interface. Names are nonempty UTF-8 strings of at most 128 bytes.
Duplicate names and target generations are configuration errors. Registration
stores the contract until service stop and is not tied to garbage
collection.

`World::acquire_service(owner, name, required)` creates and activates a fresh
native child scope under `owner`. The returned `ServiceLease` contains the
scope, holder, and service actor generations. `acquire_service_from`
additionally requires an expected service generation, which prevents a stale
name lookup from silently selecting a replacement actor. In Python,
`register(world, name, target, contract)` returns a `ServiceRef` whose
`acquire(owner)` checks the original target generation atomically.
`acquire(world, owner, name, contract, expected_target=None)` looks up the
current registration, optionally checking the supplied target. Both functions
are in `actorplane.services`.

Acquisition and output linking are allowed during actor configuration. Requests
require the holder's effective lifecycle to be active, so they cannot start
before successful activation.

Each service request names a declared input port and is checked against that
port's payload type before core request admission. `World::service_request`
routes the request with the lease scope as owner and records the exact
destination port in the envelope. `World::service_link` similarly requires a
declared service output and creates a route owned by the lease scope. Ordinary
operation terminal outcomes remain authoritative; a released lease cannot
accept a late completion.

## Quotas and lifetime

The default quotas are 64 services, 1,024 service leases, and 16 leases per
holder. The configured maxima are 4,096 services, 65,536 leases, and 1,024
leases per holder. Constructor arguments are `max_services`,
`max_service_leases`, and `max_leases_per_actor`. A zero value disables that
capacity. Each lease consumes one ordinary actor slot and adds no worker task.
Requests and links consume the existing operation, payload, mailbox, and route
budgets. Failed acquisition leaves the actor allocator and lease indexes unchanged.

Lease release is explicit through `World::release_service`. It is idempotent
for a missing or stale scope and rejects cross-World identities. Stopping a
holder removes its lease memberships and fences its lease scopes. Stopping a
service first unregisters the service and then fences every lease scope in its
other owners' trees; those owners remain alive. A stopped service generation
cannot be acquired through its old name or lease.

Dropping a Python or Rust token does not release its actor-owned scope.
`lease.release()` retires the scope explicitly. Operation results must be
consumed while that scope is alive; retirement makes their handles stale.
After release or retirement, queued cancellation can leave payload
bytes retained until a claim skips the canceled delivery, it expires, or the target is stopped; this is
reported by the normal World retention and shutdown snapshots. A service-owned
CPU job can outlive a stopped holder because the computation belongs to the
service actor; its operation still observes cancellation and its task remains
visible until it returns.

## Drain and cancellation

Draining a holder or service changes effective lifecycle eligibility. New lease
acquisitions, service requests, and links are rejected after the fence, while
already admitted deliveries may finish under the shared drain deadline. When a
service drain completes or expires, its registry entry and all lease scopes are
retired together. Retained terminal operation results remain bounded and must
be taken before the owning scope is fully retired. Polling a result does not
extend the scope's lifetime or prevent native drain completion.

`world.inspect()["services"]` exposes names, service identities, published
contracts, and lease counts. `world.inspect()["service_leases"]` exposes each
scope, holder, and service identity. Rust uses `World::services()` and
`World::service_leases()`. Snapshots are sorted; admission uses indexed lookups.

The registry uses generation-bearing actor references for every index, so slot
reuse cannot release or stop a replacement actor. Cleanup happens while the
World state mutex is held; no user callback or Python object is invoked during
registry detachment.

## Deliberate scope

Services are a named native actor contract with explicit child scopes. They do
not form a generalized resource pool, automatically wire TCP or other I/O
adapters, or add an implicit one-way send API. Typed requests and links use the
declared interface ports and the existing core admission, operation, routing,
and shutdown machinery.
