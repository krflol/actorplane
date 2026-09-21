# actorplane

A Rust-owned event runtime with a foreground Python actor driver. The native
source, routing, window accumulator and sink continue running while Python holds
the GIL. Python receives summaries through bounded queues.

This vault is also the source workspace. [actorplane.md](actorplane.md) remains
the original design contract. See [Build status](Build%20status.md) for implemented scope and
remaining gates. This is an initial development implementation, not a release
of every capability in that contract.

Source and hosted checks: [krflol/actorplane](https://github.com/krflol/actorplane).
Licensed under [MIT](LICENSE).

The [0.1.0 pre-alpha release](https://pypi.org/project/actorplane/0.1.0/) includes
a Windows x64 CPython 3.11 wheel and a source distribution:

```sh
python -m pip install actorplane==0.1.0
```

## Run it

Prerequisites: the pinned Rust 1.97.0 toolchain, GIL-enabled CPython 3.11, `uv`,
and the platform's native Rust linker/build tools. This build was verified on
Windows x64 with CPython 3.11.8. Linux/macOS jobs are configured in CI but have
not been executed from this workspace.

The [first hosted run](https://github.com/krflol/actorplane/actions/runs/35598889835)
was blocked before any job started by GitHub account billing/spending limits.
Platform validation remains pending. Fuzz execution is disabled.

The native example needs no Python package or interpreter linkage:

```sh
cargo run --locked -p actorplane-native --example native_counter
cargo run --locked -p actorplane-native --example native_sdk
cargo run --locked -p actorplane-native --example cpu
cargo run --locked -p actorplane-native --example shared_service
cargo run --locked -p actorplane-test --example virtual_pipeline
cargo run --locked -p actorplane-native --features native-io --example tcp_loopback
```

Build the Python package in the local virtual environment, then run the monitor:

```sh
uv sync --frozen --no-install-project
uv run --no-sync maturin develop --features extension-module,test-support
uv run --no-sync python examples/monitor.py
uv run --no-sync python examples/request_reply.py
uv run --no-sync python examples/supervision.py
uv run --no-sync python examples/schemas.py
uv run --no-sync python examples/components.py
uv run --no-sync python examples/envelopes.py
uv run --no-sync python examples/testing.py
uv run --no-sync python examples/connection_workflow.py
uv run --no-sync python examples/cpu.py
uv run --no-sync python examples/services.py
uv run --no-sync python examples/publications.py
```

The monitor creates an owned native pipeline, prints window summaries and stops
from a typed one-shot event. The monitor and request examples report actual counters. Timing and
counts depend on scheduler/OS timer granularity; no speedup is claimed.

```python
from actorplane import Actor, CountSnapshot, World, handles

class Monitor(Actor):
    def on_start(self, ctx):
        self.pipeline = ctx.native_counter(0.01, 0.1, target=ctx.actor)

    @handles(CountSnapshot)
    def summary(self, event, ctx):
        print(event.count, event.total)

with World(mailbox_capacity=64) as world:
    reference = world.spawn(Monitor)
    world.run_for(0.5)
```

`spawn` returns a pending reference. Construction and lifecycle callbacks occur
on the first `step`/`run` driver thread. `send` returns an admission ID; it does
not acknowledge handler completion. Self-sends queue later work. Call
`request_stop()` from a handler and `close()` from outside dispatch.

Requests reserve bounded native result slots and have exactly one terminal
outcome. `world.request(owner, target, event)` returns a nonblocking handle;
the receiver uses `ctx.reply(event)`, and `handle.result()` consumes the result.
Deadlines run natively even while Python holds the GIL. Use
`close(mode="drain", timeout=1.0)` to process admitted work under one deadline.
An already running Python callback must return before Python cleanup can finish.

Actors may create owned children with `ctx.spawn(Child)`. Explicit failure
policies select actor stop, World stop, or continued processing. Native
diagnostics use a bounded ring independent of business mailbox capacity.
Child failures produce bounded structured `Failure` records and are delivered
to a parent `on_failure(failure, ctx)` callback on a later pump. Notifications
coalesce per child and preserve the first record; captured traceback metadata
excludes exception text, arguments, locals, and source lines. Automatic restart
is not provided.

`@event` now supports bounded integers (including `uint64` ranges), floats,
booleans, strings, bytes, enums, optionals, sequences, and nested records.
`Annotated` metadata supplies `IntRange`, `FloatPolicy`, and `Length` constraints.
Python and Rust share a strict binary format; native validation checks the
registered schema before admission. See [schema examples and limits](docs/architecture/0005-native-schemas.md).

Actors can declare fresh Python or native components with `component(...)`.
Typed ports connect through native links, and named/versioned interfaces are
validated in Rust. Startup output is staged in bounded native storage; shutdown
owns the components and their routes. See [component composition](docs/architecture/0006-components-ports.md)
and [the executable example](examples/components.py).

Each delivery has a native envelope with schema and actor identities, a unique
event ID, optional deadline, correlation, causation, and bounded trace baggage.
Handlers inspect the immutable `ctx.envelope`; context methods propagate its
metadata. `MessageOptions` supplies explicit metadata at ingress. Queued and
staged messages expire natively, and timers release expired payloads even while
Python holds the GIL. See [envelope semantics](docs/architecture/0007-event-envelopes.md).

Rust components implement `NativeBehavior` with serialized state, validated
configuration, typed ports, bounded callback effects, timers, and deferred
request replies. The source, window counter, and sink use this SDK. Preparation
and startup run on the caller thread; steady-state hooks run on native workers.
See [the SDK contract and limits](docs/architecture/0008-native-sdk.md).

`actorplane.testing.TestWorld` drives the same runtime with a shared virtual
clock, explicit time advances, controlled native replies, and bounded pumps.
The Rust `actorplane-test` crate provides the Python-free harness. See
[TestWorld controls and limits](docs/architecture/0009-testworld.md).

`actorplane.tcp` provides a bounded native framed TCP listener. Each connection
reads one frame, requests a typed `TcpFrame` reply from a Python or native actor,
and writes that reply before reading again. Listener and connection scopes own
their sockets, tasks, operations, and byte reservations. The connection example
combines native echo, Python policy, a timer, and cancellation of a pending request.
See [TCP ownership, framing, and limits](docs/architecture/0010-framed-tcp.md).

The SDK's `defer_cpu_reply` moves Rust computations onto a separately bounded
blocking pool. `World(cpu_workers=2, max_cpu_jobs=32)` configures its workers and
admitted jobs. `actorplane.cpu.native_sum_squares` supplies a concrete typed
request component; CPU counters remain available after close. Running work must
cooperate with cancellation, and unfinished work stays visible after a shutdown
timeout. See [CPU admission and lifetime guarantees](docs/architecture/0011-bounded-cpu.md).

World services provide named native actors with explicit bounded leases. A
service must register an active root native component and an exact interface;
each holder receives a fresh child scope and uses typed service requests or
links. Stopping a holder revokes its scope, while stopping a service revokes
all scopes without stopping their holders. Quotas, generation checks, drain
behavior, and stale-result handling are described in [World services and
leases](docs/architecture/0012-world-services.md) and exercised by
[`examples/services.py`](examples/services.py).

The core indexes routes by source, port, owner, and target, and removes completed
operations from an exact deadline index. These indexes avoid unrelated-resource
scans on publication, actor route cleanup, operation expiry, and scoped operation
counts. Queued and staged events also have exact removable deadline indexes;
maintenance selects due actors and active drain roots, and allocation uses an
exact live count. See [indexed routing and deadlines](docs/architecture/0013-indexed-routing-deadlines.md),
[indexed event maintenance](docs/architecture/0014-indexed-event-maintenance.md),
and the [local measurement method](docs/validation/benchmark-method.md).

Publication now returns a bounded native ticket. The first routing batch captures
the subscription snapshot; later bounded turns preserve FIFO within a source and
use round-robin source scheduling. Plain core callers drive `World.route_batch()`;
NativeRuntime routes automatically and TestWorld requires explicit pumping.
Per-source/global ticket quotas, fan-out and aggregate snapshot limits, deadline
expiry, stop/drain fences, and retained terminal reports are documented in
[publication tickets](docs/architecture/0017-publication-tickets.md).

The Python driver selects native readiness hints in registration order and
waits detached on a native condition variable when idle. It preserves one
business claim per actor per pump, failure priority, and descendant-first
cleanup without scanning idle registrations. See
[Python readiness](docs/architecture/0015-python-readiness.md) for ordering,
deadline behavior, and cooperative callback limits. Native executors use
[targeted activity notifications](docs/architecture/0016-targeted-native-activity.md):
work, ownership, operation, route, and drain dependencies wake affected actors
without broadcasting to unrelated observers. Timer and deferred cancellation
checks retain their existing periodic schedule.

## Verify

```sh
cargo test --locked -p actorplane-core -p actorplane-native -p actorplane-test --features native-io
cargo clippy --locked --workspace --all-targets --features test-support -- -D warnings
cargo fmt --all -- --check
uv run --no-sync pytest
uv run --no-sync python scripts/check_dependency_firewall.py
```

The `test-support` feature enables finite, deliberate GIL-hold hooks. Integration
tests record native sink and TCP progress inside the hold, cancel natively before
release, check bridge entry counts and verify Python callbacks resume afterward.
Production wheels exclude these hooks:

```sh
uv run --no-sync maturin build --release --out dist
uv run --no-sync python scripts/smoke_wheel.py
```

See [the current API contract](docs/contracts/api.md) for ownership, codec,
admission and shutdown details, and [the boundary decision](docs/architecture/0001-native-boundary.md)
for architecture tradeoffs. The project uses the [MIT license](LICENSE).
The source is public on GitHub and the initial pre-alpha package is published
on [PyPI](https://pypi.org/project/actorplane/0.1.0/).
