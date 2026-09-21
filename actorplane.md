Python# Rust World Runtime

## Guiding architecture and implementation specification

  

**Revision:** 0.1 — proposed architecture  

**Date:** September 20, 2026  

**Audience:** Project owner, implementers, contributors, and coding agents  

**Status:** Design contract, not documentation of an existing implementation

  

> **Build a Rust application runtime that processes events independently of Python. Make Python an excellent language for authoring behavior inside that runtime.**

  

 `actorplane`. All public APIs shown here are proposals. MUST identifies a correctness or product requirement; SHOULD identifies a strong default. Numerical settings in examples are illustrative, not benchmark-derived recommendations.

  

# 1. Product charter

  

The library is a Rust-based, event-driven application runtime with a Python scripting layer. A Rust-owned **World** coordinates stateful **actors**, reusable **components**, typed messages, event dispatchers, background operations, and explicit lifecycles.

  

The central feature is not a Python event bus implemented with a faster queue. It is a native execution environment that can receive, route, inspect, filter, aggregate, schedule, and handle events without executing Python. Python should be invoked when an application needs Python-authored behavior, not whenever an internal event moves through the system.

  

This is the architectural promise:

  

**After setup, a pipeline consisting entirely of native sources, native data, native operations, and native sinks can continue making progress even while the Python dispatcher cannot run.**

  

That promise is conditional on available native executor and operating-system resources, and on the pipeline not deliberately waiting for a Python-dependent result. It is not a real-time guarantee. Python callbacks still require the interpreter; standard GIL-enabled CPython does not become a parallel Python interpreter because its event runtime is written in Rust. PyO3 provides the attach/detach boundary needed for Rust-only work and Python interaction. [S1]

  

The surrounding library should make this capability useful rather than merely impressive. Developers should obtain comprehensible ownership, safe cancellation, bounded overload behavior, discoverable interfaces, good diagnostics, and an incremental path from Python behavior to native components.

  

The primary users are developers building long-running services, automation systems, agent applications, connection-oriented systems, and stateful processing pipelines. A typical application might own one actor per connection or workflow, combine it with native networking and timing components, and reserve Python for policy and application-specific decisions.

  

Success is not “every callback is faster.” Success is “less work needs a Python callback, and the work that remains has a predictable place to execute.”

  

# 2. Architectural invariants

  

These invariants take precedence over API convenience and local performance optimizations.

  

| ID | Requirement |

| --- | --- |

| I-01 | The runtime core and native execution crates MUST build and run without Python or a transitive PyO3 dependency. |

| I-02 | A native-only event path MUST NOT attach to Python, invoke Python, or require Python object destruction for progress. |

| I-03 | Native routing MUST operate on Rust-owned metadata and payloads, not Python dictionaries, Python type lookups, or Python predicates. |

| I-04 | Every actor, component, subscription, timer, operation, and retained completion MUST have a defined owner or an explicit World-service scope. |

| I-05 | Every buffering and concurrency mechanism MUST have a limit, including pending work outside visible mailboxes. |

| I-06 | Actor references MUST include World identity and a generation; stale references MUST NOT address replacement actors. |

| I-07 | Event delivery MUST be queued, not an inline recursive callback chain. |

| I-08 | Python callbacks MUST execute through the selected Python driver, with explicit thread affinity and no reentrant dispatch. |

| I-09 | Native runtime locks MUST NOT be held while executing arbitrary Python or waiting for the Python dispatcher. |

| I-10 | Results from retired owners MUST NOT mutate replacement owners or resurrect stopped actors. |

| I-11 | Submission, mailbox admission, handler execution, and durable effects MUST NOT be presented as the same guarantee. |

| I-12 | Native progress, overload, cancellation, and shutdown MUST remain observable without a functioning Python callback loop. |

  

A convenience API that violates an invariant must be redesigned or excluded. A wrapper, decorator, or thread pool does not create an exception to these requirements.

  

# 3. Scope and defaults

  

The first release is an **in-process runtime**, not a distributed actor platform, Python compiler, game engine, database, workflow durability system, or replacement for all uses of `asyncio`.

  

Borrow Unreal's useful authoring ideas: identity-bearing actors, owned components, lifecycle hooks, typed interfaces, and named event dispatchers. Do not import rendering, scene transforms, mandatory per-frame ticking, replication, editor tooling, or engine-sized inheritance hierarchies. Epic's component and dispatcher documentation provides the conceptual inspiration, not the concurrency specification for this library. [S2, S3]

  

| Area | Initial decision |

| --- | --- |

| Native execution | Rust core with a Tokio-backed execution adapter; separately bounded CPU/blocking work. |

| Python bridge | PyO3 in a dedicated adapter crate. |

| Python dispatch | One foreground driver per World; all Python actor and Python component callbacks execute serially on its thread. |

| Native state | Independently serialized per native component or native actor; unrelated native owners may run concurrently. |

| Event payloads | Typed, immutable, Rust-owned data with explicit Python codecs. |

| Application handlers | Synchronous, short Python handlers initially; long-running work returns through events. |

| Delivery | Volatile in-memory delivery; no automatic retries and no exactly-once claim. |

| Overload | Bounded queues and explicit rejection by default; lossy policies only where declared. |

| Lifecycle | Explicit startup, stop, cancellation, and completion reports. |

| Extensions | Source-level Rust component SDK first; independently loaded binary plugins deferred. |

  

Initially support an explicitly tested set of GIL-enabled CPython versions. Treat free-threaded builds as a separate compatibility target, not a prerequisite for the architecture or an untested marketing claim. PyO3's current documentation describes distinct free-threaded safety and module-declaration requirements; dependency defaults must not silently determine the project's support policy. [S4]

  

# 4. System architecture

  

## 4.1 Separate the native runtime from its Python client

  

The native runtime owns the actor registry, component instances, lifecycle state, subscriptions, routing tables, queues, admission budgets, timers, native operations, cancellation scopes, and native telemetry.

  

The Python adapter owns Python classes, Python actor instances, bound callbacks, Python exceptions, and conversions between Python values and native event data. Native runtime structures refer to Python endpoints through numeric identifiers, not through Python objects.

  

The Python library supplies the pleasant authoring surface: decorators, actor classes, component declarations, typed references, interface facades, configuration, test helpers, and errors with useful context.

  

The core dependency direction is:

  

```text

Python application

        |

Python authoring library + PyO3 adapter

        |

Rust World API

        |

Rust core + native components + execution adapter

```

  

Native components depend on the World API and execution facilities, never on the Python adapter.

  

## 4.2 The World is an ownership boundary, not one giant mutex

  

A World may use a coordinator for registration and lifecycle transitions, but it MUST NOT execute every handler or every payload transformation on one coordinator thread. The initial implementation can use straightforward indexed registries and short critical sections. Sharding is an optimization, not the starting architecture.

  

Separate actor and endpoint mailboxes from the registry. Move payload processing into native component execution. Avoid holding registry access while routing a large fan-out; capture an eligible subscription snapshot and process it within a bounded work budget.

  

A coordinator must never wait synchronously for a Python callback to finish before allowing unrelated native work, native cancellation, or native cleanup to proceed.

  

## 4.3 Execution domains

  

| Domain | Responsibilities | Python permitted? |

| --- | --- | --- |

| Native control | Lifecycle, registration, scheduling decisions, owner fencing, cancellation, admission. | No. |

| Native asynchronous work | Network readiness, asynchronous operations, timers, short native handlers. | No. |

| Native CPU/blocking work | Parsing, compression, native computation, unavoidable blocking operations. | No. |

| Python dispatch | Payload materialization, application callbacks, Python lifecycle hooks. | Yes. |

| Optional host integration | An explicitly selected host loop, later including `asyncio`. | At its defined boundary. |

  

Do not run arbitrary Python callbacks on Tokio worker threads. Do not run large CPU loops on asynchronous workers. Tokio documents why blocking/CPU-heavy work can stall its executor and why blocking jobs need explicit concurrency limits. [S5]

  

# 5. What “processing events off the GIL” means

  

There are three important paths.

  

**Native to native.** A native source produces a Rust payload. The World validates its owner and schema, performs routing and admission, schedules a native handler, and delivers its native outputs. This path never attaches to Python.

  

**Python to native.** Python constructs or supplies event data. The adapter validates and converts that data while attached to Python. Once the World owns the native event, its subsequent processing is independent of Python. A Python producer remains a Python producer; the ingress cost is not magically removed.

  

**Native to Python.** Native processing creates a delivery addressed to a Python endpoint. A bounded native mailbox retains the native data. The Python driver later materializes a Python event and invokes its handler. Other native work does not wait for this callback unless the application explicitly models that dependency.

  

Native processing includes real behavior: field comparisons over native values, routing, counters, state transitions in native components, aggregation, batching, deadline handling, timer expiration, native I/O completions, and native output generation. Merely storing `Py<PyAny>` in a Rust channel is not this architecture.

  

A Python lambda is a Python operation even when registered through a Rust API. Native filtering must use a compiled native component or a deliberately limited, validated native predicate representation. Do not introduce a general-purpose expression language or attempt to compile arbitrary Python functions in the first release.

  

The reference pipeline should be:

  

```text

Native source -> native routing -> native accumulator

              -> native window timer -> summary event -> Python

```

  

The number of Python notifications should follow the number of summaries requested, not the number of raw inputs processed. This is an application-design target, not a claim of any measured speedup.

  

# 6. Event data and schema contract

  

## 6.1 Native envelopes

  

An internal envelope should carry an event identifier, schema identifier/version, source identity, destination or dispatcher identity, owner generation, enqueue time, optional monotonic deadline, optional correlation/causation identifiers, trace metadata, and a native payload reference.

  

Fields should be compact native types. Bound trace baggage and other metadata. Do not place arbitrary exception objects, Python dictionaries, or callbacks in an envelope.

  

Use collision-checked schema registration and compact runtime IDs. A public event name plus schema version is an interchange identity; a Rust `TypeId`, Python class address, or process-randomized hash is not a persistent schema identity.

  

## 6.2 Supported data

  

The initial schema system should support explicit integer ranges, floating-point values with declared non-finite-value policy, booleans, UTF-8 strings, bytes, enumerations, optionals, immutable sequences, and nested records. Fix the encoding rules before exposing them publicly. Reject unsupported fields at registration, not after traffic has started.

  

Python `int` fields need a documented width or a deliberate arbitrary-precision representation. Do not silently truncate them. Floating-point NaN behavior must be defined before exposing equality-based filtering or keyed aggregation. Limit nesting depth, collection length, string length, and total event size.

  

Choose a simple schema-indexed Rust representation first, with shared immutable buffers where helpful. Avoid compulsory JSON serialization for every internal hop. Native-only typed representations may be added behind the same envelope, but events crossing the Python boundary require an explicit codec.

  

## 6.3 Snapshot and buffer semantics

  

Sending an event snapshots its supported Python data into Rust-owned storage. Subsequent mutation of the caller's Python objects must not change an already admitted event. A frozen outer Python object is not sufficient when it contains mutable nested data.

  

The initial implementation should copy Python-owned bytes into native ownership unless a separately documented ownership-transfer mechanism exists. A later `NativeBuffer` can provide views over Rust-owned immutable memory with explicit lifetime and size accounting. Do not advertise zero-copy delivery when decoding, refcount management, or buffer release still needs hidden interpreter work.

  

Do not accept arbitrary Python objects in the native payload path. A future Python-local opaque-message feature must be visibly separate and excluded from native processing and native-independence guarantees. Automatic pickle is outside the design.

  

## 6.4 Registration and codecs

  

An `@event` declaration creates schema metadata and authoring helpers. At World registration, validate the complete schema and build a conversion plan. Per-event routing must not inspect Python annotations, perform `isinstance` against a Python class graph, or look up Python field names.

  

Schema failures should identify the event, field path, expected type, actual type, and limit violated. Codec failure is distinct from queue saturation or handler failure.

  

# 7. Python execution and bridge safety

  

## 7.1 Foreground driver

  

The initial `world.run()` owns Python dispatch on the calling thread, normally the main thread. Actors declared before `run()` are instantiated and initialized on that driver thread. `spawn()` returns an `ActorRef`, not a live Python actor object; readiness is a separate lifecycle state.

  

Native workers run independently. When no Python work is ready, the driver waits through a detached Rust operation rather than holding the interpreter indefinitely or busy-polling. When work becomes available, it handles a bounded batch, checks lifecycle eligibility, and calls Python. The bridge should use the current PyO3 attach/detach mechanisms and follow its lock-order guidance. [S6]

  

One World has one driver at a time. Recursive `run()`, dispatch from inside a handler, or simultaneous host drivers must fail clearly. Context managers and explicit close are the primary lifetime mechanism; finalizers are only defensive fallbacks.

  

## 7.2 State isolation

  

All Python callbacks in a World are serialized initially. That includes Python actors, Python components, and their lifecycle hooks. Their state may be mutated only on the driver thread. External callers interact through references and queued commands, not through retrieved actor objects.

  

A native component has its own serialized state execution. It can make progress while the Python behavior of its owning actor is busy. Therefore, an actor containing native components is an **ownership aggregate**, not a promise that every piece of behavior within that aggregate is globally serialized.

  

This distinction is essential. A single actor-wide lock held throughout a Python callback would make its native components depend on Python progress. Native and Python state must instead be separate, with messages as the boundary. A feature that needs cross-domain coordination must model a protocol; it cannot share mutable state implicitly.

  

## 7.3 No reentrancy or hidden blocking

  

Sending to oneself or emitting from a handler queues later work. It never executes another handler inline. Local Python helper calls are ordinary calls, but cross-actor behavior always uses messages or typed operation proxies.

  

A Python handler must not synchronously wait for World work that might require the same driver. Reject blocking waits from handler context, including seemingly harmless native operations whose dependencies are not provably Python-free. Returning an operation handle and receiving a completion event is the default pattern.

  

If future releases permit `async def` handlers, decide whether a handler retains actor exclusivity across suspension. Do not silently allow another event to mutate actor state while a suspended handler believes it has exclusive ownership. Initially reject coroutine handlers with a targeted registration error.

  

## 7.4 Python object ownership

  

Keep Python instances and callables in an adapter-owned registry. Native endpoints carry stable handler IDs and generation stamps. The runtime must never ask Python to compute an event's hash, equality, representation, route, or finalizer on a native worker.

  

A queued Python delivery retains native data until dispatch. Materialize its Python object only when the driver is ready. Account for bounded adapter batches and temporary native conversion buffers; do not move an entire backlog into an unbounded list of Python events.

  

Python references awaiting retirement remain subject to an adapter registration limit. Use per-owner retirement state and a bounded owner registry, not an unbounded stream of finalizer callbacks. While Python is unavailable, prevent unlimited creation of replacement Python instances or callback registrations. Runtime-owned native data can be reclaimed without Python; Python-owned references cannot be honestly reported as reclaimed until their driver has released them.

  

# 8. Actors, components, references, and interfaces

  

## 8.1 Actors

  

An actor is a stateful application object with identity, a lifecycle, an ownership scope, and event endpoints. It does not require its own operating-system thread. Actors may be Python-authored or native; the core lifecycle machinery is the same.

  

An `ActorRef` is a non-owning address with World ID, actor slot/ID, and generation. It supports communication and lifecycle observation, not direct access to another actor's mutable state. Retaining a reference does not keep an actor logically alive. Cross-World use is an error initially, not an implicit transport operation.

  

Separate existence, readiness, acceptance of new work, and full termination. A pending reference may exist before startup succeeds. Business messages to an actor that is not yet ready should fail with `ActorNotReady` unless the caller explicitly selects a bounded startup-admission feature.

  

## 8.2 Components

  

A component is an owned capability with its own configuration, ports, state, and cleanup requirements. Python components execute with their actor's Python state. Native components execute in native contexts and communicate with their owner through typed messages.

  

Component declarations on a class are immutable descriptions. Each actor gets new component instances; descriptors must never accidentally create a singleton socket, cache, timer, or mutable buffer shared by all actors.

  

Initial component composition is fixed during startup. Validate required interfaces, configuration, schema compatibility, and dependency cycles before activating external ingress. Dynamic component replacement and hot reconfiguration are later features.

  

World-scoped services provide intentionally shared resources, such as a connection pool. Actors own leases to those services, not the services themselves. Stopping one actor releases its lease without silently shutting down every other user.

  

## 8.3 Interfaces and dispatchers

  

An interface is a named, versioned capability contract: input commands, output events/results, errors, cancellation behavior, and relevant ordering guarantees. A Python `Protocol` can describe its authoring facade, but runtime validation must check the registered native contract rather than assume arbitrary duck typing is sufficient.

  

A method-looking call across an execution boundary is a command submission. It does not directly invoke a foreign actor or mutate a native component's state on the Python thread. Results arrive through an operation handle or typed completion event.

  

A dispatcher is a named typed output endpoint owned by an actor or component. It can have multiple subscribers, but all callbacks are queued. Subscription cancellation is automatic when either the subscriber's scope or the source endpoint ends. Global topics, where supported, are explicit World-owned dispatchers rather than unscoped string names.

  

# 9. Ownership and lifecycle

  

## 9.1 Ownership graph

  

Ownership is a tree: World, actor scopes, component scopes, and operation scopes. Dependency relationships may form a separate directed acyclic graph; do not confuse “depends on” with “owns.” A parent stop cascades into its owned children.

  

Subscriptions, timers, and tasks created through a handler's context inherit that context's owner. World ownership must be requested explicitly. Dropping a convenient Python token does not implicitly cancel its underlying owner-managed resource; use `cancel()` or stop its owner. This avoids garbage-collection timing becoming business semantics.

  

Every resource is indexed by owner so cleanup does not require scanning every subscription or operation in the World. Debug snapshots must make ownership visible.

  

## 9.2 Startup

  

Use a lifecycle such as `Allocated -> Starting -> Active -> Quiescing -> Stopping -> Stopped`. Failures are recorded as a terminal reason and enter the same teardown machinery. Immediate cancellation may skip `Quiescing`.

  

Startup allocates a fresh generation, builds component instances, validates dependency order, prepares callback and schema registrations, and stages routes and owned resources. Run configuration and startup hooks exactly once per successful invocation attempt; activate business ingress only after required startup has succeeded.

  

Python `configure(ctx)` declares links and subscriptions. `on_start(ctx)` performs application initialization. Work emitted during startup uses a bounded staging area and is released only after activation. New subscriptions must not expose a half-initialized Python handler to native traffic.

  

If startup fails, cancel staged operations, remove staged routes, close acquired resources, release registrations, and complete readiness with the original error. This is resource rollback, not a transaction that can undo arbitrary external side effects. Startup hooks must not assume a failed startup erases a network request already sent.

  

## 9.3 Stop modes

  

**Cancel** stops admission, prevents unclaimed business callbacks from starting, cancels owned timers and operations, and discards queued business work with recorded outcomes. It is the default for actor destruction and failure.

  

**Drain** first quiesces producers and new admissions, then permits already admitted work to finish until a deadline. Draining callbacks cannot create new long-lived owned work. Completion of previously accepted requests and explicitly defined cleanup actions remain permitted. At the deadline, transition to cancellation.

  

The distinction is observable. Do not document “destroy” while implementing indefinite drain, or document “graceful shutdown” while silently dropping all work.

  

## 9.4 The stop fence

  

A stop has a defined linearization point in native lifecycle state. An invocation that has already claimed an execution lease is in flight; later invocations cannot claim one after the cancellation fence. A claimed callback may finish, but cannot create valid new owned work after its owner has been fenced.

  

Queued and already batched deliveries must be revalidated at invocation claim, not only when first routed. Unsubscribe uses the same principle: after acknowledgement, no new invocation claim for that subscription succeeds; an invocation already claimed may finish.

  

Stopping invalidates future delivery to the old generation. A replacement actor receives a fresh generation and fresh state. Old timers, results, subscriptions, and operation handles cannot target it accidentally.

  

## 9.5 Cleanup and completion reporting

  

Native resources must not depend on a Python `on_stop` hook to be released. On cancellation, Rust can close ingress, revoke leases, stop timers, cancel cooperative tasks, and close native resources while the Python dispatcher is stalled.

  

Run the Python `on_stop(ctx, reason)` hook when the driver can safely do so, after the actor is fenced. It is for Python-side/application cleanup and must not assume native resources are still usable. A hook failure is reported, but does not prevent other cleanup.

  

Track native quiescence, pending Python cleanup, and unfinished work separately. A stop report should contain `native_done`, `python_done`, outstanding operations, timed-out cleanup, discarded deliveries, and errors. Report fully `Stopped` only when its stated full-stop conditions are satisfied.

  

An executing Python callback or non-cooperative native job cannot be safely treated as terminated merely because a deadline expired. Tokio likewise documents that a started `spawn_blocking` task cannot be aborted by calling `abort`. [S5] Preserve required allocations, fence results, and report incomplete termination. Hard termination requirements belong behind a process boundary.

  

World shutdown uses one shared deadline, not a fresh full timeout for every actor. It requests shutdown, propagates cancellation, and waits for tracked work; cancellation notification alone is not proof that tasks have finished. Tokio's shutdown guidance makes this separation explicit. [S7]

  

# 10. Communication and delivery semantics

  

## 10.1 Distinct operations

  

| Operation | Purpose | Immediate result |

| --- | --- | --- |

| `send(target, event)` | Nonblocking point-to-point delivery attempt. | Receipt confirming mailbox admission, or a typed rejection. |

| `publish(dispatcher, event)` | Submit a multicast event for native routing. | Ticket confirming routing-ingress admission, or a typed rejection. |

| `request(target, command, timeout=...)` | Start a correlated operation with a bounded completion slot. | Operation handle, or a typed rejection. |

| `subscribe(source, target=...)` | Add an owner-scoped typed route. | Subscription token after installation, or setup error. |

| `link(source, target_port)` | Connect compatible native ports without a Python callback. | Owner-scoped link token, or setup error. |

  

These are semantic names; exact spelling may change before API freeze. In handler context, none may wait for downstream handlers to run. Waiting variants belong only in explicitly supported external/host contexts and must detach while waiting on Rust.

  

A successful `send` means the target mailbox admitted the event. It does not mean the handler ran. A successful `publish` means the routing work was admitted; the ticket later exposes a bounded delivery report describing subscriber admissions and rejections. Neither means a business effect is durable.

  

## 10.2 Ordering

  

For a sequential sender using the same ordered endpoint, admitted events retain order at that endpoint. Concurrent senders are ordered by the runtime's admission point, not by wall-clock intent. There is no total order across unrelated actors, components, dispatchers, or execution domains.

  

FIFO is an admission/dispatch rule, not a promise that independently spawned work completes in order. A native component that launches parallel work must sequence its results explicitly if its interface requires ordered completion.

  

Initially permit one registered handler per event schema per addressed endpoint. Duplicate declarations are registration errors. Different components may subscribe independently; the runtime does not invent an implicit order between them.

  

## 10.3 Fan-out

  

A publication captures a subscription snapshot at native routing time. A subscriber added after that snapshot does not receive the publication. Every delivery still checks its subscription generation and owner lifecycle before execution.

  

Default fan-out is independent per subscriber: a saturated Python subscriber does not stall healthy native subscribers. The delivery report can record partial delivery. Rejected subscribers do not make the runtime retry already admitted subscribers.

  

Do not implement this by awaiting each mailbox in a serial loop. A single slow consumer would then block the whole routing pass. Use nonblocking per-destination admission and bounded routing batches. All-or-nothing fan-out is a separate, deferred feature requiring multi-destination reservation and rollback semantics.

  

## 10.4 Reliability and requests

  

The initial runtime offers **at-most-once invocation attempts per admitted delivery**, without automatic retries. Admission can still be followed by cancellation, expiration, failure, or process loss. This is not guaranteed execution or exactly-once effects.

  

A request reserves its result bookkeeping before work starts. Success, error, timeout, cancellation, and target termination compete to establish one terminal result. Losing late replies are discarded and counted. Timeout does not establish whether an external side effect already occurred; return an explicit indeterminate outcome where appropriate.

  

Retries require opt-in policies, bounded attempts, deadlines, and an idempotency story. Persistence, replay, and remote delivery need their own protocols and are not implied by an event ID.

  

# 11. Boundedness and overload

  

A bounded mailbox is necessary but insufficient. Tokio's channel guidance explicitly warns about implicit queues and the need to bound overall concurrency. [S8] The World must account for work before, during, and after mailbox admission.

  

Bound at least: actor/component registrations, mailbox entries and bytes, ingress routing work, subscription count, fan-out deliveries, pending operations, task submissions, simultaneously running jobs, timer entries, retry state, coalescing keys, native output buffers, Python-ready batches, adapter registrations, diagnostic retention, and trace-label cardinality.

  

Count native payload storage retained by queues, handlers, and outstanding operations. Shared immutable buffers need a consistent accounting rule: count physical native storage once and charge per-delivery metadata separately. Apply per-owner fairness quotas as well as a World-level cap. A single accepted event must not expand into unbounded outputs.

  

Reserve admission budgets before expensive copies or allocations where practical. Bound codec scratch space and native parsing buffers. Arbitrary Python handler allocations and external-library memory are not covered by a guarantee about runtime-managed buffers; do not claim a bound on total process RSS.

  

## 11.1 Policies

  

Default application-message overload is a visible `QueueFull` or `BudgetExceeded` result. Lossy notification streams may explicitly select drop-newest, drop-oldest, or latest-value coalescing. Every loss or replacement is counted by reason.

  

Coalescing is valid only for declared replaceable state updates. It is not appropriate for arbitrary commands or request completions. Its key table must itself be bounded. Superseded retained tickets receive a `Superseded` outcome where the API exposes per-delivery status.

  

A full queue must never be “solved” by spawning unlimited tasks that wait to send, by creating an unbounded side queue, or by pushing backlog into Python objects.

  

## 11.2 Control and completion capacity

  

Shutdown must not compete with a full business mailbox for its only available slot. Use reserved control capacity and coalesced per-owner cancellation/lifecycle state. Bound that state by the configured owner registry and operation limits; “control plane” is not permission to use unbounded queues.

  

Reserve a bounded terminal-result slot for an accepted request or tracked operation. Payload size limits still apply; an oversized result becomes a bounded failure result. Large native computation output must acquire its output budget before allocation, or stream through a bounded interface.

  

A stopped Python consumer can retain at most its admitted native data and bounded registration state. New native events follow the configured reject/coalesce policy while unrelated native consumers continue. If a pipeline explicitly uses reliable backpressure from that Python consumer, its resulting pause is an intentional application dependency, not a scheduler defect.

  

# 12. Scheduling, timers, and owned work

  

## 12.1 Scheduling

  

Use readiness-driven scheduling, not a mandatory tick over every actor. Idle actors should not require a Python call or a native task wakeup merely to remain alive.

  

Schedule ready endpoints fairly with bounded events and native work per turn. Give lifecycle control precedence without permitting an endless control stream to starve ordinary work. Chunk large fan-out and large native transforms, or move them to the bounded CPU executor.

  

Thread-pool size is a World/runtime configuration concern, not a per-component free-for-all. Avoid nested oversubscription. Component authors must not create private unbounded pools behind an apparently bounded operation API.

  

## 12.2 Timers

  

Timers live in Rust and use a monotonic clock. Wall-clock schedules are a separate adapter. An initial timer can deliver a registered native payload to an endpoint; it must not execute a Python callback to construct its payload on every expiration.

  

Support one-shot timers and a clearly specified periodic mode first. For fixed-rate periodic timers, default missed-tick behavior should skip or coalesce missed occurrences rather than issue an unbounded catch-up burst. Fixed-delay-after-handler-completion is a different contract and can be deferred.

  

Timer expiration means an event became eligible for delivery. It does not guarantee the Python handler ran at that exact instant. Report expiration lag separately from mailbox/dispatch lag.

  

## 12.3 Native operations

  

A submitted native operation receives owned input, cancellation state, a deadline, a budget, and a bounded completion route. It must not retain a mutable reference to its actor across asynchronous execution. State is updated later through a result event or through its native component's serialized state context.

  

All library-owned spawning goes through an ownership-aware task service. Track queued and running jobs, and ensure completion is observable even when the event consumer disappears. On owner stop, cancel queued jobs, signal running jobs, fence results, and retain only the bounded state required to finish safely.

  

Do not label `submit_python(function)` as off-GIL native computation. A future Python worker facility must say where its code runs, which interpreter rules apply, whether actor state is accessible, and how cancellation works. Prefer explicit native operations before adding generic escape hatches.

  

# 13. Failures and supervision

  

Python handler exceptions become structured `HandlerFailed` records containing actor identity/generation, event/schema identity, handler identity, exception type, traceback, and correlation information. Capture interpreter-owned details in the adapter and pass a bounded native diagnostic representation to the core.

  

The default should stop the failing actor and report the failure to its supervisor. Continuing after an exception is an opt-in application policy because actor state may have been partially mutated. Do not swallow exceptions, print them, and silently keep processing.

  

A root actor failure should produce a failing World run result by default. Applications may install a supervisor with an explicit continue, stop-child, or stop-World policy. Automatic restart is a later feature: it requires a new generation, fresh state, restart-rate limits, and a documented policy for old queued work. Do not replay messages blindly.

  

Rust errors are typed results. Catch a panic only at an intentional containment boundary where unwind behavior and state safety have been considered; do not continue using potentially inconsistent component state. Fatal process failures remain outside the runtime's recovery guarantee.

  

Cap retained diagnostics and do not route failure logging through the same saturated business channel. Failure reporting itself must not trigger infinite recursive error events.

  

# 14. Native component SDK and gradual optimization

  

The Rust SDK is a first-class product surface. A component declares its configuration schema, input ports, output ports, implemented interfaces, native state ownership, operation budgets, lifecycle behavior, cancellation behavior, and diagnostic identity.

  

A short native event handler operates on exclusively owned component state and emits bounded effects through a native context. Long operations receive copied/shared immutable input and return results. Native handlers must not use a hidden Python fallback.

  

Initially, compile components against the same Rust workspace/runtime API or deliberately matched source-level SDK. Do not promise a stable ABI for loading arbitrary independently compiled Rust trait objects. A future binary plugin mechanism needs a versioned ABI, allocator/ownership rules, runtime-instance compatibility, and lifetime management; that is separate engineering.

  

Gradual optimization should preserve a component's **contract**, not its private implementation. Keep schemas, error cases, ordering requirements, cancellation semantics, and interface names stable while replacing a Python component with a native component. Timing and concurrency differences must still be considered and tested.

  

Provide contract tests that run against both implementations. A Python implementation can serve as a reference oracle for a native implementation. Do not automatically compile Python or claim that adding `native=True` makes a Python function execute outside the interpreter.

  

The highest-value optimization is often a larger native processing segment: native parsing, filtering, accumulation, and output generation with one summary sent to Python, rather than repeated Python/native round trips for individual primitives.

  

# 15. Proposed Python authoring experience

  

The API should feel like a Python library, not a Rust API translated word for word. Decorators should declare behavior, not hide scheduling decisions. Type hints and generated stubs should make ports, events, errors, and operation results discoverable in an editor.

  

The following is a **proposed API example**, not code that can run against an existing package. It demonstrates the minimum native-processing story the implementation should support.

  

```python

from actorplane import (

    Actor, Duration, World, component, event, handles, native,

)

  
  

@event(name="example.StopMonitor", version=1)

class StopMonitor:

    pass

  
  

class Monitor(Actor):

    # Descriptions only. The World creates fresh native instances.

    source = component(

        native.PulseSource,

        interval=Duration.milliseconds(10),

    )

    counter = component(

        native.WindowCounter,

        window=Duration.seconds(1),

    )

  

    def configure(self, ctx):

        # Native ports connect without a Python forwarding callback.

        ctx.link(self.source.pulses, self.counter.input)

        ctx.subscribe(self.counter.snapshots, target=ctx.actor)

  

    def on_start(self, ctx):

        ctx.after(

            Duration.seconds(5),

            StopMonitor(),

            target=ctx.actor,

        )

  

    @handles(native.CountSnapshot)

    def on_snapshot(self, event, ctx):

        # Python observes summaries, not every source pulse.

        print(f"Window count: {event.count}")

  

    @handles(StopMonitor)

    def on_finish(self, event, ctx):

        # This requests shutdown; it does not wait inside the handler.

        ctx.world.request_stop(

            mode="drain", deadline=Duration.seconds(2),

        )

  
  

with World(

    name="native-counter-demo",

    mailbox_capacity=256,

    mailbox_bytes=1_048_576,

    native_payload_budget=67_108_864,

) as world:

    world.spawn(Monitor)

    world.run()

```

  

`PulseSource`, `WindowCounter`, and `CountSnapshot` are deliberately small reference components. Source production, counter mutation, window expiration, and snapshot construction all happen in Rust. Python handles only snapshot notifications and the application stop message. Timer precision and snapshot count depend on scheduling and the declared missed-tick policy; the example does not promise exact wall-clock delivery.

  

The test suite must also run the same native source/counter path with a native sink and no Python subscriber. Pausing the Python driver must not stop that path. A later native network source should slot into an equivalent typed pipeline without requiring a Python read/forward loop.

  

## 15.1 Minimum public concepts

  

Expose `World`, `Actor`, `ActorRef`, `Component`, typed native component proxies, `@event`, `@handles`, typed dispatchers/ports, owner-bound context helpers, operation handles, and structured exceptions. Everything else should justify its place through a concrete example.

  

Handler contexts should expose communication, scoped timers, scoped operation submission, cancellation observation, trace context, and lifecycle requests. Native capabilities must be distinguishable from Python callables in both documentation and introspection.

  

Keep common setup short, but provide explicit advanced configuration. Avoid global default Worlds, decorators that start threads at import time, automatic event subscriptions based on naming conventions, and implicit cross-actor method dispatch.

  

## 15.2 Asyncio and other hosts

  

A later `asyncio` integration should adapt the Python dispatch boundary, not move the Rust World onto Python's event loop. Choose one host loop, schedule wakeups through thread-safe facilities, and deliver bounded batches there. CPython documents that most asyncio objects are not thread-safe and provides explicit cross-thread scheduling mechanisms. [S9]

  

Do not create one scheduled Python-loop callback per raw native event. Coalesce readiness notifications. Preserve the same ownership, overload, and callback-affinity contracts as the foreground driver.

  

Host integration is separate from supporting coroutine actor handlers. It is possible to pump synchronous actor callbacks from an asyncio host without allowing arbitrary `await` inside actor state transitions.

  

# 16. Useful standard components without an oversized core

  

The core must ship with enough native behavior to prove its purpose. A useful first component set includes native timers, the reference pulse source and window counter, and a bounded native operation service with one concrete computation example.

  

The next integration should be a native I/O source and sink, such as a bounded framed TCP or local-stream adapter used in a loopback demonstration. Enforce maximum frame size, connection limits, output buffering, cancellation, and explicit close semantics. Do not turn a minimal transport demonstration into an undocumented production networking framework.

  

HTTP clients, file watching, bounded caches, persistence adapters, subprocess management, batching, debounce/throttle, and retry policies belong in optional components as their contracts become clear. Every such component must pass the same ownership and native-independence checks. A component implemented by calling a Python client library is a Python integration, not native I/O.

  

Avoid dependencies that force every user to install database drivers, cloud SDKs, an agent framework, a UI stack, or a complete networking feature set. Native components can be feature-gated in Rust and grouped into deliberate Python distribution options.

  

# 17. Observability, inspection, and debugging

  

Observability is part of the runtime contract. A user must be able to answer: “Who owns this operation?”, “Why did this event not reach Python?”, “Which queue is full?”, and “What is preventing shutdown?”

  

Provide a native snapshot containing actor/component identities and states, owned resource counts, subscription topology, queue entries and retained bytes, operation states, timer deadlines, and the boundary classification of each endpoint. Expose the snapshot through a Python API when Python is available and through native diagnostic sinks/test APIs independently of Python.

  

Distinguish generated, submitted, rejected, admitted, coalesced, expired, invocation-started, completed, failed, and cancelled events. Report the reason for every non-delivery category. Do not label the number of attempted sends as processed throughput.

  

Measure native queue delay, native handler time, Python-ready delay, Python materialization time, Python handler duration, timer expiration lag, work-in-flight, bytes crossing each boundary, and shutdown phase durations. Instrument bridge entry points so tests can count interpreter interactions attributable to native processing.

  

Tracing must retain correlation across background work and completion events without carrying Python context objects into Rust. Native logs and metrics should use bounded native sinks or exporters; a Python logging callback cannot be the sole diagnostic path for a stalled interpreter.

  

Bound high-cardinality labels and diagnostic history. Payload capture is disabled by default, with explicit redaction and sampling when enabled. Actors may process credentials, user messages, or sensitive documents; observability must not silently turn them into logs.

  

# 18. Correctness and acceptance tests

  

Tests should establish semantics before benchmarks optimize them. Use a Rust test harness that can run the World without an interpreter, plus Python integration tests for the adapter and public surface. A `TestWorld` should expose a virtual clock, controlled native completions, and stable admission ordering where configured.

  

Deterministic test scheduling is not a claim that arbitrary real network or multithreaded production execution is replay-deterministic.

  

| Test | Required evidence |

| --- | --- |

| Rust-only execution | Core/native crates build and execute a complete event pipeline without Python installed or linked. |

| Deliberately held GIL | Native source, accumulator, timer, and sink progress during a measured interval in which another thread actually holds the GIL. |

| Python dispatcher stalled | Bounded Python mailboxes fill/coalesce/reject as configured while unrelated native consumers continue. |

| Codec snapshot | Mutation of source Python containers after admission does not change delivered native data. |

| Stop/delivery race | Deliveries not claimed before the cancellation fence do not start; already claimed invocations are treated as in flight. |

| Generation reuse | Late results and stale references cannot reach a replacement actor occupying the same storage slot. |

| Saturated shutdown | A full business mailbox does not prevent native stop fencing and resource cleanup. |

| Fan-out isolation | One full Python destination does not block native destinations; the report identifies partial admission. |

| Resource budgets | Mailboxes, tasks, operations, coalescing keys, diagnostics, and retained adapter state respect configured bounds. |

| Non-reentrancy | Self-send and nested publication do not recursively invoke handlers. |

| Failure handling | A handler error produces the configured supervision action without dropping cleanup or silently continuing. |

| Startup rollback | Failed initialization removes staged routes/resources and completes readiness with the original error. |

| Request races | Completion versus timeout/cancel establishes one terminal result; late outcomes cannot produce a second result. |

| Repeated teardown | Repeated create/run/stop cycles reclaim runtime-owned resources without steadily growing registries or native buffers. |

  

## 18.1 The defining GIL test

  

Do not use only a Python busy loop as proof. Ordinary Python execution may yield the GIL, which makes a weak test of interpreter independence.

  

Provide a **test-only** bridge function that deliberately retains the GIL for a finite interval while waiting on a native test barrier. Before entering that interval, arm native sources, timers, a native sink, and a native observer. Record native progress timestamps inside the held-GIL interval, not merely aggregate counts after it ends.

  

Assert zero interpreter attachments attributable to that native pipeline and actual progress before the GIL is released. Separately verify that Python callbacks do not run during the hold and resume afterward according to queue policy. Use a finite watchdog to prevent a broken test from hanging CI.

  

The native observer must also be able to request actor cancellation during the held-GIL interval and verify native resource quiescence. A Python-thread stop request would not test this, because that request itself might be unable to execute.

  

Model-check or systematically exercise the small race-sensitive state machines: delivery claim versus stop, subscription cancellation versus dispatch, operation completion versus timeout, and admission versus actor generation retirement. Fuzz schema decoding, invalid handles, oversized inputs, and malformed native frames.

  

# 19. Performance methodology

  

Do not publish a throughput target or speedup claim before measurements exist. Establish reproducible workloads, record the environment, and retain baseline results with the source revision.

  

Benchmark distinct paths: native-to-native, Python-to-native, native-to-Python, and Python-to-Python through the runtime. Also measure useful native pipelines, not only empty queue handoffs. Vary payload size, fan-out, actor count, event mix, native work per event, and Python subscriber speed.

  

A native producer benchmark and a Python producer benchmark answer different questions. State whether Python allocation/encoding is included. Report generated, admitted, and completed throughput separately, along with rejection/loss rate; dropping most inputs is not a performance victory.

  

Measure latency distributions, especially tail latency under load, as well as CPU use, native retained bytes, transient allocation, queue depth, interpreter boundary crossings, timer lag, and stop latency. Compare the same behavior, delivery policy, and payload work. An in-thread Python function call is a useful lower-bound cost reference, not an equivalent asynchronous actor runtime.

  

The optimization model is:

  

```text

Total work cost ~= ingress/encoding

                 + native processing work

                 + Python-boundary/materialization work

                 + Python behavior work

```

  

Reduce the amount and frequency of Python-boundary work before micro-optimizing each boundary. Native-to-native paths have no per-event Python materialization term. Python-heavy paths may remain dominated by Python and conversion costs.

  

Native parallel scaling should be evaluated with independent state owners. A single stateful accumulator is intentionally serialized and is not a demonstration of multi-core scaling. Run scaling tests with several independent native components and avoid oversubscribed pools.

  

Use pinned benchmark configurations, repeated trials, and hardware-specific regression tolerances. General-purpose CI should enforce semantic bounds and detect gross regressions; do not fail every build on an arbitrary microsecond threshold measured on a noisy shared runner.

  

# 20. Repository and engineering organization

  

Keep the dependency graph legible and the first implementation small enough to audit.

  

```text

crates/

  runtime-core/       # IDs, schemas, owners, delivery, lifecycle

  runtime-tokio/      # Execution, timers, tracked native operations

  runtime-native/     # Reference and optional native components

  runtime-python/     # PyO3 bindings, codecs, driver, Python registry

  runtime-test/       # Virtual time and deterministic test utilities

python/

  actorplane/     # Authoring API, protocols, errors, type stubs

examples/

  native_counter/

  native_io_pipeline/

  connection_lifecycle/

  python_to_native_component/

benchmarks/

docs/

  architecture/

  contracts/

  tutorials/

```

  

Names are placeholders. Merge crates when it simplifies the initial implementation without weakening the Python dependency firewall. Do not create dozens of empty crates in advance of working functionality.

  

Pin a toolchain, minimum supported Rust version, dependencies, and a tested Python/wheel matrix when implementation starts. Build wheels for the supported Windows, Linux, and macOS targets, and verify installation in clean environments. Keep unsupported platforms explicit rather than inferring support from a successful local build.

  

Core API changes need an architecture decision record when they affect execution context, ownership, delivery meaning, or cancellation. A Python convenience-layer change must not quietly alter native semantics.

  

Document the component/schema compatibility policy before releasing third-party extension support. Keep public type stubs, runtime validation, examples, and reference documentation synchronized.

  

# 21. Implementation sequence and gates

  

## Gate 1 — A real Rust World

  

Implement native identities/generations, owner scopes, lifecycle state, bounded mailbox admission, queued delivery, a native timer source, and a native stateful accumulator/sink. The result must run end to end with no Python dependency.

  

**Exit evidence:** Native integration tests, bounded-overload tests, cleanup tests, and a measured reference pipeline. Not merely traits, empty methods, or logging placeholders.

  

## Gate 2 — One explicit Python boundary

  

Add PyO3 registration, one supported typed event codec, a foreground driver, one Python actor type, bounded native-to-Python delivery, and Python-to-native send. Add thread-affinity and reentrancy checks.

  

**Exit evidence:** The deliberately held-GIL test passes. Native progress and native stop fencing occur inside the held-GIL interval. Python delivery resumes correctly afterward.

  

## Gate 3 — Ownership under failure and overload

  

Complete startup rollback, stop modes, delivery claims, subscription retirement, generation fencing, operation completion races, bounded control state, and accurate shutdown reports. Add failure policy and minimal supervision.

  

**Exit evidence:** Saturation and adversarial lifecycle tests pass, including repeatedly stopped/replaced actors and delayed native completions. No unbounded pending-sender workaround exists.

  

## Gate 4 — The useful authoring library

  

Implement per-instance component descriptions, interfaces, typed ports, native links, scoped timers/operations, clear errors, type stubs, native inspection, and `TestWorld`. Keep the API small and consistent.

  

**Exit evidence:** The proposed monitor example is executable against the real package, and its native-only variant has no per-input Python interactions. Example code becomes a CI test rather than aspirational documentation.

  

## Gate 5 — Real application proof

  

Add one bounded native I/O integration and a connection/workflow example with active requests, timers, and cancellation. Add a Python component and a native replacement sharing the same contract tests.

  

**Exit evidence:** An application can stop an owner and explain the fate of its native resources, pending requests, listeners, queued events, and unfinished work. The native pipeline still behaves correctly when Python is slow.

  

## Gate 6 — Release readiness

  

Complete supported-platform wheel testing, stress testing, benchmark methodology/results, shutdown/finalization tests, tutorials, compatibility documentation, and diagnostics. Add host-loop integration only after its own concurrency contract is tested.

  

**Exit evidence:** No advertised capability is a stub; the documented guarantees have tests; limitations and unsupported configurations are visible. Choose and verify the package/project name and license separately rather than inheriting them from another project.

  

# 22. Coding-agent implementation contract

  

Implement one vertical slice at a time. Every slice must have a working entry point, observable behavior, and automated tests. Do not substitute a directory tree, trait collection, or extensive design commentary for implementation.

  

Before adding any feature, state its execution domain, state owner, payload representation, queue/memory limits, admission outcome, stop behavior, and testable completion condition. A feature missing any of these answers is not ready to merge.

  

Prefer straightforward, safe Rust and clear Python over speculative lock-free structures or elaborate macro systems. Unsafe code needs a local safety argument and targeted tests. Performance work must be motivated by a measured bottleneck and must preserve the contracts in this document.

  

Never put Python objects or callbacks in native payloads to make an early example easier. Never create a Python callback for native filtering, routing, timer payload construction, or native diagnostic export without labeling it a Python boundary and excluding it from the native-only path.

  

Never make actor cleanup depend only on `__del__`, task cancellation depend only on dropping a handle, or event delivery depend on an unbounded queue. Never claim a timeout killed executing code. Never claim an accepted event was processed unless execution completion was actually recorded.

  

A pull request must include positive tests, negative tests, ownership/cleanup tests, and relevant overload/race tests. Changes to the bridge require the held-GIL regression test. Changes to queueing require accounting tests, not just a send/receive example.

  

# 23. Operational constraints and deferred decisions

  

A Python callback can block its World’s Python driver indefinitely. Native work can continue where independent, but the runtime cannot guarantee that `world.run()` or an arbitrary Python caller returns by a shutdown deadline while stuck inside that callback. Native deadline assessment and Python control-flow recovery are different guarantees.

  

An in-process component is trusted code, not a sandbox. Native components can crash the process or violate their declared budgets; arbitrary Python can leak references or mutate global state. Runtime APIs should make correct ownership natural and reject misuse they can detect, without advertising isolation they cannot enforce.

  

Explicitly close Worlds before interpreter finalization. Do not require native worker threads to attach to an interpreter that is shutting down. Late native work must not call Python, and Python reference release must happen while its interpreter remains valid. Treat interpreter teardown/reinitialization, subinterpreters, and forking an active multithreaded World as unsupported until implemented and tested. Add process/interpreter identity checks where practical.

  

Defer multiple Python dispatch lanes, free-threaded parallel actor execution, coroutine handlers, remote actors, durable delivery, hot reload, dynamic component replacement, binary native plugins, and generalized expression compilation. None is required to prove the central value proposition.

  

When adding one of these later, preserve the native core and its ownership contracts. A different Python executor should be an adapter change, not a reason to rebuild event routing around Python objects.

  

# 24. Definition of success

  

The project is ready to call itself a Rust event-driven runtime for Python when a developer can construct a useful native pipeline from Python, run its routing and processing without Python participation per event, receive only the Python notifications they need, and reason precisely about ownership and failure.

  

The decisive demonstration is not a million empty callbacks. It is a realistic owned pipeline that continues native processing while Python is unavailable, applies documented bounded overload policies, cleans up native resources on cancellation, and resumes Python-facing behavior without stale deliveries or hidden leaks.

  

The library around that runtime should make the safe path the easy path: typed events, simple actor composition, native capabilities that are visible as native, explicit lifecycle completion, actionable diagnostics, and a tested path for moving behavior from Python to Rust.

  

> **Python defines the application. Rust owns and processes its event-driven machinery. Crossing into Python is a deliberate execution boundary, not the default implementation of every event.**

  

# Source notes

  

The architecture above is a proposal. Sources below support the external platform constraints and conceptual references, not a claim that this library already implements the specification. Official documentation was checked on September 20, 2026. Pin implementation dependencies and re-check version-specific APIs before coding.

  

**[S1] PyO3 — Parallelism.** Rust-only work and interpreter detachment; Python interaction remains an explicit boundary. https://pyo3.rs/v0.29.2/parallelism

  

**[S2] Epic Games — Components in Unreal Engine.** Conceptual reference for reusable actor-owned capabilities. https://dev.epicgames.com/documentation/en-us/unreal-engine/components-in-unreal-engine

  

**[S3] Epic Games — Event Dispatchers in Unreal Engine.** Conceptual reference for named event outputs and subscriber bindings; this proposal deliberately specifies queued delivery. https://dev.epicgames.com/documentation/en-us/unreal-engine/event-dispatchers-in-unreal-engine

  

**[S4] PyO3 — Supporting Free-Threaded Python.** Interpreter attachment, thread-safety requirements, and module declarations for free-threaded builds. https://pyo3.rs/main/free-threading

  

**[S5] Tokio — spawn_blocking.** Blocking/CPU execution limits, executor starvation concerns, and non-abortability of started blocking tasks. https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html

  

**[S6] PyO3 — Python marker API.** `Python::attach`, `Python::detach`, and mutex/interpreter deadlock guidance. https://pyo3.rs/main/doc/pyo3/marker/struct.python

  

**[S7] Tokio — Graceful Shutdown.** Distinguishing shutdown detection, cancellation notification, and waiting for tracked tasks. https://tokio.rs/tokio/topics/shutdown

  

**[S8] Tokio — Channels.** Bounded queues, backpressure, and implicit-concurrency considerations. https://tokio.rs/tokio/tutorial/channels

  

**[S9] CPython — Developing with asyncio.** Thread affinity and explicit cross-thread scheduling facilities. https://docs.python.org/3/library/asyncio-dev.html