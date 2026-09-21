# Components, ports, and native links

Components are immutable declarations attached to an actor class. A native
factory such as `component(native.PulseSource, interval=.005)` captures only
its marker type and frozen, bounded configuration. Dependencies use component
field names (`depends_on=("source",)`) and required interface identities are
validated before activation. Descriptors are shared immutable descriptions;
component instances and port proxies are created separately for every actor.

Python components derive from `Component`. An input is a synchronous
`@handles(Event)` method and an output is declared with `Output(Event)`. The
driver invokes Python component callbacks on the actor's foreground thread.
Native component ports are exposed through immutable bound proxies. `Port.send`
submits an event to an input; `Port.emit` publishes through an output's native
routes. A `LinkToken` returned by `ctx.link(...)` cancels those links individually. Port
handles carry World, slot, generation, and port index, and retain only weak
World access.

Native markers currently include `PulseSource`, `WindowCounter`, and
`SnapshotSink`, with `Pulse` and `CountSnapshot` as their typed events. Native
interfaces verify port schemas and layout compatibility in Rust. Required
interfaces are matched against named dependencies before startup and checked
against the actual native instance registration. Links are registered while an
owner starts, native tasks wait behind its activation fence, and output events
are staged in bounded native storage. Startup failure cancels staged
outputs and releases their bounded native scopes. Components count against the
World actor/owner budget. Descriptor limits are 64 component ports, 16
interfaces, and 1,024 interface entries per World.

Native routes use FIFO admission within a source and reject on full queues.
Port publication returns a bounded routing ticket; routing batches use the
first-batch subscription snapshot, round-robin ready sources, and revalidate
route generations before each mailbox admission. Routes are owned by the actor
scope, cancel on owner stop, and participate in immediate cancellation and
cooperative drain. A drain runs already admitted Python work on the foreground
driver and observes the same shutdown deadline as the owner; it does not kill
an executing callback. See [bounded publication tickets](0017-publication-tickets.md).

```python
from actorplane import Actor, World, component, native

class Pipeline(Actor):
    source = component(native.PulseSource, interval=.005)
    counter = component(native.WindowCounter, window=.02)
    sink = component(native.SnapshotSink)

    def configure(self, ctx):
        ctx.link(self.source.pulses, self.counter.input)
        ctx.link(self.counter.snapshots, self.sink.input)

with World() as world:
    world.spawn(Pipeline)
    world.run_for(.2)
    report = world.close(mode="drain")
    assert report["native_done"] and not report["timed_out"]
```

See [the executable example](../../examples/components.py) for summary callbacks
and counters, and [the API contract](../contracts/api.md) for startup, schema,
interface, and shutdown limits. Python and native snapshot sinks run the same
interface and drain-completion tests. Native-only Rust tests exercise a composed
pipeline, task-budget rollback, and draining a queue larger than one batch.

Envelope metadata is described in [event envelopes](0007-event-envelopes.md). The
[native SDK](0008-native-sdk.md) now executes the built-in components. Native I/O
services remain for later work; [TestWorld](0009-testworld.md) drives these same
components with virtual time. Source,
destination, deadline, correlation, causation, and trace metadata remain
runtime-owned envelope concerns rather than Python descriptor configuration.
