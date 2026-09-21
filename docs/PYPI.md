# actorplane 0.1.0

`actorplane` is a bounded Rust event runtime with a foreground Python actor
driver. Native sources, routing, timers, component pipelines, and payload
accounting can continue while Python is busy. Python handlers receive bounded
queued events and explicit lifecycle callbacks.

This is a pre-alpha development release. The documented runtime contract is
smaller than the long-term design: callbacks are cooperative, unsupported
platforms have not received release validation, and this release does not
promise production throughput, high availability, or a stable extension ABI.

Licensed under MIT. The license text is included in the distribution.

## Install

The published wheel targets CPython 3.11 on Windows x64:

```sh
python -m pip install actorplane==0.1.0
```

The package has no separate runtime Python dependency. A source build requires
Rust 1.97.0 or newer, CPython 3.11 development support, and the platform's
native Rust compiler and linker. Release validation for this version is limited
to Windows x64 with CPython 3.11; other platforms require their own validation.

## Minimal actor

```python
from actorplane import Actor, CountSnapshot, World, handles


class Monitor(Actor):
    def on_start(self, ctx):
        self.pipeline = ctx.native_counter(0.01, 0.1, target=ctx.actor)

    @handles(CountSnapshot)
    def summary(self, event, ctx):
        print(event.count, event.total)


with World(mailbox_capacity=64) as world:
    world.spawn(Monitor)
    world.run_for(0.5)
```

`spawn` returns a pending actor reference. Construction and lifecycle callbacks
run on the first `step` or `run` driver thread. `send` reports mailbox
admission, not handler completion. Stop and close operations fence new work and
report unfinished native work where cooperative completion is not possible.

The runtime includes bounded schemas, typed component ports, native envelopes,
request/reply operations, supervision records, virtual-time testing helpers,
framed TCP support, and bounded CPU jobs. These facilities remain subject to
the pre-alpha limits and compatibility scope of this release.
