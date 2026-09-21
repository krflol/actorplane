# Native schema plans and canonical payloads

The schema authoring layer compiles an `@event` declaration into a frozen
`SchemaPlan`. The plan owns its typed node tree and exact nested event and enum
classes. Compilation does not use a process-global class registry, so two
schemas with the same public name and version cannot change how an earlier
plan decodes. A schema identity is scoped to its World registration and its
numeric root ID is an implementation detail; it is not a persistent wire ID.

```python
from typing import Annotated
from actorplane.schemas import Event, IntRange, Length, event

@event("orders.Price", version=1)
class Price(Event):
    cents: Annotated[int, IntRange(0, (1 << 64) - 1)]
    labels: Annotated[tuple[int, ...], Length(64)]
    note: Annotated[str, Length(256)]
```

Supported nodes are signed or unsigned 64-bit integers, finite IEEE-754
binary64 floats by default, booleans, UTF-8 strings, bytes, sorted-name enums,
optionals, bounded immutable sequences, and nested event records. Integer
ranges may be narrowed with `IntRange`; `bool` is never accepted as an integer.
`FloatPolicy(allow_non_finite=True)` permits non-finite values and encodes every
NaN with canonical bits `0x7ff8000000000000`; signed zero is preserved.
Native `Value` float equality uses IEEE semantics: NaN is unequal to itself,
while signed zeros compare equal. No native keyed float aggregation/filter API
is exposed yet.
`Length` bounds strings, bytes, and sequences. Lists supplied for sequence
fields are copied into the immutable native representation and decode as
tuples. Bytes-like values are copied as bytes.

The body encodings are fixed as follows. Integers and floats occupy eight
little-endian bytes; signed integers use two's complement. Booleans use one
byte, exactly 0 or 1. Strings and bytes use a little-endian `u32` byte length
followed by their data. Strings are UTF-8 without normalization; embedded NUL
is allowed. Enums use a `u32` index into lexicographically sorted member names.
Optionals use tag 0 for absent or tag 1 followed by the value. Sequences use a
`u32` item count followed by their values. Record fields are concatenated in
declaration order, without an inner header. The runtime schema ID lives outside
this body. Rust `Schema::encode`/`decode` support the same format using `Value`.

Compilation enforces a maximum schema depth of 16, 1,024 schema nodes, and 256
record fields. The default maximum is 4,096 values per sequence and 4,096 bytes
per string or bytes field, with a 1 MiB field ceiling. Encoded values are
bounded by the plan's `max_bytes` argument and then by the World's event-byte
cap. Length prefixes and container metadata count toward those limits. Bounds
are checked before growing the output buffer. Buffer sizes are checked before
copying; strings are first bounded by character count, so temporary UTF-8
encoding is at most four times the remaining field budget. One event may contain
at most 4,096 value nodes, counting records and containers. Native validation
walks the bytes without allocating a decoded value tree. Python decoding checks
each field before constructing its value and rejects trailing data.

Records, enums, and nested values require their exact compiled types. Unsupported
general unions, maps, arbitrary objects, recursive/cyclic records, and implicit
schema migration are rejected. Event names and field names have bounded UTF-8
lengths. Versioned layouts are strict: changing a field layout requires a new
version and an explicit application migration; the runtime does not silently
translate between versions.

The native bridge receives a schema ID, validates the compact payload against
the registered plan, and owns the copied bytes for queueing, routing, timers,
and native processing. Native routes do not materialize Python annotations or
inspect Python objects per message. Native [event envelopes](0007-event-envelopes.md)
carry source, destination, deadline, correlation, causation, and bounded trace
metadata separately from the payload. Typed component ports now
use these schemas; see [components and ports](0006-components-ports.md). This
document describes the typed payload slice and does not claim the full Gate 4 API.

Schemas live for the lifetime of their World, including after actor startup
failure or retirement. `max_schemas` bounds these declarations; IDs are never
reused. Handler schema batches are atomic when registration fails, and an actor
allocation failure occurs before new schema registration. Existing admitted
callbacks and drain work may register completion schemas under the same bound;
new actor/message/task admission remains fenced. Constructing validated bytes
from a known schema does not itself admit work.
