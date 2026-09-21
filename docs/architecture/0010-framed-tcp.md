# Framed TCP transport

The optional `actorplane-native` `native-io` feature provides a bounded TCP
request/reply listener. `actorplane-python` is built with this feature and
exposes the same listener and statistics through its native extension. The
transport is implemented in
[`crates/runtime-native/src/io`](../../crates/runtime-native/src/io/), with
Python bindings in [`crates/runtime-python/src/tcp.rs`](../../crates/runtime-python/src/tcp.rs).

## Wire and schema contract

Each wire frame is a four-byte unsigned big-endian length followed by exactly
that many bytes. A zero length is valid. The configured maximum is between 1
and 65,536 bytes; the default is 16,384. A header over the configured maximum
is rejected before body allocation. Clean EOF before a header returns no more
frames. EOF after a partial header or after a nonzero header without its full
body is `Truncated`.

The shared schema is `actorplane.TcpFrame`, version 1, with one `Bytes` field
(`data`, maximum 65,536 bytes). `frame_payload` stores the native structured
payload as schema-indexed bytes; its internal length prefix is little-endian,
while the socket framing prefix remains big-endian. Replies are accepted only
when they carry the expected schema and exact internal length.

## Request lifecycle

One connection processes one request at a time: read one frame, admit one core
request, await its terminal outcome, then write one reply. There is no hidden
send queue and no interpreter callback in the socket loop. The target actor
may be native or Python; the core operation table owns completion, timeout,
cancellation, and reply retention. An unsuccessful terminal operation outcome
closes the connection without inventing a reply. Request waiting uses the core
operation's original absolute deadline and terminal winner.

The listener has a native owner and creates one bounded native child task per
accepted connection. `max_connections` defaults to 16 and is limited to 1,024.
Runtime task permits and connection semaphore permits are acquired before
connection setup. Listener setup allocates and tracks its owner, registers the
frame schema, binds the socket, creates budgets, and activates the owner. Any
failure stops the owner and releases the acquired resources. Registered schema
identities persist for the World's lifetime, including after failed startup.
The default bind address is `127.0.0.1:0`; the returned address contains the
assigned port. Python accepts numeric IP addresses, with no DNS lookup.

The listener owner can be stopped or drained. Stopping fences ingress and
closes connection work. Draining stops new accepts and idle/partial reads but
lets already admitted requests finish and write replies under the shared shutdown
deadline. No next request is read during drain. A partial
write or read timeout closes that connection; a partial wire frame is never
reused as a future frame.

Writes acquire a native control claim before polling socket output. Cancellation
may interrupt an already claimed write after some bytes reached the peer; the
connection closes and no retry is made. `frames_written` means the complete frame
was handed to the socket writer, not that the peer processed it. Peer disconnects
are detected at subsequent I/O or the request deadline; the transport does not
monitor reads concurrently while awaiting a reply.

Closing a listener does not stop an independently owned target actor. An already
claimed target callback or job may continue, with late replies fenced by the
retired request. Its work remains accounted under that target. Listener
`native_done` therefore does not imply every target callback has ended; use the
World shutdown report for that wider scope.

## Budgets and limits

`read_buffer_bytes` and `write_buffer_bytes` default to 1 MiB and each may be
configured from 1 byte through 64 MiB. The read budget is connected to the
world's `NativeBufferBudget`, so frame storage and core retained payloads
share `Config.native_payload_budget`. The write budget is a listener-local
bounded budget. Reservations are RAII permits and are released on frame drop,
I/O failure, cancellation, or task retirement. Capacity growth is charged
before it is used; a failed growth leaves the existing permit unchanged.

For an input of `n` bytes, the read side reserves its buffer capacity plus
`2 * (n + 4)` bytes for canonical encoding and payload construction before those
allocations. Core admission also charges its stored payload (`n + 8`), so these
reservations overlap conservatively until admission returns. A sufficiently
large per-frame limit alone does not guarantee admission under the aggregate
budget. The held reply is already charged to the World; the write budget adds a
local reservation of its data length without allocating another output buffer.
Counters include reservations and scratch, not only queued application data.

`World::native_buffer_budget()` clones only the shared atomic counter, not a
World or actor lease. Its permits are accounting tools; callers must keep them
with the allocations and enforce lifecycle through native task ownership. These
budgets do not bound kernel socket buffers, arbitrary application allocations,
or process RSS.

The listener rejects configurations with zero or over-24-hour read, request,
or write timeouts; defaults are 5, 1, and 5 seconds respectively. The read timeout
covers a whole frame, including idle time and body assembly. `max_frame_bytes + 8` must fit the world's maximum event
size. The listener also accounts accepted/rejected connections, frame and byte
counts, admission and budget rejections, timeout classes, malformed frames,
invalid replies, and read/write failures through `TcpStats`.
Counts exclude frame headers from byte totals. Statistics and connection/buffer
snapshots are bounded but sampled separately, so they are not one atomic view.
Unexpected listener accept failure records a bounded native failure and stops
that listener; per-connection errors close that connection and increment counters.

## Python and virtual-runtime boundary

The Python extension exposes listener owner, address, active connections,
statistics, and current read/write buffer usage. Its test-support
`_held_gil_tcp_probe` runs a native socket client on a Rust thread while the
calling Python thread remains attached, then stops the listener subtree and
reports progress timestamps and native shutdown status. The real socket
listener rejects a virtual `World`; `TestWorld` therefore does not simulate
TCP I/O. Rust applications opt into `native-io` explicitly, while the Python
extension includes it.

The Python `listen` and `native_echo` helpers both reject `TestWorld`. TCP here
provides a sequential framed transport, with no TLS, reconnect policy, request
multiplexing, durable delivery, or exactly-once external effects.

Native deferred replies follow the same operation authority: if cancellation
or retirement wins before deferred-job preparation, the factory is not run and
the operation remains terminal. This prevents a late asynchronous reply from
reopening or mutating a closed TCP request.

## Evidence

- [`crates/runtime-native/src/io/framing.rs`](../../crates/runtime-native/src/io/framing.rs)
  tests fragmentation, coalescing, truncation, oversize rejection, partial
  writes, cancellation cleanup, and budget release.
- [`crates/runtime-native/tests/tcp_transport.rs`](../../crates/runtime-native/tests/tcp_transport.rs)
  tests exact binary/empty-frame echo, malformed and truncated frames,
  connection caps, timeouts, drain/stop behavior, and shared read-budget
  pressure.
- [`crates/runtime-native/examples/tcp_loopback.rs`](../../crates/runtime-native/examples/tcp_loopback.rs)
  runs three native loopback frames and asserts drained tasks and zero buffer
  usage.
- [`crates/runtime-core/src/buffers.rs`](../../crates/runtime-core/src/buffers.rs)
  and [`crates/runtime-core/tests/buffers.rs`](../../crates/runtime-core/tests/buffers.rs)
  prove shared retained-byte accounting, concurrent reservations, and atomic
  growth rollback.
- [`crates/runtime-native/src/io/transport.rs`](../../crates/runtime-native/src/io/transport.rs)
  checks write deadline cleanup against a deterministic one-byte-capacity writer.
- [`tests/test_tcp_contract.py`](../../tests/test_tcp_contract.py) checks shared
  Python/native request behavior, stalled Python queue overload, and scope cleanup.
- [`tests/test_tcp_boundary.py`](../../tests/test_tcp_boundary.py) checks eight
  exchanges and listener shutdown during a measured GIL hold with zero bridge entries.
- [`examples/connection_workflow.py`](../../examples/connection_workflow.py)
  combines native echo, Python policy, a scoped timer, and pending-request cancellation.
