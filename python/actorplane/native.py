"""Built-in native factories; descriptors create fresh owned runtime instances."""
from .authoring import Pulse, CountSnapshot
from .components import Interface


class PulseSource:
    __actorplane_native_kind__ = "pulse_source"
    __actorplane_ports__ = (("pulses", "output", Pulse),)
    implements = (Interface("actorplane.PulseSource", outputs=(("pulses", Pulse),)),)


class WindowCounter:
    __actorplane_native_kind__ = "window_counter"
    __actorplane_ports__ = (("input", "input", Pulse), ("snapshots", "output", CountSnapshot))
    implements = (Interface("actorplane.WindowCounter", inputs=(("input", Pulse),), outputs=(("snapshots", CountSnapshot),)),)


class SnapshotSink:
    __actorplane_native_kind__ = "snapshot_sink"
    __actorplane_ports__ = (("input", "input", CountSnapshot),)
    implements = (Interface("actorplane.SnapshotSink", inputs=(("input", CountSnapshot),)),)


__all__ = ["Pulse", "CountSnapshot", "PulseSource", "WindowCounter", "SnapshotSink"]
