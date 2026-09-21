"""Native source/counter/sink composition with a Python summary callback."""
from actorplane import Actor, CountSnapshot, World, component, handles
from actorplane import native

proxies = {}

class Monitor(Actor):
    source = component(native.PulseSource, interval=.005)
    counter = component(native.WindowCounter, window=.02, depends_on=("source",))
    sink = component(native.SnapshotSink, depends_on=("counter",))

    def configure(self, ctx):
        proxies.update(source=self.source, counter=self.counter, sink=self.sink)
        ctx.link(self.source.pulses, self.counter.input)
        ctx.link(self.counter.snapshots, self.sink.input)
        ctx.subscribe(self.counter.snapshots, ctx.actor)

    @handles(CountSnapshot)
    def summary(self, event, ctx):
        print(f"summary count={event.count} total={event.total}")

if __name__ == "__main__":
    with World() as world:
        world.spawn(Monitor)
        world.run_for(.2)
        report = world.close(mode="drain", timeout=.5)
        assert report["native_done"] and report["python_done"] and not report["timed_out"]
        print("native component stats:", {name: proxy.stats() for name, proxy in proxies.items()})
        assert world.inspect()["retained_payload_bytes"] == 0
