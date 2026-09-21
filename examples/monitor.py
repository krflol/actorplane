"""Rust produces and aggregates pulses; Python receives timed summaries."""
from actorplane import Actor, CountSnapshot, Event, event, handles, World

@event("monitor.Stop", 1)
class Stop(Event):
    pass

class Monitor(Actor):
    def on_start(self, ctx):
        self.counter = ctx.native_counter(interval=0.005, window=0.05, target=ctx.actor)
        ctx.after(0.35, Stop(), target=ctx.actor)

    @handles(CountSnapshot)
    def report(self, snapshot, ctx):
        print(f"processed={snapshot.count} total={snapshot.total}")

    @handles(Stop)
    def finish(self, event, ctx):
        print("Stopping the owned native pipeline")
        ctx.world.request_stop()

if __name__ == "__main__":
    with World() as world:
        monitor = world.spawn(Monitor)
        world.run()
        print(world.inspect()["metrics"])
        print(world.close())
