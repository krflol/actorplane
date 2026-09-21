"""A bounded child failure is delivered to its Python parent supervisor."""

from actorplane import Actor, FailurePolicy, Pulse, World, handles


def main() -> None:
    handles_by_name = {}
    failures = []
    notifications = []

    class Worker(Actor):
        @handles(Pulse)
        def crash(self, event, ctx):
            raise ValueError("deliberate worker failure")

    class Parent(Actor):
        def on_start(self, ctx):
            handles_by_name["worker"] = ctx.spawn(Worker)

        def on_failure(self, failure, ctx):
            # Failure records intentionally expose type/phase/handler metadata,
            # without exception arguments or private traceback state.
            print(f"failure phase={failure.phase} type={failure.exception_type} handler={failure.handler}")
            failures.append(failure)
            return FailurePolicy.STOP_WORLD

        @handles(Pulse)
        def notification(self, event, ctx):
            notifications.append(event.value)

    with World() as world:
        parent = world.spawn(Parent)
        world.step()
        world.step()
        handles_by_name["worker"].send(Pulse(1))
        parent.send(Pulse(2))
        world.run()
        assert len(failures) == 1
        assert failures[0].phase == "Handler"
        assert failures[0].exception_type.endswith("ValueError")
        assert notifications == [2]
        assert world.diagnostics(limit=10)["entries"]
        report = world.close()
        assert report["native_done"] and report["python_done"]


if __name__ == "__main__":
    main()
