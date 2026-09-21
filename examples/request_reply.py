"""Finite request/reply example with bounded operation storage."""
from actorplane import Actor, World, event, handles

@event("example.request", 1)
class Request:
    value: int
    tags: tuple[int, ...]

@event("example.response", 1)
class Response:
    total: int

class Service(Actor):
    @handles(Request)
    def request(self, event, ctx):
        ctx.reply(Response(event.value + sum(event.tags)))

if __name__ == "__main__":
    with World(max_operations=8) as world:
        owner = world.spawn(Actor)
        service = world.spawn(Service)
        world.step()
        operation = world.request(owner, service, Request(7, (2, 3)), timeout=.5)
        world.run_for(.05)
        print(operation.result())
        print(world.inspect()["metrics"])
