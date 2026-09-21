"""Local idle Python dispatch cost versus registered actor occupancy."""
from time import perf_counter_ns

from actorplane import Actor, World


class Idle(Actor):
    pass


def main():
    print("actors,idle_step_ns")
    for count in (0, 128, 1024, 8192):
        with World(max_actors=max(1, count)) as world:
            for _ in range(count):
                world.spawn(Idle)
            world.step()
            start = perf_counter_ns()
            for _ in range(1000):
                assert world.step() == 0
            elapsed = perf_counter_ns() - start
            print(f"{count},{elapsed // 1000}")


if __name__ == "__main__":
    main()
