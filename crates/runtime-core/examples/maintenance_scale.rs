//! Local maintenance cost versus unrelated actor/queue occupancy.
use actorplane_core::{Clock, Config, EndpointKind, MessageOptions, Payload, World};
use std::time::{Duration, Instant};

fn measure(world: &World, now: Instant) -> u128 {
    let start = Instant::now();
    for _ in 0..2000 {
        world.maintain(now);
    }
    start.elapsed().as_nanos() / 2000
}

fn main() {
    println!("actors,empty_maintain_ns,future_queue_maintain_ns,allocate_ns_per_call");
    for count in [0, 128, 1024, 8192] {
        let now = Instant::now();
        let (clock, _) = Clock::manual_at(now);
        let world = World::with_clock(
            Config {
                max_actors: count + 1,
                ..Default::default()
            },
            clock,
        )
        .unwrap();
        let mut actors = Vec::with_capacity(count);
        for _ in 0..count {
            let actor = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(actor).unwrap();
            actors.push(actor);
        }
        let empty_ns = measure(&world, now);
        for actor in &actors {
            world
                .send_with(
                    *actor,
                    Payload::Pulse(1),
                    MessageOptions {
                        deadline: Some(now + Duration::from_secs(3600)),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        let future_ns = measure(&world, now);
        assert_eq!(world.snapshot().metrics.expired, 0);
        let mut allocation_ns = 0;
        for _ in 0..1000 {
            let start = Instant::now();
            let actor = world.allocate(EndpointKind::Native, None).unwrap();
            allocation_ns += start.elapsed().as_nanos();
            world.stop(actor).unwrap();
        }
        println!("{count},{empty_ns},{future_ns},{}", allocation_ns / 1000);
        world.maintain(now + Duration::from_secs(3600));
        assert_eq!(world.snapshot().metrics.expired, count as u64);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        assert!(world.close().native_done);
    }
}
