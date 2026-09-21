//! Local cost of target work versus unrelated registered activity observers.
use actorplane_core::{Config, EndpointKind, Payload, World};
use std::time::Instant;

fn main() {
    println!("unrelated_observers,send_claim_finish_ns,unrelated_versions_changed");
    for count in [0, 128, 1024, 8192] {
        let world = World::new(Config {
            max_actors: count + 1,
            ..Default::default()
        })
        .unwrap();
        let mut actors = Vec::new();
        for _ in 0..=count {
            let actor = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(actor).unwrap();
            actors.push(actor);
        }
        let target = actors.pop().unwrap();
        let observed: Vec<_> = actors
            .iter()
            .map(|actor| world.activity(*actor).unwrap())
            .collect();
        world.activity(target).unwrap();
        let start = Instant::now();
        for value in 0..2000 {
            world.send(target, Payload::Pulse(value)).unwrap();
            world.claim(target).unwrap().unwrap().finish(true);
        }
        let elapsed = start.elapsed().as_nanos() / 2000;
        let changed = actors
            .iter()
            .zip(observed)
            .filter(|(actor, version)| world.activity(**actor).unwrap() != *version)
            .count();
        println!("{count},{elapsed},{changed}");
        assert_eq!(changed, 0, "unrelated observers must stay unchanged");
        assert_eq!(world.snapshot().metrics.completed, 2000);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        assert!(world.close().native_done);
    }
}
