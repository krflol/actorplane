//! Distinguishes ingress acceptance from bounded routing and final admissions.
use actorplane_core::{Config, EndpointKind, Payload, PublicationOutcome, World};
use std::time::Instant;

fn main() {
    println!("subscribers,ingress_ns,first_batch_ns,max_batch_ns,routing_ns,batches");
    for subscribers in [0, 32, 128, 1024, 4096] {
        let world = World::new(Config {
            max_actors: subscribers + 1,
            max_subscriptions: subscribers,
            routing_batch_size: 32,
            ..Default::default()
        })
        .unwrap();
        let source = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(source).unwrap();
        let mut targets = Vec::new();
        for _ in 0..subscribers {
            let target = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(target).unwrap();
            world.subscribe(source, target).unwrap();
            targets.push(target);
        }
        let started = Instant::now();
        let ticket = world.publish(source, Payload::Pulse(1)).unwrap();
        let ingress = started.elapsed().as_nanos();
        assert_eq!(world.snapshot().metrics.admitted, 0);
        let started = Instant::now();
        let mut first = 0;
        let mut maximum = 0;
        let mut batches = 0;
        while world.routing_ready() {
            let batch_started = Instant::now();
            let progress = world.route_batch();
            let elapsed = batch_started.elapsed().as_nanos();
            assert!(progress.destinations <= 32);
            if batches == 0 {
                first = elapsed;
            }
            maximum = maximum.max(elapsed);
            batches += 1;
        }
        let routing = started.elapsed().as_nanos();
        let report = ticket.report().unwrap();
        assert_eq!(report.outcome, PublicationOutcome::Routed);
        assert_eq!(report.admitted, subscribers);
        assert_eq!(report.rejected, 0);
        assert_eq!(batches, subscribers.div_ceil(32).max(1));
        println!("{subscribers},{ingress},{first},{maximum},{routing},{batches}");
        for target in targets {
            world.claim(target).unwrap().unwrap().finish(true);
        }
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        assert_eq!(world.snapshot().routing_snapshot_entries, 0);
        assert!(world.close().native_done);
    }
}
