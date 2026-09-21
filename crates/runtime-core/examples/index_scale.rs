//! Bounded local comparison: unrelated routes and pending deadlines.
use actorplane_core::{
    ActorRef, Config, DeliveryReport, EndpointKind, OperationTable, Payload, PublicationTicket,
    World,
};
use std::time::{Duration, Instant};

fn active(world: &World) -> ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

fn route(world: &World, ticket: PublicationTicket) -> DeliveryReport {
    while world.routing_ready() {
        world.route_batch();
    }
    ticket.report().expect("publication routed")
}

fn main() {
    println!(
        "unrelated_routes,publish_claim_ns_per_call,stop_median_ns,stop_p95_ns,expiry_idle_ns_per_call"
    );
    for size in [0, 128, 1024, 8192] {
        let world = World::new(Config {
            max_actors: size + 8,
            max_subscriptions: size + 8,
            ..Default::default()
        })
        .unwrap();
        let background = active(&world);
        for _ in 0..size {
            let target = active(&world);
            world.subscribe(background, target).unwrap();
        }
        let source = active(&world);
        let target = active(&world);
        world.subscribe(source, target).unwrap();
        let start = Instant::now();
        for _ in 0..2000 {
            assert_eq!(
                route(&world, world.publish(source, Payload::Pulse(1)).unwrap()).admitted,
                1
            );
            world.claim(target).unwrap().unwrap().finish(true);
        }
        let publish_ns = start.elapsed().as_nanos() / 2000;
        let mut stops = Vec::with_capacity(101);
        for _ in 0..101 {
            let owner = active(&world);
            world.subscribe(owner, target).unwrap();
            let start = Instant::now();
            assert!(world.stop(owner).unwrap().native_done);
            stops.push(start.elapsed().as_nanos());
        }
        stops.sort_unstable();
        let mut operations = OperationTable::new(world.id(), size.max(1)).unwrap();
        let now = Instant::now();
        for _ in 0..size {
            operations
                .reserve(source, target, now + Duration::from_secs(3600))
                .unwrap();
        }
        let start = Instant::now();
        for _ in 0..2000 {
            assert!(operations.expire_ids(now).is_empty());
        }
        let expiry_ns = start.elapsed().as_nanos() / 2000;
        println!(
            "{size},{publish_ns},{},{},{expiry_ns}",
            stops[50], stops[95]
        );
        world.close();
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}
