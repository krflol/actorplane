use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, Lifecycle, Payload, PayloadType, PortDirection,
    PortSpec, World,
};
use std::{
    sync::{Arc, Barrier},
    thread,
};
mod common;
use common::route;

fn actor(world: &World) -> actorplane_core::ActorRef {
    let value = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(value).unwrap();
    value
}

#[test]
fn source_dispatches_amid_many_unrelated_routes() {
    let world = World::new(Config::default()).unwrap();
    let source = actor(&world);
    let selected = actor(&world);
    world.subscribe(source, selected).unwrap();
    let unrelated_source = actor(&world);
    let mut unrelated_targets = Vec::new();
    for _ in 0..128 {
        let target = actor(&world);
        world.subscribe(unrelated_source, target).unwrap();
        unrelated_targets.push(target);
    }
    let report = route(&world, world.publish(source, Payload::Pulse(9)).unwrap());
    assert_eq!(report.admitted, 1);
    assert_eq!(report.rejected, 0);
    let lease = world
        .claim(selected)
        .unwrap()
        .expect("selected route delivery");
    assert_eq!(lease.payload(), &Payload::Pulse(9));
    lease.finish(true);
    for target in unrelated_targets {
        assert!(world.claim(target).unwrap().is_none());
    }
}

#[test]
fn stopping_route_endpoints_removes_only_incident_routes() {
    let world = World::new(Config::default()).unwrap();
    let source = actor(&world);
    let first = actor(&world);
    let second = actor(&world);
    let unrelated_source = actor(&world);
    let unrelated_target = actor(&world);
    world.subscribe(source, first).unwrap();
    world.subscribe(source, second).unwrap();
    world.subscribe(unrelated_source, unrelated_target).unwrap();
    world.stop(first).unwrap();
    let report = route(&world, world.publish(source, Payload::Pulse(1)).unwrap());
    assert_eq!(report.admitted, 1);
    assert!(world.claim(second).unwrap().is_some());
    let unrelated = route(
        &world,
        world.publish(unrelated_source, Payload::Pulse(2)).unwrap(),
    );
    assert_eq!(unrelated.admitted, 1);
    assert!(world.claim(unrelated_target).unwrap().is_some());
}

#[test]
fn unsubscribe_invalidates_queued_delivery_and_reuses_quota() {
    let config = Config {
        max_subscriptions: 1,
        ..Default::default()
    };
    let world = World::new(config).unwrap();
    let source = actor(&world);
    let target = actor(&world);
    let subscription = world.subscribe(source, target).unwrap();
    assert_eq!(
        route(&world, world.publish(source, Payload::Pulse(1)).unwrap()).admitted,
        1
    );
    world.unsubscribe(subscription).unwrap();
    assert!(world.claim(target).unwrap().is_none());
    let replacement = world.subscribe(source, target).unwrap();
    assert_ne!(subscription, replacement);
    assert_eq!(
        route(&world, world.publish(source, Payload::Pulse(2)).unwrap()).admitted,
        1
    );
    assert_eq!(
        world.claim(target).unwrap().unwrap().payload(),
        &Payload::Pulse(2)
    );
}

#[test]
fn publish_and_unsubscribe_race_never_claims_after_fence() {
    for value in 0..64 {
        let world = Arc::new(World::new(Config::default()).unwrap());
        let source = actor(&world);
        let target = actor(&world);
        let subscription = world.subscribe(source, target).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let publish_world = world.clone();
        let publish_barrier = barrier.clone();
        let publisher = thread::spawn(move || {
            publish_barrier.wait();
            publish_world
                .publish(source, Payload::Pulse(value))
                .map(|ticket| route(&publish_world, ticket))
        });
        barrier.wait();
        world.unsubscribe(subscription).unwrap();
        let publish = publisher.join().unwrap().unwrap();
        assert!(publish.admitted <= 1);
        assert!(world.claim(target).unwrap().is_none());
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}

#[test]
fn repeated_route_mutations_reclaim_capacity_and_close_cleanly() {
    let config = Config {
        max_subscriptions: 2,
        ..Default::default()
    };
    let world = World::new(config).unwrap();
    let source = actor(&world);
    let first = actor(&world);
    let second = actor(&world);
    for _ in 0..50 {
        let one = world.subscribe(source, first).unwrap();
        let two = world.subscribe(source, second).unwrap();
        assert_eq!(world.snapshot().subscriptions, 2);
        world.unsubscribe(one).unwrap();
        world.unsubscribe(two).unwrap();
        assert_eq!(world.snapshot().subscriptions, 0);
    }
    world.close();
    assert_eq!(world.snapshot().subscriptions, 0);
}

#[test]
fn typed_link_owner_stop_fences_only_owned_route() {
    let world = World::new(Config::default()).unwrap();
    let source_one = world.allocate(EndpointKind::Native, None).unwrap();
    let source_two = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    let output = PortSpec {
        name: "out".into(),
        direction: PortDirection::Output,
        schema: PayloadType::Pulse,
    };
    let input = PortSpec {
        name: "in".into(),
        direction: PortDirection::Input,
        schema: PayloadType::Pulse,
    };
    let source_port_one = world
        .register_component(
            source_one,
            ComponentDescriptor {
                name: "S1".into(),
                version: 1,
                ports: vec![output.clone()],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    let source_port_two = world
        .register_component(
            source_two,
            ComponentDescriptor {
                name: "S2".into(),
                version: 1,
                ports: vec![output],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    let target_port = world
        .register_component(
            target,
            ComponentDescriptor {
                name: "T".into(),
                version: 1,
                ports: vec![input],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    world.activate(source_one).unwrap();
    world.activate(source_two).unwrap();
    world.activate(target).unwrap();
    let owner = actor(&world);
    let _first = world.link(owner, source_port_one, target_port).unwrap();
    let _second = world
        .link(source_two, source_port_two, target_port)
        .unwrap();
    assert_eq!(
        route(
            &world,
            world
                .publish_port(source_port_one, Payload::Pulse(1))
                .unwrap(),
        )
        .admitted,
        1
    );
    world.stop(owner).unwrap();
    let replacement = actor(&world);
    assert_eq!(replacement.slot, owner.slot);
    assert_ne!(replacement.generation, owner.generation);
    assert!(world.claim(target).unwrap().is_none());
    assert_eq!(world.state(source_one), Ok(Lifecycle::Active));
    assert_eq!(
        route(
            &world,
            world
                .publish_port(source_port_one, Payload::Pulse(3))
                .unwrap(),
        )
        .admitted,
        0
    );
    assert_eq!(
        route(
            &world,
            world
                .publish_port(source_port_two, Payload::Pulse(2))
                .unwrap(),
        )
        .admitted,
        1
    );
    let delivery = world.claim(target).unwrap().expect("unrelated owner route");
    assert_eq!(delivery.payload(), &Payload::Pulse(2));
    delivery.finish(true);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn consumer_drain_consults_only_its_incoming_producers_and_waits_for_tracked_work() {
    let world = World::new(Config::default()).unwrap();
    let source = actor(&world);
    let target = actor(&world);
    let unrelated_source = actor(&world);
    let unrelated_target = actor(&world);
    world.subscribe(source, target).unwrap();
    world.subscribe(unrelated_source, unrelated_target).unwrap();
    assert!(!world.upstream_done(target).unwrap());
    let task = world.track_task(source).unwrap();
    world
        .request_drain(source, world.now() + std::time::Duration::from_secs(10))
        .unwrap();
    assert!(!world.upstream_done(target).unwrap());
    drop(task);
    assert!(world.upstream_done(target).unwrap());
    assert!(!world.upstream_done(unrelated_target).unwrap());
    world.maintain(world.now());
    assert_eq!(world.state(source), Ok(Lifecycle::Stopped));
    assert!(world.upstream_done(target).unwrap());
    assert_eq!(world.snapshot().subscriptions, 1);
    assert!(world.close().native_done);
}
