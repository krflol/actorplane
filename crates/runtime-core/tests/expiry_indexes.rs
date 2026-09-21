use actorplane_core::{
    Clock, ComponentDescriptor, Config, EndpointKind, InterfaceSpec, Lifecycle, MessageOptions,
    Payload, PayloadType, PortDirection, PortSpec, World,
};
use std::time::{Duration, Instant};

fn setup() -> (
    World,
    actorplane_core::VirtualClock,
    Instant,
    actorplane_core::ActorRef,
    actorplane_core::ActorRef,
) {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let owner = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(owner).unwrap();
    world.activate(target).unwrap();
    (world, virtual_clock, initial, owner, target)
}

#[test]
fn out_of_fifo_deadlines_expire_without_discarding_future_entries() {
    let (world, clock, initial, owner, target) = setup();
    let future = initial + Duration::from_secs(10);
    let due = initial + Duration::from_secs(2);
    world
        .send_with(
            target,
            Payload::Pulse(10),
            MessageOptions {
                deadline: Some(future),
                ..Default::default()
            },
        )
        .unwrap();
    world
        .send_with(
            target,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(due),
                ..Default::default()
            },
        )
        .unwrap();
    clock.advance(Duration::from_secs(2)).unwrap();
    world.maintain(clock.current());
    let snapshot = world.snapshot();
    assert_eq!(
        snapshot
            .actors
            .iter()
            .find(|actor| actor.reference == target)
            .unwrap()
            .queue_entries,
        1
    );
    assert_eq!(snapshot.metrics.expired, 1);
    let delivery = world.claim(target).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(10));
    delivery.finish(true);
    let _ = owner;
}

#[test]
fn equal_deadlines_expire_once_per_queued_delivery() {
    let (world, clock, initial, owner, target) = setup();
    let deadline = initial + Duration::from_secs(1);
    for value in 0..2 {
        world
            .send_with(
                target,
                Payload::Pulse(value),
                MessageOptions {
                    deadline: Some(deadline),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert_eq!(world.snapshot().metrics.expired, 2);
    assert!(world.claim(target).unwrap().is_none());
    let _ = owner;
}

#[test]
fn failed_admission_does_not_leave_expiry_index_entries() {
    let config = Config {
        mailbox_capacity: 1,
        ..Config::default()
    };
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(config, clock).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(target).unwrap();
    let deadline = initial + Duration::from_secs(1);
    world
        .send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            }
        ),
        Err(actorplane_core::Error::QueueFull)
    );
    virtual_clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(virtual_clock.current());
    assert_eq!(world.snapshot().metrics.expired, 1);
    assert_eq!(
        world
            .snapshot()
            .actors
            .iter()
            .find(|actor| actor.reference == target)
            .unwrap()
            .queue_entries,
        0
    );
}

#[test]
fn expired_request_terminalizes_once_and_late_completion_is_fenced() {
    let (world, clock, initial, owner, target) = setup();
    let operation = world
        .request(
            owner,
            target,
            Payload::Pulse(1),
            initial + Duration::from_secs(1),
        )
        .unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Terminal(
            actorplane_core::TerminalOutcome::TimedOut
        ))
    ));
    assert!(matches!(
        world.complete_operation(operation, target, Payload::Pulse(2)),
        Ok(false)
    ));
    assert_eq!(world.snapshot().metrics.operation_timed_out, 1);
    assert!(world.take_operation(operation).unwrap().is_some());
    world.maintain(clock.current());
    assert_eq!(world.snapshot().metrics.operation_timed_out, 1);
}

#[test]
fn equal_deadline_claim_wins_for_one_and_expiry_removes_the_other() {
    let (world, clock, initial, owner, target) = setup();
    let deadline = initial + Duration::from_secs(1);
    world
        .send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        )
        .unwrap();
    world
        .send_with(
            target,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        )
        .unwrap();
    let claimed = world.claim(target).unwrap().unwrap();
    assert_eq!(claimed.payload(), &Payload::Pulse(1));
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert_eq!(world.snapshot().metrics.expired, 1);
    claimed.finish(true);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert!(world.claim(target).unwrap().is_none());
    let _ = owner;
}

#[test]
fn retired_generation_deadline_cannot_expire_replacement() {
    let (world, clock, initial, owner, old) = setup();
    let old_deadline = initial + Duration::from_secs(1);
    world
        .send_with(
            old,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(old_deadline),
                ..Default::default()
            },
        )
        .unwrap();
    world.stop(old).unwrap();
    let replacement = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(replacement).unwrap();
    assert_eq!(replacement.slot, old.slot);
    assert_ne!(replacement.generation, old.generation);
    let new_deadline = initial + Duration::from_secs(2);
    world
        .send_with(
            replacement,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(new_deadline),
                ..Default::default()
            },
        )
        .unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert_eq!(world.snapshot().metrics.expired, 0);
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert_eq!(world.snapshot().metrics.expired, 1);
    let _ = owner;
}

#[test]
fn staged_startup_expiry_flushes_only_live_entries() {
    let (world, clock, initial, _parent, child) = setup();
    // Recreate the child as a true Starting child under a Starting parent.
    world.stop(child).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    let output = world
        .register_component(
            child,
            ComponentDescriptor {
                name: "StagedSource".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "out".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::Pulse,
                }],
                interfaces: vec![InterfaceSpec {
                    name: "StagedSource".into(),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "out".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    }],
                }],
            },
        )
        .unwrap()[0];
    let input = world
        .register_component(
            target,
            ComponentDescriptor {
                name: "StagedTarget".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "in".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                }],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    world.activate(target).unwrap();
    world.activate(child).unwrap();
    world.link(child, output, input).unwrap();
    let due = initial + Duration::from_secs(1);
    let future = initial + Duration::from_secs(2);
    let _first = world
        .publish_port_with(
            output,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(due),
                ..Default::default()
            },
        )
        .unwrap();
    let _second = world
        .publish_port_with(
            output,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(future),
                ..Default::default()
            },
        )
        .unwrap();
    let _third = world
        .publish_port_with(output, Payload::Pulse(3), MessageOptions::default())
        .unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    world.activate(parent).unwrap();
    while world.routing_ready() {
        world.route_batch();
    }
    assert_eq!(
        world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == target)
            .unwrap()
            .queue_entries,
        2
    );
    let delivery = world.claim(target).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(2));
    delivery.finish(true);
    let delivery = world.claim(target).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(3));
    delivery.finish(true);
    assert_eq!(world.snapshot().metrics.expired, 1);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn nested_drain_preserves_earliest_child_deadline() {
    let (world, clock, initial, _parent, child) = setup();
    world.stop(child).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    world.activate(parent).unwrap();
    world.activate(child).unwrap();
    let task = world.track_task(child).unwrap();
    world
        .request_drain(child, initial + Duration::from_secs(1))
        .unwrap();
    world
        .request_drain(parent, initial + Duration::from_secs(10))
        .unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    world.maintain(clock.current());
    assert!(matches!(world.state(child), Ok(Lifecycle::Stopping)));
    assert!(matches!(world.state(parent), Ok(Lifecycle::Stopping)));
    assert_eq!(world.snapshot().metrics.drain_timeouts, 1);
    drop(task);
    world.maintain(clock.current());
    assert!(matches!(world.state(parent), Ok(Lifecycle::Stopped)));
    assert_eq!(world.state(child), Ok(Lifecycle::Stopped));
}

#[test]
fn stopped_staged_output_is_removed_before_replacement_activation() {
    let initial = Instant::now();
    let (clock, time) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let source = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    let output = world
        .register_component(
            source,
            ComponentDescriptor {
                name: "StoppedStaging".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "out".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::Pulse,
                }],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    let options = MessageOptions {
        deadline: Some(initial + Duration::from_secs(1)),
        ..Default::default()
    };
    for value in 0..2 {
        let ticket = world
            .publish_port_with(output, Payload::Pulse(value), options.clone())
            .unwrap();
        assert!(matches!(
            ticket.status(),
            actorplane_core::PublicationStatus::Staged
        ));
    }
    world.activate(source).unwrap();
    assert!(world.snapshot().retained_payload_bytes > 0);
    assert_eq!(world.stop(parent).unwrap().discarded, 2);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    let replacement_parent = world.allocate(EndpointKind::Native, None).unwrap();
    let replacement_source = world
        .allocate(EndpointKind::Native, Some(replacement_parent))
        .unwrap();
    assert_eq!(replacement_source.slot, source.slot);
    assert_ne!(replacement_source.generation, source.generation);
    time.advance(Duration::from_secs(1)).unwrap();
    world.activate(replacement_source).unwrap();
    world.activate(replacement_parent).unwrap();
    world.maintain(time.current());
    assert_eq!(world.snapshot().metrics.expired, 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn staged_self_link_transfers_deadline_to_queued_delivery() {
    let initial = Instant::now();
    let (clock, time) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    let ports = world
        .register_component(
            source,
            ComponentDescriptor {
                name: "SelfStaging".into(),
                version: 1,
                ports: vec![
                    PortSpec {
                        name: "out".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    },
                    PortSpec {
                        name: "in".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::Pulse,
                    },
                ],
                interfaces: vec![],
            },
        )
        .unwrap();
    world.link(source, ports[0], ports[1]).unwrap();
    world
        .publish_port_with(
            ports[0],
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(initial + Duration::from_secs(1)),
                ..Default::default()
            },
        )
        .unwrap();
    world.activate(source).unwrap();
    while world.routing_ready() {
        world.route_batch();
    }
    let snapshot = world.snapshot();
    assert_eq!(snapshot.actors[0].staged_entries, 0);
    assert_eq!(snapshot.actors[0].queue_entries, 1);
    time.advance(Duration::from_secs(1)).unwrap();
    world.maintain(time.current());
    assert!(world.claim(source).unwrap().is_none());
    assert_eq!(world.snapshot().metrics.expired, 1);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
