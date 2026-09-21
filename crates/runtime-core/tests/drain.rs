use actorplane_core::*;
use std::time::{Duration, Instant};
mod common;
use common::route;

fn world() -> World {
    World::new(Config {
        mailbox_capacity: 8,
        mailbox_bytes: 256,
        native_payload_budget: 1024,
        ..Default::default()
    })
    .unwrap()
}

fn active(world: &World, kind: EndpointKind) -> ActorRef {
    let reference = world.allocate(kind, None).unwrap();
    world.activate(reference).unwrap();
    reference
}

#[test]
fn drain_claims_admitted_fifo_and_rejects_new_work() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    world.send(target, Payload::Pulse(2)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    world.request_drain(target, deadline).unwrap();
    assert_eq!(world.state(target), Ok(Lifecycle::Quiescing));
    assert_eq!(
        world.send(target, Payload::Pulse(3)),
        Err(Error::ActorStopped)
    );
    let first = world.claim(target).unwrap().unwrap();
    assert_eq!(first.payload(), &Payload::Pulse(1));
    first.finish(true);
    let second = world.claim(target).unwrap().unwrap();
    assert_eq!(second.payload(), &Payload::Pulse(2));
    second.finish(true);
    world.maintain(deadline);
    assert_eq!(world.state(target), Ok(Lifecycle::Stopped));
}

#[test]
fn drain_rejects_new_subscriptions_tasks_and_children() {
    let world = world();
    let source = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    world
        .request_drain(source, Instant::now() + Duration::from_secs(60))
        .unwrap();
    assert_eq!(world.subscribe(source, target), Err(Error::ActorStopped));
    assert!(matches!(world.track_task(source), Err(Error::ActorStopped)));
    assert_eq!(
        world.allocate(EndpointKind::Native, Some(source)),
        Err(Error::ActorStopped)
    );
}

#[test]
fn tracked_completion_is_allowed_during_drain_but_cancel_fences_it() {
    let world = world();
    let source = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    world.subscribe(source, target).unwrap();
    let task = world.track_task(source).unwrap();
    world
        .request_drain(source, Instant::now() + Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        route(
            &world,
            task.publish_completion(source, Payload::Pulse(7)).unwrap(),
        )
        .admitted,
        1
    );
    world.stop(source).unwrap();
    assert!(matches!(
        task.publish_completion(source, Payload::Pulse(8)),
        Err(Error::ActorNotReady | Error::ActorStopped)
    ));
    drop(task);
}

#[test]
fn unsubscribe_invalidates_queued_delivery_during_drain() {
    let world = world();
    let source = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    let subscription = world.subscribe(source, target).unwrap();
    route(&world, world.publish(source, Payload::Pulse(1)).unwrap());
    world
        .request_drain(target, Instant::now() + Duration::from_secs(60))
        .unwrap();
    world.unsubscribe(subscription).unwrap();
    assert!(world.claim(target).unwrap().is_none());
    world.maintain(Instant::now() + Duration::from_secs(60));
    assert_eq!(world.state(target), Ok(Lifecycle::Stopped));
}

#[test]
fn deadline_cancels_unclaimed_but_claimed_work_remains_inflight() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    world.send(target, Payload::Pulse(2)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    world.request_drain(target, deadline).unwrap();
    let lease = world.claim(target).unwrap().unwrap();
    world.maintain(deadline);
    assert_eq!(world.state(target), Ok(Lifecycle::Stopping));
    assert_eq!(
        world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == target)
            .unwrap()
            .queue_entries,
        0
    );
    lease.finish(true);
    assert_eq!(world.state(target), Ok(Lifecycle::Stopped));
}

#[test]
fn repeated_drain_uses_earliest_shared_deadline() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    let first = Instant::now() + Duration::from_secs(10);
    let second = first + Duration::from_secs(100);
    world.request_drain(target, first).unwrap();
    world.request_drain(target, second).unwrap();
    let snapshot = world.snapshot();
    assert_eq!(
        snapshot
            .actors
            .iter()
            .find(|a| a.reference == target)
            .unwrap()
            .drain_deadline,
        Some(first)
    );
}

#[test]
fn empty_drain_past_deadline_is_not_marked_timeout() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    let report = world
        .request_drain(target, Instant::now() - Duration::from_secs(1))
        .unwrap();
    assert!(!report.timed_out);
    assert_eq!(world.state(target), Ok(Lifecycle::Stopped));
}

#[test]
fn drain_timeout_remains_sticky_in_stop_report() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    world.request_drain(target, deadline).unwrap();
    world.maintain(deadline);
    let report = world.stop(target).unwrap();
    assert!(report.timed_out);
}

#[test]
fn parent_child_drain_waits_for_native_task_before_reuse() {
    let world = World::new(Config {
        max_actors: 2,
        ..Default::default()
    })
    .unwrap();
    let parent = active(&world, EndpointKind::Native);
    let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    world.activate(child).unwrap();
    let task = world.track_task(child).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    world.request_drain(parent, deadline).unwrap();
    world.maintain(deadline);
    assert_eq!(world.state(parent), Ok(Lifecycle::Stopping));
    assert_eq!(world.state(child), Ok(Lifecycle::Stopping));
    assert_eq!(
        world.allocate(EndpointKind::Native, None),
        Err(Error::LimitExceeded)
    );
    drop(task);
    assert_eq!(world.state(parent), Ok(Lifecycle::Stopped));
    let replacement = world.allocate(EndpointKind::Native, None).unwrap();
    assert_ne!(replacement.generation, parent.generation);
}
