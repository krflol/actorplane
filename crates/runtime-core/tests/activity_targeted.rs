//! Targeted activity observers must wake only affected actors.

use actorplane_core::{
    ActorRef, Config, EndpointKind, FailureAction, FailureDetails, FailureFrame, FailurePhase,
    Lifecycle, MessageOptions, Payload, World,
};
use std::{
    sync::{Arc, Barrier, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};
mod common;
use common::route;

struct Signal(mpsc::SyncSender<()>);

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.try_send(());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.try_send(());
    }
}

fn world() -> World {
    World::new(Config {
        mailbox_capacity: 128,
        ..Config::default()
    })
    .unwrap()
}

fn active(world: &World, kind: EndpointKind) -> ActorRef {
    let actor = world.allocate(kind, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

fn observe(world: &World, actor: ActorRef) -> (u64, mpsc::Receiver<()>) {
    let observed = world.activity(actor).unwrap();
    let (sender, receiver) = mpsc::sync_channel(8);
    let waker = Waker::from(Arc::new(Signal(sender)));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        world.poll_activity(actor, observed, &mut context),
        Poll::Pending
    ));
    (observed, receiver)
}

fn assert_no_wake(receiver: &mpsc::Receiver<()>) {
    assert!(receiver.recv_timeout(Duration::from_millis(40)).is_err());
}

#[test]
fn unrelated_send_and_stop_do_not_wake_observer() {
    let world = world();
    let observer = active(&world, EndpointKind::Native);
    let unrelated = active(&world, EndpointKind::Native);
    let (_, receiver) = observe(&world, observer);

    world.send(unrelated, Payload::Pulse(1)).unwrap();
    assert_no_wake(&receiver);
    world.stop(unrelated).unwrap();
    assert_no_wake(&receiver);
}

#[test]
fn target_delivery_wakes_target_but_not_unrelated_actor() {
    let world = world();
    let source = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    let unrelated = active(&world, EndpointKind::Native);
    world.subscribe(source, target).unwrap();
    let (_, target_rx) = observe(&world, target);
    let (_, unrelated_rx) = observe(&world, unrelated);

    route(&world, world.publish(source, Payload::Pulse(7)).unwrap());
    target_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    assert_no_wake(&unrelated_rx);
}

#[test]
fn same_actor_delivery_lease_finish_wakes_after_claim() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    let lease = world.claim(target).unwrap().unwrap();
    let (_, receiver) = observe(&world, target);
    lease.finish(true);
    receiver.recv_timeout(Duration::from_millis(250)).unwrap();
}

#[test]
fn operation_owner_and_target_are_woken_but_unrelated_is_not() {
    let world = world();
    let owner = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    let unrelated = active(&world, EndpointKind::Native);
    let (_, owner_rx) = observe(&world, owner);
    let (_, target_rx) = observe(&world, target);
    let (_, unrelated_rx) = observe(&world, unrelated);

    let operation = world
        .request_with(
            owner,
            target,
            Payload::Pulse(3),
            world.now() + Duration::from_secs(1),
            MessageOptions::default(),
        )
        .unwrap();
    owner_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    target_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    assert_no_wake(&unrelated_rx);

    let lease = world.claim(target).unwrap().unwrap();
    let (_, owner_after_claim) = observe(&world, owner);
    assert!(
        world
            .complete_operation(operation, target, Payload::Pulse(4))
            .unwrap()
    );
    owner_after_claim
        .recv_timeout(Duration::from_millis(250))
        .unwrap();
    lease.finish(true);
}

#[test]
fn repeated_poll_then_admit_has_no_lost_wake() {
    let world = world();
    let target = active(&world, EndpointKind::Native);
    for _ in 0..32 {
        let observed = world.activity(target).unwrap();
        let (sender, receiver) = mpsc::sync_channel(2);
        let waker = Waker::from(Arc::new(Signal(sender)));
        let mut context = Context::from_waker(&waker);
        let barrier = Arc::new(Barrier::new(2));
        let sender_world = world.clone();
        let sender_barrier = barrier.clone();
        let sender_thread = thread::spawn(move || {
            sender_barrier.wait();
            sender_world.send(target, Payload::Pulse(1)).unwrap();
        });
        barrier.wait();
        let poll = world.poll_activity(target, observed, &mut context);
        sender_thread.join().unwrap();
        match poll {
            Poll::Pending => receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            Poll::Ready(Ok(version)) => assert_ne!(version, observed),
            Poll::Ready(Err(error)) => panic!("unexpected activity error: {error}"),
        }
        let lease = world.claim(target).unwrap().unwrap();
        lease.finish(true);
    }
}

#[test]
fn route_removal_wakes_consumer_and_failure_wakes_parent() {
    let world = world();
    let producer = active(&world, EndpointKind::Native);
    let consumer = active(&world, EndpointKind::Native);
    let route = world.subscribe(producer, consumer).unwrap();
    let (_, consumer_rx) = observe(&world, consumer);
    world.unsubscribe(route).unwrap();
    consumer_rx
        .recv_timeout(Duration::from_millis(250))
        .unwrap();

    let parent = active(&world, EndpointKind::Python);
    let child = world.allocate(EndpointKind::Python, Some(parent)).unwrap();
    world.activate(child).unwrap();
    let (_, parent_rx) = observe(&world, parent);
    let details = FailureDetails::new(
        FailurePhase::Handler,
        "handler",
        "builtins.ValueError",
        vec![FailureFrame::new("test.py", 1, "handler")],
    );
    world
        .report_failure(child, None, None, None, details, FailureAction::Continue)
        .unwrap();
    parent_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    assert_eq!(world.state(parent), Ok(Lifecycle::Active));
}

#[test]
fn child_work_wakes_an_observing_ancestor() {
    let world = world();
    let parent = active(&world, EndpointKind::Native);
    let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    world.activate(child).unwrap();
    let (_, parent_rx) = observe(&world, parent);

    world.send(child, Payload::Pulse(9)).unwrap();
    parent_rx.recv_timeout(Duration::from_millis(250)).unwrap();
}

#[test]
fn finishing_a_draining_producer_task_wakes_the_consumer() {
    let world = world();
    let producer = active(&world, EndpointKind::Native);
    let consumer = active(&world, EndpointKind::Native);
    world.subscribe(producer, consumer).unwrap();
    let producer_task = world.track_task(producer).unwrap();
    let consumer_task = world.track_task(consumer).unwrap();
    world
        .request_drain(producer, world.now() + Duration::from_secs(1))
        .unwrap();
    world
        .request_drain(consumer, world.now() + Duration::from_secs(1))
        .unwrap();
    let (_, consumer_rx) = observe(&world, consumer);
    drop(producer_task);
    // The dependency transition is committed before the task lease is dropped.
    consumer_rx
        .recv_timeout(Duration::from_millis(250))
        .unwrap();
    drop(consumer_task);
}

#[test]
fn stopping_operation_target_wakes_external_owner() {
    let world = world();
    let owner = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    let operation = world
        .request(
            owner,
            target,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    let (_, owner_rx) = observe(&world, owner);
    world.stop(target).unwrap();
    owner_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Terminal(
            actorplane_core::TerminalOutcome::TargetStopped
        ))
    ));
}

#[test]
fn stopping_operation_owner_wakes_target() {
    let world = world();
    let owner = active(&world, EndpointKind::Native);
    let target = active(&world, EndpointKind::Native);
    let operation = world
        .request(
            owner,
            target,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    let (_, target_rx) = observe(&world, target);
    let owner_task = world.track_task(owner).unwrap();
    world.stop(owner).unwrap();
    target_rx.recv_timeout(Duration::from_millis(250)).unwrap();
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Terminal(
            actorplane_core::TerminalOutcome::OwnerStopped
        ))
    ));
    drop(owner_task);
}
