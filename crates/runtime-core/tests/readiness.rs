use actorplane_core::{
    Config, EndpointKind, FailureAction, FailureDetails, FailurePhase, Lifecycle, Payload,
    PythonReadyPhase, World,
};
use std::time::Duration;

fn actor(
    world: &World,
    kind: EndpointKind,
    parent: Option<actorplane_core::ActorRef>,
) -> actorplane_core::ActorRef {
    world.allocate(kind, parent).unwrap()
}

fn active(world: &World, kind: EndpointKind) -> actorplane_core::ActorRef {
    let reference = actor(world, kind, None);
    world.activate(reference).unwrap();
    reference
}

fn next(
    world: &World,
    phase: PythonReadyPhase,
    after: Option<u64>,
    through: u64,
) -> Option<(u64, actorplane_core::ActorRef)> {
    world.python_ready_next(phase, after, through)
}

#[test]
fn start_order_uses_registration_sequence_and_cutoff() {
    let world = World::new(Config::default()).unwrap();
    let first = actor(&world, EndpointKind::Python, None);
    let second = actor(&world, EndpointKind::Python, None);
    let native = actor(&world, EndpointKind::Native, None);
    let cutoff = world.python_ready_cutoff();

    let first_ready = next(&world, PythonReadyPhase::Start, None, cutoff).unwrap();
    let second_ready = next(&world, PythonReadyPhase::Start, Some(first_ready.0), cutoff).unwrap();
    assert_eq!(first_ready.1, first);
    assert_eq!(second_ready.1, second);
    assert!(
        next(
            &world,
            PythonReadyPhase::Start,
            Some(second_ready.0),
            cutoff
        )
        .is_none()
    );
    assert!(next(&world, PythonReadyPhase::Start, None, cutoff).is_some_and(|(_, r)| r != native));

    world.activate(first).unwrap();
    world.activate(second).unwrap();
    world.stop(first).unwrap();
    world.finish_python(first).unwrap();
    let replacement = actor(&world, EndpointKind::Python, None);
    assert_eq!(replacement.slot, first.slot);
    assert!(replacement.generation > first.generation);
    let later = world.python_ready_cutoff();
    assert!(
        next(&world, PythonReadyPhase::Start, Some(cutoff), later)
            .is_some_and(|(_, r)| r == replacement)
    );
}

#[test]
fn delivery_ready_is_cleared_by_claim_and_rearmed_after_finish() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world, EndpointKind::Python);
    let source = active(&world, EndpointKind::Native);
    world.send(target, Payload::Pulse(1)).unwrap();
    let cutoff = world.python_ready_cutoff();
    let ready = next(&world, PythonReadyPhase::Delivery, None, cutoff).unwrap();
    assert_eq!(ready.1, target);
    let lease = world.claim(target).unwrap().unwrap();
    assert!(next(&world, PythonReadyPhase::Delivery, None, cutoff).is_none());
    world.send(target, Payload::Pulse(2)).unwrap();
    assert!(next(&world, PythonReadyPhase::Delivery, None, cutoff).is_none());
    lease.finish(true);
    assert!(
        next(
            &world,
            PythonReadyPhase::Delivery,
            None,
            world.python_ready_cutoff()
        )
        .is_some_and(|(_, r)| r == target)
    );
    assert_eq!(
        world.claim(target).unwrap().unwrap().payload(),
        &Payload::Pulse(2)
    );
    assert_eq!(world.state(source), Ok(Lifecycle::Active));
}

#[test]
fn business_index_fences_reused_generation() {
    let world = World::new(Config::default()).unwrap();
    let old = active(&world, EndpointKind::Python);
    world.stop(old).unwrap();
    world.finish_python(old).unwrap();
    let replacement = active(&world, EndpointKind::Python);
    assert_eq!(old.slot, replacement.slot);
    assert_ne!(old.generation, replacement.generation);
    world.send(replacement, Payload::Pulse(44)).unwrap();
    let ready = next(&world, PythonReadyPhase::Delivery, None, u64::MAX).unwrap();
    assert_eq!(ready.1, replacement);
    assert_ne!(ready.1, old);
}

#[test]
fn failure_ready_is_coalesced_and_requeued_after_control_claim() {
    let world = World::new(Config::default()).unwrap();
    let supervisor = active(&world, EndpointKind::Python);
    let child = actor(&world, EndpointKind::Native, Some(supervisor));
    let second_child = actor(&world, EndpointKind::Native, Some(supervisor));
    world.activate(child).unwrap();
    world.activate(second_child).unwrap();
    let details = FailureDetails::new(FailurePhase::Handler, "handler", "Error", vec![]);
    world
        .report_failure(
            child,
            Some(1),
            None,
            None,
            details.clone(),
            FailureAction::Continue,
        )
        .unwrap();
    world
        .report_failure(child, Some(2), None, None, details, FailureAction::Continue)
        .unwrap();
    let cutoff = world.python_ready_cutoff();
    assert_eq!(
        next(&world, PythonReadyPhase::Failure, None, cutoff)
            .unwrap()
            .1,
        supervisor
    );
    let notice = world.claim_failure(supervisor).unwrap().unwrap();
    assert!(next(&world, PythonReadyPhase::Failure, None, cutoff).is_none());
    world
        .report_failure(
            second_child,
            Some(3),
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "handler", "Error", vec![]),
            FailureAction::Continue,
        )
        .unwrap();
    notice.finish(None).unwrap();
    assert!(
        next(
            &world,
            PythonReadyPhase::Failure,
            None,
            world.python_ready_cutoff()
        )
        .is_some_and(|(_, r)| r == supervisor)
    );
}

#[test]
fn stopping_python_actor_exposes_cleanup_until_python_finishes() {
    let world = World::new(Config::default()).unwrap();
    let parent = actor(&world, EndpointKind::Python, None);
    let child = actor(&world, EndpointKind::Python, Some(parent));
    world.activate(parent).unwrap();
    world.activate(child).unwrap();
    world.stop(parent).unwrap();
    assert_eq!(world.state(parent), Ok(Lifecycle::Stopping));
    let child_cleanup = next(&world, PythonReadyPhase::Cleanup, None, u64::MAX).unwrap();
    assert_eq!(child_cleanup.1, child);
    world.finish_python(child).unwrap();
    let parent_cleanup = next(&world, PythonReadyPhase::Cleanup, None, u64::MAX).unwrap();
    assert_eq!(parent_cleanup.1, parent);
    world.finish_python(parent).unwrap();
    assert!(next(&world, PythonReadyPhase::Cleanup, None, u64::MAX).is_none());
}

#[test]
fn child_failure_is_staged_until_parent_activation() {
    let world = World::new(Config::default()).unwrap();
    let parent = actor(&world, EndpointKind::Python, None);
    let child = actor(&world, EndpointKind::Native, Some(parent));
    world.activate(child).unwrap();
    world
        .report_failure(
            child,
            Some(7),
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "handler", "Error", vec![]),
            FailureAction::Continue,
        )
        .unwrap();
    let cutoff = world.python_ready_cutoff();
    assert!(next(&world, PythonReadyPhase::Failure, None, cutoff).is_none());
    world.activate(parent).unwrap();
    assert!(
        next(&world, PythonReadyPhase::Failure, None, u64::MAX).is_some_and(|(_, r)| r == parent)
    );
}

#[test]
fn readiness_wait_wakes_for_send_and_stop() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world, EndpointKind::Python);
    let waiter = world.clone();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let waiter_barrier = barrier.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        waiter_barrier.wait();
        sent.send(waiter.wait_python_ready(Duration::from_secs(5)))
            .unwrap();
    });
    barrier.wait();
    std::thread::sleep(Duration::from_millis(10));
    world.send(target, Payload::Pulse(9)).unwrap();
    assert_eq!(received.recv_timeout(Duration::from_secs(1)), Ok(true));
    handle.join().unwrap();
}

#[test]
fn empty_readiness_wait_times_out_then_stop_wakes_cleanup() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world, EndpointKind::Python);
    assert!(!world.wait_python_ready(Duration::from_millis(20)));

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let waiter = world.clone();
    let waiter_barrier = barrier.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        waiter_barrier.wait();
        sent.send(waiter.wait_python_ready(Duration::from_secs(5)))
            .unwrap();
    });
    barrier.wait();
    world.stop(target).unwrap();
    assert_eq!(received.recv_timeout(Duration::from_secs(1)), Ok(true));
    handle.join().unwrap();
}
