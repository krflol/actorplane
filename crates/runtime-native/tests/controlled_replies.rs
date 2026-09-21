#![cfg(feature = "test-runtime")]

use actorplane_core::{
    Config, EndpointKind, OperationStatus, Payload, PayloadType, TerminalOutcome, World,
};
use actorplane_native::{NativeRuntime, controls::ControlledReplies};
use std::time::Duration;

fn setup(
    limit: usize,
) -> (
    NativeRuntime,
    ControlledReplies,
    actorplane_core::ActorRef,
    actorplane_core::ActorRef,
) {
    let runtime = NativeRuntime::new_virtual(Config::default(), 16).unwrap();
    let control = ControlledReplies::new(limit).unwrap();
    let responder = control.prepare(&runtime, None, PayloadType::Pulse).unwrap();
    let world = runtime.world().clone();
    world.activate(responder.owner).unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();
    (runtime, control, responder.owner, requester)
}

fn admit(
    runtime: &NativeRuntime,
    control: &ControlledReplies,
    _world: &World,
    id: actorplane_core::OperationId,
) {
    for _ in 0..20 {
        runtime.pump_virtual(1_000).unwrap();
        if control.pending().contains(&id) {
            return;
        }
    }
    panic!("controlled request was not admitted");
}

#[test]
fn response_is_inert_until_completed_and_retention_lasts_until_take() {
    let (mut runtime, control, responder, requester) = setup(4);
    let world = runtime.world().clone();
    let baseline = world.snapshot().retained_payload_bytes;
    let id = world
        .request(
            requester,
            responder,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    admit(&runtime, &control, &world, id);
    assert!(matches!(
        world.operation_status(id),
        Ok(OperationStatus::Pending(_))
    ));
    assert!(world.snapshot().retained_payload_bytes > baseline);
    assert!(control.complete(&world, id, Payload::Pulse(7)).unwrap());
    let retained_with_response = world.snapshot().retained_payload_bytes;
    assert!(retained_with_response > baseline);
    runtime.pump_virtual(1_000).unwrap();
    let outcome = world.take_operation(id).unwrap().unwrap();
    assert!(matches!(outcome, TerminalOutcome::Completed(_)));
    drop(outcome);
    assert!(world.snapshot().retained_payload_bytes < retained_with_response);
    runtime.pump_virtual(1_000).unwrap();
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn completion_is_single_winner_and_unknown_or_cross_world_does_not_hold_bytes() {
    let (mut runtime, control, responder, requester) = setup(4);
    let world = runtime.world().clone();
    let id = world
        .request(
            requester,
            responder,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    admit(&runtime, &control, &world, id);
    let baseline = world.snapshot().retained_payload_bytes;
    let other = World::new(Config::default()).unwrap();
    assert!(matches!(
        control.complete(&other, id, Payload::Pulse(2)),
        Err(actorplane_native::sdk::NativeError::Core(
            actorplane_core::Error::CrossWorld
        ))
    ));
    assert_eq!(other.snapshot().retained_payload_bytes, 0);
    let unknown = actorplane_core::OperationId {
        generation: id.generation + 1,
        ..id
    };
    assert!(
        !control
            .complete(&world, unknown, Payload::Pulse(3))
            .unwrap()
    );
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    assert!(control.complete(&world, id, Payload::Pulse(4)).unwrap());
    assert!(!control.complete(&world, id, Payload::Pulse(5)).unwrap());
    runtime.pump_virtual(1_000).unwrap();
    assert!(matches!(
        world.take_operation(id).unwrap(),
        Some(TerminalOutcome::Completed(_))
    ));
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn cancellation_before_factory_poll_cleans_registry_and_input() {
    let (mut runtime, control, responder, requester) = setup(4);
    let world = runtime.world().clone();
    let baseline = world.snapshot().retained_payload_bytes;
    let id = world
        .request(
            requester,
            responder,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    for _ in 0..20 {
        runtime.pump_virtual(1).unwrap();
        if control.pending().contains(&id) {
            break;
        }
    }
    assert!(control.pending().contains(&id));
    assert!(world.cancel_operation(id).unwrap());
    runtime
        .advance_virtual(Duration::from_millis(1), 1_000)
        .unwrap();
    assert!(control.pending().is_empty());
    assert!(matches!(
        world.take_operation(id).unwrap(),
        Some(TerminalOutcome::Cancelled)
    ));
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn registry_limit_rejects_second_owner_without_blocking_first() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 16).unwrap();
    let control = ControlledReplies::new(1).unwrap();
    let first = control.prepare(&runtime, None, PayloadType::Pulse).unwrap();
    let second = control.prepare(&runtime, None, PayloadType::Pulse).unwrap();
    let world = runtime.world().clone();
    world.activate(first.owner).unwrap();
    world.activate(second.owner).unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();
    let one = world
        .request(
            requester,
            first.owner,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    let two = world
        .request(
            requester,
            second.owner,
            Payload::Pulse(2),
            world.now() + Duration::from_secs(1),
        )
        .unwrap();
    admit(&runtime, &control, &world, one);
    runtime.pump_virtual(1_000).unwrap();
    assert_eq!(control.pending(), vec![one]);
    assert!(matches!(
        world.operation_status(two),
        Ok(OperationStatus::Terminal(_))
    ));
    assert!(matches!(
        world.take_operation(two).unwrap(),
        Some(TerminalOutcome::Failed { .. })
            | Some(TerminalOutcome::OwnerStopped)
            | Some(TerminalOutcome::TargetStopped)
    ));
    world.cancel_operation(one).unwrap();
    runtime
        .advance_virtual(Duration::from_millis(1), 1_000)
        .unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
