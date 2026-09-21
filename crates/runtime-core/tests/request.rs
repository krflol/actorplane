use actorplane_core::*;
use std::time::{Duration, Instant};

fn world() -> World {
    World::new(Config {
        mailbox_capacity: 1,
        mailbox_bytes: 64,
        native_payload_budget: 128,
        max_event_bytes: 32,
        ..Default::default()
    })
    .unwrap()
}
fn active(w: &World) -> ActorRef {
    let r = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(r).unwrap();
    r
}

#[test]
fn request_queuefull_rolls_back_reserved_operation() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    w.send(target, Payload::Pulse(0)).unwrap();
    assert_eq!(
        w.request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(Error::QueueFull)
    );
    assert_eq!(w.snapshot().operation_pending, 0);
}

#[test]
fn request_claim_reply_and_take() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    let lease = w.claim(target).unwrap().unwrap();
    assert_eq!(lease.operation(), Some(id));
    assert!(
        w.complete_operation(id, target, Payload::CountSnapshot { count: 1, total: 2 })
            .unwrap()
    );
    assert!(matches!(
        w.operation_status(id),
        Ok(OperationStatus::Terminal(TerminalOutcome::Completed(_)))
    ));
    lease.finish(true);
    assert!(matches!(
        w.take_operation(id).unwrap(),
        Some(TerminalOutcome::Completed(_))
    ));
}

#[test]
fn expired_request_is_not_claimed() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_millis(100),
        )
        .unwrap();
    // Claim itself must enforce the deadline, even before maintenance runs.
    std::thread::sleep(Duration::from_millis(110));
    assert!(w.claim(target).unwrap().is_none());
    assert!(matches!(
        w.take_operation(id).unwrap(),
        Some(TerminalOutcome::TimedOut)
    ));
}

#[test]
fn pending_request_keeps_owner_drain_native_report_incomplete() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let deadline = Instant::now() + Duration::from_secs(60);
    let id = w
        .request(owner, target, Payload::Pulse(1), deadline)
        .unwrap();
    let report = w.request_drain(owner, deadline).unwrap();
    assert!(!report.native_done);
    assert!(w.complete_operation(id, target, Payload::Pulse(2)).unwrap());
    w.maintain(Instant::now());
    assert_eq!(w.state(owner).unwrap(), Lifecycle::Stopped);
    assert_eq!(w.snapshot().operation_pending, 0);
    w.stop(target).unwrap();
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}

#[test]
fn timeout_reply_has_one_terminal_winner() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    w.maintain(Instant::now() + Duration::from_secs(2));
    assert!(!w.complete_operation(id, target, Payload::Pulse(2)).unwrap());
    assert!(matches!(
        w.take_operation(id).unwrap(),
        Some(TerminalOutcome::TimedOut)
    ));
}

#[test]
fn target_stop_retains_targetstopped_terminal() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    w.stop(target).unwrap();
    assert!(matches!(
        w.operation_status(id),
        Ok(OperationStatus::Terminal(TerminalOutcome::TargetStopped))
    ));
    assert!(matches!(
        w.take_operation(id).unwrap(),
        Some(TerminalOutcome::TargetStopped)
    ));
}

#[test]
fn owner_stop_retires_operation_handles() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    w.stop(owner).unwrap();
    assert!(matches!(w.operation_status(id), Err(Error::StaleReference)));
    assert_eq!(w.snapshot().operation_pending, 0);
}

#[test]
fn oversized_result_becomes_bounded_failure() {
    let w = world();
    let owner = active(&w);
    let target = active(&w);
    let id = w
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    let large = Payload::Record {
        schema: 1,
        integers: vec![0; 100],
    };
    assert!(w.complete_operation(id, target, large).unwrap());
    assert!(matches!(
        w.take_operation(id).unwrap(),
        Some(TerminalOutcome::Failed {
            code: OperationFailure::ResultTooLarge,
            ..
        })
    ));
}

#[test]
fn request_world_boundary_is_rejected() {
    let w = world();
    let other = world();
    let owner = active(&w);
    let target = active(&w);
    let foreign = ActorRef {
        world: other.id(),
        ..target
    };
    assert_eq!(
        w.request(owner, foreign, Payload::Pulse(1), Instant::now()),
        Err(Error::CrossWorld)
    );
}
