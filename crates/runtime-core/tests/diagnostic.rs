use actorplane_core::*;
use std::time::{Duration, Instant};

fn active(w: &World) -> ActorRef {
    let r = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(r).unwrap();
    r
}

#[test]
fn diagnostics_are_bounded_sequenced_and_report_gaps() {
    let w = World::new(Config {
        mailbox_capacity: 1,
        diagnostic_capacity: 2,
        ..Default::default()
    })
    .unwrap();
    let a = active(&w);
    w.send(a, Payload::Pulse(1)).unwrap();
    assert_eq!(w.send(a, Payload::Pulse(2)), Err(Error::QueueFull));
    assert_eq!(w.send(a, Payload::Pulse(3)), Err(Error::QueueFull));
    assert_eq!(w.send(a, Payload::Pulse(4)), Err(Error::QueueFull));
    assert!(matches!(w.diagnostics(0, 3), Err(Error::LimitExceeded)));
    let read = w.diagnostics(0, 2).unwrap();
    assert_eq!(read.entries.len(), 2);
    assert!(read.gap.is_some());
    assert!(
        read.entries
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert_eq!(read.dropped, 1);
}

#[test]
fn diagnostics_capture_operation_timeout_and_late_reply() {
    let w = World::new(Config {
        diagnostic_capacity: 8,
        ..Default::default()
    })
    .unwrap();
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
    let read = w.diagnostics(0, 8).unwrap();
    assert!(
        read.entries
            .iter()
            .any(|e| e.code == DiagnosticCode::OperationTimedOut)
    );
    assert!(
        read.entries
            .iter()
            .any(|e| e.code == DiagnosticCode::OperationLateResult)
    );
}

#[test]
fn diagnostics_capture_handler_failure_without_payload_text() {
    let w = World::new(Config {
        diagnostic_capacity: 8,
        ..Default::default()
    })
    .unwrap();
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
    lease.finish(false);
    let read = w.diagnostics(0, 8).unwrap();
    assert!(
        read.entries
            .iter()
            .any(|e| e.code == DiagnosticCode::HandlerFailed)
    );
    assert!(
        read.entries
            .iter()
            .all(|e| e.actor.is_some() || e.operation.is_some())
    );
}
