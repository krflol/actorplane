use actorplane_core::*;
use std::{
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

fn world() -> (World, ActorRef, ActorRef) {
    let world = World::new(Config {
        max_actors: 2,
        max_operations: 1,
        ..Config::default()
    })
    .unwrap();
    let owner = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(owner).unwrap();
    world.activate(target).unwrap();
    (world, owner, target)
}

#[test]
fn completion_cancel_race_has_one_terminal_outcome() {
    for _ in 0..200 {
        let (w, owner, target) = world();
        let id = w
            .request(
                owner,
                target,
                Payload::Pulse(1),
                Instant::now() + Duration::from_secs(60),
            )
            .unwrap();
        let gate = Barrier::new(3);
        thread::scope(|scope| {
            let cancel = scope.spawn(|| {
                gate.wait();
                w.cancel_operation(id).unwrap()
            });
            let reply = scope.spawn(|| {
                gate.wait();
                w.complete_operation(id, target, Payload::Pulse(2)).unwrap()
            });
            gate.wait();
            assert_eq!(
                cancel.join().unwrap() as u8 + reply.join().unwrap() as u8,
                1
            );
        });
        assert!(matches!(
            w.operation_status(id).unwrap(),
            OperationStatus::Terminal(_)
        ));
        assert!(!w.complete_operation(id, target, Payload::Pulse(3)).unwrap());
        assert!(!w.cancel_operation(id).unwrap());
        assert!(w.take_operation(id).unwrap().is_some());
        w.close();
        let snapshot = w.snapshot();
        assert_eq!(snapshot.retained_payload_bytes, 0);
        assert_eq!(snapshot.operation_pending + snapshot.operation_retained, 0);
        assert_eq!(
            snapshot.metrics.operation_completed + snapshot.metrics.operation_cancelled,
            1
        );
    }
}

#[test]
fn late_completion_cannot_cross_actor_and_operation_generation_reuse() {
    for _ in 0..100 {
        let (w, owner, target) = world();
        let deadline = Instant::now() + Duration::from_secs(60);
        let old = w
            .request(owner, target, Payload::Pulse(1), deadline)
            .unwrap();
        w.stop(target).unwrap();
        assert!(matches!(
            w.take_operation(old).unwrap(),
            Some(TerminalOutcome::TargetStopped)
        ));
        let replacement = w.allocate(EndpointKind::Native, None).unwrap();
        w.activate(replacement).unwrap();
        assert_eq!(replacement.slot, target.slot);
        assert_ne!(replacement.generation, target.generation);
        let new = w
            .request(owner, replacement, Payload::Pulse(2), deadline)
            .unwrap();
        assert_eq!(new.slot, old.slot);
        assert_ne!(new.generation, old.generation);
        assert!(
            !w.complete_operation(old, target, Payload::Pulse(99))
                .unwrap()
        );
        assert!(
            !w.complete_operation(old, replacement, Payload::Pulse(99))
                .unwrap()
        );
        assert_eq!(
            w.complete_operation(new, target, Payload::Pulse(99)),
            Err(Error::StaleReference)
        );
        assert!(matches!(
            w.operation_status(new).unwrap(),
            OperationStatus::Pending(_)
        ));
        assert!(
            w.complete_operation(new, replacement, Payload::Pulse(3))
                .unwrap()
        );
        let Some(TerminalOutcome::Completed(result)) = w.take_operation(new).unwrap() else {
            panic!("new request lost")
        };
        assert!(matches!(result.payload(), Payload::Pulse(3)));
        drop(result);
        w.close();
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
}

#[test]
fn completion_cancel_target_stop_timeout_contenders_preserve_one_terminal() {
    for _ in 0..100 {
        let (w, owner, target) = world();
        let deadline = Instant::now() + Duration::from_secs(60);
        let id = w
            .request(owner, target, Payload::Pulse(1), deadline)
            .unwrap();
        let gate = Barrier::new(5);
        thread::scope(|scope| {
            let reply = scope.spawn(|| {
                gate.wait();
                w.complete_operation(id, target, Payload::Pulse(2)).unwrap()
            });
            let cancel = scope.spawn(|| {
                gate.wait();
                w.cancel_operation(id).unwrap()
            });
            let stop = scope.spawn(|| {
                gate.wait();
                w.stop(target).unwrap()
            });
            let expire = scope.spawn(|| {
                gate.wait();
                w.maintain(deadline)
            });
            gate.wait();
            reply.join().unwrap();
            cancel.join().unwrap();
            stop.join().unwrap();
            expire.join().unwrap();
        });
        let snapshot = w.snapshot();
        assert_eq!(snapshot.operation_pending, 0);
        assert_eq!(snapshot.operation_retained, 1);
        assert_eq!(
            snapshot.metrics.operation_completed
                + snapshot.metrics.operation_cancelled
                + snapshot.metrics.operation_timed_out,
            1
        );
        let before = format!("{:?}", w.operation_status(id).unwrap());
        assert!(!w.cancel_operation(id).unwrap());
        assert!(
            !w.complete_operation(id, target, Payload::Pulse(99))
                .unwrap()
        );
        assert_eq!(format!("{:?}", w.operation_status(id).unwrap()), before);
        assert!(w.take_operation(id).unwrap().is_some());
        w.close();
        let snapshot = w.snapshot();
        assert_eq!(snapshot.retained_payload_bytes, 0);
        assert_eq!(snapshot.operation_pending + snapshot.operation_retained, 0);
        assert_eq!(snapshot.metrics.admitted, snapshot.metrics.cancelled);
    }
}
