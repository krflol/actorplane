use actorplane_core::{
    Config, EndpointKind, Error, FailureAction, FailureDetails, FailurePhase, Lifecycle,
    OperationStatus, Payload, World,
};

fn pair(config: Config) -> (World, actorplane_core::ActorRef, actorplane_core::ActorRef) {
    let world = World::new(config).expect("valid config");
    let parent = world
        .allocate(EndpointKind::Native, None)
        .expect("parent allocation");
    world.activate(parent).expect("parent activation");
    let child = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("child allocation");
    world.activate(child).expect("child activation");
    (world, parent, child)
}

fn failure() -> FailureDetails {
    FailureDetails::new(FailurePhase::Handler, "handler", "Error", Vec::new())
}

#[test]
fn notifications_have_an_independent_bounded_control_slot_and_coalesce() {
    let config = Config {
        mailbox_capacity: 1,
        ..Config::default()
    };
    let (world, parent, child) = pair(config);
    world
        .send(parent, Payload::Pulse(0))
        .expect("fill business mailbox");

    world
        .report_failure(
            child,
            Some(7),
            None,
            None,
            failure(),
            FailureAction::Continue,
        )
        .expect("first failure report");
    world
        .report_failure(
            child,
            Some(8),
            None,
            None,
            failure(),
            FailureAction::Continue,
        )
        .expect("second failure report");
    assert_eq!(world.send(parent, Payload::Pulse(1)), Err(Error::QueueFull));

    let lease = world
        .claim_failure(parent)
        .expect("control claim")
        .expect("coalesced notification");
    assert_eq!(lease.coalesced(), 1);
    assert_eq!(lease.failure().actor, child);
    assert!(
        world
            .claim(parent)
            .expect("business claim while control lease")
            .is_none()
    );
    lease.finish(None).expect("finish control lease");
    assert!(
        !world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == parent)
            .unwrap()
            .control_in_flight
    );
}

#[test]
fn failure_operation_attribution_rejects_unrelated_pending_operation_atomically() {
    let (world, _owner, child) = pair(Config::default());
    let unrelated = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(unrelated).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(target).unwrap();
    let operation = world
        .request(
            unrelated,
            target,
            Payload::Pulse(1),
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(
        world.report_failure(
            child,
            None,
            None,
            Some(operation),
            failure(),
            FailureAction::StopWorld
        ),
        Err(Error::InvalidConfig)
    );
    assert!(matches!(
        world.operation_status(operation),
        Ok(OperationStatus::Pending(_))
    ));
    assert_eq!(world.state(child).unwrap(), Lifecycle::Active);
    assert_eq!(world.snapshot().metrics.failures, 0);
    assert!(world.diagnostics(0, 16).unwrap().entries.is_empty());
}

#[test]
fn failure_operation_attribution_accepts_owned_descendant() {
    let (world, parent, child) = pair(Config::default());
    let target = world.allocate(EndpointKind::Native, Some(child)).unwrap();
    world.activate(target).unwrap();
    let operation = world
        .request(
            parent,
            target,
            Payload::Pulse(1),
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    assert!(
        world
            .report_failure(
                child,
                None,
                None,
                Some(operation),
                failure(),
                FailureAction::Continue
            )
            .is_ok()
    );
    assert!(matches!(
        world.operation_status(operation).unwrap(),
        OperationStatus::Terminal(actorplane_core::TerminalOutcome::Failed {
            code: actorplane_core::OperationFailure::HandlerFailed,
            ..
        })
    ));
}

#[test]
fn expired_owned_operation_records_timeout_before_failure_completion() {
    let (world, parent, child) = pair(Config::default());
    let operation = world
        .request(
            parent,
            child,
            Payload::Pulse(1),
            std::time::Instant::now() + std::time::Duration::from_millis(100),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(110));
    world
        .report_failure(
            child,
            None,
            None,
            Some(operation),
            failure(),
            FailureAction::Continue,
        )
        .unwrap();
    assert!(matches!(
        world.operation_status(operation),
        Ok(OperationStatus::Terminal(
            actorplane_core::TerminalOutcome::TimedOut
        ))
    ));
    assert_eq!(world.snapshot().metrics.operation_timed_out, 1);
    assert_eq!(world.snapshot().metrics.operation_completed, 0);
    assert!(
        world
            .diagnostics(0, 16)
            .unwrap()
            .entries
            .iter()
            .any(
                |e| e.code == actorplane_core::DiagnosticCode::OperationTimedOut
                    && e.operation == Some(operation)
            )
    );
}

#[test]
fn late_failure_for_reused_operation_stops_its_source_without_touching_replacement() {
    let (world, parent, child) = pair(Config {
        max_operations: 1,
        ..Config::default()
    });
    let other = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(other).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let old = world
        .request(parent, child, Payload::Pulse(1), deadline)
        .unwrap();
    assert!(world.cancel_operation(old).unwrap());
    assert!(world.take_operation(old).unwrap().is_some());
    let replacement = world
        .request(parent, other, Payload::Pulse(2), deadline)
        .unwrap();
    assert_eq!(old.slot, replacement.slot);
    assert_ne!(old.generation, replacement.generation);
    let record = world
        .report_failure(
            child,
            None,
            None,
            Some(old),
            failure(),
            FailureAction::StopActor,
        )
        .unwrap();
    assert_eq!(record.operation, Some(old));
    assert_eq!(world.state(child).unwrap(), Lifecycle::Stopping);
    assert!(matches!(
        world.operation_status(replacement).unwrap(),
        OperationStatus::Pending(_)
    ));
    world
        .claim_failure(parent)
        .unwrap()
        .unwrap()
        .finish(None)
        .unwrap();
    assert_eq!(world.state(child).unwrap(), Lifecycle::Stopped);
}

#[test]
fn stopped_source_waits_for_parent_claim_before_slot_reuse() {
    let config = Config {
        max_actors: 2,
        ..Config::default()
    };
    let (world, parent, child) = pair(config);
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    world.stop(child).expect("stop source");
    assert_eq!(world.state(child), Ok(Lifecycle::Stopping));
    assert_eq!(
        world.allocate(EndpointKind::Native, Some(parent)),
        Err(Error::LimitExceeded)
    );

    let lease = world
        .claim_failure(parent)
        .expect("claim source failure")
        .expect("pending failure");
    lease.finish(None).expect("acknowledge source failure");
    assert_eq!(world.state(child), Ok(Lifecycle::Stopped));
    let replacement = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("reclaim source slot");
    assert_ne!(replacement.generation, child.generation);
}

#[test]
fn failure_stop_action_cannot_stop_a_replacement_generation() {
    let (world, parent, child) = pair(Config::default());
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    let lease = world
        .claim_failure(parent)
        .expect("claim")
        .expect("notification");
    world.stop(child).expect("stop source");
    let replacement = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("replacement");
    lease
        .finish(Some(FailureAction::StopActor))
        .expect("stale stop action is harmless");
    assert_eq!(world.state(replacement), Ok(Lifecycle::Starting));
}

#[test]
fn business_claim_blocks_control_claim_and_control_claim_blocks_business_claim() {
    let (world, parent, child) = pair(Config::default());
    world
        .send(parent, Payload::Pulse(3))
        .expect("business admission");
    let business = world
        .claim(parent)
        .expect("business claim")
        .expect("business lease");
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    assert!(
        world
            .claim_failure(parent)
            .expect("control claim while business")
            .is_none()
    );
    business.finish(false);

    let control = world
        .claim_failure(parent)
        .expect("control claim")
        .expect("notification");
    assert!(
        world
            .claim(child)
            .expect("business claim while control")
            .is_none()
    );
    control.finish(None).expect("control completion");
}

#[test]
fn failure_records_survive_diagnostic_capacity_zero_and_are_not_duplicated() {
    let config = Config {
        diagnostic_capacity: 0,
        ..Config::default()
    };
    let (world, parent, child) = pair(config);
    let record = world
        .report_failure(
            child,
            Some(99),
            Some(4),
            None,
            failure(),
            FailureAction::Continue,
        )
        .expect("failure record");
    assert_eq!(record.actor, child);
    assert_eq!(record.event_id, Some(99));
    assert!(
        world
            .diagnostics(0, 0)
            .expect("diagnostic read")
            .entries
            .is_empty()
    );
    let lease = world
        .claim_failure(parent)
        .expect("claim")
        .expect("notification");
    lease.finish(None).expect("acknowledge");
    assert_eq!(world.snapshot().metrics.failures, 1);
}

#[test]
fn stop_world_failure_closes_all_ingress() {
    let (world, parent, child) = pair(Config::default());
    world
        .report_failure(child, None, None, None, failure(), FailureAction::StopWorld)
        .expect("stop-world failure");
    assert_eq!(
        world.allocate(EndpointKind::Native, Some(parent)),
        Err(Error::ActorStopped)
    );
    assert_eq!(
        world.send(parent, Payload::Pulse(1)),
        Err(Error::ActorStopped)
    );
}

#[test]
fn cloned_task_lease_keeps_one_registration_until_last_drop() {
    let (world, parent, child) = pair(Config::default());
    let task = world.track_task(child).unwrap();
    let continuation = task.clone();
    assert_eq!(
        world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == child)
            .unwrap()
            .native_tasks,
        1
    );
    world.stop(parent).unwrap();
    drop(task);
    assert_eq!(world.state(child).unwrap(), Lifecycle::Stopping);
    assert!(!world.close().native_done);
    drop(continuation);
    assert_eq!(world.state(child).unwrap(), Lifecycle::Stopped);
    assert_eq!(world.state(parent).unwrap(), Lifecycle::Stopped);
    assert!(world.close().native_done);
}

#[test]
fn supervisor_claim_racing_parent_stop_releases_all_control_reservations() {
    use std::{sync::Barrier, thread};
    for _ in 0..100 {
        let (world, parent, child) = pair(Config {
            max_actors: 2,
            ..Config::default()
        });
        world
            .report_failure(child, None, None, None, failure(), FailureAction::StopActor)
            .unwrap();
        let gate = Barrier::new(3);
        let claim = thread::scope(|scope| {
            let claim = scope.spawn(|| {
                gate.wait();
                world.claim_failure(parent).unwrap()
            });
            let stop = scope.spawn(|| {
                gate.wait();
                world.stop(parent).unwrap()
            });
            gate.wait();
            stop.join().unwrap();
            claim.join().unwrap()
        });
        if let Some(claim) = claim {
            assert!(!world.close().native_done);
            claim.finish(None).unwrap();
        }
        assert!(world.close().native_done);
        assert_eq!(world.state(parent).unwrap(), Lifecycle::Stopped);
        assert_eq!(world.state(child).unwrap(), Lifecycle::Stopped);
        assert!(
            world
                .snapshot()
                .actors
                .iter()
                .all(|actor| !actor.control_in_flight && !actor.failure_pending)
        );
    }
}

#[test]
fn stopping_parent_discards_unclaimed_child_notifications_and_reclaims_slots() {
    let config = Config {
        max_actors: 2,
        ..Config::default()
    };
    let (world, parent, child) = pair(config);
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    world.stop(parent).expect("stop parent");
    assert_eq!(world.state(parent), Ok(Lifecycle::Stopped));
    assert_eq!(world.state(child), Ok(Lifecycle::Stopped));
    world
        .allocate(EndpointKind::Native, None)
        .expect("stopped subtree releases its slots");
}

#[test]
fn close_reports_control_failure_lease_until_it_is_finished() {
    let (world, parent, child) = pair(Config::default());
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    let lease = world
        .claim_failure(parent)
        .expect("claim")
        .expect("notification");
    let report = world.close();
    assert!(
        !report.native_done,
        "control lease must keep native cleanup pending"
    );
    drop(lease);
    let report = world.close();
    assert!(
        report.native_done,
        "dropping control lease releases native cleanup"
    );
}

#[test]
fn stopping_source_with_pending_notification_reports_native_not_done() {
    let (world, _parent, child) = pair(Config::default());
    world
        .report_failure(child, None, None, None, failure(), FailureAction::Continue)
        .expect("failure report");
    let report = world.stop(child).expect("stop source");
    assert!(
        !report.native_done,
        "pending notification retains source cleanup"
    );
}
