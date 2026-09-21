use actorplane_core::{
    Config, EndpointKind, Error, FailureAction, FailureDetails, FailurePhase, Lifecycle, Payload,
    TerminalOutcome, World,
};
use std::{
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

struct Signal(mpsc::Sender<()>);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn active(world: &World) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

fn pending_observer(
    world: &World,
    actor: actorplane_core::ActorRef,
) -> (u64, Waker, mpsc::Receiver<()>) {
    let observed = world.activity(actor).unwrap();
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(tx)));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        world.poll_activity(actor, observed, &mut context),
        Poll::Pending
    ));
    (observed, waker, rx)
}

#[test]
fn failure_terminalizes_external_owner_and_wakes_only_dependencies() {
    let world = World::new(Config::default()).unwrap();
    let owner = active(&world);
    let target = active(&world);
    let unrelated = active(&world);
    let operation = world
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    let (observed, owner_waker, owner_ready) = pending_observer(&world, owner);
    let (_, _unrelated_waker, unrelated_ready) = pending_observer(&world, unrelated);
    let details = FailureDetails::new(FailurePhase::Handler, "activity-test", "Failed", vec![]);
    world
        .report_failure(
            target,
            Some(7),
            None,
            Some(operation),
            details,
            FailureAction::Continue,
        )
        .unwrap();

    assert!(owner_ready.recv_timeout(Duration::from_secs(1)).is_ok());
    assert!(unrelated_ready.try_recv().is_err());
    let mut context = Context::from_waker(&owner_waker);
    assert!(matches!(
        world.poll_activity(owner, observed, &mut context),
        Poll::Ready(Ok(version)) if version != observed
    ));
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Terminal(
            TerminalOutcome::Failed { .. }
        ))
    ));
    assert_eq!(world.state(owner), Ok(Lifecycle::Active));
    assert_eq!(world.state(unrelated), Ok(Lifecycle::Active));
}

#[test]
fn continue_failure_notification_and_native_callback_release_wake_observers() {
    let world = World::new(Config::default()).unwrap();
    let supervisor = active(&world);
    let child = world
        .allocate(EndpointKind::Native, Some(supervisor))
        .unwrap();
    world.activate(child).unwrap();
    let (supervisor_version, supervisor_waker, supervisor_ready) =
        pending_observer(&world, supervisor);
    let details = FailureDetails::new(FailurePhase::Handler, "activity-test", "Failed", vec![]);
    world
        .report_failure(child, None, None, None, details, FailureAction::Continue)
        .unwrap();
    assert!(
        supervisor_ready
            .recv_timeout(Duration::from_secs(1))
            .is_ok()
    );
    let mut context = Context::from_waker(&supervisor_waker);
    assert!(matches!(
        world.poll_activity(supervisor, supervisor_version, &mut context),
        Poll::Ready(Ok(version)) if version != supervisor_version
    ));
    let failure = world.claim_failure(supervisor).unwrap().unwrap();
    failure.finish(None).unwrap();

    let native = active(&world);
    let (callback_version, callback_waker, callback_ready) = pending_observer(&world, native);
    let callback = world.claim_native_callback(native, false).unwrap().unwrap();
    assert!(callback_ready.recv_timeout(Duration::from_secs(1)).is_ok());
    let (release_version, release_waker, release_ready) = pending_observer(&world, native);
    drop(callback);
    assert!(release_ready.recv_timeout(Duration::from_secs(1)).is_ok());
    assert!(matches!(
        world.poll_activity(native, release_version, &mut Context::from_waker(&release_waker)),
        Poll::Ready(Ok(version)) if version != release_version
    ));
    let mut context = Context::from_waker(&callback_waker);
    assert!(matches!(
        world.poll_activity(native, callback_version, &mut context),
        Poll::Ready(Ok(version)) if version != callback_version
    ));

    let (_, _, root_ready) = pending_observer(&world, native);
    world
        .report_failure(
            native,
            None,
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "root", "Failed", vec![]),
            FailureAction::Continue,
        )
        .unwrap();
    assert!(root_ready.recv_timeout(Duration::from_secs(1)).is_ok());
}

#[test]
fn failed_operation_rejects_cross_world_reference_without_waking_observer() {
    let world = World::new(Config::default()).unwrap();
    let other = World::new(Config::default()).unwrap();
    let target = active(&world);
    let foreign = active(&other);
    let (observed, waker, ready) = pending_observer(&world, target);
    let details = FailureDetails::new(FailurePhase::Handler, "activity-test", "Failed", vec![]);
    assert_eq!(
        world.report_failure(
            target,
            None,
            None,
            Some(actorplane_core::OperationId {
                world: other.id(),
                slot: foreign.slot,
                generation: foreign.generation,
            }),
            details,
            FailureAction::Continue,
        ),
        Err(Error::CrossWorld)
    );
    assert!(ready.try_recv().is_err());
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        world.poll_activity(target, observed, &mut context),
        Poll::Pending
    ));
}
