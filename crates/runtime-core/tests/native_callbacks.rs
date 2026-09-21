use actorplane_core::{
    Config, EndpointKind, FailureAction, FailureDetails, FailurePhase, Lifecycle, Payload, World,
};
use std::time::Duration;

fn failure() -> FailureDetails {
    FailureDetails::new(FailurePhase::Handler, "handler", "Error", Vec::new())
}

#[test]
fn startup_and_active_callback_eligibility_is_fenced() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    let startup_callback = world.claim_native_callback(native, true).unwrap().unwrap();
    assert!(world.claim_native_callback(native, true).unwrap().is_none());
    assert!(
        world
            .claim_native_callback(native, false)
            .unwrap()
            .is_none()
    );
    drop(startup_callback);
    world.activate(native).unwrap();
    assert!(
        world
            .claim_native_callback(native, false)
            .unwrap()
            .is_some()
    );

    let python = world.allocate(EndpointKind::Python, None).unwrap();
    world.activate(python).unwrap();
    assert!(
        world
            .claim_native_callback(python, false)
            .unwrap()
            .is_none()
    );
}

#[test]
fn delivery_and_control_callbacks_never_overlap() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(native).unwrap();
    world.send(native, Payload::Pulse(1)).unwrap();
    let delivery = world.claim(native).unwrap().unwrap();
    assert!(
        world
            .claim_native_callback(native, false)
            .unwrap()
            .is_none()
    );
    delivery.finish(true);
    let callback = world.claim_native_callback(native, false).unwrap().unwrap();
    assert!(world.claim(native).unwrap().is_none());
    drop(callback);
    assert!(world.claim(native).unwrap().is_none());
}

#[test]
fn callback_lease_keeps_stop_and_close_incomplete_until_drop() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(native).unwrap();
    let callback = world.claim_native_callback(native, false).unwrap().unwrap();
    let report = world.stop(native).unwrap();
    assert!(!report.native_done);
    assert_eq!(report.control_in_flight, 1);
    drop(callback);
    let report = world.close();
    assert!(report.native_done);
    assert_eq!(report.control_in_flight, 0);
    assert_eq!(world.state(native).unwrap(), Lifecycle::Stopped);
}

#[test]
fn fenced_actor_cannot_claim_callback_and_notifications_exclude_callback() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(native).unwrap();
    world.stop(native).unwrap();
    assert!(
        world
            .claim_native_callback(native, false)
            .unwrap()
            .is_none()
    );

    let supervisor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(supervisor).unwrap();
    let supervised = world
        .allocate(EndpointKind::Native, Some(supervisor))
        .unwrap();
    world.activate(supervised).unwrap();
    world
        .report_failure(
            supervised,
            None,
            None,
            None,
            failure(),
            FailureAction::Continue,
        )
        .unwrap();
    assert!(
        world
            .claim_native_callback(supervised, false)
            .unwrap()
            .is_none()
    );
    let notification = world.claim_failure(supervisor).unwrap().unwrap();
    assert!(
        world
            .claim_native_callback(supervisor, false)
            .unwrap()
            .is_none()
    );
    notification.finish(None).unwrap();
    assert!(
        world
            .claim_native_callback(supervised, false)
            .unwrap()
            .is_some()
    );
}

#[test]
fn callback_drop_releases_native_accounting_promptly() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(native).unwrap();
    let callback = world.claim_native_callback(native, false).unwrap().unwrap();
    world
        .request_drain(native, std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert!(
        world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == native)
            .unwrap()
            .control_in_flight
    );
    drop(callback);
    assert!(
        !world
            .snapshot()
            .actors
            .iter()
            .find(|a| a.reference == native)
            .unwrap()
            .control_in_flight
    );
    world.maintain(std::time::Instant::now());
    assert_eq!(world.state(native).unwrap(), Lifecycle::Stopped);
    assert!(
        world
            .claim_native_callback(native, false)
            .unwrap()
            .is_none()
    );
}

#[test]
fn drain_callback_is_only_eligible_during_quiescence() {
    let world = World::new(Config::default()).unwrap();
    let native = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(native).unwrap();
    assert!(world.claim_native_drain_callback(native).unwrap().is_none());
    let task = world.track_task(native).unwrap();
    world
        .request_drain(native, std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    let callback = world.claim_native_drain_callback(native).unwrap().unwrap();
    assert!(world.claim_native_drain_callback(native).unwrap().is_none());
    drop(callback);
    drop(task);
    world.maintain(std::time::Instant::now());
    assert_eq!(world.state(native).unwrap(), Lifecycle::Stopped);
    assert!(world.claim_native_drain_callback(native).unwrap().is_none());
}
