use actorplane_core::{Clock, Config, EndpointKind, Error, MessageOptions, Payload, World};
use std::time::{Duration, Instant};

fn active(world: &World) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

#[test]
fn manual_clock_freezes_across_wall_clock_sleep() {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(world.now(), initial);
    assert_eq!(virtual_clock.current(), initial);
    assert_eq!(
        virtual_clock.advance(Duration::from_secs(2)).unwrap(),
        initial + Duration::from_secs(2)
    );
    assert_eq!(world.now(), initial + Duration::from_secs(2));
}

#[test]
fn virtual_clock_overflow_does_not_mutate_time() {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let before = virtual_clock.current();
    assert_eq!(
        virtual_clock.advance(Duration::MAX),
        Err(Error::LimitExceeded)
    );
    assert_eq!(virtual_clock.current(), before);
    let world = World::with_clock(Config::default(), clock).unwrap();
    assert_eq!(world.now(), before);
}

#[test]
fn exact_manual_deadline_applies_to_admission_claim_and_completion() {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let target = active(&world);
    let deadline = initial + Duration::from_secs(1);
    world
        .send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        )
        .unwrap();
    virtual_clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(9),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        ),
        Err(Error::DeadlineExpired)
    );
    assert!(world.claim(target).unwrap().is_none());
    assert_eq!(world.snapshot().metrics.expired, 1);

    let owner = active(&world);
    let request = world
        .request(
            owner,
            target,
            Payload::Pulse(2),
            deadline + Duration::from_secs(1),
        )
        .unwrap();
    virtual_clock.advance(Duration::from_secs(1)).unwrap();
    assert!(
        !world
            .complete_operation(request, target, Payload::Pulse(3))
            .unwrap()
    );
}

#[test]
fn staged_deadline_and_diagnostic_use_same_clock_epoch() {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let source = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    let port = world
        .register_component(
            source,
            actorplane_core::ComponentDescriptor {
                name: "source".into(),
                version: 1,
                ports: vec![actorplane_core::PortSpec {
                    name: "out".into(),
                    direction: actorplane_core::PortDirection::Output,
                    schema: actorplane_core::PayloadType::Pulse,
                }],
                interfaces: Vec::new(),
            },
        )
        .unwrap()[0];
    let deadline = initial + Duration::from_secs(3);
    world
        .publish_port_with(
            port,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(deadline),
                ..Default::default()
            },
        )
        .unwrap();
    virtual_clock.advance(Duration::from_secs(3)).unwrap();
    world.maintain(world.now());
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    let diagnostics = world.diagnostics(0, 16).unwrap();
    assert!(diagnostics.entries.iter().any(|entry| entry.code
        == actorplane_core::DiagnosticCode::DeadlineExpired
        && entry.elapsed_ns == 3_000_000_000));
}

#[test]
fn held_expiry_and_failure_timestamp_use_virtual_epoch() {
    let initial = Instant::now();
    let (clock, virtual_clock) = Clock::manual_at(initial);
    let world = World::with_clock(Config::default(), clock).unwrap();
    let target = active(&world);
    let held = world
        .hold_with(
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(initial + Duration::from_secs(2)),
                ..Default::default()
            },
        )
        .unwrap();
    virtual_clock.advance(Duration::from_secs(2)).unwrap();
    assert_eq!(world.send_held(target, held), Err(Error::DeadlineExpired));
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    virtual_clock.advance(Duration::from_secs(5)).unwrap();
    let details = actorplane_core::FailureDetails::new(
        actorplane_core::FailurePhase::Handler,
        "h",
        "E",
        Vec::new(),
    );
    world
        .report_failure(
            target,
            None,
            None,
            None,
            details,
            actorplane_core::FailureAction::Continue,
        )
        .unwrap();
    let diagnostics = world.diagnostics(0, 16).unwrap();
    assert!(diagnostics.entries.iter().any(|entry| {
        entry
            .failure
            .as_ref()
            .is_some_and(|failure| failure.elapsed_ns == 7_000_000_000)
    }));
}
