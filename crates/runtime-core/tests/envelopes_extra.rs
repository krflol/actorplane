use actorplane_core::{
    ActorRef, ComponentDescriptor, Config, DiagnosticCode, EndpointKind, Error, MessageOptions,
    Payload, PayloadType, PortDirection, PortSpec, TerminalOutcome, TraceContext, World,
};
use std::time::{Duration, Instant};
mod common;
use common::route;

fn active(world: &World) -> ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

#[test]
fn routed_envelope_records_exact_destination_port() {
    let world = World::new(Config::default()).unwrap();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    let output = world
        .register_component(
            target,
            ComponentDescriptor {
                name: "target".into(),
                version: 1,
                ports: vec![
                    PortSpec {
                        name: "pulse".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::Pulse,
                    },
                    PortSpec {
                        name: "count".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::CountSnapshot,
                    },
                ],
                interfaces: Vec::new(),
            },
        )
        .unwrap();
    let source_port = world
        .register_component(
            source,
            ComponentDescriptor {
                name: "source".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "out".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::Pulse,
                }],
                interfaces: Vec::new(),
            },
        )
        .unwrap()[0];
    world.activate(source).unwrap();
    world.activate(target).unwrap();
    world.link(source, source_port, output[0]).unwrap();
    route(
        &world,
        world.publish_port(source_port, Payload::Pulse(1)).unwrap(),
    );
    let lease = world.claim(target).unwrap().unwrap();
    assert_eq!(lease.envelope().destination_port, Some(output[0]));
    lease.finish(true);
}

#[test]
fn source_validation_rejects_foreign_stale_and_stopped_sources() {
    let world = World::new(Config::default()).unwrap();
    let other = World::new(Config::default()).unwrap();
    let target = active(&world);
    let foreign = active(&other);
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                source: Some(foreign),
                ..Default::default()
            }
        ),
        Err(Error::CrossWorld)
    );
    let source = active(&world);
    world.stop(source).unwrap();
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                source: Some(source),
                ..Default::default()
            }
        ),
        Err(Error::ActorStopped)
    );
    let stale = ActorRef {
        generation: source.generation.saturating_add(1),
        ..source
    };
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                source: Some(stale),
                ..Default::default()
            }
        ),
        Err(Error::StaleReference)
    );
}

#[test]
fn queued_delivery_keeps_historical_source_after_source_stops() {
    let world = World::new(Config::default()).unwrap();
    let source = active(&world);
    let target = active(&world);
    world
        .send_with(
            target,
            Payload::Pulse(4),
            MessageOptions {
                source: Some(source),
                ..Default::default()
            },
        )
        .unwrap();
    world.stop(source).unwrap();
    let lease = world.claim(target).unwrap().unwrap();
    assert_eq!(lease.envelope().source, Some(source));
    lease.finish(true);
}

#[test]
fn rejected_trace_budget_does_not_retain_payload_bytes() {
    let world = World::new(Config {
        native_payload_budget: 8,
        ..Config::default()
    })
    .unwrap();
    let target = active(&world);
    let trace = TraceContext::new(
        [1; 16],
        [2; 8],
        true,
        vec![("tenant".into(), "native".into())],
    )
    .unwrap();
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                trace: Some(trace),
                ..Default::default()
            }
        ),
        Err(Error::BudgetExceeded)
    );
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn default_request_correlation_retains_context_until_take() {
    let world = World::new(Config::default()).unwrap();
    let owner = active(&world);
    let target = active(&world);
    let id = world
        .request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    let delivery = world.claim(target).unwrap().unwrap();
    assert_eq!(
        delivery.envelope().correlation_id,
        Some(delivery.event_id())
    );
    assert!(
        world
            .complete_operation(id, target, Payload::Pulse(2))
            .unwrap()
    );
    delivery.finish(true);
    assert!(world.snapshot().retained_payload_bytes > 0);
    assert!(matches!(
        world.take_operation(id).unwrap(),
        Some(TerminalOutcome::Completed(_))
    ));
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn expired_request_rolls_back_operation_reservation() {
    let world = World::new(Config::default()).unwrap();
    let owner = active(&world);
    let target = active(&world);
    assert_eq!(
        world.request(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() - Duration::from_millis(1)
        ),
        Err(Error::DeadlineExpired)
    );
    assert_eq!(world.snapshot().operation_pending, 0);
}

#[test]
fn claimed_delivery_survives_deadline_until_handler_finishes() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world);
    let deadline = Instant::now() + Duration::from_secs(10);
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
    let lease = world.claim(target).unwrap().unwrap();
    world.maintain(deadline + Duration::from_secs(1));
    assert_eq!(world.snapshot().retained_payload_bytes, 8);
    lease.finish(true);
    assert_eq!(world.snapshot().metrics.completed, 1);
    assert_eq!(world.snapshot().metrics.expired, 0);
}

#[test]
fn expiry_diagnostic_has_bounded_event_identity() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world);
    let deadline = Instant::now() + Duration::from_secs(10);
    let event = world
        .send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(deadline),
                correlation_id: Some(51),
                causation_id: Some(50),
                ..Default::default()
            },
        )
        .unwrap();
    world.maintain(deadline + Duration::from_millis(1));
    let diagnostics = world.diagnostics(0, 16).unwrap();
    let entry = diagnostics
        .entries
        .iter()
        .find(|entry| entry.code == DiagnosticCode::DeadlineExpired)
        .unwrap();
    assert_eq!(entry.event_id, Some(event));
    assert_eq!(entry.correlation_id, Some(51));
    assert_eq!(entry.causation_id, Some(50));
}
