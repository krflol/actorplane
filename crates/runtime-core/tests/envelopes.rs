use actorplane_core::{
    ActorRef, ComponentDescriptor, Config, EndpointKind, Error, MessageOptions, Payload,
    PayloadType, PortDirection, PortSpec, TerminalOutcome, TraceContext, World,
};
mod common;
use common::route;
use std::time::{Duration, Instant};

fn active(world: &World) -> ActorRef {
    let actor = world
        .allocate(EndpointKind::Native, None)
        .expect("allocation");
    world.activate(actor).expect("activation");
    actor
}

fn trace() -> TraceContext {
    TraceContext::new(
        [1; 16],
        [2; 8],
        true,
        vec![("tenant".into(), "test".into())],
    )
    .expect("trace context")
}

#[test]
fn admitted_envelope_preserves_source_schema_and_trace_metadata() {
    let world = World::new(Config::default()).expect("world");
    let source = active(&world);
    let target = active(&world);
    let deadline = Instant::now() + Duration::from_secs(10);
    let event = world
        .send_with(
            target,
            Payload::Pulse(7),
            MessageOptions {
                source: Some(source),
                deadline: Some(deadline),
                correlation_id: Some(11),
                causation_id: Some(10),
                trace: Some(trace()),
            },
        )
        .expect("send");
    let lease = world.claim(target).expect("claim").expect("delivery");
    let envelope = lease.envelope();
    assert_eq!(envelope.event_id, event);
    assert_eq!(envelope.source, Some(source));
    assert_eq!(envelope.destination, target);
    assert_eq!(envelope.owner, target);
    assert_eq!(envelope.schema.kind, actorplane_core::SchemaKind::Pulse);
    assert_eq!(envelope.schema.id, 0);
    assert_eq!(envelope.schema.version, 1);
    assert_eq!(envelope.deadline, Some(deadline));
    assert_eq!(envelope.correlation_id, Some(11));
    assert_eq!(envelope.causation_id, Some(10));
    assert_eq!(
        envelope.trace.as_ref().expect("trace").baggage()[0].0,
        "tenant"
    );
}

#[test]
fn trace_baggage_is_bounded_and_charged_in_retained_snapshot() {
    let world = World::new(Config {
        native_payload_budget: 2048,
        ..Config::default()
    })
    .expect("world");
    let target = active(&world);
    let context = trace();
    let expected = context.charged_bytes() + 8;
    world
        .send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                trace: Some(context),
                ..Default::default()
            },
        )
        .expect("send");
    assert!(world.snapshot().retained_payload_bytes >= expected);
    let lease = world.claim(target).expect("claim").expect("delivery");
    assert_eq!(
        lease
            .envelope()
            .trace
            .as_ref()
            .expect("trace")
            .charged_bytes(),
        expected - 8
    );
    lease.finish(true);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn expired_options_reject_before_admission_and_maintenance_expires_staged_work() {
    let world = World::new(Config::default()).expect("world");
    let target = active(&world);
    let past = Instant::now() - Duration::from_millis(1);
    assert_eq!(
        world.send_with(
            target,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(past),
                ..Default::default()
            }
        ),
        Err(Error::DeadlineExpired)
    );
    assert_eq!(world.snapshot().metrics.submitted, 1);

    let future = Instant::now() + Duration::from_secs(1);
    world
        .send_with(
            target,
            Payload::Pulse(9),
            MessageOptions {
                deadline: Some(future),
                ..Default::default()
            },
        )
        .expect("future queued event");
    world.maintain(future + Duration::from_secs(1));
    assert!(world.claim(target).expect("expired claim").is_none());
    assert_eq!(world.snapshot().metrics.expired, 1);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);

    let parent = world.allocate(EndpointKind::Native, None).expect("parent");
    let source = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("source");
    let output = world
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
        .expect("component")[0];
    let staged_deadline = Instant::now() + Duration::from_secs(1);
    world
        .publish_port_with(
            output,
            Payload::Pulse(2),
            MessageOptions {
                deadline: Some(staged_deadline),
                ..Default::default()
            },
        )
        .expect("stage future work");
    assert!(world.snapshot().retained_payload_bytes > 0);
    world.maintain(staged_deadline + Duration::from_secs(1));
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn fanout_allocates_unique_delivery_ids_and_preserves_correlation() {
    let world = World::new(Config::default()).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let first = world.allocate(EndpointKind::Native, None).expect("first");
    let second = world.allocate(EndpointKind::Native, None).expect("second");
    let output = world
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
        .expect("source component")[0];
    let input = |owner| {
        world
            .register_component(
                owner,
                ComponentDescriptor {
                    name: format!("target_{:?}", owner.slot),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "in".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::Pulse,
                    }],
                    interfaces: Vec::new(),
                },
            )
            .expect("target component")[0]
    };
    let first_input = input(first);
    let second_input = input(second);
    world.activate(source).expect("source activation");
    world.activate(first).expect("first activation");
    world.activate(second).expect("second activation");
    world.link(source, output, first_input).expect("first link");
    world
        .link(source, output, second_input)
        .expect("second link");
    let report = route(
        &world,
        world
            .publish_port_with(
                output,
                Payload::Pulse(3),
                MessageOptions {
                    correlation_id: Some(77),
                    ..Default::default()
                },
            )
            .expect("publish"),
    );
    assert_eq!(report.admitted, 2);
    let a = world.claim(first).expect("claim").expect("first");
    let b = world.claim(second).expect("claim").expect("second");
    assert_ne!(a.envelope().event_id, b.envelope().event_id);
    assert_eq!(a.envelope().correlation_id, Some(77));
    assert_eq!(b.envelope().correlation_id, Some(77));
}

#[test]
fn request_reply_propagates_context_to_completion_envelope() {
    let world = World::new(Config::default()).expect("world");
    let owner = active(&world);
    let target = active(&world);
    let request = world
        .request_with(
            owner,
            target,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(10),
            MessageOptions {
                correlation_id: Some(88),
                trace: Some(trace()),
                ..Default::default()
            },
        )
        .expect("request");
    let delivery = world
        .claim(target)
        .expect("claim")
        .expect("request delivery");
    let request_event = delivery.envelope().event_id;
    assert!(
        world
            .complete_operation(request, target, Payload::Pulse(2))
            .expect("reply")
    );
    delivery.finish(true);
    let outcome = world
        .take_operation(request)
        .expect("take")
        .expect("terminal");
    let held = match outcome {
        TerminalOutcome::Completed(held) => held,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let envelope = held.envelope().expect("reply envelope");
    assert_eq!(envelope.source, Some(target));
    assert_eq!(envelope.destination, owner);
    assert_eq!(envelope.correlation_id, Some(88));
    assert_eq!(envelope.causation_id, Some(request_event));
    assert_eq!(
        envelope.trace.as_ref().expect("reply trace").baggage(),
        trace().baggage()
    );
}

#[test]
fn invalid_source_world_and_stale_or_stopped_targets_reject_before_queueing() {
    let world = World::new(Config::default()).expect("world");
    let other = World::new(Config::default()).expect("other");
    let target = active(&world);
    let foreign = active(&other);
    assert_eq!(
        world.send_with(foreign, Payload::Pulse(1), MessageOptions::default()),
        Err(Error::CrossWorld)
    );
    world.stop(target).expect("stop target");
    assert_eq!(
        world.send_with(target, Payload::Pulse(1), MessageOptions::default()),
        Err(Error::ActorStopped)
    );
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
