use actorplane_core::*;
use std::time::Duration;

fn spec(name: &str, direction: PortDirection, schema: PayloadType) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction,
        schema,
    }
}

fn contract() -> InterfaceSpec {
    InterfaceSpec {
        name: "tests.PublicService".into(),
        version: 1,
        ports: vec![
            spec("request", PortDirection::Input, PayloadType::Pulse),
            spec("update", PortDirection::Output, PayloadType::CountSnapshot),
        ],
    }
}

fn endpoint(world: &World, kind: EndpointKind, parent: Option<ActorRef>) -> ActorRef {
    let target = world.allocate(kind, parent).unwrap();
    let contract = contract();
    let mut ports = contract.ports.clone();
    ports.extend([
        spec(
            "private_request",
            PortDirection::Input,
            PayloadType::CountSnapshot,
        ),
        spec("private_update", PortDirection::Output, PayloadType::Pulse),
    ]);
    world
        .register_component(
            target,
            ComponentDescriptor {
                name: "tests.ServiceEndpoint".into(),
                version: 1,
                ports,
                interfaces: vec![contract],
            },
        )
        .unwrap();
    world.activate(target).unwrap();
    target
}

fn holder(world: &World) -> ActorRef {
    let value = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(value).unwrap();
    value
}

#[test]
fn lease_exposes_only_published_interface_ports_and_records_exact_envelope() {
    let world = World::new(Config::default()).unwrap();
    let target = endpoint(&world, EndpointKind::Native, None);
    world.register_service("test", target, &contract()).unwrap();
    let owner = holder(&world);
    let lease = world.acquire_service(owner, "test", &contract()).unwrap();
    let deadline = world.now() + Duration::from_secs(1);
    assert_eq!(
        world.service_request(
            lease,
            "private_request",
            Payload::CountSnapshot { count: 1, total: 2 },
            deadline,
            MessageOptions::default()
        ),
        Err(Error::InvalidPort)
    );
    assert_eq!(
        world.service_link(
            lease,
            "private_update",
            world.port(target, "request").unwrap()
        ),
        Err(Error::InvalidPort)
    );
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert_eq!(world.snapshot().operation_pending, 0);
    let operation = world
        .service_request(
            lease,
            "request",
            Payload::Pulse(9),
            deadline,
            MessageOptions {
                correlation_id: Some(31),
                ..Default::default()
            },
        )
        .unwrap();
    let delivery = world.claim(target).unwrap().unwrap();
    assert_eq!(delivery.operation(), Some(operation));
    assert_eq!(delivery.envelope().source, Some(lease.scope));
    assert_eq!(
        delivery.envelope().destination_port,
        Some(world.port(target, "request").unwrap())
    );
    assert_eq!(delivery.envelope().deadline, Some(deadline));
    assert_eq!(delivery.envelope().correlation_id, Some(31));
    assert!(
        world
            .complete_operation(operation, target, Payload::Pulse(9))
            .unwrap()
    );
    delivery.finish(true);
    drop(world.take_operation(operation).unwrap());
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn service_registration_rejects_python_child_duplicate_and_invalid_contract() {
    let world = World::new(Config {
        max_services: 1,
        ..Default::default()
    })
    .unwrap();
    let owner = holder(&world);
    let python = endpoint(&world, EndpointKind::Python, None);
    let child = endpoint(&world, EndpointKind::Native, Some(owner));
    for target in [python, child] {
        assert_eq!(
            world.register_service("test", target, &contract()),
            Err(Error::InterfaceMismatch)
        );
    }
    let target = endpoint(&world, EndpointKind::Native, None);
    let mut duplicate = contract();
    duplicate.ports[1] = duplicate.ports[0].clone();
    assert_eq!(
        world.register_service("test", target, &duplicate),
        Err(Error::InterfaceMismatch)
    );
    assert_eq!(
        world.register_service("", target, &contract()),
        Err(Error::InvalidConfig)
    );
    assert!(world.services().is_empty());
    world.register_service("test", target, &contract()).unwrap();
    assert_eq!(
        world.register_service("alias", target, &contract()),
        Err(Error::InvalidConfig)
    );
    let other = endpoint(&world, EndpointKind::Native, None);
    assert_eq!(
        world.register_service("test", other, &contract()),
        Err(Error::InvalidConfig)
    );
    assert_eq!(
        world.register_service("other", other, &contract()),
        Err(Error::LimitExceeded)
    );
    assert_eq!(world.state(other), Ok(Lifecycle::Active));
    world.close();
    world.finish_python(python).unwrap();
    assert!(world.services().is_empty());
}

#[test]
fn identity_checked_acquire_and_forged_leases_cannot_mutate_replacement() {
    let world = World::new(Config::default()).unwrap();
    let owner = holder(&world);
    let first = endpoint(&world, EndpointKind::Native, None);
    world.register_service("test", first, &contract()).unwrap();
    world.stop(first).unwrap();
    let replacement = endpoint(&world, EndpointKind::Native, None);
    assert_eq!(replacement.slot, first.slot);
    assert_ne!(replacement.generation, first.generation);
    world
        .register_service("test", replacement, &contract())
        .unwrap();
    assert_eq!(
        world.acquire_service_from(owner, "test", &contract(), first),
        Err(Error::StaleReference)
    );
    assert!(world.service_leases().is_empty());
    let lease = world
        .acquire_service_from(owner, "test", &contract(), replacement)
        .unwrap();
    let forged = ServiceLease {
        owner: replacement,
        ..lease
    };
    assert_eq!(world.release_service(forged), Err(Error::StaleReference));
    assert_eq!(
        world.service_request(
            forged,
            "request",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::StaleReference)
    );
    let other_world = World::new(Config::default()).unwrap();
    assert_eq!(other_world.release_service(lease), Err(Error::CrossWorld));
    assert_eq!(world.service_leases(), vec![lease]);
    assert!(world.release_service(lease).unwrap());
}

#[test]
fn reordered_interface_matches_and_duplicate_requirement_does_not_consume_scope() {
    let world = World::new(Config::default()).unwrap();
    let target = endpoint(&world, EndpointKind::Native, None);
    let owner = holder(&world);
    world.register_service("test", target, &contract()).unwrap();
    let mut reversed = contract();
    reversed.ports.reverse();
    let lease = world.acquire_service(owner, "test", &reversed).unwrap();
    assert!(world.release_service(lease).unwrap());
    let mut duplicate = contract();
    duplicate.ports[1] = duplicate.ports[0].clone();
    let before = world.snapshot().actors.len();
    assert_eq!(
        world.acquire_service(owner, "test", &duplicate),
        Err(Error::InterfaceMismatch)
    );
    assert_eq!(world.snapshot().actors.len(), before);
    assert!(world.service_leases().is_empty());
}

#[test]
fn service_leasing_itself_and_other_service_cannot_form_shutdown_ownership_cycle() {
    let world = World::new(Config::default()).unwrap();
    let first = endpoint(&world, EndpointKind::Native, None);
    let second = endpoint(&world, EndpointKind::Native, None);
    world.register_service("first", first, &contract()).unwrap();
    world
        .register_service("second", second, &contract())
        .unwrap();
    world.acquire_service(first, "first", &contract()).unwrap();
    world.acquire_service(first, "second", &contract()).unwrap();
    world.acquire_service(second, "first", &contract()).unwrap();
    world.stop(first).unwrap();
    assert!(world.service_leases().is_empty());
    assert_eq!(world.services().len(), 1);
    assert_eq!(world.state(second), Ok(Lifecycle::Active));
    let report = world.close();
    assert!(report.native_done && !report.timed_out);
    assert!(world.services().is_empty());
}

#[test]
fn stopping_holder_cancels_claimed_request_but_service_task_remains_accounted() {
    let world = World::new(Config::default()).unwrap();
    let target = endpoint(&world, EndpointKind::Native, None);
    world.register_service("test", target, &contract()).unwrap();
    let owner = holder(&world);
    let lease = world.acquire_service(owner, "test", &contract()).unwrap();
    let operation = world
        .service_request(
            lease,
            "request",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default(),
        )
        .unwrap();
    let delivery = world.claim(target).unwrap().unwrap();
    let task = world.track_task(target).unwrap();
    assert!(world.stop(owner).unwrap().native_done);
    assert!(
        !world
            .complete_operation(operation, target, Payload::Pulse(2))
            .unwrap()
    );
    let report = world.close();
    assert!(!report.native_done);
    assert!(world.snapshot().retained_payload_bytes > 0);
    delivery.finish(false);
    drop(task);
    assert!(world.close().native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
