use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, Error, InterfaceSpec, Lifecycle, MessageOptions,
    Payload, PayloadType, PortDirection, PortSpec, World,
};
use std::time::Duration;
mod common;
use common::route;

fn port(name: &str, direction: PortDirection, schema: PayloadType) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction,
        schema,
    }
}

fn contract() -> InterfaceSpec {
    InterfaceSpec {
        name: "test.Service".into(),
        version: 1,
        ports: vec![
            port("requests", PortDirection::Input, PayloadType::Pulse),
            port("events", PortDirection::Output, PayloadType::Pulse),
        ],
    }
}

fn install(world: &World) -> (actorplane_core::ActorRef, InterfaceSpec) {
    let service = world.allocate(EndpointKind::Native, None).unwrap();
    let contract = contract();
    world
        .register_component(
            service,
            ComponentDescriptor {
                name: "TestService".into(),
                version: 1,
                ports: contract.ports.clone(),
                interfaces: vec![contract.clone()],
            },
        )
        .unwrap();
    world.activate(service).unwrap();
    world.register_service("test", service, &contract).unwrap();
    (service, contract)
}

fn actor(world: &World, parent: Option<actorplane_core::ActorRef>) -> actorplane_core::ActorRef {
    let value = world.allocate(EndpointKind::Native, parent).unwrap();
    world.activate(value).unwrap();
    value
}

#[test]
fn service_requires_exact_native_contract_and_routes_requests_by_port() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder = actor(&world, None);
    let lease = world.acquire_service(holder, "test", &contract).unwrap();
    assert_eq!(lease.owner, holder);
    assert_eq!(lease.service, service);
    assert_eq!(world.services()[0].leases, 1);
    assert_eq!(world.service_leases().len(), 1);

    let wrong = InterfaceSpec {
        version: 2,
        ..contract.clone()
    };
    assert_eq!(
        world.acquire_service(holder, "test", &wrong),
        Err(Error::InterfaceMismatch)
    );
    assert_eq!(
        world.service_request(
            lease,
            "events",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::InvalidPort)
    );
    let operation = world
        .service_request(
            lease,
            "requests",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default(),
        )
        .unwrap();
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Pending(_))
    ));
    assert_eq!(world.release_service(lease), Ok(true));
    assert_eq!(world.release_service(lease), Ok(false));
    assert_eq!(
        world.service_request(
            lease,
            "requests",
            Payload::Pulse(2),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::StaleReference)
    );
}

#[test]
fn lease_and_service_bounds_roll_back_and_reuse_capacity() {
    let config = Config {
        max_leases_per_actor: 1,
        max_service_leases: 2,
        ..Default::default()
    };
    let world = World::new(config).unwrap();
    let (_service, contract) = install(&world);
    let holder = actor(&world, None);
    let first = world.acquire_service(holder, "test", &contract).unwrap();
    assert_eq!(
        world.acquire_service(holder, "test", &contract),
        Err(Error::LimitExceeded)
    );
    assert_eq!(world.release_service(first), Ok(true));
    let second = world.acquire_service(holder, "test", &contract).unwrap();
    assert_ne!(first.scope, second.scope);
    assert_eq!(world.service_leases().len(), 1);
    assert_eq!(world.release_service(second), Ok(true));
    assert_eq!(world.service_leases().len(), 0);
}

#[test]
fn stopping_service_revokes_cross_tree_leases_but_preserves_holders() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder_one = actor(&world, None);
    let holder_two = actor(&world, None);
    let first = world
        .acquire_service(holder_one, "test", &contract)
        .unwrap();
    let second = world
        .acquire_service(holder_two, "test", &contract)
        .unwrap();
    assert_eq!(world.service_leases().len(), 2);
    world.stop(service).unwrap();
    assert!(matches!(world.state(holder_one), Ok(Lifecycle::Active)));
    assert!(matches!(world.state(holder_two), Ok(Lifecycle::Active)));
    assert!(world.service_leases().is_empty());
    assert_eq!(world.release_service(first), Ok(false));
    assert_eq!(world.release_service(second), Ok(false));
    assert_eq!(
        world.acquire_service(holder_one, "test", &contract),
        Err(Error::NotFound)
    );
}

#[test]
fn lease_link_is_removed_when_scope_is_released() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder = actor(&world, None);
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    let target_port = world
        .register_component(
            target,
            ComponentDescriptor {
                name: "Target".into(),
                version: 1,
                ports: vec![port("input", PortDirection::Input, PayloadType::Pulse)],
                interfaces: vec![],
            },
        )
        .unwrap()[0];
    world.activate(target).unwrap();
    let lease = world.acquire_service(holder, "test", &contract).unwrap();
    let link = world.service_link(lease, "events", target_port).unwrap();
    assert!(link > 0);
    assert_eq!(world.release_service(lease), Ok(true));
    let report = route(&world, world.publish(service, Payload::Pulse(4)).unwrap());
    assert_eq!(report.admitted, 0);
    assert_eq!(report.rejected, 0);
}

#[test]
fn global_lease_cap_spans_multiple_services_and_rolls_back_holder_quota() {
    let config = Config {
        max_service_leases: 2,
        max_leases_per_actor: 2,
        ..Default::default()
    };
    let world = World::new(config).unwrap();
    let (first_service, contract) = install(&world);
    let second_service = world.allocate(EndpointKind::Native, None).unwrap();
    world
        .register_component(
            second_service,
            ComponentDescriptor {
                name: "SecondService".into(),
                version: 1,
                ports: contract.ports.clone(),
                interfaces: vec![contract.clone()],
            },
        )
        .unwrap();
    world.activate(second_service).unwrap();
    world
        .register_service("second", second_service, &contract)
        .unwrap();
    let holder_one = actor(&world, None);
    let holder_two = actor(&world, None);
    let _first = world
        .acquire_service(holder_one, "test", &contract)
        .unwrap();
    let _second = world
        .acquire_service(holder_two, "second", &contract)
        .unwrap();
    assert_eq!(
        world.acquire_service(holder_one, "second", &contract),
        Err(Error::LimitExceeded)
    );
    assert_eq!(world.service_leases().len(), 2);
    assert!(matches!(world.state(first_service), Ok(Lifecycle::Active)));
}

#[test]
fn release_race_fences_admitted_operations_and_callbacks() {
    for value in 0..64 {
        let world = World::new(Config::default()).unwrap();
        let (service, contract) = install(&world);
        let holder = actor(&world, None);
        let lease = world.acquire_service(holder, "test", &contract).unwrap();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let request_world = world.clone();
        let request_start = start.clone();
        let request_lease = lease;
        let request = std::thread::scope(|scope| {
            let request_thread = scope.spawn(move || {
                request_start.wait();
                request_world.service_request(
                    request_lease,
                    "requests",
                    Payload::Pulse(value),
                    request_world.now() + Duration::from_secs(1),
                    MessageOptions::default(),
                )
            });
            start.wait();
            let release = world.release_service(lease);
            (request_thread.join().unwrap(), release)
        });
        assert_eq!(request.1, Ok(true));
        match request.0 {
            Ok(operation) => {
                assert!(matches!(
                    world.operation_status(operation),
                    Err(Error::StaleReference)
                ));
                assert_eq!(
                    world.complete_operation(operation, service, Payload::Pulse(value)),
                    Ok(false)
                );
            }
            Err(error) => assert!(matches!(
                error,
                Error::ActorStopped | Error::StaleReference | Error::ActorNotReady
            )),
        }
        assert!(world.claim(service).unwrap().is_none());
        assert!(world.service_leases().is_empty());
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}

#[test]
fn holder_drain_preserves_admitted_request_but_rejects_new_work() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder = actor(&world, None);
    let other = actor(&world, None);
    let lease = world.acquire_service(holder, "test", &contract).unwrap();
    let admitted = world
        .service_request(
            lease,
            "requests",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default(),
        )
        .unwrap();
    let report = world
        .request_drain(holder, world.now() + Duration::from_secs(1))
        .unwrap();
    assert!(!report.timed_out);
    assert!(matches!(
        world.service_request(
            lease,
            "requests",
            Payload::Pulse(2),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::ActorStopped) | Err(Error::StaleReference)
    ));
    assert!(matches!(
        world.service_link(
            lease,
            "events",
            actorplane_core::PortRef {
                owner: service,
                index: 0
            }
        ),
        Err(Error::ActorStopped) | Err(Error::StaleReference)
    ));
    let delivery = world
        .claim(service)
        .unwrap()
        .expect("admitted service request");
    assert_eq!(delivery.operation(), Some(admitted));
    assert!(
        world
            .complete_operation(admitted, service, Payload::Pulse(3))
            .unwrap()
    );
    delivery.finish(true);
    assert!(matches!(
        world.take_operation(admitted).unwrap(),
        Some(actorplane_core::TerminalOutcome::Completed(_))
    ));
    world.maintain(world.now());
    assert!(matches!(world.state(holder), Ok(Lifecycle::Stopped)));
    assert!(matches!(
        world.state(lease.scope),
        Err(Error::StaleReference) | Ok(Lifecycle::Stopped)
    ));
    assert!(matches!(world.state(service), Ok(Lifecycle::Active)));
    assert!(matches!(world.state(other), Ok(Lifecycle::Active)));
}

#[test]
fn service_drain_rejects_new_leases_and_retires_after_admitted_completion() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder = actor(&world, None);
    let lease = world.acquire_service(holder, "test", &contract).unwrap();
    let admitted = world
        .service_request(
            lease,
            "requests",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default(),
        )
        .unwrap();
    let report = world
        .request_drain(service, world.now() + Duration::from_secs(1))
        .unwrap();
    assert!(!report.timed_out);
    assert!(matches!(
        world.acquire_service(holder, "test", &contract),
        Err(Error::ActorStopped) | Err(Error::NotFound)
    ));
    assert_eq!(
        world.service_link(lease, "events", world.port(service, "requests").unwrap()),
        Err(Error::ActorStopped)
    );
    assert!(matches!(
        world.service_request(
            lease,
            "requests",
            Payload::Pulse(2),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::ActorStopped) | Err(Error::StaleReference)
    ));
    let delivery = world
        .claim(service)
        .unwrap()
        .expect("admitted request during service drain");
    assert!(
        world
            .complete_operation(admitted, service, Payload::Pulse(4))
            .unwrap()
    );
    delivery.finish(true);
    assert!(matches!(
        world.take_operation(admitted).unwrap(),
        Some(actorplane_core::TerminalOutcome::Completed(_))
    ));
    world.maintain(world.now());
    assert_eq!(world.state(service), Ok(Lifecycle::Stopped));
    assert!(!world.stop(service).unwrap().timed_out);
    assert!(world.service_leases().is_empty());
    assert!(matches!(world.state(holder), Ok(Lifecycle::Active)));
}

#[test]
fn released_scope_generation_cannot_be_used_after_slot_reuse() {
    let world = World::new(Config::default()).unwrap();
    let (service, contract) = install(&world);
    let holder = actor(&world, None);
    let old = world.acquire_service(holder, "test", &contract).unwrap();
    assert_eq!(world.release_service(old), Ok(true));
    let replacement = world.acquire_service(holder, "test", &contract).unwrap();
    assert_ne!(old.scope, replacement.scope);
    assert_eq!(
        world.service_request(
            old,
            "requests",
            Payload::Pulse(1),
            world.now() + Duration::from_secs(1),
            MessageOptions::default()
        ),
        Err(Error::StaleReference)
    );
    let old_operation = world.request(
        old.scope,
        service,
        Payload::Pulse(2),
        world.now() + Duration::from_secs(1),
    );
    assert!(matches!(old_operation, Err(Error::StaleReference)));
    assert_eq!(world.release_service(replacement), Ok(true));
}

#[test]
fn actor_capacity_failure_does_not_leave_a_lease_record() {
    let config = Config {
        max_actors: 3,
        ..Default::default()
    };
    let world = World::new(config).unwrap();
    let (_service, contract) = install(&world);
    let holder = actor(&world, None);
    let blocker = actor(&world, None);
    assert_eq!(
        world.acquire_service(holder, "test", &contract),
        Err(Error::LimitExceeded)
    );
    assert!(world.service_leases().is_empty());
    // Free the blocker slot and prove the failed admission left the registry
    // and actor allocator reusable.
    world.stop(blocker).unwrap();
    let lease = world.acquire_service(holder, "test", &contract).unwrap();
    assert_eq!(world.service_leases().len(), 1);
    assert_eq!(world.release_service(lease), Ok(true));
}
