use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, Error, InterfaceSpec, Lifecycle, Payload,
    PayloadType, PortDirection, PortSpec, World,
};
mod common;
use common::route;

fn spec(name: &str, direction: PortDirection, schema: PayloadType) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction,
        schema,
    }
}

fn descriptor(name: &str, ports: Vec<PortSpec>) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.into(),
        version: 1,
        ports,
        interfaces: Vec::<InterfaceSpec>::new(),
    }
}

#[test]
fn typed_publish_stages_while_effectively_starting_and_flushes_fifo_on_root_activation() {
    let world = World::new(Config::default()).expect("world");
    let parent = world.allocate(EndpointKind::Native, None).expect("parent");
    let source = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("source");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    let output = world
        .register_component(
            source,
            descriptor(
                "source",
                vec![spec("out", PortDirection::Output, PayloadType::Pulse)],
            ),
        )
        .expect("source component")[0];
    let input = world
        .register_component(
            target,
            descriptor(
                "target",
                vec![spec("in", PortDirection::Input, PayloadType::Pulse)],
            ),
        )
        .expect("target component")[0];
    world.activate(target).expect("target active");
    world.link(source, output, input).expect("link");

    let first = world
        .publish_port(output, Payload::Pulse(1))
        .expect("first staged publish");
    let second = world
        .publish_port(output, Payload::Pulse(2))
        .expect("second staged publish");
    assert!(matches!(
        first.status(),
        actorplane_core::PublicationStatus::Staged
    ));
    assert!(matches!(
        second.status(),
        actorplane_core::PublicationStatus::Staged
    ));
    assert_eq!(world.state(source), Ok(Lifecycle::Starting));
    world.activate(source).expect("source activation");
    assert_eq!(world.state(source), Ok(Lifecycle::Active));
    assert_eq!(world.execution_state(source), Ok(Lifecycle::Starting));
    assert!(world.claim(target).unwrap().is_none());
    world.activate(parent).expect("root activation");
    let first_report = route(&world, first);
    let second_report = route(&world, second);
    assert_eq!(first_report.staged, 1);
    assert_eq!(second_report.staged, 1);
    let a = world
        .claim(target)
        .expect("first claim")
        .expect("first delivery");
    assert_eq!(a.payload(), &Payload::Pulse(1));
    a.finish(true);
    let b = world
        .claim(target)
        .expect("second claim")
        .expect("second delivery");
    assert_eq!(b.payload(), &Payload::Pulse(2));
}

#[test]
fn staging_respects_mailbox_cap_and_cancellation_releases_bytes() {
    let config = Config {
        mailbox_capacity: 1,
        ..Default::default()
    };
    let world = World::new(config).expect("world");
    let parent = world.allocate(EndpointKind::Native, None).expect("parent");
    let source = world
        .allocate(EndpointKind::Native, Some(parent))
        .expect("source");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    let output = world
        .register_component(
            source,
            descriptor(
                "source",
                vec![spec("out", PortDirection::Output, PayloadType::Pulse)],
            ),
        )
        .expect("source component")[0];
    let input = world
        .register_component(
            target,
            descriptor(
                "target",
                vec![spec("in", PortDirection::Input, PayloadType::Pulse)],
            ),
        )
        .expect("target component")[0];
    world.activate(target).expect("target active");
    world.link(source, output, input).expect("link");
    world
        .publish_port(output, Payload::Pulse(1))
        .expect("first stage");
    assert_eq!(
        world.publish_port(output, Payload::Pulse(2)),
        Err(Error::QueueFull)
    );
    assert_eq!(world.snapshot().retained_payload_bytes, 8);
    world.stop(parent).expect("cancel parent");
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert_eq!(world.snapshot().metrics.discarded, 1);
}

#[test]
fn stale_and_cross_world_sources_fail_before_staging() {
    let world = World::new(Config::default()).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let output = world
        .register_component(
            source,
            descriptor(
                "source",
                vec![spec("out", PortDirection::Output, PayloadType::Pulse)],
            ),
        )
        .expect("component")[0];
    world.stop(source).expect("stop source");
    let replacement = world
        .allocate(EndpointKind::Native, None)
        .expect("replacement");
    assert_ne!(replacement.generation, source.generation);
    assert_eq!(
        world.publish_port(output, Payload::Pulse(1)),
        Err(Error::StaleReference)
    );
    let other = World::new(Config::default()).expect("other world");
    assert_eq!(
        other.publish_port(output, Payload::Pulse(1)),
        Err(Error::CrossWorld)
    );
}

#[test]
fn direct_send_respects_typed_input_and_metadata_is_stable() {
    let world = World::new(Config::default()).expect("world");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    world
        .register_component(
            target,
            descriptor(
                "target",
                vec![spec("in", PortDirection::Input, PayloadType::CountSnapshot)],
            ),
        )
        .expect("component");
    let metadata = world
        .component(target)
        .expect("metadata")
        .expect("component metadata");
    assert_eq!(metadata.name, "target");
    assert_eq!(metadata.version, 1);
    world.activate(target).expect("target active");
    assert_eq!(
        world.send(target, Payload::Pulse(1)),
        Err(Error::SchemaMismatch)
    );
    assert!(
        world
            .send(target, Payload::CountSnapshot { count: 1, total: 1 })
            .is_ok()
    );
}
