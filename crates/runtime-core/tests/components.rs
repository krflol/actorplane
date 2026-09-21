use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, Error, InterfaceSpec, Payload, PayloadType,
    PortDirection, PortSpec, World,
};
mod common;
use common::route;

fn component(
    name: &str,
    ports: Vec<PortSpec>,
    interfaces: Vec<InterfaceSpec>,
) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.into(),
        version: 1,
        ports,
        interfaces,
    }
}

fn port(name: &str, direction: PortDirection, schema: PayloadType) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction,
        schema,
    }
}

#[test]
fn link_rejects_incompatible_direction_and_schema() {
    let world = World::new(Config::default()).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    let outputs = world
        .register_component(
            source,
            component(
                "source",
                vec![port("out", PortDirection::Output, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("source component");
    let inputs = world
        .register_component(
            target,
            component(
                "target",
                vec![port("in", PortDirection::Input, PayloadType::CountSnapshot)],
                vec![],
            ),
        )
        .expect("target component");
    world.activate(source).expect("source activation");
    world.activate(target).expect("target activation");
    assert_eq!(
        world.link(source, inputs[0], outputs[0]),
        Err(Error::InvalidPort)
    );
    assert_eq!(
        world.link(source, outputs[0], inputs[0]),
        Err(Error::SchemaMismatch)
    );
}

#[test]
fn interface_identity_collision_does_not_partially_register_component() {
    let world = World::new(Config::default()).expect("world");
    let first = world.allocate(EndpointKind::Native, None).expect("first");
    let second = world.allocate(EndpointKind::Native, None).expect("second");
    let first_ports = vec![port("in", PortDirection::Input, PayloadType::Pulse)];
    let first_interface = InterfaceSpec {
        name: "monitor.Input".into(),
        version: 1,
        ports: first_ports.clone(),
    };
    world
        .register_component(
            first,
            component("first", first_ports, vec![first_interface]),
        )
        .expect("first registration");
    let conflicting_ports = vec![port("in", PortDirection::Input, PayloadType::CountSnapshot)];
    let conflicting_interface = InterfaceSpec {
        name: "monitor.Input".into(),
        version: 1,
        ports: conflicting_ports.clone(),
    };
    assert_eq!(
        world.register_component(
            second,
            component("second", conflicting_ports, vec![conflicting_interface])
        ),
        Err(Error::InterfaceMismatch)
    );
    assert!(world.component(second).expect("component lookup").is_none());
}

#[test]
fn source_port_selection_routes_only_matching_links() {
    let world = World::new(Config::default()).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let pulse_target = world
        .allocate(EndpointKind::Native, None)
        .expect("pulse target");
    let count_target = world
        .allocate(EndpointKind::Native, None)
        .expect("count target");
    let source_ports = world
        .register_component(
            source,
            component(
                "source",
                vec![
                    port("pulse", PortDirection::Output, PayloadType::Pulse),
                    port("count", PortDirection::Output, PayloadType::CountSnapshot),
                ],
                vec![],
            ),
        )
        .expect("source component");
    let pulse_input = world
        .register_component(
            pulse_target,
            component(
                "pulse",
                vec![port("in", PortDirection::Input, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("pulse component")[0];
    let count_input = world
        .register_component(
            count_target,
            component(
                "count",
                vec![port("in", PortDirection::Input, PayloadType::CountSnapshot)],
                vec![],
            ),
        )
        .expect("count component")[0];
    for reference in [source, pulse_target, count_target] {
        world.activate(reference).expect("activation");
    }
    world
        .link(source, source_ports[0], pulse_input)
        .expect("pulse link");
    world
        .link(source, source_ports[1], count_input)
        .expect("count link");
    let pulse_report = route(
        &world,
        world
            .publish_port(source_ports[0], Payload::Pulse(4))
            .expect("pulse publish"),
    );
    assert_eq!(pulse_report.admitted, 1);
    assert_eq!(pulse_report.rejected, 0);
    assert!(world.claim(pulse_target).expect("pulse claim").is_some());
    assert!(world.claim(count_target).expect("count claim").is_none());
}

#[test]
fn owner_stop_invalidates_queued_claim_and_cross_world_or_stale_ports_fail() {
    let world = World::new(Config::default()).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    let output = world
        .register_component(
            source,
            component(
                "source",
                vec![port("out", PortDirection::Output, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("source component")[0];
    let input = world
        .register_component(
            target,
            component(
                "target",
                vec![port("in", PortDirection::Input, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("target component")[0];
    world.activate(source).expect("source activation");
    world.activate(target).expect("target activation");
    world.link(source, output, input).expect("link");
    route(
        &world,
        world
            .publish_port(output, Payload::Pulse(1))
            .expect("publish"),
    );
    world.stop(source).expect("owner stop");
    assert!(
        world
            .claim(target)
            .expect("claim after owner stop")
            .is_none()
    );

    let other = World::new(Config::default()).expect("other world");
    assert_eq!(
        other.publish_port(output, Payload::Pulse(2)),
        Err(Error::CrossWorld)
    );
    let replacement = world
        .allocate(EndpointKind::Native, None)
        .expect("replacement");
    assert_ne!(replacement.generation, source.generation);
    assert_eq!(
        world.publish_port(output, Payload::Pulse(3)),
        Err(Error::StaleReference)
    );
}

#[test]
fn saturated_fanout_rejects_one_target_without_stalling_healthy_target() {
    let config = Config {
        mailbox_capacity: 1,
        ..Default::default()
    };
    let world = World::new(config).expect("world");
    let source = world.allocate(EndpointKind::Native, None).expect("source");
    let full = world
        .allocate(EndpointKind::Native, None)
        .expect("full target");
    let healthy = world
        .allocate(EndpointKind::Native, None)
        .expect("healthy target");
    let output = world
        .register_component(
            source,
            component(
                "source",
                vec![port("out", PortDirection::Output, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("source component")[0];
    let full_input = world
        .register_component(
            full,
            component(
                "full",
                vec![port("in", PortDirection::Input, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("full component")[0];
    let healthy_input = world
        .register_component(
            healthy,
            component(
                "healthy",
                vec![port("in", PortDirection::Input, PayloadType::Pulse)],
                vec![],
            ),
        )
        .expect("healthy component")[0];
    for reference in [source, full, healthy] {
        world.activate(reference).expect("activation");
    }
    world.link(source, output, full_input).expect("full link");
    world
        .link(source, output, healthy_input)
        .expect("healthy link");
    world
        .send_port(full_input, Payload::Pulse(0))
        .expect("fill full target");
    let report = route(
        &world,
        world
            .publish_port(output, Payload::Pulse(1))
            .expect("fanout"),
    );
    assert_eq!(report.admitted, 1);
    assert_eq!(report.rejected, 1);
    assert!(world.claim(healthy).expect("healthy claim").is_some());
}
