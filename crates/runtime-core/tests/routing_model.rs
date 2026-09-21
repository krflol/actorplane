//! Deterministic model exercise for every route index, including actor reuse.
use actorplane_core::*;
use std::collections::BTreeMap;
mod common;
use common::route;

#[derive(Clone, Copy)]
struct Route {
    owner: ActorRef,
    source: ActorRef,
    target: ActorRef,
    port: Option<u16>,
}

fn actor(world: &World) -> ActorRef {
    let reference = world.allocate(EndpointKind::Native, None).unwrap();
    world
        .register_component(
            reference,
            ComponentDescriptor {
                name: "tests.Indexed".into(),
                version: 1,
                interfaces: vec![],
                ports: vec![
                    PortSpec {
                        name: "input".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::Pulse,
                    },
                    PortSpec {
                        name: "first".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    },
                    PortSpec {
                        name: "second".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    },
                ],
            },
        )
        .unwrap();
    world.activate(reference).unwrap();
    reference
}

fn random(state: &mut u64) -> usize {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*state >> 16) as usize
}

#[test]
fn route_indexes_match_independent_model_through_bounded_mutations_and_reuse() {
    for seed in [11, 337, 1019] {
        let mut rng = seed;
        let world = World::new(Config {
            max_actors: 12,
            max_subscriptions: 16,
            ..Default::default()
        })
        .unwrap();
        let mut actors: Vec<_> = (0..12).map(|_| actor(&world)).collect();
        let mut model = BTreeMap::<u64, Route>::new();
        for step in 0..3000 {
            let source_index = random(&mut rng) % actors.len();
            let source = actors[source_index];
            let target = actors[random(&mut rng) % actors.len()];
            let owner = actors[random(&mut rng) % actors.len()];
            let port = match random(&mut rng) % 3 {
                0 => None,
                index => Some(index as u16),
            };
            match random(&mut rng) % 6 {
                0..=2 => {
                    let result = if let Some(index) = port {
                        world.link(
                            owner,
                            PortRef {
                                owner: source,
                                index,
                            },
                            PortRef {
                                owner: target,
                                index: 0,
                            },
                        )
                    } else {
                        world.subscribe(source, target)
                    };
                    let duplicate = model.values().any(|route| {
                        route.source == source
                            && route.target == target
                            && (port.is_none() || route.port == port)
                    });
                    if model.len() == 16 {
                        assert_eq!(result, Err(Error::LimitExceeded));
                    } else if duplicate {
                        assert_eq!(result, Err(Error::DuplicateSubscription));
                    } else {
                        let id = result.unwrap();
                        model.insert(
                            id,
                            Route {
                                source,
                                target,
                                owner: if port.is_some() { owner } else { source },
                                port,
                            },
                        );
                    }
                }
                3 => {
                    if !model.is_empty() {
                        let id = *model.keys().nth(random(&mut rng) % model.len()).unwrap();
                        world.unsubscribe(id).unwrap();
                        model.remove(&id);
                        assert_eq!(world.unsubscribe(id), Err(Error::NotFound));
                    }
                }
                4 => {
                    world.stop(source).unwrap();
                    model.retain(|_, route| {
                        route.source != source && route.target != source && route.owner != source
                    });
                    actors[source_index] = actor(&world);
                    assert_eq!(actors[source_index].slot, source.slot);
                    assert_ne!(actors[source_index].generation, source.generation);
                    assert_eq!(
                        world.publish(source, Payload::Pulse(1)),
                        Err(Error::StaleReference)
                    );
                }
                _ => {
                    let expected: Vec<_> = model
                        .values()
                        .filter(|route| route.source == source && route.port == port)
                        .map(|route| route.target)
                        .collect();
                    let ticket = if let Some(index) = port {
                        world.publish_port(
                            PortRef {
                                owner: source,
                                index,
                            },
                            Payload::Pulse(step),
                        )
                    } else {
                        world.publish(source, Payload::Pulse(step))
                    }
                    .unwrap();
                    let report = route(&world, ticket);
                    assert_eq!(report.admitted, expected.len(), "seed={seed} step={step}");
                    assert_eq!(report.rejected, 0);
                    let mut received = Vec::new();
                    for target in &actors {
                        while let Some(delivery) = world.claim(*target).unwrap() {
                            assert_eq!(delivery.payload(), &Payload::Pulse(step));
                            assert_eq!(
                                delivery.envelope().dispatcher,
                                port.map(|index| PortRef {
                                    owner: source,
                                    index
                                })
                            );
                            received.push((delivery.event_id(), *target));
                            delivery.finish(true);
                        }
                    }
                    received.sort_by_key(|(id, _)| *id);
                    assert_eq!(
                        received
                            .into_iter()
                            .map(|(_, target)| target)
                            .collect::<Vec<_>>(),
                        expected,
                        "admission order seed={seed} step={step}"
                    );
                    assert_eq!(world.snapshot().retained_payload_bytes, 0);
                }
            }
            assert_eq!(
                world.snapshot().subscriptions,
                model.len(),
                "seed={seed} step={step}"
            );
            let expected_links: Vec<_> = model
                .iter()
                .filter_map(|(id, route)| route.port.map(|_| *id))
                .collect();
            assert_eq!(
                world
                    .links()
                    .into_iter()
                    .map(|link| link.id)
                    .collect::<Vec<_>>(),
                expected_links
            );
            for target in &actors {
                assert_eq!(
                    world.upstream_done(*target).unwrap(),
                    !model.values().any(|route| route.target == *target),
                    "upstream seed={seed} step={step}"
                );
            }
        }
        assert!(world.close().native_done);
        assert_eq!(world.snapshot().subscriptions, 0);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}
