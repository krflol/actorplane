use super::ActorRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

/// Exact deadline reference counts for queued and staged entries of one actor.
pub(crate) struct ActorDeadlines {
    counts: BTreeMap<Instant, usize>,
}

impl ActorDeadlines {
    pub(crate) fn new() -> Self {
        Self {
            counts: BTreeMap::new(),
        }
    }

    pub(crate) fn add(&mut self, deadline: Option<Instant>) {
        let Some(deadline) = deadline else { return };
        *self.counts.entry(deadline).or_default() += 1;
    }

    pub(crate) fn remove(&mut self, deadline: Option<Instant>) {
        let Some(deadline) = deadline else { return };
        let count = self
            .counts
            .get_mut(&deadline)
            .expect("deadline reference exists");
        *count -= 1;
        if *count == 0 {
            self.counts.remove(&deadline);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.counts.clear();
    }

    pub(crate) fn first(&self) -> Option<Instant> {
        self.counts.keys().next().copied()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.counts.len()
    }
}

/// One eager, replacement-safe deadline entry per actor.
pub(crate) struct DeadlineIndex {
    actors: HashMap<ActorRef, Instant>,
    ordered: BTreeSet<(Instant, u64, u32, u64)>,
}

impl DeadlineIndex {
    pub(crate) fn new() -> Self {
        Self {
            actors: HashMap::new(),
            ordered: BTreeSet::new(),
        }
    }

    pub(crate) fn set(&mut self, actor: ActorRef, deadline: Option<Instant>) {
        if deadline.is_some_and(|deadline| self.actors.get(&actor) == Some(&deadline)) {
            return;
        }
        self.remove(actor);
        let Some(deadline) = deadline else { return };
        self.actors.insert(actor, deadline);
        self.ordered
            .insert((deadline, actor.world, actor.slot, actor.generation));
    }

    pub(crate) fn remove(&mut self, actor: ActorRef) {
        let Some(deadline) = self.actors.remove(&actor) else {
            return;
        };
        self.ordered
            .remove(&(deadline, actor.world, actor.slot, actor.generation));
    }

    pub(crate) fn take_due(&mut self, now: Instant) -> Vec<ActorRef> {
        let due: Vec<_> = self
            .ordered
            .range(..=(now, u64::MAX, u32::MAX, u64::MAX))
            .copied()
            .collect();
        let mut actors = Vec::with_capacity(due.len());
        for (deadline, world, slot, generation) in due {
            self.ordered.remove(&(deadline, world, slot, generation));
            let actor = ActorRef {
                world,
                slot,
                generation,
            };
            self.actors.remove(&actor);
            actors.push(actor);
        }
        actors.sort_by_key(|actor| (actor.slot, actor.generation, actor.world));
        actors
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        debug_assert_eq!(self.actors.len(), self.ordered.len());
        self.actors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Clock, ComponentDescriptor, Config, EndpointKind, InterfaceSpec, Lifecycle, MessageOptions,
        Payload, PayloadType, PortDirection, PortSpec, World,
    };

    fn actor(slot: u32, generation: u64) -> ActorRef {
        ActorRef {
            world: 7,
            slot,
            generation,
        }
    }

    #[test]
    fn actor_deadlines_refcount_duplicate_entries_and_none() {
        let start = Instant::now();
        let later = start + std::time::Duration::from_secs(1);
        let mut deadlines = ActorDeadlines::new();
        deadlines.add(None);
        deadlines.add(Some(later));
        deadlines.add(Some(later));
        assert_eq!(deadlines.first(), Some(later));
        assert_eq!(deadlines.len(), 1);
        deadlines.remove(Some(later));
        assert_eq!(deadlines.first(), Some(later));
        deadlines.remove(Some(later));
        assert_eq!(deadlines.first(), None);
        deadlines.clear();
        assert_eq!(deadlines.len(), 0);
    }

    #[test]
    fn deadline_replacements_keep_one_entry_and_generations_independent() {
        let start = Instant::now();
        let early = start + std::time::Duration::from_secs(1);
        let late = start + std::time::Duration::from_secs(2);
        let mut index = DeadlineIndex::new();
        let old = actor(3, 1);
        let replacement = actor(3, 2);
        index.set(old, Some(early));
        index.set(old, Some(late));
        index.set(replacement, Some(early));
        index.set(actor(4, 1), Some(early));
        assert_eq!(index.len(), 3);
        assert!(
            index
                .take_due(start + std::time::Duration::from_millis(1500))
                .contains(&replacement)
        );
        assert!(
            !index
                .take_due(start + std::time::Duration::from_millis(1500))
                .contains(&old)
        );
        assert_eq!(index.len(), 1);
        assert_eq!(index.take_due(late), vec![old]);
        assert_eq!(index.len(), 0);
        index.remove(old);
    }

    #[test]
    fn due_order_is_slot_generation_world_and_cardinality_is_exact() {
        let start = Instant::now();
        let mut index = DeadlineIndex::new();
        for slot in 0..1000 {
            index.set(actor(slot, 1), Some(start));
            index.set(
                actor(slot, 1),
                Some(start + std::time::Duration::from_secs(1)),
            );
            index.set(actor(slot, 1), Some(start));
        }
        assert_eq!(index.len(), 1000);
        let due = index.take_due(start);
        assert_eq!(due.len(), 1000);
        assert!(due.windows(2).all(|pair| {
            (pair[0].slot, pair[0].generation, pair[0].world)
                <= (pair[1].slot, pair[1].generation, pair[1].world)
        }));
        assert_eq!(index.len(), 0);
    }

    #[cfg(test)]
    fn assert_world_indexes(world: &World) {
        let state = world.inner.state.lock().unwrap();
        let mut expected_events = HashMap::new();
        let mut expected_staged = BTreeMap::new();
        let mut live = 0usize;
        let mut expected_drains = BTreeMap::new();
        for actor in state.actors.iter().flatten() {
            if actor.state != Lifecycle::Stopped {
                live += 1;
            }
            let mut counts = BTreeMap::new();
            for delivery in &actor.queue {
                if let Some(deadline) = delivery.envelope.deadline {
                    *counts.entry(deadline).or_insert(0) += 1;
                }
            }
            for (_, stored, _) in &actor.staged {
                if let Some(deadline) = stored.options.deadline {
                    *counts.entry(deadline).or_insert(0) += 1;
                }
            }
            assert_eq!(actor.deadlines.counts, counts);
            if let Some(deadline) = counts.keys().next().copied() {
                expected_events.insert(actor.reference, deadline);
            }
            if !actor.staged.is_empty() {
                expected_staged.insert(actor.reference.slot, actor.reference);
            }
            if actor.state == Lifecycle::Quiescing && actor.drain_root == Some(actor.reference) {
                expected_drains.insert(actor.reference.slot, actor.reference);
            }
        }
        assert_eq!(state.live_actors, live);
        assert_eq!(state.event_deadlines.actors, expected_events);
        let expected_order: BTreeSet<_> = expected_events
            .iter()
            .map(|(actor, deadline)| (*deadline, actor.world, actor.slot, actor.generation))
            .collect();
        assert_eq!(state.event_deadlines.ordered, expected_order);
        assert_eq!(state.staged_actors, expected_staged);
        assert_eq!(state.drain_roots, expected_drains);
        assert_eq!(
            state.event_deadlines.actors.len(),
            state.event_deadlines.ordered.len()
        );
    }

    #[test]
    fn world_deadline_indexes_track_staged_claimed_failed_and_reused_work() {
        let config = Config {
            mailbox_capacity: 1,
            ..Config::default()
        };
        let initial = Instant::now();
        let (clock, virtual_clock) = Clock::manual_at(initial);
        let world = World::with_clock(config, clock).unwrap();
        let source = world.allocate(EndpointKind::Native, None).unwrap();
        let target = world.allocate(EndpointKind::Native, None).unwrap();
        world
            .register_component(
                source,
                ComponentDescriptor {
                    name: "ExpirySource".into(),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "out".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    }],
                    interfaces: vec![],
                },
            )
            .unwrap();
        world
            .register_component(
                target,
                ComponentDescriptor {
                    name: "ExpiryTarget".into(),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "in".into(),
                        direction: PortDirection::Input,
                        schema: PayloadType::Pulse,
                    }],
                    interfaces: vec![],
                },
            )
            .unwrap();
        world.activate(target).unwrap();
        let source_port = world.port(source, "out").unwrap();
        let target_port = world.port(target, "in").unwrap();
        world.link(source, source_port, target_port).unwrap();
        let deadline = world.now() + std::time::Duration::from_secs(60);
        world
            .publish_port_with(
                source_port,
                Payload::Pulse(1),
                MessageOptions {
                    deadline: Some(deadline),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_world_indexes(&world);
        world.activate(source).unwrap();
        assert_world_indexes(&world);
        world.route_batch();
        assert_world_indexes(&world);
        let delivery = world.claim(target).unwrap().unwrap();
        assert_world_indexes(&world);
        delivery.finish(true);
        assert_world_indexes(&world);
        assert!(
            world
                .send_with(
                    target,
                    Payload::Pulse(2),
                    MessageOptions {
                        deadline: Some(deadline),
                        ..Default::default()
                    }
                )
                .is_ok()
        );
        assert_world_indexes(&world);
        assert!(
            world
                .send_with(
                    target,
                    Payload::Pulse(3),
                    MessageOptions {
                        deadline: Some(deadline),
                        ..Default::default()
                    },
                )
                .is_err()
        );
        assert_world_indexes(&world);
        virtual_clock
            .advance(std::time::Duration::from_secs(60))
            .unwrap();
        world.maintain(virtual_clock.current());
        assert_world_indexes(&world);

        // A replacement source can leave staged work behind; stopping it must
        // remove that work and its deadline references before slot reuse.
        world.stop(source).unwrap();
        let staged_source = world.allocate(EndpointKind::Native, None).unwrap();
        let staged_port = world
            .register_component(
                staged_source,
                ComponentDescriptor {
                    name: "ExpiryStagedSource".into(),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "out".into(),
                        direction: PortDirection::Output,
                        schema: PayloadType::Pulse,
                    }],
                    interfaces: vec![],
                },
            )
            .unwrap()[0];
        world.link(staged_source, staged_port, target_port).unwrap();
        world
            .publish_port_with(
                staged_port,
                Payload::Pulse(4),
                MessageOptions {
                    deadline: Some(initial + std::time::Duration::from_secs(120)),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_world_indexes(&world);
        world.stop(staged_source).unwrap();
        assert_world_indexes(&world);

        world.stop(target).unwrap();
        assert_world_indexes(&world);

        // Drain indexes must retain nested ownership until the child task is
        // released, while a service lease contributes ordinary live actors.
        let parent = world.allocate(EndpointKind::Native, None).unwrap();
        let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
        world.activate(parent).unwrap();
        world.activate(child).unwrap();
        let task = world.track_task(child).unwrap();
        let now = virtual_clock.current();
        world
            .request_drain(child, now + std::time::Duration::from_secs(1))
            .unwrap();
        world
            .request_drain(parent, now + std::time::Duration::from_secs(10))
            .unwrap();
        assert_world_indexes(&world);
        virtual_clock
            .advance(std::time::Duration::from_secs(1))
            .unwrap();
        world.maintain(virtual_clock.current());
        assert_world_indexes(&world);
        drop(task);
        world.maintain(virtual_clock.current());
        assert_world_indexes(&world);

        let service = world.allocate(EndpointKind::Native, None).unwrap();
        let service_contract = InterfaceSpec {
            name: "ExpiryService".into(),
            version: 1,
            ports: vec![
                PortSpec {
                    name: "requests".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                },
                PortSpec {
                    name: "events".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::Pulse,
                },
            ],
        };
        world
            .register_component(
                service,
                ComponentDescriptor {
                    name: "ExpiryServiceComponent".into(),
                    version: 1,
                    ports: service_contract.ports.clone(),
                    interfaces: vec![service_contract.clone()],
                },
            )
            .unwrap();
        world.activate(service).unwrap();
        world
            .register_service("expiry", service, &service_contract)
            .unwrap();
        let holder = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(holder).unwrap();
        let lease = world
            .acquire_service(holder, "expiry", &service_contract)
            .unwrap();
        assert_world_indexes(&world);
        assert!(world.release_service(lease).unwrap());
        assert_world_indexes(&world);

        for _ in 0..100 {
            let actor = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(actor).unwrap();
            world
                .send_with(
                    actor,
                    Payload::Pulse(5),
                    MessageOptions {
                        deadline: Some(virtual_clock.current() + std::time::Duration::from_secs(1)),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_world_indexes(&world);
            let delivery = world.claim(actor).unwrap().unwrap();
            assert_world_indexes(&world);
            delivery.finish(true);
            world.stop(actor).unwrap();
            assert_world_indexes(&world);
        }
        assert!(world.close().native_done);
        assert_world_indexes(&world);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}
