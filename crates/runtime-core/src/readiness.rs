use crate::{ActorRef, EndpointKind, Lifecycle, State, World};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included},
    time::Duration,
};

/// The bounded readiness queues exposed to the Python scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PythonReadyPhase {
    Start,
    Failure,
    Delivery,
    Cleanup,
}

/// Ordered, generation-safe readiness hints.
///
/// Entries are hints only: callers must revalidate lifecycle and claimability
/// under the World lock before invoking user code. Each actor has at most one
/// entry in each phase, represented by its monotonic readiness order.
#[derive(Debug, Default)]
pub(crate) struct PythonReadyIndex {
    start: BTreeMap<u64, ActorRef>,
    failure: BTreeMap<u64, ActorRef>,
    delivery: BTreeMap<u64, ActorRef>,
    cleanup: BTreeMap<u64, ActorRef>,
}

impl PythonReadyIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn set(
        &mut self,
        order: u64,
        actor: ActorRef,
        start: bool,
        failure: bool,
        delivery: bool,
        cleanup: bool,
    ) -> bool {
        let mut changed = false;
        changed |= set_entry(&mut self.start, order, actor, start);
        changed |= set_entry(&mut self.failure, order, actor, failure);
        changed |= set_entry(&mut self.delivery, order, actor, delivery);
        changed |= set_entry(&mut self.cleanup, order, actor, cleanup);
        changed
    }

    pub(crate) fn next(
        &self,
        phase: PythonReadyPhase,
        after: Option<u64>,
        through: u64,
    ) -> Option<(u64, ActorRef)> {
        let entries = self.entries(phase);
        match phase {
            PythonReadyPhase::Cleanup => {
                let upper = match after {
                    Some(0) => return None,
                    Some(after) => through.min(after - 1),
                    None => through,
                };
                entries
                    .range(..=upper)
                    .next_back()
                    .map(|(order, actor)| (*order, *actor))
            }
            PythonReadyPhase::Start | PythonReadyPhase::Failure | PythonReadyPhase::Delivery => {
                let after = after.unwrap_or(0);
                if after >= through {
                    return None;
                }
                entries
                    .range((Excluded(after), Included(through)))
                    .next()
                    .map(|(order, actor)| (*order, *actor))
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.start.is_empty()
            && self.failure.is_empty()
            && self.delivery.is_empty()
            && self.cleanup.is_empty()
    }

    fn entries(&self, phase: PythonReadyPhase) -> &BTreeMap<u64, ActorRef> {
        match phase {
            PythonReadyPhase::Start => &self.start,
            PythonReadyPhase::Failure => &self.failure,
            PythonReadyPhase::Delivery => &self.delivery,
            PythonReadyPhase::Cleanup => &self.cleanup,
        }
    }
}

fn set_entry(
    map: &mut BTreeMap<u64, ActorRef>,
    order: u64,
    actor: ActorRef,
    present: bool,
) -> bool {
    if present {
        map.insert(order, actor).is_none()
    } else {
        map.remove(&order);
        false
    }
}

impl World {
    /// Highest allocated actor order. A driver captures this once per phase to
    /// exclude registrations created by callbacks during that phase.
    pub fn python_ready_cutoff(&self) -> u64 {
        self.inner.state.lock().unwrap().next_ready_order - 1
    }

    /// Read at most one Python readiness hint without claiming work. Orders are
    /// immutable across an actor's lifetime and never reused with its slot.
    /// `through` is an inclusive upper cutoff in every phase; `after` is an
    /// exclusive cursor, ascending except for reverse-order cleanup.
    /// Delivery/control claims still validate lifecycle, queue fences, and time.
    pub fn python_ready_next(
        &self,
        phase: PythonReadyPhase,
        after: Option<u64>,
        through: u64,
    ) -> Option<(u64, ActorRef)> {
        self.inner
            .state
            .lock()
            .unwrap()
            .python_ready
            .next(phase, after, through)
    }

    /// Wait on the same state mutex used to publish readiness, avoiding a lost
    /// notification between the driver's empty query and its detached wait.
    pub fn wait_python_ready(&self, timeout: Duration) -> bool {
        let state = self.inner.state.lock().unwrap();
        let (state, _) = self
            .inner
            .python_ready_changed
            .wait_timeout_while(state, timeout, |state| state.python_ready.is_empty())
            .unwrap();
        !state.python_ready.is_empty()
    }

    pub(super) fn refresh_python_ready_locked(&self, reference: ActorRef, state: &mut State) {
        let Ok(actor) = self.check_ref(reference, state) else {
            return;
        };
        if actor.kind != EndpointKind::Python {
            return;
        }
        let order = actor.ready_order;
        let registered = !actor.python_done;
        let executable = registered
            && !actor.in_flight
            && !actor.control_in_flight
            && matches!(
                self.execution_state_locked(reference, state),
                Ok(Lifecycle::Active | Lifecycle::Quiescing)
            );
        let start = registered && actor.state == Lifecycle::Starting;
        let failure = executable && actor.pending_failures > 0;
        let delivery = executable && !actor.queue.is_empty();
        let cleanup = registered && matches!(actor.state, Lifecycle::Stopping | Lifecycle::Stopped);
        if state
            .python_ready
            .set(order, reference, start, failure, delivery, cleanup)
        {
            // Condvar notifications do not invoke application code. The waiter
            // can inspect the committed predicate only after this lock drops.
            self.inner.python_ready_changed.notify_all();
        }
    }

    pub(super) fn refresh_python_subtree_locked(&self, reference: ActorRef, state: &mut State) {
        let Ok(actor) = self.check_ref(reference, state) else {
            return;
        };
        let children = actor.children.clone();
        self.refresh_python_ready_locked(reference, state);
        for child in children {
            self.refresh_python_subtree_locked(child, state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Clock, ComponentDescriptor, Config, FailureAction, FailureDetails, FailurePhase,
        InterfaceSpec, MessageOptions, Payload, PayloadType, PortDirection, PortSpec,
    };
    use std::collections::HashMap;

    fn actor(slot: u32) -> ActorRef {
        ActorRef {
            world: 9,
            slot,
            generation: 1,
        }
    }

    #[test]
    fn phases_are_bounded_and_eagerly_removed() {
        let mut index = PythonReadyIndex::new();
        assert!(index.is_empty());
        assert!(index.set(1, actor(1), true, false, true, false));
        assert!(index.set(2, actor(2), false, true, false, true));
        assert_eq!(
            index.next(PythonReadyPhase::Start, None, 2),
            Some((1, actor(1)))
        );
        assert_eq!(
            index.next(PythonReadyPhase::Failure, None, 2),
            Some((2, actor(2)))
        );
        assert_eq!(
            index.next(PythonReadyPhase::Delivery, None, 2),
            Some((1, actor(1)))
        );
        assert_eq!(
            index.next(PythonReadyPhase::Cleanup, None, 2),
            Some((2, actor(2)))
        );
        assert!(!index.set(1, actor(1), false, false, false, false));
        assert!(!index.set(2, actor(2), false, false, false, false));
        assert!(index.is_empty());
    }

    #[test]
    fn ascending_and_cleanup_bounds_are_generation_ordered() {
        let mut index = PythonReadyIndex::new();
        index.set(10, actor(10), true, false, false, true);
        index.set(20, actor(20), true, false, false, true);
        index.set(30, actor(30), true, false, false, true);
        assert_eq!(
            index.next(PythonReadyPhase::Start, Some(10), 20),
            Some((20, actor(20)))
        );
        assert_eq!(index.next(PythonReadyPhase::Start, Some(20), 20), None);
        assert_eq!(
            index.next(PythonReadyPhase::Cleanup, Some(30), 30),
            Some((20, actor(20)))
        );
        assert_eq!(
            index.next(PythonReadyPhase::Cleanup, Some(20), 20),
            Some((10, actor(10)))
        );
        assert_eq!(index.next(PythonReadyPhase::Cleanup, Some(10), 10), None);
    }

    #[test]
    fn repeated_refresh_does_not_report_new_entry() {
        let mut index = PythonReadyIndex::new();
        assert!(index.set(7, actor(7), false, true, false, false));
        assert!(!index.set(7, actor(7), false, true, false, false));
        assert!(!index.set(7, actor(7), false, true, false, false));
        assert!(!index.set(7, actor(7), false, false, false, false));
    }

    fn assert_world_index_matches_actors(world: &World) {
        let records = {
            let state = world.inner.state.lock().unwrap();
            state
                .actors
                .iter()
                .flatten()
                .map(|actor| {
                    (
                        actor.reference,
                        actor.ready_order,
                        actor.kind,
                        actor.state,
                        actor.parent,
                        actor.python_done,
                        actor.in_flight,
                        actor.control_in_flight,
                        actor.pending_failures,
                        actor.notification.is_some(),
                        !actor.queue.is_empty(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let by_reference: HashMap<_, _> = records.iter().map(|record| (record.0, record)).collect();
        let effective = |reference: ActorRef| {
            let mut current = Some(reference);
            while let Some(value) = current {
                let record = by_reference[&value];
                if !matches!(record.3, Lifecycle::Active | Lifecycle::Quiescing) {
                    return record.3;
                }
                current = record.4;
            }
            Lifecycle::Active
        };
        let mut expected = PythonReadyIndex::new();
        for record in &records {
            let pending_failures = records
                .iter()
                .filter(|child| child.4 == Some(record.0) && child.9)
                .count();
            assert_eq!(record.8, pending_failures);
            if record.2 != EndpointKind::Python {
                continue;
            }
            let executable = !record.5
                && !record.6
                && !record.7
                && matches!(
                    effective(record.0),
                    Lifecycle::Active | Lifecycle::Quiescing
                );
            expected.set(
                record.1,
                record.0,
                !record.5 && record.3 == Lifecycle::Starting,
                executable && record.8 > 0,
                executable && record.10,
                !record.5 && matches!(record.3, Lifecycle::Stopping | Lifecycle::Stopped),
            );
        }
        let state = world.inner.state.lock().unwrap();
        assert_eq!(state.python_ready.start, expected.start);
        assert_eq!(state.python_ready.failure, expected.failure);
        assert_eq!(state.python_ready.delivery, expected.delivery);
        assert_eq!(state.python_ready.cleanup, expected.cleanup);
    }

    #[test]
    fn world_readiness_reconstructs_queue_failure_cleanup_and_service_lifecycle() {
        let initial = std::time::Instant::now();
        let (clock, virtual_clock) = Clock::manual_at(initial);
        let world = World::with_clock(Config::default(), clock).unwrap();
        let parent = world.allocate(EndpointKind::Python, None).unwrap();
        let child = world.allocate(EndpointKind::Python, Some(parent)).unwrap();
        let native_child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
        assert_world_index_matches_actors(&world);

        world.activate(parent).unwrap();
        assert_world_index_matches_actors(&world);
        world.activate(child).unwrap();
        world.activate(native_child).unwrap();
        assert_world_index_matches_actors(&world);

        world.send(child, Payload::Pulse(1)).unwrap();
        assert_world_index_matches_actors(&world);
        let delivery = world.claim(child).unwrap().unwrap();
        assert_world_index_matches_actors(&world);
        delivery.finish(true);
        assert_world_index_matches_actors(&world);

        world
            .send_with(
                child,
                Payload::Pulse(99),
                MessageOptions {
                    deadline: Some(initial + std::time::Duration::from_secs(1)),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_world_index_matches_actors(&world);
        virtual_clock
            .advance(std::time::Duration::from_secs(1))
            .unwrap();
        world.maintain(virtual_clock.current());
        assert_world_index_matches_actors(&world);

        let details = FailureDetails::new(FailurePhase::Handler, "readiness", "Error", vec![]);
        world
            .report_failure(
                native_child,
                Some(1),
                None,
                None,
                details.clone(),
                FailureAction::Continue,
            )
            .unwrap();
        world
            .report_failure(
                native_child,
                Some(2),
                None,
                None,
                details.clone(),
                FailureAction::Continue,
            )
            .unwrap();
        assert_world_index_matches_actors(&world);
        let failure = world.claim_failure(parent).unwrap().unwrap();
        assert_world_index_matches_actors(&world);
        world
            .report_failure(
                native_child,
                Some(3),
                None,
                None,
                details,
                FailureAction::Continue,
            )
            .unwrap();
        assert_world_index_matches_actors(&world);
        failure.finish(None).unwrap();
        assert_world_index_matches_actors(&world);
        let failure = world.claim_failure(parent).unwrap().unwrap();
        failure.finish(None).unwrap();
        assert_world_index_matches_actors(&world);

        let service = world.allocate(EndpointKind::Native, None).unwrap();
        let contract = InterfaceSpec {
            name: "ReadinessService".into(),
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
                    name: "ReadinessServiceComponent".into(),
                    version: 1,
                    ports: contract.ports.clone(),
                    interfaces: vec![contract.clone()],
                },
            )
            .unwrap();
        world.activate(service).unwrap();
        world
            .register_service("readiness", service, &contract)
            .unwrap();
        let lease = world
            .acquire_service(parent, "readiness", &contract)
            .unwrap();
        assert_world_index_matches_actors(&world);
        assert!(world.release_service(lease).unwrap());
        assert_world_index_matches_actors(&world);

        world.stop(child).unwrap();
        assert_world_index_matches_actors(&world);
        world.finish_python(child).unwrap();
        assert_world_index_matches_actors(&world);

        for value in 0..100 {
            let replacement = world.allocate(EndpointKind::Python, None).unwrap();
            world.activate(replacement).unwrap();
            world
                .send_with(
                    replacement,
                    Payload::Pulse(value),
                    MessageOptions {
                        deadline: Some(virtual_clock.current() + std::time::Duration::from_secs(1)),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_world_index_matches_actors(&world);
            let delivery = world.claim(replacement).unwrap().unwrap();
            assert_world_index_matches_actors(&world);
            delivery.finish(true);
            world.stop(replacement).unwrap();
            assert_world_index_matches_actors(&world);
            world.finish_python(replacement).unwrap();
            assert_world_index_matches_actors(&world);
        }

        let before = {
            let state = world.inner.state.lock().unwrap();
            (state.actors.len(), state.live_actors)
        };
        world.inner.state.lock().unwrap().next_ready_order = u64::MAX;
        assert_eq!(
            world.allocate(EndpointKind::Python, None),
            Err(crate::Error::LimitExceeded)
        );
        let state = world.inner.state.lock().unwrap();
        assert_eq!(state.actors.len(), before.0);
        assert_eq!(state.live_actors, before.1);
        assert_eq!(state.next_ready_order, u64::MAX);
        drop(state);

        world.close();
        world.finish_python(parent).unwrap();
        assert_world_index_matches_actors(&world);
        assert!(world.inner.state.lock().unwrap().python_ready.is_empty());
    }
}
