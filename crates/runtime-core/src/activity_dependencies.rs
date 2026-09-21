//! Activity follows ownership and explicit route/operation dependencies.
//! Versions change under State's lock; arbitrary Wakers run only after it is released.
use crate::{ActorRef, Error, Lifecycle, OperationId, State, TerminalOutcome, World};
use std::time::Instant;

impl World {
    fn mark_activity_ancestors_locked(
        &self,
        reference: ActorRef,
        state: &State,
        registry: &mut crate::activity::ActivityRegistry,
    ) {
        let mut next = Some(reference);
        while let Some(reference) = next {
            let Ok(actor) = self.check_ref(reference, state) else {
                break;
            };
            registry.mark(reference);
            next = actor.parent;
        }
    }

    pub(crate) fn mark_activity_dependents_locked(&self, reference: ActorRef, state: &State) {
        let mut registry = self.inner.activity.lock().unwrap();
        let mut next = Some(reference);
        while let Some(reference) = next {
            let Ok(actor) = self.check_ref(reference, state) else {
                break;
            };
            registry.mark(reference);
            next = actor.parent;
            // A draining consumer checks the producer's queue, delivery, task count,
            // and lifecycle. An Active producer cannot satisfy upstream_done yet.
            if matches!(
                actor.state,
                Lifecycle::Quiescing | Lifecycle::Stopping | Lifecycle::Stopped
            ) {
                for id in state.subs.source_actor_ids(reference) {
                    let route = state.subs.get(&id).expect("indexed route");
                    if self
                        .check_ref(route.target, state)
                        .is_ok_and(|target| target.state == Lifecycle::Quiescing)
                    {
                        self.mark_activity_ancestors_locked(route.target, state, &mut registry);
                    }
                }
            }
        }
    }

    pub(crate) fn mark_activity_tree_locked(&self, reference: ActorRef, state: &State) {
        let Ok(actor) = self.check_ref(reference, state) else {
            return;
        };
        self.mark_activity_dependents_locked(reference, state);
        for child in &actor.children {
            self.mark_activity_tree_locked(*child, state);
        }
    }

    pub(crate) fn mark_route_activity_locked(&self, route: &crate::routing::Sub, state: &State) {
        self.mark_activity_dependents_locked(route.owner, state);
        if route.source != route.owner {
            self.mark_activity_dependents_locked(route.source, state);
        }
        if route.target != route.owner && route.target != route.source {
            self.mark_activity_dependents_locked(route.target, state);
        }
    }

    pub(crate) fn mark_operation_activity_locked(
        &self,
        owner: ActorRef,
        target: Option<ActorRef>,
        state: &State,
    ) {
        self.mark_activity_dependents_locked(owner, state);
        if let Some(target) = target
            && target != owner
        {
            self.mark_activity_dependents_locked(target, state);
        }
    }

    pub(crate) fn complete_operation_locked(
        &self,
        id: OperationId,
        outcome: TerminalOutcome,
        state: &mut State,
    ) -> Result<bool, Error> {
        let (owner, target) = state.operations.participants(id)?;
        let changed = state.operations.complete(id, outcome)?;
        if changed {
            self.mark_operation_activity_locked(owner, target, state);
        }
        Ok(changed)
    }

    pub(crate) fn expire_operation_locked(
        &self,
        id: OperationId,
        now: Instant,
        state: &mut State,
    ) -> Result<bool, Error> {
        let (owner, target) = state.operations.participants(id)?;
        let changed = state.operations.expire_one(id, now)?;
        if changed {
            self.mark_operation_activity_locked(owner, target, state);
        }
        Ok(changed)
    }
}
