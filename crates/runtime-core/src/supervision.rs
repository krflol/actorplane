//! Failure control slots are reserved by actor registration, independently of
//! business mailboxes. One pending notification per child coalesces repeats.
use super::*;

pub(crate) struct Notification {
    failure: Arc<FailureRecord>,
    coalesced: u64,
}

pub struct FailureLease {
    inner: Arc<Inner>,
    supervisor: ActorRef,
    notification: Notification,
    active: bool,
}

impl World {
    #[allow(clippy::too_many_arguments)]
    pub fn report_failure(
        &self,
        reference: ActorRef,
        event_id: Option<u64>,
        schema: Option<u32>,
        operation: Option<OperationId>,
        details: FailureDetails,
        action: FailureAction,
    ) -> Result<Arc<FailureRecord>, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let actor = self.check_ref(reference, &state)?;
        if actor.state == Lifecycle::Stopped {
            return Err(Error::ActorStopped);
        }
        let parent = actor.parent;
        if operation.is_some_and(|operation| operation.world != self.id()) {
            return Err(Error::CrossWorld);
        }
        if let Some(operation_id) = operation
            && let Ok(OperationStatus::Pending(pending)) = state.operations.status(operation_id)
        {
            let mut current = pending.target;
            let mut owned = false;
            loop {
                if current == reference {
                    owned = true;
                    break;
                }
                let Some(parent_ref) = self.check_ref(current, &state)?.parent else {
                    break;
                };
                current = parent_ref;
            }
            if !owned {
                return Err(Error::InvalidConfig);
            }
        }
        let sequence = state.next_failure;
        state.next_failure = sequence.checked_add(1).ok_or(Error::LimitExceeded)?;
        let notify = details.phase() != FailurePhase::Supervisor
            && parent.is_some_and(|parent| {
                self.check_ref(parent, &state).is_ok_and(|p| {
                    matches!(
                        p.state,
                        Lifecycle::Starting | Lifecycle::Active | Lifecycle::Quiescing
                    )
                })
            });
        let record = Arc::new(FailureRecord {
            sequence,
            elapsed_ns: self
                .now()
                .saturating_duration_since(state.failure_epoch)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            actor: reference,
            event_id,
            schema,
            operation,
            details,
            action,
        });
        state.metrics.failures += 1;
        state.diagnostics.record_failure(record.clone());
        state.last_error = Some(record.clone());
        if let Some(operation) = operation {
            if self
                .expire_operation_locked(operation, self.now(), &mut state)
                .unwrap_or(false)
            {
                state.metrics.operation_timed_out += 1;
                self.diagnostic(
                    &mut state,
                    DiagnosticCode::OperationTimedOut,
                    None,
                    Some(operation),
                );
            }
            if self
                .complete_operation_locked(
                    operation,
                    TerminalOutcome::Failed {
                        code: OperationFailure::HandlerFailed,
                        message: "operation handler failed".into(),
                    },
                    &mut state,
                )
                .unwrap_or(false)
            {
                state.metrics.operation_completed += 1;
            }
        }
        let actor = self.check_ref_mut(reference, &mut state)?;
        actor.reported_failure = event_id;
        actor.errors = actor.errors.saturating_add(1);
        actor.last_error = Some(record.clone());
        let new_notification = if notify {
            if let Some(pending) = &mut actor.notification {
                pending.coalesced = pending.coalesced.saturating_add(1);
                state.metrics.notifications_coalesced += 1;
                false
            } else {
                actor.notification = Some(Notification {
                    failure: record.clone(),
                    coalesced: 0,
                });
                true
            }
        } else {
            false
        };
        if new_notification {
            self.check_ref_mut(parent.expect("notification has a supervisor"), &mut state)?
                .pending_failures += 1;
        }
        self.mark_activity_dependents_locked(reference, &state);
        self.apply_failure_action(reference, action, &mut state)?;
        if let Some(parent) = parent {
            self.refresh_python_ready_locked(parent, &mut state);
        }
        Ok(record)
    }

    fn apply_failure_action(
        &self,
        reference: ActorRef,
        action: FailureAction,
        state: &mut State,
    ) -> Result<(), Error> {
        match action {
            FailureAction::Continue => (),
            FailureAction::StopWorld => {
                self.close_locked(state);
            }
            FailureAction::StopActor => {
                // A notification may outlive its source generation. It remains
                // evidence, but must never stop a replacement in the same slot.
                match self.stop_locked(reference, state) {
                    Ok(_) | Err(Error::StaleReference) => (),
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub fn claim_failure(&self, supervisor: ActorRef) -> Result<Option<FailureLease>, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let actor = self.check_ref(supervisor, &state)?;
        if actor.in_flight
            || actor.control_in_flight
            || !matches!(
                self.execution_state_locked(supervisor, &state)?,
                Lifecycle::Active | Lifecycle::Quiescing
            )
        {
            return Ok(None);
        }
        let children = self.check_ref(supervisor, &state)?.children.clone();
        let source = children
            .into_iter()
            .filter_map(|child| {
                self.check_ref(child, &state)
                    .ok()?
                    .notification
                    .as_ref()
                    .map(|notification| (child, notification.failure.sequence))
            })
            .min_by_key(|(_, sequence)| *sequence)
            .map(|(reference, _)| reference);
        let Some(source) = source else {
            return Ok(None);
        };
        let notification = self
            .check_ref_mut(source, &mut state)?
            .notification
            .take()
            .unwrap();
        let actor = self.check_ref_mut(supervisor, &mut state)?;
        actor.pending_failures -= 1;
        actor.control_in_flight = true;
        self.mark_activity_dependents_locked(supervisor, &state);
        self.refresh_python_ready_locked(supervisor, &mut state);
        state.metrics.notifications_delivered += 1;
        self.try_finalize(source, &mut state);
        Ok(Some(FailureLease {
            inner: self.inner.clone(),
            supervisor,
            notification,
            active: true,
        }))
    }

    pub(crate) fn discard_notifications_for(&self, supervisor: ActorRef, state: &mut State) {
        let children = match self.check_ref(supervisor, state) {
            Ok(actor) => actor.children.clone(),
            Err(_) => return,
        };
        let sources: Vec<_> = children
            .into_iter()
            .filter(|child| {
                self.check_ref(*child, state)
                    .is_ok_and(|actor| actor.notification.is_some())
            })
            .collect();
        let had_sources = !sources.is_empty();
        for source in sources {
            self.check_ref_mut(source, state).unwrap().notification = None;
            self.check_ref_mut(supervisor, state)
                .unwrap()
                .pending_failures -= 1;
            state.metrics.notifications_discarded += 1;
            self.try_finalize(source, state);
        }
        if had_sources {
            self.mark_activity_dependents_locked(supervisor, state);
        }
        self.refresh_python_ready_locked(supervisor, state);
    }
}

impl FailureLease {
    pub fn failure(&self) -> &Arc<FailureRecord> {
        &self.notification.failure
    }
    pub fn coalesced(&self) -> u64 {
        self.notification.coalesced
    }
    pub fn finish(mut self, action: Option<FailureAction>) -> Result<(), Error> {
        let world = World {
            inner: self.inner.clone(),
        };
        let _activity = world.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let result = action.map_or(Ok(()), |action| {
            world.apply_failure_action(self.notification.failure.actor, action, &mut state)
        });
        self.active = false;
        world
            .check_ref_mut(self.supervisor, &mut state)?
            .control_in_flight = false;
        world.try_finalize(self.supervisor, &mut state);
        result
    }
}

impl Drop for FailureLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let world = World {
            inner: self.inner.clone(),
        };
        let _activity = world.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        if let Ok(actor) = world.check_ref_mut(self.supervisor, &mut state) {
            actor.control_in_flight = false;
        }
        state.metrics.notifications_discarded += 1;
        world.try_finalize(self.supervisor, &mut state);
    }
}

impl TaskLease {
    pub fn owner(&self) -> ActorRef {
        self.guard.owner
    }
}
