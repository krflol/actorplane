use super::*;

impl World {
    pub(super) fn expire_delivery_locked(
        &self,
        delivery: &Delivery,
        now: Instant,
        state: &mut State,
    ) {
        state.metrics.expired += 1;
        state.metrics.cancelled += 1;
        state.metrics.discarded += 1;
        state.diagnostics.record_event(
            DiagnosticCode::DeadlineExpired,
            &delivery.envelope,
            delivery.operation,
        );
        if let Some(operation) = delivery.operation
            && self
                .expire_operation_locked(operation, now, state)
                .unwrap_or(false)
        {
            state.metrics.operation_timed_out += 1;
            self.diagnostic(
                state,
                DiagnosticCode::OperationTimedOut,
                Some(delivery.envelope.destination),
                Some(operation),
            );
        }
    }

    pub(super) fn expire_events_locked(&self, now: Instant, state: &mut State) {
        for owner in state.event_deadlines.take_due(now) {
            let index = owner.slot as usize;
            let actor = self
                .check_ref_mut(owner, state)
                .expect("indexed deadline owner remains registered");
            let mut queue = std::mem::take(&mut actor.queue);
            let mut removed_bytes = 0;
            queue.retain(|delivery| {
                if delivery
                    .envelope
                    .deadline
                    .is_some_and(|deadline| deadline <= now)
                {
                    removed_bytes += delivery.payload.bytes;
                    state.actors[index]
                        .as_mut()
                        .expect("actor remains registered")
                        .deadlines
                        .remove(delivery.envelope.deadline);
                    self.expire_delivery_locked(delivery, now, state);
                    false
                } else {
                    true
                }
            });
            let actor = state.actors[index]
                .as_mut()
                .expect("actor remains registered");
            actor.queue = queue;
            actor.queue_bytes -= removed_bytes;
            let mut staged_bytes = 0;
            let mut expired_staged = 0;
            let mut expired_publications = Vec::new();
            let deadlines = &mut actor.deadlines;
            actor.staged.retain(|(_, stored, publication)| {
                if stored
                    .options
                    .deadline
                    .is_some_and(|deadline| deadline <= now)
                {
                    staged_bytes += stored.bytes;
                    expired_staged += 1;
                    expired_publications.push(*publication);
                    deadlines.remove(stored.options.deadline);
                    false
                } else {
                    true
                }
            });
            actor.staged_bytes -= staged_bytes;
            let earliest = actor.deadlines.first();
            let staged_empty = actor.staged.is_empty();
            state.event_deadlines.set(owner, earliest);
            if staged_empty {
                state.staged_actors.remove(&owner.slot);
            }
            for publication in expired_publications {
                self.cancel_publication_locked(publication, PublicationOutcome::Expired, state);
            }
            self.mark_activity_dependents_locked(owner, state);
            self.refresh_python_ready_locked(owner, state);
            state.metrics.expired += expired_staged;
            state.metrics.cancelled += expired_staged;
            state.metrics.discarded += expired_staged;
            for _ in 0..expired_staged {
                self.diagnostic(state, DiagnosticCode::DeadlineExpired, Some(owner), None);
            }
        }
    }

    pub fn elapsed_ns(&self) -> u64 {
        self.instant_ns(self.now())
    }
    pub fn instant_ns(&self, instant: Instant) -> u64 {
        instant
            .saturating_duration_since(self.inner.epoch)
            .as_nanos()
            .min(u64::MAX as u128) as u64
    }
    pub(super) fn next_event_id(&self) -> Result<u64, Error> {
        self.inner
            .next_event
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::LimitExceeded)
    }
    pub(super) fn check_deadline(
        &self,
        options: &MessageOptions,
        now: Instant,
    ) -> Result<(), Error> {
        if options.deadline.is_some_and(|deadline| deadline <= now) {
            Err(Error::DeadlineExpired)
        } else {
            Ok(())
        }
    }
    pub(super) fn validate_source(
        &self,
        options: &MessageOptions,
        state: &State,
        allow_pending: bool,
    ) -> Result<(), Error> {
        options.validate()?;
        if let Some(source) = options.source {
            match self.execution_state_locked(source, state)? {
                Lifecycle::Active => (),
                Lifecycle::Starting | Lifecycle::Quiescing if allow_pending => (),
                Lifecycle::Starting => return Err(Error::ActorNotReady),
                _ => return Err(Error::ActorStopped),
            }
        }
        Ok(())
    }
    fn schema_identity(&self, payload: &Payload, state: &State) -> Result<SchemaIdentity, Error> {
        Ok(match payload {
            Payload::Pulse(_) => SchemaIdentity {
                kind: SchemaKind::Pulse,
                id: 0,
                version: 1,
            },
            Payload::CountSnapshot { .. } => SchemaIdentity {
                kind: SchemaKind::CountSnapshot,
                id: 0,
                version: 1,
            },
            Payload::Record { schema, .. } => SchemaIdentity {
                kind: SchemaKind::Record,
                id: *schema,
                version: 0,
            },
            Payload::Structured(record) => SchemaIdentity {
                kind: SchemaKind::Structured,
                id: record.schema(),
                version: state
                    .schemas
                    .schema(record.schema())
                    .map_err(|_| Error::SchemaMismatch)?
                    .version,
            },
        })
    }
    pub(super) fn envelope_for(
        &self,
        target: ActorRef,
        stored: &Stored,
        subscription: Option<u64>,
        event_id: u64,
        now: Instant,
        state: &State,
    ) -> Result<Envelope, Error> {
        let actor = self.check_ref(target, state)?;
        let destination_port = subscription
            .and_then(|id| state.subs.get(&id))
            .and_then(|sub| sub.target_port)
            .or_else(|| {
                actor
                    .component
                    .as_ref()?
                    .ports
                    .iter()
                    .position(|port| {
                        port.direction == PortDirection::Input
                            && port.schema.matches(&stored.payload)
                    })
                    .map(|index| index as u16)
            })
            .map(|index| PortRef {
                owner: target,
                index,
            });
        Ok(Envelope {
            event_id,
            schema: self.schema_identity(&stored.payload, state)?,
            source: stored.options.source,
            destination: target,
            dispatcher: stored.dispatcher,
            destination_port,
            owner: target,
            enqueued_at: now,
            deadline: stored.options.deadline,
            correlation_id: stored.options.correlation_id,
            causation_id: stored.options.causation_id,
            trace: stored.options.trace.clone(),
        })
    }
}
