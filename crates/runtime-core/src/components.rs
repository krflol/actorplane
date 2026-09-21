//! Source-level component contracts. All ports use FIFO admission, reject on
//! overload, and cancel unclaimed deliveries when their owner or route ends.
use super::*;
use std::collections::HashSet;

pub const MAX_PORTS: usize = 64;
pub const MAX_INTERFACES: usize = 16;
pub const MAX_WORLD_INTERFACES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PayloadType {
    Pulse,
    CountSnapshot,
    Structured(u32),
}
impl PayloadType {
    pub fn matches(self, payload: &Payload) -> bool {
        match (self, payload) {
            (Self::Pulse, Payload::Pulse(_))
            | (Self::CountSnapshot, Payload::CountSnapshot { .. }) => true,
            (Self::Structured(id), Payload::Structured(record)) => id == record.schema(),
            _ => false,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PortDirection {
    Input,
    Output,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortSpec {
    pub name: String,
    pub direction: PortDirection,
    pub schema: PayloadType,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceSpec {
    pub name: String,
    pub version: u32,
    pub ports: Vec<PortSpec>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDescriptor {
    pub name: String,
    pub version: u32,
    pub ports: Vec<PortSpec>,
    pub interfaces: Vec<InterfaceSpec>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PortRef {
    pub owner: ActorRef,
    pub index: u16,
}
#[derive(Clone, Debug)]
pub struct LinkSnapshot {
    pub id: u64,
    pub owner: ActorRef,
    pub source: PortRef,
    pub target: PortRef,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128
}
fn validate_ports(ports: &[PortSpec], state: &State) -> Result<(), Error> {
    if ports.len() > MAX_PORTS {
        return Err(Error::LimitExceeded);
    }
    let mut names = HashSet::new();
    let mut inputs = HashSet::new();
    for port in ports {
        if !valid_name(&port.name) || !names.insert(port.name.as_str()) {
            return Err(Error::InvalidPort);
        }
        if port.direction == PortDirection::Input && !inputs.insert(port.schema) {
            return Err(Error::InvalidPort);
        }
        if let PayloadType::Structured(id) = port.schema {
            state
                .schemas
                .schema(id)
                .map_err(|_| Error::SchemaMismatch)?;
        }
    }
    Ok(())
}
impl World {
    pub(super) fn flush_staged_locked(&self, state: &mut State) {
        self.expire_events_locked(self.now(), state);
        let ready: Vec<_> = state
            .staged_actors
            .values()
            .copied()
            .filter(|source| self.execution_state_locked(*source, state) == Ok(Lifecycle::Active))
            .collect();
        for source in ready {
            let actor = self
                .check_ref_mut(source, state)
                .expect("registered source");
            let queued = std::mem::take(&mut actor.staged);
            actor.staged_bytes = 0;
            for (_, stored, _) in &queued {
                actor.deadlines.remove(stored.options.deadline);
            }
            let earliest = actor.deadlines.first();
            state.event_deadlines.set(source, earliest);
            state.staged_actors.remove(&source.slot);
            for (_, _, publication) in queued {
                self.enqueue_publication_locked(publication, state);
            }
            self.mark_activity_dependents_locked(source, state);
        }
    }
    /// Register once while Starting. Interface identities are immutable and
    /// retained for this World's lifetime, subject to a fixed 1024-entry cap.
    pub fn register_component(
        &self,
        owner: ActorRef,
        descriptor: ComponentDescriptor,
    ) -> Result<Vec<PortRef>, Error> {
        let mut state = self.inner.state.lock().unwrap();
        let actor = self.check_ref(owner, &state)?;
        if state.closed || actor.state != Lifecycle::Starting || actor.component.is_some() {
            return Err(Error::ActorNotReady);
        }
        if !matches!(
            self.execution_state_locked(owner, &state)?,
            Lifecycle::Starting | Lifecycle::Active
        ) {
            return Err(Error::ActorStopped);
        }
        if !valid_name(&descriptor.name)
            || descriptor.version == 0
            || descriptor.interfaces.len() > MAX_INTERFACES
        {
            return Err(Error::InvalidConfig);
        }
        validate_ports(&descriptor.ports, &state)?;
        let mut incoming = HashMap::new();
        let mut identities = HashSet::new();
        for interface in &descriptor.interfaces {
            if !valid_name(&interface.name) || interface.version == 0 {
                return Err(Error::InterfaceMismatch);
            }
            validate_ports(&interface.ports, &state)?;
            if interface
                .ports
                .iter()
                .any(|port| !descriptor.ports.contains(port))
            {
                return Err(Error::InterfaceMismatch);
            }
            let key = (interface.name.clone(), interface.version);
            if !identities.insert(key.clone()) {
                return Err(Error::InterfaceMismatch);
            }
            let mut normalized = interface.clone();
            normalized.ports.sort_by(|a, b| a.name.cmp(&b.name));
            if let Some(prior) = state.interfaces.get(&key) {
                if prior != &normalized {
                    return Err(Error::InterfaceMismatch);
                }
            } else if incoming.insert(key, normalized).is_some() {
                return Err(Error::InterfaceMismatch);
            }
        }
        if state.interfaces.len() + incoming.len() > MAX_WORLD_INTERFACES {
            return Err(Error::LimitExceeded);
        }
        let ports = (0..descriptor.ports.len())
            .map(|index| PortRef {
                owner,
                index: index as u16,
            })
            .collect();
        state.interfaces.extend(incoming);
        self.check_ref_mut(owner, &mut state)?.component = Some(Arc::new(descriptor.clone()));
        Ok(ports)
    }
    pub fn component(&self, owner: ActorRef) -> Result<Option<Arc<ComponentDescriptor>>, Error> {
        Ok(self
            .check_ref(owner, &self.inner.state.lock().unwrap())?
            .component
            .clone())
    }
    pub fn require_interface(
        &self,
        owner: ActorRef,
        required: &InterfaceSpec,
    ) -> Result<(), Error> {
        let state = self.inner.state.lock().unwrap();
        validate_ports(&required.ports, &state)?;
        let descriptor = self
            .check_ref(owner, &state)?
            .component
            .as_ref()
            .ok_or(Error::InterfaceMismatch)?;
        if descriptor.interfaces.iter().any(|contract| {
            contract.name == required.name
                && contract.version == required.version
                && contract.ports.len() == required.ports.len()
                && required
                    .ports
                    .iter()
                    .all(|port| contract.ports.contains(port))
        }) {
            Ok(())
        } else {
            Err(Error::InterfaceMismatch)
        }
    }
    pub fn port(&self, owner: ActorRef, name: &str) -> Result<PortRef, Error> {
        let state = self.inner.state.lock().unwrap();
        let descriptor = self
            .check_ref(owner, &state)?
            .component
            .as_ref()
            .ok_or(Error::InvalidPort)?;
        let index = descriptor
            .ports
            .iter()
            .position(|p| p.name == name)
            .ok_or(Error::InvalidPort)?;
        Ok(PortRef {
            owner,
            index: index as u16,
        })
    }
    pub(super) fn port_locked<'a>(
        &self,
        port: PortRef,
        state: &'a State,
    ) -> Result<&'a PortSpec, Error> {
        self.check_ref(port.owner, state)?
            .component
            .as_ref()
            .and_then(|c| c.ports.get(port.index as usize))
            .ok_or(Error::InvalidPort)
    }
    pub fn link(&self, owner: ActorRef, source: PortRef, target: PortRef) -> Result<u64, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        for reference in [owner, source.owner, target.owner] {
            if !matches!(
                self.execution_state_locked(reference, &state)?,
                Lifecycle::Starting | Lifecycle::Active
            ) {
                return Err(Error::ActorStopped);
            }
        }
        let src = self.port_locked(source, &state)?;
        let dst = self.port_locked(target, &state)?;
        if src.direction != PortDirection::Output || dst.direction != PortDirection::Input {
            return Err(Error::InvalidPort);
        }
        if src.schema != dst.schema {
            return Err(Error::SchemaMismatch);
        }
        if state.subs.active_len() >= self.inner.cfg.max_subscriptions {
            return Err(Error::LimitExceeded);
        }
        if state.subs.typed_duplicate(source, target) {
            return Err(Error::DuplicateSubscription);
        }
        let id = self.inner.next_sub.fetch_add(1, Ordering::Relaxed);
        let route = super::routing::Sub {
            owner,
            source: source.owner,
            source_port: Some(source.index),
            target: target.owner,
            target_port: Some(target.index),
        };
        state.subs.insert(id, route.clone());
        self.mark_route_activity_locked(&route, &state);
        Ok(id)
    }
    pub fn send_port(&self, port: PortRef, payload: Payload) -> Result<u64, Error> {
        self.send_port_with(port, payload, MessageOptions::default())
    }
    pub fn send_port_with(
        &self,
        port: PortRef,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<u64, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        state.metrics.submitted += 1;
        let result = (|| {
            let spec = self.port_locked(port, &state)?;
            if spec.direction != PortDirection::Input {
                return Err(Error::InvalidPort);
            }
            if !spec.schema.matches(&payload) {
                return Err(Error::SchemaMismatch);
            }
            let stored = self.make_stored_with(payload, options, None)?;
            let id = self.admit(port.owner, stored, None, false, None, &mut state)?;
            self.check_ref_mut(port.owner, &mut state)?
                .queue
                .back_mut()
                .expect("admitted event")
                .envelope
                .destination_port = Some(port);
            Ok(id)
        })();
        if let Err(error) = &result {
            self.reject(&mut state, error, port.owner);
        }
        result
    }
    pub fn publish_port(
        &self,
        source: PortRef,
        payload: Payload,
    ) -> Result<PublicationTicket, Error> {
        self.publish_port_with(source, payload, MessageOptions::default())
    }
    pub fn publish_port_with(
        &self,
        source: PortRef,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<PublicationTicket, Error> {
        self.publish_internal(source.owner, payload, None, Some(source.index), options)
    }
    pub fn links(&self) -> Vec<LinkSnapshot> {
        self.inner
            .state
            .lock()
            .unwrap()
            .subs
            .iter()
            .filter_map(|(id, sub)| {
                sub.source_port.map(|index| LinkSnapshot {
                    id: *id,
                    owner: sub.owner,
                    source: PortRef {
                        owner: sub.source,
                        index,
                    },
                    target: PortRef {
                        owner: sub.target,
                        index: sub.target_port.expect("typed link target"),
                    },
                })
            })
            .collect()
    }
    /// A draining consumer may finish after every linked producer has released
    /// its work and its queue. An active external producer keeps the drain open
    /// until the shared deadline instead of silently losing a late completion.
    pub fn upstream_done(&self, target: ActorRef) -> Result<bool, Error> {
        let state = self.inner.state.lock().unwrap();
        let target = self.check_ref(target, &state)?;
        Ok(target.queue.is_empty()
            && !target.in_flight
            && state.publications.pending_for(target.reference) == 0
            && state
                .subs
                .target_ids(target.reference)
                .into_iter()
                .all(|id| {
                    let sub = state.subs.get(&id).expect("indexed route");
                    self.check_ref(sub.source, &state).is_ok_and(|source| {
                        matches!(
                            source.state,
                            Lifecycle::Quiescing | Lifecycle::Stopping | Lifecycle::Stopped
                        ) && source.native_tasks == 0
                            && state.publications.pending_for(source.reference) == 0
                            && source.queue.is_empty()
                            && !source.in_flight
                    })
                }))
    }
}
impl TaskLease {
    pub fn publish_port_completion(
        &self,
        source: PortRef,
        payload: Payload,
    ) -> Result<PublicationTicket, Error> {
        self.publish_port_completion_with(source, payload, MessageOptions::default())
    }
    pub fn publish_port_completion_with(
        &self,
        source: PortRef,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<PublicationTicket, Error> {
        World {
            inner: self.guard.inner.clone(),
        }
        .publish_internal(
            source.owner,
            payload,
            Some(self.guard.owner),
            Some(source.index),
            options,
        )
    }
}
impl DeliveryLease {
    /// An already claimed callback may emit typed completion effects during
    /// drain. The source must belong to the callback's ownership subtree.
    pub fn publish_port_completion(
        &self,
        source: PortRef,
        payload: Payload,
    ) -> Result<PublicationTicket, Error> {
        self.publish_port_completion_with(source, payload, MessageOptions::default())
    }
    pub fn publish_port_completion_with(
        &self,
        source: PortRef,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<PublicationTicket, Error> {
        options.validate()?;
        if options.source.is_some_and(|owner| owner != source.owner) {
            return Err(Error::InvalidMetadata);
        }
        let mut metadata = self.envelope().child_options(source.owner);
        metadata.deadline = match (metadata.deadline, options.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        metadata.correlation_id = options.correlation_id.or(metadata.correlation_id);
        metadata.causation_id = options.causation_id.or(metadata.causation_id);
        metadata.trace = options.trace.or(metadata.trace);
        World {
            inner: self.inner.clone(),
        }
        .publish_internal(
            source.owner,
            payload,
            Some(self.target),
            Some(source.index),
            metadata,
        )
    }
}
