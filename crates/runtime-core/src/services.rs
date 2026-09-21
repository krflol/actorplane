//! Bounded native service registration and scoped leases.
use super::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServiceLease {
    pub scope: ActorRef,
    pub owner: ActorRef,
    pub service: ActorRef,
}

#[derive(Clone, Debug)]
pub struct ServiceSnapshot {
    pub name: String,
    pub service: ActorRef,
    pub contract: Arc<InterfaceSpec>,
    pub leases: usize,
}

pub(crate) struct Registry {
    records: HashMap<String, Record>,
    by_target: HashMap<ActorRef, String>,
    leases: HashMap<ActorRef, ServiceLease>,
    by_holder: HashMap<ActorRef, HashSet<ActorRef>>,
}

struct Record {
    name: String,
    service: ActorRef,
    contract: Arc<InterfaceSpec>,
    scopes: HashSet<ActorRef>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            records: HashMap::new(),
            by_target: HashMap::new(),
            leases: HashMap::new(),
            by_holder: HashMap::new(),
        }
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128
}

fn contract_matches(descriptor: &ComponentDescriptor, required: &InterfaceSpec) -> bool {
    if !valid_name(&required.name) || required.version == 0 || required.ports.len() > 64 {
        return false;
    }
    let mut names = HashSet::new();
    if required
        .ports
        .iter()
        .any(|port| !names.insert(port.name.as_str()))
    {
        return false;
    }
    descriptor.interfaces.iter().any(|contract| {
        contract.name == required.name
            && contract.version == required.version
            && contract.ports.len() == required.ports.len()
            && required
                .ports
                .iter()
                .all(|port| contract.ports.contains(port))
    })
}

fn contracts_equal(left: &InterfaceSpec, right: &InterfaceSpec) -> bool {
    left.name == right.name
        && left.version == right.version
        && left.ports.len() == right.ports.len()
        && {
            let mut left_names = HashSet::new();
            let mut right_names = HashSet::new();
            left.ports
                .iter()
                .all(|port| left_names.insert(port.name.as_str()))
                && right
                    .ports
                    .iter()
                    .all(|port| right_names.insert(port.name.as_str()))
                && right
                    .ports
                    .iter()
                    .all(|port| left.ports.iter().any(|candidate| candidate == port))
        }
}

impl World {
    fn validate_lease_locked<'a>(
        &self,
        lease: ServiceLease,
        state: &'a State,
    ) -> Result<&'a Record, Error> {
        if lease.scope.world != self.id()
            || lease.owner.world != self.id()
            || lease.service.world != self.id()
        {
            return Err(Error::CrossWorld);
        }
        if state.services.leases.get(&lease.scope).copied() != Some(lease) {
            return Err(Error::StaleReference);
        }
        if !matches!(
            self.execution_state_locked(lease.scope, state)?,
            Lifecycle::Starting | Lifecycle::Active
        ) || self.execution_state_locked(lease.service, state)? != Lifecycle::Active
        {
            return Err(Error::ActorStopped);
        }
        let name = state
            .services
            .by_target
            .get(&lease.service)
            .ok_or(Error::StaleReference)?;
        let record = state
            .services
            .records
            .get(name)
            .ok_or(Error::StaleReference)?;
        if !record.scopes.contains(&lease.scope) {
            return Err(Error::StaleReference);
        }
        Ok(record)
    }

    pub fn service_request(
        &self,
        lease: ServiceLease,
        port: &str,
        payload: Payload,
        deadline: Instant,
        options: MessageOptions,
    ) -> Result<OperationId, Error> {
        let state = self.inner.state.lock().unwrap();
        let record = self.validate_lease_locked(lease, &state)?;
        if self.execution_state_locked(lease.scope, &state)? != Lifecycle::Active {
            return Err(Error::ActorNotReady);
        }
        let declared = record
            .contract
            .ports
            .iter()
            .find(|spec| spec.name == port && spec.direction == PortDirection::Input)
            .ok_or(Error::InvalidPort)?;
        if !declared.schema.matches(&payload) {
            return Err(Error::SchemaMismatch);
        }
        let service_actor = self.check_ref(record.service, &state)?;
        let descriptor = service_actor
            .component
            .as_ref()
            .ok_or(Error::InterfaceMismatch)?;
        let port_ref = descriptor
            .ports
            .iter()
            .enumerate()
            .find(|(_, spec)| spec.name == port && spec.direction == PortDirection::Input)
            .map(|(index, _)| PortRef {
                owner: record.service,
                index: index as u16,
            })
            .ok_or(Error::InvalidPort)?;
        let spec = descriptor
            .ports
            .get(port_ref.index as usize)
            .ok_or(Error::InvalidPort)?;
        if !spec.schema.matches(&payload) {
            return Err(Error::SchemaMismatch);
        }
        drop(state);
        self.request_with_port(
            lease.scope,
            lease.service,
            payload,
            deadline,
            options,
            Some(port_ref),
        )
    }

    pub fn service_link(
        &self,
        lease: ServiceLease,
        output: &str,
        target: PortRef,
    ) -> Result<u64, Error> {
        let state = self.inner.state.lock().unwrap();
        let record = self.validate_lease_locked(lease, &state)?;
        if !record
            .contract
            .ports
            .iter()
            .any(|spec| spec.name == output && spec.direction == PortDirection::Output)
        {
            return Err(Error::InvalidPort);
        }
        let descriptor = self
            .check_ref(record.service, &state)?
            .component
            .as_ref()
            .ok_or(Error::InterfaceMismatch)?;
        let source = descriptor
            .ports
            .iter()
            .enumerate()
            .find(|(_, spec)| spec.name == output && spec.direction == PortDirection::Output)
            .map(|(index, _)| PortRef {
                owner: record.service,
                index: index as u16,
            })
            .ok_or(Error::InvalidPort)?;
        drop(state);
        self.link(lease.scope, source, target)
    }

    pub fn register_service(
        &self,
        name: &str,
        target: ActorRef,
        required: &InterfaceSpec,
    ) -> Result<(), Error> {
        let mut state = self.inner.state.lock().unwrap();
        if !valid_name(name) {
            return Err(Error::InvalidConfig);
        }
        if state.services.records.contains_key(name)
            || state.services.by_target.contains_key(&target)
        {
            return Err(Error::InvalidConfig);
        }
        if state.services.records.len() >= self.inner.cfg.max_services {
            return Err(Error::LimitExceeded);
        }
        let actor = self.check_ref(target, &state)?;
        if actor.kind != EndpointKind::Native
            || actor.parent.is_some()
            || actor.state != Lifecycle::Active
            || !contract_matches(
                actor.component.as_deref().ok_or(Error::InterfaceMismatch)?,
                required,
            )
        {
            return Err(Error::InterfaceMismatch);
        }
        let record = Record {
            name: name.to_owned(),
            service: target,
            contract: Arc::new(required.clone()),
            scopes: HashSet::new(),
        };
        state.services.by_target.insert(target, name.to_owned());
        state.services.records.insert(name.to_owned(), record);
        self.mark_activity_dependents_locked(target, &state);
        drop(state);
        self.touch_activity();
        Ok(())
    }

    pub fn acquire_service(
        &self,
        owner: ActorRef,
        name: &str,
        required: &InterfaceSpec,
    ) -> Result<ServiceLease, Error> {
        self.acquire_service_locked_target(owner, name, required, None)
    }

    /// Acquire only if the named registration still identifies this endpoint generation.
    pub fn acquire_service_from(
        &self,
        owner: ActorRef,
        name: &str,
        required: &InterfaceSpec,
        expected_service: ActorRef,
    ) -> Result<ServiceLease, Error> {
        if expected_service.world != self.id() {
            return Err(Error::CrossWorld);
        }
        self.acquire_service_locked_target(owner, name, required, Some(expected_service))
    }

    fn acquire_service_locked_target(
        &self,
        owner: ActorRef,
        name: &str,
        required: &InterfaceSpec,
        expected_service: Option<ActorRef>,
    ) -> Result<ServiceLease, Error> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::ActorStopped);
        }
        if !valid_name(name) {
            return Err(Error::InvalidConfig);
        }
        if !matches!(
            self.execution_state_locked(owner, &state)?,
            Lifecycle::Starting | Lifecycle::Active
        ) {
            return Err(Error::ActorStopped);
        }
        let record = state
            .services
            .records
            .get(name)
            .ok_or(if expected_service.is_some() {
                Error::StaleReference
            } else {
                Error::NotFound
            })?;
        if expected_service.is_some_and(|expected| expected != record.service) {
            return Err(Error::StaleReference);
        }
        if !contracts_equal(record.contract.as_ref(), required) {
            return Err(Error::InterfaceMismatch);
        }
        let service = record.service;
        let service_actor = self.check_ref(record.service, &state)?;
        if service_actor.state != Lifecycle::Active {
            return Err(Error::ActorStopped);
        }
        if state.services.leases.len() >= self.inner.cfg.max_service_leases
            || state.services.by_holder.get(&owner).map_or(0, HashSet::len)
                >= self.inner.cfg.max_leases_per_actor
        {
            return Err(Error::LimitExceeded);
        }
        let scope = self.allocate_locked(EndpointKind::Native, Some(owner), &mut state)?;
        self.check_ref_mut(scope, &mut state)?.state = Lifecycle::Active;
        self.refresh_python_ready_locked(scope, &mut state);
        let lease = ServiceLease {
            scope,
            owner,
            service,
        };
        let record = state
            .services
            .records
            .get_mut(name)
            .expect("service checked");
        record.scopes.insert(scope);
        state.services.leases.insert(scope, lease);
        state
            .services
            .by_holder
            .entry(owner)
            .or_default()
            .insert(scope);
        self.mark_activity_dependents_locked(scope, &state);
        self.mark_activity_dependents_locked(service, &state);
        drop(state);
        self.touch_activity();
        Ok(lease)
    }

    pub fn release_service(&self, lease: ServiceLease) -> Result<bool, Error> {
        if lease.scope.world != self.id()
            || lease.owner.world != self.id()
            || lease.service.world != self.id()
        {
            return Err(Error::CrossWorld);
        }
        let mut state = self.inner.state.lock().unwrap();
        let Some(current) = state.services.leases.get(&lease.scope) else {
            return Ok(false);
        };
        if current != &lease {
            return Err(Error::StaleReference);
        }
        self.stop_locked(lease.scope, &mut state)?;
        drop(state);
        self.touch_activity();
        Ok(true)
    }

    pub fn services(&self) -> Vec<ServiceSnapshot> {
        let state = self.inner.state.lock().unwrap();
        let mut values: Vec<_> = state
            .services
            .records
            .values()
            .map(|record| ServiceSnapshot {
                name: record.name.clone(),
                service: record.service,
                contract: record.contract.clone(),
                leases: record.scopes.len(),
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn service_leases(&self) -> Vec<ServiceLease> {
        let state = self.inner.state.lock().unwrap();
        let mut values: Vec<_> = state.services.leases.values().copied().collect();
        values.sort_by_key(|lease| (lease.scope.slot, lease.scope.generation));
        values
    }

    pub(crate) fn detach_scope(&self, scope: ActorRef, state: &mut State) {
        let Some(lease) = state.services.leases.remove(&scope) else {
            return;
        };
        if let Some(name) = state.services.by_target.get(&lease.service).cloned()
            && let Some(record) = state.services.records.get_mut(&name)
        {
            record.scopes.remove(&scope);
        }
        if let Some(scopes) = state.services.by_holder.get_mut(&lease.owner) {
            scopes.remove(&scope);
            if scopes.is_empty() {
                state.services.by_holder.remove(&lease.owner);
            }
        }
        self.mark_activity_dependents_locked(lease.owner, state);
        self.mark_activity_dependents_locked(lease.service, state);
    }

    pub(crate) fn detach_service(&self, service: ActorRef, state: &mut State) {
        let Some(name) = state.services.by_target.remove(&service) else {
            return;
        };
        let Some(record) = state.services.records.remove(&name) else {
            return;
        };
        let mut scopes: Vec<_> = record.scopes.into_iter().collect();
        scopes.sort_by_key(|scope| (scope.slot, scope.generation));
        for scope in scopes {
            let lease = state.services.leases.remove(&scope);
            if let Some(lease) = lease {
                let owner = lease.owner;
                if let Some(holders) = state.services.by_holder.get_mut(&owner) {
                    holders.remove(&scope);
                    if holders.is_empty() {
                        state.services.by_holder.remove(&owner);
                    }
                }
                self.mark_activity_dependents_locked(owner, state);
            }
            self.mark_activity_dependents_locked(service, state);
            let _ = self.fence_locked(scope, state);
        }
    }
}
