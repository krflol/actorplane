#![forbid(unsafe_code)]

mod activity;
mod activity_dependencies;
mod publications;
pub use publications::{PublicationOutcome, PublicationStatus, PublicationTicket, RoutingProgress};
mod buffers;
pub use buffers::{NativeBufferBudget, NativeBufferPermit};
mod callbacks;
mod clock;
pub use callbacks::NativeCallbackLease;
pub use clock::{Clock, VirtualClock};
pub mod components;
pub mod diagnostics;
pub mod envelope;
mod envelope_runtime;
mod expiry;
pub use envelope::{Envelope, MessageOptions, SchemaIdentity, SchemaKind, TraceContext};
pub mod failures;
pub mod operations;
pub mod schema;
pub use components::{
    ComponentDescriptor, InterfaceSpec, PayloadType, PortDirection, PortRef, PortSpec,
};
mod readiness;
mod routing;
pub use readiness::PythonReadyPhase;
pub mod services;
mod shutdown;
pub mod supervision;
pub use diagnostics::{DiagnosticCode, DiagnosticEntry, DiagnosticGap, DiagnosticRead};
pub use failures::{FailureAction, FailureDetails, FailureFrame, FailurePhase, FailureRecord};
pub use operations::{
    OperationFailure, OperationId, OperationStatus, OperationTable, TerminalOutcome,
};
pub use services::{ServiceLease, ServiceSnapshot};
pub use supervision::FailureLease;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_actors: usize,
    pub mailbox_capacity: usize,
    pub mailbox_bytes: usize,
    pub native_payload_budget: usize,
    pub max_subscriptions: usize,
    pub max_event_bytes: usize,
    pub max_operations: usize,
    pub diagnostic_capacity: usize,
    pub max_schemas: usize,
    pub max_services: usize,
    pub max_service_leases: usize,
    pub max_leases_per_actor: usize,
    pub max_publications: usize,
    pub max_publications_per_source: usize,
    pub max_publication_fanout: usize,
    pub max_routing_snapshot_entries: usize,
    pub routing_batch_size: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_actors: 1024,
            mailbox_capacity: 64,
            mailbox_bytes: 1 << 20,
            native_payload_budget: 16 << 20,
            max_subscriptions: 4096,
            max_event_bytes: 1 << 20,
            max_operations: 1024,
            diagnostic_capacity: 1024,
            max_schemas: 128,
            max_services: 64,
            max_service_leases: 1024,
            max_leases_per_actor: 16,
            max_publications: 256,
            max_publications_per_source: 32,
            max_publication_fanout: 4096,
            max_routing_snapshot_entries: 16384,
            routing_batch_size: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    QueueFull,
    BudgetExceeded,
    ActorNotReady,
    ActorStopped,
    StaleReference,
    CrossWorld,
    LimitExceeded,
    InvalidConfig,
    NotFound,
    DuplicateSubscription,
    SchemaMismatch,
    InvalidPort,
    InterfaceMismatch,
    InvalidMetadata,
    DeadlineExpired,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ActorRef {
    pub world: u64,
    pub slot: u32,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EndpointKind {
    #[default]
    Native,
    Python,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    Starting,
    Active,
    Quiescing,
    Stopping,
    Stopped,
}
pub type SubscriptionId = u64;
#[derive(Clone, Debug, PartialEq)]
pub enum Payload {
    Pulse(i64),
    CountSnapshot { count: u64, total: i64 },
    Record { schema: u32, integers: Vec<i64> },
    Structured(StructuredRecord),
}
/// Canonical schema-indexed bytes validated and copied by their owning World.
#[derive(Clone, Debug, PartialEq)]
pub struct StructuredRecord {
    world: u64,
    schema: u32,
    bytes: Box<[u8]>,
}
impl StructuredRecord {
    pub fn world(&self) -> u64 {
        self.world
    }
    pub fn schema(&self) -> u32 {
        self.schema
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl Payload {
    fn bytes(&self) -> usize {
        match self {
            Self::Pulse(_) => 8,
            Self::CountSnapshot { .. } => 16,
            Self::Record { integers, .. } => 4 + integers.capacity() * 8,
            Self::Structured(record) => 4 + record.bytes.len(),
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct Metrics {
    pub publication_submitted: u64,
    pub publication_admitted: u64,
    pub publication_rejected: u64,
    pub publication_completed: u64,
    pub publication_cancelled: u64,
    pub publication_expired: u64,
    pub publication_failed: u64,
    pub submitted: u64,
    pub admitted: u64,
    pub rejected: u64,
    pub started: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub discarded: u64,
    pub expired: u64,
    pub drain_timeouts: u64,
    pub operation_completed: u64,
    pub operation_timed_out: u64,
    pub operation_cancelled: u64,
    pub operation_late_results: u64,
    pub failures: u64,
    pub notifications_coalesced: u64,
    pub notifications_delivered: u64,
    pub notifications_discarded: u64,
}
#[derive(Clone, Debug)]
pub struct ActorSnapshot {
    pub reference: ActorRef,
    pub kind: EndpointKind,
    pub state: Lifecycle,
    pub queue_entries: usize,
    pub queue_bytes: usize,
    pub staged_entries: usize,
    pub staged_bytes: usize,
    pub component: Option<Arc<ComponentDescriptor>>,
    pub in_flight: bool,
    pub control_in_flight: bool,
    pub failure_pending: bool,
    pub native_tasks: usize,
    pub python_done: bool,
    pub parent: Option<ActorRef>,
    pub drain_deadline: Option<Instant>,
}
#[derive(Clone, Debug)]
pub struct WorldSnapshot {
    pub actors: Vec<ActorSnapshot>,
    pub retained_payload_bytes: usize,
    pub subscriptions: usize,
    pub metrics: Metrics,
    pub operation_pending: usize,
    pub operation_retained: usize,
    pub publications_pending: usize,
    pub publications_retained: usize,
    pub routing_snapshot_entries: usize,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StopReport {
    pub native_done: bool,
    pub python_done: bool,
    pub in_flight: usize,
    pub discarded: usize,
    pub timed_out: bool,
    pub queued: usize,
    pub delivery_in_flight: usize,
    pub control_in_flight: usize,
    pub native_tasks: usize,
    pub python_pending: usize,
    pub pending_notifications: usize,
    pub outstanding_operations: usize,
    pub retained_operations: usize,
    pub outstanding_publications: usize,
    pub retained_publications: usize,
    /// Lifetime failures in this ownership scope, including retired children.
    pub errors: u64,
    /// Only the most recent bounded record is retained here. Full recent history
    /// is available separately, subject to the diagnostic ring's capacity.
    pub last_error: Option<Arc<FailureRecord>>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    pub admitted: usize,
    pub rejected: usize,
    pub staged: usize,
    pub matched: usize,
    pub outcome: PublicationOutcome,
}

struct Stored {
    payload: Payload,
    options: MessageOptions,
    dispatcher: Option<PortRef>,
    bytes: usize,
    retained: Arc<AtomicUsize>,
}
impl Drop for Stored {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Delivery {
    event_id: u64,
    envelope: Envelope,
    payload: Arc<Stored>,
    subscription: Option<u64>,
    operation: Option<OperationId>,
}
struct Actor {
    reference: ActorRef,
    ready_order: u64,
    kind: EndpointKind,
    state: Lifecycle,
    parent: Option<ActorRef>,
    children: Vec<ActorRef>,
    queue: VecDeque<Delivery>,
    queue_bytes: usize,
    staged: VecDeque<(u16, Arc<Stored>, u64)>,
    staged_bytes: usize,
    deadlines: expiry::ActorDeadlines,
    in_flight: bool,
    control_in_flight: bool,
    notification: Option<supervision::Notification>,
    pending_failures: usize,
    reported_failure: Option<u64>,
    native_tasks: usize,
    python_done: bool,
    drain_root: Option<ActorRef>,
    drain_deadline: Option<Instant>,
    publication_cutoff: Option<u64>,
    drain_timed_out: bool,
    errors: u64,
    last_error: Option<Arc<FailureRecord>>,
    component: Option<Arc<ComponentDescriptor>>,
}
struct State {
    actors: Vec<Option<Actor>>,
    live_actors: usize,
    next_ready_order: u64,
    python_ready: readiness::PythonReadyIndex,
    event_deadlines: expiry::DeadlineIndex,
    staged_actors: BTreeMap<u32, ActorRef>,
    drain_roots: BTreeMap<u32, ActorRef>,
    free: Vec<u32>,
    subs: routing::RouteTable,
    metrics: Metrics,
    closed: bool,
    operations: OperationTable,
    publications: publications::PublicationTable,
    diagnostics: diagnostics::History,
    next_failure: u64,
    failure_epoch: Instant,
    last_error: Option<Arc<FailureRecord>>,
    schemas: schema::SchemaRegistry,
    interfaces: HashMap<(String, u32), InterfaceSpec>,
    services: services::Registry,
}
struct Inner {
    id: u64,
    cfg: Config,
    retained: Arc<AtomicUsize>,
    next_event: AtomicU64,
    next_sub: AtomicU64,
    epoch: Instant,
    clock: Clock,
    state: Mutex<State>,
    python_ready_changed: Condvar,
    activity: Mutex<activity::ActivityRegistry>,
    routing_signal: Mutex<publications::RoutingSignal>,
}
#[derive(Clone)]
pub struct World {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct HeldPayload {
    stored: Arc<Stored>,
    world: u64,
    envelope: Option<Arc<Envelope>>,
}
impl HeldPayload {
    pub fn world(&self) -> u64 {
        self.world
    }
    pub fn payload(&self) -> &Payload {
        &self.stored.payload
    }
    pub fn envelope(&self) -> Option<&Envelope> {
        self.envelope.as_deref()
    }
    pub fn options(&self) -> &MessageOptions {
        &self.stored.options
    }
    pub fn retained_bytes(&self) -> usize {
        self.stored.bytes
    }
}
#[derive(Clone)]
pub struct TaskLease {
    guard: Arc<TaskRegistration>,
}
struct TaskRegistration {
    inner: Arc<Inner>,
    owner: ActorRef,
}

static NEXT_WORLD: AtomicU64 = AtomicU64::new(1);
impl World {
    fn diagnostic(
        &self,
        state: &mut State,
        code: DiagnosticCode,
        actor: Option<ActorRef>,
        operation: Option<OperationId>,
    ) {
        state.diagnostics.record(code, actor, operation);
    }
    fn reject(&self, state: &mut State, error: &Error, actor: ActorRef) {
        state.metrics.rejected += 1;
        self.diagnostic(state, error.into(), Some(actor), None);
    }
    pub fn new(cfg: Config) -> Result<Self, Error> {
        Self::with_clock(cfg, Clock::system())
    }

    pub fn with_clock(cfg: Config, clock: Clock) -> Result<Self, Error> {
        if cfg.max_actors == 0
            || cfg.max_actors > u32::MAX as usize
            || cfg.mailbox_capacity == 0
            || cfg.mailbox_bytes == 0
            || cfg.native_payload_budget == 0
            || cfg.max_event_bytes == 0
            || cfg.max_operations == 0
            || cfg.max_services > 4096
            || cfg.max_service_leases > 65536
            || cfg.max_leases_per_actor > 1024
            || !(1..=65536).contains(&cfg.max_publications)
            || !(1..=cfg.max_publications).contains(&cfg.max_publications_per_source)
            || !(1..=65536).contains(&cfg.max_publication_fanout)
            || !(1..=1048576).contains(&cfg.max_routing_snapshot_entries)
            || !(1..=4096).contains(&cfg.routing_batch_size)
        {
            return Err(Error::InvalidConfig);
        }
        let world_id = NEXT_WORLD.fetch_add(1, Ordering::Relaxed);
        let epoch = clock.now();
        Ok(Self {
            inner: Arc::new(Inner {
                id: world_id,
                retained: Arc::new(AtomicUsize::new(0)),
                next_event: AtomicU64::new(1),
                next_sub: AtomicU64::new(1),
                epoch,
                clock: clock.clone(),
                state: Mutex::new(State {
                    actors: Vec::new(),
                    live_actors: 0,
                    next_ready_order: 1,
                    python_ready: readiness::PythonReadyIndex::new(),
                    event_deadlines: expiry::DeadlineIndex::new(),
                    staged_actors: BTreeMap::new(),
                    drain_roots: BTreeMap::new(),
                    free: Vec::new(),
                    subs: routing::RouteTable::new(),
                    metrics: Metrics::default(),
                    closed: false,
                    operations: OperationTable::new(world_id, cfg.max_operations)?,
                    publications: publications::PublicationTable::new(
                        cfg.max_publications,
                        cfg.max_publications_per_source,
                    ),
                    diagnostics: diagnostics::History::new(
                        cfg.diagnostic_capacity,
                        epoch,
                        clock.clone(),
                    ),
                    next_failure: 1,
                    failure_epoch: epoch,
                    last_error: None,
                    interfaces: HashMap::new(),
                    schemas: schema::SchemaRegistry::new(cfg.max_schemas)
                        .map_err(|_| Error::InvalidConfig)?,
                    services: services::Registry::new(),
                }),
                activity: Mutex::new(activity::ActivityRegistry::new()),
                routing_signal: Mutex::new(publications::RoutingSignal::default()),
                python_ready_changed: Condvar::new(),
                cfg,
            }),
        })
    }
    pub fn id(&self) -> u64 {
        self.inner.id
    }
    pub fn now(&self) -> Instant {
        self.inner.clock.now()
    }
    pub fn is_virtual(&self) -> bool {
        self.inner.clock.is_virtual()
    }
    /// Atomically install collision-checked native conversion plans.
    pub fn register_schemas(
        &self,
        schemas: Vec<schema::Schema>,
    ) -> Result<Vec<u32>, schema::SchemaError> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed
            && !state
                .actors
                .iter()
                .flatten()
                .any(|actor| actor.state == Lifecycle::Quiescing || actor.in_flight)
        {
            return Err(schema::SchemaError::new("World is closed"));
        }
        state.schemas.register_batch(schemas)
    }
    pub fn schema(&self, id: u32) -> Result<Arc<schema::Schema>, schema::SchemaError> {
        self.inner.state.lock().unwrap().schemas.schema(id)
    }
    pub fn schema_count(&self) -> usize {
        self.inner.state.lock().unwrap().schemas.len()
    }
    /// Immutable World admission limits, also used to validate native adapters.
    pub fn config(&self) -> &Config {
        &self.inner.cfg
    }
    /// Copy canonical data into native ownership after validation. Registration
    /// metadata is held by the World; a payload cannot cross its World budget.
    pub fn structured(&self, id: u32, bytes: &[u8]) -> Result<Payload, schema::SchemaError> {
        if bytes.len().saturating_add(4) > self.inner.cfg.max_event_bytes {
            return Err(schema::SchemaError::new(
                "event exceeds configured byte limit",
            ));
        }
        let descriptor = {
            let state = self.inner.state.lock().unwrap();
            state.schemas.schema(id)?
        };
        descriptor.validate_registered_bytes(bytes)?;
        Ok(Payload::Structured(StructuredRecord {
            world: self.id(),
            schema: id,
            bytes: bytes.to_vec().into_boxed_slice(),
        }))
    }
    pub fn diagnostics(&self, after_sequence: u64, limit: usize) -> Result<DiagnosticRead, Error> {
        let st = self.inner.state.lock().unwrap();
        if limit > self.inner.cfg.diagnostic_capacity {
            return Err(Error::LimitExceeded);
        }
        Ok(st.diagnostics.read(after_sequence, limit))
    }
    fn check_ref<'a>(&self, r: ActorRef, st: &'a State) -> Result<&'a Actor, Error> {
        if r.world != self.id() {
            return Err(Error::CrossWorld);
        };
        let a = st
            .actors
            .get(r.slot as usize)
            .and_then(|x| x.as_ref())
            .ok_or(Error::StaleReference)?;
        if a.reference.generation != r.generation {
            return Err(Error::StaleReference);
        };
        Ok(a)
    }
    fn check_ref_mut<'a>(&self, r: ActorRef, st: &'a mut State) -> Result<&'a mut Actor, Error> {
        if r.world != self.id() {
            return Err(Error::CrossWorld);
        };
        let a = st
            .actors
            .get_mut(r.slot as usize)
            .and_then(|x| x.as_mut())
            .ok_or(Error::StaleReference)?;
        if a.reference.generation != r.generation {
            return Err(Error::StaleReference);
        };
        Ok(a)
    }
    pub fn allocate(
        &self,
        kind: EndpointKind,
        parent: Option<ActorRef>,
    ) -> Result<ActorRef, Error> {
        let _activity = self.activity_change();
        let mut st = self.inner.state.lock().unwrap();
        if st.closed {
            return Err(Error::ActorStopped);
        }
        self.allocate_locked(kind, parent, &mut st)
    }
    pub(crate) fn allocate_locked(
        &self,
        kind: EndpointKind,
        parent: Option<ActorRef>,
        st: &mut State,
    ) -> Result<ActorRef, Error> {
        if st.live_actors >= self.inner.cfg.max_actors {
            return Err(Error::LimitExceeded);
        };
        if let Some(p) = parent {
            let pa = self.check_ref(p, st)?;
            if pa.state != Lifecycle::Active && pa.state != Lifecycle::Starting {
                return Err(Error::ActorStopped);
            }
            let mut depth = 1usize;
            let mut ancestor = pa.parent;
            while let Some(next) = ancestor {
                depth += 1;
                if depth > 128 {
                    return Err(Error::LimitExceeded);
                }
                ancestor = self.check_ref(next, st)?.parent;
            }
        }
        let next_ready_order = st
            .next_ready_order
            .checked_add(1)
            .ok_or(Error::LimitExceeded)?;
        let slot = st.free.pop().unwrap_or(st.actors.len() as u32);
        let generation = st
            .actors
            .get(slot as usize)
            .and_then(|a| a.as_ref())
            .map_or(1, |a| a.reference.generation + 1);
        let r = ActorRef {
            world: self.id(),
            slot,
            generation,
        };
        let a = Actor {
            reference: r,
            ready_order: st.next_ready_order,
            kind,
            state: Lifecycle::Starting,
            parent,
            children: Vec::new(),
            queue: VecDeque::new(),
            queue_bytes: 0,
            staged: VecDeque::new(),
            staged_bytes: 0,
            deadlines: expiry::ActorDeadlines::new(),
            in_flight: false,
            control_in_flight: false,
            notification: None,
            pending_failures: 0,
            reported_failure: None,
            native_tasks: 0,
            python_done: kind == EndpointKind::Native,
            drain_root: None,
            drain_deadline: None,
            publication_cutoff: None,
            drain_timed_out: false,
            errors: 0,
            last_error: None,
            component: None,
        };
        if slot as usize == st.actors.len() {
            st.actors.push(Some(a))
        } else {
            st.actors[slot as usize] = Some(a)
        };
        if let Some(p) = parent {
            self.check_ref_mut(p, st)?.children.push(r);
        }
        st.live_actors += 1;
        st.next_ready_order = next_ready_order;
        self.refresh_python_ready_locked(r, st);
        self.mark_activity_dependents_locked(r, st);
        Ok(r)
    }
    pub fn activate(&self, r: ActorRef) -> Result<(), Error> {
        let mut st = self.inner.state.lock().unwrap();
        let parent = self.check_ref(r, &st)?.parent;
        if let Some(p) = parent {
            let pa = self.check_ref(p, &st)?;
            if pa.state != Lifecycle::Starting && pa.state != Lifecycle::Active {
                return Err(Error::ActorStopped);
            }
        }
        let a = self.check_ref_mut(r, &mut st)?;
        if a.state != Lifecycle::Starting {
            return Err(Error::ActorStopped);
        }
        a.state = Lifecycle::Active;
        self.refresh_python_subtree_locked(r, &mut st);
        self.mark_activity_tree_locked(r, &st);
        self.flush_staged_locked(&mut st);
        drop(st);
        self.touch_activity();
        Ok(())
    }
    fn reserve(&self, n: usize) -> Result<(), Error> {
        let mut old = self.inner.retained.load(Ordering::Acquire);
        loop {
            if n > self.inner.cfg.native_payload_budget.saturating_sub(old) {
                return Err(Error::BudgetExceeded);
            };
            match self.inner.retained.compare_exchange_weak(
                old,
                old + n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(v) => old = v,
            }
        }
    }
    fn make_stored_with(
        &self,
        p: Payload,
        options: MessageOptions,
        dispatcher: Option<PortRef>,
    ) -> Result<Arc<Stored>, Error> {
        options.validate()?;
        if options
            .source
            .is_some_and(|source| source.world != self.id())
        {
            return Err(Error::CrossWorld);
        }
        if let Payload::Structured(record) = &p
            && record.world != self.id()
        {
            return Err(Error::CrossWorld);
        }
        let n = p
            .bytes()
            .checked_add(options.charged_bytes())
            .ok_or(Error::LimitExceeded)?;
        if n > self.inner.cfg.max_event_bytes {
            return Err(Error::LimitExceeded);
        }
        self.reserve(n)?;
        Ok(Arc::new(Stored {
            payload: p,
            options,
            dispatcher,
            bytes: n,
            retained: self.inner.retained.clone(),
        }))
    }
    pub fn hold(&self, p: Payload) -> Result<HeldPayload, Error> {
        self.hold_with(p, MessageOptions::default())
    }
    pub fn hold_with(&self, p: Payload, options: MessageOptions) -> Result<HeldPayload, Error> {
        let state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::ActorStopped);
        }
        self.validate_source(&options, &state, true)?;
        self.check_deadline(&options, self.now())?;
        Ok(HeldPayload {
            stored: self.make_stored_with(p, options, None)?,
            world: self.id(),
            envelope: None,
        })
    }
    fn admit(
        &self,
        r: ActorRef,
        stored: Arc<Stored>,
        sub: Option<u64>,
        completion: bool,
        operation: Option<OperationId>,
        st: &mut State,
    ) -> Result<u64, Error> {
        let now = self.now();
        self.check_deadline(&stored.options, now)?;
        self.validate_source(&stored.options, st, completion)?;
        let id = self.next_event_id()?;
        let mut envelope = self.envelope_for(r, &stored, sub, id, now, st)?;
        if operation.is_some() && envelope.correlation_id.is_none() {
            envelope.correlation_id = Some(id);
        }
        let execution = self.execution_state_locked(r, st)?;
        let a = self.check_ref_mut(r, st)?;
        if execution != Lifecycle::Active && !(completion && execution == Lifecycle::Quiescing) {
            return Err(
                if execution == Lifecycle::Starting || a.state == Lifecycle::Starting {
                    Error::ActorNotReady
                } else {
                    Error::ActorStopped
                },
            );
        };
        if let Some(descriptor) = &a.component
            && !descriptor.ports.iter().any(|port| {
                port.direction == PortDirection::Input && port.schema.matches(&stored.payload)
            })
        {
            return Err(Error::SchemaMismatch);
        }
        if a.queue.len() >= self.inner.cfg.mailbox_capacity
            || a.queue_bytes + stored.bytes > self.inner.cfg.mailbox_bytes
        {
            return Err(Error::QueueFull);
        };
        a.queue_bytes += stored.bytes;
        a.deadlines.add(envelope.deadline);
        a.queue.push_back(Delivery {
            event_id: id,
            envelope,
            payload: stored,
            subscription: sub,
            operation,
        });
        let earliest = a.deadlines.first();
        st.event_deadlines.set(r, earliest);
        self.refresh_python_ready_locked(r, st);
        st.metrics.admitted += 1;
        self.mark_activity_dependents_locked(r, st);
        Ok(id)
    }
    pub fn send(&self, r: ActorRef, p: Payload) -> Result<u64, Error> {
        self.send_with(r, p, MessageOptions::default())
    }
    pub fn send_with(
        &self,
        r: ActorRef,
        p: Payload,
        options: MessageOptions,
    ) -> Result<u64, Error> {
        let mut st = self.inner.state.lock().unwrap();
        st.metrics.submitted += 1;
        let stored = match self.make_stored_with(p, options, None) {
            Ok(x) => x,
            Err(e) => {
                self.reject(&mut st, &e, r);
                return Err(e);
            }
        };
        match self.admit(r, stored, None, false, None, &mut st) {
            Ok(id) => {
                drop(st);
                self.touch_activity();
                Ok(id)
            }
            Err(e) => {
                self.reject(&mut st, &e, r);
                Err(e)
            }
        }
    }
    pub fn send_held(&self, r: ActorRef, held: HeldPayload) -> Result<u64, Error> {
        let mut st = self.inner.state.lock().unwrap();
        st.metrics.submitted += 1;
        if held.world != self.id() {
            self.reject(&mut st, &Error::CrossWorld, r);
            return Err(Error::CrossWorld);
        }
        match self.admit(r, held.stored.clone(), None, false, None, &mut st) {
            Ok(id) => {
                drop(st);
                self.touch_activity();
                Ok(id)
            }
            Err(e) => {
                self.reject(&mut st, &e, r);
                Err(e)
            }
        }
    }
    pub fn send_held_from(
        &self,
        owner: ActorRef,
        target: ActorRef,
        held: HeldPayload,
    ) -> Result<u64, Error> {
        let mut st = self.inner.state.lock().unwrap();
        st.metrics.submitted += 1;
        if held.world != self.id() {
            self.reject(&mut st, &Error::CrossWorld, target);
            return Err(Error::CrossWorld);
        }
        match self.check_ref(owner, &st) {
            Ok(a)
                if a.state == Lifecycle::Active
                    && self.execution_state_locked(owner, &st)? == Lifecycle::Active => {}
            Ok(_) => {
                self.reject(&mut st, &Error::ActorStopped, owner);
                return Err(Error::ActorStopped);
            }
            Err(e) => {
                self.reject(&mut st, &e, owner);
                return Err(e);
            }
        }
        match self.admit(target, held.stored.clone(), None, false, None, &mut st) {
            Ok(id) => {
                drop(st);
                self.touch_activity();
                Ok(id)
            }
            Err(e) => {
                self.reject(&mut st, &e, target);
                Err(e)
            }
        }
    }
    pub fn request(
        &self,
        owner: ActorRef,
        target: ActorRef,
        payload: Payload,
        deadline: Instant,
    ) -> Result<OperationId, Error> {
        self.request_with(owner, target, payload, deadline, MessageOptions::default())
    }
    pub fn request_with(
        &self,
        owner: ActorRef,
        target: ActorRef,
        payload: Payload,
        deadline: Instant,
        options: MessageOptions,
    ) -> Result<OperationId, Error> {
        self.request_with_port(owner, target, payload, deadline, options, None)
    }
    pub(crate) fn request_with_port(
        &self,
        owner: ActorRef,
        target: ActorRef,
        payload: Payload,
        deadline: Instant,
        mut options: MessageOptions,
        destination_port: Option<PortRef>,
    ) -> Result<OperationId, Error> {
        let _activity = self.activity_change();
        if options.source.is_some_and(|source| source != owner) {
            return Err(Error::InvalidMetadata);
        }
        options.source = Some(owner);
        let deadline = options
            .deadline
            .map_or(deadline, |other| other.min(deadline));
        options.deadline = Some(deadline);
        let mut st = self.inner.state.lock().unwrap();
        st.metrics.submitted += 1;
        let owner_ready = match self.execution_state_locked(owner, &st) {
            Ok(v) => v,
            Err(e) => {
                self.reject(&mut st, &e, owner);
                return Err(e);
            }
        };
        let target_ready = match self.execution_state_locked(target, &st) {
            Ok(v) => v,
            Err(e) => {
                self.reject(&mut st, &e, target);
                return Err(e);
            }
        };
        if owner_ready != Lifecycle::Active || target_ready != Lifecycle::Active {
            self.reject(&mut st, &Error::ActorNotReady, target);
            return Err(Error::ActorNotReady);
        }
        let id = match st.operations.reserve(owner, target, deadline) {
            Ok(id) => id,
            Err(e) => {
                self.reject(&mut st, &e, owner);
                return Err(e);
            }
        };
        let stored = match self.make_stored_with(payload, options, None) {
            Ok(x) => x,
            Err(e) => {
                let _ = st.operations.release_unsubmitted(id);
                self.reject(&mut st, &e, target);
                return Err(e);
            }
        };
        let event_id = match self.admit(target, stored.clone(), None, false, Some(id), &mut st) {
            Ok(event_id) => event_id,
            Err(e) => {
                let _ = st.operations.release_unsubmitted(id);
                self.reject(&mut st, &e, target);
                return Err(e);
            }
        };
        if let Some(port) = destination_port {
            self.check_ref_mut(target, &mut st)?
                .queue
                .back_mut()
                .expect("admitted request")
                .envelope
                .destination_port = Some(port);
        }
        let envelope = self
            .check_ref(target, &st)?
            .queue
            .back()
            .expect("admitted request")
            .envelope
            .clone();
        debug_assert_eq!(envelope.event_id, event_id);
        st.operations.set_context(
            id,
            HeldPayload {
                stored,
                world: self.id(),
                envelope: Some(Arc::new(envelope)),
            },
        )?;
        self.mark_activity_dependents_locked(owner, &st);
        Ok(id)
    }
    pub fn complete_operation(
        &self,
        id: OperationId,
        target: ActorRef,
        payload: Payload,
    ) -> Result<bool, Error> {
        self.complete_operation_with(id, target, payload, None)
    }
    pub fn complete_operation_with(
        &self,
        id: OperationId,
        target: ActorRef,
        payload: Payload,
        options: Option<MessageOptions>,
    ) -> Result<bool, Error> {
        let _activity = self.activity_change();
        let mut st = self.inner.state.lock().unwrap();
        if self
            .expire_operation_locked(id, self.now(), &mut st)
            .unwrap_or(false)
        {
            st.metrics.operation_timed_out += 1;
            self.diagnostic(
                &mut st,
                DiagnosticCode::OperationTimedOut,
                Some(target),
                Some(id),
            );
        }
        let pending = match st.operations.status(id) {
            Ok(OperationStatus::Pending(p)) => p,
            Ok(OperationStatus::Terminal(_)) => {
                st.metrics.operation_late_results += 1;
                self.diagnostic(
                    &mut st,
                    DiagnosticCode::OperationLateResult,
                    Some(target),
                    Some(id),
                );
                return Ok(false);
            }
            Err(Error::StaleReference) => {
                st.metrics.operation_late_results += 1;
                self.diagnostic(
                    &mut st,
                    DiagnosticCode::OperationLateResult,
                    Some(target),
                    Some(id),
                );
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        if pending.target != target {
            return Err(Error::StaleReference);
        }
        if !matches!(
            self.execution_state_locked(pending.owner, &st)?,
            Lifecycle::Active | Lifecycle::Quiescing
        ) || !matches!(
            self.execution_state_locked(target, &st)?,
            Lifecycle::Active | Lifecycle::Quiescing
        ) {
            return Err(Error::ActorStopped);
        }
        let mut metadata = pending
            .context
            .as_ref()
            .and_then(HeldPayload::envelope)
            .map_or_else(MessageOptions::default, |envelope| {
                envelope.child_options(target)
            });
        if let Some(options) = options {
            options.validate()?;
            if options.source.is_some_and(|source| source != target) {
                return Err(Error::InvalidMetadata);
            }
            metadata.deadline = match (metadata.deadline, options.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            metadata.correlation_id = options.correlation_id.or(metadata.correlation_id);
            metadata.causation_id = options.causation_id.or(metadata.causation_id);
            metadata.trace = options.trace.or(metadata.trace);
        }
        metadata.source = Some(target);
        if self.check_deadline(&metadata, self.now()).is_err() {
            if self.complete_operation_locked(id, TerminalOutcome::TimedOut, &mut st)? {
                st.metrics.operation_timed_out += 1;
                self.diagnostic(
                    &mut st,
                    DiagnosticCode::OperationTimedOut,
                    Some(target),
                    Some(id),
                );
            }
            return Ok(false);
        }
        let result = match self.make_stored_with(payload, metadata, None) {
            Ok(stored) => {
                let envelope = self.envelope_for(
                    pending.owner,
                    &stored,
                    None,
                    self.next_event_id()?,
                    self.now(),
                    &st,
                )?;
                HeldPayload {
                    stored,
                    world: self.id(),
                    envelope: Some(Arc::new(envelope)),
                }
            }
            Err(_) => {
                let won = self.complete_operation_locked(
                    id,
                    TerminalOutcome::Failed {
                        code: OperationFailure::ResultTooLarge,
                        message: "operation result exceeds configured budget".into(),
                    },
                    &mut st,
                )?;
                if won {
                    st.metrics.operation_completed += 1;
                }
                return Ok(won);
            }
        };
        if self.check_deadline(result.options(), self.now()).is_err() {
            if self.complete_operation_locked(id, TerminalOutcome::TimedOut, &mut st)? {
                st.metrics.operation_timed_out += 1;
                self.diagnostic(
                    &mut st,
                    DiagnosticCode::OperationTimedOut,
                    Some(target),
                    Some(id),
                );
            }
            return Ok(false);
        }
        let won =
            self.complete_operation_locked(id, TerminalOutcome::Completed(result), &mut st)?;
        if won {
            st.metrics.operation_completed += 1;
        } else {
            st.metrics.operation_late_results += 1;
            self.diagnostic(
                &mut st,
                DiagnosticCode::OperationLateResult,
                Some(target),
                Some(id),
            );
        }
        Ok(won)
    }
    pub fn cancel_operation(&self, id: OperationId) -> Result<bool, Error> {
        let _activity = self.activity_change();
        let mut st = self.inner.state.lock().unwrap();
        let won = self.complete_operation_locked(id, TerminalOutcome::Cancelled, &mut st)?;
        if won {
            st.metrics.operation_cancelled += 1;
        }
        Ok(won)
    }
    pub fn operation_status(&self, id: OperationId) -> Result<OperationStatus, Error> {
        self.inner.state.lock().unwrap().operations.status(id)
    }
    pub fn take_operation(&self, id: OperationId) -> Result<Option<TerminalOutcome>, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let (owner, _) = state.operations.participants(id)?;
        let result = state.operations.take(id)?;
        if result.is_some() {
            self.mark_activity_dependents_locked(owner, &state);
        }
        Ok(result)
    }
    pub fn subscribe(&self, source: ActorRef, target: ActorRef) -> Result<u64, Error> {
        let mut st = self.inner.state.lock().unwrap();
        let source_state = self.check_ref(source, &st)?.state;
        let target_state = self.check_ref(target, &st)?.state;
        if !matches!(source_state, Lifecycle::Starting | Lifecycle::Active)
            || !matches!(target_state, Lifecycle::Starting | Lifecycle::Active)
        {
            return Err(Error::ActorStopped);
        };
        if st.subs.active_len() >= self.inner.cfg.max_subscriptions {
            return Err(Error::LimitExceeded);
        };
        if st.subs.source_target_exists(source, target) {
            return Err(Error::DuplicateSubscription);
        };
        let id = self.inner.next_sub.fetch_add(1, Ordering::Relaxed);
        st.subs.insert(
            id,
            routing::Sub {
                owner: source,
                source,
                target,
                source_port: None,
                target_port: None,
            },
        );
        self.mark_route_activity_locked(st.subs.get(&id).expect("inserted route"), &st);
        drop(st);
        self.touch_activity();
        Ok(id)
    }
    pub fn unsubscribe(&self, id: u64) -> Result<(), Error> {
        let mut st = self.inner.state.lock().unwrap();
        let route = st.subs.remove(&id).ok_or(Error::NotFound)?;
        self.mark_route_activity_locked(&route, &st);
        drop(st);
        self.touch_activity();
        Ok(())
    }
    pub fn publish(&self, source: ActorRef, p: Payload) -> Result<PublicationTicket, Error> {
        self.publish_with(source, p, MessageOptions::default())
    }
    pub fn publish_with(
        &self,
        source: ActorRef,
        p: Payload,
        options: MessageOptions,
    ) -> Result<PublicationTicket, Error> {
        self.publish_internal(source, p, None, None, options)
    }
    fn publish_internal(
        &self,
        source: ActorRef,
        p: Payload,
        completion_owner: Option<ActorRef>,
        source_port: Option<u16>,
        mut options: MessageOptions,
    ) -> Result<PublicationTicket, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        state.metrics.publication_submitted += 1;
        state.metrics.submitted += 1;
        let result = (|| {
            if options.source.is_some_and(|owner| owner != source) {
                return Err(Error::InvalidMetadata);
            }
            options.source = Some(source);
            self.validate_source(&options, &state, true)?;
            self.check_deadline(&options, self.now())?;
            if let Payload::Structured(record) = &p
                && record.world != self.id()
            {
                return Err(Error::CrossWorld);
            }
            if let Some(index) = source_port {
                let spec = self.port_locked(
                    PortRef {
                        owner: source,
                        index,
                    },
                    &state,
                )?;
                if spec.direction != PortDirection::Output || !spec.schema.matches(&p) {
                    return Err(Error::SchemaMismatch);
                }
            }
            let execution = self.execution_state_locked(source, &state)?;
            let staged = execution == Lifecycle::Starting
                && completion_owner.is_none()
                && source_port.is_some();
            if !staged
                && execution != Lifecycle::Active
                && !(completion_owner.is_some() && execution == Lifecycle::Quiescing)
            {
                return Err(Error::ActorNotReady);
            }
            if let Some(owner) = completion_owner {
                if !matches!(
                    self.execution_state_locked(owner, &state)?,
                    Lifecycle::Active | Lifecycle::Quiescing
                ) {
                    return Err(Error::ActorStopped);
                }
                let mut current = source;
                while current != owner {
                    current = self
                        .check_ref(current, &state)?
                        .parent
                        .ok_or(Error::CrossWorld)?;
                }
            }
            self.submit_publication_locked(
                source,
                source_port,
                p,
                options,
                completion_owner.is_some(),
                &mut state,
            )
        })();
        if let Err(error) = &result {
            state.metrics.publication_rejected += 1;
            self.reject(&mut state, error, source);
        }
        result
    }
    pub fn claim(&self, r: ActorRef) -> Result<Option<DeliveryLease>, Error> {
        let _activity = self.activity_change();
        let mut st = self.inner.state.lock().unwrap();
        {
            let a = self.check_ref(r, &st)?;
            if !matches!(
                self.execution_state_locked(r, &st)?,
                Lifecycle::Active | Lifecycle::Quiescing
            ) || a.in_flight
                || a.control_in_flight
            {
                return Ok(None);
            }
        }
        loop {
            let (d, earliest) = {
                let a = self.check_ref_mut(r, &mut st)?;
                match a.queue.pop_front() {
                    Some(d) => {
                        a.queue_bytes -= d.payload.bytes;
                        a.deadlines.remove(d.envelope.deadline);
                        (d, a.deadlines.first())
                    }
                    None => return Ok(None),
                }
            };
            st.event_deadlines.set(r, earliest);
            self.refresh_python_ready_locked(r, &mut st);
            self.mark_activity_dependents_locked(r, &st);
            let now = self.now();
            if d.envelope.deadline.is_some_and(|deadline| deadline <= now) {
                self.expire_delivery_locked(&d, now, &mut st);
                continue;
            }
            if let Some(operation) = d.operation {
                if self
                    .expire_operation_locked(operation, self.now(), &mut st)
                    .unwrap_or(false)
                {
                    st.metrics.operation_timed_out += 1;
                    self.diagnostic(
                        &mut st,
                        DiagnosticCode::OperationTimedOut,
                        Some(r),
                        Some(operation),
                    );
                }
                let valid_operation = matches!(
                    st.operations.status(operation),
                    Ok(OperationStatus::Pending(_))
                );
                if !valid_operation {
                    st.metrics.cancelled += 1;
                    continue;
                }
            }
            let valid = d
                .subscription
                .map(|id| st.subs.contains_active(id))
                .unwrap_or(true);
            if !valid {
                st.metrics.cancelled += 1;
                continue;
            }
            self.check_ref_mut(r, &mut st)?.in_flight = true;
            self.refresh_python_ready_locked(r, &mut st);
            st.metrics.started += 1;
            return Ok(Some(DeliveryLease {
                inner: self.inner.clone(),
                target: r,
                operation: d.operation,
                delivery: Some(d),
            }));
        }
    }
    pub fn stop(&self, r: ActorRef) -> Result<StopReport, Error> {
        let mut st = self.inner.state.lock().unwrap();
        let result = self.stop_locked(r, &mut st);
        drop(st);
        self.touch_activity();
        result
    }
    /// Reject new ingress and drain admitted deliveries under a shared deadline.
    pub fn request_drain(&self, r: ActorRef, deadline: Instant) -> Result<StopReport, Error> {
        let mut state = self.inner.state.lock().unwrap();
        self.drain_locked(r, deadline, &mut state)?;
        self.maintain_locked(self.now(), &mut state);
        let report = self.report_locked(r, &state);
        drop(state);
        self.touch_activity();
        report
    }
    pub fn drain_all(&self, deadline: Instant) -> StopReport {
        let mut state = self.inner.state.lock().unwrap();
        let discarded_before = state.metrics.discarded;
        state.closed = true;
        let roots: Vec<_> = state
            .actors
            .iter()
            .flatten()
            .filter(|a| a.parent.is_none() && a.state != Lifecycle::Stopped)
            .map(|a| a.reference)
            .collect();
        for root in &roots {
            let _ = self.drain_locked(*root, deadline, &mut state);
        }
        self.maintain_locked(self.now(), &mut state);
        let mut report = self.world_report_locked(&state);
        report.discarded = state.metrics.discarded.saturating_sub(discarded_before) as usize;
        drop(state);
        self.touch_activity();
        report
    }
    fn drain_locked(
        &self,
        reference: ActorRef,
        deadline: Instant,
        state: &mut State,
    ) -> Result<(), Error> {
        let actor = self.check_ref(reference, state)?;
        match actor.state {
            Lifecycle::Stopped | Lifecycle::Stopping => return Ok(()),
            Lifecycle::Starting => {
                self.stop_locked(reference, state)?;
                return Ok(());
            }
            Lifecycle::Quiescing => {
                let root = actor.drain_root.unwrap_or(reference);
                let actor = self.check_ref_mut(root, state)?;
                actor.drain_deadline = Some(
                    actor
                        .drain_deadline
                        .map_or(deadline, |old| old.min(deadline)),
                );
                self.mark_activity_dependents_locked(root, state);
                return Ok(());
            }
            Lifecycle::Active => {}
        }
        let deadline = self.mark_quiescing(reference, reference, deadline, state)?;
        self.check_ref_mut(reference, state)?.drain_deadline = Some(deadline);
        state.drain_roots.insert(reference.slot, reference);
        Ok(())
    }
    fn mark_quiescing(
        &self,
        reference: ActorRef,
        root: ActorRef,
        mut deadline: Instant,
        state: &mut State,
    ) -> Result<Instant, Error> {
        let publication_cutoff = state.publications.last_issued();
        let children = {
            let actor = self.check_ref_mut(reference, state)?;
            if matches!(actor.state, Lifecycle::Stopping | Lifecycle::Stopped) {
                return Ok(deadline);
            }
            if let Some(previous) = actor.drain_deadline {
                deadline = deadline.min(previous);
            }
            actor.state = Lifecycle::Quiescing;
            actor.publication_cutoff.get_or_insert(publication_cutoff);
            actor.drain_root = Some(root);
            actor.drain_deadline = None;
            actor.children.clone()
        };
        state.drain_roots.remove(&reference.slot);
        for child in children {
            deadline = self.mark_quiescing(child, root, deadline, state)?;
        }
        self.refresh_python_ready_locked(reference, state);
        self.mark_activity_dependents_locked(reference, state);
        Ok(deadline)
    }
    fn drained_locked(&self, reference: ActorRef, state: &State) -> bool {
        let Ok(actor) = self.check_ref(reference, state) else {
            return true;
        };
        actor.queue.is_empty()
            && actor.staged.is_empty()
            && state.publications.pending_for(reference) == 0
            && !actor.in_flight
            && !actor.control_in_flight
            && actor.notification.is_none()
            && actor.native_tasks == 0
            && state.operations.pending_for(actor.reference) == 0
            && state.operations.pending_for_target(actor.reference) == 0
            && actor
                .children
                .iter()
                .all(|child| self.drained_locked(*child, state))
    }
    /// Called by the native control service; it never invokes application code.
    pub fn maintain(&self, now: Instant) {
        let mut state = self.inner.state.lock().unwrap();
        let stamp = |state: &State| {
            (
                state.metrics.expired,
                state.metrics.operation_timed_out,
                state.metrics.publication_expired,
                state.drain_roots.len(),
            )
        };
        let before = stamp(&state);
        self.maintain_locked(now, &mut state);
        let changed = before != stamp(&state);
        drop(state);
        if changed {
            self.touch_activity();
        }
    }
    fn maintain_locked(&self, now: Instant, state: &mut State) {
        self.expire_events_locked(now, state);
        self.expire_publications_locked(now, state);
        let participants = state.operations.due_participants(now);
        let expired = state.operations.expire_ids(now);
        for participants in participants {
            self.mark_operation_activity_locked(
                participants.owner,
                Some(participants.target),
                state,
            );
        }
        state.metrics.operation_timed_out += expired.len() as u64;
        for id in expired {
            self.diagnostic(state, DiagnosticCode::OperationTimedOut, None, Some(id));
        }
        let roots: Vec<_> = state.drain_roots.values().copied().collect();
        for root in roots {
            if state.drain_roots.get(&root.slot) != Some(&root) {
                continue;
            }
            let expired = self
                .check_ref(root, state)
                .ok()
                .and_then(|a| a.drain_deadline)
                .is_some_and(|deadline| now >= deadline);
            let drained = self.drained_locked(root, state);
            if expired || drained {
                if expired && !drained {
                    self.check_ref_mut(root, state)
                        .expect("live drain root")
                        .drain_timed_out = true;
                    state.metrics.drain_timeouts += 1;
                    self.diagnostic(state, DiagnosticCode::DrainTimeout, Some(root), None);
                }
                let _ = self.stop_locked(root, state);
            }
        }
    }
    fn stop_locked(&self, r: ActorRef, st: &mut State) -> Result<StopReport, Error> {
        let discarded_before = st.metrics.discarded;
        self.fence_locked(r, st)?;
        let mut report = self.report_locked(r, st)?;
        report.discarded = st.metrics.discarded.saturating_sub(discarded_before) as usize;
        Ok(report)
    }
    fn fence_locked(&self, r: ActorRef, st: &mut State) -> Result<(), Error> {
        self.check_ref(r, st)?;
        self.cancel_source_publications_locked(r, st);
        st.event_deadlines.remove(r);
        st.staged_actors.remove(&r.slot);
        st.drain_roots.remove(&r.slot);
        self.detach_scope(r, st);
        self.detach_service(r, st);
        self.discard_notifications_for(r, st);
        let participants = st.operations.pending_participants_for(r);
        let cancelled = st.operations.cancel_owner(r) + st.operations.stop_target(r);
        for participants in participants {
            self.mark_operation_activity_locked(participants.owner, Some(participants.target), st);
        }
        st.metrics.operation_cancelled += cancelled as u64;
        for _ in 0..cancelled {
            self.diagnostic(st, DiagnosticCode::Cancelled, Some(r), None);
        }
        let children = self.check_ref(r, st)?.children.clone();
        for c in children {
            self.fence_locked(c, st)?;
        }
        let already = {
            let a = self.check_ref(r, st)?;
            a.state == Lifecycle::Stopped
        };
        if already {
            return Ok(());
        }
        let route_ids = st.subs.actor_route_ids(r);
        for id in route_ids {
            if let Some(route) = st.subs.remove(&id) {
                self.mark_route_activity_locked(&route, st);
            }
        }
        let discarded = {
            let a = self.check_ref_mut(r, st)?;
            a.state = Lifecycle::Stopping;
            let mut n = 0;
            n += a.staged.len();
            a.staged.clear();
            a.staged_bytes = 0;
            a.deadlines.clear();
            while a.queue.pop_front().is_some() {
                n += 1;
            }
            a.queue_bytes = 0;
            n
        };
        st.metrics.cancelled += discarded as u64;
        st.metrics.discarded += discarded as u64;
        self.try_finalize(r, st);
        Ok(())
    }
    fn try_finalize(&self, r: ActorRef, st: &mut State) {
        self.refresh_python_ready_locked(r, st);
        self.mark_activity_dependents_locked(r, st);
        let (parent, done) = {
            let Some(Some(a)) = st.actors.get(r.slot as usize) else {
                return;
            };
            if a.reference != r {
                return;
            };
            let children_done = a.children.iter().all(|c| {
                st.actors
                    .get(c.slot as usize)
                    .and_then(|x| x.as_ref())
                    .map(|x| x.reference != *c || x.state == Lifecycle::Stopped)
                    .unwrap_or(true)
            });
            (
                a.parent,
                a.state == Lifecycle::Stopping
                    && !a.in_flight
                    && !a.control_in_flight
                    && a.notification.is_none()
                    && a.native_tasks == 0
                    && a.python_done
                    && children_done,
            )
        };
        if done {
            let mut finalized = false;
            if let Some(Some(a)) = st.actors.get_mut(r.slot as usize)
                && a.state != Lifecycle::Stopped
            {
                a.state = Lifecycle::Stopped;
                finalized = true;
            }
            if finalized {
                self.refresh_python_ready_locked(r, st);
                st.live_actors -= 1;
                st.operations.retire_owner(r);
                st.free.push(r.slot);
                let retired = st.actors[r.slot as usize].as_ref().unwrap();
                let timed_out = retired.drain_timed_out;
                let errors = retired.errors;
                let last_error = retired.last_error.clone();
                if let Some(p) = parent {
                    if let Some(Some(parent_actor)) = st.actors.get_mut(p.slot as usize)
                        && parent_actor.reference == p
                    {
                        parent_actor.children.retain(|child| *child != r);
                        parent_actor.drain_timed_out |= timed_out;
                        parent_actor.errors = parent_actor.errors.saturating_add(errors);
                        shutdown::keep_latest(&mut parent_actor.last_error, last_error.as_ref());
                    }
                    self.try_finalize(p, st);
                }
            }
        }
    }
    pub fn finish_python(&self, r: ActorRef) -> Result<(), Error> {
        let _activity = self.activity_change();
        let mut st = self.inner.state.lock().unwrap();
        self.check_ref_mut(r, &mut st)?.python_done = true;
        self.try_finalize(r, &mut st);
        Ok(())
    }
    pub fn track_task(&self, r: ActorRef) -> Result<TaskLease, Error> {
        let mut st = self.inner.state.lock().unwrap();
        let a = self.check_ref_mut(r, &mut st)?;
        if a.state != Lifecycle::Starting && a.state != Lifecycle::Active {
            return Err(Error::ActorStopped);
        };
        a.native_tasks += 1;
        self.mark_activity_dependents_locked(r, &st);
        drop(st);
        self.touch_activity();
        Ok(TaskLease {
            guard: Arc::new(TaskRegistration {
                inner: self.inner.clone(),
                owner: r,
            }),
        })
    }
    pub fn state(&self, r: ActorRef) -> Result<Lifecycle, Error> {
        let st = self.inner.state.lock().unwrap();
        Ok(self.check_ref(r, &st)?.state)
    }
    fn execution_state_locked(&self, r: ActorRef, st: &State) -> Result<Lifecycle, Error> {
        let mut current = self.check_ref(r, st)?;
        let mut saw_starting = false;
        let mut saw_quiescing = false;
        loop {
            match current.state {
                Lifecycle::Stopping | Lifecycle::Stopped => return Ok(Lifecycle::Stopping),
                Lifecycle::Starting => saw_starting = true,
                Lifecycle::Quiescing => saw_quiescing = true,
                Lifecycle::Active => {}
            }
            let Some(parent) = current.parent else {
                return Ok(if saw_quiescing {
                    Lifecycle::Quiescing
                } else if saw_starting {
                    Lifecycle::Starting
                } else {
                    Lifecycle::Active
                });
            };
            current = self.check_ref(parent, st)?;
        }
    }
    pub fn execution_state(&self, r: ActorRef) -> Result<Lifecycle, Error> {
        let st = self.inner.state.lock().unwrap();
        self.execution_state_locked(r, &st)
    }
    pub fn snapshot(&self) -> WorldSnapshot {
        let st = self.inner.state.lock().unwrap();
        WorldSnapshot {
            actors: st
                .actors
                .iter()
                .filter_map(|a| {
                    a.as_ref().map(|a| ActorSnapshot {
                        reference: a.reference,
                        kind: a.kind,
                        state: a.state,
                        queue_entries: a.queue.len(),
                        queue_bytes: a.queue_bytes,
                        staged_entries: a.staged.len(),
                        staged_bytes: a.staged_bytes,
                        component: a.component.clone(),
                        in_flight: a.in_flight,
                        control_in_flight: a.control_in_flight,
                        failure_pending: a.notification.is_some(),
                        native_tasks: a.native_tasks,
                        python_done: a.python_done,
                        parent: a.parent,
                        drain_deadline: a.drain_deadline,
                    })
                })
                .collect(),
            retained_payload_bytes: self.inner.retained.load(Ordering::Acquire),
            subscriptions: st.subs.active_len(),
            metrics: st.metrics.clone(),
            operation_pending: st.operations.pending_count(),
            operation_retained: st.operations.retained_terminal_count(),
            publications_pending: st.publications.pending(),
            publications_retained: st.publications.retained(),
            routing_snapshot_entries: st.publications.snapshot_entries,
        }
    }
    pub fn close(&self) -> StopReport {
        let mut st = self.inner.state.lock().unwrap();
        let report = self.close_locked(&mut st);
        drop(st);
        self.touch_activity();
        report
    }
    fn close_locked(&self, st: &mut State) -> StopReport {
        let discarded_before = st.metrics.discarded;
        st.closed = true;
        self.close_routing_locked();
        let refs: Vec<_> = st
            .actors
            .iter()
            .filter_map(|a| {
                a.as_ref()
                    .filter(|a| a.parent.is_none())
                    .map(|a| a.reference)
            })
            .collect();
        for r in refs {
            let _ = self.fence_locked(r, st);
        }
        let mut out = self.world_report_locked(st);
        out.discarded = st.metrics.discarded.saturating_sub(discarded_before) as usize;
        out
    }
}

pub struct DeliveryLease {
    inner: Arc<Inner>,
    target: ActorRef,
    operation: Option<OperationId>,
    delivery: Option<Delivery>,
}
impl DeliveryLease {
    pub fn envelope(&self) -> &Envelope {
        &self.delivery.as_ref().unwrap().envelope
    }
    pub fn payload(&self) -> &Payload {
        &self.delivery.as_ref().unwrap().payload.payload
    }
    pub fn event_id(&self) -> u64 {
        self.delivery.as_ref().unwrap().event_id
    }
    pub fn operation(&self) -> Option<OperationId> {
        self.operation
    }
    pub fn finish(mut self, success: bool) {
        self.complete(success)
    }
    fn complete(&mut self, success: bool) {
        if let Some(delivery) = self.delivery.take() {
            let event_id = delivery.event_id;
            drop(delivery);
            let world = World {
                inner: self.inner.clone(),
            };
            let mut st = self.inner.state.lock().unwrap();
            if let Some(Some(a)) = st.actors.get_mut(self.target.slot as usize) {
                a.in_flight = false;
            }
            st.metrics.completed += success as u64;
            st.metrics.failed += (!success) as u64;
            if !success
                && st.actors[self.target.slot as usize]
                    .as_ref()
                    .is_none_or(|a| a.reported_failure != Some(event_id))
            {
                st.diagnostics.record(
                    DiagnosticCode::HandlerFailed,
                    Some(self.target),
                    self.operation,
                );
            }
            if let Some(operation) = self.operation
                && !success
                && world
                    .complete_operation_locked(
                        operation,
                        TerminalOutcome::Failed {
                            code: OperationFailure::HandlerFailed,
                            message: "operation handler failed".into(),
                        },
                        &mut st,
                    )
                    .unwrap_or(false)
            {
                st.metrics.operation_completed += 1;
            }
            world.try_finalize(self.target, &mut st);
            drop(st);
            world.touch_activity();
        }
    }
}
impl Drop for DeliveryLease {
    fn drop(&mut self) {
        if self.delivery.is_some() {
            let world = World {
                inner: self.inner.clone(),
            };
            let mut st = self.inner.state.lock().unwrap();
            self.delivery.take();
            if let Some(Some(a)) = st.actors.get_mut(self.target.slot as usize) {
                a.in_flight = false;
            }
            st.metrics.cancelled += 1;
            if let Some(operation) = self.operation
                && world
                    .complete_operation_locked(operation, TerminalOutcome::Cancelled, &mut st)
                    .unwrap_or(false)
            {
                st.metrics.operation_cancelled += 1;
                World {
                    inner: self.inner.clone(),
                }
                .diagnostic(
                    &mut st,
                    DiagnosticCode::Cancelled,
                    Some(self.target),
                    Some(operation),
                );
            }
            world.try_finalize(self.target, &mut st);
            drop(st);
            world.touch_activity();
        }
    }
}
impl Drop for TaskRegistration {
    fn drop(&mut self) {
        let mut st = self.inner.state.lock().unwrap();
        if let Some(Some(a)) = st.actors.get_mut(self.owner.slot as usize)
            && a.reference == self.owner
            && a.native_tasks > 0
        {
            a.native_tasks -= 1;
        }
        World {
            inner: self.inner.clone(),
        }
        .try_finalize(self.owner, &mut st);
        drop(st);
        World {
            inner: self.inner.clone(),
        }
        .touch_activity();
    }
}

impl TaskLease {
    /// An already tracked native operation may publish its bounded completion
    /// while draining. A cancellation fence still rejects the publication.
    pub fn publish_completion(
        &self,
        source: ActorRef,
        payload: Payload,
    ) -> Result<PublicationTicket, Error> {
        World {
            inner: self.guard.inner.clone(),
        }
        .publish_internal(
            source,
            payload,
            Some(self.guard.owner),
            None,
            MessageOptions::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn world() -> World {
        World::new(Config {
            mailbox_capacity: 2,
            mailbox_bytes: 64,
            native_payload_budget: 128,
            ..Default::default()
        })
        .unwrap()
    }
    fn active(w: &World, k: EndpointKind) -> ActorRef {
        let r = w.allocate(k, None).unwrap();
        w.activate(r).unwrap();
        r
    }
    #[test]
    fn fifo_and_bounds() {
        let w = world();
        let a = active(&w, EndpointKind::Native);
        w.send(a, Payload::Pulse(1)).unwrap();
        w.send(a, Payload::Pulse(2)).unwrap();
        assert_eq!(w.send(a, Payload::Pulse(3)), Err(Error::QueueFull));
        let l = w.claim(a).unwrap().unwrap();
        assert_eq!(l.payload(), &Payload::Pulse(1));
        l.finish(true);
    }
    #[test]
    fn startup_rejects_send_but_allows_staged_route() {
        let w = world();
        let s = w.allocate(EndpointKind::Native, None).unwrap();
        let t = w.allocate(EndpointKind::Native, None).unwrap();
        assert_eq!(w.send(t, Payload::Pulse(1)), Err(Error::ActorNotReady));
        assert!(w.subscribe(s, t).is_ok());
        w.activate(s).unwrap();
        assert!(w.publish(s, Payload::Pulse(1)).is_ok());
        w.route_batch();
        assert!(w.claim(t).unwrap().is_none());
    }
    #[test]
    fn active_child_waits_for_starting_parent() {
        let w = world();
        let parent = w.allocate(EndpointKind::Native, None).unwrap();
        let child = w.allocate(EndpointKind::Native, Some(parent)).unwrap();
        w.activate(child).unwrap();
        assert_eq!(w.execution_state(child), Ok(Lifecycle::Starting));
        assert_eq!(w.send(child, Payload::Pulse(1)), Err(Error::ActorNotReady));
        assert_eq!(
            w.publish(child, Payload::Pulse(1)),
            Err(Error::ActorNotReady)
        );
        w.activate(parent).unwrap();
        assert_eq!(w.execution_state(child), Ok(Lifecycle::Active));
        w.send(child, Payload::Pulse(1)).unwrap();
    }
    #[test]
    fn stopping_endpoints_cannot_add_routes() {
        let w = world();
        let s = active(&w, EndpointKind::Native);
        let t = active(&w, EndpointKind::Native);
        w.stop(s).unwrap();
        assert_eq!(w.subscribe(s, t), Err(Error::ActorStopped));
    }
    #[test]
    fn stale_and_crossworld() {
        let w = world();
        let a = w.allocate(EndpointKind::Native, None).unwrap();
        assert_eq!(w.state(ActorRef { world: 99, ..a }), Err(Error::CrossWorld));
        w.activate(a).unwrap();
        w.stop(a).unwrap();
        assert_eq!(w.send(a, Payload::Pulse(1)), Err(Error::ActorStopped));
    }
    #[test]
    fn unsubscribe_fence_and_capacity_reclaimed() {
        let w = World::new(Config {
            max_subscriptions: 1,
            ..Config::default()
        })
        .unwrap();
        let s = active(&w, EndpointKind::Native);
        let t = active(&w, EndpointKind::Native);
        let id = w.subscribe(s, t).unwrap();
        w.unsubscribe(id).unwrap();
        assert!(w.subscribe(s, t).is_ok());
    }
    #[test]
    fn stop_removes_routes() {
        let w = World::new(Config {
            max_subscriptions: 1,
            ..Config::default()
        })
        .unwrap();
        let s = active(&w, EndpointKind::Native);
        let t = active(&w, EndpointKind::Native);
        w.subscribe(s, t).unwrap();
        w.stop(s).unwrap();
        let s2 = active(&w, EndpointKind::Native);
        assert!(w.subscribe(s2, t).is_ok());
    }
    #[test]
    fn max_actor_slot_reuse_and_close() {
        let w = World::new(Config {
            max_actors: 1,
            ..Config::default()
        })
        .unwrap();
        for _ in 0..8 {
            let a = active(&w, EndpointKind::Native);
            w.stop(a).unwrap();
        }
        let a = active(&w, EndpointKind::Native);
        w.close();
        assert_eq!(
            w.allocate(EndpointKind::Native, None),
            Err(Error::ActorStopped)
        );
        assert_eq!(w.state(a), Ok(Lifecycle::Stopped));
    }
    #[test]
    fn old_parent_cannot_stop_replacement_child() {
        let w = World::new(Config {
            max_actors: 4,
            ..Config::default()
        })
        .unwrap();
        let old = active(&w, EndpointKind::Native);
        let child = w.allocate(EndpointKind::Native, Some(old)).unwrap();
        w.activate(child).unwrap();
        w.stop(child).unwrap();
        let new_parent = active(&w, EndpointKind::Native);
        let replacement = w.allocate(EndpointKind::Native, Some(new_parent)).unwrap();
        w.activate(replacement).unwrap();
        w.stop(old).unwrap();
        assert_eq!(w.state(replacement), Ok(Lifecycle::Active));
    }
    #[test]
    fn finalized_children_are_unlinked_and_parent_can_finish() {
        let w = World::new(Config::default()).unwrap();
        let parent = active(&w, EndpointKind::Native);
        let other = active(&w, EndpointKind::Native);
        for _ in 0..100 {
            let child = w.allocate(EndpointKind::Native, Some(parent)).unwrap();
            w.activate(child).unwrap();
            w.stop(child).unwrap();
            let replacement = w.allocate(EndpointKind::Native, Some(other)).unwrap();
            w.activate(replacement).unwrap();
            w.stop(replacement).unwrap();
        }
        w.stop(parent).unwrap();
        assert_eq!(w.state(parent), Ok(Lifecycle::Stopped));
    }
    #[test]
    fn inflight_blocks_reuse_then_release() {
        let w = World::new(Config {
            max_actors: 1,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        w.send(a, Payload::Pulse(1)).unwrap();
        let lease = w.claim(a).unwrap().unwrap();
        w.stop(a).unwrap();
        assert_eq!(
            w.allocate(EndpointKind::Native, None),
            Err(Error::LimitExceeded)
        );
        lease.finish(true);
        let b = w.allocate(EndpointKind::Native, None).unwrap();
        assert_ne!(a.generation, b.generation);
    }
    #[test]
    fn repeated_finish_python_does_not_duplicate_free() {
        let w = World::new(Config {
            max_actors: 1,
            ..Config::default()
        })
        .unwrap();
        let a = w.allocate(EndpointKind::Python, None).unwrap();
        w.activate(a).unwrap();
        w.stop(a).unwrap();
        w.finish_python(a).unwrap();
        w.finish_python(a).unwrap();
        let b = w.allocate(EndpointKind::Native, None).unwrap();
        assert_eq!(
            w.allocate(EndpointKind::Native, None),
            Err(Error::LimitExceeded)
        );
        assert_ne!(a.generation, b.generation);
    }
    #[test]
    fn fanout_charges_physical_payload_once() {
        let w = world();
        let s = active(&w, EndpointKind::Native);
        let a = active(&w, EndpointKind::Native);
        let b = active(&w, EndpointKind::Native);
        w.subscribe(s, a).unwrap();
        w.subscribe(s, b).unwrap();
        w.publish(
            s,
            Payload::Record {
                schema: 1,
                integers: vec![1, 2],
            },
        )
        .unwrap();
        assert_eq!(w.snapshot().retained_payload_bytes, 20);
        w.route_batch();
        let x = w.claim(a).unwrap().unwrap();
        assert_eq!(w.snapshot().retained_payload_bytes, 20);
        x.finish(true);
        assert_eq!(w.snapshot().retained_payload_bytes, 20);
        let y = w.claim(b).unwrap().unwrap();
        y.finish(true);
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
    #[test]
    fn partial_fanout_isolates_full_target() {
        let w = world();
        let s = active(&w, EndpointKind::Native);
        let full = active(&w, EndpointKind::Native);
        let ok = active(&w, EndpointKind::Native);
        w.send(full, Payload::Pulse(0)).unwrap();
        w.send(full, Payload::Pulse(0)).unwrap();
        w.subscribe(s, full).unwrap();
        w.subscribe(s, ok).unwrap();
        let r = w.publish(s, Payload::Pulse(1)).unwrap();
        w.route_batch();
        let r = r.report().unwrap();
        assert_eq!(r.admitted, 1);
        assert_eq!(r.rejected, 1);
        assert!(w.claim(ok).unwrap().is_some());
    }
    #[test]
    fn payload_error_counts_rejected() {
        let w = World::new(Config {
            max_event_bytes: 4,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        assert_eq!(w.send(a, Payload::Pulse(1)), Err(Error::LimitExceeded));
        assert_eq!(w.snapshot().metrics.rejected, 1);
    }
    #[test]
    fn held_payload_accounts_until_timer_release() {
        let w = World::new(Config {
            native_payload_budget: 8,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        let held = w.hold(Payload::Pulse(1)).unwrap();
        assert_eq!(w.snapshot().retained_payload_bytes, 8);
        assert!(matches!(
            w.hold(Payload::Pulse(2)),
            Err(Error::BudgetExceeded)
        ));
        w.send_held(a, held).unwrap();
        assert_eq!(w.snapshot().retained_payload_bytes, 8);
        let lease = w.claim(a).unwrap().unwrap();
        lease.finish(true);
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
    #[test]
    fn dropped_held_payload_releases_budget() {
        let w = World::new(Config {
            native_payload_budget: 8,
            ..Config::default()
        })
        .unwrap();
        let held = w.hold(Payload::Pulse(1)).unwrap();
        assert_eq!(w.snapshot().retained_payload_bytes, 8);
        drop(held);
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
    #[test]
    fn held_payload_cannot_cross_world_budget() {
        let a = world();
        let b = world();
        let target = active(&b, EndpointKind::Native);
        let held = a.hold(Payload::Pulse(1)).unwrap();
        assert_eq!(b.send_held(target, held), Err(Error::CrossWorld));
        assert_eq!(a.snapshot().retained_payload_bytes, 0);
        assert_eq!(b.snapshot().retained_payload_bytes, 0);
    }
    #[test]
    fn tracked_task_delays_native_stop_and_slot_reuse() {
        let w = World::new(Config {
            max_actors: 1,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        let task = w.track_task(a).unwrap();
        let report = w.stop(a).unwrap();
        assert!(!report.native_done);
        assert_eq!(w.state(a), Ok(Lifecycle::Stopping));
        assert_eq!(
            w.allocate(EndpointKind::Native, None),
            Err(Error::LimitExceeded)
        );
        drop(task);
        assert_eq!(w.state(a), Ok(Lifecycle::Stopped));
        assert!(w.allocate(EndpointKind::Native, None).is_ok());
    }
    #[test]
    fn child_task_keeps_parent_unretired() {
        let w = World::new(Config {
            max_actors: 3,
            ..Config::default()
        })
        .unwrap();
        let p = active(&w, EndpointKind::Native);
        let c = w.allocate(EndpointKind::Native, Some(p)).unwrap();
        w.activate(c).unwrap();
        let task = w.track_task(c).unwrap();
        w.stop(p).unwrap();
        assert_eq!(w.state(c), Ok(Lifecycle::Stopping));
        assert_eq!(w.state(p), Ok(Lifecycle::Stopping));
        drop(task);
        assert_eq!(w.state(c), Ok(Lifecycle::Stopped));
        assert_eq!(w.state(p), Ok(Lifecycle::Stopped));
    }
    #[test]
    fn native_done_is_independent_of_python_cleanup() {
        let w = world();
        let p = w.allocate(EndpointKind::Python, None).unwrap();
        w.activate(p).unwrap();
        let report = w.stop(p).unwrap();
        assert!(report.native_done);
        assert!(!report.python_done);
        w.finish_python(p).unwrap();
    }
    #[test]
    fn execution_state_includes_ancestor_fence() {
        let w = world();
        let p = active(&w, EndpointKind::Native);
        let c = w.allocate(EndpointKind::Native, Some(p)).unwrap();
        w.activate(c).unwrap();
        assert_eq!(w.execution_state(c), Ok(Lifecycle::Active));
        w.stop(p).unwrap();
        assert_eq!(w.execution_state(c), Ok(Lifecycle::Stopping));
    }
    #[test]
    fn held_send_requires_active_owner() {
        let w = world();
        let owner = active(&w, EndpointKind::Native);
        let target = active(&w, EndpointKind::Native);
        let held = w.hold(Payload::Pulse(1)).unwrap();
        w.stop(owner).unwrap();
        assert_eq!(
            w.send_held_from(owner, target, held),
            Err(Error::ActorStopped)
        );
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
    #[test]
    fn concurrent_claim_stop_has_one_terminal_outcome() {
        for _ in 0..64 {
            let w = world();
            let a = active(&w, EndpointKind::Native);
            w.send(a, Payload::Pulse(1)).unwrap();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let claim_world = w.clone();
            let claim_barrier = barrier.clone();
            let claim = std::thread::spawn(move || {
                claim_barrier.wait();
                claim_world.claim(a).unwrap()
            });
            let stop_world = w.clone();
            let stop_barrier = barrier.clone();
            let stop = std::thread::spawn(move || {
                stop_barrier.wait();
                stop_world.stop(a).unwrap()
            });
            let lease = claim.join().unwrap();
            let _report = stop.join().unwrap();
            if let Some(lease) = lease {
                lease.finish(true);
            }
            assert_eq!(w.state(a), Ok(Lifecycle::Stopped));
            assert_eq!(w.snapshot().retained_payload_bytes, 0);
            assert_eq!(w.snapshot().actors[0].queue_entries, 0);
        }
    }
    #[test]
    fn mailbox_byte_budget_is_distinct_from_entry_budget() {
        let w = World::new(Config {
            mailbox_capacity: 8,
            mailbox_bytes: 8,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        w.send(a, Payload::Pulse(1)).unwrap();
        assert_eq!(w.send(a, Payload::Pulse(2)), Err(Error::QueueFull));
    }
    #[test]
    fn payload_capacity_is_charged() {
        let w = World::new(Config {
            max_event_bytes: 100,
            ..Config::default()
        })
        .unwrap();
        let a = active(&w, EndpointKind::Native);
        let mut integers = Vec::with_capacity(1000);
        integers.push(1);
        assert_eq!(
            w.send(
                a,
                Payload::Record {
                    schema: 1,
                    integers
                }
            ),
            Err(Error::LimitExceeded)
        );
    }
    #[test]
    fn lease_drop_and_finish_metrics_conserve_delivery_outcomes() {
        let w = world();
        let a = active(&w, EndpointKind::Native);
        w.send(a, Payload::Pulse(1)).unwrap();
        w.send(a, Payload::Pulse(2)).unwrap();
        drop(w.claim(a).unwrap().unwrap());
        w.claim(a).unwrap().unwrap().finish(true);
        let metrics = w.snapshot().metrics;
        assert_eq!(metrics.admitted, 2);
        assert_eq!(metrics.started, 2);
        assert_eq!(metrics.cancelled, 1);
        assert_eq!(metrics.completed, 1);
        assert_eq!(metrics.failed, 0);
    }
    #[test]
    fn ownership_depth_is_bounded() {
        let w = World::new(Config {
            max_actors: 200,
            ..Config::default()
        })
        .unwrap();
        let mut parent = active(&w, EndpointKind::Native);
        let mut rejected = false;
        for _ in 0..140 {
            match w.allocate(EndpointKind::Native, Some(parent)) {
                Ok(child) => {
                    w.activate(child).unwrap();
                    parent = child;
                }
                Err(Error::LimitExceeded) => {
                    rejected = true;
                    break;
                }
                Err(e) => panic!("unexpected allocation error: {e:?}"),
            }
        }
        assert!(rejected);
    }
}
