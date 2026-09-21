//! Bounded publication ingress, retained tickets, and cooperative fan-out turns.
use super::*;
use std::{
    collections::{BTreeSet, HashSet},
    task::{Context, Poll, Waker},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PublicationOutcome {
    #[default]
    Pending,
    Routed,
    Cancelled,
    Expired,
    FanoutLimit,
    SnapshotFull,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationStatus {
    Staged,
    Queued,
    Routing(DeliveryReport),
    Terminal(DeliveryReport),
}

struct TicketState {
    id: u64,
    source: ActorRef,
    status: Mutex<PublicationStatus>,
    _permit: TicketPermit,
}

/// Acceptance of native routing work, not a receipt for subscriber admission.
/// Clones share one bounded result slot. Dropping the last external handle does
/// not cancel accepted work; its slot is released when routing also finishes.
#[derive(Clone)]
pub struct PublicationTicket(Arc<TicketState>);
impl fmt::Debug for PublicationTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublicationTicket")
            .field("id", &self.id())
            .field("source", &self.source())
            .field("status", &self.status())
            .finish()
    }
}
impl PublicationTicket {
    pub fn id(&self) -> u64 {
        self.0.id
    }
    pub fn source(&self) -> ActorRef {
        self.0.source
    }
    pub fn status(&self) -> PublicationStatus {
        self.0.status.lock().unwrap().clone()
    }
    pub fn report(&self) -> Option<DeliveryReport> {
        match self.status() {
            PublicationStatus::Terminal(report) => Some(report),
            _ => None,
        }
    }
}
impl PartialEq for PublicationTicket {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id() && self.source().world == other.source().world
    }
}
impl Eq for PublicationTicket {}

struct TicketBudget {
    limit: usize,
    per_source: usize,
    counts: Mutex<(usize, HashMap<ActorRef, usize>)>,
}
struct TicketPermit {
    budget: Arc<TicketBudget>,
    source: ActorRef,
}
impl Drop for TicketPermit {
    fn drop(&mut self) {
        let mut counts = self.budget.counts.lock().unwrap();
        counts.0 -= 1;
        let count = counts
            .1
            .get_mut(&self.source)
            .expect("reserved ticket owner");
        *count -= 1;
        if *count == 0 {
            counts.1.remove(&self.source);
        }
    }
}

pub(crate) struct Publication {
    pub(crate) source: ActorRef,
    pub(crate) source_port: Option<u16>,
    pub(crate) stored: Arc<Stored>,
    staged: bool,
    completion: bool,
    ticket: PublicationTicket,
    routes: Option<Vec<(u64, ActorRef)>>,
    cursor: usize,
    report: DeliveryReport,
}

pub(crate) struct PublicationTable {
    entries: BTreeMap<u64, Publication>,
    by_source: HashMap<ActorRef, VecDeque<u64>>,
    ready: VecDeque<ActorRef>,
    ready_set: HashSet<ActorRef>,
    deadlines: BTreeSet<(Instant, u64)>,
    next_id: u64,
    budget: Arc<TicketBudget>,
    pub(crate) snapshot_entries: usize,
}
impl PublicationTable {
    pub(crate) fn new(limit: usize, per_source: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            by_source: HashMap::new(),
            ready: VecDeque::new(),
            ready_set: HashSet::new(),
            deadlines: BTreeSet::new(),
            next_id: 1,
            budget: Arc::new(TicketBudget {
                limit,
                per_source,
                counts: Mutex::new((0, HashMap::new())),
            }),
            snapshot_entries: 0,
        }
    }
    fn reserve(&mut self, source: ActorRef, staged: bool) -> Result<PublicationTicket, Error> {
        let next_id = self.next_id.checked_add(1).ok_or(Error::LimitExceeded)?;
        let mut counts = self.budget.counts.lock().unwrap();
        if counts.0 >= self.budget.limit
            || counts.1.get(&source).copied().unwrap_or(0) >= self.budget.per_source
        {
            return Err(Error::QueueFull);
        }
        counts.0 += 1;
        *counts.1.entry(source).or_default() += 1;
        drop(counts);
        let ticket = PublicationTicket(Arc::new(TicketState {
            id: self.next_id,
            source,
            status: Mutex::new(if staged {
                PublicationStatus::Staged
            } else {
                PublicationStatus::Queued
            }),
            _permit: TicketPermit {
                budget: self.budget.clone(),
                source,
            },
        }));
        self.next_id = next_id;
        Ok(ticket)
    }
    fn insert(&mut self, publication: Publication) {
        let id = publication.ticket.id();
        let source = publication.source;
        if let Some(deadline) = publication.stored.options.deadline {
            self.deadlines.insert((deadline, id));
        }
        self.by_source.entry(source).or_default().push_back(id);
        self.entries.insert(id, publication);
        self.ready_source(source);
    }
    fn ready_source(&mut self, source: ActorRef) {
        let ready = self
            .by_source
            .get(&source)
            .and_then(|ids| ids.front())
            .and_then(|id| self.entries.get(id))
            .is_some_and(|publication| !publication.staged);
        if ready && self.ready_set.insert(source) {
            self.ready.push_back(source);
        }
    }
    fn remove_indexes(&mut self, publication: &Publication) {
        let source = publication.source;
        let id = publication.ticket.id();
        if let Some(deadline) = publication.stored.options.deadline {
            self.deadlines.remove(&(deadline, id));
        }
        if let Some(routes) = &publication.routes {
            self.snapshot_entries -= routes.len();
        }
        if let Some(queue) = self.by_source.get_mut(&source) {
            queue.retain(|candidate| *candidate != id);
            if queue.is_empty() {
                self.by_source.remove(&source);
            }
        }
        if self.ready_set.remove(&source) {
            self.ready.retain(|candidate| *candidate != source);
        }
        self.ready_source(source);
    }
    pub(crate) fn pending_for(&self, source: ActorRef) -> usize {
        self.by_source.get(&source).map_or(0, VecDeque::len)
    }
    pub(crate) fn last_issued(&self) -> u64 {
        self.next_id - 1
    }
    pub(crate) fn pending(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn retained(&self) -> usize {
        self.budget.counts.lock().unwrap().0 - self.pending()
    }
    pub(crate) fn counts_for_scope(&self, scope: &HashSet<ActorRef>) -> (usize, usize) {
        let pending = scope.iter().map(|source| self.pending_for(*source)).sum();
        let counts = self.budget.counts.lock().unwrap();
        let total: usize = scope
            .iter()
            .map(|source| counts.1.get(source).copied().unwrap_or(0))
            .sum();
        (pending, total - pending)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoutingProgress {
    pub destinations: usize,
    pub publications: usize,
    pub pending: usize,
    pub ready: bool,
}

#[derive(Default)]
pub(crate) struct RoutingSignal {
    dirty: bool,
    waker: Option<Waker>,
    closed: bool,
}
impl World {
    fn mark_routing_locked(&self) {
        self.inner.routing_signal.lock().unwrap().dirty = true;
    }
    pub(crate) fn wake_routing(&self) {
        let waker = {
            let mut signal = self.inner.routing_signal.lock().unwrap();
            if !std::mem::take(&mut signal.dirty) {
                return;
            }
            signal.waker.take()
        };
        if let Some(waker) = waker {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake()));
        }
    }
    pub(crate) fn close_routing_locked(&self) {
        let mut signal = self.inner.routing_signal.lock().unwrap();
        signal.closed = true;
        signal.dirty = true;
    }
    /// One native router may await this readiness predicate. A new waiter
    /// replaces its predecessor. Waker cloning/disposal happens outside locks.
    /// Cancel-close is terminal (ActorStopped); drain keeps completion routing
    /// available. A terminal poll never retains the incoming observer.
    pub fn poll_routing_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let incoming = cx.waker().clone();
        let (ready, closed, retired) = {
            let state = self.inner.state.lock().unwrap();
            let mut signal = self.inner.routing_signal.lock().unwrap();
            if signal.closed {
                (false, true, signal.waker.take())
            } else if !state.publications.ready.is_empty() {
                (true, false, None)
            } else {
                (false, false, signal.waker.replace(incoming))
            }
        };
        drop(retired);
        if closed {
            Poll::Ready(Err(Error::ActorStopped))
        } else if ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    pub fn routing_pending(&self) -> usize {
        self.inner.state.lock().unwrap().publications.pending()
    }
    pub fn routing_ready(&self) -> bool {
        !self
            .inner
            .state
            .lock()
            .unwrap()
            .publications
            .ready
            .is_empty()
    }

    pub(crate) fn enqueue_publication_locked(&self, id: u64, state: &mut State) {
        if let Some(publication) = state.publications.entries.get_mut(&id) {
            publication.staged = false;
            *publication.ticket.0.status.lock().unwrap() = PublicationStatus::Queued;
            let source = publication.source;
            state.publications.ready_source(source);
            self.mark_activity_dependents_locked(source, state);
            self.mark_routing_locked();
        }
    }

    pub(crate) fn submit_publication_locked(
        &self,
        source: ActorRef,
        source_port: Option<u16>,
        payload: Payload,
        options: MessageOptions,
        completion: bool,
        state: &mut State,
    ) -> Result<PublicationTicket, Error> {
        let staged = self.execution_state_locked(source, state)? == Lifecycle::Starting;
        // Ticket/result capacity is reserved before the retained payload copy.
        let ticket = state.publications.reserve(source, staged)?;
        let stored = self.make_stored_with(
            payload,
            options,
            source_port.map(|index| PortRef {
                owner: source,
                index,
            }),
        )?;
        if staged {
            let actor = self.check_ref_mut(source, state)?;
            if actor.staged.len() >= self.inner.cfg.mailbox_capacity
                || stored.bytes
                    > self
                        .inner
                        .cfg
                        .mailbox_bytes
                        .saturating_sub(actor.staged_bytes)
            {
                return Err(Error::QueueFull);
            }
            actor.staged_bytes += stored.bytes;
            actor.deadlines.add(stored.options.deadline);
            actor.staged.push_back((
                source_port.expect("typed staged publication"),
                stored.clone(),
                ticket.id(),
            ));
            let earliest = actor.deadlines.first();
            state.event_deadlines.set(source, earliest);
            state.staged_actors.insert(source.slot, source);
        }
        state.publications.insert(Publication {
            source,
            source_port,
            stored,
            staged,
            completion,
            ticket: ticket.clone(),
            routes: None,
            cursor: 0,
            report: DeliveryReport {
                staged: usize::from(staged),
                ..Default::default()
            },
        });
        state.metrics.publication_admitted += 1;
        self.mark_activity_dependents_locked(source, state);
        if !staged {
            self.mark_routing_locked();
        }
        Ok(ticket)
    }

    fn finish_publication_locked(
        &self,
        mut publication: Publication,
        outcome: PublicationOutcome,
        state: &mut State,
    ) {
        publication.report.outcome = outcome;
        if publication.routes.is_some() {
            let remaining = publication.report.matched - publication.cursor;
            publication.report.rejected += remaining;
            state.metrics.rejected += remaining as u64;
        }
        state.publications.remove_indexes(&publication);
        *publication.ticket.0.status.lock().unwrap() =
            PublicationStatus::Terminal(publication.report);
        match outcome {
            PublicationOutcome::Routed => state.metrics.publication_completed += 1,
            PublicationOutcome::Expired => state.metrics.publication_expired += 1,
            PublicationOutcome::FanoutLimit | PublicationOutcome::SnapshotFull => {
                state.metrics.publication_failed += 1
            }
            _ => state.metrics.publication_cancelled += 1,
        }
        self.mark_activity_dependents_locked(publication.source, state);
        if !state.publications.ready.is_empty() {
            self.mark_routing_locked();
        }
    }
    pub(crate) fn cancel_publication_locked(
        &self,
        id: u64,
        outcome: PublicationOutcome,
        state: &mut State,
    ) {
        if let Some(publication) = state.publications.entries.remove(&id) {
            self.finish_publication_locked(publication, outcome, state);
        }
    }
    pub(crate) fn cancel_source_publications_locked(&self, source: ActorRef, state: &mut State) {
        let ids: Vec<_> = state
            .publications
            .by_source
            .get(&source)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .collect();
        for id in ids {
            self.cancel_publication_locked(id, PublicationOutcome::Cancelled, state);
        }
    }
    pub(crate) fn expire_publications_locked(&self, now: Instant, state: &mut State) {
        let due: Vec<_> = state
            .publications
            .deadlines
            .iter()
            .take_while(|(deadline, _)| *deadline <= now)
            .map(|(_, id)| *id)
            .collect();
        for id in due {
            self.cancel_publication_locked(id, PublicationOutcome::Expired, state);
        }
    }

    /// Route one source's oldest publication through at most routing_batch_size
    /// destination attempts. Snapshot copying is separately capped by the
    /// fan-out and aggregate snapshot-entry limits. Call again in a later turn.
    pub fn route_batch(&self) -> RoutingProgress {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let Some(source) = state.publications.ready.pop_front() else {
            return RoutingProgress {
                pending: state.publications.pending(),
                ..Default::default()
            };
        };
        state.publications.ready_set.remove(&source);
        let id = *state.publications.by_source[&source]
            .front()
            .expect("ready source head");
        let mut publication = state
            .publications
            .entries
            .remove(&id)
            .expect("ready publication");
        let mut outcome = None;
        if !matches!(
            self.execution_state_locked(source, &state),
            Ok(Lifecycle::Active | Lifecycle::Quiescing)
        ) {
            outcome = Some(PublicationOutcome::Cancelled);
        } else if self
            .check_deadline(&publication.stored.options, self.now())
            .is_err()
        {
            outcome = Some(PublicationOutcome::Expired);
        }
        if outcome.is_none() && publication.routes.is_none() {
            let count = state.subs.source_count(source, publication.source_port);
            if count > self.inner.cfg.max_publication_fanout {
                outcome = Some(PublicationOutcome::FanoutLimit);
            } else if count
                > self
                    .inner
                    .cfg
                    .max_routing_snapshot_entries
                    .saturating_sub(state.publications.snapshot_entries)
            {
                outcome = Some(PublicationOutcome::SnapshotFull);
            } else {
                publication.routes = Some(
                    state
                        .subs
                        .source_ids(source, publication.source_port)
                        .into_iter()
                        .map(|id| (id, state.subs.get(&id).expect("indexed route").target))
                        .collect(),
                );
                publication.report.matched = count;
                state.publications.snapshot_entries += count;
            }
        }
        let mut destinations = 0;
        if outcome.is_none() {
            let routes = publication.routes.as_ref().expect("captured snapshot");
            while publication.cursor < routes.len()
                && destinations < self.inner.cfg.routing_batch_size
            {
                if self
                    .check_deadline(&publication.stored.options, self.now())
                    .is_err()
                {
                    outcome = Some(PublicationOutcome::Expired);
                    break;
                }
                let (route, target) = routes[publication.cursor];
                publication.cursor += 1;
                destinations += 1;
                let result = (|| {
                    let route_owner = state.subs.get(&route).ok_or(Error::NotFound)?.owner;
                    match self.execution_state_locked(route_owner, &state)? {
                        Lifecycle::Active | Lifecycle::Quiescing => (),
                        Lifecycle::Starting => return Err(Error::ActorNotReady),
                        _ => return Err(Error::ActorStopped),
                    }
                    let target_actor = self.check_ref(target, &state)?;
                    if self.execution_state_locked(target, &state)? == Lifecycle::Quiescing
                        && !publication.completion
                        && target_actor
                            .publication_cutoff
                            .is_none_or(|cutoff| id > cutoff)
                    {
                        return Err(Error::ActorStopped);
                    }
                    // This work was already accepted from the source. The
                    // cutoff above separately fences new target ingress.
                    self.admit(
                        target,
                        publication.stored.clone(),
                        Some(route),
                        true,
                        None,
                        &mut state,
                    )
                })();
                match result {
                    Ok(_) => publication.report.admitted += 1,
                    Err(error) => {
                        publication.report.rejected += 1;
                        self.reject(&mut state, &error, target);
                    }
                }
            }
            if publication.cursor == routes.len() && outcome.is_none() {
                outcome = Some(PublicationOutcome::Routed);
            }
        }
        let publications = usize::from(outcome.is_some());
        if let Some(outcome) = outcome {
            self.finish_publication_locked(publication, outcome, &mut state);
        } else {
            *publication.ticket.0.status.lock().unwrap() =
                PublicationStatus::Routing(publication.report.clone());
            state.publications.entries.insert(id, publication);
            state.publications.ready_source(source);
        }
        RoutingProgress {
            destinations,
            publications,
            pending: state.publications.pending(),
            ready: !state.publications.ready.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn assert_indexes(world: &World) {
        let state = world.inner.state.lock().unwrap();
        let table = &state.publications;
        let mut indexed = BTreeSet::new();
        for (source, ids) in &table.by_source {
            assert!(!ids.is_empty());
            let mut previous = 0;
            for id in ids {
                assert!(*id > previous);
                previous = *id;
                assert!(indexed.insert(*id));
                assert_eq!(table.entries[id].source, *source);
            }
        }
        assert_eq!(indexed, table.entries.keys().copied().collect());
        let ready: HashSet<_> = table.ready.iter().copied().collect();
        assert_eq!(ready.len(), table.ready.len());
        assert_eq!(ready, table.ready_set);
        let expected_ready: HashSet<_> = table
            .by_source
            .iter()
            .filter_map(|(source, ids)| (!table.entries[&ids[0]].staged).then_some(*source))
            .collect();
        assert_eq!(ready, expected_ready);
        let deadlines: BTreeSet<_> = table
            .entries
            .iter()
            .filter_map(|(id, p)| p.stored.options.deadline.map(|d| (d, *id)))
            .collect();
        assert_eq!(deadlines, table.deadlines);
        assert_eq!(
            table.snapshot_entries,
            table
                .entries
                .values()
                .filter_map(|p| p.routes.as_ref())
                .map(Vec::len)
                .sum::<usize>()
        );
        assert!(table.snapshot_entries <= world.config().max_routing_snapshot_entries);
        let counts = table.budget.counts.lock().unwrap();
        assert_eq!(counts.0, counts.1.values().sum());
        assert!(counts.0 <= world.config().max_publications);
        assert!(
            counts
                .1
                .values()
                .all(|n| *n <= world.config().max_publications_per_source && *n > 0)
        );
    }

    #[test]
    fn indexes_and_reservations_remain_exact_through_expiry_stop_and_generation_reuse() {
        let (clock, time) = Clock::manual_at(Instant::now());
        let world = World::with_clock(
            Config {
                routing_batch_size: 1,
                max_publications: 8,
                max_publications_per_source: 4,
                ..Default::default()
            },
            clock,
        )
        .unwrap();
        let target = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(target).unwrap();
        for round in 0..100 {
            let source = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(source).unwrap();
            world.subscribe(source, target).unwrap();
            let ticket = world
                .publish_with(
                    source,
                    Payload::Pulse(round),
                    MessageOptions {
                        deadline: Some(world.now() + Duration::from_secs(1)),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_indexes(&world);
            let second = world.publish(source, Payload::Pulse(round)).unwrap();
            assert_indexes(&world);
            if round % 2 == 0 {
                world.route_batch();
            } else {
                time.advance(Duration::from_secs(1)).unwrap();
                world.maintain(world.now());
            }
            assert_indexes(&world);
            world.stop(source).unwrap();
            assert_indexes(&world);
            assert!(ticket.report().is_some());
            assert!(second.report().is_some());
            drop((ticket, second));
            while let Some(lease) = world.claim(target).unwrap() {
                lease.finish(true);
            }
            assert_indexes(&world);
            let state = world.inner.state.lock().unwrap();
            assert_eq!(state.publications.pending(), 0);
            assert_eq!(state.publications.retained(), 0);
            assert!(state.publications.deadlines.is_empty());
            assert_eq!(state.publications.snapshot_entries, 0);
        }
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }

    #[test]
    fn publication_identity_exhaustion_rejects_without_budget_or_payload_mutation() {
        let world = World::new(Config::default()).unwrap();
        let source = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(source).unwrap();
        world.inner.state.lock().unwrap().publications.next_id = u64::MAX;
        for _ in 0..2 {
            assert_eq!(
                world.publish(source, Payload::Pulse(1)),
                Err(Error::LimitExceeded)
            );
        }
        assert_indexes(&world);
        assert_eq!(world.snapshot().publications_retained, 0);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }
}
