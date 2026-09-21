use super::{ActorRef, Error, HeldPayload};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId {
    pub world: u64,
    pub slot: u32,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationOwner {
    pub owner: ActorRef,
    pub target: ActorRef,
}

#[derive(Clone)]
pub struct PendingOperation {
    pub owner: ActorRef,
    pub target: ActorRef,
    pub deadline: Instant,
    pub context: Option<HeldPayload>,
}

impl fmt::Debug for PendingOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingOperation")
            .field("owner", &self.owner)
            .field("target", &self.target)
            .field("deadline", &self.deadline)
            .field("context", &self.context.as_ref().map(|_| "held"))
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationFailure {
    HandlerFailed,
    ResultTooLarge,
}

#[derive(Clone)]
pub enum TerminalOutcome {
    Completed(HeldPayload),
    Failed {
        code: OperationFailure,
        message: String,
    },
    TimedOut,
    Cancelled,
    OwnerStopped,
    TargetStopped,
}
impl fmt::Debug for TerminalOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(_) => f.debug_tuple("Completed").finish(),
            Self::Failed { code, message } => f
                .debug_struct("Failed")
                .field("code", code)
                .field("message", message)
                .finish(),
            Self::TimedOut => f.write_str("TimedOut"),
            Self::Cancelled => f.write_str("Cancelled"),
            Self::OwnerStopped => f.write_str("OwnerStopped"),
            Self::TargetStopped => f.write_str("TargetStopped"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OperationStatus {
    Pending(PendingOperation),
    Terminal(TerminalOutcome),
}

enum Entry {
    Pending(PendingOperation),
    Terminal(TerminalOutcome),
}

pub struct OperationTable {
    world: u64,
    max: usize,
    slots: Vec<Option<Entry>>,
    generations: Vec<u64>,
    owners: Vec<Option<ActorRef>>,
    free: Vec<u32>,
    live: usize,
    pending: usize,
    pending_owners: HashMap<ActorRef, usize>,
    owner_index: HashMap<ActorRef, HashSet<OperationId>>,
    target_index: HashMap<ActorRef, HashSet<OperationId>>,
    deadlines: BTreeSet<(Instant, u32, u64)>,
}

impl OperationTable {
    pub fn new(world_id: u64, max_operations: usize) -> Result<Self, Error> {
        if max_operations == 0 || max_operations > u32::MAX as usize {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            world: world_id,
            max: max_operations,
            slots: Vec::new(),
            generations: Vec::new(),
            owners: Vec::new(),
            free: Vec::new(),
            live: 0,
            pending: 0,
            pending_owners: HashMap::new(),
            owner_index: HashMap::new(),
            target_index: HashMap::new(),
            deadlines: BTreeSet::new(),
        })
    }
    fn id(&self, slot: u32) -> OperationId {
        OperationId {
            world: self.world,
            slot,
            generation: self.generations[slot as usize],
        }
    }
    fn locate(&self, id: OperationId) -> Result<usize, Error> {
        if id.world != self.world {
            return Err(Error::CrossWorld);
        }
        let i = id.slot as usize;
        if i >= self.slots.len() || self.generations[i] != id.generation || self.slots[i].is_none()
        {
            return Err(Error::StaleReference);
        }
        Ok(i)
    }
    fn index_pending(&mut self, id: OperationId, p: &PendingOperation) {
        self.owners[id.slot as usize] = Some(p.owner);
        *self.pending_owners.entry(p.owner).or_default() += 1;
        self.owner_index.entry(p.owner).or_default().insert(id);
        self.target_index.entry(p.target).or_default().insert(id);
        self.deadlines.insert((p.deadline, id.slot, id.generation));
    }
    fn remove_index(&mut self, id: OperationId, p: &PendingOperation) {
        self.owners[id.slot as usize] = None;
        self.remove_pending_owner(p.owner);
        if let Some(set) = self.owner_index.get_mut(&p.owner) {
            set.remove(&id);
            if set.is_empty() {
                self.owner_index.remove(&p.owner);
            }
        }
        if let Some(set) = self.target_index.get_mut(&p.target) {
            set.remove(&id);
            if set.is_empty() {
                self.target_index.remove(&p.target);
            }
        }
        self.deadlines.remove(&(p.deadline, id.slot, id.generation));
    }
    fn normalize_message(message: String) -> String {
        // Copy even a short message: its original allocation may be arbitrarily
        // larger than its length, and must not escape the retained-record bound.
        let mut end = message.len().min(1024);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message[..end].to_owned()
    }
    fn retire_generation(&mut self, index: usize) {
        if let Some(next) = self.generations[index].checked_add(1) {
            self.generations[index] = next;
            self.free.push(index as u32);
        }
    }
    pub fn pending_count(&self) -> usize {
        self.pending
    }
    pub fn len(&self) -> usize {
        self.live_count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn retained_terminal_count(&self) -> usize {
        self.live - self.pending
    }
    pub fn reserve(
        &mut self,
        owner: ActorRef,
        target: ActorRef,
        deadline: Instant,
    ) -> Result<OperationId, Error> {
        if owner.world != self.world || target.world != self.world {
            return Err(Error::CrossWorld);
        }
        if self.live_count() >= self.max {
            return Err(Error::LimitExceeded);
        }
        if self.free.is_empty() && self.slots.len() >= self.max {
            return Err(Error::LimitExceeded);
        }
        let slot = self.free.pop().unwrap_or_else(|| {
            let n = self.slots.len() as u32;
            self.slots.push(None);
            self.generations.push(1);
            self.owners.push(None);
            n
        });
        let id = self.id(slot);
        let p = PendingOperation {
            owner,
            target,
            deadline,
            context: None,
        };
        self.slots[slot as usize] = Some(Entry::Pending(p.clone()));
        self.index_pending(id, &p);
        self.live += 1;
        self.pending += 1;
        Ok(id)
    }
    pub fn set_context(&mut self, id: OperationId, context: HeldPayload) -> Result<(), Error> {
        if context.world() != self.world {
            return Err(Error::CrossWorld);
        }
        let index = self.locate(id)?;
        match self.slots[index].as_mut() {
            Some(Entry::Pending(operation)) => {
                if operation.context.is_some() {
                    return Err(Error::InvalidConfig);
                }
                operation.context = Some(context);
                Ok(())
            }
            Some(Entry::Terminal(_)) => Err(Error::InvalidConfig),
            None => Err(Error::StaleReference),
        }
    }
    fn live_count(&self) -> usize {
        self.live
    }
    pub fn complete(
        &mut self,
        id: OperationId,
        mut outcome: TerminalOutcome,
    ) -> Result<bool, Error> {
        if let TerminalOutcome::Completed(payload) = &outcome
            && payload.world() != self.world
        {
            return Err(Error::CrossWorld);
        }
        let i = self.locate(id)?;
        let old = std::mem::replace(
            self.slots[i].as_mut().unwrap(),
            Entry::Terminal(TerminalOutcome::Cancelled),
        );
        match old {
            Entry::Pending(p) => {
                self.remove_target_index(id, &p);
                self.remove_pending_owner(p.owner);
                self.deadlines.remove(&(p.deadline, id.slot, id.generation));
                self.pending -= 1;
                if let TerminalOutcome::Failed {
                    ref mut message, ..
                } = outcome
                {
                    *message = Self::normalize_message(std::mem::take(message));
                }
                self.slots[i] = Some(Entry::Terminal(outcome));
                Ok(true)
            }
            Entry::Terminal(existing) => {
                self.slots[i] = Some(Entry::Terminal(existing));
                Ok(false)
            }
        }
    }
    pub fn status(&self, id: OperationId) -> Result<OperationStatus, Error> {
        let i = self.locate(id)?;
        Ok(match self.slots[i].as_ref().unwrap() {
            Entry::Pending(p) => OperationStatus::Pending(p.clone()),
            Entry::Terminal(o) => OperationStatus::Terminal(o.clone()),
        })
    }
    pub fn take(&mut self, id: OperationId) -> Result<Option<TerminalOutcome>, Error> {
        let i = self.locate(id)?;
        let old = self.slots[i].take().unwrap();
        match old {
            Entry::Pending(p) => {
                self.slots[i] = Some(Entry::Pending(p));
                Ok(None)
            }
            Entry::Terminal(o) => {
                self.remove_owner_index(id);
                self.live -= 1;
                self.retire_generation(i);
                Ok(Some(o))
            }
        }
    }
    pub fn release_unsubmitted(&mut self, id: OperationId) -> Result<bool, Error> {
        let i = self.locate(id)?;
        let old = self.slots[i].take().unwrap();
        match old {
            Entry::Pending(p) => {
                self.remove_index(id, &p);
                self.pending -= 1;
                self.live -= 1;
                self.retire_generation(i);
                Ok(true)
            }
            Entry::Terminal(o) => {
                self.slots[i] = Some(Entry::Terminal(o));
                Ok(false)
            }
        }
    }
    fn transition_ids(&mut self, ids: Vec<OperationId>, outcome: TerminalOutcome) -> usize {
        let mut n = 0;
        for id in ids {
            if self.complete(id, outcome.clone()).unwrap_or(false) {
                n += 1;
            }
        }
        n
    }
    fn remove_owner_index(&mut self, id: OperationId) {
        if let Some(owner) = self.owners[id.slot as usize].take()
            && let Some(ids) = self.owner_index.get_mut(&owner)
        {
            ids.remove(&id);
            if ids.is_empty() {
                self.owner_index.remove(&owner);
            }
        }
    }
    fn remove_pending_owner(&mut self, owner: ActorRef) {
        if let Some(count) = self.pending_owners.get_mut(&owner) {
            *count -= 1;
            if *count == 0 {
                self.pending_owners.remove(&owner);
            }
        }
    }
    fn remove_target_index(&mut self, id: OperationId, p: &PendingOperation) {
        if let Some(ids) = self.target_index.get_mut(&p.target) {
            ids.remove(&id);
            if ids.is_empty() {
                self.target_index.remove(&p.target);
            }
        }
    }
    pub fn pending_for(&self, owner: ActorRef) -> usize {
        self.pending_owners.get(&owner).copied().unwrap_or(0)
    }
    pub fn pending_for_target(&self, target: ActorRef) -> usize {
        self.target_index.get(&target).map_or(0, HashSet::len)
    }

    /// Return the owner and target metadata for a live operation without
    /// cloning a retained result payload. Terminal records intentionally keep
    /// only their owner metadata until `take` retires them.
    pub(crate) fn participants(
        &self,
        id: OperationId,
    ) -> Result<(ActorRef, Option<ActorRef>), Error> {
        let index = self.locate(id)?;
        let owner = self.owners[index].ok_or(Error::StaleReference)?;
        let target = match self.slots[index].as_ref() {
            Some(Entry::Pending(operation)) => Some(operation.target),
            Some(Entry::Terminal(_)) => None,
            None => return Err(Error::StaleReference),
        };
        Ok((owner, target))
    }

    /// Return each pending operation touching `actor` once, in stable slot /
    /// generation order. This is an index-only query; it never scans slots.
    pub(crate) fn pending_participants_for(&self, actor: ActorRef) -> Vec<OperationOwner> {
        let mut ids = HashSet::new();
        if let Some(values) = self.owner_index.get(&actor) {
            ids.extend(values.iter().copied());
        }
        if let Some(values) = self.target_index.get(&actor) {
            ids.extend(values.iter().copied());
        }
        let mut ids: Vec<_> = ids.into_iter().collect();
        ids.sort_by_key(|id| (id.slot, id.generation));
        ids.into_iter()
            .filter_map(|id| {
                let index = self.locate(id).ok()?;
                match self.slots[index].as_ref()? {
                    Entry::Pending(operation) => Some(OperationOwner {
                        owner: operation.owner,
                        target: operation.target,
                    }),
                    Entry::Terminal(_) => None,
                }
            })
            .collect()
    }

    /// Return due pending operation endpoints using the deadline index. The
    /// ordering matches `expire_ids`: due entries are reduced to operation
    /// identities and sorted by slot/generation, without scanning unrelated
    /// live operations.
    pub(crate) fn due_participants(&self, now: Instant) -> Vec<OperationOwner> {
        let mut ids: Vec<_> = self
            .deadlines
            .iter()
            .take_while(|(deadline, _, _)| *deadline <= now)
            .filter_map(|(_, slot, generation)| {
                let id = OperationId {
                    world: self.world,
                    slot: *slot,
                    generation: *generation,
                };
                self.locate(id).ok().map(|_| id)
            })
            .collect();
        ids.sort_by_key(|id| (id.slot, id.generation));
        ids.into_iter()
            .filter_map(|id| {
                let index = self.locate(id).ok()?;
                match self.slots[index].as_ref()? {
                    Entry::Pending(operation) => Some(OperationOwner {
                        owner: operation.owner,
                        target: operation.target,
                    }),
                    Entry::Terminal(_) => None,
                }
            })
            .collect()
    }
    /// Count each pending operation once even if both ends belong to the scope.
    pub(crate) fn counts_for_scope(&self, scope: &HashSet<ActorRef>) -> (usize, usize) {
        let mut ids = HashSet::new();
        for actor in scope {
            if let Some(values) = self.owner_index.get(actor) {
                ids.extend(values);
            }
            if let Some(values) = self.target_index.get(actor) {
                ids.extend(values);
            }
        }
        let mut pending = 0;
        let mut retained = 0;
        for id in ids {
            if let Ok(index) = self.locate(id) {
                match self.slots[index].as_ref() {
                    Some(Entry::Pending(_)) => pending += 1,
                    Some(Entry::Terminal(_))
                        if self.owners[index].is_some_and(|owner| scope.contains(&owner)) =>
                    {
                        retained += 1
                    }
                    _ => (),
                }
            }
        }
        (pending, retained)
    }
    pub fn retire_owner(&mut self, owner: ActorRef) -> usize {
        let ids: Vec<_> = self
            .owner_index
            .get(&owner)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let mut n = 0;
        let mut ids = ids;
        ids.sort_by_key(|id| (id.slot, id.generation));
        for id in ids {
            let _ = self.complete(id, TerminalOutcome::OwnerStopped);
            if self.take(id).ok().flatten().is_some() {
                n += 1;
            }
        }
        n
    }
    pub fn cancel_owner(&mut self, owner: ActorRef) -> usize {
        let ids: Vec<_> = self
            .owner_index
            .get(&owner)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let mut ids = ids;
        ids.sort_by_key(|id| (id.slot, id.generation));
        self.transition_ids(ids, TerminalOutcome::OwnerStopped)
    }
    pub fn stop_target(&mut self, target: ActorRef) -> usize {
        let ids: Vec<_> = self
            .target_index
            .get(&target)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        let mut ids = ids;
        ids.sort_by_key(|id| (id.slot, id.generation));
        self.transition_ids(ids, TerminalOutcome::TargetStopped)
    }
    pub fn expire(&mut self, now: Instant) -> usize {
        self.expire_ids(now).len()
    }
    pub fn expire_ids(&mut self, now: Instant) -> Vec<OperationId> {
        let mut ids = Vec::new();
        while let Some(&(deadline, slot, generation)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            self.deadlines.pop_first();
            let id = OperationId {
                world: self.world,
                slot,
                generation,
            };
            if self.locate(id).is_ok() {
                ids.push(id);
            }
        }
        for id in &ids {
            self.complete(*id, TerminalOutcome::TimedOut)
                .expect("live pending operation");
        }
        ids.sort_by_key(|id| (id.slot, id.generation));
        ids
    }
    /// Enforce the deadline at an individual operation's claim boundary without
    /// scanning the World table. Returns whether this call established timeout.
    pub fn expire_one(&mut self, id: OperationId, now: Instant) -> Result<bool, Error> {
        let index = self.locate(id)?;
        if matches!(&self.slots[index], Some(Entry::Pending(p)) if p.deadline <= now) {
            self.complete(id, TerminalOutcome::TimedOut)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    fn refs() -> (ActorRef, ActorRef) {
        (
            ActorRef {
                world: 7,
                slot: 1,
                generation: 1,
            },
            ActorRef {
                world: 7,
                slot: 2,
                generation: 1,
            },
        )
    }
    #[test]
    fn retained_failure_does_not_keep_oversized_string_capacity() {
        let (owner, target) = refs();
        let mut table = OperationTable::new(7, 1).unwrap();
        let id = table.reserve(owner, target, Instant::now()).unwrap();
        let mut message = String::with_capacity(1_000_000);
        message.push_str("failure");
        table
            .complete(
                id,
                TerminalOutcome::Failed {
                    code: OperationFailure::HandlerFailed,
                    message,
                },
            )
            .unwrap();
        let Entry::Terminal(TerminalOutcome::Failed { message, .. }) =
            table.slots[id.slot as usize].as_ref().unwrap()
        else {
            panic!("missing failure")
        };
        assert!(message.capacity() <= 1024);
    }

    #[test]
    fn participant_metadata_tracks_pending_terminal_and_retired_states() {
        let (owner, target) = refs();
        let mut table = OperationTable::new(7, 2).unwrap();
        let id = table
            .reserve(owner, target, Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(table.participants(id).unwrap(), (owner, Some(target)));
        assert_eq!(
            table.pending_participants_for(owner),
            vec![OperationOwner { owner, target }]
        );
        assert!(table.complete(id, TerminalOutcome::TimedOut).unwrap());
        assert_eq!(table.participants(id).unwrap(), (owner, None));
        assert!(table.pending_participants_for(owner).is_empty());
        assert!(table.take(id).unwrap().is_some());
        assert_eq!(table.participants(id), Err(Error::StaleReference));
    }

    #[test]
    fn participant_union_deduplicates_operation_touching_both_ends() {
        let (owner, target) = refs();
        let mut table = OperationTable::new(7, 3).unwrap();
        let both = table
            .reserve(owner, owner, Instant::now() + Duration::from_secs(1))
            .unwrap();
        let other = table
            .reserve(owner, target, Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            table.pending_participants_for(owner),
            vec![
                OperationOwner {
                    owner,
                    target: owner,
                },
                OperationOwner { owner, target },
            ]
        );
        assert_eq!(
            table.pending_participants_for(target),
            vec![OperationOwner { owner, target }]
        );
        table.release_unsubmitted(both).unwrap();
        table.release_unsubmitted(other).unwrap();
    }

    #[test]
    fn due_participants_use_deadline_index_and_exclude_future_entries() {
        let (owner, target) = refs();
        let now = Instant::now();
        let mut table = OperationTable::new(7, 3).unwrap();
        let future = table
            .reserve(owner, target, now + Duration::from_secs(10))
            .unwrap();
        let due_late_slot = table
            .reserve(target, owner, now + Duration::from_secs(1))
            .unwrap();
        let due_early_slot = table.reserve(owner, owner, now).unwrap();
        assert_eq!(
            table.due_participants(now + Duration::from_secs(2)),
            vec![
                OperationOwner {
                    owner: target,
                    target: owner,
                },
                OperationOwner {
                    owner,
                    target: owner,
                },
            ]
        );
        assert_eq!(table.participants(future).unwrap(), (owner, Some(target)));
        table.release_unsubmitted(future).unwrap();
        table.release_unsubmitted(due_late_slot).unwrap();
        table.release_unsubmitted(due_early_slot).unwrap();
    }
    #[test]
    fn indexed_counts_track_reuse_rollback_and_retirement() {
        let (owner, target) = refs();
        let mut table = OperationTable::new(7, 3).unwrap();
        for _ in 0..100 {
            let a = table.reserve(owner, target, Instant::now()).unwrap();
            let b = table.reserve(owner, target, Instant::now()).unwrap();
            let c = table.reserve(target, owner, Instant::now()).unwrap();
            assert_eq!(
                (table.len(), table.pending_count(), table.pending_for(owner)),
                (3, 3, 2)
            );
            assert_eq!(table.deadlines.len(), table.pending_count());
            table.release_unsubmitted(b).unwrap();
            assert_eq!(table.deadlines.len(), table.pending_count());
            assert!(table.expire_one(a, Instant::now()).unwrap());
            assert_eq!(table.deadlines.len(), table.pending_count());
            assert!(!table.expire_one(a, Instant::now()).unwrap());
            assert_eq!(
                (table.pending_for(owner), table.pending_for_target(target)),
                (0, 0)
            );
            assert_eq!(
                (table.pending_count(), table.retained_terminal_count()),
                (1, 1)
            );
            table.take(a).unwrap().unwrap();
            assert_eq!(table.deadlines.len(), table.pending_count());
            assert_eq!(table.retire_owner(target), 1);
            assert_eq!(table.deadlines.len(), table.pending_count());
            assert!(table.status(c).is_err());
            assert!(table.is_empty());
            assert!(table.owner_index.is_empty());
            assert!(table.target_index.is_empty());
            assert!(table.pending_owners.is_empty());
            assert!(table.deadlines.is_empty());
            assert!(table.owners.iter().all(Option::is_none));
            assert_eq!(table.slots.len(), 3);
        }
    }
    #[test]
    fn one_winner_and_terminal_retention() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 1).unwrap();
        let id = t.reserve(a, b, Instant::now()).unwrap();
        assert!(t.complete(id, TerminalOutcome::TimedOut).unwrap());
        assert!(!t.complete(id, TerminalOutcome::Cancelled).unwrap());
        assert!(matches!(
            t.status(id).unwrap(),
            OperationStatus::Terminal(TerminalOutcome::TimedOut)
        ));
        assert!(matches!(
            t.take(id).unwrap(),
            Some(TerminalOutcome::TimedOut)
        ));
    }
    #[test]
    fn pending_limit_and_reuse_stale() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 1).unwrap();
        let id = t.reserve(a, b, Instant::now()).unwrap();
        assert_eq!(t.reserve(a, b, Instant::now()), Err(Error::LimitExceeded));
        t.complete(id, TerminalOutcome::Cancelled).unwrap();
        let old = t.take(id).unwrap();
        assert!(old.is_some());
        let n = t.reserve(a, b, Instant::now()).unwrap();
        assert_ne!(id.generation, n.generation);
        assert!(matches!(t.status(id), Err(Error::StaleReference)));
    }
    #[test]
    fn indexes_cancel_and_expire() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 3).unwrap();
        let x = t
            .reserve(a, b, Instant::now() - Duration::from_secs(1))
            .unwrap();
        let y = t
            .reserve(a, b, Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(t.expire(Instant::now()), 1);
        assert_eq!(t.cancel_owner(a), 1);
        assert!(matches!(
            t.status(x).unwrap(),
            OperationStatus::Terminal(TerminalOutcome::TimedOut)
        ));
        assert!(matches!(
            t.status(y).unwrap(),
            OperationStatus::Terminal(TerminalOutcome::OwnerStopped)
        ));
    }

    #[test]
    fn deadline_index_orders_equal_deadlines_and_removes_rollbacks() {
        let (a, b) = refs();
        let mut table = OperationTable::new(7, 4).unwrap();
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        let first = table.reserve(a, b, later).unwrap();
        let second = table.reserve(b, a, later).unwrap();
        let third = table.reserve(a, b, now).unwrap();
        assert_eq!(table.deadlines.len(), 3);
        assert_eq!(table.expire_ids(now), vec![third]);
        table.release_unsubmitted(first).unwrap();
        assert_eq!(table.deadlines.len(), 1);
        assert_eq!(table.expire_ids(later), vec![second]);
        assert!(table.deadlines.is_empty());
    }

    #[test]
    fn old_generation_deadline_cannot_expire_reused_slot() {
        let (a, b) = refs();
        let mut table = OperationTable::new(7, 2).unwrap();
        let old = table
            .reserve(a, b, Instant::now() - Duration::from_secs(1))
            .unwrap();
        table.release_unsubmitted(old).unwrap();
        let replacement = table
            .reserve(a, b, Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_ne!(old.generation, replacement.generation);
        assert!(table.expire_ids(Instant::now()).is_empty());
        assert!(matches!(
            table.status(replacement),
            Ok(OperationStatus::Pending(_))
        ));
        assert_eq!(table.deadlines.len(), table.pending_count());
    }

    #[test]
    fn completed_future_operations_do_not_accumulate_deadline_entries() {
        let (a, b) = refs();
        let mut table = OperationTable::new(7, 3).unwrap();
        let future = Instant::now() + Duration::from_secs(60);
        let survivor = table.reserve(a, b, future).unwrap();
        for _ in 0..100 {
            let id = table.reserve(a, b, future).unwrap();
            assert_eq!(table.deadlines.len(), table.pending_count());
            table.complete(id, TerminalOutcome::Cancelled).unwrap();
            table.take(id).unwrap();
            assert_eq!(table.deadlines.len(), table.pending_count());
        }
        assert!(matches!(
            table.status(survivor),
            Ok(OperationStatus::Pending(_))
        ));
        assert_eq!(table.deadlines.len(), 1);
    }

    #[test]
    fn scope_counts_union_owner_and_target_once() {
        let (owner, target) = refs();
        let mut table = OperationTable::new(7, 4).unwrap();
        let both = table.reserve(owner, target, Instant::now()).unwrap();
        let reverse = table.reserve(target, owner, Instant::now()).unwrap();
        let mut scope = HashSet::new();
        scope.insert(owner);
        scope.insert(target);
        assert_eq!(table.counts_for_scope(&scope), (2, 0));
        table.complete(both, TerminalOutcome::Cancelled).unwrap();
        table.take(both).unwrap();
        assert_eq!(table.counts_for_scope(&scope), (1, 0));
        table.complete(reverse, TerminalOutcome::Cancelled).unwrap();
        assert_eq!(table.counts_for_scope(&scope), (0, 1));
    }
    #[test]
    fn diagnostic_is_valid_utf8_and_bounded() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 1).unwrap();
        let id = t.reserve(a, b, Instant::now()).unwrap();
        let msg = "é".repeat(700);
        t.complete(
            id,
            TerminalOutcome::Failed {
                code: OperationFailure::HandlerFailed,
                message: msg,
            },
        )
        .unwrap();
        if let OperationStatus::Terminal(TerminalOutcome::Failed { message, .. }) =
            t.status(id).unwrap()
        {
            assert!(message.len() <= 1024);
            assert!(message.is_char_boundary(message.len()));
        }
    }
    #[test]
    fn world_boundary() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 1).unwrap();
        let foreign = ActorRef { world: 8, ..a };
        assert_eq!(
            t.reserve(foreign, b, Instant::now()),
            Err(Error::CrossWorld)
        );
        let id = t.reserve(a, b, Instant::now()).unwrap();
        assert!(matches!(
            t.status(OperationId { world: 8, ..id }),
            Err(Error::CrossWorld)
        ));
    }

    #[test]
    fn failed_loser_preserves_held_success_and_releases_on_take() {
        let (a, b) = refs();
        let world = super::super::World::new(super::super::Config::default()).unwrap();
        let held = world.hold(super::super::Payload::Pulse(1)).unwrap();
        let mut t = OperationTable::new(world.id(), 1).unwrap();
        let id = t
            .reserve(
                ActorRef {
                    world: world.id(),
                    ..a
                },
                ActorRef {
                    world: world.id(),
                    ..b
                },
                Instant::now(),
            )
            .unwrap();
        assert!(t.complete(id, TerminalOutcome::Completed(held)).unwrap());
        assert!(
            !t.complete(
                id,
                TerminalOutcome::Failed {
                    code: OperationFailure::HandlerFailed,
                    message: "late".into()
                }
            )
            .unwrap()
        );
        assert_eq!(world.snapshot().retained_payload_bytes, 8);
        drop(t.take(id).unwrap());
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }

    #[test]
    fn generation_overflow_retires_slot_without_growth() {
        let (a, b) = refs();
        let mut t = OperationTable::new(7, 1).unwrap();
        let id = t.reserve(a, b, Instant::now()).unwrap();
        t.complete(id, TerminalOutcome::Cancelled).unwrap();
        t.generations[id.slot as usize] = u64::MAX;
        t.take(OperationId {
            generation: u64::MAX,
            ..id
        })
        .unwrap();
        assert_eq!(t.len(), 0);
        assert_eq!(t.reserve(a, b, Instant::now()), Err(Error::LimitExceeded));
    }

    #[test]
    fn pending_context_is_attached_once_and_debug_is_payload_free() {
        let world = super::super::World::new(super::super::Config::default()).unwrap();
        let owner = world
            .allocate(super::super::EndpointKind::Native, None)
            .unwrap();
        let target = world
            .allocate(super::super::EndpointKind::Native, None)
            .unwrap();
        let mut table = OperationTable::new(world.id(), 1).unwrap();
        let id = table.reserve(owner, target, Instant::now()).unwrap();
        let context = world.hold(super::super::Payload::Pulse(7)).unwrap();
        table.set_context(id, context).unwrap();
        assert_eq!(
            table.set_context(id, world.hold(super::super::Payload::Pulse(8)).unwrap()),
            Err(Error::InvalidConfig)
        );
        let debug = format!("{:?}", table.status(id).unwrap());
        assert!(debug.contains("context: Some(\"held\")"));
        assert!(!debug.contains("Pulse"));
        assert_eq!(
            table.set_context(id, world.hold(super::super::Payload::Pulse(9)).unwrap()),
            Err(Error::InvalidConfig)
        );
    }
}
