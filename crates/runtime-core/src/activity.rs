use crate::{ActorRef, Error, Lifecycle};
use std::{
    collections::{BTreeMap, BTreeSet},
    task::{Context, Poll, Waker},
};

struct Slot {
    actor: ActorRef,
    version: u64,
    overflow: bool,
    waker: Option<Waker>,
}

pub(crate) struct Retired {
    waker: Waker,
    wake: bool,
}

pub(crate) struct ActivityRegistry {
    slots: BTreeMap<u32, Slot>,
    dirty: BTreeSet<u32>,
}

impl ActivityRegistry {
    pub(crate) fn new() -> Self {
        Self {
            slots: BTreeMap::new(),
            dirty: BTreeSet::new(),
        }
    }

    pub(crate) fn version(&mut self, actor: ActorRef) -> (Result<u64, Error>, Option<Retired>) {
        let slot = self.slots.entry(actor.slot).or_insert_with(|| Slot {
            actor,
            version: 1,
            overflow: false,
            waker: None,
        });
        if slot.actor != actor {
            let retired = slot.waker.take().map(|waker| Retired { waker, wake: true });
            *slot = Slot {
                actor,
                version: 1,
                overflow: false,
                waker: None,
            };
            self.dirty.remove(&actor.slot);
            return (Ok(1), retired);
        }
        if slot.overflow {
            (Err(Error::LimitExceeded), None)
        } else {
            (Ok(slot.version), None)
        }
    }

    pub(crate) fn poll(
        &mut self,
        actor: ActorRef,
        observed: u64,
        incoming: &mut Option<Waker>,
    ) -> (Poll<Result<u64, Error>>, Option<Retired>) {
        let slot = self.slots.entry(actor.slot).or_insert_with(|| Slot {
            actor,
            version: 1,
            overflow: false,
            waker: None,
        });
        let replaced_generation = slot.actor != actor;
        let mut retired = if replaced_generation {
            let retired = slot.waker.take().map(|waker| Retired { waker, wake: true });
            *slot = Slot {
                actor,
                version: 1,
                overflow: false,
                waker: None,
            };
            self.dirty.remove(&actor.slot);
            retired
        } else {
            None
        };
        if replaced_generation {
            return (Poll::Ready(Ok(slot.version)), retired);
        }
        if slot.overflow {
            return (Poll::Ready(Err(Error::LimitExceeded)), retired);
        }
        if slot.version != observed {
            return (Poll::Ready(Ok(slot.version)), retired);
        }
        if slot
            .waker
            .as_ref()
            .is_none_or(|old| !old.will_wake(incoming.as_ref().unwrap()))
        {
            let replaced = slot.waker.replace(incoming.take().unwrap());
            if retired.is_none() {
                retired = replaced.map(|waker| Retired { waker, wake: false });
            }
        }
        (Poll::Pending, retired)
    }

    /// Mark an already-registered actor while the World state lock is held.
    pub(crate) fn mark(&mut self, actor: ActorRef) {
        let Some(slot) = self.slots.get_mut(&actor.slot) else {
            return;
        };
        if slot.actor != actor {
            // Leave the old generation in place. A subsequent version/poll
            // call retires its observer and wakes it outside both locks.
            self.dirty.insert(actor.slot);
            return;
        }
        if slot.version == u64::MAX {
            slot.overflow = true;
        } else {
            slot.version += 1;
        }
        self.dirty.insert(actor.slot);
    }

    pub(crate) fn notify_marked(&mut self) -> Vec<Waker> {
        let dirty = std::mem::take(&mut self.dirty);
        dirty
            .into_iter()
            .filter_map(|slot| {
                self.slots
                    .get_mut(&slot)
                    .and_then(|entry| entry.waker.take())
            })
            .collect()
    }
}

impl super::World {
    pub(crate) fn activity_change(&self) -> ActivityChange<'_> {
        ActivityChange(self)
    }

    /// Snapshot an opaque activity version before inspecting this actor's work.
    /// Changes to its own work, owned descendants, effective lifecycle, routes,
    /// operations, or draining upstream producers can advance the version.
    /// Unrelated actors do not advance it; this is not a World snapshot version.
    pub fn activity(&self, actor: ActorRef) -> Result<u64, Error> {
        let (result, retired) = {
            let state = self.inner.state.lock().unwrap();
            self.check_ref(actor, &state)?;
            self.inner.activity.lock().unwrap().version(actor)
        };
        wake_retired(retired);
        result
    }

    /// Wait until activity differs from `observed`, or the actor is stopping.
    /// There is one observer per actor: a pending poll replaces its Waker.
    /// Snapshot activity before checking work, then poll using that version;
    /// always recheck authoritative state after a wake. Wakes may coalesce.
    /// Stale/cross-World handles and version exhaustion return an error.
    pub fn poll_activity(
        &self,
        actor: ActorRef,
        observed: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<u64, Error>> {
        let mut incoming = Some(cx.waker().clone());
        let (result, retired) = {
            let state = self.inner.state.lock().unwrap();
            match self.check_ref(actor, &state) {
                Err(error) => (Poll::Ready(Err(error)), None),
                Ok(reference)
                    if matches!(reference.state, Lifecycle::Stopping | Lifecycle::Stopped) =>
                {
                    let (result, retired) = self.inner.activity.lock().unwrap().version(actor);
                    (Poll::Ready(result), retired)
                }
                Ok(_) => self
                    .inner
                    .activity
                    .lock()
                    .unwrap()
                    .poll(actor, observed, &mut incoming),
            }
        };
        wake_retired(retired);
        result
    }

    pub(crate) fn touch_activity(&self) {
        self.wake_routing();
        let wakers = self.inner.activity.lock().unwrap().notify_marked();
        for waker in wakers {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake()));
        }
    }
}

fn wake_retired(retired: Option<Retired>) {
    if let Some(retired) = retired
        && retired.wake
    {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| retired.waker.wake()));
    }
}

pub(crate) struct ActivityChange<'a>(&'a super::World);
impl Drop for ActivityChange<'_> {
    fn drop(&mut self) {
        self.0.touch_activity();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn actor(slot: u32, generation: u64) -> ActorRef {
        ActorRef {
            world: 1,
            slot,
            generation,
        }
    }

    #[test]
    fn dirty_marks_are_coalesced_and_isolated() {
        let mut registry = ActivityRegistry::new();
        let first = actor(1, 1);
        let second = actor(2, 1);
        assert_eq!(registry.version(first).0, Ok(1));
        assert_eq!(registry.version(second).0, Ok(1));
        registry.mark(first);
        registry.mark(first);
        assert!(registry.notify_marked().is_empty());
        assert_eq!(registry.version(first).0, Ok(3));
        assert_eq!(registry.version(second).0, Ok(1));
    }

    #[test]
    fn overflow_is_scoped_to_one_actor() {
        let mut registry = ActivityRegistry::new();
        let first = actor(1, 1);
        let second = actor(2, 1);
        let _ = registry.version(first);
        let _ = registry.version(second);
        registry.slots.get_mut(&1).unwrap().version = u64::MAX;
        registry.mark(first);
        assert_eq!(registry.version(first).0, Err(Error::LimitExceeded));
        assert_eq!(registry.version(second).0, Ok(1));
        registry.mark(second);
        assert_eq!(registry.version(second).0, Ok(2));
    }

    #[test]
    fn generation_replacement_clears_dirty_slot() {
        let mut registry = ActivityRegistry::new();
        let old = actor(1, 1);
        let replacement = actor(1, 2);
        let _ = registry.version(old);
        registry.mark(old);
        assert_eq!(registry.version(replacement).0, Ok(1));
        assert!(registry.notify_marked().is_empty());
    }

    #[test]
    fn replacement_poll_retires_old_pending_observer_before_flush() {
        use std::{
            sync::{Arc, mpsc},
            task::{Poll, Wake, Waker},
        };

        struct Signal(mpsc::Sender<()>);
        impl Wake for Signal {
            fn wake(self: Arc<Self>) {
                let _ = self.0.send(());
            }
            fn wake_by_ref(self: &Arc<Self>) {
                let _ = self.0.send(());
            }
        }

        let mut registry = ActivityRegistry::new();
        let old = actor(1, 1);
        let replacement = actor(1, 2);
        assert_eq!(registry.version(old).0, Ok(1));
        let (tx, rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(Signal(tx)));
        let mut incoming = Some(waker.clone());
        assert!(matches!(
            registry.poll(old, 1, &mut incoming).0,
            Poll::Pending
        ));
        registry.mark(replacement);

        let mut replacement_waker = Some(Waker::noop().clone());
        let (result, retired) = registry.poll(replacement, 1, &mut replacement_waker);
        assert_eq!(result, Poll::Ready(Ok(1)));
        let retired = retired.expect("old observer must be retired");
        assert!(retired.wake);
        retired.waker.wake();
        assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok());
    }
}
