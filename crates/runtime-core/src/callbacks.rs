//! Claims for native startup and periodic callbacks, sharing reserved control
//! capacity with failure notifications, independently of business mailboxes.
use super::*;

pub struct NativeCallbackLease {
    inner: Arc<Inner>,
    owner: ActorRef,
}

impl World {
    pub fn claim_native_callback(
        &self,
        owner: ActorRef,
        startup: bool,
    ) -> Result<Option<NativeCallbackLease>, Error> {
        self.claim_native_hook(
            owner,
            if startup {
                Lifecycle::Starting
            } else {
                Lifecycle::Active
            },
        )
    }

    /// Claim a quiesce/drain callback before the stop fence. Cleanup after that
    /// fence remains covered by the executor's tracked task lifetime.
    pub fn claim_native_drain_callback(
        &self,
        owner: ActorRef,
    ) -> Result<Option<NativeCallbackLease>, Error> {
        self.claim_native_hook(owner, Lifecycle::Quiescing)
    }

    fn claim_native_hook(
        &self,
        owner: ActorRef,
        expected: Lifecycle,
    ) -> Result<Option<NativeCallbackLease>, Error> {
        let _activity = self.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        let execution = self.execution_state_locked(owner, &state)?;
        let actor = self.check_ref_mut(owner, &mut state)?;
        let eligible = execution == expected
            && (expected != Lifecycle::Starting || actor.state == Lifecycle::Starting);
        if actor.kind != EndpointKind::Native
            || !eligible
            || actor.in_flight
            || actor.control_in_flight
            || actor.notification.is_some()
        {
            return Ok(None);
        }
        actor.control_in_flight = true;
        self.mark_activity_dependents_locked(owner, &state);
        self.refresh_python_ready_locked(owner, &mut state);
        Ok(Some(NativeCallbackLease {
            inner: self.inner.clone(),
            owner,
        }))
    }
}

impl Drop for NativeCallbackLease {
    fn drop(&mut self) {
        let world = World {
            inner: self.inner.clone(),
        };
        let _activity = world.activity_change();
        let mut state = self.inner.state.lock().unwrap();
        if let Ok(actor) = world.check_ref_mut(self.owner, &mut state) {
            actor.control_in_flight = false;
        }
        world.try_finalize(self.owner, &mut state);
    }
}
