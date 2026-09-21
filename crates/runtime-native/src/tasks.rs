use super::{CatchPanic, Inner};
use crate::cpu::{self, CpuOutcome, CpuReservation};
use crate::sdk_config::NativeError;
use actorplane_core::{
    FailureAction, FailureDetails, FailurePhase, Lifecycle, MessageOptions, Payload, TaskLease,
    World,
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{runtime::Handle, sync::OwnedSemaphorePermit, time};

/// The bounded native task submission service shared by runtime components.
#[derive(Clone)]
pub(crate) struct TaskService {
    world: World,
    handle: Handle,
    inner: Arc<Inner>,
}

impl TaskService {
    pub(crate) fn new(world: World, handle: Handle, inner: Arc<Inner>) -> Self {
        Self {
            world,
            handle,
            inner,
        }
    }

    pub(crate) fn world(&self) -> &World {
        &self.world
    }

    pub(crate) fn permits(&self, count: u32) -> Result<OwnedSemaphorePermit, String> {
        self.inner
            .slots
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| "native task limit reached".into())
    }

    pub(crate) fn reserve_cpu(&self) -> Result<CpuReservation, NativeError> {
        let cpu = self
            .inner
            .cpu
            .as_ref()
            .ok_or(NativeError::Application("CpuUnavailableInTestWorld"))?;
        cpu.reserve().map_err(NativeError::Limit)
    }

    pub(crate) fn spawn_cpu<C, W>(
        &self,
        global: OwnedSemaphorePermit,
        lease: actorplane_core::TaskLease,
        reservation: CpuReservation,
        cancelled: C,
        work: W,
    ) -> Result<(), NativeError>
    where
        C: Fn() -> bool + Send + Sync + 'static,
        W: FnOnce() -> CpuOutcome + Send + 'static,
    {
        let job = cpu::CpuJob::new(
            self.world.clone(),
            reservation,
            global,
            lease,
            cancelled,
            work,
        );
        let future = job.run();
        let mut tasks = self.inner.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        #[cfg(feature = "test-runtime")]
        if let Some(signal) = &self.inner.drive {
            tasks.push(self.handle.spawn(crate::testing::Tracked {
                future: Box::pin(future),
                signal: signal.clone(),
            }));
            return Ok(());
        }
        tasks.push(self.handle.spawn(future));
        Ok(())
    }

    pub(crate) fn spawn<F, Fut>(
        &self,
        permit: OwnedSemaphorePermit,
        lease: TaskLease,
        make_future: F,
    ) where
        F: FnOnce(TaskLease) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.inner.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        let world = self.world.clone();
        let future = async move {
            let _permit = permit;
            let owner = lease.owner();
            let _task_guard = lease.clone();
            let failed =
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| make_future(lease)))
                {
                    Ok(future) => CatchPanic {
                        future: Box::pin(future),
                    }
                    .await
                    .is_err(),
                    Err(_) => true,
                };
            if failed {
                let _ = world.report_failure(
                    owner,
                    None,
                    None,
                    None,
                    FailureDetails::new(FailurePhase::Handler, "native_task", "RustPanic", vec![]),
                    FailureAction::StopActor,
                );
            }
        };
        #[cfg(feature = "test-runtime")]
        if let Some(signal) = &self.inner.drive {
            tasks.push(self.handle.spawn(crate::testing::Tracked {
                future: Box::pin(future),
                signal: signal.clone(),
            }));
            return;
        }
        tasks.push(self.handle.spawn(future));
    }

    pub(crate) fn after_with(
        &self,
        owner: actorplane_core::ActorRef,
        target: actorplane_core::ActorRef,
        delay: Duration,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<(), String> {
        self.after_inner(owner, target, delay, payload, options, None)
            .map_err(|error| error.to_string())
    }

    /// Schedule a timer while retaining a caller-owned local job permit for
    /// the complete timer lifetime, including startup gating and delay.
    pub(crate) fn after_with_local(
        &self,
        owner: actorplane_core::ActorRef,
        target: actorplane_core::ActorRef,
        delay: Duration,
        payload: Payload,
        options: MessageOptions,
        local: OwnedSemaphorePermit,
    ) -> Result<(), NativeError> {
        self.after_inner(owner, target, delay, payload, options, Some(local))
    }

    fn after_inner(
        &self,
        owner: actorplane_core::ActorRef,
        target: actorplane_core::ActorRef,
        delay: Duration,
        payload: Payload,
        mut options: MessageOptions,
        local: Option<OwnedSemaphorePermit>,
    ) -> Result<(), NativeError> {
        if delay > super::MAX_PERIOD {
            return Err(NativeError::Limit("timer delay"));
        }
        if options.source.is_some_and(|source| source != owner) {
            return Err(actorplane_core::Error::InvalidMetadata.into());
        }
        options.source = Some(owner);
        options.validate()?;
        match self.world.state(target)? {
            Lifecycle::Starting | Lifecycle::Active => (),
            _ => return Err(actorplane_core::Error::ActorStopped.into()),
        }
        // Local job accounting supplements the native runtime-wide budget;
        // retaining a local permit must never allow the global limit to be
        // bypassed.
        let permit = self
            .permits(1)
            .map_err(|_| NativeError::Limit("runtime tasks"))?;
        let held = self.world.hold_with(payload, options)?;
        let deadline = held.options().deadline;
        let lease = self.world.track_task(owner)?;
        let world = self.world.clone();
        self.spawn(permit, lease, move |lease| async move {
            let _local_permit = local;
            let _lease = lease;
            loop {
                if super::is_active(&world, owner) {
                    break;
                }
                if !matches!(world.execution_state(owner), Ok(Lifecycle::Starting)) {
                    return;
                }
                if deadline.is_some_and(|at| world.now() >= at) {
                    return;
                }
                time::sleep(super::CHECK_INTERVAL).await;
            }
            let expire = time::sleep(delay);
            tokio::pin!(expire);
            let remaining = deadline.map(|at| at.saturating_duration_since(world.now()));
            let deadline_sleep = async move {
                match remaining {
                    Some(delay) => time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(deadline_sleep);
            let mut poll = super::periodic(super::CHECK_INTERVAL);
            loop {
                tokio::select! {
                    _ = &mut expire => { let _ = world.send_held_from(owner, target, held); break; }
                    _ = &mut deadline_sleep => break,
                    _ = poll.tick() => { if !super::is_active(&world, owner) { break; } }
                }
            }
        });
        Ok(())
    }
}
