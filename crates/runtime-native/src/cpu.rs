//! Admission and lifetime accounting for native CPU/blocking operations.
use actorplane_core::{FailureAction, FailureDetails, FailurePhase, TaskLease, World};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug)]
pub struct CpuConfig {
    pub workers: usize,
    pub max_jobs: usize,
}
impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            workers: 2,
            max_jobs: 32,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CpuOutcome {
    Completed,
    Cancelled,
    Panicked,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct CpuStats {
    pub queued: u64,
    pub running: u64,
    pub submitted: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub panicked: u64,
    pub rejected: u64,
    pub workers: usize,
    pub max_jobs: usize,
}

/// Read-only counters that stay live after scheduler shutdown. This owns no
/// World, task, payload, or worker thread.
#[derive(Clone)]
pub struct CpuMonitor {
    pub(crate) executor: Option<Arc<CpuExecutor>>,
}
impl CpuMonitor {
    pub fn stats(&self) -> CpuStats {
        self.executor
            .as_ref()
            .map_or(CpuStats::default(), |executor| executor.stats())
    }
}
#[derive(Default)]
struct Counters {
    queued: AtomicU64,
    running: AtomicU64,
    submitted: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
    panicked: AtomicU64,
    rejected: AtomicU64,
}
pub(crate) struct CpuExecutor {
    workers: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    counters: Counters,
    workers_limit: usize,
    jobs_limit: usize,
}
impl CpuExecutor {
    pub(crate) fn new(config: CpuConfig) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(Semaphore::new(config.workers)),
            jobs: Arc::new(Semaphore::new(config.max_jobs)),
            counters: Counters::default(),
            workers_limit: config.workers,
            jobs_limit: config.max_jobs,
        })
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<CpuReservation, &'static str> {
        let Ok(permit) = self.jobs.clone().try_acquire_owned() else {
            self.counters.rejected.fetch_add(1, Ordering::AcqRel);
            return Err("CPU job limit reached");
        };
        self.counters.queued.fetch_add(1, Ordering::AcqRel);
        Ok(CpuReservation {
            executor: self.clone(),
            queued: true,
            _permit: permit,
        })
    }
    pub(crate) fn stats(&self) -> CpuStats {
        CpuStats {
            queued: self.counters.queued.load(Ordering::Acquire),
            running: self.counters.running.load(Ordering::Acquire),
            submitted: self.counters.submitted.load(Ordering::Acquire),
            completed: self.counters.completed.load(Ordering::Acquire),
            cancelled: self.counters.cancelled.load(Ordering::Acquire),
            panicked: self.counters.panicked.load(Ordering::Acquire),
            rejected: self.counters.rejected.load(Ordering::Acquire),
            workers: self.workers_limit,
            max_jobs: self.jobs_limit,
        }
    }
}
pub(crate) struct CpuReservation {
    executor: Arc<CpuExecutor>,
    queued: bool,
    _permit: OwnedSemaphorePermit,
}
impl Drop for CpuReservation {
    fn drop(&mut self) {
        if self.queued {
            self.executor.counters.queued.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Constructed before spawning, so even an unpolled/aborted future has guarded
/// capture destruction. No runtime lock is held while user captures are dropped.
pub(crate) struct CpuJob<C, W> {
    world: World,
    work: Option<W>,
    cancelled: Option<C>,
    outcome: CpuOutcome,
    reservation: CpuReservation,
    worker: Option<OwnedSemaphorePermit>,
    // Release the task lease before the global permit; idle observation then
    // also implies the core's native task accounting has been retired.
    lease: TaskLease,
    _global: OwnedSemaphorePermit,
}
impl<C, W> CpuJob<C, W> {
    pub(crate) fn new(
        world: World,
        reservation: CpuReservation,
        global: OwnedSemaphorePermit,
        lease: TaskLease,
        cancelled: C,
        work: W,
    ) -> Self {
        reservation
            .executor
            .counters
            .submitted
            .fetch_add(1, Ordering::AcqRel);
        Self {
            world,
            work: Some(work),
            cancelled: Some(cancelled),
            outcome: CpuOutcome::Cancelled,
            reservation,
            worker: None,
            lease,
            _global: global,
        }
    }
    fn report_panic(&mut self) {
        self.outcome = CpuOutcome::Panicked;
        let _ = self.world.report_failure(
            self.lease.owner(),
            None,
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "native_cpu", "RustPanic", vec![]),
            FailureAction::StopActor,
        );
    }
    fn start(&mut self, worker: OwnedSemaphorePermit) {
        self.reservation.queued = false;
        self.reservation
            .executor
            .counters
            .queued
            .fetch_sub(1, Ordering::AcqRel);
        self.reservation
            .executor
            .counters
            .running
            .fetch_add(1, Ordering::AcqRel);
        self.worker = Some(worker);
    }
}
impl<C, W> Drop for CpuJob<C, W> {
    fn drop(&mut self) {
        // Keep all task/byte/CPU accounting alive through arbitrary capture
        // destruction, including Tokio abort and never-started blocking tasks.
        let work_panicked = catch_unwind(AssertUnwindSafe(|| drop(self.work.take()))).is_err();
        let cancel_panicked =
            catch_unwind(AssertUnwindSafe(|| drop(self.cancelled.take()))).is_err();
        if work_panicked || cancel_panicked {
            self.report_panic();
        }
        let counters = &self.reservation.executor.counters;
        if !self.reservation.queued {
            counters.running.fetch_sub(1, Ordering::AcqRel);
        }
        match self.outcome {
            CpuOutcome::Completed => &counters.completed,
            CpuOutcome::Cancelled => &counters.cancelled,
            CpuOutcome::Panicked => &counters.panicked,
        }
        .fetch_add(1, Ordering::AcqRel);
    }
}
impl<C, W> CpuJob<C, W>
where
    C: Fn() -> bool + Send + Sync + 'static,
    W: FnOnce() -> CpuOutcome + Send + 'static,
{
    fn is_cancelled(&mut self) -> bool {
        match catch_unwind(AssertUnwindSafe(|| {
            (self.cancelled.as_ref().expect("CPU cancellation"))()
        })) {
            Ok(cancelled) => cancelled,
            Err(_) => {
                self.report_panic();
                true
            }
        }
    }
    fn execute(mut self) {
        if self.is_cancelled() {
            return;
        }
        match catch_unwind(AssertUnwindSafe(self.work.take().expect("CPU work"))) {
            Ok(outcome) => self.outcome = outcome,
            Err(_) => self.report_panic(),
        }
    }
    pub(crate) async fn run(mut self) {
        let acquire = self.reservation.executor.workers.clone().acquire_owned();
        tokio::pin!(acquire);
        loop {
            if self.is_cancelled() {
                return;
            }
            tokio::select! {
                biased;
                result = &mut acquire => {
                    let Ok(worker) = result else { return; };
                    self.start(worker);
                    // The blocking closure owns the complete job. Aborting this
                    // async monitor cannot release any of its lifetime guards.
                    let _ = tokio::task::spawn_blocking(move || self.execute()).await;
                    return;
                }
                _ = tokio::time::sleep(super::CHECK_INTERVAL) => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actorplane_core::{Config, EndpointKind, Payload};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    struct DropBomb {
        world: World,
        slots: Arc<Semaphore>,
        observed: Arc<AtomicBool>,
    }
    impl Drop for DropBomb {
        fn drop(&mut self) {
            self.observed.store(
                self.world.snapshot().retained_payload_bytes >= 8
                    && self.world.shutdown_report().native_tasks == 1
                    && self.slots.available_permits() == 0,
                Ordering::SeqCst,
            );
            panic!("ordinary captured destructor panic");
        }
    }

    #[test]
    fn unpolled_and_preexecution_cancel_keep_guards_through_panicking_capture_drop() {
        for reserved_worker in [false, true] {
            let world = World::new(Config::default()).unwrap();
            let owner = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(owner).unwrap();
            let executor = CpuExecutor::new(CpuConfig {
                workers: 1,
                max_jobs: 1,
            });
            let slots = Arc::new(Semaphore::new(1));
            let observed = Arc::new(AtomicBool::new(false));
            let invoked = Arc::new(AtomicUsize::new(0));
            let entered = invoked.clone();
            let bomb = DropBomb {
                world: world.clone(),
                slots: slots.clone(),
                observed: observed.clone(),
            };
            let input = world.hold(Payload::Pulse(1)).unwrap();
            let mut job = CpuJob::new(
                world.clone(),
                executor.reserve().unwrap(),
                slots.clone().try_acquire_owned().unwrap(),
                world.track_task(owner).unwrap(),
                || true,
                move || {
                    entered.fetch_add(1, Ordering::SeqCst);
                    drop(bomb);
                    drop(input);
                    CpuOutcome::Completed
                },
            );
            assert!(!world.stop(owner).unwrap().native_done);
            if reserved_worker {
                job.start(executor.workers.clone().try_acquire_owned().unwrap());
                job.execute();
            } else {
                drop(job.run()); // No Tokio poll ever happened.
            }
            assert!(observed.load(Ordering::SeqCst));
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            assert_eq!(slots.available_permits(), 1);
            let stats = executor.stats();
            assert_eq!((stats.queued, stats.running, stats.panicked), (0, 0, 1));
            assert_eq!(world.snapshot().metrics.failures, 1);
            assert!(world.shutdown_report().native_done);
            assert_eq!(world.snapshot().retained_payload_bytes, 0);
        }
    }
}
