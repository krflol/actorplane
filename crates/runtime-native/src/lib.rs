//! Bounded Tokio execution for the reference source, window counter and sink.
#![forbid(unsafe_code)]

use actorplane_core::{
    ActorRef, EndpointKind, FailureAction, FailureDetails, FailurePhase, Lifecycle, MessageOptions,
    Payload, StopReport, TaskLease, World,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

const CHECK_INTERVAL: Duration = Duration::from_millis(1);
const MAX_PERIOD: Duration = Duration::from_secs(86400);
const BATCH: usize = 32;
pub mod components;
pub mod computation;
pub mod controls;
pub mod cpu;
#[cfg(feature = "native-io")]
pub mod io;
pub mod sdk;
mod sdk_config;
mod tasks;
#[cfg(feature = "test-runtime")]
pub mod testing;

struct CatchPanic<F: Future> {
    future: Pin<Box<F>>,
}
impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, ()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            this.future.as_mut().poll(cx)
        })) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PipelineSnapshot {
    pub generated: u64,
    pub processed: u64,
    pub summaries: u64,
    pub sink_received: u64,
    pub first_progress_ns: Option<u64>,
    pub last_progress_ns: Option<u64>,
    pub last_total: i64,
    pub total_ticks: i64,
}

struct Inner {
    slots: Arc<Semaphore>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    max: usize,
    cpu: Option<Arc<cpu::CpuExecutor>>,
    #[cfg(feature = "test-runtime")]
    drive: Option<Arc<testing::DriveSignal>>,
}

pub struct NativeRuntime {
    world: World,
    runtime: Option<Runtime>,
    inner: Arc<Inner>,
    control: Option<JoinHandle<()>>,
    routing: Option<JoinHandle<()>>,
    shutdown_timed_out: bool,
    #[cfg(feature = "test-runtime")]
    virtual_driver: Option<testing::VirtualDriver>,
}

impl NativeRuntime {
    pub fn new(world: World, max_tasks: usize) -> Result<Self, String> {
        Self::new_with_cpu_config(world, max_tasks, cpu::CpuConfig::default())
    }

    pub fn new_with_cpu_config(
        world: World,
        max_tasks: usize,
        cpu_config: cpu::CpuConfig,
    ) -> Result<Self, String> {
        if world.is_virtual() {
            return Err("a virtual World requires the explicitly driven test runtime".into());
        }
        if max_tasks == 0 || max_tasks > 65536 {
            return Err("native task limit must be in 1..65536".into());
        }
        if !(1..=64).contains(&cpu_config.workers) || !(1..=4096).contains(&cpu_config.max_jobs) {
            return Err("CPU workers must be in 1..64 and max_jobs in 1..4096".into());
        }
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(2)
            .max_blocking_threads(cpu_config.workers)
            .enable_time();
        #[cfg(feature = "native-io")]
        builder.enable_io();
        let runtime = builder.build().map_err(|e| e.to_string())?;
        let control_world = world.clone();
        let control = runtime.spawn(async move {
            let mut tick = periodic(CHECK_INTERVAL);
            loop {
                tick.tick().await;
                control_world.maintain(Instant::now());
            }
        });
        let mut runtime_state = Self {
            world,
            runtime: Some(runtime),
            inner: Arc::new(Inner {
                slots: Arc::new(Semaphore::new(max_tasks)),
                tasks: Mutex::new(Vec::new()),
                max: max_tasks,
                cpu: Some(cpu::CpuExecutor::new(cpu_config)),
                #[cfg(feature = "test-runtime")]
                drive: None,
            }),
            control: Some(control),
            routing: None,
            shutdown_timed_out: false,
            #[cfg(feature = "test-runtime")]
            virtual_driver: None,
        };
        runtime_state.start_routing_service();
        Ok(runtime_state)
    }

    pub fn cpu_stats(&self) -> cpu::CpuStats {
        self.inner
            .cpu
            .as_ref()
            .map_or(cpu::CpuStats::default(), |cpu| cpu.stats())
    }

    pub fn cpu_monitor(&self) -> cpu::CpuMonitor {
        cpu::CpuMonitor {
            executor: self.inner.cpu.clone(),
        }
    }

    pub fn world(&self) -> &World {
        &self.world
    }
    pub fn elapsed_ns(&self) -> u64 {
        self.world.elapsed_ns()
    }
    pub fn active_tasks(&self) -> usize {
        self.inner.max - self.inner.slots.available_permits()
    }
    pub fn control_tasks(&self) -> usize {
        usize::from(
            self.control
                .as_ref()
                .is_some_and(|task| !task.is_finished()),
        )
    }

    pub fn routing_tasks(&self) -> usize {
        usize::from(
            self.routing
                .as_ref()
                .is_some_and(|task| !task.is_finished()),
        )
    }

    fn start_routing_service(&mut self) {
        #[cfg(feature = "test-runtime")]
        if let Some(driver) = self.virtual_driver.as_ref() {
            let Some(runtime) = self.runtime.as_ref() else {
                return;
            };
            let signal = driver.signal();
            let world = self.world.clone();
            self.routing = Some(runtime.spawn(testing::Tracked {
                future: Box::pin(routing_service(world)),
                signal,
            }));
            return;
        }
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        let world = self.world.clone();
        self.routing = Some(runtime.spawn(routing_service(world)));
    }

    pub(crate) fn task_service(&self) -> Result<tasks::TaskService, String> {
        let runtime = self.runtime.as_ref().ok_or("native runtime is closed")?;
        Ok(tasks::TaskService::new(
            self.world.clone(),
            runtime.handle().clone(),
            self.inner.clone(),
        ))
    }

    fn permits(&self, count: u32) -> Result<OwnedSemaphorePermit, String> {
        self.task_service()?.permits(count)
    }

    fn spawn<F, Fut>(&self, permit: OwnedSemaphorePermit, lease: TaskLease, make_future: F)
    where
        F: FnOnce(TaskLease) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.task_service()
            .expect("runtime checked before reservation")
            .spawn(permit, lease, make_future)
    }

    pub fn start_counter(
        &self,
        parent: Option<ActorRef>,
        interval: Duration,
        window: Duration,
        python_target: Option<ActorRef>,
    ) -> Result<CounterPipeline, String> {
        if interval.is_zero() || window.is_zero() || interval > MAX_PERIOD || window > MAX_PERIOD {
            return Err("interval and window must be positive and at most 24 hours".into());
        }
        let mut permits = self.permits(3)?;
        let owner = self
            .world
            .allocate(EndpointKind::Native, parent)
            .map_err(|e| e.to_string())?;
        let setup = (|| {
            let source = self.world.allocate(EndpointKind::Native, Some(owner))?;
            let counter = self.world.allocate(EndpointKind::Native, Some(owner))?;
            let sink = self.world.allocate(EndpointKind::Native, Some(owner))?;
            self.world.subscribe(source, counter)?;
            self.world.subscribe(counter, sink)?;
            if let Some(target) = python_target {
                self.world.subscribe(counter, target)?;
            }
            let source_lease = self.world.track_task(owner)?;
            let counter_lease = self.world.track_task(owner)?;
            let sink_lease = self.world.track_task(owner)?;
            for endpoint in [source, counter, sink, owner] {
                self.world.activate(endpoint)?;
            }
            Ok::<_, actorplane_core::Error>((
                source,
                counter,
                sink,
                source_lease,
                counter_lease,
                sink_lease,
            ))
        })();
        let (source, counter, sink, source_lease, counter_lease, sink_lease) = match setup {
            Ok(setup) => setup,
            Err(e) => {
                let _ = self.world.stop(owner);
                return Err(e.to_string());
            }
        };
        let stats = Arc::new(Mutex::new(PipelineSnapshot::default()));
        let world = self.world.clone();
        let source_stats = stats.clone();
        self.spawn(
            permits.split(1).unwrap(),
            source_lease,
            move |lease| async move {
                let _lease = lease;
                if !wait_active(&world, owner).await {
                    return;
                }
                let mut tick = periodic(interval);
                let mut check = periodic(CHECK_INTERVAL);
                loop {
                    tokio::select! {
                        _ = check.tick() => { if !is_active(&world, owner) { break; } }
                        _ = tick.tick() => {
                            if !is_active(&world, owner) { break; }
                            {
                                let mut s = source_stats.lock().unwrap();
                                s.generated = s.generated.saturating_add(1);
                                s.total_ticks = s.total_ticks.saturating_add(1);
                                let t = world.elapsed_ns();
                                s.first_progress_ns.get_or_insert(t);
                                s.last_progress_ns = Some(t);
                            }
                            // Each attempt is a real queued native routing operation.
                            let _ = world.publish(source, Payload::Pulse(1));
                        }
                    }
                }
            },
        );
        let world = self.world.clone();
        let counter_stats = stats.clone();
        let counter_done = Arc::new(AtomicBool::new(false));
        let done_guard = CompletionFlag(counter_done.clone());
        self.spawn(permits.split(1).unwrap(), counter_lease, move |lease| async move {
            let _done = done_guard;
            if !wait_ready(&world, owner, true).await { return; }
            let mut windows = periodic(window);
            let mut poll = periodic(CHECK_INTERVAL);
            let mut count = 0u64;
            let mut total = 0i64;
            loop {
                tokio::select! {
                    _ = windows.tick() => {
                        if !matches!(world.execution_state(owner), Ok(Lifecycle::Active | Lifecycle::Quiescing)) { break; }
                        if count != 0 {
                            let _ = lease.publish_completion(counter, Payload::CountSnapshot { count, total });
                            let mut s = counter_stats.lock().unwrap();
                            s.summaries = s.summaries.saturating_add(1);
                            s.last_total = total;
                            count = 0;
                            total = 0;
                        }
                    }
                    _ = poll.tick() => {
                        let state = world.execution_state(owner);
                        if !matches!(state, Ok(Lifecycle::Active | Lifecycle::Quiescing)) { break; }
                        let mut drained = false;
                        for _ in 0..BATCH {
                            let Ok(Some(delivery)) = world.claim(counter) else { drained = true; break; };
                            let update = match delivery.payload() {
                                Payload::Pulse(value) => count.checked_add(1).zip(total.checked_add(*value)),
                                _ => None,
                            };
                            let Some((new_count, new_total)) = update else {
                                let _ = world.report_failure(owner, Some(delivery.event_id()), None, delivery.operation(),
                                    FailureDetails::new(FailurePhase::Handler, "native_counter", "InvalidPulseOrOverflow", vec![]),
                                    FailureAction::StopActor);
                                delivery.finish(false);
                                let _ = world.stop(owner);
                                return;
                            };
                            count = new_count;
                            total = new_total;
                            counter_stats.lock().unwrap().processed += 1;
                            delivery.finish(true);
                        }
                        if drained && state == Ok(Lifecycle::Quiescing)
                            && world.upstream_done(counter).unwrap_or(false)
                        {
                            if count > 0 {
                                let _ = lease.publish_completion(counter, Payload::CountSnapshot { count, total });
                                let mut s = counter_stats.lock().unwrap();
                                s.summaries = s.summaries.saturating_add(1);
                                s.last_total = total;
                            }
                            break;
                        }
                    }
                }
            }
        });
        let world = self.world.clone();
        let sink_stats = stats.clone();
        self.spawn(permits, sink_lease, move |lease| async move {
            let _lease = lease;
            if !wait_ready(&world, owner, true).await {
                return;
            }
            let mut poll = periodic(CHECK_INTERVAL);
            loop {
                poll.tick().await;
                let state = world.execution_state(owner);
                if !matches!(state, Ok(Lifecycle::Active | Lifecycle::Quiescing)) {
                    break;
                }
                for _ in 0..BATCH {
                    let Ok(Some(lease)) = world.claim(sink) else {
                        if state == Ok(Lifecycle::Quiescing)
                            && counter_done.load(Ordering::Acquire)
                            && world.upstream_done(sink).unwrap_or(false)
                        {
                            return;
                        }
                        break;
                    };
                    if !matches!(lease.payload(), Payload::CountSnapshot { .. }) {
                        lease.finish(false);
                        let _ = world.stop(owner);
                        return;
                    }
                    sink_stats.lock().unwrap().sink_received += 1;
                    lease.finish(true);
                }
            }
        });
        Ok(CounterPipeline {
            owner,
            source,
            counter,
            sink,
            stats,
            world: self.world.clone(),
        })
    }

    pub fn after(
        &self,
        owner: ActorRef,
        target: ActorRef,
        delay: Duration,
        payload: Payload,
    ) -> Result<(), String> {
        self.after_with(owner, target, delay, payload, MessageOptions::default())
    }

    pub fn after_with(
        &self,
        owner: ActorRef,
        target: ActorRef,
        delay: Duration,
        payload: Payload,
        options: MessageOptions,
    ) -> Result<(), String> {
        self.task_service()?
            .after_with(owner, target, delay, payload, options)
    }

    pub fn close(&mut self, timeout: Duration) -> Result<StopReport, String> {
        #[cfg(feature = "test-runtime")]
        if self.virtual_driver.is_some() {
            return self.close_virtual(timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or("shutdown duration overflow")?;
        let initial = self.world.close();
        if let Some(routing) = self.routing.take() {
            routing.abort();
        }
        if let Some(control) = self.control.take() {
            control.abort();
        }
        self.inner.slots.close();
        let Some(runtime) = self.runtime.take() else {
            let mut report = initial;
            report.timed_out |= self.shutdown_timed_out;
            return Ok(report);
        };
        while self.active_tasks() > 0 && Instant::now() < deadline {
            std::thread::sleep(CHECK_INTERVAL);
        }
        self.shutdown_timed_out |= self.active_tasks() > 0;
        let tasks = std::mem::take(&mut *self.inner.tasks.lock().unwrap());
        for task in tasks {
            if !task.is_finished() {
                task.abort();
            }
        }
        runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        let mut report = self.world.shutdown_report();
        report.discarded = initial.discarded;
        report.timed_out |= self.shutdown_timed_out;
        Ok(report)
    }

    pub fn wait_native_idle(&self, timeout: Duration) -> bool {
        #[cfg(feature = "test-runtime")]
        if self.virtual_driver.is_some() {
            return self.wait_virtual_idle(timeout).unwrap_or(false);
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        while self.active_tasks() > 0 || self.world.routing_pending() > 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(CHECK_INTERVAL);
        }
        // The maintenance task may be scheduled just after the final native
        // lease and publication disappear. Apply that lifecycle transition
        // before reporting idle so a drained owner is not observed Quiescing.
        self.world.maintain(Instant::now());
        true
    }
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        let _ = self.close(Duration::from_millis(100));
    }
}

fn periodic(period: Duration) -> time::Interval {
    let mut interval = time::interval_at(time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

async fn routing_service(world: World) {
    loop {
        // Readiness is distinct from pending: staged publications may be
        // retained while their owner is still Starting.  Waiting on the
        // dedicated signal avoids polling those entries as a hot loop.
        if std::future::poll_fn(|cx| world.poll_routing_ready(cx))
            .await
            .is_err()
        {
            break;
        }
        let _progress = world.route_batch();
        // Every routing turn yields, including zero-destination publications,
        // so a continuously admitted stream cannot starve lifecycle work.
        tokio::task::yield_now().await;
    }
}

fn is_active(world: &World, owner: ActorRef) -> bool {
    matches!(world.execution_state(owner), Ok(Lifecycle::Active))
}
async fn wait_active(world: &World, owner: ActorRef) -> bool {
    wait_ready(world, owner, false).await
}
async fn wait_ready(world: &World, owner: ActorRef, allow_drain: bool) -> bool {
    loop {
        match world.execution_state(owner) {
            Ok(Lifecycle::Active) => return true,
            Ok(Lifecycle::Quiescing) if allow_drain => return true,
            Ok(Lifecycle::Starting) => time::sleep(CHECK_INTERVAL).await,
            _ => return false,
        }
    }
}

struct CompletionFlag(Arc<AtomicBool>);
impl Drop for CompletionFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct CounterPipeline {
    pub owner: ActorRef,
    pub source: ActorRef,
    pub counter: ActorRef,
    pub sink: ActorRef,
    stats: Arc<Mutex<PipelineSnapshot>>,
    world: World,
}
impl CounterPipeline {
    pub fn snapshot(&self) -> PipelineSnapshot {
        *self.stats.lock().unwrap()
    }
    pub fn stop(&self) {
        let _ = self.world.stop(self.owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actorplane_core::{Config, EndpointKind};

    #[test]
    fn shutdown_timeout_keeps_noncooperative_task_counted_until_it_returns() {
        use std::sync::mpsc;
        let world = World::new(Config::default()).unwrap();
        let owner = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(owner).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), 1).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        runtime.spawn(
            runtime.permits(1).unwrap(),
            world.track_task(owner).unwrap(),
            move |_lease| async move {
                started_tx.send(()).unwrap();
                // An intentional noncooperative poll, with a watchdog so a
                // failing assertion cannot strand a worker indefinitely.
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            },
        );
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let report = runtime.close(Duration::from_millis(5)).unwrap();
        assert!(report.timed_out);
        assert!(!report.native_done);
        assert!(report.python_done);
        assert_eq!(report.native_tasks, 1);
        assert_eq!(report.in_flight, 1);
        assert_eq!(world.state(owner).unwrap(), Lifecycle::Stopping);
        release_tx.send(()).unwrap();
        assert!(runtime.wait_native_idle(Duration::from_secs(1)));
        let report = runtime.close(Duration::ZERO).unwrap();
        assert!(report.native_done && report.timed_out);
        assert_eq!(report.native_tasks, 0);
        assert_eq!(world.state(owner).unwrap(), Lifecycle::Stopped);
    }

    #[test]
    fn panicking_native_task_is_contained_and_fenced() {
        let world = World::new(Config::default()).unwrap();
        let owner = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(owner).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), 1).unwrap();
        let permit = runtime.permits(1).unwrap();
        let lease = world.track_task(owner).unwrap();
        runtime.spawn(permit, lease, |_lease| async move {
            panic!("test panic payload is not reported")
        });
        assert!(runtime.wait_native_idle(Duration::from_secs(1)));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !matches!(world.state(owner), Ok(Lifecycle::Stopped)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(matches!(world.state(owner), Ok(Lifecycle::Stopped)));
        assert_eq!(runtime.active_tasks(), 0);
        let history = world.diagnostics(0, 16).unwrap();
        let failure = history
            .entries
            .iter()
            .find_map(|entry| entry.failure.as_ref())
            .expect("panic diagnostic");
        assert_eq!(failure.details.exception_type(), "RustPanic");
        assert_eq!(failure.actor, owner);
        runtime.close(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn native_counter_type_failure_stops_owner() {
        let world = World::new(Config::default()).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
        let pipeline = runtime
            .start_counter(None, Duration::from_millis(1), Duration::from_secs(1), None)
            .unwrap();
        world
            .send(
                pipeline.counter,
                Payload::Record {
                    schema: 1,
                    integers: vec![1],
                },
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !matches!(world.state(pipeline.owner), Ok(Lifecycle::Stopped))
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(matches!(
            world.state(pipeline.owner),
            Ok(Lifecycle::Stopped)
        ));
        assert!(
            world
                .diagnostics(0, 16)
                .unwrap()
                .entries
                .iter()
                .any(|entry| entry
                    .failure
                    .as_ref()
                    .is_some_and(
                        |failure| failure.details.exception_type() == "InvalidPulseOrOverflow"
                    ))
        );
        runtime.close(Duration::from_secs(1)).unwrap();
    }

    #[cfg(feature = "test-runtime")]
    #[test]
    fn legacy_counter_waits_for_pending_source_and_completion_routes_during_drain() {
        let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
        let world = runtime.world.clone();
        let routing = runtime.routing.take().expect("routing service");
        routing.abort();
        let pipeline = runtime
            .start_counter(
                None,
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                None,
            )
            .unwrap();

        let _ = runtime.pump_virtual(128).unwrap();

        world.publish(pipeline.source, Payload::Pulse(7)).unwrap();
        world
            .request_drain(pipeline.owner, world.now() + Duration::from_secs(1))
            .unwrap();
        let _ = runtime
            .advance_virtual(Duration::from_millis(1), 128)
            .unwrap();
        assert_eq!(pipeline.snapshot().processed, 0);
        assert_eq!(world.routing_pending(), 1);
        assert_eq!(runtime.active_tasks(), 2);

        // Admit the source pulse manually. The counter must remain retained
        // until its final completion publication is routed.
        world.route_batch();
        let _ = runtime
            .advance_virtual(Duration::from_millis(1), 128)
            .unwrap();
        let snapshot = pipeline.snapshot();
        assert_eq!(snapshot.processed, 1);
        assert_eq!(snapshot.summaries, 1);
        assert!(world.routing_pending() > 0);

        world.route_batch();
        let _ = runtime
            .advance_virtual(Duration::from_millis(1), 256)
            .unwrap();
        assert!(runtime.wait_virtual_idle(Duration::from_secs(1)).unwrap());
        assert_eq!(pipeline.snapshot().sink_received, 1);
        let report = runtime.close(Duration::from_secs(1)).unwrap();
        assert!(report.native_done);
        assert!(!report.timed_out);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
    }

    #[test]
    fn task_factory_panic_is_contained_with_its_owner_reservation() {
        let world = World::new(Config::default()).unwrap();
        let owner = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(owner).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), 1).unwrap();
        runtime.spawn(
            runtime.permits(1).unwrap(),
            world.track_task(owner).unwrap(),
            |_lease| {
                panic!("task factory failed");
                #[allow(unreachable_code)]
                std::future::ready(())
            },
        );
        assert!(runtime.wait_native_idle(Duration::from_secs(1)));
        assert_eq!(world.state(owner).unwrap(), Lifecycle::Stopped);
        assert_eq!(world.snapshot().metrics.failures, 1);
        runtime.close(Duration::from_secs(1)).unwrap();
    }
}
