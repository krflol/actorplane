//! Explicitly driven, single-thread native execution with frozen virtual time.
//! This uses the production SDK and Tokio futures, never a simulated behavior.
use super::*;
use actorplane_core::{Clock, Config, VirtualClock};
use std::{
    sync::atomic::AtomicU64,
    task::Waker,
    thread::{self, ThreadId},
};

pub(crate) struct DriveSignal {
    polls: AtomicU64,
    parked: AtomicBool,
    driver: Mutex<Option<Waker>>,
}
impl DriveSignal {
    fn new() -> Self {
        Self {
            polls: AtomicU64::new(0),
            parked: AtomicBool::new(false),
            driver: Mutex::new(None),
        }
    }
    fn wake_driver(&self) {
        let waker = self.driver.lock().unwrap().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn park(&self) {
        self.parked.store(true, Ordering::Release);
        self.wake_driver();
    }
    fn polled(&self) {
        let _ = self
            .polls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_add(1))
            });
        self.wake_driver();
    }
}

pub(crate) struct Tracked<F: Future> {
    pub(crate) future: Pin<Box<F>>,
    pub(crate) signal: Arc<DriveSignal>,
}
impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.signal.polled();
        this.future.as_mut().poll(cx)
    }
}

pub(crate) struct VirtualDriver {
    clock: VirtualClock,
    signal: Arc<DriveSignal>,
    owner: ThreadId,
}

impl VirtualDriver {
    pub(crate) fn signal(&self) -> Arc<DriveSignal> {
        self.signal.clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PumpReport {
    pub polls: u64,
    pub idle: bool,
}

impl NativeRuntime {
    /// Create a runtime with no worker threads. Only this creating thread may
    /// pump or advance it. Time never advances as a side effect of idle waiting.
    pub fn new_virtual(config: Config, max_tasks: usize) -> Result<Self, String> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err("TestWorld cannot be constructed inside a Tokio runtime".into());
        }
        if max_tasks == 0 || max_tasks > 65536 {
            return Err("native task limit must be in 1..65536".into());
        }
        let signal = Arc::new(DriveSignal::new());
        let hook = signal.clone();
        let runtime = Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .event_interval(1)
            .on_thread_park(move || hook.park())
            .build()
            .map_err(|error| error.to_string())?;
        let epoch = runtime.block_on(async { time::Instant::now().into_std() });
        let (clock, control) = Clock::manual_at(epoch);
        let world = World::with_clock(config, clock).map_err(|error| error.to_string())?;
        let mut runtime_state = Self {
            world,
            runtime: Some(runtime),
            control: None,
            routing: None,
            shutdown_timed_out: false,
            inner: Arc::new(Inner {
                slots: Arc::new(Semaphore::new(max_tasks)),
                tasks: Mutex::new(Vec::new()),
                max: max_tasks,
                cpu: None,
                drive: Some(signal.clone()),
            }),
            virtual_driver: Some(VirtualDriver {
                clock: control,
                signal,
                owner: thread::current().id(),
            }),
        };
        runtime_state.start_routing_service();
        Ok(runtime_state)
    }

    pub fn is_virtual(&self) -> bool {
        self.virtual_driver.is_some()
    }

    fn virtual_state(&self) -> Result<&VirtualDriver, String> {
        let driver = self
            .virtual_driver
            .as_ref()
            .ok_or("runtime is not a TestWorld")?;
        if driver.owner != thread::current().id() {
            return Err("TestWorld must be driven on its creating thread".into());
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err("TestWorld driving is not reentrant".into());
        }
        Ok(driver)
    }

    /// Run currently ready work without advancing the clock. The budget counts
    /// polls of owned native tasks, not wall time or application instructions.
    /// Hooks/future polls must return; blocking application code is cooperative.
    pub fn pump_virtual(&self, max_polls: u64) -> Result<PumpReport, String> {
        if !(1..=1_000_000).contains(&max_polls) {
            return Err("max_polls must be in 1..1000000".into());
        }
        let driver = self.virtual_state()?;
        let runtime = self.runtime.as_ref().ok_or("native runtime is closed")?;
        self.world.maintain(self.world.now());
        let signal = &driver.signal;
        signal.parked.store(false, Ordering::Release);
        let start = signal.polls.load(Ordering::Acquire);
        let mut last_idle = None;
        let result = runtime.block_on(std::future::poll_fn(|cx| {
            *signal.driver.lock().unwrap() = Some(cx.waker().clone());
            let polls = signal.polls.load(Ordering::Acquire);
            if polls == u64::MAX || polls.saturating_sub(start) >= max_polls {
                return Poll::Ready(PumpReport {
                    polls: polls.saturating_sub(start),
                    idle: false,
                });
            }
            if signal.parked.swap(false, Ordering::AcqRel) {
                // The first park also polls the timer driver without blocking.
                // A second park with no intervening task polls proves it did
                // not release additional ready native work.
                if last_idle == Some(polls) {
                    return Poll::Ready(PumpReport {
                        polls: polls.saturating_sub(start),
                        idle: true,
                    });
                }
                last_idle = Some(polls);
            }
            Poll::Pending
        }));
        signal.driver.lock().unwrap().take();
        let tokio_now = runtime.block_on(async { time::Instant::now().into_std() });
        if tokio_now != driver.clock.current() {
            return Err("TestWorld clock advanced without an explicit advance".into());
        }
        self.world.maintain(self.world.now());
        Ok(result)
    }

    /// Jump the clock, apply expiry first, then run ready work. Periodic native
    /// components retain their normal missed-tick Skip behavior.
    pub fn advance_virtual(
        &self,
        duration: Duration,
        max_polls: u64,
    ) -> Result<PumpReport, String> {
        if duration > MAX_PERIOD {
            return Err("virtual advance exceeds 24 hours".into());
        }
        if !(1..=1_000_000).contains(&max_polls) {
            return Err("max_polls must be in 1..1000000".into());
        }
        let driver = self.virtual_state()?;
        let runtime = self.runtime.as_ref().ok_or("native runtime is closed")?;
        driver
            .clock
            .advance(duration)
            .map_err(|error| error.to_string())?;
        self.world.maintain(self.world.now());
        // Core and Tokio clocks move before any application task is polled.
        let before = driver.signal.polls.load(Ordering::Acquire);
        runtime.block_on(time::advance(duration));
        let advanced_polls = driver
            .signal
            .polls
            .load(Ordering::Acquire)
            .saturating_sub(before);
        let remaining = max_polls.saturating_sub(advanced_polls);
        if remaining == 0 {
            return Ok(PumpReport {
                polls: advanced_polls,
                idle: false,
            });
        }
        let report = self.pump_virtual(remaining)?;
        Ok(PumpReport {
            polls: advanced_polls + report.polls,
            idle: report.idle,
        })
    }

    pub(crate) fn wait_virtual_idle(&self, timeout: Duration) -> Result<bool, String> {
        self.virtual_state()?;
        let deadline = self
            .world
            .now()
            .checked_add(timeout)
            .ok_or("wait duration overflow")?;
        let mut remaining = 100_000;
        while (self.active_tasks() > 0 || self.world.routing_pending() > 0) && remaining > 0 {
            let report = self.pump_virtual(remaining)?;
            remaining = remaining.saturating_sub(report.polls.max(1));
            if self.active_tasks() == 0 && self.world.routing_pending() == 0 {
                return Ok(true);
            }
            if self.world.now() >= deadline || remaining == 0 {
                return Ok(false);
            }
            if report.idle {
                let step = deadline
                    .saturating_duration_since(self.world.now())
                    .min(CHECK_INTERVAL);
                let report = self.advance_virtual(step, remaining)?;
                remaining = remaining.saturating_sub(report.polls.max(1));
            }
        }
        Ok(self.active_tasks() == 0 && self.world.routing_pending() == 0)
    }

    pub(crate) fn close_virtual(&mut self, timeout: Duration) -> Result<StopReport, String> {
        self.virtual_state()?;
        self.world
            .now()
            .checked_add(timeout)
            .ok_or("shutdown duration overflow")?;
        let initial = self.world.close();
        if let Some(routing) = self.routing.take() {
            routing.abort();
        }
        self.inner.slots.close();
        if self.runtime.is_none() {
            let mut report = initial;
            report.timed_out |= self.shutdown_timed_out;
            return Ok(report);
        }
        self.shutdown_timed_out |= !self.wait_virtual_idle(timeout)?;
        let tasks = std::mem::take(&mut *self.inner.tasks.lock().unwrap());
        for task in tasks {
            if !task.is_finished() {
                task.abort();
            }
        }
        // Dropping the current-thread runtime disposes cancelled futures now;
        // task leases remain owned until their destructors actually return.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::ZERO);
        }
        let mut report = self.world.shutdown_report();
        report.discarded = initial.discarded;
        report.timed_out |= self.shutdown_timed_out;
        Ok(report)
    }
}
