//! Source-level native components with serialized state and bounded effects.

pub use crate::sdk_config::{DrainPolicy, NativeError, NativeLimits, NativeSpec};
use crate::{CatchPanic, NativeRuntime, tasks::TaskService};
use actorplane_core::{
    ActorRef, DeliveryLease, EndpointKind, Envelope, FailureAction, FailureDetails, FailurePhase,
    FailureRecord, HeldPayload, Lifecycle, MessageOptions, Payload, PortRef, PublicationTicket,
    TaskLease, World,
    operations::{OperationId, OperationStatus},
    schema::Value,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

pub type NativeResult<T = ()> = Result<T, NativeError>;

/// State is created once, moved into one executor task, and never exposed by a
/// handle. Hooks are short synchronous Rust code; async jobs own their input.
pub trait NativeBehavior: Send + 'static {
    fn on_start(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult;
    fn on_tick(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_quiesce(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_failure(
        &mut self,
        _failure: &FailureRecord,
        _coalesced: u64,
        _ctx: &mut NativeContext<'_>,
    ) -> NativeResult<FailureAction> {
        Ok(FailureAction::StopWorld)
    }
    fn on_drain(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_stop(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativeStats {
    pub turns: u64,
    pub handled: u64,
    pub ticks: u64,
    pub effects_attempted: u64,
    pub jobs_submitted: u64,
}

#[derive(Clone)]
pub struct NativeHandle {
    pub owner: ActorRef,
    pub ports: Vec<PortRef>,
    spec: Arc<NativeSpec>,
    stats: Arc<Mutex<NativeStats>>,
}
impl NativeHandle {
    pub fn spec(&self) -> &NativeSpec {
        &self.spec
    }
    pub fn stats(&self) -> NativeStats {
        *self.stats.lock().unwrap()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Startup,
    Event,
    Tick,
    Supervisor,
    Quiesce,
    Drain,
    Stop,
}

struct Execution {
    service: TaskService,
    owner: ActorRef,
    lease: TaskLease,
    spec: Arc<NativeSpec>,
    stats: Arc<Mutex<NativeStats>>,
    jobs: Arc<Semaphore>,
}
impl Execution {
    fn context<'a>(
        &'a self,
        phase: Phase,
        delivery: Option<&'a DeliveryLease>,
    ) -> NativeContext<'a> {
        NativeContext {
            execution: self,
            phase,
            delivery,
            remaining: self.spec.limits.effects_per_callback,
            deferred: false,
        }
    }
    fn world(&self) -> &World {
        self.service.world()
    }
    fn failure(
        &self,
        phase: FailurePhase,
        delivery: Option<&DeliveryLease>,
        error: &NativeError,
        panic: bool,
    ) {
        let _ = self.world().report_failure(
            self.owner,
            delivery.map(DeliveryLease::event_id),
            delivery.map(|value| value.envelope().schema.id),
            delivery.and_then(DeliveryLease::operation),
            FailureDetails::new(phase, &self.spec.descriptor.name, error.code(), vec![]),
            if panic || phase != FailurePhase::Handler {
                FailureAction::StopActor
            } else {
                self.spec.failure_action
            },
        );
    }
}

/// A borrowed effect context cannot escape a hook or borrow component state
/// into a 'static asynchronous operation.
pub struct NativeContext<'a> {
    execution: &'a Execution,
    phase: Phase,
    delivery: Option<&'a DeliveryLease>,
    remaining: usize,
    deferred: bool,
}
impl NativeContext<'_> {
    pub fn owner(&self) -> ActorRef {
        self.execution.owner
    }
    pub fn envelope(&self) -> Option<&Envelope> {
        self.delivery.map(DeliveryLease::envelope)
    }
    pub fn operation(&self) -> Option<OperationId> {
        self.delivery.and_then(DeliveryLease::operation)
    }
    pub fn state(&self) -> NativeResult<Lifecycle> {
        Ok(self.execution.world().execution_state(self.owner())?)
    }
    pub fn port(&self, name: &str) -> NativeResult<PortRef> {
        Ok(self.execution.world().port(self.owner(), name)?)
    }
    fn effect(&mut self) -> NativeResult {
        {
            let mut stats = self.execution.stats.lock().unwrap();
            stats.effects_attempted = stats.effects_attempted.saturating_add(1);
        }
        if self.phase == Phase::Stop {
            return Err(NativeError::Core(actorplane_core::Error::ActorStopped));
        }
        if self.remaining == 0 {
            return Err(NativeError::Limit("callback effects"));
        }
        self.remaining -= 1;
        Ok(())
    }
    fn options(&self, options: MessageOptions) -> NativeResult<MessageOptions> {
        options.validate()?;
        if options.source.is_some_and(|source| source != self.owner()) {
            return Err(NativeError::Core(actorplane_core::Error::InvalidMetadata));
        }
        let mut result = self
            .envelope()
            .map_or_else(MessageOptions::default, |value| {
                value.child_options(self.owner())
            });
        result.source = Some(self.owner());
        result.deadline = match (result.deadline, options.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        result.correlation_id = options.correlation_id.or(result.correlation_id);
        result.causation_id = options.causation_id.or(result.causation_id);
        result.trace = options.trace.or(result.trace);
        Ok(result)
    }
    fn new_work(&self) -> NativeResult {
        if !matches!(
            self.phase,
            Phase::Startup | Phase::Event | Phase::Tick | Phase::Supervisor
        ) || !matches!(self.state()?, Lifecycle::Starting | Lifecycle::Active)
        {
            return Err(NativeError::Core(actorplane_core::Error::ActorStopped));
        }
        Ok(())
    }
    pub fn emit(&mut self, port: &str, payload: Payload) -> NativeResult<PublicationTicket> {
        self.emit_with(port, payload, MessageOptions::default())
    }
    pub fn emit_with(
        &mut self,
        port: &str,
        payload: Payload,
        options: MessageOptions,
    ) -> NativeResult<PublicationTicket> {
        self.effect()?;
        let output = self.port(port)?;
        let options = self.options(options)?;
        Ok(if let Some(delivery) = self.delivery {
            delivery.publish_port_completion_with(output, payload, options)?
        } else if matches!(self.phase, Phase::Tick | Phase::Drain | Phase::Quiesce) {
            self.execution
                .lease
                .publish_port_completion_with(output, payload, options)?
        } else {
            self.execution
                .world()
                .publish_port_with(output, payload, options)?
        })
    }
    pub fn send(
        &mut self,
        target: ActorRef,
        payload: Payload,
        options: MessageOptions,
    ) -> NativeResult<u64> {
        self.effect()?;
        self.new_work()?;
        Ok(self
            .execution
            .world()
            .send_with(target, payload, self.options(options)?)?)
    }
    pub fn reply(&mut self, payload: Payload) -> NativeResult<bool> {
        self.effect()?;
        if self.deferred {
            return Err(NativeError::Application("ReplyAlreadyDeferred"));
        }
        let operation = self
            .operation()
            .ok_or(NativeError::Application("NotARequest"))?;
        Ok(self
            .execution
            .world()
            .complete_operation(operation, self.owner(), payload)?)
    }
    pub fn after(
        &mut self,
        delay: Duration,
        target: ActorRef,
        payload: Payload,
        options: MessageOptions,
    ) -> NativeResult {
        self.effect()?;
        self.new_work()?;
        let local = self
            .execution
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| NativeError::Limit("component jobs"))?;
        self.execution.service.after_with_local(
            self.owner(),
            target,
            delay,
            payload,
            self.options(options)?,
            local,
        )
    }
    /// Complete this request asynchronously, using immutable owned input.
    /// Admission reserves both the component and runtime task budgets first.
    pub fn defer_reply<F, Fut>(&mut self, input: Payload, factory: F) -> NativeResult
    where
        F: FnOnce(HeldPayload, JobContext) -> Fut + Send + 'static,
        Fut: Future<Output = NativeResult<Payload>> + Send + 'static,
    {
        self.effect()?;
        if self.deferred {
            return Err(NativeError::Application("ReplyAlreadyDeferred"));
        }
        let operation = self
            .operation()
            .ok_or(NativeError::Application("NotARequest"))?;
        // Cancellation may win after this delivery was claimed, including
        // retirement/reuse of the result slot. That is ordinary request loss,
        // not a component failure; do not invoke its deferred factory.
        let retired = || {
            matches!(
                self.execution.world().operation_status(operation),
                Ok(OperationStatus::Terminal(_)) | Err(actorplane_core::Error::StaleReference)
            )
        };
        if retired() {
            self.deferred = true;
            return Ok(());
        }
        let owner = self.owner();
        let prepared = (|| {
            self.new_work()?;
            let options = self.options(MessageOptions::default())?;
            let input = self.execution.world().hold_with(input, options)?;
            if input.retained_bytes() > self.execution.spec.limits.max_job_input_bytes {
                return Err(NativeError::Limit("operation input bytes"));
            }
            let local = self
                .execution
                .jobs
                .clone()
                .try_acquire_owned()
                .map_err(|_| NativeError::Limit("component jobs"))?;
            let permit = self
                .execution
                .service
                .permits(1)
                .map_err(|_| NativeError::Limit("runtime tasks"))?;
            let lease = self.execution.world().track_task(owner)?;
            Ok((input, local, permit, lease))
        })();
        let (input, local, permit, lease) = match prepared {
            Ok(value) => value,
            Err(_) if retired() => {
                self.deferred = true;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let job = JobContext {
            world: self.execution.world().clone(),
            owner,
            operation,
            deadline: input.options().deadline.expect("job deadline"),
            output: Arc::new(Mutex::new(None)),
        };
        let identity = self.execution.spec.descriptor.name.clone();
        let envelope = self.envelope().cloned();
        let action = self.execution.spec.failure_action;
        self.execution
            .service
            .spawn(permit, lease, move |lease| async move {
                let _lease = lease;
                let _local = local;
                run_job(job, input, factory, identity, envelope, action).await;
            });
        self.deferred = true;
        let mut stats = self.execution.stats.lock().unwrap();
        stats.jobs_submitted = stats.jobs_submitted.saturating_add(1);
        Ok(())
    }

    /// Complete a request on the runtime's separately bounded blocking pool.
    /// The closure owns native input and must check `JobContext::is_cancelled`
    /// during long computations. Running closures cannot be forcibly stopped.
    pub fn defer_cpu_reply<F>(&mut self, input: Payload, factory: F) -> NativeResult
    where
        F: FnOnce(HeldPayload, JobContext) -> NativeResult<Payload> + Send + 'static,
    {
        self.effect()?;
        if self.deferred {
            return Err(NativeError::Application("ReplyAlreadyDeferred"));
        }
        let operation = self
            .operation()
            .ok_or(NativeError::Application("NotARequest"))?;
        let retired = || {
            matches!(
                self.execution.world().operation_status(operation),
                Ok(OperationStatus::Terminal(_)) | Err(actorplane_core::Error::StaleReference)
            )
        };
        if retired() {
            self.deferred = true;
            return Ok(());
        }
        let owner = self.owner();
        let prepared = (|| {
            self.new_work()?;
            // Reserve CPU admission first: TestWorld and a saturated CPU queue
            // reject before retaining another input or reserving a task.
            let cpu = self.execution.service.reserve_cpu()?;
            let options = self.options(MessageOptions::default())?;
            let input = self.execution.world().hold_with(input, options)?;
            if input.retained_bytes() > self.execution.spec.limits.max_job_input_bytes {
                return Err(NativeError::Limit("operation input bytes"));
            }
            let local = self
                .execution
                .jobs
                .clone()
                .try_acquire_owned()
                .map_err(|_| NativeError::Limit("component jobs"))?;
            let permit = self
                .execution
                .service
                .permits(1)
                .map_err(|_| NativeError::Limit("runtime tasks"))?;
            let lease = self.execution.world().track_task(owner)?;
            Ok((cpu, input, local, permit, lease))
        })();
        let (cpu, input, local, permit, lease) = match prepared {
            Ok(value) => value,
            Err(_) if retired() => {
                self.deferred = true;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let job = JobContext {
            world: self.execution.world().clone(),
            owner,
            operation,
            deadline: input.options().deadline.expect("CPU job deadline"),
            output: Arc::new(Mutex::new(None)),
        };
        let cancellation = job.clone();
        let identity = self.execution.spec.descriptor.name.clone();
        let envelope = self.envelope().cloned();
        let action = self.execution.spec.failure_action;
        self.execution.service.spawn_cpu(
            permit,
            lease,
            cpu,
            move || cancellation.is_cancelled(),
            move || {
                let _local = local;
                run_cpu_job(job, input, factory, identity, envelope, action)
            },
        )?;
        self.deferred = true;
        let mut stats = self.execution.stats.lock().unwrap();
        stats.jobs_submitted = stats.jobs_submitted.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone)]
pub struct JobContext {
    world: World,
    owner: ActorRef,
    operation: OperationId,
    deadline: Instant,
    output: Arc<Mutex<Option<actorplane_core::NativeBufferPermit>>>,
}
impl JobContext {
    pub fn owner(&self) -> ActorRef {
        self.owner
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Reserve World bytes before allocating native scratch.
    /// Keep the permit alive for the allocation's lifetime. This is accounting,
    /// not an allocator: application captures and allocations require discipline.
    pub fn reserve_buffer(
        &self,
        bytes: usize,
    ) -> NativeResult<actorplane_core::NativeBufferPermit> {
        Ok(self.world.native_buffer_budget().reserve(bytes)?)
    }
    /// Reserve before allocating an output. The job retains one grow-only
    /// reservation through result admission, even after the factory returns.
    /// Admission also charges the retained result, conservatively overlapping
    /// the two reservations until processing finishes.
    pub fn reserve_output(&self, bytes: usize) -> NativeResult {
        if bytes > self.world.config().max_event_bytes {
            return Err(NativeError::Limit("operation output bytes"));
        }
        let mut output = self.output.lock().unwrap();
        match output.as_mut() {
            Some(permit) => permit.try_grow(bytes.saturating_sub(permit.bytes()))?,
            None => *output = Some(self.world.native_buffer_budget().reserve(bytes)?),
        }
        Ok(())
    }
    pub fn is_cancelled(&self) -> bool {
        self.world.now() >= self.deadline
            || !matches!(
                self.world.execution_state(self.owner),
                Ok(Lifecycle::Active | Lifecycle::Quiescing)
            )
            || !matches!(
                self.world.operation_status(self.operation),
                Ok(OperationStatus::Pending(_))
            )
    }
}

fn run_cpu_job<F>(
    job: JobContext,
    input: HeldPayload,
    factory: F,
    identity: String,
    envelope: Option<Envelope>,
    action: FailureAction,
) -> crate::cpu::CpuOutcome
where
    F: FnOnce(HeldPayload, JobContext) -> NativeResult<Payload>,
{
    if job.is_cancelled() {
        return crate::cpu::CpuOutcome::Cancelled;
    }
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(input, job.clone())))
            .unwrap_or(Err(NativeError::Panic));
    // Cancellation/deadline retirement is ordinary loss of this result. A panic
    // remains a component failure even when its requester has disappeared.
    if job.is_cancelled() && !matches!(&result, Err(NativeError::Panic)) {
        return crate::cpu::CpuOutcome::Cancelled;
    }
    let panicked = matches!(&result, Err(NativeError::Panic));
    let completed = result.and_then(|payload| {
        job.world
            .complete_operation(job.operation, job.owner, payload)
            .map(|_| ())
            .map_err(NativeError::Core)
    });
    if let Err(error) = completed {
        let _ = job.world.report_failure(
            job.owner,
            envelope.as_ref().map(|value| value.event_id),
            envelope.as_ref().map(|value| value.schema.id),
            Some(job.operation),
            FailureDetails::new(FailurePhase::Handler, &identity, error.code(), vec![]),
            if matches!(error, NativeError::Panic) {
                FailureAction::StopActor
            } else {
                action
            },
        );
    }
    if panicked {
        crate::cpu::CpuOutcome::Panicked
    } else {
        crate::cpu::CpuOutcome::Completed
    }
}

async fn run_job<F, Fut>(
    job: JobContext,
    input: HeldPayload,
    factory: F,
    identity: String,
    envelope: Option<Envelope>,
    action: FailureAction,
) where
    F: FnOnce(HeldPayload, JobContext) -> Fut + Send + 'static,
    Fut: Future<Output = NativeResult<Payload>> + Send + 'static,
{
    if job.is_cancelled() {
        return;
    }
    let future =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(input, job.clone())));
    let result = match future {
        Err(_) => Err(NativeError::Panic),
        Ok(future) => {
            let future = CatchPanic {
                future: Box::pin(future),
            };
            tokio::pin!(future);
            let mut check = crate::periodic(Duration::from_millis(1));
            loop {
                tokio::select! {
                    result = &mut future => break result.unwrap_or(Err(NativeError::Panic)),
                    _ = check.tick() => if job.is_cancelled() { return; },
                }
            }
        }
    };
    let completed = result.and_then(|payload| {
        job.world
            .complete_operation(job.operation, job.owner, payload)
            .map(|_| ())
            .map_err(NativeError::Core)
    });
    if let Err(error) = completed {
        let _ = job.world.report_failure(
            job.owner,
            envelope.as_ref().map(|value| value.event_id),
            envelope.as_ref().map(|value| value.schema.id),
            Some(job.operation),
            FailureDetails::new(FailurePhase::Handler, &identity, error.code(), vec![]),
            if matches!(error, NativeError::Panic) {
                FailureAction::StopActor
            } else {
                action
            },
        );
    }
}

fn invoke(callback: impl FnOnce() -> NativeResult) -> (NativeResult, bool) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        Ok(result) => (result, false),
        Err(_) => (Err(NativeError::Panic), true),
    }
}

impl NativeRuntime {
    /// Validate, create fresh state, and run bounded synchronous startup on the
    /// preparing thread. The returned scope stays Starting until activation.
    pub fn prepare_native<C, F>(
        &self,
        parent: Option<ActorRef>,
        mut spec: NativeSpec,
        configuration: Value,
        factory: F,
    ) -> NativeResult<NativeHandle>
    where
        C: NativeBehavior,
        F: FnOnce(Value) -> NativeResult<C>,
    {
        let configuration = spec.validate_config(&configuration)?;
        spec.configuration = spec.configuration.normalized_clone();
        spec.descriptor = spec.descriptor.clone();
        let service = self
            .task_service()
            .map_err(|_| NativeError::Limit("runtime closed"))?;
        let permit = service
            .permits(1)
            .map_err(|_| NativeError::Limit("runtime tasks"))?;
        let owner = self.world().allocate(EndpointKind::Native, parent)?;
        let ports = match self
            .world()
            .register_component(owner, spec.descriptor.clone())
        {
            Ok(ports) => ports,
            Err(error) => {
                let _ = self.world().stop(owner);
                return Err(error.into());
            }
        };
        let lease = match self.world().track_task(owner) {
            Ok(lease) => lease,
            Err(error) => {
                let _ = self.world().stop(owner);
                return Err(error.into());
            }
        };
        let startup_claim = match self.world().claim_native_callback(owner, true) {
            Ok(Some(claim)) => claim,
            result => {
                let _ = self.world().stop(owner);
                return Err(result
                    .err()
                    .unwrap_or(actorplane_core::Error::ActorStopped)
                    .into());
            }
        };
        let stats = Arc::new(Mutex::new(NativeStats::default()));
        let spec = Arc::new(spec);
        let execution = Execution {
            service,
            owner,
            lease,
            stats: stats.clone(),
            jobs: Arc::new(Semaphore::new(spec.limits.max_jobs)),
            spec: spec.clone(),
        };
        let constructed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(configuration)));
        let mut component = match constructed {
            Ok(Ok(component)) => component,
            other => {
                let error = match other {
                    Ok(Err(error)) => error,
                    _ => NativeError::Panic,
                };
                execution.failure(FailurePhase::Construction, None, &error, true);
                return Err(error);
            }
        };
        let (result, panic) =
            invoke(|| component.on_start(&mut execution.context(Phase::Startup, None)));
        if let Err(error) = result {
            execution.failure(FailurePhase::Start, None, &error, panic);
            if !panic {
                cleanup(&mut component, &execution);
            }
            dispose(component, &execution);
            return Err(error);
        }
        if !matches!(self.world().state(owner), Ok(Lifecycle::Starting)) {
            cleanup(&mut component, &execution);
            dispose(component, &execution);
            return Err(NativeError::Core(actorplane_core::Error::ActorStopped));
        }
        drop(startup_claim);
        let guard = execution.lease.clone();
        self.task_service()
            .expect("runtime borrowed during preparation")
            .spawn(permit, guard, move |_| async move {
                run_component(component, execution).await;
            });
        Ok(NativeHandle {
            owner,
            ports,
            spec,
            stats,
        })
    }
}

fn cleanup<C: NativeBehavior>(component: &mut C, execution: &Execution) {
    let (result, panic) = invoke(|| component.on_stop(&mut execution.context(Phase::Stop, None)));
    if let Err(error) = result {
        execution.failure(FailurePhase::Stop, None, &error, panic);
    }
}

fn dispose<C: NativeBehavior>(component: C, execution: &Execution) {
    let (result, panic) = invoke(|| {
        drop(component);
        Ok(())
    });
    if let Err(error) = result {
        execution.failure(FailurePhase::Stop, None, &error, panic);
    }
}

async fn run_component<C: NativeBehavior>(mut component: C, execution: Execution) {
    let world = execution.world();
    let owner = execution.owner;
    let mut quiescing = false;
    let mut next_tick = None;
    let mut panicked = false;
    loop {
        let observed = match world.activity(owner) {
            Ok(observed) => observed,
            Err(error) => {
                execution.failure(FailurePhase::Stop, None, &NativeError::Core(error), false);
                break;
            }
        };
        let state = world.execution_state(owner);
        if matches!(state, Ok(Lifecycle::Stopping | Lifecycle::Stopped) | Err(_)) {
            break;
        }
        if state == Ok(Lifecycle::Starting) {
            let _ = std::future::poll_fn(|cx| world.poll_activity(owner, observed, cx)).await;
            continue;
        }
        {
            let mut stats = execution.stats.lock().unwrap();
            stats.turns = stats.turns.saturating_add(1);
        }
        if let Ok(Some(failure)) = world.claim_failure(owner) {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                component.on_failure(
                    failure.failure(),
                    failure.coalesced(),
                    &mut execution.context(Phase::Supervisor, None),
                )
            }));
            let action = match result {
                Ok(Ok(action)) => action,
                other => {
                    let error = match other {
                        Ok(Err(error)) => error,
                        _ => {
                            panicked = true;
                            NativeError::Panic
                        }
                    };
                    execution.failure(FailurePhase::Supervisor, None, &error, panicked);
                    FailureAction::StopWorld
                }
            };
            let _ = failure.finish(Some(action));
            if panicked
                || !matches!(
                    world.execution_state(owner),
                    Ok(Lifecycle::Active | Lifecycle::Quiescing)
                )
            {
                break;
            }
        }
        if state == Ok(Lifecycle::Quiescing) && !quiescing {
            let Ok(Some(_claim)) = world.claim_native_drain_callback(owner) else {
                let _ = std::future::poll_fn(|cx| world.poll_activity(owner, observed, cx)).await;
                continue;
            };
            quiescing = true;
            let (result, panic) =
                invoke(|| component.on_quiesce(&mut execution.context(Phase::Quiesce, None)));
            if let Err(error) = result {
                execution.failure(FailurePhase::Stop, None, &error, panic);
                panicked = panic;
                break;
            }
            if execution.spec.drain == DrainPolicy::Producer {
                break;
            }
        }
        let mut consumed = 0;
        while consumed < execution.spec.limits.events_per_turn {
            let Ok(Some(delivery)) = world.claim(owner) else {
                break;
            };
            let (result, panic) = invoke(|| {
                component.on_event(
                    delivery.payload(),
                    &mut execution.context(Phase::Event, Some(&delivery)),
                )
            });
            if let Err(error) = &result {
                execution.failure(FailurePhase::Handler, Some(&delivery), error, panic);
            }
            delivery.finish(result.is_ok());
            consumed += 1;
            let mut stats = execution.stats.lock().unwrap();
            stats.handled = stats.handled.saturating_add(1);
            if panic {
                panicked = true;
                break;
            }
        }
        if panicked {
            break;
        }
        if !matches!(
            world.execution_state(owner),
            Ok(Lifecycle::Active | Lifecycle::Quiescing)
        ) {
            break;
        }
        if quiescing
            && world.upstream_done(owner).unwrap_or(false)
            && execution.jobs.available_permits() == execution.spec.limits.max_jobs
        {
            let Ok(Some(_claim)) = world.claim_native_drain_callback(owner) else {
                let _ = std::future::poll_fn(|cx| world.poll_activity(owner, observed, cx)).await;
                continue;
            };
            let (result, panic) =
                invoke(|| component.on_drain(&mut execution.context(Phase::Drain, None)));
            if let Err(error) = result {
                execution.failure(FailurePhase::Stop, None, &error, panic);
            }
            panicked = panic;
            break;
        }
        let mut tick_blocked = false;
        if !quiescing && let Some(period) = execution.spec.period {
            let deadline = next_tick.get_or_insert_with(|| tokio::time::Instant::now() + period);
            tick_blocked = tokio::time::Instant::now() >= *deadline;
            if tick_blocked && let Ok(Some(_claim)) = world.claim_native_callback(owner, false) {
                tick_blocked = false;
                let (result, panic) =
                    invoke(|| component.on_tick(&mut execution.context(Phase::Tick, None)));
                if let Err(error) = result {
                    execution.failure(FailurePhase::Handler, None, &error, panic);
                }
                panicked = panic;
                let mut stats = execution.stats.lock().unwrap();
                stats.ticks = stats.ticks.saturating_add(1);
                // Preserve the original cadence, skipping missed occurrences.
                let now = tokio::time::Instant::now();
                let remainder =
                    now.saturating_duration_since(*deadline).as_nanos() % period.as_nanos();
                *deadline = now + period - Duration::from_nanos(remainder as u64);
            }
        }
        if panicked {
            break;
        }
        if consumed == execution.spec.limits.events_per_turn {
            tokio::task::yield_now().await;
            continue;
        }
        let changed = std::future::poll_fn(|cx| world.poll_activity(owner, observed, cx));
        // A due tick blocked by another claim waits for that claim's release;
        // polling an already elapsed timer would spin until the owner is free.
        if let Some(deadline) = next_tick.filter(|_| !quiescing && !tick_blocked) {
            tokio::select! { _ = changed => {}, _ = tokio::time::sleep_until(deadline) => {} }
        } else {
            let _ = changed.await;
        }
    }
    if !panicked {
        cleanup(&mut component, &execution);
    }
    // Component destructors run while its tracked task is still retained.
    dispose(component, &execution);
}
