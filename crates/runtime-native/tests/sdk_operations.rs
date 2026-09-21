use actorplane_core::{
    ComponentDescriptor, Config, InterfaceSpec, MessageOptions, Payload, PayloadType,
    PortDirection, PortSpec, TraceContext, World,
    schema::{Field, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(Instant::now() < end, "condition did not become true");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn spec(name: &str) -> NativeSpec {
    let ports = vec![PortSpec {
        name: "input".into(),
        direction: PortDirection::Input,
        schema: PayloadType::Pulse,
    }];
    NativeSpec::new(
        ComponentDescriptor {
            name: name.into(),
            version: 1,
            interfaces: vec![InterfaceSpec {
                name: name.into(),
                version: 1,
                ports: ports.clone(),
            }],
            ports,
        },
        Schema {
            name: format!("{name}.Config"),
            version: 1,
            fields: Vec::<Field>::new(),
        },
    )
}

#[test]
fn defer_reply_runs_owned_job_and_completes_request() {
    struct Deferred {
        jobs: Arc<AtomicUsize>,
    }
    impl NativeBehavior for Deferred {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            let jobs = self.jobs.clone();
            let input = payload.clone();
            ctx.defer_reply(input, move |held, _job| async move {
                jobs.fetch_add(1, Ordering::SeqCst);
                Ok(held.payload().clone())
            })
        }
    }

    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let jobs = Arc::new(AtomicUsize::new(0));
    let jobs_for_factory = jobs.clone();
    let handle = runtime
        .prepare_native::<Deferred, _>(None, spec("Deferred"), Value::Record(vec![]), move |_| {
            Ok(Deferred {
                jobs: jobs_for_factory,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let requester = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    world.activate(requester).unwrap();
    let trace =
        TraceContext::new([3; 16], [4; 8], true, vec![("test".into(), "sdk".into())]).unwrap();
    let operation = world
        .request_with(
            requester,
            handle.owner,
            Payload::Pulse(9),
            Instant::now() + Duration::from_secs(1),
            MessageOptions {
                correlation_id: Some(77),
                trace: Some(trace),
                ..MessageOptions::default()
            },
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    assert_eq!(jobs.load(Ordering::SeqCst), 1);
    let outcome = world.take_operation(operation).unwrap().unwrap();
    match outcome {
        actorplane_core::TerminalOutcome::Completed(result) => {
            let envelope = result.envelope().expect("deferred result envelope");
            assert_eq!(envelope.correlation_id, Some(77));
            assert!(envelope.causation_id.is_some());
            assert_eq!(*envelope.trace.as_ref().unwrap().trace_id(), [3; 16]);
        }
        other => panic!("unexpected operation outcome: {other:?}"),
    }
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn max_jobs_admits_one_pending_future_and_fallback_replies_second() {
    struct Limited {
        factories: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
    }
    impl NativeBehavior for Limited {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            let value = match payload {
                Payload::Pulse(value) => *value,
                _ => 0,
            };
            let factories = self.factories.clone();
            let started = self.started.clone();
            match ctx.defer_reply(payload.clone(), move |held, _job| async move {
                factories.fetch_add(1, Ordering::SeqCst);
                started.fetch_add(1, Ordering::SeqCst);
                if value == 1 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                Ok(held.payload().clone())
            }) {
                Ok(()) => Ok(()),
                Err(NativeError::Limit(_)) if value == 2 => {
                    ctx.reply(Payload::Pulse(22))?;
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
    }

    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let factories = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let f = factories.clone();
    let s = started.clone();
    let mut native = spec("Limited");
    native.limits.max_jobs = 1;
    let handle = runtime
        .prepare_native::<Limited, _>(None, native, Value::Record(vec![]), move |_| {
            Ok(Limited {
                factories: f,
                started: s,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let requester = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    world.activate(requester).unwrap();
    let first = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    let second = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(2),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(second),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    assert!(matches!(
        world.take_operation(second).unwrap(),
        Some(actorplane_core::TerminalOutcome::Completed(_))
    ));
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    world.stop(handle.owner).unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(first),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    assert!(world.take_operation(first).unwrap().is_some());
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn stopping_owner_cancels_pending_job_without_late_completion() {
    let (world, mut runtime, handle, requester, dropped, started, baseline) = guarded_setup();
    let operation = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Pending(_))
        )
    });
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    world.stop(handle.owner).unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    let outcome = world
        .take_operation(operation)
        .unwrap()
        .expect("cancelled operation");
    assert!(matches!(
        outcome,
        actorplane_core::TerminalOutcome::OwnerStopped
            | actorplane_core::TerminalOutcome::TargetStopped
            | actorplane_core::TerminalOutcome::Cancelled
    ));
    wait_until(|| dropped.load(Ordering::SeqCst) == 1);
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    assert!(matches!(
        world.take_operation(operation),
        Err(actorplane_core::Error::StaleReference)
    ));
    assert!(matches!(
        world.complete_operation(operation, handle.owner, Payload::Pulse(100)),
        Err(actorplane_core::Error::StaleReference) | Ok(false)
    ));
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn close_reports_noncooperative_job_incomplete_at_short_deadline() {
    struct Blocking {
        started: Arc<AtomicUsize>,
    }
    impl NativeBehavior for Blocking {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            let started = self.started.clone();
            ctx.defer_reply(payload.clone(), move |_held, _job| async move {
                started.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
                Ok(Payload::Pulse(3))
            })
        }
    }

    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let started_for_factory = started.clone();
    let handle = runtime
        .prepare_native::<Blocking, _>(None, spec("Blocking"), Value::Record(vec![]), move |_| {
            Ok(Blocking {
                started: started_for_factory,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let requester = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    world.activate(requester).unwrap();
    let operation = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    let report = runtime.close(Duration::from_millis(5)).unwrap();
    assert!(
        report.timed_out,
        "shutdown unexpectedly met the short deadline: {report:?}"
    );
    assert!(
        !report.native_done,
        "shutdown falsely reported native completion: {report:?}"
    );
    assert!(
        report.native_tasks > 0,
        "missing native task accounting: {report:?}"
    );
    wait_until(|| runtime.active_tasks() == 0);
    assert!(matches!(
        world.operation_status(operation),
        Ok(actorplane_core::OperationStatus::Terminal(_))
            | Err(actorplane_core::Error::StaleReference)
    ));
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn cancelled_job_cannot_complete_reused_actor_or_operation_generation() {
    let (world, mut runtime, old, requester, dropped, started, _) = guarded_setup();
    let first = world
        .request(
            requester,
            old.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    world.stop(old.owner).unwrap();
    wait_until(|| world.state(old.owner) == Ok(actorplane_core::Lifecycle::Stopped));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        world.take_operation(first).unwrap(),
        Some(actorplane_core::TerminalOutcome::TargetStopped)
    ));
    let replacement = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    assert_eq!(replacement.slot, old.owner.slot);
    assert_ne!(replacement.generation, old.owner.generation);
    world.activate(replacement).unwrap();
    let next = world
        .request(
            requester,
            replacement,
            Payload::Pulse(2),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    assert_ne!(next, first);
    assert!(matches!(
        world.complete_operation(first, old.owner, Payload::Pulse(99)),
        Err(actorplane_core::Error::StaleReference) | Ok(false)
    ));
    assert!(matches!(
        world.operation_status(next),
        Ok(actorplane_core::OperationStatus::Pending(_))
    ));
    let delivery = world.claim(replacement).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(2));
    assert!(
        world
            .complete_operation(next, replacement, Payload::Pulse(22))
            .unwrap()
    );
    delivery.finish(true);
    match world.take_operation(next).unwrap().unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(22))
        }
        outcome => panic!("unexpected replacement outcome: {outcome:?}"),
    }
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct GuardedJob {
    dropped: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
}
impl NativeBehavior for GuardedJob {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let dropped = self.dropped.clone();
        let started = self.started.clone();
        ctx.defer_reply(payload.clone(), move |held, _job| async move {
            started.fetch_add(1, Ordering::SeqCst);
            let _guard = DropGuard(dropped);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(held.payload().clone())
        })
    }
}

fn guarded_setup() -> (
    World,
    NativeRuntime,
    actorplane_native::sdk::NativeHandle,
    actorplane_core::ActorRef,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    usize,
) {
    let world = World::new(Config::default()).unwrap();
    let runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let for_factory = dropped.clone();
    let for_started = started.clone();
    let handle = runtime
        .prepare_native::<GuardedJob, _>(None, spec("Guarded"), Value::Record(vec![]), move |_| {
            Ok(GuardedJob {
                dropped: for_factory,
                started: for_started,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let requester = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    world.activate(requester).unwrap();
    let baseline = world.snapshot().retained_payload_bytes;
    (
        world, runtime, handle, requester, dropped, started, baseline,
    )
}

#[test]
fn explicit_cancel_drops_job_input_and_releases_retained_bytes() {
    let (world, mut runtime, handle, requester, dropped, started, baseline) = guarded_setup();
    let operation = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(8),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Pending(_))
        )
    });
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    assert!(world.cancel_operation(operation).unwrap());
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    assert!(matches!(
        world.take_operation(operation).unwrap(),
        Some(actorplane_core::TerminalOutcome::Cancelled)
    ));
    wait_until(|| dropped.load(Ordering::SeqCst) == 1);
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    assert!(matches!(
        world.complete_operation(operation, handle.owner, Payload::Pulse(100)),
        Err(actorplane_core::Error::StaleReference) | Ok(false)
    ));
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn deadline_timeout_drops_job_input_and_preserves_timeout_terminal() {
    let (world, mut runtime, handle, requester, dropped, started, baseline) = guarded_setup();
    let deadline = Instant::now() + Duration::from_secs(5);
    let operation = world
        .request(requester, handle.owner, Payload::Pulse(8), deadline)
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Pending(_))
        )
    });
    wait_until(|| started.load(Ordering::SeqCst) == 1);
    // Drive native expiry at the declared deadline after the future has begun.
    world.maintain(deadline);
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    assert!(matches!(
        world.take_operation(operation).unwrap(),
        Some(actorplane_core::TerminalOutcome::TimedOut)
    ));
    wait_until(|| dropped.load(Ordering::SeqCst) == 1);
    assert_eq!(world.snapshot().retained_payload_bytes, baseline);
    assert!(matches!(
        world.complete_operation(operation, handle.owner, Payload::Pulse(100)),
        Err(actorplane_core::Error::StaleReference) | Ok(false)
    ));
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn timers_respect_both_global_task_and_local_job_budgets() {
    struct Timers {
        second: Arc<AtomicUsize>,
    }
    impl NativeBehavior for Timers {
        fn on_event(&mut self, _: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            let options = MessageOptions::default();
            ctx.after(
                Duration::from_secs(30),
                ctx.owner(),
                Payload::Pulse(1),
                options.clone(),
            )?;
            match ctx.after(
                Duration::from_secs(30),
                ctx.owner(),
                Payload::Pulse(2),
                options,
            ) {
                Err(NativeError::Limit(_)) => {
                    self.second.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                other => panic!("second timer unexpectedly admitted: {other:?}"),
            }
        }
    }

    // First exhaust global tasks with spare local permits; then exhaust local
    // jobs while global capacity remains. The executor uses one global permit.
    for (max_tasks, max_jobs) in [(2, 8), (8, 1)] {
        let world = World::new(Config::default()).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), max_tasks).unwrap();
        let second = Arc::new(AtomicUsize::new(0));
        let second_for_factory = second.clone();
        let mut native = spec("Timers");
        native.limits.max_jobs = max_jobs;
        let handle = runtime
            .prepare_native::<Timers, _>(None, native, Value::Record(vec![]), move |_| {
                Ok(Timers {
                    second: second_for_factory,
                })
            })
            .unwrap();
        world.activate(handle.owner).unwrap();
        world.send(handle.owner, Payload::Pulse(1)).unwrap();
        wait_until(|| second.load(Ordering::SeqCst) == 1);
        runtime.close(Duration::from_secs(1)).unwrap();
    }
}

#[test]
fn oversized_deferred_input_is_rejected_before_factory_and_can_reply_fallback() {
    struct BoundedInput;
    impl NativeBehavior for BoundedInput {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            let result = ctx.defer_reply(payload.clone(), |_, _| async {
                panic!("oversized input reached its factory")
            });
            assert!(matches!(
                result,
                Err(NativeError::Limit("operation input bytes"))
            ));
            assert!(ctx.reply(Payload::Pulse(0))?);
            Ok(())
        }
    }
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let mut native = spec("BoundedInput");
    native.limits.max_job_input_bytes = 7; // Pulse stores eight bytes.
    let handle = runtime
        .prepare_native(None, native, Value::Record(vec![]), |_| Ok(BoundedInput))
        .unwrap();
    world.activate(handle.owner).unwrap();
    let id = world
        .request(
            handle.owner,
            handle.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(id),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    match world.take_operation(id).unwrap().unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(0))
        }
        outcome => panic!("fallback reply failed: {outcome:?}"),
    }
    assert_eq!(handle.stats().jobs_submitted, 0);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
