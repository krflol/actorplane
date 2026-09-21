use actorplane_core::{
    ActorRef, ComponentDescriptor, Config, EndpointKind, FailureAction, HeldPayload, Lifecycle,
    MessageOptions, OperationId, OperationStatus, Payload, PayloadType, PortDirection, PortSpec,
    TerminalOutcome, TraceContext, World,
    schema::{Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    cpu::CpuConfig,
    sdk::{JobContext, NativeBehavior, NativeContext, NativeHandle, NativeResult, NativeSpec},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

type Work = Arc<dyn Fn(HeldPayload, JobContext) -> NativeResult<Payload> + Send + Sync>;
struct Compute {
    work: Work,
    denied: Arc<AtomicUsize>,
}
impl NativeBehavior for Compute {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let work = self.work.clone();
        if let Err(error) = ctx.defer_cpu_reply(payload.clone(), move |input, job| work(input, job))
        {
            if matches!(error, actorplane_native::sdk::NativeError::Limit(_)) {
                self.denied.fetch_add(1, Ordering::SeqCst);
                ctx.reply(Payload::Pulse(-1))?;
                Ok(())
            } else {
                Err(error)
            }
        } else {
            Ok(())
        }
    }
}
#[track_caller]
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "CPU condition did not become true"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn actor(world: &World) -> ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}
fn prepare(
    runtime: &NativeRuntime,
    work: Work,
    local_jobs: usize,
    denied: Arc<AtomicUsize>,
) -> NativeHandle {
    let mut spec = NativeSpec::new(
        ComponentDescriptor {
            name: "CpuTest".into(),
            version: 1,
            interfaces: vec![],
            ports: vec![PortSpec {
                name: "requests".into(),
                direction: PortDirection::Input,
                schema: PayloadType::Pulse,
            }],
        },
        Schema {
            name: "CpuTest.Config".into(),
            version: 1,
            fields: vec![],
        },
    );
    spec.limits.max_jobs = local_jobs;
    spec.failure_action = FailureAction::Continue;
    let handle = runtime
        .prepare_native(None, spec, Value::Record(vec![]), move |_| {
            Ok(Compute { work, denied })
        })
        .unwrap();
    runtime.world().activate(handle.owner).unwrap();
    handle
}
fn request(world: &World, owner: ActorRef, target: ActorRef, value: i64) -> OperationId {
    world
        .request(
            owner,
            target,
            Payload::Pulse(value),
            world.now() + Duration::from_secs(3),
        )
        .unwrap()
}
fn take(world: &World, operation: OperationId) -> TerminalOutcome {
    wait(|| {
        matches!(
            world.operation_status(operation),
            Ok(OperationStatus::Terminal(_))
        )
    });
    world.take_operation(operation).unwrap().unwrap()
}
fn pulse(world: &World, operation: OperationId, expected: i64) {
    match take(world, operation) {
        TerminalOutcome::Completed(reply) => assert_eq!(reply.payload(), &Payload::Pulse(expected)),
        other => panic!("expected CPU reply: {other:?}"),
    }
}
fn runtime(world: &World, workers: usize, max_jobs: usize, max_tasks: usize) -> NativeRuntime {
    NativeRuntime::new_with_cpu_config(world.clone(), max_tasks, CpuConfig { workers, max_jobs })
        .unwrap()
}
fn clean(runtime: &mut NativeRuntime) {
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(
        report.native_done && report.python_done && !report.timed_out,
        "{report:?}"
    );
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(runtime.cpu_stats().queued, 0);
    assert_eq!(runtime.cpu_stats().running, 0);
    assert_eq!(runtime.world().snapshot().retained_payload_bytes, 0);
}

#[test]
fn cpu_admission_is_bounded_and_queued_cancel_does_not_wait_for_busy_worker() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 1, 2, 12);
    let owner = actor(&world);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    let denied = Arc::new(AtomicUsize::new(0));
    let first = prepare(
        &runtime,
        Arc::new(move |input, _| {
            started_tx.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(4))
                .unwrap();
            Ok(input.payload().clone())
        }),
        4,
        denied.clone(),
    );
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let second = prepare(
        &runtime,
        Arc::new(move |input, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(input.payload().clone())
        }),
        4,
        denied.clone(),
    );
    let running = request(&world, owner, first.owner, 1);
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let queued = request(&world, owner, second.owner, 2);
    wait(|| runtime.cpu_stats().queued == 1);
    assert_eq!(runtime.cpu_stats().running, 1);
    let rejected = request(&world, owner, second.owner, 3);
    pulse(&world, rejected, -1);
    assert_eq!(denied.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.cpu_stats().rejected, 1);

    // A blocked CPU closure cannot occupy the asynchronous timer workers.
    let timer = actor(&world);
    runtime
        .after(owner, timer, Duration::from_millis(5), Payload::Pulse(99))
        .unwrap();
    wait(|| {
        world
            .snapshot()
            .actors
            .iter()
            .any(|a| a.reference == timer && a.queue_entries == 1)
    });
    let delivery = world.claim(timer).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(99));
    delivery.finish(true);
    assert!(world.cancel_operation(queued).unwrap());
    assert!(matches!(take(&world, queued), TerminalOutcome::Cancelled));
    wait(|| runtime.cpu_stats().queued == 0);
    assert_eq!(factories.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.cpu_stats().running, 1);
    let replacement = request(&world, owner, second.owner, 4);
    wait(|| runtime.cpu_stats().queued == 1);
    release_tx.send(()).unwrap();
    pulse(&world, running, 1);
    pulse(&world, replacement, 4);
    wait(|| runtime.cpu_stats().running == 0);
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.cpu_stats().submitted, 3);
    assert_eq!(runtime.cpu_stats().cancelled, 1);
    clean(&mut runtime);
}

#[test]
fn noncooperative_cpu_close_retains_task_input_and_reports_incomplete_until_return() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 1, 2, 8);
    let owner = actor(&world);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    let worker = prepare(
        &runtime,
        Arc::new(move |input, _| {
            started_tx.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(4))
                .unwrap();
            Ok(input.payload().clone())
        }),
        2,
        Arc::default(),
    );
    request(&world, owner, worker.owner, 7);
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let report = runtime.close(Duration::from_millis(5)).unwrap();
    assert!(report.timed_out && !report.native_done);
    assert!(report.native_tasks >= 1 && runtime.active_tasks() >= 1);
    assert_eq!(runtime.cpu_stats().running, 1);
    assert!(world.snapshot().retained_payload_bytes >= 8);
    assert_eq!(world.state(worker.owner), Ok(Lifecycle::Stopping));
    release_tx.send(()).unwrap();
    assert!(runtime.wait_native_idle(Duration::from_secs(2)));
    let report = runtime.close(Duration::ZERO).unwrap();
    assert!(report.native_done && report.timed_out);
    assert_eq!(report.native_tasks, 0);
    assert_eq!(runtime.cpu_stats().running, 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn cpu_reply_preserves_request_metadata_and_original_deadline() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 2, 4, 8);
    let owner = actor(&world);
    let trace =
        TraceContext::new([1; 16], [2; 8], true, vec![("cpu".into(), "test".into())]).unwrap();
    let expected = trace.clone();
    let deadline = world.now() + Duration::from_secs(2);
    let worker = prepare(
        &runtime,
        Arc::new(move |input, job| {
            assert_eq!(job.deadline(), deadline);
            assert_eq!(input.options().correlation_id, Some(23));
            assert_eq!(input.options().trace, Some(expected.clone()));
            let reservation = job.reserve_buffer(32)?;
            assert_eq!(reservation.bytes(), 32);
            Ok(input.payload().clone())
        }),
        2,
        Arc::default(),
    );
    let operation = world
        .request_with(
            owner,
            worker.owner,
            Payload::Pulse(42),
            deadline,
            MessageOptions {
                correlation_id: Some(23),
                trace: Some(trace.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    match take(&world, operation) {
        TerminalOutcome::Completed(reply) => {
            assert_eq!(reply.payload(), &Payload::Pulse(42));
            assert_eq!(reply.options().correlation_id, Some(23));
            assert_eq!(reply.options().trace, Some(trace));
        }
        other => panic!("unexpected CPU result: {other:?}"),
    }
    clean(&mut runtime);
}

#[test]
fn cpu_panic_is_contained_and_counted_without_leaking_reservations() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 1, 1, 8);
    let owner = actor(&world);
    let worker = prepare(
        &runtime,
        Arc::new(|_, _| panic!("private CPU panic message")),
        1,
        Arc::default(),
    );
    let operation = request(&world, owner, worker.owner, 1);
    assert!(!matches!(
        take(&world, operation),
        TerminalOutcome::Completed(_)
    ));
    wait(|| runtime.cpu_stats().panicked == 1);
    assert_eq!(world.snapshot().metrics.failures, 1);
    let history = world.diagnostics(0, 32).unwrap();
    assert!(!format!("{history:?}").contains("private CPU panic message"));
    wait(|| world.state(worker.owner) == Ok(Lifecycle::Stopped));
    clean(&mut runtime);
}

#[test]
fn queued_cpu_deadline_expires_without_running_factory() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 1, 2, 8);
    let owner = actor(&world);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let worker = prepare(
        &runtime,
        Arc::new(move |input, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            started_tx.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(4))
                .unwrap();
            Ok(input.payload().clone())
        }),
        2,
        Arc::default(),
    );
    let first = request(&world, owner, worker.owner, 1);
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let expiring = world
        .request(
            owner,
            worker.owner,
            Payload::Pulse(2),
            world.now() + Duration::from_millis(40),
        )
        .unwrap();
    wait(|| runtime.cpu_stats().queued == 1);
    assert!(matches!(take(&world, expiring), TerminalOutcome::TimedOut));
    wait(|| runtime.cpu_stats().queued == 0);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    pulse(&world, first, 1);
    clean(&mut runtime);
}

#[test]
fn cpu_jobs_also_obey_component_and_global_task_caps() {
    for (local, global) in [(1, 8), (4, 2)] {
        let world = World::new(Config::default()).unwrap();
        let mut runtime = runtime(&world, 1, 8, global);
        let owner = actor(&world);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release = Mutex::new(release_rx);
        let denied = Arc::new(AtomicUsize::new(0));
        let worker = prepare(
            &runtime,
            Arc::new(move |input, _| {
                started_tx.send(()).unwrap();
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(4))
                    .unwrap();
                Ok(input.payload().clone())
            }),
            local,
            denied.clone(),
        );
        let first = request(&world, owner, worker.owner, 1);
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = request(&world, owner, worker.owner, 2);
        pulse(&world, second, -1);
        assert_eq!(denied.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.cpu_stats().queued, 0);
        assert_eq!(runtime.cpu_stats().running, 1);
        release_tx.send(()).unwrap();
        pulse(&world, first, 1);
        clean(&mut runtime);
    }
}

#[test]
fn cpu_oversized_output_uses_reserved_failure_result_and_releases_bytes() {
    let world = World::new(Config {
        max_event_bytes: 8,
        ..Config::default()
    })
    .unwrap();
    let mut runtime = runtime(&world, 1, 2, 8);
    let owner = actor(&world);
    let worker = prepare(
        &runtime,
        Arc::new(|_, _| Ok(Payload::CountSnapshot { count: 1, total: 2 })),
        2,
        Arc::default(),
    );
    let operation = request(&world, owner, worker.owner, 1);
    assert!(matches!(
        take(&world, operation),
        TerminalOutcome::Failed {
            code: actorplane_core::operations::OperationFailure::ResultTooLarge,
            ..
        }
    ));
    wait(|| runtime.cpu_stats().running == 0);
    assert_eq!(world.snapshot().operation_retained, 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    clean(&mut runtime);
}

#[test]
fn both_cpu_workers_can_block_while_timers_and_queued_cancellation_progress() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = runtime(&world, 2, 3, 12);
    let owner = actor(&world);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    let worker = prepare(
        &runtime,
        Arc::new(move |input, _| {
            started_tx.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(4))
                .unwrap();
            Ok(input.payload().clone())
        }),
        3,
        Arc::default(),
    );
    let first = request(&world, owner, worker.owner, 1);
    let second = request(&world, owner, worker.owner, 2);
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(runtime.cpu_stats().running, 2);
    let third = request(&world, owner, worker.owner, 3);
    wait(|| runtime.cpu_stats().queued == 1);
    let timer = actor(&world);
    runtime
        .after(owner, timer, Duration::from_millis(5), Payload::Pulse(9))
        .unwrap();
    wait(|| {
        world
            .snapshot()
            .actors
            .iter()
            .any(|a| a.reference == timer && a.queue_entries == 1)
    });
    let delivery = world.claim(timer).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(9));
    delivery.finish(true);
    world.cancel_operation(third).unwrap();
    assert!(matches!(take(&world, third), TerminalOutcome::Cancelled));
    wait(|| runtime.cpu_stats().queued == 0);
    assert_eq!(runtime.cpu_stats().running, 2);
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    pulse(&world, first, 1);
    pulse(&world, second, 2);
    clean(&mut runtime);
}

#[test]
fn cpu_output_reservation_survives_factory_return_through_result_admission() {
    for budget in [32, 64] {
        let world = World::new(Config {
            native_payload_budget: budget,
            max_event_bytes: 32,
            ..Config::default()
        })
        .unwrap();
        let mut runtime = runtime(&world, 1, 2, 8);
        let owner = actor(&world);
        let observer = world.clone();
        let worker = prepare(
            &runtime,
            Arc::new(move |_input, job| {
                job.reserve_output(16)?;
                job.reserve_output(8)?; // A smaller request never releases the reservation.
                assert_eq!(observer.snapshot().retained_payload_bytes, 32);
                if budget == 32 {
                    assert!(job.reserve_output(17).is_err());
                    assert_eq!(observer.snapshot().retained_payload_bytes, 32);
                }
                assert!(job.reserve_output(33).is_err()); // Event cap, before allocation.
                Ok(Payload::CountSnapshot { count: 1, total: 4 })
            }),
            2,
            Arc::default(),
        );
        let operation = request(&world, owner, worker.owner, 1);
        match (budget, take(&world, operation)) {
            (
                32,
                TerminalOutcome::Failed {
                    code: actorplane_core::operations::OperationFailure::ResultTooLarge,
                    ..
                },
            ) => (),
            (64, TerminalOutcome::Completed(result)) => assert_eq!(
                result.payload(),
                &Payload::CountSnapshot { count: 1, total: 4 }
            ),
            (_, other) => panic!("output reservation admission: {other:?}"),
        }
        wait(|| runtime.cpu_stats().running == 0);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        clean(&mut runtime);
    }
}
