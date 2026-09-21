use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, OperationStatus, Payload, PayloadType,
    PortDirection, PortSpec, TerminalOutcome, World,
    schema::{Field, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    cpu::CpuConfig,
    sdk::{NativeBehavior, NativeContext, NativeResult, NativeSpec},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    time::{Duration, Instant},
};

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::yield_now();
    }
}

struct BlockingCpu {
    started: Sender<()>,
    release: Arc<Mutex<Receiver<()>>>,
    calls: Arc<AtomicUsize>,
}
impl NativeBehavior for BlockingCpu {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let started = self.started.clone();
        let release = self.release.clone();
        let calls = self.calls.clone();
        ctx.defer_cpu_reply(payload.clone(), move |held, _job| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                started.send(()).unwrap();
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(4))
                    .expect("test must release the first CPU computation");
            }
            Ok(held.payload().clone())
        })
    }
}

fn prepare_blocking(
    runtime: &NativeRuntime,
    started: Sender<()>,
    release: Arc<Mutex<Receiver<()>>>,
    calls: Arc<AtomicUsize>,
) -> actorplane_native::sdk::NativeHandle {
    let spec = NativeSpec::new(
        ComponentDescriptor {
            name: "BlockingCpuRetirement".into(),
            version: 1,
            interfaces: vec![],
            ports: vec![PortSpec {
                name: "requests".into(),
                direction: PortDirection::Input,
                schema: PayloadType::Pulse,
            }],
        },
        Schema {
            name: "BlockingCpuRetirement.Config".into(),
            version: 1,
            fields: Vec::<Field>::new(),
        },
    );
    runtime
        .prepare_native(None, spec, Value::Record(vec![]), move |_| {
            Ok(BlockingCpu {
                started,
                release,
                calls,
            })
        })
        .unwrap()
}

#[test]
fn running_cancel_fences_late_cpu_result_and_reused_operation_slot() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new_with_cpu_config(
        world.clone(),
        8,
        CpuConfig {
            workers: 1,
            max_jobs: 2,
        },
    )
    .unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Arc::new(Mutex::new(release_rx));
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = prepare_blocking(&runtime, started_tx, release.clone(), calls.clone());
    world.activate(handle.owner).unwrap();
    let first = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(5),
        )
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(world.cancel_operation(first).unwrap());
    assert!(matches!(
        world.take_operation(first).unwrap(),
        Some(TerminalOutcome::Cancelled)
    ));
    let second = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(2),
            world.now() + Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(first.slot, second.slot);
    assert_ne!(first.generation, second.generation);
    wait_until(|| runtime.cpu_stats().queued == 1);
    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !matches!(
        world.operation_status(second),
        Ok(OperationStatus::Terminal(_))
    ) {
        assert!(
            Instant::now() < deadline,
            "replacement operation did not finish: {:?}",
            world.operation_status(second)
        );
        std::thread::yield_now();
    }
    assert_ne!(first, second);
    match world.take_operation(second).unwrap().unwrap() {
        TerminalOutcome::Completed(reply) => assert_eq!(reply.payload(), &Payload::Pulse(2)),
        other => panic!("unexpected replacement result: {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done && !report.timed_out);
    assert_eq!(runtime.cpu_stats().cancelled, 1);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn admitted_cpu_reply_finishes_during_drain_before_shared_deadline() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new_with_cpu_config(
        world.clone(),
        8,
        CpuConfig {
            workers: 1,
            max_jobs: 2,
        },
    )
    .unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = prepare_blocking(
        &runtime,
        started_tx,
        Arc::new(Mutex::new(release_rx)),
        Arc::default(),
    );
    world.activate(handle.owner).unwrap();
    let operation = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(1),
            world.now() + Duration::from_secs(2),
        )
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let report = world
        .request_drain(handle.owner, world.now() + Duration::from_secs(2))
        .unwrap();
    assert!(!report.native_done);
    assert_eq!(runtime.cpu_stats().running, 1);
    release_tx.send(()).unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(operation),
            Ok(OperationStatus::Terminal(_))
        )
    });
    match world.take_operation(operation).unwrap().unwrap() {
        TerminalOutcome::Completed(reply) => assert_eq!(reply.payload(), &Payload::Pulse(1)),
        other => panic!("drained CPU reply: {other:?}"),
    }
    wait_until(|| world.state(handle.owner) == Ok(actorplane_core::Lifecycle::Stopped));
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done && !report.timed_out);
    assert_eq!(runtime.cpu_stats().completed, 1);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
#[cfg(feature = "test-runtime")]
fn sum_squares_rejects_test_world_before_cpu_resources_are_reserved() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
    let result = runtime.prepare_sum_squares(None);
    assert!(matches!(
        result,
        Err(actorplane_native::sdk::NativeError::Application(
            "CpuUnavailableInTestWorld"
        ))
    ));
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(runtime.cpu_stats().queued, 0);
    runtime.close(Duration::from_secs(1)).unwrap();
}
