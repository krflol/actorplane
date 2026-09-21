use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, InterfaceSpec, OperationStatus, Payload,
    PayloadType, PortDirection, PortSpec, World,
    schema::{Field, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
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
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::yield_now();
    }
}

struct CancelAware {
    started: Sender<()>,
    release: Arc<Mutex<Receiver<()>>>,
    factories: Arc<AtomicUsize>,
    events: usize,
    cpu: bool,
}

impl NativeBehavior for CancelAware {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        self.events += 1;
        if self.events == 1 {
            self.started
                .send(())
                .expect("test observer must remain alive");
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .expect("test must retire the operation before releasing the handler");
        }
        let factories = self.factories.clone();
        if self.cpu {
            return ctx.defer_cpu_reply(payload.clone(), move |held, _job| {
                factories.fetch_add(1, Ordering::SeqCst);
                Ok(held.payload().clone())
            });
        }
        ctx.defer_reply(payload.clone(), move |held, _job| async move {
            factories.fetch_add(1, Ordering::SeqCst);
            Ok(held.payload().clone())
        })
    }
}

#[test]
fn cancelled_defer_submission_is_benign_and_fresh_request_still_completes() {
    cancelled_submission(false);
}

#[test]
fn cancelled_cpu_submission_is_benign_and_fresh_request_still_completes() {
    cancelled_submission(true);
}

fn cancelled_submission(cpu: bool) {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let factories = Arc::new(AtomicUsize::new(0));
    let factories_for_factory = factories.clone();
    let spec = NativeSpec::new(
        ComponentDescriptor {
            name: "CancelAware".into(),
            version: 1,
            interfaces: vec![InterfaceSpec {
                name: "CancelAware".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "requests".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                }],
            }],
            ports: vec![PortSpec {
                name: "requests".into(),
                direction: PortDirection::Input,
                schema: PayloadType::Pulse,
            }],
        },
        Schema {
            name: "CancelAware.Config".into(),
            version: 1,
            fields: Vec::<Field>::new(),
        },
    );
    let handle = runtime
        .prepare_native::<CancelAware, _>(None, spec, Value::Record(vec![]), move |_| {
            Ok(CancelAware {
                started: started_tx,
                release: Arc::new(Mutex::new(release_rx)),
                factories: factories_for_factory,
                events: 0,
                cpu,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();

    let first = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(11),
            world.now() + Duration::from_secs(5),
        )
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(world.cancel_operation(first).unwrap());
    let _ = world.take_operation(first).unwrap();
    release_tx.send(()).unwrap();
    wait_until(|| handle.stats().handled >= 1);
    assert_eq!(
        world.state(handle.owner),
        Ok(actorplane_core::Lifecycle::Active)
    );
    assert_eq!(handle.stats().jobs_submitted, 0);
    assert_eq!(world.snapshot().metrics.failed, 0);
    assert_eq!(world.snapshot().metrics.failures, 0);
    assert_eq!(factories.load(Ordering::SeqCst), 0);

    let second = world
        .request(
            requester,
            handle.owner,
            Payload::Pulse(22),
            world.now() + Duration::from_secs(5),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(second),
            Ok(OperationStatus::Terminal(_))
        )
    });
    let outcome = world.take_operation(second).unwrap().unwrap();
    match outcome {
        actorplane_core::TerminalOutcome::Completed(reply) => {
            assert_eq!(reply.payload(), &Payload::Pulse(22));
        }
        other => panic!("fresh request did not complete: {other:?}"),
    }
    wait_until(|| factories.load(Ordering::SeqCst) == 1);
    assert_eq!(handle.stats().jobs_submitted, 1);
    runtime.close(Duration::from_secs(1)).unwrap();
}
