use actorplane_core::{
    ComponentDescriptor, Config, FailureAction, Lifecycle, Payload, PayloadType, PortDirection,
    PortSpec, World,
    schema::{Field, FieldType, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{
        DrainPolicy, NativeBehavior, NativeContext, NativeError, NativeLimits, NativeResult,
        NativeSpec,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

fn config_schema() -> Schema {
    Schema {
        name: "Config".into(),
        version: 1,
        fields: vec![Field {
            name: "enabled".into(),
            ty: FieldType::Bool,
        }],
    }
}

fn spec(name: &str, ports: Vec<PortSpec>) -> NativeSpec {
    NativeSpec {
        descriptor: ComponentDescriptor {
            name: name.into(),
            version: 1,
            ports,
            interfaces: Vec::new(),
        },
        configuration: config_schema(),
        limits: NativeLimits::default(),
        period: None,
        drain: DrainPolicy::Consumer,
        failure_action: FailureAction::Continue,
    }
}

struct Supervisor {
    failures: Arc<AtomicUsize>,
}
impl NativeBehavior for Supervisor {
    fn on_event(&mut self, _payload: &Payload, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_failure(
        &mut self,
        _failure: &actorplane_core::FailureRecord,
        _coalesced: u64,
        _ctx: &mut NativeContext<'_>,
    ) -> NativeResult<FailureAction> {
        self.failures.fetch_add(1, Ordering::AcqRel);
        Ok(FailureAction::Continue)
    }
}

struct FailsOnce {
    failed: bool,
}
impl NativeBehavior for FailsOnce {
    fn on_event(&mut self, _payload: &Payload, _ctx: &mut NativeContext<'_>) -> NativeResult {
        if !self.failed {
            self.failed = true;
            Err(NativeError::Application("first_failure"))
        } else {
            Ok(())
        }
    }
}

struct BlockingTick {
    begun: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    ticks: Arc<AtomicUsize>,
}
impl NativeBehavior for BlockingTick {
    fn on_event(&mut self, _payload: &Payload, _ctx: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
    fn on_tick(&mut self, _ctx: &mut NativeContext<'_>) -> NativeResult {
        self.ticks.fetch_add(1, Ordering::AcqRel);
        self.begun.store(true, Ordering::Release);
        while !self.release.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
}

#[test]
fn supervisor_continue_receives_one_child_failure_and_child_continues() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let failures = Arc::new(AtomicUsize::new(0));
    let parent = runtime
        .prepare_native::<Supervisor, _>(
            None,
            spec("supervisor", vec![]),
            Value::Record(vec![Value::Bool(true)]),
            {
                let failures = failures.clone();
                move |_| Ok(Supervisor { failures })
            },
        )
        .unwrap();
    let child = runtime
        .prepare_native::<FailsOnce, _>(
            Some(parent.owner),
            spec(
                "child",
                vec![PortSpec {
                    name: "input".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                }],
            ),
            Value::Record(vec![Value::Bool(true)]),
            |_| Ok(FailsOnce { failed: false }),
        )
        .unwrap();
    world.activate(parent.owner).unwrap();
    world.activate(child.owner).unwrap();
    world.send(child.owner, Payload::Pulse(1)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while failures.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(failures.load(Ordering::Acquire), 1);
    world.send(child.owner, Payload::Pulse(2)).unwrap();
    let handled_deadline = Instant::now() + Duration::from_secs(1);
    while child.stats().handled < 2 && Instant::now() < handled_deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(child.stats().handled, 2);
    assert_eq!(world.state(parent.owner).unwrap(), Lifecycle::Active);
    assert_eq!(world.state(child.owner).unwrap(), Lifecycle::Active);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn blocking_tick_holds_control_slot_until_release_then_stops_once() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let begun = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let handle = runtime
        .prepare_native::<BlockingTick, _>(
            None,
            NativeSpec {
                period: Some(Duration::from_millis(1)),
                ..spec("ticker", vec![])
            },
            Value::Record(vec![Value::Bool(true)]),
            {
                let begun = begun.clone();
                let release = release.clone();
                let ticks = ticks.clone();
                move |_| {
                    Ok(BlockingTick {
                        begun,
                        release,
                        ticks,
                    })
                }
            },
        )
        .unwrap();
    world.activate(handle.owner).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !begun.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(begun.load(Ordering::Acquire));
    let report = world.stop(handle.owner).unwrap();
    assert!(!report.native_done);
    assert!(report.control_in_flight > 0);
    let before = ticks.load(Ordering::Acquire);
    release.store(true, Ordering::Release);
    assert!(runtime.wait_native_idle(Duration::from_secs(1)));
    assert_eq!(ticks.load(Ordering::Acquire), before);
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done);
}

#[test]
fn externally_held_callback_does_not_spin_overdue_periodic_ticks() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let ticks = Arc::new(AtomicUsize::new(0));
    let handle = runtime
        .prepare_native::<BlockingTick, _>(
            None,
            NativeSpec {
                period: Some(Duration::from_millis(10)),
                ..spec("ticker", vec![])
            },
            Value::Record(vec![Value::Bool(true)]),
            {
                let begun = Arc::new(AtomicBool::new(false));
                let release = Arc::new(AtomicBool::new(true));
                let ticks = ticks.clone();
                move |_| {
                    Ok(BlockingTick {
                        begun,
                        release,
                        ticks,
                    })
                }
            },
        )
        .unwrap();
    world.activate(handle.owner).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let callback = loop {
        if let Some(callback) = world.claim_native_callback(handle.owner, false).unwrap() {
            break callback;
        }
        assert!(
            Instant::now() < deadline,
            "callback slot was never available"
        );
        thread::sleep(Duration::from_millis(2));
    };
    let before = ticks.load(Ordering::Acquire);
    thread::sleep(Duration::from_millis(40));
    assert_eq!(ticks.load(Ordering::Acquire), before);
    assert!(handle.stats().turns > 0, "executor never ran");
    let settled_turns = handle.stats().turns;
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        handle.stats().turns,
        settled_turns,
        "overdue tick spun while control claim was held"
    );
    drop(callback);
    let deadline = Instant::now() + Duration::from_secs(1);
    while ticks.load(Ordering::Acquire) == before && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(ticks.load(Ordering::Acquire) > before);
    world.stop(handle.owner).unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
