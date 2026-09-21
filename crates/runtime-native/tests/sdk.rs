use actorplane_core::{
    ComponentDescriptor, Config, InterfaceSpec, Lifecycle, MessageOptions, Payload, PayloadType,
    PortDirection, PortSpec, TraceContext, World,
    schema::{Field, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn spec(name: &str, ports: Vec<PortSpec>) -> NativeSpec {
    let descriptor = ComponentDescriptor {
        name: name.into(),
        version: 1,
        interfaces: vec![InterfaceSpec {
            name: name.into(),
            version: 1,
            ports: ports.clone(),
        }],
        ports,
    };
    NativeSpec::new(
        descriptor,
        Schema {
            name: format!("{name}.Config"),
            version: 1,
            fields: Vec::<Field>::new(),
        },
    )
}

fn input(name: &str) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction: PortDirection::Input,
        schema: PayloadType::Pulse,
    }
}

fn output(name: &str) -> PortSpec {
    PortSpec {
        name: name.into(),
        direction: PortDirection::Output,
        schema: PayloadType::Pulse,
    }
}

#[derive(Default)]
struct Echo {
    seen: Arc<AtomicUsize>,
    next: i64,
}
impl NativeBehavior for Echo {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        self.seen.fetch_add(1, Ordering::SeqCst);
        if let Some(envelope) = ctx.envelope()
            && envelope.trace.is_some()
        {
            assert_eq!(envelope.correlation_id, Some(7));
        }
        if ctx.operation().is_some() {
            self.next += 1;
            let response = match payload {
                Payload::Pulse(value) => Payload::Pulse(value + self.next),
                other => other.clone(),
            };
            ctx.reply(response)?;
        }
        Ok(())
    }
}

#[test]
fn two_native_instances_keep_state_and_request_replies_independent() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let first_seen = Arc::new(AtomicUsize::new(0));
    let second_seen = Arc::new(AtomicUsize::new(0));
    let first = runtime
        .prepare_native::<Echo, _>(
            None,
            spec("EchoOne", vec![input("in")]),
            Value::Record(vec![]),
            {
                let seen = first_seen.clone();
                move |_| Ok(Echo { seen, next: 0 })
            },
        )
        .unwrap();
    let second = runtime
        .prepare_native::<Echo, _>(
            None,
            spec("EchoTwo", vec![input("in")]),
            Value::Record(vec![]),
            {
                let seen = second_seen.clone();
                move |_| Ok(Echo { seen, next: 0 })
            },
        )
        .unwrap();
    world.activate(first.owner).unwrap();
    world.activate(second.owner).unwrap();
    let trace =
        TraceContext::new([1; 16], [2; 8], true, vec![("tenant".into(), "sdk".into())]).unwrap();
    let id = world
        .request_with(
            first.owner,
            second.owner,
            Payload::Pulse(41),
            Instant::now() + Duration::from_secs(1),
            MessageOptions {
                correlation_id: Some(7),
                trace: Some(trace.clone()),
                ..MessageOptions::default()
            },
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
            assert_eq!(result.payload(), &Payload::Pulse(42));
            assert_eq!(result.options().correlation_id, Some(7));
            assert_eq!(result.options().trace, Some(trace));
        }
        other => panic!("unexpected metadata response: {other:?}"),
    }
    let first_id = world
        .request(
            second.owner,
            first.owner,
            Payload::Pulse(10),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(first_id),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    match world.take_operation(first_id).unwrap().unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(11))
        }
        other => panic!("unexpected first-instance response: {other:?}"),
    }
    let second_id = world
        .request(
            first.owner,
            second.owner,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(second_id),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    match world.take_operation(second_id).unwrap().unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(3))
        }
        other => panic!("unexpected second response: {other:?}"),
    }
    let third_id = world
        .request(
            second.owner,
            first.owner,
            Payload::Pulse(20),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    wait_until(|| {
        matches!(
            world.operation_status(third_id),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    match world.take_operation(third_id).unwrap().unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(22))
        }
        other => panic!("unexpected third response: {other:?}"),
    }
    assert_eq!(first_seen.load(Ordering::SeqCst), 2);
    assert_eq!(second_seen.load(Ordering::SeqCst), 2);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn invalid_config_does_not_call_factory_and_start_failure_rolls_back() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let mut invalid = spec("Invalid", vec![]);
    invalid.configuration.fields.push(Field {
        name: "value".into(),
        ty: actorplane_core::schema::FieldType::Int { min: 0, max: 1 },
    });
    let result =
        runtime.prepare_native::<Echo, _>(None, invalid, Value::Record(vec![Value::Int(4)]), {
            let called = called.clone();
            move |_| {
                called.store(true, Ordering::SeqCst);
                Ok(Echo::default())
            }
        });
    assert!(result.is_err());
    assert!(!called.load(Ordering::SeqCst));

    struct Fails;
    impl NativeBehavior for Fails {
        fn on_start(&mut self, ctx: &mut NativeContext<'_>) -> NativeResult {
            ctx.emit("out", Payload::Pulse(1))?;
            ctx.after(
                Duration::from_secs(5),
                ctx.owner(),
                Payload::Pulse(2),
                MessageOptions::default(),
            )?;
            Err(NativeError::Application("StartupSentinel"))
        }
        fn on_event(&mut self, _: &Payload, _: &mut NativeContext<'_>) -> NativeResult {
            Ok(())
        }
    }
    let result = runtime.prepare_native::<Fails, _>(
        None,
        spec("Fails", vec![output("out")]),
        Value::Record(vec![]),
        |_| Ok(Fails),
    );
    assert!(matches!(
        result,
        Err(NativeError::Application("StartupSentinel"))
    ));
    runtime.close(Duration::from_secs(1)).unwrap();
    assert!(
        world
            .snapshot()
            .actors
            .iter()
            .all(|actor| matches!(actor.state, Lifecycle::Stopped))
    );
}

#[test]
fn callback_effect_budget_admits_only_two_effects() {
    struct Effects;
    impl NativeBehavior for Effects {
        fn on_event(&mut self, _: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            assert!(ctx.emit("out", Payload::Pulse(1)).is_ok());
            assert!(ctx.emit("out", Payload::Pulse(2)).is_ok());
            assert!(matches!(
                ctx.emit("out", Payload::Pulse(3)),
                Err(NativeError::Limit(_))
            ));
            Ok(())
        }
    }
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let mut native = spec("Effects", vec![input("in"), output("out")]);
    native.limits.effects_per_callback = 2;
    let handle = runtime
        .prepare_native::<Effects, _>(None, native, Value::Record(vec![]), |_| Ok(Effects))
        .unwrap();
    let receiver = world
        .allocate(actorplane_core::EndpointKind::Native, None)
        .unwrap();
    let receiver_ports = world
        .register_component(
            receiver,
            ComponentDescriptor {
                name: "Receiver".into(),
                version: 1,
                interfaces: vec![],
                ports: vec![input("in")],
            },
        )
        .unwrap();
    world
        .link(handle.owner, handle.ports[1], receiver_ports[0])
        .unwrap();
    world.activate(receiver).unwrap();
    world.activate(handle.owner).unwrap();
    world.send(handle.owner, Payload::Pulse(1)).unwrap();
    wait_until(|| handle.stats().handled == 1);
    wait_until(|| {
        world
            .snapshot()
            .actors
            .iter()
            .find(|actor| actor.reference == receiver)
            .is_some_and(|actor| actor.queue_entries == 2)
    });
    let first = world.claim(receiver).unwrap().unwrap();
    assert_eq!(first.payload(), &Payload::Pulse(1));
    first.finish(true);
    let second = world.claim(receiver).unwrap().unwrap();
    assert_eq!(second.payload(), &Payload::Pulse(2));
    second.finish(true);
    assert!(world.claim(receiver).unwrap().is_none());
    assert_eq!(handle.stats().effects_attempted, 3);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn idle_native_handle_does_not_spin_turns() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let handle = runtime
        .prepare_native::<Echo, _>(None, spec("Idle", vec![]), Value::Record(vec![]), |_| {
            Ok(Echo::default())
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    wait_until(|| handle.stats().turns > 0);
    std::thread::sleep(Duration::from_millis(20));
    let before = handle.stats().turns;
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(before, handle.stats().turns);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn panic_in_event_stops_component_and_does_not_reinvoke_it() {
    struct Panics {
        calls: Arc<AtomicUsize>,
    }
    impl NativeBehavior for Panics {
        fn on_event(&mut self, _: &Payload, _: &mut NativeContext<'_>) -> NativeResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("intentional native panic")
        }
    }
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = runtime
        .prepare_native::<Panics, _>(
            None,
            spec("Panics", vec![input("in")]),
            Value::Record(vec![]),
            {
                let calls = calls.clone();
                move |_| Ok(Panics { calls })
            },
        )
        .unwrap();
    world.activate(handle.owner).unwrap();
    world.send(handle.owner, Payload::Pulse(1)).unwrap();
    wait_until(|| {
        matches!(
            world.execution_state(handle.owner),
            Ok(Lifecycle::Stopping | Lifecycle::Stopped)
        )
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    runtime.close(Duration::from_secs(1)).unwrap();
    assert_eq!(runtime.active_tasks(), 0);
    assert!(matches!(world.state(handle.owner), Ok(Lifecycle::Stopped)));
}
