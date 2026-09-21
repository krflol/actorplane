use actorplane_core::{
    ComponentDescriptor, Config, InterfaceSpec, Lifecycle, MessageOptions, Payload, PayloadType,
    PortDirection, PortSpec, TraceContext, World,
    schema::{Field, FieldType, Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

struct Scaling {
    factor: i64,
    saw_correlation: Arc<AtomicBool>,
}

impl NativeBehavior for Scaling {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let Some(envelope) = ctx.envelope() else {
            return Err(NativeError::Application("missing request envelope"));
        };
        if envelope.correlation_id == Some(9) {
            self.saw_correlation.store(true, Ordering::SeqCst);
        }
        let factor = self.factor;
        let input = payload.clone();
        ctx.defer_reply(input, move |held, job| async move {
            // Stand-in for an asynchronous wait; this example performs no I/O.
            tokio::time::sleep(Duration::from_millis(2)).await;
            if job.is_cancelled() {
                return Err(NativeError::Core(actorplane_core::Error::DeadlineExpired));
            }
            match held.payload() {
                Payload::Pulse(value) => value
                    .checked_mul(factor)
                    .map(Payload::Pulse)
                    .ok_or(NativeError::Application("ScaleOverflow")),
                _ => Err(NativeError::Application("expected pulse")),
            }
        })
    }
}

fn spec(name: &str) -> NativeSpec {
    let port = PortSpec {
        name: "requests".into(),
        direction: PortDirection::Input,
        schema: PayloadType::Pulse,
    };
    NativeSpec::new(
        ComponentDescriptor {
            name: name.into(),
            version: 1,
            ports: vec![port.clone()],
            interfaces: vec![InterfaceSpec {
                name: name.into(),
                version: 1,
                ports: vec![port],
            }],
        },
        Schema {
            name: format!("{name}.Config"),
            version: 1,
            fields: vec![Field {
                name: "factor".into(),
                ty: FieldType::Int { min: 1, max: 100 },
            }],
        },
    )
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !predicate() {
        assert!(Instant::now() < deadline, "native SDK example timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(Config::default())?;
    let mut runtime = NativeRuntime::new(world.clone(), 8)?;
    let first_metadata = Arc::new(AtomicBool::new(false));
    let second_metadata = Arc::new(AtomicBool::new(false));

    let first = runtime.prepare_native::<Scaling, _>(
        None,
        spec("ScalingTwo"),
        Value::Record(vec![Value::Int(2)]),
        {
            let saw = first_metadata.clone();
            move |config| {
                let Value::Record(fields) = config else {
                    return Err(NativeError::Application("bad config"));
                };
                let Value::Int(factor) = fields[0] else {
                    return Err(NativeError::Application("bad factor"));
                };
                Ok(Scaling {
                    factor,
                    saw_correlation: saw,
                })
            }
        },
    )?;
    let second = runtime.prepare_native::<Scaling, _>(
        None,
        spec("ScalingThree"),
        Value::Record(vec![Value::Int(3)]),
        {
            let saw = second_metadata.clone();
            move |config| {
                let Value::Record(fields) = config else {
                    return Err(NativeError::Application("bad config"));
                };
                let Value::Int(factor) = fields[0] else {
                    return Err(NativeError::Application("bad factor"));
                };
                Ok(Scaling {
                    factor,
                    saw_correlation: saw,
                })
            }
        },
    )?;
    world.activate(first.owner)?;
    world.activate(second.owner)?;

    let trace = TraceContext::new(
        [7; 16],
        [8; 8],
        true,
        vec![("example".into(), "sdk".into())],
    )?;
    let options = MessageOptions {
        correlation_id: Some(9),
        trace: Some(trace),
        ..MessageOptions::default()
    };
    let first_request = world.request_with(
        first.owner,
        first.owner,
        Payload::Pulse(7),
        Instant::now() + Duration::from_secs(1),
        options.clone(),
    )?;
    let second_request = world.request_with(
        second.owner,
        second.owner,
        Payload::Pulse(7),
        Instant::now() + Duration::from_secs(1),
        options,
    )?;
    wait_until(|| {
        matches!(
            world.operation_status(first_request),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });
    wait_until(|| {
        matches!(
            world.operation_status(second_request),
            Ok(actorplane_core::OperationStatus::Terminal(_))
        )
    });

    match world.take_operation(first_request)?.unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(14));
            let envelope = result.envelope().unwrap();
            assert_eq!(envelope.correlation_id, Some(9));
            assert_eq!(*envelope.trace.as_ref().unwrap().trace_id(), [7; 16]);
        }
        other => panic!("first request failed: {other:?}"),
    }
    match world.take_operation(second_request)?.unwrap() {
        actorplane_core::TerminalOutcome::Completed(result) => {
            assert_eq!(result.payload(), &Payload::Pulse(21))
        }
        other => panic!("second request failed: {other:?}"),
    }
    assert!(first_metadata.load(Ordering::SeqCst));
    assert!(second_metadata.load(Ordering::SeqCst));
    assert!(matches!(world.state(first.owner), Ok(Lifecycle::Active)));

    let report = runtime.close(Duration::from_secs(1))?;
    assert!(report.native_done && !report.timed_out);
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    println!("native SDK scaling example completed: 7 -> 14 and 21");
    Ok(())
}
