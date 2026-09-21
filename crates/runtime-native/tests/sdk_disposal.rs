use actorplane_core::{
    ComponentDescriptor, Config, Lifecycle, Payload, World,
    schema::{Schema, Value},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{DrainPolicy, NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn spec() -> NativeSpec {
    NativeSpec::new(
        ComponentDescriptor {
            name: "Disposal".into(),
            version: 1,
            ports: vec![],
            interfaces: vec![],
        },
        Schema {
            name: "Disposal.Config".into(),
            version: 1,
            fields: vec![],
        },
    )
}

struct PanickingDrop {
    calls: Arc<AtomicUsize>,
    fail_start: bool,
}
impl NativeBehavior for PanickingDrop {
    fn on_start(&mut self, _: &mut NativeContext<'_>) -> NativeResult {
        if self.fail_start {
            Err(NativeError::Application("StartFailed"))
        } else {
            Ok(())
        }
    }
    fn on_event(&mut self, _: &Payload, _: &mut NativeContext<'_>) -> NativeResult {
        Ok(())
    }
}
impl Drop for PanickingDrop {
    fn drop(&mut self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("drop panic");
    }
}

#[test]
fn startup_rollback_contains_component_destructor_panic() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 2).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let result = runtime.prepare_native(None, spec(), Value::Record(vec![]), |_| {
        Ok(PanickingDrop {
            calls: calls.clone(),
            fail_start: true,
        })
    });
    assert!(matches!(
        result,
        Err(NativeError::Application("StartFailed"))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        world
            .snapshot()
            .actors
            .iter()
            .all(|actor| actor.state == Lifecycle::Stopped)
    );
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn normal_shutdown_contains_component_destructor_panic() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 2).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = runtime
        .prepare_native(None, spec(), Value::Record(vec![]), |_| {
            Ok(PanickingDrop {
                calls: calls.clone(),
                fail_start: false,
            })
        })
        .unwrap();
    world.activate(handle.owner).unwrap();
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}

#[test]
fn producer_with_input_port_is_rejected_before_construction() {
    let mut native = spec();
    native.drain = DrainPolicy::Producer;
    native.descriptor.ports.push(actorplane_core::PortSpec {
        name: "in".into(),
        direction: actorplane_core::PortDirection::Input,
        schema: actorplane_core::PayloadType::Pulse,
    });
    assert!(matches!(
        native.validate_config(&Value::Record(vec![])),
        Err(NativeError::Application("ProducerHasInputPorts"))
    ));
}
