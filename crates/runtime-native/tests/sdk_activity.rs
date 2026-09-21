//! Native scheduler activity should not wake idle components for unrelated work.

use actorplane_core::{
    ComponentDescriptor, Config, InterfaceSpec, Payload, PayloadType, PortDirection, PortSpec,
    World,
    schema::{Field, Schema},
};
use actorplane_native::{
    NativeRuntime,
    sdk::{NativeBehavior, NativeContext, NativeResult, NativeSpec},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}

fn spec(name: &str, input: bool) -> NativeSpec {
    let ports = if input {
        vec![PortSpec {
            name: "in".into(),
            direction: PortDirection::Input,
            schema: PayloadType::Pulse,
        }]
    } else {
        Vec::new()
    };
    NativeSpec::new(
        ComponentDescriptor {
            name: name.into(),
            version: 1,
            ports: ports.clone(),
            interfaces: vec![InterfaceSpec {
                name: name.into(),
                version: 1,
                ports,
            }],
        },
        Schema {
            name: format!("{name}.Config"),
            version: 1,
            fields: Vec::<Field>::new(),
        },
    )
}

struct Counting {
    calls: Arc<AtomicUsize>,
}

impl NativeBehavior for Counting {
    fn on_event(&mut self, _: &Payload, _: &mut NativeContext<'_>) -> NativeResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn unrelated_idle_native_components_do_not_take_extra_turns() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 32).unwrap();
    let target_calls = Arc::new(AtomicUsize::new(0));
    let target = runtime
        .prepare_native::<Counting, _>(
            None,
            spec("ActivityTarget", true),
            actorplane_core::schema::Value::Record(Vec::new()),
            {
                let target_calls = target_calls.clone();
                move |_| {
                    Ok(Counting {
                        calls: target_calls,
                    })
                }
            },
        )
        .unwrap();
    let mut idle = Vec::new();
    for index in 0..16 {
        idle.push(
            runtime
                .prepare_native::<Counting, _>(
                    None,
                    spec(&format!("Idle{index}"), false),
                    actorplane_core::schema::Value::Record(Vec::new()),
                    |_| {
                        Ok(Counting {
                            calls: Arc::new(AtomicUsize::new(0)),
                        })
                    },
                )
                .unwrap(),
        );
    }
    world.activate(target.owner).unwrap();
    for handle in &idle {
        world.activate(handle.owner).unwrap();
    }
    wait_until(|| idle.iter().all(|handle| handle.stats().turns > 0));
    let mut previous = Vec::new();
    let mut stable = 0;
    wait_until(|| {
        let current: Vec<_> = idle.iter().map(|handle| handle.stats().turns).collect();
        if current == previous {
            stable += 1;
        } else {
            stable = 0;
            previous = current;
        }
        stable >= 3
    });
    let before: Vec<_> = idle.iter().map(|handle| handle.stats().turns).collect();
    let versions: Vec<_> = idle
        .iter()
        .map(|handle| world.activity(handle.owner).unwrap())
        .collect();

    for round in 1..=16 {
        world.send(target.owner, Payload::Pulse(round)).unwrap();
        wait_until(|| world.snapshot().metrics.completed == round as u64);
    }
    assert_eq!(target_calls.load(Ordering::SeqCst), 16);
    // Give runnable idle executors a scheduling window after the final delivery.
    thread::sleep(Duration::from_millis(20));
    let after: Vec<_> = idle.iter().map(|handle| handle.stats().turns).collect();
    assert_eq!(before, after);
    for (handle, observed) in idle.iter().zip(versions) {
        assert_eq!(world.activity(handle.owner).unwrap(), observed);
    }

    runtime.close(Duration::from_secs(1)).unwrap();
}
