#![cfg(feature = "test-runtime")]
use actorplane_core::{Config, EndpointKind, Payload};
use actorplane_native::{NativeRuntime, components::ComponentKind};
use std::time::Duration;

#[test]
fn invalid_or_nested_controls_preserve_clock_and_pending_work() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
    let world = runtime.world().clone();
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    runtime
        .after(actor, actor, Duration::from_millis(10), Payload::Pulse(1))
        .unwrap();
    runtime.pump_virtual(100).unwrap();
    assert!(
        runtime
            .advance_virtual(Duration::from_secs(86401), 100)
            .is_err()
    );
    assert!(
        runtime
            .advance_virtual(Duration::from_millis(10), 0)
            .is_err()
    );
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(runtime.pump_virtual(100).is_err());
                assert!(
                    runtime
                        .advance_virtual(Duration::from_millis(10), 100)
                        .is_err()
                );
            })
            .join()
            .unwrap();
    });
    let outer = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    outer.block_on(async {
        assert!(NativeRuntime::new_virtual(Config::default(), 8).is_err());
        assert!(runtime.pump_virtual(100).is_err());
        assert!(
            runtime
                .advance_virtual(Duration::from_millis(10), 100)
                .is_err()
        );
        assert!(runtime.close(Duration::ZERO).is_err());
    });
    assert_eq!(world.elapsed_ns(), 0);
    assert!(world.claim(actor).unwrap().is_none());
    assert_eq!(world.snapshot().retained_payload_bytes, 8);
    runtime
        .advance_virtual(Duration::from_millis(10), 100)
        .unwrap();
    let delivery = world.claim(actor).unwrap().unwrap();
    assert_eq!(delivery.payload(), &Payload::Pulse(1));
    delivery.finish(true);
    assert!(runtime.close(Duration::ZERO).unwrap().native_done);
}

#[test]
fn advance_accounts_ready_timer_polls_against_the_same_budget() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 16).unwrap();
    let world = runtime.world().clone();
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    for value in 0..8 {
        runtime
            .after(
                actor,
                actor,
                Duration::from_millis(10),
                Payload::Pulse(value),
            )
            .unwrap();
    }
    assert!(runtime.pump_virtual(100).unwrap().idle);
    let report = runtime
        .advance_virtual(Duration::from_millis(10), 1)
        .unwrap();
    assert_eq!(report.polls, 1);
    assert!(!report.idle);
    assert_eq!(world.elapsed_ns(), 10_000_000);
    assert!(runtime.pump_virtual(100).unwrap().idle);
    let mut received = Vec::new();
    while let Some(delivery) = world.claim(actor).unwrap() {
        received.push(delivery.payload().clone());
        delivery.finish(true);
    }
    assert_eq!(received.len(), 8);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert!(runtime.close(Duration::ZERO).unwrap().native_done);
}

#[test]
fn idle_pump_never_advances_and_real_timer_obeys_explicit_clock() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
    let world = runtime.world().clone();
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    runtime
        .after(actor, actor, Duration::from_millis(10), Payload::Pulse(42))
        .unwrap();
    assert!(runtime.pump_virtual(100).unwrap().idle);
    assert_eq!(world.elapsed_ns(), 0);
    assert!(world.claim(actor).unwrap().is_none());
    runtime
        .advance_virtual(Duration::from_millis(9), 100)
        .unwrap();
    assert!(world.claim(actor).unwrap().is_none());
    runtime
        .advance_virtual(Duration::from_millis(1), 100)
        .unwrap();
    let delivery = world
        .claim(actor)
        .unwrap()
        .expect("timer must be ready at its deadline");
    assert_eq!(delivery.payload(), &Payload::Pulse(42));
    assert_eq!(
        world.instant_ns(delivery.envelope().enqueued_at),
        10_000_000
    );
    delivery.finish(true);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn sdk_source_runs_on_virtual_clock_and_skips_missed_ticks() {
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
    let parent = runtime
        .world()
        .allocate(EndpointKind::Native, None)
        .unwrap();
    runtime.world().activate(parent).unwrap();
    let source = runtime
        .prepare_component(
            parent,
            ComponentKind::PulseSource {
                interval: Duration::from_millis(10),
            },
        )
        .unwrap();
    runtime.world().activate(source.owner).unwrap();
    assert!(runtime.pump_virtual(100).unwrap().idle);
    runtime
        .advance_virtual(Duration::from_millis(10), 100)
        .unwrap();
    assert_eq!(source.stats().generated, 1);
    runtime
        .advance_virtual(Duration::from_millis(100), 100)
        .unwrap();
    assert_eq!(source.stats().generated, 2);
    assert_eq!(runtime.world().elapsed_ns(), 110_000_000);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn native_self_send_cycle_returns_at_poll_budget_without_advancing_time() {
    use actorplane_core::{
        ComponentDescriptor, MessageOptions,
        schema::{Schema, Value},
    };
    use actorplane_native::sdk::{NativeBehavior, NativeContext, NativeResult, NativeSpec};
    struct Cycle;
    impl NativeBehavior for Cycle {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            ctx.send(ctx.owner(), payload.clone(), MessageOptions::default())?;
            Ok(())
        }
    }
    let mut runtime = NativeRuntime::new_virtual(Config::default(), 8).unwrap();
    let mut spec = NativeSpec::new(
        ComponentDescriptor {
            name: "Cycle".into(),
            version: 1,
            ports: vec![actorplane_core::PortSpec {
                name: "input".into(),
                direction: actorplane_core::PortDirection::Input,
                schema: actorplane_core::PayloadType::Pulse,
            }],
            interfaces: vec![],
        },
        Schema {
            name: "Cycle.Config".into(),
            version: 1,
            fields: vec![],
        },
    );
    spec.limits.events_per_turn = 1;
    let native = runtime
        .prepare_native(None, spec, Value::Record(vec![]), |_| Ok(Cycle))
        .unwrap();
    runtime.world().activate(native.owner).unwrap();
    runtime
        .world()
        .send(native.owner, Payload::Pulse(1))
        .unwrap();
    let report = runtime.pump_virtual(5).unwrap();
    assert!(!report.idle);
    assert_eq!(report.polls, 5);
    assert_eq!(runtime.world().elapsed_ns(), 0);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn invalid_deferred_result_finishes_failed_instead_of_remaining_pending() {
    use actorplane_core::{
        ComponentDescriptor, TerminalOutcome,
        schema::{Schema, Value},
    };
    use actorplane_native::sdk::{NativeBehavior, NativeContext, NativeResult, NativeSpec};
    struct Oversized;
    impl NativeBehavior for Oversized {
        fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
            ctx.defer_reply(payload.clone(), |_, _| async {
                Ok(Payload::CountSnapshot { count: 1, total: 1 })
            })
        }
    }
    let mut runtime = NativeRuntime::new_virtual(
        Config {
            max_event_bytes: 8,
            ..Config::default()
        },
        8,
    )
    .unwrap();
    let spec = NativeSpec::new(
        ComponentDescriptor {
            name: "Oversized".into(),
            version: 1,
            ports: vec![actorplane_core::PortSpec {
                name: "input".into(),
                direction: actorplane_core::PortDirection::Input,
                schema: actorplane_core::PayloadType::Pulse,
            }],
            interfaces: vec![],
        },
        Schema {
            name: "Oversized.Config".into(),
            version: 1,
            fields: vec![],
        },
    );
    let native = runtime
        .prepare_native(None, spec, Value::Record(vec![]), |_| Ok(Oversized))
        .unwrap();
    let owner = runtime
        .world()
        .allocate(EndpointKind::Native, None)
        .unwrap();
    runtime.world().activate(owner).unwrap();
    runtime.world().activate(native.owner).unwrap();
    let id = runtime
        .world()
        .request(
            owner,
            native.owner,
            Payload::Pulse(1),
            runtime.world().now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(runtime.pump_virtual(100).unwrap().idle);
    assert!(matches!(
        runtime.world().take_operation(id).unwrap(),
        Some(TerminalOutcome::Failed { .. })
    ));
    assert_eq!(runtime.world().snapshot().retained_payload_bytes, 0);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}
