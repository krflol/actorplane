use actorplane_core::{
    Clock, Config, EndpointKind, OperationStatus, Payload, TerminalOutcome, World,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn operation_expiry_wakes_both_endpoints_for_each_expiry_path() {
    for path in ["maintain", "claim", "late-result"] {
        let (clock, control) = Clock::manual_at(Instant::now());
        let world = World::with_clock(Config::default(), clock).unwrap();
        let actors: Vec<_> = (0..3)
            .map(|_| {
                let actor = world.allocate(EndpointKind::Native, None).unwrap();
                world.activate(actor).unwrap();
                actor
            })
            .collect();
        let (owner, target, unrelated) = (actors[0], actors[1], actors[2]);
        let operation = world
            .request(
                owner,
                target,
                Payload::Pulse(1),
                world.now() + Duration::from_secs(1),
            )
            .unwrap();
        if path != "claim" {
            world.claim(target).unwrap().unwrap().finish(true);
        }
        let signals: Vec<_> = actors
            .iter()
            .map(|actor| {
                let signal = Arc::new(Signal::default());
                let version = world.activity(*actor).unwrap();
                let waker = Waker::from(signal.clone());
                assert!(
                    world
                        .poll_activity(*actor, version, &mut Context::from_waker(&waker))
                        .is_pending()
                );
                (signal, version)
            })
            .collect();
        control.advance(Duration::from_secs(1)).unwrap();
        match path {
            "maintain" => world.maintain(world.now()),
            "claim" => assert!(world.claim(target).unwrap().is_none()),
            _ => assert!(
                !world
                    .complete_operation(operation, target, Payload::Pulse(2))
                    .unwrap()
            ),
        }
        assert!(
            matches!(
                world.operation_status(operation),
                Ok(OperationStatus::Terminal(TerminalOutcome::TimedOut))
            ),
            "{path}"
        );
        for (signal, _) in &signals[..2] {
            assert_eq!(signal.0.load(Ordering::SeqCst), 1, "{path}");
        }
        assert_eq!(signals[2].0.0.load(Ordering::SeqCst), 0, "{path}");
        assert_eq!(world.activity(unrelated).unwrap(), signals[2].1, "{path}");
        assert!(world.close().native_done);
    }
}
