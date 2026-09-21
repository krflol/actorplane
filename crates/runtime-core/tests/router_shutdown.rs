use actorplane_core::{Config, EndpointKind, Error, Payload, World};
use std::{
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

struct Probe {
    world: World,
    signal: mpsc::Sender<&'static str>,
}
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        let _ = self.world.snapshot();
        self.signal.send("wake").unwrap();
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.world.snapshot();
        let _ = self.signal.send("drop");
    }
}

#[test]
fn close_releases_idle_observer_and_terminal_poll_retains_no_new_observer() {
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let world = World::new(Config::default()).unwrap();
        let waiter = Waker::from(Arc::new(Probe {
            world: world.clone(),
            signal: tx.clone(),
        }));
        assert!(
            world
                .poll_routing_ready(&mut Context::from_waker(&waiter))
                .is_pending()
        );
        drop(waiter); // Only the World owns this observer, which owns the World.
        assert!(world.close().native_done);
        let terminal = Waker::from(Arc::new(Probe {
            world: world.clone(),
            signal: tx,
        }));
        assert!(matches!(
            world.poll_routing_ready(&mut Context::from_waker(&terminal)),
            Poll::Ready(Err(Error::ActorStopped))
        ));
        drop(terminal); // Must drop immediately, without requiring another close.
    });
    for expected in ["wake", "drop", "drop"] {
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), expected);
    }
    worker.join().unwrap();
}

#[test]
fn draining_world_keeps_router_available_for_owned_completions() {
    let world = World::new(Config::default()).unwrap();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(source).unwrap();
    let task = world.track_task(source).unwrap();
    world.drain_all(world.now() + Duration::from_secs(5));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(world.poll_routing_ready(&mut cx).is_pending());
    let completion = task.publish_completion(source, Payload::Pulse(1)).unwrap();
    assert!(matches!(
        world.poll_routing_ready(&mut cx),
        Poll::Ready(Ok(()))
    ));
    world.route_batch();
    assert!(completion.report().is_some());
    drop(task);
    assert!(world.close().native_done);
    assert!(matches!(
        world.poll_routing_ready(&mut cx),
        Poll::Ready(Err(Error::ActorStopped))
    ));
}

#[test]
fn stop_world_failure_also_terminalizes_routing() {
    use actorplane_core::{FailureAction, FailureDetails, FailurePhase};
    let world = World::new(Config::default()).unwrap();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(source).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(world.poll_routing_ready(&mut cx).is_pending());
    world
        .report_failure(
            source,
            None,
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "router-test", "Failed", vec![]),
            FailureAction::StopWorld,
        )
        .unwrap();
    assert!(matches!(
        world.poll_routing_ready(&mut cx),
        Poll::Ready(Err(Error::ActorStopped))
    ));
}
