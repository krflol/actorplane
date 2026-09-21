use actorplane_core::{
    ComponentDescriptor, Config, EndpointKind, Error, Payload, PayloadType, PortDirection,
    PortSpec, World,
};
mod common;
use common::route;
use std::{
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
};

struct Signal(mpsc::Sender<()>);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn active(world: &World) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

#[test]
fn activity_is_ready_after_activation_and_parent_activation() {
    let world = World::new(Config::default()).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let child = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    let observed = world.activity(child).unwrap();
    let (_tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(_tx)));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        world.poll_activity(child, observed, &mut cx),
        Poll::Pending
    ));
    world.activate(parent).unwrap();
    assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok());
    assert!(matches!(
        world.poll_activity(child, observed, &mut cx),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn activity_wakes_on_admission_and_finish_but_not_maintain() {
    let world = World::new(Config::default()).unwrap();
    let target = active(&world);
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(tx)));
    let mut cx = Context::from_waker(&waker);
    let observed = world.activity(target).unwrap();
    assert!(matches!(
        world.poll_activity(target, observed, &mut cx),
        Poll::Pending
    ));
    world.maintain(std::time::Instant::now());
    assert!(rx.try_recv().is_err());
    world.send(target, Payload::Pulse(1)).unwrap();
    assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok());
    let observed = world.activity(target).unwrap();
    let delivery = world.claim(target).unwrap().unwrap();
    delivery.finish(true);
    assert!(matches!(
        world.poll_activity(target, observed, &mut cx),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn stopped_actor_completes_observer_and_stale_reference_errors() {
    let world = World::new(Config::default()).unwrap();
    let actor = active(&world);
    let observed = world.activity(actor).unwrap();
    world.stop(actor).unwrap();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        world.poll_activity(actor, observed, &mut cx),
        Poll::Ready(Ok(_))
    ));
    let stale = actorplane_core::ActorRef {
        generation: actor.generation + 1,
        ..actor
    };
    assert_eq!(world.activity(stale), Err(Error::StaleReference));
}

#[test]
fn typed_port_admission_and_drain_all_wake_observer() {
    let world = World::new(Config::default()).unwrap();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    let output = world
        .register_component(
            source,
            ComponentDescriptor {
                name: "source".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "out".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::Pulse,
                }],
                interfaces: Vec::new(),
            },
        )
        .unwrap()[0];
    let input = world
        .register_component(
            target,
            ComponentDescriptor {
                name: "target".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "in".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                }],
                interfaces: Vec::new(),
            },
        )
        .unwrap()[0];
    world.activate(source).unwrap();
    world.activate(target).unwrap();
    world.link(source, output, input).unwrap();
    let observed = world.activity(target).unwrap();
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(tx)));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        world.poll_activity(target, observed, &mut cx),
        Poll::Pending
    ));
    route(
        &world,
        world.publish_port(output, Payload::Pulse(1)).unwrap(),
    );
    assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok());
    let observed = world.activity(target).unwrap();
    assert!(matches!(
        world.poll_activity(target, observed, &mut cx),
        Poll::Pending
    ));
    world.drain_all(std::time::Instant::now() + std::time::Duration::from_secs(1));
    assert!(rx.recv_timeout(std::time::Duration::from_secs(1)).is_ok());
}

#[test]
fn replacing_listener_drops_it_outside_world_locks() {
    struct ReentrantDrop {
        world: World,
        done: mpsc::Sender<()>,
    }
    impl Wake for ReentrantDrop {
        fn wake(self: Arc<Self>) {
            let _ = self.world.snapshot();
        }
    }
    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            let _ = self.world.snapshot();
            let _ = self.done.send(());
        }
    }
    let world = World::new(Config::default()).unwrap();
    let actor = active(&world);
    let observed = world.activity(actor).unwrap();
    let (done, received) = mpsc::channel();
    std::thread::spawn(move || {
        let waker = Waker::from(Arc::new(ReentrantDrop {
            world: world.clone(),
            done,
        }));
        assert!(
            world
                .poll_activity(actor, observed, &mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker); // The registry now owns the last reference.
        assert!(
            world
                .poll_activity(actor, observed, &mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    });
    received
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("listener disposal re-entered a locked World");
}

#[test]
fn waking_can_reenter_world_and_panics_do_not_skip_other_listeners() {
    struct ReentrantWake {
        world: World,
        done: mpsc::Sender<()>,
    }
    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            let _ = self.world.snapshot();
            let _ = self.done.send(());
        }
    }
    struct Panics;
    impl Wake for Panics {
        fn wake(self: Arc<Self>) {
            panic!("listener panic");
        }
    }
    let world = World::new(Config::default()).unwrap();
    let first = active(&world);
    let second = active(&world);
    let source = active(&world);
    world.subscribe(source, first).unwrap();
    world.subscribe(source, second).unwrap();
    let (done, received) = mpsc::channel();
    let (admitted, result) = mpsc::channel();
    let route_world = world.clone();
    std::thread::spawn(move || {
        for (actor, waker) in [
            (first, Waker::from(Arc::new(Panics))),
            (
                second,
                Waker::from(Arc::new(ReentrantWake {
                    world: world.clone(),
                    done,
                })),
            ),
        ] {
            let observed = world.activity(actor).unwrap();
            assert!(
                world
                    .poll_activity(actor, observed, &mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        admitted
            .send(world.publish(source, Payload::Pulse(1)))
            .unwrap();
    });
    let ticket = result
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let report = route(&route_world, ticket);
    received
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("wake re-entered a locked World or was skipped");
    assert_eq!(report.admitted, 2);
}
