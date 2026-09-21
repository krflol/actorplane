use super::*;
use crate::tasks::TaskService;
use actorplane_core::{
    EndpointKind, HeldPayload, Lifecycle, OperationId, OperationStatus, TerminalOutcome,
};
use std::future::Future;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time,
};

impl NativeRuntime {
    /// Bind an owned native listener. A connection permits one framed request
    /// followed by one framed reply; all sockets close on scope cancellation.
    pub fn listen_tcp(
        &self,
        parent: ActorRef,
        target: ActorRef,
        config: TcpConfig,
    ) -> Result<TcpListenerHandle, TcpError> {
        config.validate()?;
        if self.world().is_virtual() {
            return Err(TcpError::VirtualRuntime);
        }
        if config.max_frame_bytes + 8 > self.world().config().max_event_bytes {
            return Err(TcpError::InvalidConfig);
        }
        if !matches!(
            self.world().execution_state(target)?,
            Lifecycle::Starting | Lifecycle::Active
        ) {
            return Err(TcpError::Closed);
        }
        let service = self.task_service().map_err(|_| TcpError::Closed)?;
        let permit = service.permits(1).map_err(|_| TcpError::TaskLimit)?;
        let owner = self.world().allocate(EndpointKind::Native, Some(parent))?;
        let setup = (|| {
            let lease = self.world().track_task(owner)?;
            let startup = self
                .world()
                .claim_native_callback(owner, true)?
                .ok_or(TcpError::Closed)?;
            let schema = register_frame(self.world())?;
            let listener =
                std::net::TcpListener::bind(config.bind).map_err(|e| TcpError::Bind(e.kind()))?;
            listener
                .set_nonblocking(true)
                .map_err(|e| TcpError::Bind(e.kind()))?;
            let address = listener
                .local_addr()
                .map_err(|e| TcpError::Bind(e.kind()))?;
            let runtime = self.runtime.as_ref().ok_or(TcpError::Closed)?;
            let _entered = runtime.enter();
            let listener = TcpListener::from_std(listener).map_err(|e| TcpError::Bind(e.kind()))?;
            let shared = Arc::new(Shared {
                stats: Mutex::new(TcpStats::default()),
                connections: Mutex::new(BTreeMap::new()),
                read_budget: BufferBudget::with_world(config.read_buffer_bytes, self.world())
                    .map_err(|_| TcpError::InvalidConfig)?,
                write_budget: BufferBudget::new(config.write_buffer_bytes)
                    .map_err(|_| TcpError::InvalidConfig)?,
            });
            self.world().activate(owner)?;
            drop(startup);
            Ok::<_, TcpError>((lease, listener, address, shared, schema))
        })();
        let (lease, listener, address, shared, schema) = match setup {
            Ok(value) => value,
            Err(error) => {
                let _ = self.world().stop(owner);
                return Err(error);
            }
        };
        let handle = TcpListenerHandle {
            owner,
            address,
            shared: shared.clone(),
        };
        let worker = service.clone();
        service.spawn(permit, lease, move |_lease| async move {
            listen(worker, owner, target, config, shared, schema, listener).await;
        });
        Ok(handle)
    }
}

/// One observer per endpoint, shared by its sequential socket/operation phases.
async fn changed_out_of(world: &World, owner: ActorRef, allow_drain: bool) {
    loop {
        let Ok(version) = world.activity(owner) else {
            return;
        };
        match world.execution_state(owner) {
            Ok(Lifecycle::Active) => (),
            Ok(Lifecycle::Quiescing) if allow_drain => (),
            _ => return,
        }
        if std::future::poll_fn(|cx| world.poll_activity(owner, version, cx))
            .await
            .is_err()
        {
            return;
        }
        tokio::task::yield_now().await;
    }
}

async fn listen(
    service: TaskService,
    owner: ActorRef,
    target: ActorRef,
    config: TcpConfig,
    shared: Arc<Shared>,
    schema: u32,
    listener: TcpListener,
) {
    let world = service.world().clone();
    if !crate::wait_active(&world, owner).await {
        return;
    }
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let mut turns = 0;
    loop {
        if turns == 16 {
            turns = 0;
            tokio::task::yield_now().await;
        }
        let accepted = tokio::select! {
            biased;
            _ = changed_out_of(&world, owner, false) => break,
            result = listener.accept() => result,
        };
        let (stream, _) = match accepted {
            Ok(value) => value,
            Err(_) => {
                shared.stats.lock().unwrap().listener_errors += 1;
                let _ = world.report_failure(
                    owner,
                    None,
                    None,
                    None,
                    actorplane_core::FailureDetails::new(
                        actorplane_core::FailurePhase::Handler,
                        "tcp_listener",
                        "AcceptFailed",
                        vec![],
                    ),
                    actorplane_core::FailureAction::StopActor,
                );
                break;
            }
        };
        turns += 1;
        let Ok(connection_slot) = slots.clone().try_acquire_owned() else {
            shared.stats.lock().unwrap().rejected_connections += 1;
            continue;
        };
        let Ok(task_slot) = service.permits(1) else {
            shared.stats.lock().unwrap().rejected_connections += 1;
            continue;
        };
        let Ok(connection) = world.allocate(EndpointKind::Native, Some(owner)) else {
            shared.stats.lock().unwrap().rejected_connections += 1;
            continue;
        };
        let lease = match world.track_task(connection).and_then(|lease| {
            world.activate(connection)?;
            Ok(lease)
        }) {
            Ok(lease) => lease,
            Err(_) => {
                let _ = world.stop(connection);
                shared.stats.lock().unwrap().rejected_connections += 1;
                continue;
            }
        };
        shared
            .connections
            .lock()
            .unwrap()
            .insert(connection.slot, connection);
        shared.stats.lock().unwrap().accepted_connections += 1;
        let guard = ConnectionGuard {
            world: world.clone(),
            owner: connection,
            shared: shared.clone(),
            _slot: connection_slot,
        };
        let request_world = world.clone();
        let config = config.clone();
        let shared = shared.clone();
        service.spawn(task_slot, lease, move |_lease| async move {
            let _guard = guard;
            connection_loop(
                &request_world,
                connection,
                target,
                &config,
                &shared,
                schema,
                stream,
            )
            .await;
        });
    }
    // Quiescing closes ingress; existing connection tasks finish their admitted
    // request/reply before stopping. Cancel already fenced their entire subtree.
}

struct ConnectionGuard {
    world: World,
    owner: ActorRef,
    shared: Arc<Shared>,
    _slot: OwnedSemaphorePermit,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let _ = self.world.stop(self.owner);
        self.shared
            .connections
            .lock()
            .unwrap()
            .remove(&self.owner.slot);
        self.shared.stats.lock().unwrap().closed_connections += 1;
    }
}
struct RequestGuard {
    world: World,
    id: OperationId,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let _ = self.world.cancel_operation(self.id);
        let _ = self.world.take_operation(self.id);
    }
}

#[derive(Clone, Copy)]
enum End {
    Cancel,
    Drain,
    Timeout,
}

async fn stage<F: Future>(
    world: &World,
    owner: ActorRef,
    duration: Duration,
    allow_drain: bool,
    future: F,
) -> Result<F::Output, End> {
    tokio::select! {
        biased;
        _ = changed_out_of(world, owner, allow_drain) => {
            if matches!(world.execution_state(owner), Ok(Lifecycle::Quiescing)) { Err(End::Drain) }
            else { Err(End::Cancel) }
        }
        _ = time::sleep(duration) => Err(End::Timeout),
        value = future => Ok(value),
    }
}
fn count_end(shared: &Shared, end: End) {
    let mut stats = shared.stats.lock().unwrap();
    match end {
        End::Cancel => stats.cancelled_connections += 1,
        End::Drain => stats.drained_connections += 1,
        End::Timeout => (),
    }
}
fn count_frame_error(shared: &Shared, error: framing::FrameError) {
    let mut stats = shared.stats.lock().unwrap();
    match error {
        framing::FrameError::TooLarge => stats.oversized_frames += 1,
        framing::FrameError::Truncated => stats.truncated_frames += 1,
        framing::FrameError::BudgetExceeded | framing::FrameError::AllocationFailed => {
            stats.budget_rejections += 1
        }
        _ => stats.read_errors += 1,
    }
}
async fn await_reply(
    world: &World,
    owner: ActorRef,
    id: OperationId,
    deadline: std::time::Instant,
) -> Option<TerminalOutcome> {
    loop {
        if world.now() >= deadline {
            world.maintain(world.now());
        }
        let version = world.activity(owner).ok()?;
        match world.operation_status(id) {
            Ok(OperationStatus::Terminal(_)) => return world.take_operation(id).ok().flatten(),
            Ok(OperationStatus::Pending(_)) => (),
            Err(_) => return None,
        }
        tokio::select! {
            result = std::future::poll_fn(|cx| world.poll_activity(owner, version, cx)) => { result.ok()?; }
            _ = time::sleep_until(time::Instant::from_std(deadline)) => (),
        }
        tokio::task::yield_now().await;
    }
}

async fn connection_loop(
    world: &World,
    owner: ActorRef,
    target: ActorRef,
    config: &TcpConfig,
    shared: &Shared,
    schema: u32,
    mut stream: TcpStream,
) {
    loop {
        let input = match stage(
            world,
            owner,
            config.read_timeout,
            false,
            framing::read_frame(&mut stream, &shared.read_budget, config.max_frame_bytes),
        )
        .await
        {
            Ok(Ok(Some(input))) => input,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                count_frame_error(shared, error);
                break;
            }
            Err(end) => {
                count_end(shared, end);
                if matches!(end, End::Timeout) {
                    shared.stats.lock().unwrap().read_timeouts += 1;
                }
                break;
            }
        };
        {
            let mut stats = shared.stats.lock().unwrap();
            stats.frames_received += 1;
            stats.bytes_received += input.bytes().len() as u64;
        }
        // Reserve canonical scratch and the under-construction payload before
        // either allocation. On admission core payload accounting takes over.
        let scratch = match shared.read_budget.reserve(2 * (input.bytes().len() + 4)) {
            Ok(permit) => permit,
            Err(error) => {
                count_frame_error(shared, error);
                break;
            }
        };
        let payload = match frame_payload(world, schema, input.bytes()) {
            Ok(payload) => payload,
            Err(_) => {
                shared.stats.lock().unwrap().admission_rejections += 1;
                break;
            }
        };
        let deadline = world.now() + config.request_timeout;
        let id = match world.request(owner, target, payload, deadline) {
            Ok(id) => id,
            Err(_) => {
                shared.stats.lock().unwrap().admission_rejections += 1;
                break;
            }
        };
        drop(scratch);
        drop(input);
        shared.stats.lock().unwrap().requests_admitted += 1;
        let request_guard = RequestGuard {
            world: world.clone(),
            id,
        };
        let result = tokio::select! {
            biased;
            _ = changed_out_of(world, owner, true) => Err(End::Cancel),
            value = await_reply(world, owner, id, deadline) => Ok(value),
        };
        let reply: HeldPayload = match result {
            Ok(Some(TerminalOutcome::Completed(reply))) => reply,
            Ok(Some(TerminalOutcome::TimedOut)) => {
                shared.stats.lock().unwrap().request_timeouts += 1;
                break;
            }
            Ok(_) => {
                shared.stats.lock().unwrap().failed_requests += 1;
                break;
            }
            Err(end) => {
                count_end(shared, end);
                if matches!(end, End::Timeout) {
                    world.maintain(world.now());
                    shared.stats.lock().unwrap().request_timeouts += 1;
                }
                break;
            }
        };
        drop(request_guard);
        let output = match frame_bytes(reply.payload(), schema) {
            Ok(bytes) if bytes.len() <= config.max_frame_bytes => bytes,
            _ => {
                shared.stats.lock().unwrap().invalid_replies += 1;
                break;
            }
        };
        let output_permit = match shared.write_budget.reserve(output.len()) {
            Ok(permit) => permit,
            Err(_) => {
                shared.stats.lock().unwrap().budget_rejections += 1;
                break;
            }
        };
        // Claim the external write before its stop fence. A claimed write may
        // have partially reached the peer when cancelled; always close then.
        let claim = world
            .claim_native_callback(owner, false)
            .ok()
            .flatten()
            .or_else(|| world.claim_native_drain_callback(owner).ok().flatten());
        let Some(claim) = claim else {
            shared.stats.lock().unwrap().cancelled_connections += 1;
            break;
        };
        let result = stage(
            world,
            owner,
            config.write_timeout,
            true,
            framing::write_frame(&mut stream, output, config.max_frame_bytes),
        )
        .await;
        match result {
            Ok(Ok(())) => {
                let mut stats = shared.stats.lock().unwrap();
                stats.frames_written += 1;
                stats.bytes_written += output.len() as u64;
            }
            Ok(Err(_)) => {
                shared.stats.lock().unwrap().write_errors += 1;
                break;
            }
            Err(end) => {
                count_end(shared, end);
                if matches!(end, End::Timeout) {
                    shared.stats.lock().unwrap().write_timeouts += 1;
                }
                break;
            }
        }
        drop(claim);
        drop(output_permit);
        drop(reply);
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn write_deadline_drops_partial_frame_without_an_unbounded_pending_sender() {
        let world = World::new(actorplane_core::Config::default()).unwrap();
        let owner = world.allocate(EndpointKind::Native, None).unwrap();
        world.activate(owner).unwrap();
        let budget = BufferBudget::new(8).unwrap();
        let permit = budget.reserve(8).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let result = stage(
            &world,
            owner,
            Duration::from_millis(5),
            true,
            framing::write_frame(&mut writer, b"12345678", 8),
        )
        .await;
        assert!(matches!(result, Err(End::Timeout)));
        // A byte reached the peer; the only safe reuse policy is connection close.
        drop(writer);
        drop(permit);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, [0]);
        assert_eq!(budget.used(), 0);
        world.stop(owner).unwrap();
    }
}
