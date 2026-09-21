//! Bounded, externally completed native request handlers for deterministic tests.

use crate::NativeRuntime;
use crate::sdk::{NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec};
use actorplane_core::{
    ActorRef, ComponentDescriptor, HeldPayload, OperationId, OperationStatus, Payload, PayloadType,
    PortDirection, PortSpec, World,
    schema::{Schema, Value},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

struct Entry {
    owner: ActorRef,
    sender: oneshot::Sender<HeldPayload>,
}

struct Registry {
    limit: usize,
    entries: Mutex<HashMap<OperationId, Entry>>,
}

/// A bounded registry of native requests whose results are supplied by a test
/// driver or external deterministic harness.
#[derive(Clone)]
pub struct ControlledReplies {
    registry: Arc<Registry>,
}

impl ControlledReplies {
    pub fn new(limit: usize) -> NativeResult<Self> {
        if !(1..=65_536).contains(&limit) {
            return Err(NativeError::Limit("controlled replies"));
        }
        Ok(Self {
            registry: Arc::new(Registry {
                limit,
                entries: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Prepare an actual native component. Its input port is named `requests`.
    pub fn prepare(
        &self,
        runtime: &NativeRuntime,
        parent: Option<ActorRef>,
        input: PayloadType,
    ) -> NativeResult<crate::sdk::NativeHandle> {
        let descriptor = ComponentDescriptor {
            name: "ControlledReplies".into(),
            version: 1,
            // This is an internal test responder. Its input schema is carried
            // by the port and must not create a globally colliding interface
            // identity for each responder instance.
            interfaces: Vec::new(),
            ports: vec![PortSpec {
                name: "requests".into(),
                direction: PortDirection::Input,
                schema: input,
            }],
        };
        let mut spec = NativeSpec::new(
            descriptor,
            Schema {
                name: "ControlledReplies.Config".into(),
                version: 1,
                fields: Vec::new(),
            },
        );
        spec.limits.max_jobs = self.registry.limit.min(1024);
        runtime.prepare_native::<ControlledBehavior, _>(
            parent,
            spec,
            Value::Record(Vec::new()),
            |_| {
                Ok(ControlledBehavior {
                    registry: self.registry.clone(),
                })
            },
        )
    }

    pub fn pending(&self) -> Vec<OperationId> {
        let mut ids: Vec<_> = self
            .registry
            .entries
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect();
        ids.sort_by_key(|id| (id.world, id.slot, id.generation));
        ids
    }

    /// Complete a pending request through its real deferred-reply channel.
    /// Returns false for unknown, stale, or already-terminal requests.
    pub fn complete(&self, world: &World, id: OperationId, payload: Payload) -> NativeResult<bool> {
        if world.id() != id.world {
            return Err(NativeError::Core(actorplane_core::Error::CrossWorld));
        }
        if !matches!(world.operation_status(id), Ok(OperationStatus::Pending(_))) {
            return Ok(false);
        }
        {
            let entries = self.registry.entries.lock().unwrap();
            let Some(entry) = entries.get(&id) else {
                return Ok(false);
            };
            if entry.owner.world != world.id() {
                return Err(NativeError::Core(actorplane_core::Error::CrossWorld));
            }
        }
        let held = world.hold(payload)?;
        let sender = {
            let mut entries = self.registry.entries.lock().unwrap();
            let Some(entry) = entries.get(&id) else {
                drop(held);
                return Ok(false);
            };
            if entry.owner.world != world.id() {
                drop(held);
                return Err(NativeError::Core(actorplane_core::Error::CrossWorld));
            }
            entries.remove(&id).map(|entry| entry.sender).unwrap()
        };
        match sender.send(held) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn register(
        &self,
        id: OperationId,
        owner: ActorRef,
        sender: oneshot::Sender<HeldPayload>,
    ) -> bool {
        let mut entries = self.registry.entries.lock().unwrap();
        if entries.len() >= self.registry.limit || entries.contains_key(&id) {
            return false;
        }
        entries.insert(id, Entry { owner, sender });
        true
    }

    fn remove(&self, id: OperationId) {
        let sender = self
            .registry
            .entries
            .lock()
            .unwrap()
            .remove(&id)
            .map(|entry| entry.sender);
        drop(sender);
    }
}

struct ControlledBehavior {
    registry: Arc<Registry>,
}

struct CleanupGuard {
    registry: ControlledReplies,
    id: OperationId,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.registry.remove(self.id);
    }
}

impl NativeBehavior for ControlledBehavior {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let id = ctx
            .operation()
            .ok_or(NativeError::Application("ControlledRequestRequired"))?;
        let (sender, receiver) = oneshot::channel();
        let registry = ControlledReplies {
            registry: self.registry.clone(),
        };
        if !registry.register(id, ctx.owner(), sender) {
            return Err(NativeError::Limit("controlled replies"));
        }
        // Move cleanup into the deferred factory closure itself. If admission
        // fails before the future is created, dropping this closure still
        // removes the registry entry.
        let cleanup = CleanupGuard {
            registry: registry.clone(),
            id,
        };
        let result = ctx.defer_reply(payload.clone(), move |input, _job| async move {
            let _cleanup = cleanup;
            let _input = input;
            match receiver.await {
                Ok(response) => Ok(response.payload().clone()),
                Err(_) => Err(NativeError::Application("ControlledReplyCancelled")),
            }
        });
        if result.is_err() {
            registry.remove(id);
        }
        result
    }
}
