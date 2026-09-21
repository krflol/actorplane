//! Bounded framed TCP request/response transport. One request is outstanding per
//! connection; there is no hidden pending-send queue or interpreter callback.
pub mod framing;
mod transport;

use crate::{
    NativeRuntime,
    sdk::{NativeBehavior, NativeContext, NativeHandle, NativeResult, NativeSpec},
};
use actorplane_core::{
    ActorRef, ComponentDescriptor, Payload, PayloadType, PortDirection, PortSpec, World,
    schema::{Field, FieldType, Schema, Value},
};
use framing::BufferBudget;
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

pub const MAX_FRAME_BYTES: usize = 65_536;

#[derive(Clone, Debug)]
pub struct TcpConfig {
    pub bind: SocketAddr,
    pub max_connections: usize,
    pub max_frame_bytes: usize,
    pub read_buffer_bytes: usize,
    pub write_buffer_bytes: usize,
    pub read_timeout: Duration,
    pub request_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            max_connections: 16,
            max_frame_bytes: 16_384,
            read_buffer_bytes: 1_048_576,
            write_buffer_bytes: 1_048_576,
            read_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(5),
        }
    }
}
impl TcpConfig {
    pub fn validate(&self) -> Result<(), TcpError> {
        if !(1..=1024).contains(&self.max_connections)
            || !(1..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes)
            || !(1..=64 * 1024 * 1024).contains(&self.read_buffer_bytes)
            || !(1..=64 * 1024 * 1024).contains(&self.write_buffer_bytes)
            || [self.read_timeout, self.request_timeout, self.write_timeout]
                .iter()
                .any(|period| period.is_zero() || *period > Duration::from_secs(86400))
        {
            return Err(TcpError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpError {
    InvalidConfig,
    VirtualRuntime,
    Closed,
    TaskLimit,
    Bind(std::io::ErrorKind),
    Core(actorplane_core::Error),
    Schema,
}
impl std::fmt::Display for TcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TcpError {}
impl From<actorplane_core::Error> for TcpError {
    fn from(value: actorplane_core::Error) -> Self {
        Self::Core(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct TcpStats {
    pub accepted_connections: u64,
    pub rejected_connections: u64,
    pub closed_connections: u64,
    pub cancelled_connections: u64,
    pub drained_connections: u64,
    pub frames_received: u64,
    pub requests_admitted: u64,
    pub frames_written: u64,
    pub bytes_received: u64,
    pub bytes_written: u64,
    pub oversized_frames: u64,
    pub truncated_frames: u64,
    pub read_errors: u64,
    pub write_errors: u64,
    pub read_timeouts: u64,
    pub request_timeouts: u64,
    pub write_timeouts: u64,
    pub budget_rejections: u64,
    pub admission_rejections: u64,
    pub failed_requests: u64,
    pub invalid_replies: u64,
    pub listener_errors: u64,
}

struct Shared {
    stats: Mutex<TcpStats>,
    connections: Mutex<BTreeMap<u32, ActorRef>>,
    read_budget: BufferBudget,
    write_budget: BufferBudget,
}
#[derive(Clone)]
pub struct TcpListenerHandle {
    pub owner: ActorRef,
    pub address: SocketAddr,
    shared: Arc<Shared>,
}
impl TcpListenerHandle {
    pub fn stats(&self) -> TcpStats {
        self.shared.stats.lock().unwrap().clone()
    }
    pub fn connections(&self) -> Vec<ActorRef> {
        self.shared
            .connections
            .lock()
            .unwrap()
            .values()
            .copied()
            .collect()
    }
    pub fn read_buffer_bytes(&self) -> usize {
        self.shared.read_budget.used()
    }
    pub fn write_buffer_bytes(&self) -> usize {
        self.shared.write_budget.used()
    }
}

pub fn frame_schema() -> Schema {
    Schema {
        name: "actorplane.TcpFrame".into(),
        version: 1,
        fields: vec![Field {
            name: "data".into(),
            ty: FieldType::Bytes {
                max_bytes: MAX_FRAME_BYTES,
            },
        }],
    }
}
pub fn register_frame(world: &World) -> Result<u32, TcpError> {
    Ok(world
        .register_schemas(vec![frame_schema()])
        .map_err(|_| TcpError::Schema)?[0])
}
/// Canonical native framing event, useful for native clients and contract tests.
pub fn frame_payload(world: &World, schema: u32, data: &[u8]) -> Result<Payload, TcpError> {
    if data.len() > MAX_FRAME_BYTES {
        return Err(TcpError::InvalidConfig);
    }
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(4 + data.len())
        .map_err(|_| TcpError::InvalidConfig)?;
    encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
    encoded.extend_from_slice(data);
    world
        .structured(schema, &encoded)
        .map_err(|_| TcpError::Schema)
}
pub fn frame_bytes(payload: &Payload, schema: u32) -> Result<&[u8], TcpError> {
    let Payload::Structured(record) = payload else {
        return Err(TcpError::Schema);
    };
    if record.schema() != schema || record.bytes().len() < 4 {
        return Err(TcpError::Schema);
    }
    let length = u32::from_le_bytes(record.bytes()[..4].try_into().unwrap()) as usize;
    if length > MAX_FRAME_BYTES || record.bytes().len() != 4 + length {
        return Err(TcpError::Schema);
    }
    Ok(&record.bytes()[4..])
}

struct Echo;
impl NativeBehavior for Echo {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        ctx.reply(payload.clone())?;
        Ok(())
    }
}
impl NativeRuntime {
    /// A real serialized SDK request handler, sharing TcpFrame with Python actors.
    pub fn prepare_tcp_echo(&self, parent: Option<ActorRef>) -> NativeResult<NativeHandle> {
        let schema = register_frame(self.world())
            .map_err(|_| crate::sdk::NativeError::Application("TcpSchema"))?;
        let spec = NativeSpec::new(
            ComponentDescriptor {
                name: "TcpEcho".into(),
                version: 1,
                ports: vec![PortSpec {
                    name: "requests".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Structured(schema),
                }],
                interfaces: vec![],
            },
            Schema {
                name: "TcpEcho.Config".into(),
                version: 1,
                fields: vec![],
            },
        );
        self.prepare_native(parent, spec, Value::Record(vec![]), |_| Ok(Echo))
    }
}
