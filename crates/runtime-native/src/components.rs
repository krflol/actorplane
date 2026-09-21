use actorplane_core::{
    ActorRef, ComponentDescriptor, InterfaceSpec, Payload, PayloadType, PortDirection, PortRef,
    PortSpec,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug)]
pub enum ComponentKind {
    PulseSource { interval: Duration },
    WindowCounter { window: Duration },
    SnapshotSink,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ComponentStats {
    pub generated: u64,
    pub processed: u64,
    pub summaries: u64,
    pub sink_received: u64,
    pub last_total: i64,
}

#[derive(Clone)]
pub struct NativeComponent {
    pub owner: ActorRef,
    pub ports: Vec<PortRef>,
    kind: ComponentKind,
    stats: Arc<Mutex<ComponentStats>>,
}
impl NativeComponent {
    pub fn stats(&self) -> ComponentStats {
        *self.stats.lock().unwrap()
    }
    pub fn kind(&self) -> &ComponentKind {
        &self.kind
    }
}

fn descriptor(kind: &ComponentKind) -> ComponentDescriptor {
    let (name, ports) = match kind {
        ComponentKind::PulseSource { .. } => (
            "actorplane.PulseSource",
            vec![PortSpec {
                name: "pulses".into(),
                direction: PortDirection::Output,
                schema: PayloadType::Pulse,
            }],
        ),
        ComponentKind::WindowCounter { .. } => (
            "actorplane.WindowCounter",
            vec![
                PortSpec {
                    name: "input".into(),
                    direction: PortDirection::Input,
                    schema: PayloadType::Pulse,
                },
                PortSpec {
                    name: "snapshots".into(),
                    direction: PortDirection::Output,
                    schema: PayloadType::CountSnapshot,
                },
            ],
        ),
        ComponentKind::SnapshotSink => (
            "actorplane.SnapshotSink",
            vec![PortSpec {
                name: "input".into(),
                direction: PortDirection::Input,
                schema: PayloadType::CountSnapshot,
            }],
        ),
    };
    ComponentDescriptor {
        name: name.into(),
        version: 1,
        ports: ports.clone(),
        interfaces: vec![InterfaceSpec {
            name: name.into(),
            version: 1,
            ports,
        }],
    }
}

use crate::sdk::{
    DrainPolicy, NativeBehavior, NativeContext, NativeError, NativeResult, NativeSpec,
};
use actorplane_core::schema::{Field, FieldType, Schema, Value};

struct BuiltinBehavior {
    kind: ComponentKind,
    stats: Arc<Mutex<ComponentStats>>,
    count: u64,
    total: i64,
}
impl BuiltinBehavior {
    fn flush(&mut self, ctx: &mut NativeContext<'_>) -> NativeResult {
        if self.count != 0 {
            ctx.emit(
                "snapshots",
                Payload::CountSnapshot {
                    count: self.count,
                    total: self.total,
                },
            )?;
            let mut stats = self.stats.lock().unwrap();
            stats.summaries = stats.summaries.saturating_add(1);
            stats.last_total = self.total;
            self.count = 0;
            self.total = 0;
        }
        Ok(())
    }
}
impl NativeBehavior for BuiltinBehavior {
    fn on_event(&mut self, payload: &Payload, _ctx: &mut NativeContext<'_>) -> NativeResult {
        match (&self.kind, payload) {
            (ComponentKind::WindowCounter { .. }, Payload::Pulse(value)) => {
                let (count, total) = self
                    .count
                    .checked_add(1)
                    .zip(self.total.checked_add(*value))
                    .ok_or(NativeError::Application("NativeCounterOverflow"))?;
                self.count = count;
                self.total = total;
                let mut stats = self.stats.lock().unwrap();
                stats.processed = stats.processed.saturating_add(1);
                Ok(())
            }
            (ComponentKind::SnapshotSink, Payload::CountSnapshot { .. }) => {
                let mut stats = self.stats.lock().unwrap();
                stats.sink_received = stats.sink_received.saturating_add(1);
                Ok(())
            }
            _ => Err(NativeError::Application("InvalidComponentInput")),
        }
    }
    fn on_tick(&mut self, ctx: &mut NativeContext<'_>) -> NativeResult {
        match self.kind {
            ComponentKind::PulseSource { .. } => {
                ctx.emit("pulses", Payload::Pulse(1))?;
                let mut stats = self.stats.lock().unwrap();
                stats.generated = stats.generated.saturating_add(1);
                Ok(())
            }
            ComponentKind::WindowCounter { .. } => self.flush(ctx),
            ComponentKind::SnapshotSink => Ok(()),
        }
    }
    fn on_drain(&mut self, ctx: &mut NativeContext<'_>) -> NativeResult {
        if matches!(self.kind, ComponentKind::WindowCounter { .. }) {
            self.flush(ctx)?;
        }
        Ok(())
    }
}

impl super::NativeRuntime {
    pub fn prepare_component(
        &self,
        parent: ActorRef,
        kind: ComponentKind,
    ) -> Result<NativeComponent, String> {
        let period = match kind {
            ComponentKind::PulseSource { interval } => Some(interval),
            ComponentKind::WindowCounter { window } => Some(window),
            ComponentKind::SnapshotSink => None,
        };
        if period.is_some_and(|value| value.is_zero() || value > Duration::from_secs(86400)) {
            return Err("component period must be positive and at most 24 hours".into());
        }
        let descriptor = descriptor(&kind);
        let configuration = Schema {
            name: format!("{}.Config", descriptor.name),
            version: 1,
            fields: period.map_or_else(Vec::new, |_| {
                vec![Field {
                    name: "period_ns".into(),
                    ty: FieldType::UInt {
                        min: 1,
                        max: 86_400_000_000_000,
                    },
                }]
            }),
        };
        let config = Value::Record(
            period.map_or_else(Vec::new, |value| vec![Value::UInt(value.as_nanos() as u64)]),
        );
        let mut spec = NativeSpec::new(descriptor, configuration);
        spec.period = period;
        spec.drain = if matches!(kind, ComponentKind::PulseSource { .. }) {
            DrainPolicy::Producer
        } else {
            DrainPolicy::Consumer
        };
        spec.limits.max_jobs = 0;
        spec.limits.max_job_input_bytes = 0;
        let stats = Arc::new(Mutex::new(ComponentStats::default()));
        let behavior = BuiltinBehavior {
            kind: kind.clone(),
            stats: stats.clone(),
            count: 0,
            total: 0,
        };
        let handle = self
            .prepare_native(Some(parent), spec, config, |_| Ok(behavior))
            .map_err(|error| error.to_string())?;
        Ok(NativeComponent {
            owner: handle.owner,
            ports: handle.ports,
            kind,
            stats,
        })
    }
}
