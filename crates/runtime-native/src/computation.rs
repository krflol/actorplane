//! Bounded native CPU request components.

use crate::sdk::{
    NativeBehavior, NativeContext, NativeError, NativeHandle, NativeResult, NativeSpec,
};
use actorplane_core::{
    ActorRef, ComponentDescriptor, InterfaceSpec, Payload, PayloadType, PortDirection, PortSpec,
    schema::Schema,
};

struct SumSquares;

impl NativeBehavior for SumSquares {
    fn on_event(&mut self, payload: &Payload, ctx: &mut NativeContext<'_>) -> NativeResult {
        let input = payload.clone();
        ctx.defer_cpu_reply(input, |held, job| {
            let Payload::Pulse(value) = held.payload() else {
                return Err(NativeError::Application("CpuInputSchema"));
            };
            if !(0..=1_000_000).contains(value) {
                return Err(NativeError::Application("CpuInputRange"));
            }
            let n = *value as u64;
            let mut total = 0i64;
            for index in 1..=n {
                let square = (index * index) as i64;
                total = total
                    .checked_add(square)
                    .ok_or(NativeError::Application("CpuOverflow"))?;
                if index % 1024 == 0 && job.is_cancelled() {
                    return Err(NativeError::Application("CpuCancelled"));
                }
            }
            Ok(Payload::CountSnapshot { count: n, total })
        })
    }
}

fn descriptor() -> ComponentDescriptor {
    let input = PortSpec {
        name: "requests".into(),
        direction: PortDirection::Input,
        schema: PayloadType::Pulse,
    };
    ComponentDescriptor {
        name: "actorplane.CpuSumSquares".into(),
        version: 1,
        ports: vec![input.clone()],
        interfaces: vec![InterfaceSpec {
            name: "actorplane.CpuSumSquares".into(),
            version: 1,
            ports: vec![input],
        }],
    }
}

impl super::NativeRuntime {
    /// Prepare a CPU-backed sum-of-squares component; caller activates its owner.
    pub fn prepare_sum_squares(&self, parent: Option<ActorRef>) -> NativeResult<NativeHandle> {
        if self.world().is_virtual() {
            return Err(NativeError::Application("CpuUnavailableInTestWorld"));
        }
        let spec = NativeSpec::new(
            descriptor(),
            Schema {
                name: "actorplane.CpuSumSquares.Config".into(),
                version: 1,
                fields: vec![],
            },
        );
        self.prepare_native::<SumSquares, _>(
            parent,
            spec,
            actorplane_core::schema::Value::Record(vec![]),
            |_| Ok(SumSquares),
        )
    }
}
