use actorplane_core::{Config, Error, Payload, PayloadType, StopReport, World};
pub use actorplane_native::testing::PumpReport;
use actorplane_native::{NativeRuntime, controls::ControlledReplies, sdk::NativeHandle};
use std::time::Duration;

/// A deterministic facade over the production virtual native runtime. Pumping
/// is cooperative and bounded by `max_steps`; advancing jumps virtual time
/// only after ready work is drained. `send_batch` performs no implicit pump.
pub struct TestWorld {
    world: World,
    runtime: NativeRuntime,
    controlled: ControlledReplies,
    max_steps: u64,
}

impl TestWorld {
    pub fn new(config: Config, max_tasks: usize, max_steps: u64) -> Result<Self, String> {
        if !(1..=1_000_000).contains(&max_steps) {
            return Err("max_steps must be in 1..1000000".into());
        }
        let runtime = NativeRuntime::new_virtual(config, max_tasks)?;
        let world = runtime.world().clone();
        let controlled = ControlledReplies::new(max_tasks).map_err(|error| error.to_string())?;
        Ok(Self {
            world,
            runtime,
            controlled,
            max_steps,
        })
    }

    pub fn world(&self) -> &World {
        &self.world
    }
    pub fn runtime(&self) -> &NativeRuntime {
        &self.runtime
    }
    pub fn controlled(&self) -> &ControlledReplies {
        &self.controlled
    }

    pub fn run_until_idle(&self) -> Result<PumpReport, String> {
        let report = self.runtime.pump_virtual(self.max_steps)?;
        if !report.idle {
            return Err("pump budget exhausted before idle".into());
        }
        Ok(report)
    }

    pub fn advance(&self, duration: Duration) -> Result<PumpReport, String> {
        if duration > Duration::from_secs(86_400) {
            return Err("virtual advance exceeds 24 hours".into());
        }
        let first = self.runtime.pump_virtual(self.max_steps)?;
        if !first.idle {
            return Err("pump budget exhausted before advance".into());
        }
        let remaining = self.max_steps.saturating_sub(first.polls);
        if remaining == 0 {
            return Err("pump budget exhausted before advance".into());
        }
        let report = self.runtime.advance_virtual(duration, remaining)?;
        if !report.idle {
            return Err("pump budget exhausted after advance".into());
        }
        Ok(PumpReport {
            polls: first.polls + report.polls,
            idle: true,
        })
    }

    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.world.elapsed_ns())
    }

    pub fn native_responder(
        &mut self,
        parent: Option<actorplane_core::ActorRef>,
        input: PayloadType,
    ) -> Result<NativeHandle, String> {
        let handle = self
            .controlled
            .prepare(&self.runtime, parent, input)
            .map_err(|error| error.to_string())?;
        if let Err(error) = self.world.activate(handle.owner) {
            let _ = self.world.stop(handle.owner);
            return Err(error.to_string());
        }
        Ok(handle)
    }

    /// Admit in slice order without dispatching between entries. Each result
    /// describes independent admission, not processing or an atomic batch.
    /// The caller owns the slice and result-vector size; mailbox bounds apply.
    pub fn send_batch(
        &self,
        batch: &[(actorplane_core::ActorRef, Payload)],
    ) -> Vec<Result<u64, Error>> {
        batch
            .iter()
            .map(|(target, payload)| self.world.send(*target, payload.clone()))
            .collect()
    }

    pub fn close(&mut self, timeout: Duration) -> Result<StopReport, String> {
        self.runtime.close(timeout)
    }
}
