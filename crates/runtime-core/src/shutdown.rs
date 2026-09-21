//! Read-only shutdown accounting. Reports retain at most one bounded failure.
use super::*;
use std::collections::HashSet;

pub(crate) fn keep_latest(
    current: &mut Option<Arc<FailureRecord>>,
    candidate: Option<&Arc<FailureRecord>>,
) {
    if let Some(candidate) = candidate
        && current
            .as_ref()
            .is_none_or(|old| old.sequence < candidate.sequence)
    {
        *current = Some(candidate.clone());
    }
}

impl StopReport {
    fn add_actor(&mut self, actor: &Actor) {
        self.queued += actor.queue.len() + actor.staged.len();
        self.delivery_in_flight += usize::from(actor.in_flight);
        self.control_in_flight += usize::from(actor.control_in_flight);
        self.native_tasks += actor.native_tasks;
        self.python_pending += usize::from(!actor.python_done);
        self.pending_notifications += usize::from(actor.notification.is_some());
        self.errors = self.errors.saturating_add(actor.errors);
        keep_latest(&mut self.last_error, actor.last_error.as_ref());
        self.timed_out |= actor.drain_timed_out;
    }

    fn finish_counts(&mut self) {
        self.in_flight = self.delivery_in_flight + self.control_in_flight + self.native_tasks;
        self.native_done = self.queued == 0
            && self.in_flight == 0
            && self.pending_notifications == 0
            && self.outstanding_operations == 0;
        self.native_done &= self.outstanding_publications == 0;
        self.python_done = self.python_pending == 0;
    }
}

impl World {
    /// Snapshot an ownership scope without requesting or advancing shutdown.
    /// `discarded` is zero: it counts work discarded by a mutating stop call.
    pub fn stop_report(&self, reference: ActorRef) -> Result<StopReport, Error> {
        self.report_locked(reference, &self.inner.state.lock().unwrap())
    }

    pub fn shutdown_report(&self) -> StopReport {
        self.world_report_locked(&self.inner.state.lock().unwrap())
    }

    pub(crate) fn report_locked(
        &self,
        reference: ActorRef,
        state: &State,
    ) -> Result<StopReport, Error> {
        let mut report = StopReport::default();
        let mut pending = vec![reference];
        let mut scope = HashSet::new();
        while let Some(reference) = pending.pop() {
            let actor = self.check_ref(reference, state)?;
            scope.insert(reference);
            report.add_actor(actor);
            pending.extend(actor.children.iter().copied());
        }
        (report.outstanding_operations, report.retained_operations) =
            state.operations.counts_for_scope(&scope);
        (
            report.outstanding_publications,
            report.retained_publications,
        ) = state.publications.counts_for_scope(&scope);
        report.finish_counts();
        Ok(report)
    }

    pub(crate) fn world_report_locked(&self, state: &State) -> StopReport {
        let mut report = StopReport::default();
        for actor in state.actors.iter().flatten() {
            report.add_actor(actor);
        }
        // Retired generations may have been reused; World totals do not depend
        // on which actor slots remain, or on diagnostic history capacity.
        report.errors = state.metrics.failures;
        report.last_error = state.last_error.clone();
        report.timed_out = state.metrics.drain_timeouts != 0;
        report.outstanding_operations = state.operations.pending_count();
        report.retained_operations = state.operations.retained_terminal_count();
        report.outstanding_publications = state.publications.pending();
        report.retained_publications = state.publications.retained();
        report.finish_counts();
        report
    }
}
