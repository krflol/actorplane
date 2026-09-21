use crate::{ActorRef, Clock, FailurePhase, FailureRecord, OperationId};
use std::{collections::VecDeque, sync::Arc, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticCode {
    QueueFull,
    BudgetExceeded,
    ActorNotReady,
    ActorStopped,
    StaleReference,
    CrossWorld,
    LimitExceeded,
    InvalidConfig,
    NotFound,
    DuplicateSubscription,
    SchemaMismatch,
    InvalidPort,
    InterfaceMismatch,
    InvalidMetadata,
    DeadlineExpired,
    HandlerFailed,
    StartupFailed,
    CleanupFailed,
    SupervisorFailed,
    OperationTimedOut,
    OperationLateResult,
    OwnerStopped,
    TargetStopped,
    DrainTimeout,
    Cancelled,
}
impl From<&crate::Error> for DiagnosticCode {
    fn from(error: &crate::Error) -> Self {
        use crate::Error;
        match error {
            Error::QueueFull => Self::QueueFull,
            Error::BudgetExceeded => Self::BudgetExceeded,
            Error::ActorNotReady => Self::ActorNotReady,
            Error::ActorStopped => Self::ActorStopped,
            Error::StaleReference => Self::StaleReference,
            Error::CrossWorld => Self::CrossWorld,
            Error::LimitExceeded => Self::LimitExceeded,
            Error::InvalidConfig => Self::InvalidConfig,
            Error::NotFound => Self::NotFound,
            Error::DuplicateSubscription => Self::DuplicateSubscription,
            Error::SchemaMismatch => Self::SchemaMismatch,
            Error::InvalidPort => Self::InvalidPort,
            Error::InterfaceMismatch => Self::InterfaceMismatch,
            Error::InvalidMetadata => Self::InvalidMetadata,
            Error::DeadlineExpired => Self::DeadlineExpired,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticEntry {
    pub sequence: u64,
    pub elapsed_ns: u64,
    pub code: DiagnosticCode,
    pub actor: Option<ActorRef>,
    pub operation: Option<OperationId>,
    pub event_id: Option<u64>,
    pub correlation_id: Option<u64>,
    pub causation_id: Option<u64>,
    pub failure: Option<Arc<FailureRecord>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiagnosticGap {
    pub first_available: u64,
    pub requested_after: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticRead {
    pub entries: Vec<DiagnosticEntry>,
    pub next_sequence: u64,
    pub dropped: u64,
    pub gap: Option<DiagnosticGap>,
}
pub(crate) struct History {
    capacity: usize,
    next: u64,
    dropped: u64,
    entries: VecDeque<DiagnosticEntry>,
    clock: Clock,
    epoch: Instant,
    exhausted: bool,
}
impl History {
    pub(crate) fn new(capacity: usize, epoch: Instant, clock: Clock) -> Self {
        Self {
            capacity,
            next: 1,
            dropped: 0,
            entries: VecDeque::new(),
            epoch,
            clock,
            exhausted: false,
        }
    }
    pub(crate) fn record(
        &mut self,
        code: DiagnosticCode,
        actor: Option<ActorRef>,
        operation: Option<OperationId>,
    ) {
        self.record_entry(code, actor, operation, None, None);
    }
    pub(crate) fn record_event(
        &mut self,
        code: DiagnosticCode,
        envelope: &crate::Envelope,
        operation: Option<OperationId>,
    ) {
        self.record_entry(
            code,
            Some(envelope.destination),
            operation,
            None,
            Some(envelope),
        );
    }
    pub(crate) fn record_failure(&mut self, failure: Arc<FailureRecord>) {
        let code = match failure.details.phase() {
            FailurePhase::Handler => DiagnosticCode::HandlerFailed,
            FailurePhase::Stop => DiagnosticCode::CleanupFailed,
            FailurePhase::Supervisor => DiagnosticCode::SupervisorFailed,
            _ => DiagnosticCode::StartupFailed,
        };
        self.record_entry(
            code,
            Some(failure.actor),
            failure.operation,
            Some(failure),
            None,
        );
    }
    fn record_entry(
        &mut self,
        code: DiagnosticCode,
        actor: Option<ActorRef>,
        operation: Option<OperationId>,
        failure: Option<Arc<FailureRecord>>,
        envelope: Option<&crate::Envelope>,
    ) {
        if self.capacity == 0 || self.exhausted {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        let sequence = self.next;
        if sequence == u64::MAX {
            self.exhausted = true;
        } else {
            self.next += 1;
        }
        let elapsed_ns = self
            .clock
            .now()
            .saturating_duration_since(self.epoch)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        self.entries.push_back(DiagnosticEntry {
            sequence,
            elapsed_ns,
            code,
            actor,
            operation,
            event_id: envelope
                .map(|value| value.event_id)
                .or_else(|| failure.as_ref().and_then(|value| value.event_id)),
            correlation_id: envelope.and_then(|value| value.correlation_id),
            causation_id: envelope.and_then(|value| value.causation_id),
            failure,
        });
    }
    pub(crate) fn read(&self, after: u64, limit: usize) -> DiagnosticRead {
        let first = self.entries.front().map_or(self.next, |e| e.sequence);
        let gap = (after.saturating_add(1) < first).then_some(DiagnosticGap {
            first_available: first,
            requested_after: after,
        });
        let entries = self
            .entries
            .iter()
            .filter(|e| e.sequence > after)
            .take(limit)
            .cloned()
            .collect();
        DiagnosticRead {
            entries,
            next_sequence: self.next,
            dropped: self.dropped,
            gap,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequence_exhaustion_never_reuses_a_cursor() {
        let mut history = History::new(2, Instant::now(), Clock::system());
        history.next = u64::MAX;
        history.record(DiagnosticCode::QueueFull, None, None);
        history.record(DiagnosticCode::HandlerFailed, None, None);
        assert_eq!(history.entries.len(), 1);
        assert_eq!(history.entries[0].sequence, u64::MAX);
        assert_eq!(history.dropped, 1);
        assert!(history.read(u64::MAX, 2).entries.is_empty());
    }
    #[test]
    fn disabled_history_allocates_no_records_and_counts_drops() {
        let mut history = History::new(0, Instant::now(), Clock::system());
        history.record(DiagnosticCode::QueueFull, None, None);
        assert!(history.entries.is_empty());
        assert_eq!(history.dropped, 1);
    }
}
