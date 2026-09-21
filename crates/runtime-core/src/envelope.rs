//! Bounded metadata carried with an admitted event.

use crate::{ActorRef, Error, PortRef};
use std::sync::Arc;
use std::time::Instant;

const MAX_BAGGAGE: usize = 8;
const MAX_KEY_BYTES: usize = 64;
const MAX_VALUE_BYTES: usize = 256;
const MAX_BAGGAGE_BYTES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    sampled: bool,
    baggage: Arc<[(String, String)]>,
}

impl TraceContext {
    pub fn new(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        sampled: bool,
        baggage: Vec<(String, String)>,
    ) -> Result<Self, Error> {
        if trace_id.iter().all(|byte| *byte == 0) || span_id.iter().all(|byte| *byte == 0) {
            return Err(Error::InvalidMetadata);
        }
        if baggage.len() > MAX_BAGGAGE {
            return Err(Error::InvalidMetadata);
        }
        let mut normalized = Vec::with_capacity(baggage.len());
        let mut total = 0usize;
        for (key, value) in baggage {
            if key.is_empty()
                || key.len() > MAX_KEY_BYTES
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
                || value.len() > MAX_VALUE_BYTES
            {
                return Err(Error::InvalidMetadata);
            }
            if !normalized
                .iter()
                .all(|(prior, _): &(String, String)| prior != &key)
            {
                return Err(Error::InvalidMetadata);
            }
            let key = String::from(key.as_str());
            let value = String::from(value.as_str());
            total = total
                .checked_add(key.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(Error::InvalidMetadata)?;
            if total > MAX_BAGGAGE_BYTES {
                return Err(Error::InvalidMetadata);
            }
            normalized.push((key, value));
        }
        Ok(Self {
            trace_id,
            span_id,
            sampled,
            baggage: Arc::from(normalized.into_boxed_slice()),
        })
    }

    pub fn trace_id(&self) -> &[u8; 16] {
        &self.trace_id
    }
    pub fn span_id(&self) -> &[u8; 8] {
        &self.span_id
    }
    pub fn sampled(&self) -> bool {
        self.sampled
    }
    pub fn baggage(&self) -> &[(String, String)] {
        &self.baggage
    }
    pub fn charged_bytes(&self) -> usize {
        24 + 1
            + self
                .baggage
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MessageOptions {
    pub source: Option<ActorRef>,
    pub deadline: Option<Instant>,
    pub correlation_id: Option<u64>,
    pub causation_id: Option<u64>,
    pub trace: Option<TraceContext>,
}

impl MessageOptions {
    pub fn validate(&self) -> Result<(), Error> {
        if self.correlation_id == Some(0) || self.causation_id == Some(0) {
            return Err(Error::InvalidMetadata);
        }
        Ok(())
    }

    pub fn charged_bytes(&self) -> usize {
        self.trace.as_ref().map_or(0, TraceContext::charged_bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaKind {
    Pulse,
    CountSnapshot,
    Record,
    Structured,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaIdentity {
    pub kind: SchemaKind,
    pub id: u32,
    pub version: u32,
}

#[derive(Clone, Debug)]
pub struct Envelope {
    pub event_id: u64,
    pub schema: SchemaIdentity,
    pub source: Option<ActorRef>,
    pub destination: ActorRef,
    pub dispatcher: Option<PortRef>,
    pub destination_port: Option<PortRef>,
    pub owner: ActorRef,
    pub enqueued_at: Instant,
    pub deadline: Option<Instant>,
    pub correlation_id: Option<u64>,
    pub causation_id: Option<u64>,
    pub trace: Option<TraceContext>,
}

impl Envelope {
    pub fn child_options(&self, source: ActorRef) -> MessageOptions {
        MessageOptions {
            source: Some(source),
            deadline: self.deadline,
            correlation_id: Some(self.correlation_id.unwrap_or(self.event_id)),
            causation_id: Some(self.event_id),
            trace: self.trace.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> ([u8; 16], [u8; 8]) {
        ([1; 16], [2; 8])
    }

    #[test]
    fn trace_context_validates_and_charges_bounded_baggage() {
        let (trace, span) = ids();
        let context =
            TraceContext::new(trace, span, true, vec![("tenant".into(), "native".into())]).unwrap();
        assert_eq!(context.charged_bytes(), 24 + 1 + 12);
        assert_eq!(context.trace_id(), &trace);
        assert!(TraceContext::new([0; 16], span, false, Vec::new()).is_err());
        assert!(
            TraceContext::new(trace, span, false, vec![("bad/key".into(), "x".into())]).is_err()
        );
    }

    #[test]
    fn baggage_is_unique_and_deeply_owned() {
        let (trace, span) = ids();
        let mut key = String::from("key");
        let mut value = String::from("value");
        let context =
            TraceContext::new(trace, span, false, vec![(key.clone(), value.clone())]).unwrap();
        key.push('x');
        value.push('x');
        assert_eq!(
            context.baggage()[0],
            (String::from("key"), String::from("value"))
        );
        assert!(
            TraceContext::new(
                trace,
                span,
                false,
                vec![("key".into(), "a".into()), ("key".into(), "b".into())]
            )
            .is_err()
        );
    }

    #[test]
    fn options_reject_zero_ids_and_child_propagates_context() {
        let (trace, span) = ids();
        let context = TraceContext::new(trace, span, true, Vec::new()).unwrap();
        assert!(
            MessageOptions {
                correlation_id: Some(0),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        let actor = ActorRef {
            world: 1,
            slot: 2,
            generation: 3,
        };
        let envelope = Envelope {
            event_id: 9,
            schema: SchemaIdentity {
                kind: SchemaKind::Pulse,
                id: 1,
                version: 1,
            },
            source: None,
            destination: actor,
            dispatcher: None,
            destination_port: None,
            owner: actor,
            enqueued_at: Instant::now(),
            deadline: None,
            correlation_id: None,
            causation_id: None,
            trace: Some(context.clone()),
        };
        let child = envelope.child_options(actor);
        assert_eq!(child.correlation_id, Some(9));
        assert_eq!(child.causation_id, Some(9));
        assert_eq!(child.trace, Some(context));
    }
}
