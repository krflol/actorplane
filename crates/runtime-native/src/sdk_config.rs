//! Bounded configuration and validation for source-level native SDK components.
use actorplane_core::schema::{Schema, SchemaError, Value};
use actorplane_core::{ComponentDescriptor, FailureAction, PortDirection};
use std::{error::Error as StdError, fmt, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeLimits {
    pub events_per_turn: usize,
    pub effects_per_callback: usize,
    pub max_jobs: usize,
    pub max_job_input_bytes: usize,
    pub max_config_bytes: usize,
}

impl Default for NativeLimits {
    fn default() -> Self {
        Self {
            events_per_turn: 32,
            effects_per_callback: 64,
            max_jobs: 8,
            max_job_input_bytes: 32768,
            max_config_bytes: 16384,
        }
    }
}

impl NativeLimits {
    pub fn validate(&self) -> Result<(), NativeError> {
        if !(1..=1024).contains(&self.events_per_turn) {
            return Err(NativeError::Limit("events_per_turn"));
        }
        if !(1..=1024).contains(&self.effects_per_callback) {
            return Err(NativeError::Limit("effects_per_callback"));
        }
        if self.max_jobs > 1024 {
            return Err(NativeError::Limit("max_jobs"));
        }
        if self.max_jobs > 0 && self.max_job_input_bytes == 0 {
            return Err(NativeError::Limit("max_job_input_bytes"));
        }
        if self.max_job_input_bytes > 1_048_576 {
            return Err(NativeError::Limit("max_job_input_bytes"));
        }
        if !(1..=65536).contains(&self.max_config_bytes) {
            return Err(NativeError::Limit("max_config_bytes"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainPolicy {
    Producer,
    Consumer,
}

#[derive(Debug)]
pub enum NativeError {
    Core(actorplane_core::Error),
    Schema(SchemaError),
    Limit(&'static str),
    Application(&'static str),
    Panic,
}
impl From<actorplane_core::Error> for NativeError {
    fn from(value: actorplane_core::Error) -> Self {
        Self::Core(value)
    }
}
impl From<SchemaError> for NativeError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
    }
}
impl fmt::Display for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => write!(f, "{error}"),
            Self::Schema(error) => write!(f, "{error}"),
            Self::Limit("runtime tasks") => f.write_str("native task limit reached"),
            Self::Limit(label) => write!(f, "limit exceeded: {label}"),
            Self::Application(code) => f.write_str(code),
            Self::Panic => f.write_str("native panic"),
        }
    }
}
impl StdError for NativeError {}
impl NativeError {
    pub fn code(&self) -> &str {
        match self {
            Self::Core(_) => "CoreError",
            Self::Schema(_) => "SchemaError",
            Self::Limit(_) => "LimitExceeded",
            Self::Application(code) => code,
            Self::Panic => "NativePanic",
        }
    }
}

#[derive(Clone, Debug)]
pub struct NativeSpec {
    pub descriptor: ComponentDescriptor,
    pub configuration: Schema,
    pub limits: NativeLimits,
    pub period: Option<Duration>,
    pub drain: DrainPolicy,
    pub failure_action: FailureAction,
}

impl NativeSpec {
    pub fn new(descriptor: ComponentDescriptor, configuration: Schema) -> Self {
        Self {
            descriptor,
            configuration,
            limits: NativeLimits::default(),
            period: None,
            drain: DrainPolicy::Consumer,
            failure_action: FailureAction::StopActor,
        }
    }

    pub fn validate_config(&self, value: &Value) -> Result<Value, NativeError> {
        self.limits.validate()?;
        if self.drain == DrainPolicy::Producer
            && self
                .descriptor
                .ports
                .iter()
                .any(|port| port.direction == PortDirection::Input)
        {
            return Err(NativeError::Application("ProducerHasInputPorts"));
        }
        if let Some(period) = self.period
            && (period.is_zero() || period > Duration::from_secs(86400))
        {
            return Err(NativeError::Limit("period"));
        }
        self.configuration.validate()?;
        let bytes = self
            .configuration
            .encode(value, self.limits.max_config_bytes)?;
        let decoded = self.configuration.decode(&bytes)?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actorplane_core::schema::{Field, FieldType};

    fn spec() -> NativeSpec {
        let schema = Schema {
            name: "Config".into(),
            version: 1,
            fields: vec![Field {
                name: "count".into(),
                ty: FieldType::Int { min: 0, max: 10 },
            }],
        };
        let descriptor = ComponentDescriptor {
            name: "test".into(),
            version: 1,
            ports: vec![],
            interfaces: vec![],
        };
        NativeSpec::new(descriptor, schema)
    }

    #[test]
    fn defaults_and_limits_are_bounded() {
        assert_eq!(NativeLimits::default().events_per_turn, 32);
        let limits = NativeLimits {
            events_per_turn: 0,
            ..NativeLimits::default()
        };
        assert!(limits.validate().is_err());
        let limits = NativeLimits {
            max_jobs: 1,
            max_job_input_bytes: 0,
            ..NativeLimits::default()
        };
        assert!(limits.validate().is_err());
    }

    #[test]
    fn configuration_is_validated_and_copied() {
        let native = spec();
        let value = Value::Record(vec![Value::Int(4)]);
        let copy = native.validate_config(&value).expect("valid config");
        assert_eq!(copy, value);
        assert!(
            native
                .validate_config(&Value::Record(vec![Value::Int(99)]))
                .is_err()
        );
    }

    #[test]
    fn invalid_schema_and_period_are_rejected() {
        let mut native = spec();
        native.period = Some(Duration::ZERO);
        assert!(
            native
                .validate_config(&Value::Record(vec![Value::Int(1)]))
                .is_err()
        );
    }
}
