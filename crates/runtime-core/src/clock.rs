use crate::Error;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct Clock {
    inner: Arc<ClockState>,
}

enum ClockState {
    System,
    Manual(Mutex<Instant>),
}

#[derive(Clone)]
pub struct VirtualClock {
    clock: Clock,
}

impl Clock {
    /// Creates a system monotonic clock without a mutex on reads.
    pub fn system() -> Self {
        Self {
            inner: Arc::new(ClockState::System),
        }
    }

    pub fn manual_at(initial: Instant) -> (Self, VirtualClock) {
        let clock = Self {
            inner: Arc::new(ClockState::Manual(Mutex::new(initial))),
        };
        (
            clock.clone(),
            VirtualClock {
                clock: clock.clone(),
            },
        )
    }

    pub fn now(&self) -> Instant {
        match self.inner.as_ref() {
            ClockState::System => Instant::now(),
            ClockState::Manual(current) => *current.lock().unwrap(),
        }
    }

    pub fn is_virtual(&self) -> bool {
        matches!(self.inner.as_ref(), ClockState::Manual(_))
    }
}

impl VirtualClock {
    pub fn current(&self) -> Instant {
        self.clock.now()
    }

    pub fn advance(&self, by: Duration) -> Result<Instant, Error> {
        let ClockState::Manual(current) = self.clock.inner.as_ref() else {
            return Err(Error::InvalidConfig);
        };
        let mut current = current.lock().unwrap();
        let next = current.checked_add(by).ok_or(Error::LimitExceeded)?;
        if next < *current {
            return Err(Error::LimitExceeded);
        }
        *current = next;
        Ok(next)
    }
}
