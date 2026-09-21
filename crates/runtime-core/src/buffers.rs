//! World-scoped native scratch-buffer accounting.
//!
//! Buffer reservations use the same retained-byte counter as held payloads,
//! so temporary native I/O cannot exceed the world's configured budget.

use crate::{Error, World};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
pub struct NativeBufferBudget {
    retained: Arc<AtomicUsize>,
    limit: usize,
}

impl NativeBufferBudget {
    pub(crate) fn new(retained: Arc<AtomicUsize>, limit: usize) -> Self {
        Self { retained, limit }
    }

    pub fn used(&self) -> usize {
        self.retained.load(Ordering::Acquire)
    }

    pub fn reserve(&self, bytes: usize) -> Result<NativeBufferPermit, Error> {
        self.reserve_bytes(bytes)?;
        Ok(NativeBufferPermit {
            budget: self.clone(),
            bytes,
        })
    }

    fn reserve_bytes(&self, bytes: usize) -> Result<(), Error> {
        let mut current = self.retained.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or(Error::BudgetExceeded)?;
            if next > self.limit {
                return Err(Error::BudgetExceeded);
            }
            match self.retained.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(value) => current = value,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.retained.fetch_sub(bytes, Ordering::AcqRel);
    }
}

pub struct NativeBufferPermit {
    budget: NativeBufferBudget,
    bytes: usize,
}

impl NativeBufferPermit {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn try_grow(&mut self, additional: usize) -> Result<(), Error> {
        if additional == 0 {
            return Ok(());
        }
        let total = self
            .bytes
            .checked_add(additional)
            .ok_or(Error::BudgetExceeded)?;
        self.budget.reserve_bytes(additional)?;
        self.bytes = total;
        Ok(())
    }
}

impl Drop for NativeBufferPermit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

impl World {
    pub fn native_buffer_budget(&self) -> NativeBufferBudget {
        NativeBufferBudget::new(
            self.inner.retained.clone(),
            self.inner.cfg.native_payload_budget,
        )
    }
}
