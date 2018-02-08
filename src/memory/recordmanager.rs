use std::sync::atomic::{AtomicPtr};

pub trait RecordManager {
    type Record;
    
    /// Allocates the given Record as an Atomic pointer
    fn allocate(&self, Self::Record) -> AtomicPtr<Self::Record>;
    /// Add a record to the thread-local list of retired data
    fn retire(&self, AtomicPtr<Self::Record>);
    /// Protect a hazardous reference
    fn protect(&self, &AtomicPtr<Self::Record>);
}