use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::ptr;
use std::fmt::Debug;
use thread_local::CachedThreadLocal;

pub struct HPBRManager<T: Send + Debug> {
    local_hazards: CachedThreadLocal<Box<Vec<HazardPointer<T>>>>,
    head: AtomicPtr<HazardPointer<T>>
}

struct HazardPointer<T: Send + Debug> {
    protected: T,
    next: AtomicPtr<HazardPointer<T>>,
    active: AtomicBool,
}