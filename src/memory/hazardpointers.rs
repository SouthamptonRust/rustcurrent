use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::fmt::Debug;
use thread_local::CachedThreadLocal;

pub struct HPBRManager<T: Send + Debug> {
    thread_info: CachedThreadLocal<ThreadLocalInfo<T>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize
}

impl<T: Send + Debug> HPBRManager<T> {
    fn new(max_retired: usize) -> Self {
        HPBRManager {
            thread_info: CachedThreadLocal::new(),
            head: AtomicPtr::default(),
            max_retired
        }
    }
}

impl<T: Send + Debug> RecordManager for HPBRManager<T> {
    type Record = T;

    fn allocate(&self, data: Self::Record) -> AtomicPtr<Self::Record> {
        AtomicPtr::default()
    }

    fn retire(&self, record: AtomicPtr<Self::Record>) {

    }

    fn protect(&self, record: AtomicPtr<Self::Record>) {

    }
}

struct HazardPointer<T: Send + Debug> {
    protected: Option<T>,
    next: AtomicPtr<HazardPointer<T>>,
    active: AtomicBool
}

impl<T: Send + Debug> HazardPointer<T> {
    fn new() -> Self {
        HazardPointer {
            protected: None,
            next: AtomicPtr::default(),
            active: AtomicBool::new(false)
        }
    }

    fn protect(&mut self, record: T) {
        self.protected = Some(record);
    }

    fn activate(&self) -> bool {
        self.active.compare_and_swap(false, true, Ordering::AcqRel)
    }
}

struct ThreadLocalInfo<T: Send + Debug> {
    local_hazards: Box<Vec<HazardPointer<T>>>,
    retired_list: Box<Vec<AtomicPtr<T>>>,
    retired_number: usize
}

impl<T: Send + Debug> ThreadLocalInfo<T> {
    fn new() -> Self {
        ThreadLocalInfo {
            local_hazards: Box::new(Vec::new()),
            retired_list: Box::new(Vec::new()),
            retired_number: 0
        }
    }
}