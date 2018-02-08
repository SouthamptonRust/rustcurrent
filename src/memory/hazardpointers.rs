use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::fmt::Debug;
use thread_local::CachedThreadLocal;
use std::collections::VecDeque;

pub struct HPBRManager<'a, T: Send + Debug + 'a> {
    thread_info: CachedThreadLocal<ThreadLocalInfo<'a, T>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize
}

impl<'a, T: Send + Debug> HPBRManager<'a, T> {
    fn new(max_retired: usize) -> Self {
        HPBRManager {
            thread_info: CachedThreadLocal::new(),
            head: AtomicPtr::default(),
            max_retired
        }
    }

    fn allocate(&self, data: T) -> AtomicPtr<T> {
        AtomicPtr::new(Box::into_raw(Box::new(data)))
    }

    fn retire(&self, record: *mut T) {

    }

    fn protect(&self, record: *mut T, hazard_num: usize) {
        let thread_info = self.thread_info.get_or(|| Box::new(ThreadLocalInfo::new()));

    }
}

struct HazardPointer<T: Send + Debug> {
    protected: Option<*mut T>,
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

    fn protect(&mut self, record: *mut T) {
        self.protected = Some(record);
    }

    fn activate(&self) -> bool {
        self.active.compare_and_swap(false, true, Ordering::AcqRel)
    }
}

unsafe impl<'a, T: Debug + Send + 'a> Send for ThreadLocalInfo<'a, T> {}

struct ThreadLocalInfo<'a, T: Send + Debug + 'a> {
    local_hazards: Box<Vec<&'a mut HazardPointer<T>>>,
    retired_list: Box<VecDeque<AtomicPtr<T>>>,
    retired_number: usize
}

impl<'a, T: Send + Debug> ThreadLocalInfo<'a, T> {
    fn new() -> Self {
        ThreadLocalInfo {
            local_hazards: Box::new(Vec::new()),
            retired_list: Box::new(VecDeque::new()),
            retired_number: 0
        }
    }
}