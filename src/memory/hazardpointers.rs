use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::fmt::Debug;
use thread_local::CachedThreadLocal;
use std::collections::VecDeque;

pub struct HPBRManager<'a, T: Send + Debug + 'a> {
    thread_info: CachedThreadLocal<ThreadLocalInfo<'a, T>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize,
    num_hp_per_thread: usize
}

impl<'a, T: Send + Debug> HPBRManager<'a, T> {
    fn new(max_retired: usize, num_hp_per_thread: usize) -> Self {
        HPBRManager {
            thread_info: CachedThreadLocal::new(),
            head: AtomicPtr::default(),
            max_retired,
            num_hp_per_thread
        }
    }

    fn allocate(&self, data: T) -> AtomicPtr<T> {
        AtomicPtr::new(Box::into_raw(Box::new(data)))
    }

    fn allocate_hp(&self) -> *mut HazardPointer<T> {
        let mut new_hp = HazardPointer::new();
        let new_hp_ptr =  Box::into_raw(Box::new(new_hp));
        let old_head = self.head.load(Ordering::AcqRel);

        loop {
            if self.head.compare_and_swap(old_head, new_hp_ptr, Ordering::AcqRel) == old_head {
                break;
            }
        }

        new_hp_ptr
    }

    fn retire(&self, record: *mut T) {

    }

    fn protect(&self, record: *mut T, hazard_num: usize) {
        let thread_info = self.thread_info.get_or(|| {
            let mut starting_hp: Vec<&'a mut HazardPointer<T>> = Vec::new();
            for _ in 0..self.num_hp_per_thread {
                let hp = self.allocate_hp();
                unsafe {
                    starting_hp.push(&mut (*hp))
                }
            }
            Box::new(ThreadLocalInfo::new(self.num_hp_per_thread))
        });

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
    retired_list: Box<VecDeque<*mut T>>,
    retired_number: usize
}

impl<'a, T: Send + Debug> ThreadLocalInfo<'a, T> {
    fn new(num_hp: usize) -> Self {
        let info = ThreadLocalInfo {
            local_hazards: Box::new(Vec::new()),
            retired_list: Box::new(VecDeque::new()),
            retired_number: 0
        };
        for i in 0..num_hp {
        }

        info
    }
}