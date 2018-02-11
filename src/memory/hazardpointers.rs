use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::fmt::Debug;
use thread_local::CachedThreadLocal;
use std::collections::VecDeque;
use std::cell::UnsafeCell;
use std::fmt;

pub struct HPBRManager<'a, T: Send + Debug + 'a> {
    thread_info: CachedThreadLocal<UnsafeCell<ThreadLocalInfo<'a, T>>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize,
    num_hp_per_thread: usize
}

impl<'a, T: Send + Debug + 'a> Debug for HPBRManager<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut thread_info_string = match self.thread_info.get() {
            None => "".to_owned(),
            Some(cell) => {
                let mut result = "".to_owned();
                unsafe {
                    let thread_info = &*cell.get();
                    result = format!("{:?}", thread_info);
                }
                result
            }
        };

        write!(f, "{:?}", &thread_info_string[..])
    }
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
        let old_head = self.head.load(Ordering::Acquire);

        // CAS push the new hazard pointer onto the global list
        // We do not need to worry about freeing as we will not be deleting hazard pointers
        loop {            
            let old_head = self.head.load(Ordering::Acquire);
            unsafe {
                (*new_hp_ptr).next.store(old_head, Ordering::Release);
            }
            if self.head.compare_and_swap(old_head, new_hp_ptr, Ordering::AcqRel) == old_head {
                break;
            }
        }

        new_hp_ptr
    }

    fn retire(&self, record: *mut T) {

    }

    fn protect(&mut self, record: *mut T, hazard_num: usize) {
        let mut thread_info_ptr = self.thread_info.get_or(|| {
            let mut starting_hp: Vec<&'a mut HazardPointer<T>> = Vec::new();
            for _ in 0..self.num_hp_per_thread {
                let hp = self.allocate_hp();
                unsafe {
                    starting_hp.push(&mut (*hp));
                }
            }
            Box::new(UnsafeCell::new(ThreadLocalInfo::new(starting_hp)))
        }).get();

        unsafe {
            let thread_info_mut = &mut *thread_info_ptr;
            thread_info_mut.local_hazards[hazard_num].protect(record);
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
struct ThreadLocalInfo<'a, T: Send + Debug + 'a> {
    local_hazards: Vec<&'a mut HazardPointer<T>>,
    retired_list: Box<VecDeque<*mut T>>,
    retired_number: usize
}

impl<'a, T: Send + Debug> ThreadLocalInfo<'a, T> {
    fn new(starting_hazards: Vec<&'a mut HazardPointer<T>>) -> Self {
        ThreadLocalInfo {
            local_hazards: starting_hazards,
            retired_list: Box::new(VecDeque::new()),
            retired_number: 0
        }
    }
}

mod tests {
    use super::HPBRManager;

    #[test]
    fn test_add_hp() {
        let mut manager : HPBRManager<u8> = HPBRManager::new(100, 2);
        manager.protect(Box::into_raw(Box::new(32)), 1);
        println!("{:?}", manager);

        
    }
}