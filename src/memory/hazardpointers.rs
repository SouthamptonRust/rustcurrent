use memory::recordmanager::RecordManager;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::fmt::Debug;
use thread_local::CachedThreadLocal;
use std::collections::{VecDeque, HashSet};
use std::cell::UnsafeCell;
use std::fmt;
use std::ptr;

pub struct HPBRManager<T: Send + Debug> {
    thread_info: CachedThreadLocal<UnsafeCell<ThreadLocalInfo<T>>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize,
    num_hp_per_thread: usize
}

impl<'a, T: Send + Debug + 'a> Debug for HPBRManager<T> {
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

        write!(f, "HPBRManager(\n\tthread_info: {:?}, \n\thead: {:?}, \n\tmax_retired: {:?}", &thread_info_string, self.head, self.max_retired)
    }
}

impl<'a, T: Send + Debug> HPBRManager<T> {
    pub fn new(max_retired: usize, num_hp_per_thread: usize) -> Self {
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

    pub fn retire(&self, record: *mut T, hazard_num: usize) {
        unsafe {
            let thread_info_mut = self.get_mut_thread_info();
            thread_info_mut.get_mut_hazard_pointer(hazard_num).unprotect();
            thread_info_mut.retired_list.push_back(record);
            thread_info_mut.retired_number += 1;

            if thread_info_mut.retired_number > self.max_retired {
                self.scan();
            }
        }
    }

    pub fn protect(&self, record: *mut T, hazard_num: usize) {
        unsafe {
            let thread_info_mut = self.get_mut_thread_info();
            thread_info_mut.get_mut_hazard_pointer(hazard_num).protect(record);
        }
    }

    /// Where the main deletion aspect of the HBPRManager takes place
    /// Deletes any retired nodes of this thread which are not protected by hazard pointers
    fn scan(&self) {
        println!("-------------------------------");
        println!("           SCANNING            ");
        println!("-------------------------------");
        let mut hazard_set: HashSet<*mut T> = HashSet::new();
        let mut current = self.head.load(Ordering::Relaxed);

        // Loop through the hazard list and add all non-nulls to the hazard list
        while !ptr::eq(current, ptr::null()) {
            unsafe {
                let hazard_pointer = &*current;
                if let Some(ptr) = hazard_pointer.protected {
                    hazard_set.insert(ptr);
                }
                current = hazard_pointer.next.load(Ordering::Relaxed);
            }
        }

        // This will store the nodes that cannot yet be deleted
        let mut new_retired_list: VecDeque<*mut T> = VecDeque::new();
        unsafe {
            let thread_info = self.get_mut_thread_info();
            for ptr in thread_info.retired_list.drain(..) {
                if hazard_set.contains(&ptr) {
                    new_retired_list.push_back(ptr);
                } else {
                    Self::free(ptr);
                }
            }
            println!("DELETED: {}", thread_info.retired_number - new_retired_list.len());
            thread_info.retired_number = new_retired_list.len();
            thread_info.retired_list = Box::new(new_retired_list);
        }
        println!("FINISHED SCANNING");
    }

    fn free(garbage: *mut T) {
        // Letting this box go out of scope should call Drop on the garbage
        // Seems to work after very basic
        unsafe {
            let boxed_garbage = Box::from_raw(garbage);
            //println!("Attempting to drop: {:?}", &boxed_garbage);
        }
    }

    /// Get the thread local info described in the paper as a mutable reference.
    /// On first access, will create hazard pointers for the thread and add them
    /// to the central list.
    unsafe fn get_mut_thread_info(&self) -> &mut ThreadLocalInfo<T> {
        // If this is the first time the threadlocal data is being access, create
        // it and allocate new hps
        let thread_info_ptr = self.thread_info.get_or(|| {
            let mut starting_hp: Vec<*mut HazardPointer<T>> = Vec::new();
            for _ in 0..self.num_hp_per_thread {
                let hp = self.allocate_hp();
                starting_hp.push(hp);
            }
            Box::new(UnsafeCell::new(ThreadLocalInfo::new(starting_hp)))
        }).get();

        &mut *thread_info_ptr
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

    fn unprotect(&mut self) {
        self.protected = None;
    }

    fn activate(&self) -> bool {
        self.active.compare_and_swap(false, true, Ordering::AcqRel)
    }
}

impl<T: Debug + Send> Debug for HazardPointer<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let val_string = match self.protected {
            None => format!("protected: None"),
            Some(ptr) => {
                unsafe {
                    let val = &*ptr;
                    format!("protected: {{ Pointer: {:?}, Value: {:?} }}", ptr, val)
                }
            }
        };

        write!(f, "HazardPointer: {{ {}, next: {:?}, active: {:?} }}", &val_string, self.next, self.active)
    }
}

unsafe impl<T: Debug + Send> Send for ThreadLocalInfo<T> {}

#[derive(Debug)]
struct ThreadLocalInfo<T: Send + Debug> {
    local_hazards: Vec<*mut HazardPointer<T>>,
    retired_list: Box<VecDeque<*mut T>>,
    retired_number: usize
}

impl<'a, T: Send + Debug> ThreadLocalInfo<T> {
    fn new(starting_hazards: Vec<*mut HazardPointer<T>>) -> Self {
        ThreadLocalInfo {
            local_hazards: starting_hazards,
            retired_list: Box::new(VecDeque::new()),
            retired_number: 0
        }
    }

    unsafe fn get_mut_hazard_pointer(&mut self, hazard_index: usize) -> &mut HazardPointer<T> {
        &mut *self.local_hazards[hazard_index]
    }
}

mod tests {
    use super::HPBRManager;

    #[derive(Debug)]
    struct Foo {
        data: u8
    }
    
    impl Drop for Foo {
        fn drop(&mut self) {
            println!("Dropping: {:?}", self.data);
        }
    }

    #[test]
    fn test_add_hp() {
        let mut manager : HPBRManager<Foo> = HPBRManager::new(100, 2);
        let test_pointer_one = Box::into_raw(Box::new(Foo {data: 32}));
        let test_pointer_two = Box::into_raw(Box::new(Foo {data: 24}));
        manager.protect(test_pointer_one, 0);
        manager.protect(test_pointer_two, 1);
        println!("{:?}", manager);
        manager.retire(test_pointer_one, 0);
        println!("{:?}", manager);
        manager.scan();

        println!("{:?}", manager);
                
    }
}