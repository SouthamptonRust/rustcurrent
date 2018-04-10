use std::sync::atomic::{AtomicPtr, Ordering, AtomicBool};
use std::sync::atomic;
use std::fmt::Debug;
use thread_local::CachedThreadLocal;
use std::collections::{VecDeque, HashSet};
use std::cell::UnsafeCell;
use std::fmt;
use std::ptr;
use std::mem;

/// A Hazard Pointer based memory manager for use in lock-free data structures.
///
/// This is an implementation of a Hazard Pointer Based Reclamation Manager, based on 
/// the papers [Hazard Pointers: Safe Memory Reclamation for Lock-Free Objects]
/// (https://dl.acm.org/citation.cfm?id=987595) and [Safe Memory Reclamation for Dynamic
/// Lock-Free Objects Using Atomic Reads and Writes](https://dl.acm.org/citation.cfm?id=571829).
///
/// When a thread needs to read a memory address which could potentially be freed by
/// another thread, it protects it using this manager's `protect` function. This ensures
/// that it cannot be deleted, which would cause errors or inconsistency. 
///
/// When a thread wishes to free memory, it calls the `retire` function, which adds the
/// address to its free list. Once the size of a thread's free list exceeds a certain
/// number, the thread becomes responsible for garbage collection, scanning through the 
/// hazard pointers of all threads. If an address is not protected by any hazard pointers, 
/// it can be freed, otherwise it is kept in the free list until the next collection.
///
/// If a thread is exiting the structure for good, it can call the `unprotect` function
/// to clear one of its hazard pointers. This stops the resources it was protecting
/// from never being freed.
///
/// Since deletion is performed by each thread individually, it is impossible for a panicking
/// thread to lock up the entire memory manager. This guarantees that even if a thread panics,
/// all resources will be freed other than those in the free list of that thread,
/// and those protected by its hazard pointers.
///
/// Hazard Pointers are stored in a thread-local data structure and pointed to from a global
/// linked list. They are initialised the first time a thread tries to protect a record. The
/// optimisations provided by the `thread_local` crate ensure that a thread's access to its own
/// hazard pointers is of the order of nanoseconds, so there should be no performance hit. 
///
/// Records are freed by reclaiming `Box` ownership, so the manager should be used with raw pointers
/// created through the `Box::into_raw()` function.
pub struct HPBRManager<T: Send> {
    thread_info: CachedThreadLocal<UnsafeCell<ThreadLocalInfo<T>>>,
    head: AtomicPtr<HazardPointer<T>>,
    max_retired: usize,
    num_hp_per_thread: usize
}

impl<'a, T: Send + Debug + 'a> Debug for HPBRManager<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let thread_info_string = match self.thread_info.get() {
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

impl<'a, T: Send> HPBRManager<T> {
    /// Create a new HPBRManager with a maximum number of records to keep in the free list
    /// and the number of hazard pointers to create for each thread.
    /// # Examples
    /// ```
    /// let manager: HBPRManager<*mut u8> = HPBRManager::new(100, 1);
    /// ``` 
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
        let new_hp = HazardPointer::new();
        let new_hp_ptr =  Box::into_raw(Box::new(new_hp));

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

    /// Retire the given record, which was protected inside the given hazard pointer. If the 
    /// number of records in the free list is bigger than the maximum allowed, this call will trigger
    /// garbage collection for this thread.
    /// # Unsafe
    /// Make sure the record pointer is a valid address that has not already been freed.
    /// # Examples
    /// ```
    /// let manager: HBPRManager<*mut u8> = HPBRManager::new(100, 1);
    /// let ptr = Box::into_raw(Box::new(8u8));
    /// manager.protect(ptr, 0);
    /// // Operate on ptr...
    /// manager.retire(ptr, 0); // Add the resource to this thread's free list
    /// ```
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

    /// Protect the given record with in the given hazard pointer. The caller should always check after protection
    /// that the proteced record has not changed before operating on it, to make sure the protected record has not
    /// already been removed and possibly freed.
    /// # Unsafe
    /// Make sure the record pointer is a valid address that has not already been freed.
    /// # Examples
    /// ```
    /// let hazard = self.head_node;
    /// manager.protect(hazard, 0);         // Store in the first hazard pointer
    /// while hazard != self.head_node {    // Ensure we protect a non-freed address
    ///     hazard = self.head_node;
    ///     manager.protect(hazard, 0);
    /// }
    /// // Now we can operate on hazard without any worries!
    /// ```
    pub fn protect(&self, record: *mut T, hazard_num: usize) {
        unsafe {
            atomic::fence(Ordering::Release);
            let thread_info_mut = self.get_mut_thread_info();
            thread_info_mut.get_mut_hazard_pointer(hazard_num).protect(record);
        }
    }

    /// Set the given hazard pointer to null. This ensures that the record will not be
    /// prevented from being freed by this hazard pointer and should be used if a thread exits
    /// a data structure for good.
    /// # Examples
    /// ```
    /// let manager: HBPRManager<*mut u8> = HPBRManager::new(100, 1);
    /// let ptr = Box::into_raw(Box::new(8u8));
    /// manager.protect(ptr, 0);
    /// // Operate on ptr...
    /// manager.retire(ptr, 0); // Will not allow ptr to be freed
    /// manager.unprotect(0);   // ptr can now be freed
    /// ```
    pub fn unprotect(&self, hazard_num: usize) {
        unsafe {
            let thread_info_mut = self.get_mut_thread_info();
            thread_info_mut.get_mut_hazard_pointer(hazard_num).unprotect();
        }
    }
    
    pub fn protect_dynamic(&'a self, record: *mut T) -> HPHandle<'a, T> {
        unsafe {
            let thread_info_mut = self.get_mut_thread_info();
            for i in thread_info_mut.starting_hazards_num..thread_info_mut.local_hazards.len() {
                let hp = thread_info_mut.get_mut_hazard_pointer(i);
                match hp.protected {
                    None => {
                        hp.protect(record);
                        return HPHandle::new(i, self)
                    },
                    Some(_) => {}
                }
            }
            let new_hp = self.allocate_hp();
            let new_hp_index = thread_info_mut.add_dynamic_hazard_pointer(new_hp);
            HPHandle::new(new_hp_index, self)
        }
    }

    fn unprotect_dynamic(&self, hp_index: usize) {
        unsafe {
            let thread_info_mut = self.get_mut_thread_info();
            thread_info_mut.get_mut_hazard_pointer(hp_index).unprotect();
        }
    }

    /// This function is provided for use in data structure destructors. If somehow
    /// there is data in both a retired list and still accessible from a data structure as
    /// `drop` is called, it is possible to cause a double free, as an HPBRManager will free
    /// all resources in its hazard pointers and free lists when it is dropped. 
    /// This function is slow, unsafe,
    /// and should not be used to implement any kind of logic.
    /// # Unsafe
    /// The pointer provided must be a valid one.
    /// # Examples
    /// ```
    /// let manager: HBPRManager<*mut u8> = HPBRManager::new(100, 1);
    /// let ptr = Box::into_raw(Box::new(8u8));
    /// manager.protect(ptr, 0);
    /// // Operate on ptr...
    /// manager.retire(ptr, 0); // Add the resource to this thread's free list
    /// assert!(manager.check_in_free_list(ptr)); // true!
    /// ```
    pub unsafe fn check_in_free_list(&mut self, record: *mut T) -> bool {
        for local in self.thread_info.iter_mut() {
            let info = &*local.get();
            if info.retired_list.contains(&record) {return true}
        }
        false
    }

    /// Where the main deletion aspect of the HBPRManager takes place
    /// Deletes any retired nodes of this thread which are not protected by hazard pointers
    fn scan(&self) {
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
            thread_info.retired_number = new_retired_list.len();
            thread_info.retired_list = Box::new(new_retired_list);
        }
    }

    fn free(garbage: *mut T) {
        // Letting this box go out of scope should call Drop on the garbage
        unsafe {
            Box::from_raw(garbage);
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

pub struct HPHandle<'a, T: 'a + Send> {
    index: usize,
    manager: &'a HPBRManager<T>
}

impl<'a, T: Send> HPHandle<'a, T> {
    fn new(index: usize, manager: &'a HPBRManager<T>) -> HPHandle<'a, T> {
        HPHandle {
            index,
            manager
        }
    }
}

impl<'a, T: Send> Drop for HPHandle<'a, T> {
    fn drop(&mut self) {
        self.manager.unprotect_dynamic(self.index);
    }
}

struct HazardPointer<T: Send> {
    protected: Option<*mut T>,
    next: AtomicPtr<HazardPointer<T>>,
    active: AtomicBool
}


impl<T: Send> Drop for HazardPointer<T> {
    fn drop(&mut self) {
        //println!("Dropping hazard pointer");
    }
}

impl<T: Send> HazardPointer<T> {
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

impl<T: Send + Debug> Debug for HazardPointer<T> {
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

unsafe impl<T: Send> Send for ThreadLocalInfo<T> {}

#[derive(Debug)]
struct ThreadLocalInfo<T: Send> {
    local_hazards: Vec<*mut HazardPointer<T>>,
    retired_list: Box<VecDeque<*mut T>>,
    retired_number: usize,
    starting_hazards_num: usize
}

impl<T: Send> ThreadLocalInfo<T> {
    fn new(starting_hazards: Vec<*mut HazardPointer<T>>) -> Self {
        let starting_hazards_num = starting_hazards.len(); 
        ThreadLocalInfo {
            local_hazards: starting_hazards,
            retired_list: Box::new(VecDeque::new()),
            retired_number: 0,
            starting_hazards_num
        }
    }

    unsafe fn get_mut_hazard_pointer(&mut self, hazard_index: usize) -> &mut HazardPointer<T> {
        &mut *self.local_hazards[hazard_index]
    }

    fn add_dynamic_hazard_pointer(&mut self, hazard_pointer: *mut HazardPointer<T>) -> usize {
        self.local_hazards.push(hazard_pointer);
        self.local_hazards.len() - 1
    }
}

impl<T: Send> Drop for ThreadLocalInfo<T> {
    fn drop(&mut self) {
        // Free all nodes left over at program end
        for garbage in self.retired_list.drain(..) {
            unsafe {
                Box::from_raw(garbage);
            }
        }
        // Need to replace the vector in the struct with an empty one to take possession of it
        let hp_vec = mem::replace(&mut self.local_hazards, Vec::new());
        for hp_ptr in hp_vec {
            unsafe {
                Box::from_raw(hp_ptr);
            }
        }
    }
}

mod tests {
    #![allow(unused_imports)]
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
    #[ignore]
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