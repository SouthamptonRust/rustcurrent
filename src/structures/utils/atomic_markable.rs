use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed};

pub fn is_marked<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match ptr_usize & 0x1 {
        0 => false,
        _ => true,
    }
}

pub fn unmark<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize & !(0x1)) as *mut T
}

pub fn mark<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize | 0x1) as *mut T
}

pub struct AtomicMarkablePtr<T: Send> {
    ptr: AtomicPtr<T>
}

impl <T: Send> AtomicMarkablePtr<T> {
    pub fn get_ptr(&self) -> Option<*mut T> {
        let ptr = self.ptr.load(Acquire);
        if ptr.is_null() { None } else { Some(ptr) }
    }

    pub fn compare_and_mark(&self, old: *mut T) -> Result<*mut T, *mut T> {
        let marked_ptr = mark(old);
        self.ptr.compare_exchange(old, marked_ptr, Release, Relaxed)
    }

    pub fn compare_exchange(&self, current: *mut T, new: *mut T) -> Result<*mut T, *mut T> {
        self.ptr.compare_exchange(current, new, Release, Relaxed)
    }
}

impl<T: Send> Default for AtomicMarkablePtr<T> {
    fn default() -> Self {
        AtomicMarkablePtr {
            ptr: AtomicPtr::default()
        }
    }
}

impl<T: Send> Drop for AtomicMarkablePtr<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.load(Relaxed);
        if !is_marked(ptr) && !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}