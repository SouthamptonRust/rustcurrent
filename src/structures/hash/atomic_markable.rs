use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed};
use std::marker::PhantomData;

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

pub fn is_marked_second<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match (ptr_usize & 0x2) >> 1 {
        0 => false,
        _ => true
    }
}

pub fn unmark_second<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize & !(0x2)) as *mut T
}

pub fn mark_second<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize | 0x2) as *mut T
}

#[derive(Debug)]
pub struct AtomicMarkablePtr<T: Send> {
    ptr: AtomicUsize,
    _phantom: PhantomData<T>
}

impl <T: Send> AtomicMarkablePtr<T> {
    pub fn get_ptr(&self) -> Option<*mut T> {
        match self.ptr.load(Acquire) {
            0 => None,
            ptr_val => Some(ptr_val as *mut T)
        }
    }

    pub fn mark(&self) {
        if !is_marked_second(self.ptr.load(Acquire) as *mut T) {
            self.ptr.fetch_or(0x1, Release);
        }
    }

    pub fn compare_and_mark(&self, old: *mut T) -> Result<*mut T, *mut T> {
        let marked_ptr = mark(old);
        match self.ptr.compare_exchange(old as usize, marked_ptr as usize, Release, Relaxed) {
            Ok(ptr) => Ok(ptr as *mut T),
            Err(ptr) => Err(ptr as *mut T)
        }
    }

    pub fn compare_exchange(&self, current: *mut T, new: *mut T) -> Result<*mut T, *mut T> {
        match self.ptr.compare_exchange(current as usize, new as usize, Release, Relaxed) {
            Ok(ptr) => Ok(ptr as *mut T),
            Err(ptr) => Err(ptr as *mut T)
        }
    }

    pub fn store(&self, val: *mut T) {
        self.ptr.store(val as usize, Release);
    }
}

impl<T: Send> Default for AtomicMarkablePtr<T> {
    fn default() -> Self {
        AtomicMarkablePtr {
            ptr: AtomicUsize::default(),
            _phantom: PhantomData
        }
    }
}

impl<T: Send> Drop for AtomicMarkablePtr<T>
{
    fn drop(&mut self) {
        let mut ptr = self.ptr.load(Relaxed) as *mut T;
        ptr = unmark(unmark_second(ptr));
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}