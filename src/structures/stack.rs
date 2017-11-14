use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

pub struct Stack<T: Send + Sync> {
    head: AtomicPtr<Node<T>>
}

struct Node<T> {
    data: Option<T>,
    next: *mut Node<T>
}

impl<T: Send + Sync> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack {
            head: AtomicPtr::default()
        }
    }

    pub fn push(&mut self, val: T) {
        let mut node = Node {
            data: Some(val),
            next: ptr::null_mut()
        };

        loop {
            if self.try_push(&mut node) {
                break;
            }
        };
    }

    fn try_push(&mut self, node: *mut Node<T>) -> bool {
        let old_head = self.head.load(Ordering::Relaxed);
        unsafe {
            (*node).next = old_head;
        }
        match self.head.compare_exchange_weak(old_head, node, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => true,
            Err(_) => false
        }
    }
}