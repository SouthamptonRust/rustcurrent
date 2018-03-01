use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::fmt::Debug;
use std::ptr;

#[derive(Debug)]
pub struct Queue<T: Debug + Send> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    manager: HPBRManager<Node<T>>
}

#[derive(Debug)]
pub struct Node<T: Debug + Send> {
    next: AtomicPtr<Node<T>>,
    value: Option<T>
}

impl<T: Send + Debug> Queue<T> {
    pub fn new() -> Self {
        let dummy_node = Box::into_raw(Box::new(Node::new_dummy_node()));
        Queue {
            head: AtomicPtr::new(dummy_node),
            tail: AtomicPtr::new(dummy_node),
            manager: HPBRManager::new(100, 2)
        }
    }

    pub fn enqueue(&self, val: T) {
        let mut node = Box::new(Node::new(val));
        loop {
            node = match self.try_enqueue(node) {
                Ok(_) => { return; },
                Err(old_node) => old_node
            }
        }
    }

    pub fn try_enqueue(&self, val: Box<Node<T>>) -> Result<(), Box<Node<T>>> {
        let tail = self.tail.load(Ordering::Acquire);
        self.manager.protect(tail, 0);
        // Is the tail still consistent? Required for the hazard pointer to work
        if !ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
            return Err(val)
        }
        unsafe {
            let next = (*tail).next.load(Ordering::Acquire);
            // Is the tail still consistent?
            if !ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
                return Err(val)
            }
            // Is the tail actually the end of the queue?
            if !next.is_null() {
                // If it isn't, try to make next the end of the queue
                let _ = self.tail.compare_exchange_weak(tail, next, Ordering::AcqRel, Ordering::Release);
                return Err(val)
            }
            let node_ptr = Box::into_raw(val);
            // Try to CAS our node onto the end of the queue
            match (*tail).next.compare_exchange_weak(ptr::null_mut(), node_ptr, Ordering::AcqRel, Ordering::Release) {
                Ok(_) => {
                    // Success! Set our new node to the tail
                    let _ = self.tail.compare_exchange_weak(tail, node_ptr, Ordering::AcqRel, Ordering::Release);
                    return Ok(())
                },
                // Failure :( try again
                Err(_) => {
                    return Err(Box::from_raw(node_ptr))
                }
            }
        }
    }

    pub fn dequeue(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_dequeue() {
                return val
            }
        }
    }

    pub fn try_dequeue(&self) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::Acquire);
        self.manager.protect(head, 0);
        if !ptr::eq(head, self.head.load(Ordering::Acquire)) {
            return Err(())
        }
        let tail = self.tail.load(Ordering::Acquire);
        unsafe {
            let next = (*head).next.load(Ordering::Acquire);
            self.manager.protect(next, 1);
            if !ptr::eq(head, self.head.load(Ordering::Acquire)) {
                return Err(())
            }
            if next.is_null() {
                return Ok(None)
            }
            // If the queue isn't empty, but head == tail, then the tail must be falling behind
            if ptr::eq(head, tail) {
                // Help it to catch up!
                let _ = self.tail.compare_exchange_weak(tail, next, Ordering::AcqRel, Ordering::Acquire);
                return Err(())
            }
        }

        Err(())
    }
}

impl<T: Send + Debug> Node<T> {
    fn new(value: T) -> Self {
        Node {
            next: AtomicPtr::default(),
            value: Some(value)
        }
    }

    fn new_dummy_node() -> Self {
        Node {
            next: AtomicPtr::default(),
            value: None
        }
    }
}

mod tests {
    use super::Queue;
    use std::sync::Arc;
    use std::thread;
    use std::sync::atomic::Ordering;

    fn test_push_single_threaded() {
        let mut queue : Queue<u8> = Queue::new();
        queue.enqueue(8);
        unsafe {
            assert_eq!((*queue.head.load(Ordering::Relaxed)).value, Some(8));
        }
        queue.enqueue(7);
        assert_eq!(queue.dequeue(), Some(8));
        assert_eq!(queue.dequeue(), Some(7));
        assert_eq!(queue.dequeue(), None);
    }
}
