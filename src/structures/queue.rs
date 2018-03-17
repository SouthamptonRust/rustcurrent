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

    fn try_enqueue(&self, val: Box<Node<T>>) -> Result<(), Box<Node<T>>> {
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
                let _ = self.tail.compare_exchange_weak(tail, next, Ordering::AcqRel, Ordering::Acquire);
                return Err(val)
            }
            let node_ptr = Box::into_raw(val);
            // Try to CAS our node onto the end of the queue
            match (*tail).next.compare_exchange_weak(ptr::null_mut(), node_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    // Success! Set our new node to the tail
                    let _ = self.tail.compare_exchange(tail, node_ptr, Ordering::AcqRel, Ordering::Acquire);
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

    fn try_dequeue(&self) -> Result<Option<T>, ()> {
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
            match self.head.compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    let node = Node::replace(next);
                    let data = node.value;
                    self.manager.retire(head, 0);
                    return Ok(data)
                },
                Err(_) => {
                    return Err(())
                }
            }
        }
    }
}

impl<T: Send + Debug> Drop for Queue<T> {
    fn drop(&mut self) {
        let mut current = self.head.load(Ordering::Relaxed);
        while !current.is_null() {
            unsafe {
                let next = (*current).next.load(Ordering::Relaxed);
                Box::from_raw(current);
                current = next;
            }
        }
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

    unsafe fn replace(dest: *mut Self) -> Self {
        let next_ptr = (*dest).next.load(Ordering::Acquire);
        let node = Node {
            next: AtomicPtr::new(next_ptr),
            value: None
        };
        ptr::replace(dest, node)
    }
}

impl<T: Send + Debug> Default for Node<T> {
    fn default() -> Self {
        Node {
            next: AtomicPtr::default(),
            value: None
        }
    }
}

mod tests {
    #![allow(unused_imports)]
    use super::Queue;
    use std::sync::Arc;
    use std::thread;
    use std::sync::atomic::Ordering;

    #[test]
    #[ignore]
    fn test_queue_single_threaded() {
        let mut queue : Queue<u8> = Queue::new();
        queue.enqueue(8);
        unsafe {
            println!("{:?}", *queue.head.load(Ordering::Relaxed));
            let head = (*queue.head.load(Ordering::Relaxed)).next.load(Ordering::Relaxed);
            assert_eq!((*head).value, Some(8));
        }
        queue.enqueue(7);
        assert_eq!(queue.dequeue(), Some(8));
        assert_eq!(queue.dequeue(), Some(7));
        assert_eq!(queue.dequeue(), None);

        for i in 0..100 {
            queue.enqueue(i);
        }
        for i in 0..100 {
            assert_eq!(queue.dequeue(), Some(i));
        }
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    #[ignore]
    fn test_queue_multithreaded() {
        let mut queue: Arc<Queue<u32>> = Arc::new(Queue::new());
        let mut waitvec: Vec<thread::JoinHandle<()>> = Vec::new();

        for i in 0..20 {
            let mut queue_copy = queue.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    queue_copy.enqueue(i);
                }
                //println!("Push thread {} complete", i);
            }));
            queue_copy = queue.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    loop {
                        match queue_copy.dequeue() {
                            Some(_) => {break},
                            None => {} 
                        }
                    }
                }
                println!("Pop thread {} complete", i);
            }));
        }

        for handle in waitvec {
            match handle.join() {
                Ok(_) => {},
                Err(some) => println!("Couldn't join! {:?}", some) 
            }
        }
        println!("Joined all");
        assert_eq!(None, queue.dequeue());
    }
}
