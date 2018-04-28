use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use std::thread;
use std::time::Duration;
use rand::{SmallRng, NewRng, Rng};
use std::cell::UnsafeCell;
use std::cmp;

const MAX_BACKOFF: u32 = 2048;

/// A lock-free Michael-Scott queue.
///
/// This queue is an implementation of that described in [Simple, Fast, and Practical
/// Non-blocking and Blocking Concurrent Queue Algorithms](https://dl.acm.org/citation.cfm?id=248106). 
/// It is implemented as a linked-list of nodes.
#[derive(Debug)]
pub struct Queue<T: Send> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    manager: HPBRManager<Node<T>>,
    rng: UnsafeCell<SmallRng>
}

unsafe impl<T: Send> Sync for Queue<T> {}

#[derive(Debug)]
struct Node<T: Send> {
    next: AtomicPtr<Node<T>>,
    value: Option<T>
}

impl<T: Send> Queue<T> {
    /// Create a new Queue.
    /// # Examples
    /// ```
    /// let queue: Queue<String> = Queue::new();
    /// ```
    pub fn new() -> Self {
        let dummy_node = Box::into_raw(Box::new(Node::new_dummy_node()));
        Queue {
            head: AtomicPtr::new(dummy_node),
            tail: AtomicPtr::new(dummy_node),
            manager: HPBRManager::new(100, 2),
            rng: UnsafeCell::new(SmallRng::new())
        }
    }
    
    fn backoff(&self, max_backoff: u32) -> u32 {
        unsafe {
            let rng = &mut *self.rng.get();
            let backoff_time = rng.gen_range(0, max_backoff);
            thread::sleep(Duration::new(0, backoff_time * 10));    
        }
        cmp::min(max_backoff * 2, MAX_BACKOFF)
    }

    /// Add a new element to the back of the queue.
    /// # Examples
    /// ```
    /// let queue: Queue<String> = Queue::new();
    /// queue.enqueue("hello".to_owned());
    /// ```
    pub fn enqueue(&self, val: T) {
        let mut backoff = 1;
        let mut node = Box::new(Node::new(val));
        loop {
            node = match self.try_enqueue(node) {
                Ok(_) => { return; },
                Err(old_node) => old_node
            };
            backoff = self.backoff(backoff);
        }
    }

    fn try_enqueue(&self, val: Box<Node<T>>) -> Result<(), Box<Node<T>>> {
        let tail = self.tail.load(Ordering::Acquire);
        self.manager.protect(tail, 0);
        // Is the tail still consistent? Required for the hazard pointer to work
        if !ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
            return Err(val)
        }
        let next = unsafe { (*tail).next.load(Ordering::Acquire) };

        // Is the tail actually the end of the queue?
        if !next.is_null() {
            // If it isn't, try to make next the end of the queue
            let _ = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);
            return Err(val)
        }

        let node_ptr = Box::into_raw(val);
        // Try to CAS our node onto the end of the queue
        unsafe {
            match (*tail).next.compare_exchange(ptr::null_mut(), node_ptr, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => {
                    // Success! Set our new node to the tail
                    let _ = self.tail.compare_exchange(tail, node_ptr, Ordering::Release, Ordering::Relaxed);
                    return Ok(())
                },
                // Failure :( try again
                Err(_) => {
                    return Err(Box::from_raw(node_ptr))
                }
            }
        }
    }

    /// Take an element from the front of the queue, or return None if the queue is empty.
    /// # Examples
    /// ```
    /// let queue: Queue<String> = Queue::new();
    /// queue.enqueue("hello".to_owned());
    /// assert_eq!(queue.dequeue(), Some("hello".to_owned()));
    /// ```
    pub fn dequeue(&self) -> Option<T> {
        let mut backoff = 1;
        loop {
            if let Ok(val) = self.try_dequeue() {
                return val
            }
            backoff = self.backoff(backoff);
        }
    }

    fn try_dequeue(&self) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::Acquire);
        self.manager.protect(head, 0);
        if !ptr::eq(head, self.head.load(Ordering::Acquire)) {
            return Err(())
        }

        let next = unsafe {(*head).next.load(Ordering::Acquire)};
        self.manager.protect(next, 1);
        if !ptr::eq(next, unsafe { (*head).next.load(Ordering::Acquire) }) {
            return Err(())
        }

        let tail = self.tail.load(Ordering::Acquire);
        
        if next.is_null() {
            return Ok(None)
        }

        if ptr::eq(head, tail) {
            let _ = self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);
            return Err(());
        }

        match self.head.compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                let node = unsafe { ptr::read(next) };
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

impl<T: Send> Drop for Queue<T> {
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

impl<T: Send> Node<T> {
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

impl<T: Send> Default for Node<T> {
    fn default() -> Self {
        Node {
            next: AtomicPtr::default(),
            value: None
        }
    }
}

mod tests {
    #![allow(unused_imports)]
    extern crate im;
    use self::im::Vector;

    use rand::{thread_rng, Rng};

    use super::Queue;
    use std::sync::Arc;
    use std::thread;
    use std::sync::atomic::Ordering;

    use super::super::super::testing::linearizability_tester::{LinearizabilityTester, LinearizabilityResult, ThreadLog};

    #[test]
     
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

    #[test]
    fn test_linearizable() {
        let queue: Queue<usize> = Queue::new();
        let sequential: Vector<usize> = Vector::new();

        let mut linearizer: LinearizabilityTester<Queue<usize>, Vector<usize>, usize> 
                = LinearizabilityTester::new(8, 1000000, queue, sequential);

        fn sequential_dequeue(queue: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
            match queue.pop_front() {
                Some((arc, vec)) => {
                    let res = *arc;
                    (vec, Some(res))
                },
                None => (Vector::new(), None)
            }
        } 

        fn sequential_enqueue(queue: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
            (queue.push_back(val.unwrap()), None)
        }

        fn worker(id: usize, log: &mut ThreadLog<Queue<usize>, Vector<usize>, usize>) {
            for _ in 0..1000 {
                let rand = thread_rng().gen_range(0, 101);
                if rand < 30 {
                    let val = thread_rng().gen();
                    log.log_val(id, Queue::enqueue, val, format!("enqueue: {}", val), sequential_enqueue);
                } else {
                    log.log(id, Queue::dequeue, "dequeue".to_owned(), sequential_dequeue);
                }
            }
        }

        let result = linearizer.run(worker);

        println!("{:?}", result);

        match result {
            LinearizabilityResult::Success => assert!(true),
            _ => assert!(false)
        }
    }
}
