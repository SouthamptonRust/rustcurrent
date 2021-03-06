use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr};
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed};
use std::ptr;
use std::cell::UnsafeCell;
use super::utils::atomic_markable::AtomicMarkablePtr;
use super::utils::atomic_markable;
use rand::{Rng, SmallRng, NewRng};

/// A lock-free k-FIFO segmented queue.
///
/// This is an implementation of a k-FIFO queue as described in [Fast and Scalable k-FIFO Queues]
/// (https://link.springer.com/chapter/10.1007/978-3-642-39958-9_18). The idea behind the k-FIFO
/// queue is to relax the consistency requirement of the queue so that any of the `k` first
/// enqueued elements can be the first to be dequeued. This allows for greater scalability since
/// there is less contention, and the drift from normal queue semantics is bounded by `k`.
///
/// The queue is implemented as a linked-list of nodes, each containing an array of elements. 
/// Once a node is full, a new one is enqueued. Once a node is empty, it is dequeued and freed.
/// 
/// If relaxed consistency is undesirable, do not set `k` to 1. Instead, use the Queue structure
/// from the `rustcurrent` library as it is far better optimised for that scenario.
pub struct SegQueue<T: Send> {
    head:AtomicPtr<Segment<T>>,
    tail: AtomicPtr<Segment<T>>,
    manager: HPBRManager<Segment<T>>,
    rng: UnsafeCell<SmallRng>,
    k: usize
}

unsafe impl<T: Send> Sync for SegQueue<T> {}

impl<T: Send> SegQueue<T> {
    /// Create a new SegQueue with a given node size. The node size must be
    /// a power of 2.
    /// # Examples
    /// ```
    /// let queue: SegQueue<u8> = SegQueue::new(8);
    /// ```
    pub fn new(k: usize) -> Self {
        if (k & !(k - 1)) != k {
            panic!("k must be a non-zero power of 2!")
        }
        let init_node = Box::into_raw(Box::new(Segment::new(k)));
        SegQueue {
            head: AtomicPtr::new(init_node),
            tail: AtomicPtr::new(init_node),
            manager: HPBRManager::new(100, 2),
            rng: UnsafeCell::new(SmallRng::new()),
            k
        }
    }

    /// Enqueue the given data.
    /// # Examples
    /// ```
    /// let queue: SegQueue<u8> = SegQueue::new(8);
    /// queue.enqueue(8);
    /// ``` 
    pub fn enqueue(&self, data: T) {
        let mut data_box = Box::new(data);
        loop {
            data_box = match self.try_enqueue(data_box) {
                Ok(()) => { return; },
                Err(val) => val
            };
        }
    }

    fn try_enqueue(&self, mut data: Box<T>) -> Result<(), Box<T>> {
        let tail = self.tail.load(Acquire);
        self.manager.protect(tail, 0);

        if !ptr::eq(tail, self.tail.load(Acquire)) {
            self.manager.unprotect(0);
            return Err(data)
        }

        let rand: usize = unsafe { (*self.rng.get()).gen() };
        let permutation_start = rand & (self.k - 1);
        let permutation = OrderGenerator::new(permutation_start, self.k);

        for index in permutation.iter() {
            let cell = &Segment::get_cells_from_ptr(tail)[index];
            data = match cell.get_ptr() {
                None => {
                    let item_ptr = Box::into_raw(data);
                    match cell.compare_exchange(ptr::null_mut(), item_ptr) {
                        Ok(_) => { return Ok(()) },
                        Err(_) => { unsafe { Box::from_raw(item_ptr) } }
                    }
                },
                Some(_) => { continue; }
            }
        }

        // No available position, need to create a new segment
        self.advance_tail(tail);
        Err(data)
    }

    /// Attempt to dequeue a piece of data, returning None if the queue is empty. If
    /// the front segment is empty, it will be dequeued.
    /// # Examples
    /// ```
    /// let queue: SegQueue<u8> = SegQueue::new(8);
    /// queue.enqueue(8);
    /// assert_eq!(queue.dequeue(), Some(8));
    /// ```
    pub fn dequeue(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_dequeue() {
                return val
            }
        }
    }

    fn try_dequeue(&self) -> Result<Option<T>, ()> {
        let head = self.head.load(Acquire);
        self.manager.protect(head, 0);
        if !ptr::eq(head, self.head.load(Acquire)) {
            return Err(())
        }

        let rand: usize = unsafe { (*self.rng.get()).gen() };
        let permutation_start = rand & (self.k - 1);
        let permutation = OrderGenerator::new(permutation_start, self.k);

        let mut has_empty = false;
        for index in permutation.iter() {
            let cell = &Segment::get_cells_from_ptr(head)[index];
            match cell.get_ptr() {
                Some(item_ptr) => {
                    if !atomic_markable::is_marked(item_ptr) {
                        // Try to mark it as deleted
                        match cell.compare_and_mark(item_ptr) {
                            Ok(_) => { 
                                // We got it, read
                                // Need to now treat this as unitialised memory
                                let data = unsafe { ptr::read(item_ptr) };
                                unsafe { Box::from_raw(item_ptr) } ; 
                                return Ok(Some(data)) 
                            },
                            Err(_) => {
                                // We didn't get it
                            }
                        }
                    }
                },
                None => {
                    has_empty = true;
                }
            }
        }

        // How do we tell if the queue is empty?
        if ptr::eq(head, self.tail.load(Acquire)) || has_empty {
            // Must be the last node, because there are empty slots
            // If we reach the end and there are empty spots, we return None
            return Ok(None)
        }

        // Queue is not empty but we didn't find a slot - need to advance the head
        self.advance_head(head);
        Err(())
    }

    fn advance_tail(&self, tail_old: *mut Segment<T>) {
        if ptr::eq(tail_old, self.tail.load(Acquire)) {
            let next = unsafe { (*tail_old).next.load(Acquire)}; 
            if next.is_null() {
                // Create a new segment
                let new_seg_ptr: *mut Segment<T> = Box::into_raw(Box::new(Segment::new(self.k)));
                unsafe {
                    match (*tail_old).next.compare_exchange(next, new_seg_ptr, Release, Relaxed) {
                        Ok(_) => {
                            match self.tail.compare_exchange(tail_old, new_seg_ptr, Release, Relaxed) {
                                Ok(_) => {},
                                Err(_) => {}
                            }
                        },
                        Err(_) => { Box::from_raw(new_seg_ptr); }
                    }
                }
            } else {
                let _ = self.tail.compare_exchange(tail_old, next, Release, Relaxed);
            }
        }
    }

    fn advance_head(&self, head_old: *mut Segment<T>) {
        if ptr::eq(head_old, self.head.load(Acquire)) {
            let mut tail = self.tail.load(Acquire);
            self.manager.protect(tail, 1);
            while tail != self.tail.load(Acquire) {
                tail = self.tail.load(Acquire);
                self.manager.protect(tail, 1);
            }
            if ptr::eq(tail, head_old) {
                let tail_next = unsafe { (*tail).next.load(Acquire) }; 
                if tail_next.is_null() {
                    // Queue only has one segment
                    return;
                }    
                let _ = self.tail.compare_exchange(tail, tail_next, Release, Relaxed);
            }
            let head_next = unsafe { (*head_old).next.load(Acquire) };
            match self.head.compare_exchange(head_old, head_next, Release, Relaxed) {
                Ok(_) => {
                    self.manager.retire(head_old, 0);
                },
                Err(_) => {}
            }
        }
        
    }
}

impl<T: Send> Drop for SegQueue<T> {
    fn drop(&mut self) {
        let mut current = self.head.load(Relaxed);
        while !current.is_null() {
            unsafe {
                let next = (*current).next.load(Relaxed);
                Box::from_raw(current);
                current = next;
            }
        } 
    }
}

struct Segment<T: Send> {
    cells: Vec<AtomicMarkablePtr<T>>,
    next: AtomicPtr<Segment<T>>
}

impl<T: Send> Segment<T> {
    fn new(k: usize) -> Self {
        let mut cells: Vec<AtomicMarkablePtr<T>> = Vec::new();
        for _ in 0..k {
            cells.push(AtomicMarkablePtr::default())
        }
        Segment {
            cells,
            next: AtomicPtr::default()
        }
    }

    fn get_cells_from_ptr<'a>(ptr: *mut Segment<T>) -> &'a Vec<AtomicMarkablePtr<T>> {
        unsafe { &(*ptr).cells }
    }
}

struct OrderGenerator {
    start: usize,
    size: usize
}

impl OrderGenerator {
    fn new(start: usize, size: usize) -> OrderGenerator {
        OrderGenerator {
            start,
            size
        }
    }

    fn iter(&self) -> OrderGeneratorIterator {
        OrderGeneratorIterator {
            generator: self,
            current: self.start,
            start: self.start,
            bitmask: self.size - 1,
            started: false
        }
    }
}

pub struct OrderGeneratorIterator<'a> {
    generator: &'a OrderGenerator,
    current: usize,
    start: usize,
    bitmask: usize,
    started: bool
}

impl<'a> Iterator for OrderGeneratorIterator<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let result = Some(self.current & self.bitmask);
        if self.current & self.bitmask == self.start && self.started {
            return None
        }
        self.current += 1;
        self.started = true;
        result
    }
}

mod tests {
    #![allow(unused_imports)]
    extern crate im;
    use self::im::Vector;

    use rand::{thread_rng, Rng};
    use super::{SegQueue, OrderGenerator};
    use std::sync::Arc;
    use std::thread;
    
    use super::super::super::testing::{LinearizabilityTester, LinearizabilityResult, ThreadLog}; 

    #[test]
     
    fn test_single_threaded() {
        let gen = OrderGenerator::new(12, 32);

        for index in gen.iter() {
            println!("{}", index);
        }
    }

    #[test]
    fn test_with_contention() {
        let mut queue: Arc<SegQueue<u16>> = Arc::new(SegQueue::new(32));
        
        let mut waitvec: Vec<thread::JoinHandle<()>> = Vec::new();

        for thread_no in 0..20 {
            let mut queue_copy = queue.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    queue_copy.enqueue(i);
                }
                println!("Push thread {} complete", thread_no);
            }));
            queue_copy = queue.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    let mut num = 0;
                    loop {
                        match queue_copy.dequeue() {
                            Some(_) => {num = 0; break},
                            None => {
                                num += 1;
                                if num > 1000 {
                                    //println!("{:?}", queue_copy);
                                    println!("{}", num);
                                    num = 0;
                                }
                            } 
                        }
                    }
                }
                //println!("Pop thread {} complete", thread_no);
            }));
        }
        
        for handle in waitvec {
            match handle.join() {
                Ok(some) => {println!("joined {:?}", some)},
                Err(some) => println!("Couldn't join! {:?}", some) 
            }
        }
        println!("Joined all");
        assert_eq!(None, queue.dequeue());
    }

    #[test]
    fn test_linearizabile_k_one() {
        let queue: SegQueue<usize> = SegQueue::new(1);
        let sequential: Vector<usize> = Vector::new();

        let mut linearizer: LinearizabilityTester<SegQueue<usize>, Vector<usize>, usize> 
                = LinearizabilityTester::new(8, 1000000, queue, sequential);

        fn sequential_dequeue(queue: &Vector<usize>, _val: Option<usize>) -> (Vector<usize>, Option<usize>) {
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

        fn worker(id: usize, log: &mut ThreadLog<SegQueue<usize>, Vector<usize>, usize>) {
            for _ in 0..1000 {
                let rand = thread_rng().gen_range(0, 101);
                if rand < 30 {
                    let val = thread_rng().gen();
                    log.log_val(id, SegQueue::enqueue, val, format!("enqueue: {}", val), sequential_enqueue);
                } else {
                    log.log(id, SegQueue::dequeue, "dequeue".to_owned(), sequential_dequeue);
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