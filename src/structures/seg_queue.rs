use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::fmt::Debug;
use std::ptr;
use rand;
use rand::Rng;

pub struct SegQueue<T: Send + Debug> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    manager: HPBRManager<Node<T>>,
    k: usize
}

impl<T: Send + Debug> SegQueue<T> {
    pub fn new(k: usize) -> Self {
        let init_node: *mut Node<T> = Box::into_raw(Box::new(Node::new(k)));
        SegQueue {
            head: AtomicPtr::new(init_node),
            tail: AtomicPtr::new(init_node),
            manager: HPBRManager::new(100, 2),
            k
        }
    }

    pub fn enqueue(&self, data: T) {
        let mut vec: Vec<usize> = (0..self.k).collect();
        let vals = vec.as_mut_slice();
        let mut data_box = Box::new(Some(data));
        loop {
            data_box = match self.try_enqueue(data_box, vals) {
                Ok(()) => { return; },
                Err(val) => val
            };    
        }
    }

    fn try_enqueue(&self, data: Box<Option<T>>, vals: &mut[usize]) -> Result<(), Box<Option<T>>> {
        let tail = self.tail.load(Ordering::Relaxed);
        self.manager.protect(tail, 0);

        if !ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
            self.manager.unprotect(0);
            return Err(data);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(vals);
        
        if let Ok((index, old_ptr)) = self.find_empty_slot(tail, vals) {
            if ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
                let data_ptr = Box::into_raw(data);
                unsafe {
                    match (*tail).data[index].compare_exchange_weak(old_ptr, data_ptr, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(old) => {
                            // Use the committed function to check the addition or reverse it
                            // Free the old data
                            Box::from_raw(old);
                            return Ok(())
                        },
                        Err(_) => {
                            return Err(Box::from_raw(data_ptr))
                        }
                    }
                }
            } else {
                // The tail has changed so we should not try an insertion
                return Err(data)
            }
        } else {
            // Advance the tail, either by adding the new block or adjusting the tail
            self.advance_tail(tail);
            return Err(data)
        }
    }

    // TODO: write dequeue

    fn find_empty_slot(&self, node_ptr: *mut Node<T>, order: &[usize]) -> Result<(usize, *mut Option<T>), ()> {
        unsafe {
            let node = &*node_ptr;
            for i in order {
                let old_ptr = node.data[*i].load(Ordering::Relaxed);
                match *old_ptr {
                    Some(_) => {},
                    None => {return Ok((*i, old_ptr));}
                }
            }
        }
        
        Err(())
    }

    fn advance_tail(&self, old_tail: *mut Node<T>) {
        if ptr::eq(old_tail, self.tail.load(Ordering::Relaxed)) {
            unsafe {
                let next = (*old_tail).next.load(Ordering::Relaxed);
                if next.is_null() {
                    // Create a new tail segment and advance if possible
                    let new_seg_ptr: *mut Node<T> = Box::into_raw(Box::new(Node::new(self.k)));
                    match (*old_tail).next.compare_exchange_weak(next, new_seg_ptr, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => { let _ = self.tail.compare_exchange(old_tail, new_seg_ptr, Ordering::AcqRel, Ordering::Acquire); },
                        Err(_) => { Box::from_raw(new_seg_ptr); } // Delete the unused new segment if we can't swap in
                    }
                } else {
                    // Advance tail, because it is out of sync somehow
                    let _ = self.tail.compare_exchange(old_tail, next, Ordering::AcqRel, Ordering::Acquire);
                }
            }
        }
    }

    fn advance_head(&self, old_head: *mut Node<T>) {
        let head = self.head.load(Ordering::Relaxed);
        // Head doesn't need protecting, we ONLY use it if it's equal to old_head, which should be protected already
        if ptr::eq(head, old_head) {
            let tail = self.tail.load(Ordering::Relaxed);
            unsafe {
                let tail_next = (*tail).next.load(Ordering::Relaxed);
                let head_next = (*head).next.load(Ordering::Relaxed);
                if ptr::eq(head, self.head.load(Ordering::Relaxed)) {
                    if ptr::eq(tail, head) {
                        if tail_next.is_null() {
                            // Queue only has one segment, so we don't remove it
                            return;
                        } else if ptr::eq(tail, self.tail.load(Ordering::Relaxed)) {
                            // Set the tail to point to the next block, so the queue has two segments
                            let _ = self.tail.compare_exchange(tail, tail_next, Ordering::AcqRel, Ordering::Acquire);
                        }
                    }
                    // TODO: Set the head to be deleted, might need for the commit function
                    // Advance the head and retire old_head
                    let _ = self.head.compare_exchange(head, head_next, Ordering::AcqRel, Ordering::Acquire);
                    self.manager.retire(head, 0);
                }
            }
        }
    }
}

#[derive(Debug)]
struct Node<T: Send + Debug> {
    data: Vec<AtomicPtr<Option<T>>>,
    next: AtomicPtr<Node<T>>
}   

impl<T: Send + Debug> Node<T> {
    fn new(k: usize) -> Self {
        let mut data = Vec::new();
        for _ in 0..k {
            data.push(AtomicPtr::new(Box::into_raw(Box::new(None))));
        }
        Node {
            data,
            next: AtomicPtr::default()
        }
    }
}