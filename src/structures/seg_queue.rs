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

#[derive(Debug)]
struct Node<T: Send + Debug> {
    data: Vec<AtomicPtr<Option<T>>>,
    next: AtomicPtr<Node<T>>
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
        let mut data_box = Box::new(data);
        loop {
            data_box = match self.try_enqueue(data_box, vals) {
                Ok(()) => { return; },
                Err(val) => val
            };    
        }
    }

    fn try_enqueue(&self, data: Box<T>, vals: &mut[usize]) -> Result<(), Box<T>> {
        let tail = self.tail.load(Ordering::Relaxed);
        self.manager.protect(tail, 0);

        if !ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
            self.manager.unprotect(0);
            return Err(data);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(vals);
        
        if let Ok(index) = self.find_empty_slot(tail, vals) {

        } else {

        }

        Ok(())  
    }

    fn find_empty_slot(&self, node_ptr: *mut Node<T>, order: &[usize]) -> Result<usize, ()> {
        unsafe {
            let node = &*node_ptr;
            for i in order {
                match *node.data[*i].load(Ordering::Relaxed) {
                    Some(_) => {},
                    None => {return Ok(*i);}
                }
            }
        }
        
        Err(())
    }
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