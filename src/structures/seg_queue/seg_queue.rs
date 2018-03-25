use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr};
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed};
use std::ptr;
use super::atomic_markable::AtomicMarkablePtr;
use super::atomic_markable;
use rand;
use rand::Rng;

pub struct SegQueue<T: Send> {
    head:AtomicPtr<Segment<T>>,
    tail: AtomicPtr<Segment<T>>,
    manager: HPBRManager<Segment<T>>,
    k: usize
}

impl<T: Send> SegQueue<T> {
    pub fn new(k: usize) -> Self {
        let init_node = Box::into_raw(Box::new(Segment::new(k)));
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
            }
        }
    }

    fn try_enqueue(&self, mut data: Box<T>, indices: &mut[usize]) -> Result<(), Box<T>> {
        let tail = self.tail.load(Acquire);
        self.manager.protect(tail, 0);

        if !ptr::eq(tail, self.tail.load(Acquire)) {
            self.manager.unprotect(0);
            return Err(data)
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(indices);

        for index in indices {
            let cell = &Segment::get_cells_from_ptr(tail)[*index];
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

    pub fn dequeue(&self) -> Option<T> {
        let mut vec: Vec<usize> = (0..self.k).collect();
        let vals = vec.as_mut_slice();
        loop {
            if let Ok(val) = self.try_dequeue(vals) {
                return val
            }
        }
    }

    fn try_dequeue(&self, indices: &mut[usize]) -> Result<Option<T>, ()> {
        let head = self.head.load(Acquire);
        self.manager.protect(head, 0);
        if !ptr::eq(head, self.head.load(Acquire)) {
            return Err(())
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(indices);

        let mut hasEmpty = false;
        for index in indices {
            let cell = &Segment::get_cells_from_ptr(head)[*index];
            match cell.get_ptr() {
                Some(item_ptr) => {
                    if !atomic_markable::is_marked(item_ptr) {
                        // Try to mark it as deleted
                        match cell.compare_and_mark(item_ptr) {
                            Ok(_) => { 
                                // We got it, read
                                // Need to now treat this as unitialised memory
                                unsafe { return Ok(Some(ptr::read(item_ptr))) }
                            },
                            Err(_) => {
                                // We didn't get it
                            }
                        }
                    }
                },
                None => {
                    hasEmpty = true;
                }
            }
        }

        // How do we tell if the queue is empty?
        if hasEmpty {
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
                                Err(_) => { println!("Risky"); Box::from_raw(new_seg_ptr); }
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

