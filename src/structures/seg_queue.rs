use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::fmt::Debug;
use std::fmt;
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
            manager: HPBRManager::new(100, 3),
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
                            // This needs to be done because of a data race with dequeuing advancing the head
                            // Free the old data
                            return match self.commit(tail, data_ptr, index) {
                                true => {
                                    Box::from_raw(old);
                                    Ok(())
                                },
                                false => Err(Box::from_raw(data_ptr)) 
                            }
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

    unsafe fn commit(&self, tail_old: *mut Node<T>, item_ptr: *mut Option<T>, index: usize) -> bool {
        if !ptr::eq((*tail_old).data[index].load(Ordering::Acquire), item_ptr) {
            // Already dequeued
            return true;
        }
        let head = self.head.load(Ordering::Acquire);
        let new_none_ptr: *mut Option<T> = Box::into_raw(Box::new(None));

        if (*tail_old).deleted {
            return match (*tail_old).data[index].compare_exchange(item_ptr, new_none_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => false,
                Err(_) => {
                    Box::from_raw(new_none_ptr);
                    true
                } 
            }
        } else if ptr::eq(head, tail_old) {
            return match self.head.compare_exchange(head, head, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    Box::from_raw(new_none_ptr);
                    true
                },
                Err(_) => {
                    return match (*tail_old).data[index].compare_exchange(item_ptr, new_none_ptr, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            false
                        },
                        Err(_) => {
                            Box::from_raw(new_none_ptr);
                            true
                        }
                    }  
                }
            }
        } else if !(*tail_old).deleted {
            return true
        } else {
            return match (*tail_old).data[index].compare_exchange(item_ptr, new_none_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => false,
                Err(_) => {
                    Box::from_raw(new_none_ptr);
                    true
                }
            }
        }
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

    pub fn try_dequeue(&self, vals: &mut[usize]) -> Result<Option<T>, ()> {
        let head = self.head.load(Ordering::Relaxed);
        self.manager.protect(head, 0);
        if !ptr::eq(head, self.head.load(Ordering::Acquire)) {
            return Err(())
        }
        
        let mut rng = rand::thread_rng();
        rng.shuffle(vals);
        let found = self.find_item(head, vals);
        let tail = self.tail.load(Ordering::Acquire);

        if ptr::eq(head, self.head.load(Ordering::Acquire)) {
            match found {
                Ok((index, item_ptr)) => {
                    if ptr::eq(head, tail) {
                        self.advance_tail(tail);
                    };
                    let new_none_ptr: *mut Option<T> = Box::into_raw(Box::new(None));
                    return match (*head).data[index].compare_exchange(item_ptr, new_none_ptr, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            let data = ptr::replace(item_ptr, None);
                            Box::from_raw(item_ptr);
                            Ok(data)
                        },
                        Err(_) => {
                            Box::from_raw(new_none_ptr);
                            Err(())
                        }
                    }
                },
                Err(()) => {
                    if ptr::eq(head, tail) && ptr::eq(tail, self.tail.load(Ordering::Acquire)) {
                        return Ok(None)
                    }
                    self.advance_head(head);
                    return Err(())
                }
            }
        }
        Err(())
    }

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

    fn find_item(&self, node_ptr: *mut Node<T>, order: &[usize]) -> Result<(usize, *mut Option<T>), ()> {
        unsafe {
            let node = &*node_ptr;
            for i in order {
                let old_ptr = node.data[*i].load(Ordering::Relaxed);
                match *old_ptr {
                    Some(_) => { return Ok((*i, old_ptr))},
                    None => {}
                }
            }
        }
        
        Err(())
    }

    fn advance_tail(&self, old_tail: *mut Node<T>) {
        let tail_current = self.tail.load(Ordering::Acquire);
        if ptr::eq(tail_current, old_tail) {
            unsafe {
                let next = (*old_tail).next.load(Ordering::Relaxed);
                if ptr::eq(old_tail, self.tail.load(Ordering::Relaxed)) {
                    if next.is_null() {
                        // Create a new tail segment and advance if possible
                        let new_seg_ptr: *mut Node<T> = Box::into_raw(Box::new(Node::new(self.k)));
                        match (*old_tail).next.compare_exchange(next, new_seg_ptr, Ordering::AcqRel, Ordering::Acquire) {
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
                        } 
                        if ptr::eq(tail, self.tail.load(Ordering::Relaxed)) {
                            // Set the tail to point to the next block, so the queue has two segments
                            let _ = self.tail.compare_exchange(tail, tail_next, Ordering::AcqRel, Ordering::Acquire);
                        }
                    }
                    // TODO: Set the head to be deleted, might need for the commit function
                    // Advance the head and retire old_head
                    match self.head.compare_exchange(head, head_next, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            (*head).deleted = true;
                            self.manager.retire(head, 0);
                        },
                        Err(_) => {}
                    }
                }
            }
        }
    }
}

impl<T: Send + Debug> Debug for SegQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut start_ptr = self.head.load(Ordering::Relaxed);
        let mut node_string = "[".to_owned();
        unsafe {
            while !start_ptr.is_null() {
                node_string.push_str(&format!("\n\t{:?}", *start_ptr));
                start_ptr = (*start_ptr).next.load(Ordering::Relaxed);
            }
        }
        node_string += "]";
        write!(f, "SegQueue{{ {} }}", node_string)
    }
}

struct Node<T: Send + Debug> {
    data: Vec<AtomicPtr<Option<T>>>,
    next: AtomicPtr<Node<T>>,
    deleted: bool
}   

impl<T: Send + Debug> Node<T> {
    fn new(k: usize) -> Self {
        let mut data = Vec::new();
        for _ in 0..k {
            data.push(AtomicPtr::new(Box::into_raw(Box::new(None))));
        }
        Node {
            data,
            next: AtomicPtr::default(),
            deleted: false
        }
    }
}

impl<T: Send + Debug> Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut vals_string = "[".to_owned();
        unsafe {
            for atom_ptr in &self.data {
                let ptr = atom_ptr.load(Ordering::Relaxed);
                if !ptr.is_null() {
                    vals_string.push_str(&format!("({:?}: {:?})", atom_ptr, *ptr));
                }
            }
        }
        vals_string += "]";
        write!(f, "Node {{ Values: {}, Next: {:?} }}", &vals_string, self.next)
    }
}

mod tests {
    use super::SegQueue;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_enqueue() {
        let queue: SegQueue<u8> = SegQueue::new(4);

        let mut poss_set: HashSet<u8> = HashSet::new();

        queue.enqueue(3);
        poss_set.insert(3);
        queue.enqueue(4);
        poss_set.insert(4);
        queue.enqueue(5);
        poss_set.insert(5);
        queue.enqueue(6);
        poss_set.insert(6);

        queue.enqueue(7);

        println!("{:?}", queue);
        
        let res = queue.dequeue().unwrap();
        assert!(poss_set.contains(&res));
        poss_set.remove(&res);

        let res = queue.dequeue().unwrap();
        assert!(poss_set.contains(&res));
        poss_set.remove(&res);

        let res = queue.dequeue().unwrap();
        assert!(poss_set.contains(&res));
        poss_set.remove(&res);

        let res = queue.dequeue().unwrap();
        assert!(poss_set.contains(&res));
        poss_set.remove(&res);

        println!("{:?}", queue);

        assert_eq!(Some(7), queue.dequeue());
        assert_eq!(None, queue.dequeue());

        println!("{:?}", queue);
    }

    #[test]
    fn test_with_contention() {
        let mut queue: Arc<SegQueue<u16>> = Arc::new(SegQueue::new(20));
        
        let mut waitvec: Vec<thread::JoinHandle<()>> = Vec::new();

        for thread_no in 0..20 {
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
                    let mut num = 0;
                    loop {
                        match queue_copy.dequeue() {
                            Some(_) => {num = 0; break},
                            None => {
                                num += 1;
                                if num > 1000 {
                                    println!("{:?}", queue_copy);
                                    num = 0;
                                }
                            } 
                        }
                    }
                }
                println!("Pop thread {} complete", thread_no);
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