use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use std::fmt::Debug;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::cell::UnsafeCell;
use rand::{Rng};
use rand;
use memory::HPBRManager;
use std::mem;

// TODO - find out why data starts to set itself to impossible address - check address of data at literally every stage

#[derive(Debug)]
pub struct Stack<T: Send + Debug> {
    head: AtomicPtr<Node<T>>,
    elimination: EliminationLayer<T>,
    manager: HPBRManager<Node<T>>,
    elimination_on: bool
}

#[derive(Debug)]
pub struct Node<T: Debug> {
    data: Option<T>,
    next: AtomicPtr<Node<T>>
}

impl<T: Send + Debug> Stack<T> {
    pub fn new(elimination_on: bool) -> Stack<T> {
        Stack {
            head: AtomicPtr::default(),
            elimination: EliminationLayer::new(40, 5),
            manager: HPBRManager::new(3000, 1),
            elimination_on
        }
    }

    pub fn push(&self, val: T) {
        let mut node = Box::new(Node::new(val));
        loop {
            node = match self.try_push(node) {
                Ok(_) => { return; }
                Err(old_node) => old_node
            };
            if self.elimination_on {
                let mut data = mem::replace(&mut node.data, None);
                data = match self.elimination.try_eliminate(OpType::Push, data) {
                    Ok(_) => { return; }
                    Err(old_node) => old_node
                };
                node.data = data;
            }
            println!("Fail node: {:?} for thread {:?}", node, thread::current().id());
        }
    }

    fn try_push(&self, mut node: Box<Node<T>>) -> Result<(), Box<Node<T>>> {
        let old_head = self.head.load(Ordering::Acquire);
        node.next = AtomicPtr::new(old_head);

        let node_ptr = Box::into_raw(node);
        match self.head.compare_exchange_weak(old_head, node_ptr, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(_) => {
                unsafe {
                    Err(Box::from_raw(node_ptr))
                }
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_pop() {
                return val
            }
            if self.elimination_on {
                if let Ok(val) = self.elimination.try_eliminate(OpType::Pop, None) {
                    return val
                }
            }
        }
    }

    pub fn try_pop(&self) -> Result<Option<T>, ()> {
        let old_head = self.head.load(Ordering::Acquire);
        if old_head.is_null() {
            return Ok(None)
        }
        unsafe {
            self.manager.protect(old_head, 0);
            if !ptr::eq(old_head, self.head.load(Ordering::Acquire)) {
                return Err(())
            }
            let new_head = (*old_head).next.load(Ordering::Acquire);
            match self.head.compare_exchange_weak(old_head, new_head, Ordering::AcqRel, Ordering::Acquire) {
                Err(_) => Err(()),
                Ok(old_head) => {
                    let old_head_val = ptr::replace(old_head, Node::default());
                    let data = old_head_val.data;
                    self.manager.retire(old_head, 0);
                    Ok(data)
                }
            }
        }
    }
}

impl<T: Debug + Send> Drop for Stack<T> {
    // We can assume that when drop is called, the program holds no more references to the stack
    // This means we can walk the stack, freeing all the data within
    fn drop(&mut self) {
        let mut current = self.head.load(Ordering::Relaxed);
        let mut count = 0;
        while !ptr::eq(current, ptr::null()) {
            unsafe {
                let next = (*current).next.load(Ordering::Relaxed);
                Box::from_raw(current);
                current = next;
            }
            count += 1;
        }
    }
}

impl<T: Debug> Node<T> {
    fn new_as_pointer(val: T) -> *mut Self {
        Box::into_raw(Box::new(Node {
            data: Some(val),
            next: AtomicPtr::default()
        }))
    }

    fn new(val: T) -> Self {
        Node {
            data: Some(val),
            next: AtomicPtr::default()
        }
    }
}

impl<T: Debug> Default for Node<T> {
    fn default() -> Self {
        Node {
            data: None,
            next: AtomicPtr::default()
        }
    }
} 

#[derive(Debug)]
struct EliminationLayer<T: Send + Debug> {
    operations: UnsafeCell<HashMap<thread::ThreadId, AtomicPtr<OpInfo<T>>>>,
    collisions: Vec<AtomicPtr<Option<thread::ThreadId>>>,
    collision_size: usize,
    manager: HPBRManager<OpInfo<T>>
}

unsafe impl<T: Debug + Send> Sync for EliminationLayer<T> {}

impl<T: Debug + Send> Drop for EliminationLayer<T> {
    fn drop(&mut self) {
        unsafe {
            // Delete all the opinfos pointed to by the map
            let operations = &mut *self.operations.get();
            for (_, ptr) in operations.into_iter() {
                let raw_ptr = ptr.load(Ordering::Relaxed);
                if !ptr::eq(raw_ptr, ptr::null()) && !self.manager.check_if_hazard(raw_ptr) {
                    Box::from_raw(raw_ptr);
                }
            }

            // Clear out the collision vector
            for ptr in self.collisions.iter() {
                if !ptr::eq(ptr, ptr::null()) {
                    Box::from_raw(ptr.load(Ordering::Relaxed));
                }
            }
        }
    }
}

impl<T: Debug + Send> EliminationLayer<T> {
    fn new(max_threads: usize, collision_size: usize) -> Self {
        let mut collisions: Vec<AtomicPtr<Option<thread::ThreadId>>> = Vec::new();
        for _ in 0..collision_size {
            collisions.push(AtomicPtr::new(Box::into_raw(Box::default())))
        }
        EliminationLayer {
            operations: UnsafeCell::new(HashMap::with_capacity(max_threads)),
            collisions,
            collision_size,
            manager: HPBRManager::new(1, 3)
        }
    }

    // TODO finish writing this
    fn try_eliminate(&self, op: OpType, data: Option<T>) -> Result<Option<T>, Option<T>> {
        let op_info = OpInfo::new(op.clone(), data);
        let op_info_ptr = Box::into_raw(Box::new(op_info));
        self.manager.protect(op_info_ptr, 0);
        let thread_id = thread::current().id();

        unsafe {
            let operations = self.get_mut_operations();
            operations.entry(thread_id)
                      .or_insert(AtomicPtr::default())
                      .store(op_info_ptr, Ordering::Release);
        }

        let them_pos = self.choose_position();
        let mut them_ptr = ptr::null_mut();
        loop {
            them_ptr = self.collisions[them_pos].load(Ordering::Acquire);
            let me = Box::into_raw(Box::new(Some(thread_id)));
            if self.collisions[them_pos].compare_exchange_weak(them_ptr, me, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                break;
            }
        }

        unsafe {
            let them = match *them_ptr {
                Some(id) => id,
                None => {
                    let my_info = ptr::replace(op_info_ptr, OpInfo::default());
                    let data = ptr::replace(my_info.data, None);
                    return Err(data);
                }
            };
            let operations = self.get_mut_operations();
            let them_atomic = operations.get(&them).unwrap();
            let them_info_ptr = them_atomic.load(Ordering::Acquire);
            self.manager.protect(them_info_ptr, 1);

            if OpType::check_complimentary(op.clone(), (*them_info_ptr).operation.clone()) {
                let me_atomic = operations.get(&thread_id).unwrap();
                let mut new_none = OpInfo::new_done_as_pointer((*op_info_ptr).data);
                if me_atomic.compare_exchange_weak(op_info_ptr, new_none, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    new_none = OpInfo::new_done_as_pointer((*op_info_ptr).data);
                    if them_atomic.compare_exchange_weak(them_info_ptr, new_none, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        println!("{:?} eliminated active with {:?}", thread_id, them);
                        
                        // Can retire our info, since the one in the data structure is "new_none"
                        self.manager.retire(op_info_ptr, 0);

                        // Take ownership of their info, since the one in the structure is "new_none"
                        let info = ptr::replace(them_info_ptr, OpInfo::default());
                        self.manager.retire(them_info_ptr, 1);

                        let data = ptr::replace(info.data, None);
                        // Delete the empty data
                        Box::from_raw(info.data);

                        return Ok(data);
                    } else {
                        println!("{:?} failed to eliminate active", thread_id);

                        // Free the unused none_ptr
                        Box::from_raw(new_none);
                        // Don't need to protect them_ptr anymore
                        self.manager.unprotect(1);
                        
                        // Free our info, since the one in the data structure is "new_none"
                        let my_info = ptr::replace(op_info_ptr, OpInfo::default());
                        self.manager.retire(op_info_ptr, 0);
                        let data = ptr::replace(my_info.data, None);
                        Box::from_raw(my_info.data);
                        return Err(data);
                    } 
                } else {
                    println!("{:?} eliminated passive!", thread_id);
                    // If my info has been swapped out, then someone else will have freed my info
                    // Free the unused none ptr
                    Box::from_raw(new_none);
                    
                    let info_ptr = me_atomic.load(Ordering::Acquire);
                    self.manager.protect(info_ptr, 0);
                    let new_info = ptr::replace(info_ptr, OpInfo::default());
                    // Retire the info we claimed
                    self.manager.retire(info_ptr, 0);

                    let data = ptr::replace(new_info.data, None);
                    Box::from_raw(new_info.data);

                    return Ok(data);
                }
            }
        }

        thread::sleep(Duration::from_millis(20));

        unsafe {
            let operations = self.get_mut_operations();
            let me_atomic = operations.get(&thread_id).unwrap();
            
            let new_none = OpInfo::new_done_as_pointer((*op_info_ptr).data);
            if me_atomic.compare_exchange_weak(op_info_ptr, new_none, Ordering::AcqRel, Ordering::Acquire).is_err() {
                println!("{:?} eliminated passive!", thread_id);
                // Free the unused none node
                Box::from_raw(new_none);

                let info_ptr = me_atomic.load(Ordering::Acquire);
                self.manager.protect(info_ptr, 0);
                let new_info = ptr::replace(info_ptr, OpInfo::default());
                self.manager.retire(info_ptr, 0);
                let data = ptr::replace(new_info.data, None);

                Box::from_raw(new_info.data);
                return Ok(data);
            } else {
                // I swapped my info out so I'm the only one holding it
                let my_info = ptr::replace(op_info_ptr, OpInfo::default());
                self.manager.retire(op_info_ptr, 0);
                let data = ptr::replace(my_info.data, None);
                Box::from_raw(my_info.data);
                Err(data)
            }
        }
    }

    fn get_eliminate_partner(&self, me: thread::ThreadId) -> Option<thread::ThreadId> {
        let position = self.choose_position();
        let me_ptr = Box::into_raw(Box::new(Some(me)));
        unsafe {
            loop {
                let them_ptr = self.collisions[position].load(Ordering::Acquire);
                if let Ok(ptr) = self.collisions[position].compare_exchange_weak(them_ptr, me_ptr, Ordering::AcqRel, Ordering::Acquire) {
                    return *ptr
                }
            }
        }
    }

    // This function can also handle retiring
    fn get_data(&self, ptr: *mut OpInfo<T>, op: &OpType) -> Option<T> {
        match op {
            &OpType::Push => {
                self.manager.unprotect(0);
                self.manager.unprotect(1);
                None
            },
            &OpType::Pop => {
                unsafe {
                    println!("Using opinfo: {:?}, {:?}", *ptr, thread::current().id());
                    let info = ptr::replace(ptr, OpInfo::default());
                    println!("{:?}", info.data);
                    let data = ptr::replace(info.data, None);
                    self.manager.retire(ptr, 1);
                    return data
                }
            },
            &OpType::Done => {
                println!("Error occurred, read incompatible op node");
                None
            }
        }
    }

    fn choose_position(&self) -> usize {
        return rand::thread_rng().gen_range(0, self.collision_size);
    }

    unsafe fn get_mut_operations(&self) -> &mut HashMap<thread::ThreadId, AtomicPtr<OpInfo<T>>> {
        &mut *self.operations.get()
    }
}

//unsafe impl<T: Debug> Sync for EliminationLayerOld<T> {}

#[derive(Debug)]
struct OpInfo<T: Debug + Send> {
    operation: OpType,
    data: *mut Option<T>
}

impl<T: Debug + Send> Drop for OpInfo<T> {
    fn drop(&mut self) {
        println!("========================== Dropping OpInfo");
    }
}

unsafe impl<T: Debug + Send> Send for OpInfo<T> {}

impl<T: Debug + Send> OpInfo<T> {
    fn new(operation: OpType, data: Option<T>) -> Self {
        OpInfo {
            operation,
            data: Box::into_raw(Box::new(data))
        }
    }

    fn new_as_ptr(operation: OpType, data: Option<T>) -> *mut Self {
        let data_ptr = Box::into_raw(Box::new(data));
        Box::into_raw(Box::new(OpInfo {
            operation,
            data: data_ptr
        }))
    }

    // Should probably free the opinfo pointer
    fn get_boxed_node(opinfo_ptr: *mut Self) -> Option<T> {
        unsafe {
            println!("Getting box node for thread: {:?} ------ op info is: {:?}", thread::current().id(), *opinfo_ptr);
            let opinfo = ptr::replace(opinfo_ptr, OpInfo::default());
            println!("{:?}", opinfo);
            ptr::replace(opinfo.data, None)
        }
    }

    fn new_done_as_pointer(data: *mut Option<T>) -> *mut Self {
        Box::into_raw(Box::new(OpInfo {
            operation: OpType::Done,
            data
        }))
    }
}

impl<T: Debug + Send> Default for OpInfo<T> {
    fn default() -> Self {
        OpInfo {
            operation: OpType::Done,
            data: Box::into_raw(Box::default())
        }
    }
}

#[derive(Debug)]
#[derive(Clone)]
enum OpType {
    Pop,
    Push,
    Done
}

impl OpType {
    fn check_complimentary(op1: OpType, op2: OpType) -> bool {
        match (op1, op2) {
            (OpType::Push, OpType::Pop) => true,
            (OpType::Pop, OpType::Push) => true,
            (_, _) => false
        }
    }
}

mod tests {
    use super::Stack;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::sync::Arc;
    use std::cell::RefCell;

    #[derive(Debug)]
    #[derive(PartialEq)]
    struct Foo {
        data: u8
    }
    
    impl Drop for Foo {
        fn drop(&mut self) {
            println!("Dropping: {:?}", self.data);
        }
    }

    fn test_push_single_threaded() {
        let mut stack : Stack<u8> = Stack::new(true);

        stack.push(4u8);
        println!("{:?}", stack);
        stack.push(3);
        println!("{:?}", stack);
        stack.push(1);
        println!("{:?}", stack);
        unsafe {
            let val = (*stack.head.load(Ordering::Relaxed)).data;
            assert_eq!(val, Some(1));
            let next_val = (*(*stack.head.load(Ordering::Relaxed)).next.load(Ordering::Relaxed)).data;
            assert_eq!(next_val, Some(3));
        }
    }

    fn test_pop_single_threaded() {
        let mut stack : Stack<Foo> = Stack::new(true);

        stack.push(Foo {data: 1});
        stack.push(Foo {data: 2});
        stack.push(Foo {data: 4});

        println!("{:?}", stack.manager);

        assert_eq!(stack.pop(), Some(Foo {data: 4}));
        assert_eq!(stack.pop(), Some(Foo {data: 2}));
        assert_eq!(stack.pop(), Some(Foo {data: 1}));
        assert_eq!(stack.pop(), None);
        assert_eq!(stack.pop(), None);

        println!("{:?}", stack.manager);
    }

    #[ignore]
    fn test_thread_id() {
        for i in 0..10 {
            thread::spawn(|| {
                println!("{:?}", thread::current().id());
            });
        }
    }

    #[test]
    fn test_elimination_no_segfault() {
        let stack: Arc<Stack<u8>> = Arc::new(Stack::new(true));
        let mut waitvec: Vec<thread::JoinHandle<()>> = Vec::new();
        for _ in 0..20 {
            let stack_copy = stack.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    stack_copy.push(2);
                }
            }));
        }
        for thread_no in 0..20 {
            let stack_copy = stack.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    loop {
                        match stack_copy.pop() {
                            Some(n) => { break },
                            None => ()
                        }
                    }
                }
            }));
        }
        for handle in waitvec {
            match handle.join() {
                Ok(_) => {},
                Err(some) => println!("Couldn't join! {:?}", some) 
            }
        }
        println!("Joined all");
        assert_eq!(None, stack.pop());
    }
}