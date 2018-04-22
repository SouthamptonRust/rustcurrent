use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering::{Relaxed, Release, Acquire}};
use std::ptr;
use super::HashMap;
use std::{thread, thread::ThreadId};
use std::time::Duration;
use std::cell::UnsafeCell;
use rand::{Rng, SmallRng, NewRng};
use memory::HPBRManager;
use std::mem;

/// A lock-free stack with optional elimination backoff.
///
/// This is an implementation of a [Treiber Stack](http://domino.research.ibm.com/library/cyberdig.nsf/papers/58319A2ED2B1078985257003004617EF/$File/rj5118.pdf)
/// with an optional elimination backoff layer as described by [Colvin and Groves](http://ieeexplore.ieee.org/document/4343950/).
/// If the elimination layer is turned on, then when the stack is heavily contended, operations will
/// attempt to match each other to exchange values without touching the stack at all, in a attempt to
/// increase scalability.
/// 
/// The stack can be used in a multithreaded context by wrapping it in an Arc.
/// # Usage
/// ```
/// let stack: Arc<Stack<u8>> = Arc::new(Stack::new(true));
/// for _ in 0..8 {
///     let stack_clone = stack.clone();
///     thread::spawn(move || {
///         stack.push(8);
///         stack.pop();
///     });
/// }
/// ```

pub struct Stack<T: Send> {
    head: AtomicPtr<Node<T>>,
    elimination: EliminationLayer<T>,
    manager: HPBRManager<Node<T>>,
    elimination_on: bool
}


struct Node<T: Send> {
    data: Option<T>,
    next: AtomicPtr<Node<T>>
}

impl<T: Send> Stack<T> {
    /// Create a new stack, with or without elimination layer.
    /// # Examples
    /// ```
    /// let stack: Stack<u8> = Stack::new(true);
    /// ```
    pub fn new(elimination_on: bool) -> Stack<T> {
        Stack {
            head: AtomicPtr::default(),
            elimination: EliminationLayer::new(5),
            manager: HPBRManager::new(200, 1),
            elimination_on
        }
    }

    pub fn new_with_collision_size(elimination_on: bool, collision_size: usize) -> Self {
        Self {
            head: AtomicPtr::default(),
            elimination: EliminationLayer::new(collision_size),
            manager: HPBRManager::new(200, 1),
            elimination_on
        }
    }

    /// Push a piece of data onto the stack. This operation blocks until success,
    /// which is guaranteed by the lock-free data structure.
    /// # Examples
    /// ```
    /// let stack: Stack<String> = Stack::new(true);
    /// stack.push("hello".to_owned());
    /// ```
    pub fn push(&self, val: T) {
        let mut node_ptr = Box::into_raw(Box::new(Node::new(val)));
        let mut thread_info_ptr: *mut ThreadInfo<T> = ptr::null_mut();
        loop {
            node_ptr = match self.try_push(node_ptr) {
                Ok(_) => {
                    if !thread_info_ptr.is_null() {
                        // Make sure this doesn't need to be done with the memory manager
                        unsafe { Box::from_raw(thread_info_ptr) };
                    } 
                    return; 
                }
                Err(old_node) => old_node
            };
            if thread_info_ptr.is_null() {
                thread_info_ptr = Box::into_raw(Box::new(ThreadInfo::new(Some(node_ptr), OpType::Push)));
            }
            if self.elimination_on {
                match self.elimination.try_eliminate(thread_info_ptr, OpType::Push) {
                    Ok(_) => {
                        return
                    },
                    Err(_) => {}
                }
            }
        }
    }

    fn try_push(&self, node_ptr: *mut Node<T>) -> Result<(), *mut Node<T>> {
        let old_head = self.head.load(Acquire);
        unsafe { (*node_ptr).next = AtomicPtr::new(old_head) };

        match self.head.compare_exchange(old_head, node_ptr, Release, Relaxed) {
            Ok(_) => Ok(()),
            Err(_) => {
                Err(node_ptr)
            }
        }
    }

    /// Pop a piece of data from the top of the stack, or return None if the stack
    /// is empty. Blocks until success.
    /// # Examples
    /// ```
    /// let stack: Stack<String> = Stack::new(true);
    /// stack.push("hello".to_owned());
    /// assert_eq!(stack.pop(), "hello".to_owned()); 
    /// ```
    pub fn pop(&self) -> Option<T> {
        let mut thread_info_ptr: *mut ThreadInfo<T> = ptr::null_mut();
        loop {
            if let Ok(val) = self.try_pop() {
                if !thread_info_ptr.is_null() {
                    unsafe { Box::from_raw(thread_info_ptr) };
                }
                return val
            }
            if thread_info_ptr.is_null() {
                thread_info_ptr = Box::into_raw(Box::new(ThreadInfo::new(None, OpType::Pop)));
            }
            if self.elimination_on {
                if let Ok(val) = self.elimination.try_eliminate(thread_info_ptr, OpType::Pop) {
                    unsafe { Box::from_raw(thread_info_ptr) };
                    return val
                }
            }
        }
    }

    fn try_pop(&self) -> Result<Option<T>, ()> {
        let old_head = self.head.load(Acquire);
        if old_head.is_null() {
            return Ok(None)
        }
        unsafe {
            self.manager.protect(old_head, 0);
            if !ptr::eq(old_head, self.head.load(Acquire)) {
                return Err(())
            }
            let new_head = (*old_head).next.load(Acquire);
            match self.head.compare_exchange_weak(old_head, new_head, Release, Relaxed) {
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

fn get_id() -> usize {
    unsafe { mem::transmute::<ThreadId, u64>(thread::current().id()) as usize } 
}

impl<T: Send> Default for Stack<T> {
    fn default() -> Self {
        Self {
            head: AtomicPtr::default(),
            elimination: EliminationLayer::new(5),
            manager: HPBRManager::new(200, 1),
            elimination_on: false
        }
    }
}

impl<T: Send> Drop for Stack<T> {
    // We can assume that when drop is called, the program holds no more references to the stack
    // This means we can walk the stack, freeing all the data within
    fn drop(&mut self) {
        let mut current = self.head.load(Relaxed);
        while !ptr::eq(current, ptr::null()) {
            unsafe {
                let next = (*current).next.load(Relaxed);
                Box::from_raw(current);
                current = next;
            }
        }
    }
}

impl<T: Send> Node<T> {
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

impl<T: Send> Default for Node<T> {
    fn default() -> Self {
        Node {
            data: None,
            next: AtomicPtr::default()
        }
    }
} 

struct EliminationLayer<T: Send> {
    location: HashMap<usize, AtomicPtr<ThreadInfo<T>>>,
    collision: Vec<AtomicUsize>,
    rng: UnsafeCell<SmallRng>,
    manager: HPBRManager<ThreadInfo<T>>
}

unsafe impl<T: Send> Sync for EliminationLayer<T> {}

struct ThreadInfo<T: Send> {
    id: usize,
    op: OpType,
    node: Option<*mut Node<T>>
}

unsafe impl<T: Send> Send for ThreadInfo<T> {} 

#[derive(Copy)]
#[derive(Clone)]
enum OpType {
    Push,
    Pop
}

// Segfault is on the nodes, not the thread info. How to manage this?
impl<T: Send> EliminationLayer<T> {
    fn new(collision_size: usize) -> Self {
        let mut collision = Vec::with_capacity(collision_size);
        for _ in 0..collision_size {
            collision.push(AtomicUsize::new(usize::max_value()))
        }
        Self {
            location: HashMap::new(),
            collision,
            rng: UnsafeCell::new(SmallRng::new()),
            manager: HPBRManager::new(100, 2)
        }
    }

    fn try_eliminate(&self, me_info_ptr: *mut ThreadInfo<T>, op: OpType) -> Result<Option<T>, ()> {
        let me_id = get_id();

        match self.location.get(&me_id) {
            None => {
                match self.location.insert(me_id, AtomicPtr::new(me_info_ptr)) {
                    Ok(()) => {},
                    Err(_) => {
                        return Err(())
                    }
                }
            },
            Some(data_guard) => {
                data_guard.data().store(me_info_ptr, Release);
            }
        }

        let position = self.get_position();

        let mut them_id = self.collision[position].load(Acquire);
        while let Err(current) = self.collision[position].compare_exchange(them_id, me_id, Release, Relaxed) {
            them_id = current;
        }

        if them_id != usize::max_value() {
            match self.location.get(&them_id) {
                None => {},
                Some(data_guard) => {
                    let them_info_ptr = data_guard.data().load(Acquire);
                    self.manager.protect(them_info_ptr, 0);

                    if is_complimentary(them_id, them_info_ptr, data_guard.data(), op) {
                        let me_atomic = self.location.get(&me_id).unwrap().data();
                        match me_atomic.compare_exchange(me_info_ptr, ptr::null_mut(), Release, Relaxed) {
                            Ok(_) => {
                                return self.try_collision(me_info_ptr, them_info_ptr, data_guard.data(), me_atomic)
                            },
                            Err(current) => {
                                let ret_val = self.finish_collision(current, op);
                                self.manager.unprotect(0);
                                return ret_val
                            }
                        }                        
                    }
                    self.manager.unprotect(0); 
                }
            }
        }
        thread::sleep(Duration::new(0, 100));
        let me_atomic = self.location.get(&me_id).unwrap().data();
        match me_atomic.compare_exchange(me_info_ptr, ptr::null_mut(), Release, Relaxed) {
            Ok(_) => {
                return Err(())
            },
            Err(current) => return self.finish_collision(current, op)
        }
    }

    fn get_position(&self) -> usize {
        let rand = unsafe { &mut *self.rng.get() };
        rand.gen_range(0, self.collision.len())
    }

    fn try_collision(&self, me_ptr: *mut ThreadInfo<T>, them_ptr: *mut ThreadInfo<T>, 
                     them_atomic: &AtomicPtr<ThreadInfo<T>>, me_atomic: &AtomicPtr<ThreadInfo<T>>)
            -> Result<Option<T>, ()> 
    {
        let me = unsafe { &*me_ptr };

        match me.op {
            OpType::Push => {
                match them_atomic.compare_exchange(them_ptr, me_ptr, Release, Relaxed) {
                    Ok(_) => {
                        self.manager.unprotect(0);
                        return Ok(None)
                    },
                    Err(_) => {
                        self.manager.unprotect(0);
                        return Err(())
                    }
                }
            },
            OpType::Pop => {
                match them_atomic.compare_exchange(them_ptr, ptr::null_mut(), Release, Relaxed) {
                    Ok(_) => {
                        let mut owned_info = unsafe { ptr::read(them_ptr) };
                        let mut node_ptr = mem::replace(&mut owned_info.node, None).unwrap();
                        let node = unsafe { ptr::replace(node_ptr, Node::default()) };
                        unsafe { Box::from_raw(node_ptr) };
                        self.manager.retire(them_ptr, 0);
                        me_atomic.store(ptr::null_mut(), Release);
                        return Ok(node.data)
                    },
                    Err(_) => {
                        self.manager.unprotect(0);
                        return Err(())
                    }
                }
            }
        }
    }

    fn finish_collision(&self, new_info_ptr: *mut ThreadInfo<T>, me_op: OpType) -> Result<Option<T>, ()> {
        match me_op {
            OpType::Push => { return Ok(None) },
            OpType::Pop => {
                let mut owned_info = unsafe { ptr::read(new_info_ptr) };
                let mut node_ptr = mem::replace(&mut owned_info.node, None).unwrap();
                let node = unsafe { ptr::replace(node_ptr, Node::default()) };
                unsafe { Box::from_raw(node_ptr) };
                self.location.get(&get_id()).unwrap().data().store(ptr::null_mut(), Release);
                self.manager.retire(new_info_ptr, 0);
                return Ok(node.data)
            }
        }
    } 
}

fn is_complimentary<T: Send>(them_id: usize, them_ptr: *mut ThreadInfo<T>, 
                             them_atomic: &AtomicPtr<ThreadInfo<T>>, me_op: OpType) -> bool 
{
    if them_ptr.is_null() || !ptr::eq(them_atomic.load(Acquire), them_ptr) {
        return false
    }
    
    let them_info = unsafe { &*them_ptr };

    if them_id == them_info.id {
        return match (them_info.op, me_op) {
            (OpType::Pop, OpType::Push) => true,
            (OpType::Push, OpType::Pop) => true,
            _ => false
        }
    }

    false
}

impl<T: Send> Drop for EliminationLayer<T> {
    fn drop(&mut self) {
        for guard in self.location.iter() {
            let ptr = guard.data().load(Relaxed);
            if !ptr.is_null() {
                unsafe { Box::from_raw(ptr) };
            }
        }
    }
}

impl<T: Send> ThreadInfo<T> {
    fn new(node: Option<*mut Node<T>>, op: OpType) -> Self {
        Self {
            id: get_id(),
            op,
            node
        }
    }
}

mod tests {
    #![allow(unused_imports)]
    extern crate im;
    use self::im::Vector;

    use rand::{thread_rng, Rng};

    use super::Stack;
    use super::get_id;
    use super::super::super::testing::linearizability_tester::{LinearizabilityTester, ThreadLog};

    use std::sync::atomic::Ordering;
    use std::{thread, thread::ThreadId};
    use std::sync::Arc;
    use std::cell::RefCell;
    use std::mem;
    use std::collections;

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
        let stack : Stack<u8> = Stack::new(true);

        stack.push(4u8);
        //println!("{:?}", stack);
        stack.push(3);
        //println!("{:?}", stack);
        stack.push(1);
        //println!("{:?}", stack);
        unsafe {
            let val = (*stack.head.load(Ordering::Relaxed)).data;
            assert_eq!(val, Some(1));
            let next_val = (*(*stack.head.load(Ordering::Relaxed)).next.load(Ordering::Relaxed)).data;
            assert_eq!(next_val, Some(3));
        }
    }

    fn test_pop_single_threaded() {
        let stack : Stack<Foo> = Stack::new(true);

        stack.push(Foo {data: 1});
        stack.push(Foo {data: 2});
        stack.push(Foo {data: 4});

        //println!("{:?}", stack.manager);

        assert_eq!(stack.pop(), Some(Foo {data: 4}));
        assert_eq!(stack.pop(), Some(Foo {data: 2}));
        assert_eq!(stack.pop(), Some(Foo {data: 1}));
        assert_eq!(stack.pop(), None);
        assert_eq!(stack.pop(), None);

        //println!("{:?}", stack.manager);
    }

    #[test]
    #[ignore]
    fn test_thread_id() {
        let threadid = thread::current().id();
        let num_id = get_id();

        assert_eq!(unsafe { mem::transmute::<usize, ThreadId>(num_id) }, threadid);
        assert_eq!(get_id(), unsafe { mem::transmute::<ThreadId, usize>(threadid) });
    }

    #[test]
    #[ignore]
    fn stress_test_elimination() {
        let stack: Arc<Stack<u8>> = Arc::new(Stack::new(true));
        let mut waitvec: Vec<thread::JoinHandle<()>> = Vec::new();
        for thread_no in 0..20 {
            let stack_copy = stack.clone();
            waitvec.push(thread::spawn(move || {
                for i in 0..10000 {
                    stack_copy.push(2);
                }
                println!("push thread {} complete", thread_no);
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
                println!("pop thread {} finished", thread_no);
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

    #[test]
    #[ignore]
    fn test_linearizable() {
        let stack: Stack<usize> = Stack::new(true);
        let sequential: Vector<usize> = Vector::new();
        let mut linearizer: LinearizabilityTester<Stack<usize>, Vector<usize>, usize> 
                = LinearizabilityTester::new(8, 1000000, stack, sequential);

        fn sequential_pop(stack: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
            match stack.pop_back() {
                Some((arc, vec)) => {
                    let res = *arc;
                    (vec, Some(res))
                },
                None => (Vector::new(), None)
            }
        }

        fn sequential_push(stack: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
            (stack.push_back(val.unwrap()), None)
        }

        fn worker(id: usize, log: &mut ThreadLog<Stack<usize>, Vector<usize>, usize>) {
            for _ in 0..1000 {
                let rand = thread_rng().gen_range(0, 101);
                if rand < 30 {
                    // push
                    let val = thread_rng().gen_range(0, 122222);
                    log.log_val(id, Stack::push, val, format!("push: {}", val), sequential_push);
                } else {
                    // pop
                    log.log(id, Stack::pop, "pop".to_owned(), sequential_pop)
                }
            }
        }

        let result = linearizer.run(worker);

        println!("{:?}", result);
    }
}