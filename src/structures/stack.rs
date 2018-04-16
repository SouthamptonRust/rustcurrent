use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use std::collections;
use super::HashMap;
use std::{thread, thread::ThreadId};
use std::time::Duration;
use std::cell::UnsafeCell;
use rand::{Rng, SmallRng, NewRng};
use rand;
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
            elimination: EliminationLayer::new(),
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
        let mut node = Box::new(Node::new(val));
        loop {
            node = match self.try_push(node) {
                Ok(_) => { return; }
                Err(old_node) => old_node
            };
            if self.elimination_on {
                node = match self.elimination.try_eliminate(Some(node), OpType::Push) {
                    Ok(_) => { return; },
                    Err(node) => node.unwrap()
                };
            }
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

    /// Pop a piece of data from the top of the stack, or return None if the stack
    /// is empty. Blocks until success.
    /// # Examples
    /// ```
    /// let stack: Stack<String> = Stack::new(true);
    /// stack.push("hello".to_owned());
    /// assert_eq!(stack.pop(), "hello".to_owned()); 
    /// ```
    pub fn pop(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_pop() {
                return val
            }
            if self.elimination_on {
                if let Ok(val) = self.elimination.try_eliminate(None, OpType::Pop) {
                    return val
                }
            }
        }
    }

    fn try_pop(&self) -> Result<Option<T>, ()> {
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

impl<T: Send> Drop for Stack<T> {
    // We can assume that when drop is called, the program holds no more references to the stack
    // This means we can walk the stack, freeing all the data within
    fn drop(&mut self) {
        let mut current = self.head.load(Ordering::Relaxed);
        while !ptr::eq(current, ptr::null()) {
            unsafe {
                let next = (*current).next.load(Ordering::Relaxed);
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
    location: HashMap<ThreadId, AtomicPtr<ThreadInfo<T>>>,
    rng: UnsafeCell<SmallRng>,
    manager: HPBRManager<ThreadInfo<T>>
}

struct ThreadInfo<T: Send> {
    id: ThreadId,
    op: OpType,
    node: Option<Node<T>>
}

enum OpType {
    Push,
    Pop
}

impl<T: Send> EliminationLayer<T> {
    fn new() -> Self {
        Self {
            location: HashMap::new(),
            rng: UnsafeCell::new(SmallRng::new()),
            manager: HPBRManager::new(100, 1)
        }
    }

    fn try_eliminate(&self, node: Option<Box<Node<T>>>, op: OpType) -> Result<Option<T>, Option<Box<Node<T>>>> {
        Ok(None)
    }
}

impl<T: Send> ThreadInfo<T> {
    fn new(node: Node<T>, op: OpType) -> Self {
        Self {
            id: thread::current().id(),
            op,
            node: Some(node)
        }
    }
}

mod tests {
    #![allow(unused_imports)]
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
        for i in 0..10 {
            thread::spawn(|| {
                println!("{:?}", thread::current().id());
            });
        }
    }

    #[test]
    #[ignore]
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