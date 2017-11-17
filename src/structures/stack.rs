use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use std::fmt::Debug;
use std::collections::HashMap;
use std::thread;

#[derive(Debug)]
pub struct Stack<T: Send + Sync + Debug> {
    head: AtomicPtr<Node<T>>
}

#[derive(Debug)]
pub struct Node<T: Debug> {
    data: Option<T>,
    next: AtomicPtr<Node<T>>
}

impl<'a, T: Send + Sync + Debug> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack {
            head: AtomicPtr::default()
        }
    }

    pub fn push(&mut self, val: T) {
        // Create a new node on the heap, with a pointer to it
        let node = Node::new_as_pointer(val);

        loop {
            if self.try_push(node) {
                break;
            }
        };
    }

    fn try_push(&mut self, node: *mut Node<T>) -> bool {
        let old_head = self.head.load(Ordering::Acquire);
        unsafe {
            (*node).next = AtomicPtr::new(old_head);
        }
        match self.head.compare_exchange_weak(old_head, node, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        loop {
            if let Ok(node) = self.try_pop() {
                if node.is_null() {
                    return None;
                } else {
                    unsafe {
                        return ptr::replace(node, Node {
                            data: None,
                            next: AtomicPtr::default()
                        }).data;
                            // Memory leak here, Node is never removed
                    }
                }
            }
        }
    }

    fn try_pop(&mut self) -> Result<*mut Node<T>, *mut Node<T>> {
        let old_head = self.head.load(Ordering::Acquire);
        if old_head.is_null() {
            return Ok(old_head);    
                // If null, return early to avoid accessing
        }
        unsafe {
            let new_head = (*old_head).next.load(Ordering::Acquire);
            self.head.compare_exchange_weak(old_head, new_head, Ordering::AcqRel, Ordering::Acquire)
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
}

struct EliminationLayer<T: Debug> {
    operations: HashMap<thread::ThreadId, AtomicPtr<OpInfo<T>>>,
        // If we bound the number of threads, and preallocate the HashMap,
        // it should be fine to access concurrently because rehashing will
        // never happen, as guaranteed by the runtime.
    collisions: Vec<AtomicPtr<Option<thread::ThreadId>>>
}

#[derive(Clone)]
struct OpInfo<T: Debug> {
    operation: Option<OpType>,
    node: *mut Node<T>
}

#[derive(Clone)]
enum OpType {
    Pop,
    Push
}

impl<T: Debug> EliminationLayer<T> {
    fn new(max_threads: usize, collision_size: usize) -> Self {
        let mut collisions = Vec::with_capacity(collision_size);
        for _ in 0..collision_size {
            collisions.push(AtomicPtr::new(Box::into_raw(Box::new(None))));
        }
        Self {
            operations: HashMap::with_capacity(max_threads),
            collisions: collisions
        }
    }

    fn try_eliminate(&mut self, opinfo: OpInfo<T>) -> Result<T, OpInfo<T>> {
        let my_info_ptr = Box::into_raw(Box::new(opinfo));
        self.operations.entry(thread::current().id()).or_insert(AtomicPtr::default()).store(my_info_ptr, Ordering::Acquire);
        let position = Self::choose_position();
        let mut them = ptr::null_mut();
        
        loop {
            them = self.collisions[position].load(Ordering::Acquire);
            let me = Box::into_raw(Box::new(Some(thread::current().id())));
            if self.collisions[position].compare_exchange_weak(them, me, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                break;
            }
        }

        let mut their_info: Option<&AtomicPtr<OpInfo<T>>> = None;
        unsafe {
            if (*them).is_none() {
                return Err(opinfo);
            }
            their_info = self.operations.get(&ptr::read(them).unwrap());
            if their_info.is_none() {
                return Err(opinfo);
            }
        }
        
        unsafe {
            let their_op = ptr::read(their_info.unwrap().load(Ordering::Acquire));
            let my_info = ptr::read(my_info_ptr);
            if my_info.check_complimentary(their_op.operation.as_ref()) {

            }
        }
        Err(opinfo)
    }

    fn choose_position() -> usize {
        unimplemented!();
    }
}

impl<T: Debug> OpInfo<T> {
    fn new_from_pointer(node: *mut Node<T>, op: OpType) -> Self {
        OpInfo {
            operation: Some(op),
            node
        }
    }

    fn new_as_pointer(node: *mut Node<T>, op: OpType) -> *mut Self {
        Box::into_raw(Box::new({
            OpInfo {
                operation: Some(op),
                node
            }
        }))
    }

    fn new_from_data(data: T, op: OpType) -> Self {
        OpInfo {
            operation: Some(op),
            node: Node::new_as_pointer(data)
        }
    }

    fn check_complimentary(&self, op: Option<&OpType>) -> bool {
        match (self.operation.as_ref(), op) {
            (Some(&OpType::Push), Some(&OpType::Pop)) => true,
            (Some(&OpType::Pop), Some(&OpType::Push)) => true,
            _ => false
        }
    }
}

mod tests {
    use super::Stack;
    use std::sync::atomic::Ordering;
    use std::thread;

    #[test]
    fn test_push_single_threaded() {
        let mut stack : Stack<u8> = Stack::new();

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

    #[test]
    fn test_pop_single_threaded() {
        let mut stack : Stack<u8> = Stack::new();

        stack.push(1);
        println!("{:?}", stack);
        stack.push(2);
        println!("{:?}", stack);
        stack.push(3);
        println!("{:?}", stack);

        assert_eq!(stack.pop(), Some(3));
        println!("{:?}", stack);
        assert_eq!(stack.pop(), Some(2));
        println!("{:?}", stack);
        assert_eq!(stack.pop(), Some(1));
        println!("{:?}", stack);
        assert_eq!(stack.pop(), None);
        assert_eq!(stack.pop(), None);
    }

    #[test]
    fn test_thread_id() {
        for i in 0..10 {
            thread::spawn(|| {
                println!("{:?}", thread::current().id());
            });
        }
    }
}