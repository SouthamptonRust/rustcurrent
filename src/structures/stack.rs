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

// TODO memory management

#[derive(Debug)]
pub struct Stack<T: Send + Debug> {
    head: AtomicPtr<Node<T>>,
    elimination: EliminationLayer<T>,
    manager: HPBRManager<Node<T>>
}

#[derive(Debug)]
pub struct Node<T: Debug> {
    data: Option<T>,
    next: AtomicPtr<Node<T>>
}

impl<T: Send + Debug> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack {
            head: AtomicPtr::default(),
            elimination: EliminationLayer::new(40, 5),
            manager: HPBRManager::new(100, 1)
        }
    }

    pub fn push(&self, val: T) {
        // Create a new node on the heap, with a pointer to it
        let node = Node::new_as_pointer(val);
        let opinfo_ptr = OpInfo::new_as_pointer(node, OpType::Push);
        loop {
            if self.try_push(node) {
                break;
            }
            if self.elimination.try_eliminate(opinfo_ptr).is_ok() {
                println!("{:?} Eliminated!", thread::current().id());
                break;
            }
            println!("{:?} Failed", thread::current().id());
        };
    }

    fn try_push(&self, node: *mut Node<T>) -> bool {
        let old_head = self.head.load(Ordering::Acquire);
        unsafe {
            (*node).next = AtomicPtr::new(old_head);
        }
        match self.head.compare_exchange_weak(old_head, node, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    pub fn pop(&self) -> Option<T> {
        let op_info_ptr = OpInfo::new_as_pointer(ptr::null_mut(), OpType::Pop);
        loop {
            if let Ok(node) = self.try_pop() {
                if node.is_null() {
                    return None;
                } else {
                    unsafe {
                        let node_val = ptr::replace(node, Node {data: None, next: AtomicPtr::default()});
                        let data = node_val.data;
                        // Retire the node
                        self.manager.retire(node, 0);
                        return data;
                    }
                }
            }
            if let Ok(val) = self.elimination.try_eliminate(op_info_ptr) {
                println!("{:?} Eliminated: {:?}", thread::current().id(), val);
                return val;
            }
            println!("{:?} Failed", thread::current().id());
        }
    }

    fn try_pop(&self) -> Result<*mut Node<T>, *mut Node<T>> {
        let old_head = self.head.load(Ordering::Acquire); // This is the pointer that needs protecting -- if it is freed, then the read of new_head will FAIL and segfault like a binch
        if old_head.is_null() {
            return Ok(old_head);    
                // If null, return early to avoid accessing
        }
        unsafe {
            self.manager.protect(old_head, 0);
            if !ptr::eq(old_head, self.head.load(Ordering::Acquire)) {
                return Err(old_head);
            }
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

#[derive(Debug)]
struct EliminationLayer<T: Debug> {
    operations: UnsafeCell<HashMap<thread::ThreadId, AtomicPtr<OpInfo<T>>>>,
        // If we bound the number of threads, and preallocate the HashMap,
        // it should be fine to access concurrently because rehashing will
        // never happen, as guaranteed by the runtime.
    collisions: Vec<AtomicPtr<Option<thread::ThreadId>>>,
    collision_size: usize
}

unsafe impl<T: Debug> Sync for EliminationLayer<T> {}

#[derive(Clone)]
#[derive(Debug)]
struct OpInfo<T: Debug> {
    operation: Option<OpType>,
    node: *mut Node<T>
}

#[derive(Debug)]
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
            operations: UnsafeCell::new(HashMap::with_capacity(max_threads)),
            collisions: collisions,
            collision_size
        }
    }

    fn try_eliminate(&self, my_info_ptr: *mut OpInfo<T>) -> Result<Option<T>, OpInfo<T>> {
        println!("{:?} Let's eliminate", thread::current().id());
        
        unsafe {
            println!("{:?} Store my info {:?}", thread::current().id(), ptr::read(my_info_ptr));
            self.operations.get().as_mut().unwrap().entry(thread::current().id()).or_insert(AtomicPtr::default()).store(my_info_ptr, Ordering::Release);
        }
        let position = self.choose_position();
        //println!("{:?} Colliding at: {}", thread::current().id(), position);             
        let mut them = ptr::null_mut();
        
        println!("{:?} Read their info", thread::current().id());
        loop {
            them = self.collisions[position].load(Ordering::Acquire);
            let me = Box::into_raw(Box::new(Some(thread::current().id())));
            if self.collisions[position].compare_exchange_weak(them, me, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                break;
            }
        }
        println!("{:?} Retrieved info", thread::current().id());
        let mut their_info_option: Option<&AtomicPtr<OpInfo<T>>> = None;
        unsafe {
            if (*them).is_none() {
                println!("{:?} Failed, they have no info", thread::current().id());
                return Err(ptr::read(my_info_ptr));
            }
            their_info_option = self.operations.get().as_mut().unwrap().get(&ptr::read(them).unwrap());
            if their_info_option.is_none() {
                println!("{:?} Failed, they have no info", thread::current().id());
                return Err(ptr::read(my_info_ptr));
            }
            println!("{:?} Attempting elimination with {:?}", thread::current().id(), ptr::read(them));
            println!("{:?} Retrieved their info! {:?}", thread::current().id(), their_info_option);
            let their_atomic = their_info_option.unwrap();
            let their_info_ptr = their_atomic.load(Ordering::Acquire);
            let their_info = ptr::read(their_info_ptr);
            println!("{:?} Retrieved their info from ptr! {:?}", thread::current().id(), their_info);
            let my_info = ptr::read(my_info_ptr);
            if my_info.check_complimentary(their_info.operation.as_ref()) {
                let my_atomic = self.operations.get().as_mut().unwrap().get(&thread::current().id()).unwrap();
                if my_atomic.compare_exchange_weak(
                                my_info_ptr, 
                                OpInfo::new_none_as_pointer(my_info.node), 
                                Ordering::AcqRel,
                                Ordering::Acquire).is_ok() {
                    if their_atomic.compare_exchange_weak(
                                        their_info_ptr,
                                        OpInfo::new_none_as_pointer(my_info.node),
                                        Ordering::AcqRel,
                                        Ordering::Acquire).is_ok() {
                        println!("{:?} Eliminated active!", thread::current().id());
                        match my_info.operation {
                            Some(OpType::Pop) => return Ok(ptr::read(their_info.node).data),
                            Some(OpType::Push) => return Ok(None),
                            _ => { println!("wtf"); return Ok(None) }
                        }
                    } else {
                        // Can't swap, another elimination has happened
                        println!("{:?} Failed.", thread::current().id());
                        return Err(ptr::read(my_info_ptr));
                    }
                } else {
                    // We've already been eliminated, read the new value
                    println!("{:?} Eliminated passive", thread::current().id());
                    match my_info.operation {
                        Some(OpType::Pop) => return Ok(self.operations.get().as_mut().unwrap().get(&thread::current().id())                                   .and_then(|atomic| {
                                                                ptr::read(ptr::read(atomic.load(Ordering::Acquire)).node).data
                                                        })),
                        Some(OpType::Push) => return Ok(None),
                        _ => { println!("wtf!"); return Ok(None)}
                    }   
                    
                }
            }
            println!("{:?} Non-complimentary op", thread::current().id());
            thread::sleep(Duration::from_millis(500));
            let my_atomic = self.operations.get().as_mut().unwrap().get(&thread::current().id()).unwrap();
            if my_atomic.compare_exchange_weak(
                            my_info_ptr,
                            OpInfo::new_none_as_pointer(my_info.node),
                            Ordering::AcqRel,
                            Ordering::Acquire).is_err() {
                println!("{:?} Eliminated passive!", thread::current().id());
                match my_info.operation {
                            Some(OpType::Pop) => {
                                let my_atomic = self.operations.get().as_mut().unwrap().get(&thread::current().id());
                                let my_new_info = ptr::read(my_atomic.unwrap().load(Ordering::Acquire));
                                println!("{:?} My new info: {:?}", thread::current().id(), my_new_info);
                                let my_new_data = ptr::read(my_new_info.node).data;
                                return Ok(my_new_data)
                            },
                            Some(OpType::Push) => return Ok(None),
                            _ => { println!("wtf"); return Ok(None) }
                        }
            } else {
                println!("{:?} Failed", thread::current().id());
                return Err(ptr::read(my_info_ptr))
            }   
        }
    }

    fn choose_position(&self) -> usize {
        return rand::thread_rng().gen_range(0, self.collision_size);
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

    fn new_none_as_pointer(node: *mut Node<T>) -> *mut Self {
        Box::into_raw(Box::new({
            OpInfo {
                operation: None,
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

    #[test]
    #[ignore]
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
        let mut stack : Stack<Foo> = Stack::new();

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
    fn test_elimination_no_segfault() {
        let stack: Arc<Stack<u8>> = Arc::new(Stack::new());
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
                            Some(_) => break,
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