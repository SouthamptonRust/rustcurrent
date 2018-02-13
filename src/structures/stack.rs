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
    //elimination: EliminationLayerOld<T>,
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
            //elimination: EliminationLayerOld::new(40, 5),
            manager: HPBRManager::new(3000, 1)
        }
    }

    pub fn push(&self, val: T) {
        let mut node = Box::new(Node::new(val));
        loop {
            node = match self.try_push(node) {
                Ok(_) => { return; }
                Err(old_node) => old_node
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

    pub fn pop(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_pop() {
                return val
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
struct EliminationLayerOld<T: Debug> {
    operations: UnsafeCell<HashMap<thread::ThreadId, AtomicPtr<OpInfoOld<T>>>>,
        // If we bound the number of threads, and preallocate the HashMap,
        // it should be fine to access concurrently because rehashing will
        // never happen, as guaranteed by the runtime.
    collisions: Vec<AtomicPtr<Option<thread::ThreadId>>>,
    collision_size: usize
}

struct EliminationLayer<T: Debug> {
    operations: UnsafeCell<HashMap<thread::ThreadId, AtomicPtr<OpInfo<T>>>>,
    collisions: Vec<AtomicPtr<Option<thread::ThreadId>>>,
    collision_size: usize
}

impl<T: Debug> EliminationLayer<T> {
    fn new(max_threads: usize, collision_size: usize) -> Self {
        let mut collisions: Vec<AtomicPtr<Option<thread::ThreadId>>> = Vec::new();
        for _ in 0..collision_size {
            collisions.push(AtomicPtr::new(Box::into_raw(Box::default())))
        }
        EliminationLayer {
            operations: UnsafeCell::new(HashMap::with_capacity(max_threads)),
            collisions,
            collision_size
        }
    }

    // TODO finish writing this 
    fn try_eliminate(&self, op: OpType, node: Box<Node<T>>) -> Result<Option<T>, Box<Node<T>>> {
        let op_info_ptr = OpInfo::new_as_ptr(op, node);
        let thread_id = thread::current().id();

        unsafe {
            let mut_operations = self.get_mut_operations();
            mut_operations.entry(thread_id)
                          .or_insert(AtomicPtr::default())
                          .store(op_info_ptr, Ordering::Release);
        }

        let them = match self.get_eliminate_partner(thread_id) {
            Ok(their_id) => their_id,
            Err(_) => { return Err(OpInfo::get_boxed_node(op_info_ptr))}
        };

        Ok(None)
    }

    fn get_eliminate_partner(&self, me: thread::ThreadId) -> Result<thread::ThreadId, ()> {
        let position = self.choose_position();
        let me_ptr = Box::into_raw(Box::new(Some(me)));
        unsafe {
            loop {
                let them_ptr = self.collisions[position].load(Ordering::Acquire);
                if let Ok(them_ptr) = self.collisions[position].compare_exchange_weak(them_ptr, me_ptr, Ordering::AcqRel, Ordering::Release) {
                    return match *them_ptr {
                        Some(id) => Ok(id),
                        None => Err(())
                    }
                }
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

struct OpInfo<T: Debug> {
    operation: OpType,
    node: Box<Node<T>>
}

impl<T: Debug> OpInfo<T> {
    fn new(operation: OpType, node: Box<Node<T>>) -> Self {
        OpInfo {
            operation,
            node
        }
    }

    fn new_as_ptr(operation: OpType, node: Box<Node<T>>) -> *mut Self {
        Box::into_raw(Box::new(OpInfo {
            operation,
            node
        }))
    }

    // Should probably free the opinfo pointer
    fn get_boxed_node(opinfo_ptr: *mut Self) -> Box<Node<T>> {
        unsafe {
            let opinfo = ptr::replace(opinfo_ptr, OpInfo::default());
            opinfo.node
        }
    }
}

impl<T: Debug> Default for OpInfo<T> {
    fn default() -> Self {
        OpInfo {
            operation: OpType::Done,
            node: Box::default()
        }
    }
}

#[derive(Clone)]
#[derive(Debug)]
struct OpInfoOld<T: Debug> {
    operation: Option<OpType>,
    node: *mut Node<T>
}

#[derive(Debug)]
#[derive(Clone)]
enum OpType {
    Pop,
    Push,
    Done
}



impl<T: Debug> EliminationLayerOld<T> {
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

    fn try_eliminate_old(&self, my_info_ptr: *mut OpInfoOld<T>) -> Result<Option<T>, OpInfoOld<T>> {
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
        let mut their_info_option: Option<&AtomicPtr<OpInfoOld<T>>> = None;
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
                                OpInfoOld::new_none_as_pointer(my_info.node), 
                                Ordering::AcqRel,
                                Ordering::Acquire).is_ok() {
                    if their_atomic.compare_exchange_weak(
                                        their_info_ptr,
                                        OpInfoOld::new_none_as_pointer(my_info.node),
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
                            OpInfoOld::new_none_as_pointer(my_info.node),
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

impl<T: Debug> OpInfoOld<T> {
    fn new_from_pointer(node: *mut Node<T>, op: OpType) -> Self {
        OpInfoOld {
            operation: Some(op),
            node
        }
    }

    fn new_as_pointer(node: *mut Node<T>, op: OpType) -> *mut Self {
        Box::into_raw(Box::new({
            OpInfoOld {
                operation: Some(op),
                node
            }
        }))
    }

    fn new_none_as_pointer(node: *mut Node<T>) -> *mut Self {
        Box::into_raw(Box::new({
            OpInfoOld {
                operation: None,
                node
            }
        }))
    }

    fn new_from_data(data: T, op: OpType) -> Self {
        OpInfoOld {
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