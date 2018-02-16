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
                node = match self.elimination.try_eliminate(OpType::Push, node) {
                    Ok(_) => { return; }
                    Err(old_node) => old_node
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

    pub fn pop(&self) -> Option<T> {
        loop {
            if let Ok(val) = self.try_pop() {
                return val
            }
            if self.elimination_on {
                if let Ok(val) = self.elimination.try_eliminate(OpType::Pop, Box::default()) {
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
    fn try_eliminate(&self, op: OpType, node: Box<Node<T>>) -> Result<Option<T>, Box<Node<T>>> {
        println!("{:?} Eliminating", thread::current().id());
        let op_info_ptr = OpInfo::new_as_ptr(op.clone(), node);
        let thread_id = thread::current().id();
        self.manager.protect(op_info_ptr, 0);

        unsafe {
            let mut_operations = self.get_mut_operations();
            let old_info_ptr = mut_operations.entry(thread_id)
                                              .or_insert(AtomicPtr::default())
                                              .swap(op_info_ptr, Ordering::Release);
            // Free the old entry, it cannot be accessed
            if !ptr::eq(old_info_ptr, ptr::null()) {
                //self.manager.retire(old_info_ptr, 2);
            }
        }

        if let Some(them) = self.get_eliminate_partner(thread_id) {
            println!("Partner is: {:?}", them);
            unsafe {
                let mut_operations = self.get_mut_operations();
                // We can unwrap because otherwise the thread cannot be in the collision vector
                let me_atomic_ptr = mut_operations.get(&thread_id).unwrap();
                let them_atomic_ptr = mut_operations.get(&them).unwrap();
                // NEED TO PROTECT HERE
                // it's possible for another thread to be matching on this collision and get this same pointer
                let them_ptr = them_atomic_ptr.load(Ordering::Acquire);
                self.manager.protect(them_ptr, 1);
                
                if OpType::check_complimentary(&(*op_info_ptr).operation, &(*them_ptr).operation) {
                    if Self::try_swap_info_me(me_atomic_ptr, op_info_ptr) {
                        if Self::try_swap_info_them(them_atomic_ptr, them_ptr, op_info_ptr) {
                            // Successful
                            // I now own the them_ptr and need to free it
                            // I also need to free my op_info
                            println!("Success: {:?} with {:?}", thread_id, them);
                            return Ok(self.get_data(them_ptr, &op));

                            // NEED TO RETIRE HERE
                        }
                        // get_boxed_node should retire the OpInfo
                        self.manager.unprotect(1);
                        let node = OpInfo::get_boxed_node(op_info_ptr);
                        println!("Failed: {:?} with {:?}", thread_id, them);
                        return Err(node);
                        // Different elimination happened
                    }
                    println!("Success: {:?} with {:?}", thread_id, them);
                    // Can't read from op_info_ptr, as it will point to dummy data
                    // opinfo ptr has been swapped out here
                    self.manager.unprotect(1);
                    return Ok(self.get_data(mut_operations.get(&thread_id).unwrap().load(Ordering::Acquire), &op));
                    // Already been eliminated, success
                }
            }       
        }

        // Down here is the passive elimination section
        thread::sleep(Duration::from_millis(20));

        unsafe {
            let me_atomic_ptr = self.get_mut_operations().get(&thread_id).unwrap();
            if !Self::try_swap_info_me(me_atomic_ptr, op_info_ptr) {
                println!("Success: {:?} passively", thread_id);
                return Ok(self.get_data(me_atomic_ptr.load(Ordering::Acquire), &op));
                // Elimination has happened
            }
        }
        // Elimination failed
        println!("Failed: {:?} passively", thread_id);
        Err(OpInfo::get_boxed_node(op_info_ptr))
    }

    fn get_eliminate_partner(&self, me: thread::ThreadId) -> Option<thread::ThreadId> {
        let position = self.choose_position();
        let me_ptr = Box::into_raw(Box::new(Some(me)));
        unsafe {
            loop {
                let them_ptr = self.collisions[position].load(Ordering::Acquire);
                if let Ok(them_ptr) = self.collisions[position].compare_exchange_weak(them_ptr, me_ptr, Ordering::AcqRel, Ordering::Acquire) {
                    return *them_ptr
                }
            }
        }
    }

    fn try_swap_info_me(me_atomic: &AtomicPtr<OpInfo<T>>, me_ptr: *mut OpInfo<T>) -> bool {
        match me_atomic.compare_exchange_weak(me_ptr, OpInfo::create_none_opinfo(me_ptr), Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => true,
            Err(ptr) => {
                unsafe {
                    Box::from_raw(ptr);
                }
                false
            }
        }
    }

    fn try_swap_info_them(them_atomic: &AtomicPtr<OpInfo<T>>, them_ptr: *mut OpInfo<T>, me_ptr: *mut OpInfo<T>) -> bool {
        match them_atomic.compare_exchange_weak(them_ptr, OpInfo::create_none_opinfo(me_ptr), Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => true,
            Err(ptr) => {
                unsafe {
                    Box::from_raw(ptr);
                }
                false
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
                    let info = ptr::replace(ptr, OpInfo::default());
                    let node = ptr::replace(info.node, Node::default());
                    //self.manager.retire(ptr, 1);
                    //self.manager.retire(info.node, 1);
                    return node.data
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
    node: *mut Node<T>
}

impl<T: Debug + Send> Drop for OpInfo<T> {
    fn drop(&mut self) {
        println!("========================== Dropping OpInfo");
    }
}

unsafe impl<T: Debug + Send> Send for OpInfo<T> {}

impl<T: Debug + Send> OpInfo<T> {
    fn new(operation: OpType, node: Box<Node<T>>) -> Self {
        OpInfo {
            operation,
            node: Box::into_raw(node)
        }
    }

    fn new_as_ptr(operation: OpType, node: Box<Node<T>>) -> *mut Self {
        println!("{:?}", node);
        Box::into_raw(Box::new(OpInfo {
            operation,
            node: Box::into_raw(node)
        }))
    }

    // Should probably free the opinfo pointer
    fn get_boxed_node(opinfo_ptr: *mut Self) -> Box<Node<T>> {
        unsafe {
            let opinfo = ptr::replace(opinfo_ptr, OpInfo::default());
            Box::from_raw(opinfo.node)
        }
    }

    fn create_none_opinfo(opinfo_ptr: *mut Self) -> *mut Self {
        unsafe {
            let node_ptr = (*opinfo_ptr).node;
            Box::into_raw(Box::new(OpInfo {
                operation: OpType::Done,
                node: node_ptr
            }))
        }
    }
}

impl<T: Debug + Send> Default for OpInfo<T> {
    fn default() -> Self {
        OpInfo {
            operation: OpType::Done,
            node: ptr::null_mut()
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
    fn check_complimentary(op1: &OpType, op2: &OpType) -> bool {
        match (op1, op2) {
            (&OpType::Push, &OpType::Pop) => true,
            (&OpType::Pop, &OpType::Push) => true,
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