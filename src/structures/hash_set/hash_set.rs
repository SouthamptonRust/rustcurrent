use std::sync::atomic::Ordering::{Release};
use std::hash::{Hash, Hasher, BuildHasher};
use std::ptr;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;
use super::atomic_markable::AtomicMarkablePtr;
use super::atomic_markable;

const HEAD_SIZE: usize = 256;
const CHILD_SIZE: usize = 16;
const KEY_SIZE: usize = 64;
const MAX_FAILURES: u64 = 10;

pub struct HashSet<T: Send> {
    head: Vec<AtomicMarkablePtr<Node<T>>>,
    hasher: RandomState,
    head_size: usize,
    shift_step: usize,
    manager: HPBRManager<Node<T>>
}

impl<T: Hash + Send> HashSet<T> {
    pub fn new() -> Self {
        let mut head: Vec<AtomicMarkablePtr<Node<T>>> = Vec::with_capacity(HEAD_SIZE);
        for _ in 0..HEAD_SIZE {
            head.push(AtomicMarkablePtr::default());
        }

        Self {
            head,
            hasher: RandomState::new(),
            head_size: HEAD_SIZE,
            shift_step: f64::floor((CHILD_SIZE as f64).log2()) as usize,
            manager: HPBRManager::new(100, 1)
        }
    }

    fn hash<Q: ?Sized>(&self, value: &Q) -> u64
    where T: Borrow<Q>,
          Q: Hash + Send
    {
        let mut hasher = self.hasher.build_hasher();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn expand(&self, bucket: &Vec<AtomicMarkablePtr<Node<T>>>, pos: usize, shift_amount:usize) -> *mut Node<T> {
        let node = bucket[pos].get_ptr().unwrap();
        self.manager.protect(atomic_markable::unmark(node), 0);
        if atomic_markable::is_marked_second(node) {
            return node
        }

        let node2 = bucket[pos].get_ptr().unwrap();
        if !ptr::eq(node, node2) {
            return node2
        }

        let array_node: ArrayNode<T> = ArrayNode::new(CHILD_SIZE);
        let hash = unsafe { match &*atomic_markable::unmark(node) {
            &Node::Data(ref data_node) => data_node.hash,
            &Node::Array(_) => { panic!("Unexpected array node!") }
        }};

        let new_pos = (hash >> (shift_amount + self.shift_step)) as usize & (CHILD_SIZE - 1);
        array_node.array[new_pos].store(atomic_markable::unmark(node));

        let array_node_ptr = Box::into_raw(Box::new(Node::Array(array_node)));
        let array_node_ptr_marked = atomic_markable::mark_second(array_node_ptr);

        return match bucket[pos].compare_exchange(node, array_node_ptr_marked) {
            Ok(_) => array_node_ptr_marked,
            Err(current) => {
                let vec = get_bucket(array_node_ptr);
                vec[new_pos].store(ptr::null_mut());
                unsafe { Box::from_raw(array_node_ptr) };
                current
            }
        }
    }

    fn insert(&self, data: T) -> Result<(), T> {
        let hash = self.hash(&data);
        let mut mut_hash = hash;
        let mut bucket = &self.head;
        let mut r = 0usize;

        while r < (KEY_SIZE) { 
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash = mut_hash >> self.shift_step;
            let mut fail_count = 0;
            let mut node = bucket[pos].get_ptr();

            loop {
                if fail_count > MAX_FAILURES {
                    bucket[pos].mark();
                }
            }

            r += self.shift_step;
        }

        Ok(())
    }
}

fn get_bucket<'a, T: Send>(node_ptr: *mut Node<T>) -> &'a Vec<AtomicMarkablePtr<Node<T>>> {
    unsafe {
        match &*(atomic_markable::unmark_second(node_ptr)) {
            &Node::Data(_) => panic!("Unexpected data node!: {:b}", node_ptr as usize),
            &Node::Array(ref array_node) => &array_node.array
        }
    }
}

fn get_data_node<'a, T: Send>(node_ptr: *mut Node<T>) -> &'a DataNode<T> {
    unsafe {
        match &*(atomic_markable::unmark(node_ptr)) {
            &Node::Data(ref data_node) => data_node,
            &Node::Array(_) => panic!("Unexpected array node!: {:b}", node_ptr as usize)
        }
    }
}

enum Node<T: Send> {
    Data(DataNode<T>),
    Array(ArrayNode<T>)
}

struct DataNode<T: Send> {
    value: T,
    hash: u64
}

struct ArrayNode<T: Send> {
    array: Vec<AtomicMarkablePtr<Node<T>>>,
    size: usize
}

impl<T: Send> ArrayNode<T> {
    fn new(size: usize) -> Self {
        let mut array = Vec::with_capacity(size);
        for _ in 0..size {
            array.push(AtomicMarkablePtr::default());
        }

        ArrayNode {
            array,
            size
        }
    }
}