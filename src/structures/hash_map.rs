use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::ptr;
use std::marker::PhantomData;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;

const HEAD_SIZE: usize = 64;
const KEY_SIZE: usize = 64;
const MAX_FAILURES: u64 = 10;

pub struct HashMap<K, V> 
where K: Send + Debug,
      V: Send + Debug
{
    head: Vec<AtomicMarkablePtr<K, V>>,
    hasher: RandomState,
    head_size: usize,
    shift_step: usize,
    manager: HPBRManager<Node<K, V>>
}

impl<K: Eq + Hash + Debug + Send, V: Send + Debug> HashMap<K, V> {
    /// Create a new Wait-Free HashMap with the default head size
    fn new() -> Self {
        let mut head: Vec<AtomicMarkablePtr<K, V>> = Vec::with_capacity(HEAD_SIZE);
        for _ in 0..HEAD_SIZE {
            head.push(AtomicMarkablePtr::default());
        }

        Self {
            head,
            hasher: RandomState::new(),
            head_size: HEAD_SIZE,
            shift_step: f64::floor((HEAD_SIZE as f64).log2()) as usize,
            manager: HPBRManager::new(100, 1)
        }   
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Attempt to insert into the HashMap
    /// Returns Ok on success and Error on failure containing the attempted
    /// insert data
    fn insert(&self, key: K, mut value: V) -> Result<(), (K, V)> {
        let mut hash = self.hash(&key);
        let mut bucket = &self.head;
        let mut r = 0usize;
        while r < (KEY_SIZE - self.shift_step) {
            // Get the position as defined by the lowest n bits of the key
            let position = hash as usize & (bucket.len() - 1);
            hash >>= self.shift_step;
            let mut node = bucket[position].get_node();
            let mut fail_count = 0;
            loop {
                if fail_count > MAX_FAILURES {
                    // Mark the node for expansion if there is too much contention
                    bucket[position].mark();
                }
                match node {
                    None => {
                        // No data currently in this position! Try inserting
                        value = match bucket[position].try_insertion(ptr::null_mut(), hash, value) {
                            Ok(()) => { return Ok(()) },
                            Err(val) => val
                        }
                    },
                    Some(node_ptr) => {
                        if bucket[position].is_marked() {
                            // EXPAND THE MAP
                        }
                        unsafe {
                            match &*node_ptr {
                                &Node::Array(ref array_node) => {
                                    // This is safe because an ArrayNode will NEVER be removed
                                    // Once it is in the data structure, it cannot be a hazard
                                    bucket = &array_node.array;
                                    break;
                                },
                                &Node::Data(ref data_node) => {
                                    self.manager.protect(node_ptr, 0);
                                    // If we cannot unwrap node2 here, something has gone very wrong
                                    let node2 = bucket[position].get_node().unwrap();
                                    if !ptr::eq(node_ptr, node2) {
                                        node = Some(node2);
                                        fail_count += 1;
                                        continue;
                                    } else if data_node.key == hash {
                                        return Err((key, value))
                                    } else {
                                        // expand map and check if array node
                                    }
                                }
                            }
                        }
                    }
                }
            }

            r += self.shift_step;
        }

        Ok(())
    }

}

#[derive(Debug)]
struct AtomicMarkablePtr<K, V> {
    ptr: AtomicUsize,
    marker: PhantomData<Node<K, V>>
}

impl<K, V> AtomicMarkablePtr<K, V>
where K: Eq + Hash + Debug,
      V: Send + Debug       
{    
    fn new_data_node(key: u64, value: V) -> Self {
        let data_node: DataNode<K, V> = DataNode::new(key, value);
        let data_ptr = Box::into_raw(Box::new(data_node));
        let ptr = AtomicUsize::new(data_ptr as usize);
        Self {
            ptr: ptr,
            marker: PhantomData
        }
    }

    fn new_array_node(size: usize) -> Self {
        let array_node: ArrayNode<K, V> = ArrayNode::new(size);
        let node_ptr = Box::into_raw(Box::new(array_node));
        let marked_ptr = (node_ptr as usize) | 0x2;
        let ptr = AtomicUsize::new(marked_ptr);
        Self {
            ptr: ptr,
            marker: PhantomData
        }
    }

    fn mark(&self) {
        self.ptr.fetch_or(0x1, Ordering::SeqCst);
    }

    fn unmark(&self) -> *mut Node<K, V> {
        (self.ptr.load(Ordering::SeqCst) | 0x1) as *mut Node<K, V>
    }

    fn is_marked(&self) -> bool {
        match self.ptr.load(Ordering::SeqCst) & 0x1 {
            1 => true,
            _ => false
        }
    }

    fn is_array_node(&self) -> bool {
        match self.ptr.load(Ordering::SeqCst) & 0x2 {
            1 => true,
            _ => false
        }
    
    }

    fn get_node(&self) -> Option<*mut Node<K, V>> {
        match self.ptr.load(Ordering::SeqCst) {
            0 => None,
            ptr => {
                Some(match ptr | 0x1 {
                    1 => (ptr | 0x1) as *mut Node<K, V>,
                    _ => ptr as *mut Node<K, V>
                })
            }
        }
    }

    fn try_insertion(&self, old: *mut Node<K, V>, hash: u64, value: V) -> Result<(), V> {
        let data_node: DataNode<K, V> = DataNode::new(hash, value);
        let data_node_ptr = Box::into_raw(Box::new(data_node));
        let usize_ptr = data_node_ptr as usize;
        let usize_old = old as usize;

        match self.ptr.compare_exchange_weak(usize_old, usize_ptr, Ordering::SeqCst, Ordering::Acquire) {
            Ok(usize_old) => Ok(()),
            Err(_) => {
                unsafe {
                    let node = ptr::replace(data_node_ptr, DataNode::default());
                    Box::from_raw(data_node_ptr);
                    Err(node.value.unwrap())
                }
            }
        }
    }
}

impl<K, V> Default for AtomicMarkablePtr<K, V>
where K: Eq + Hash + Debug,
      V: Send + Debug
{
    fn default() -> Self {
        Self {
            ptr: AtomicUsize::default(),
            marker: PhantomData
        }
    }
} 

#[derive(Debug)]
struct DataNode<K, V> {
    key: u64,
    value: Option<V>,
    marker: PhantomData<K>
}

impl<K, V> DataNode<K, V> 
where K: Eq + Hash + Debug,
      V: Send + Debug 
{
    fn new(key: u64, value: V) -> Self {
        Self {
            key,
            value: Some(value),
            marker: PhantomData
        }
    }
}

impl<K, V> Default for DataNode<K, V>
where K: Eq + Hash + Debug,
      V: Send + Debug
{
    fn default() -> Self {
        Self {
            key: 0u64,
            value: None,
            marker: PhantomData
        }
    }
} 

#[derive(Debug)]
struct ArrayNode<K, V> {
    array: Vec<AtomicMarkablePtr<K, V>>,
    size: usize
}

impl<K, V> ArrayNode<K, V>
where K: Eq + Hash + Debug,
      V: Send + Debug  
{
    fn new(size: usize) -> Self {
        let mut array = Vec::with_capacity(size);
        for _ in 0..size {
            array.push(AtomicMarkablePtr::default());
        }
        Self {
            array,
            size
        }
    }
}

#[derive(Debug)]
enum Node<K, V> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}

