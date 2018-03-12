use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::ptr;
use std::marker::PhantomData;
use std::collections::hash_map::RandomState;

const HEAD_SIZE: usize = 64;
const KEY_SIZE: usize = 64;

pub struct HashMap<K, V> {
    head: Vec<AtomicMarkablePtr<K, V>>,
    hasher: RandomState,
    head_size: usize,
    shift_step: usize
}

impl<K: Eq + Hash + Debug, V: Send + Debug> HashMap<K, V> {
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
            shift_step: f64::floor((HEAD_SIZE as f64).log2()) as usize
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
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        let mut hash = self.hash(&key);
        let mut bucket = &self.head;
        let mut r = 0usize;
        while r < (KEY_SIZE - self.shift_step) {
            let position = hash as usize & (self.shift_step - 1);
            hash >>= self.shift_step;

            let mut node = &bucket[position];

            r += self.shift_step;
        }

        Ok(())
    }

}

struct AtomicMarkablePtr<K, V> {
    ptr: Option<AtomicUsize>,
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
            ptr: Some(ptr),
            marker: PhantomData
        }
    }

    fn new_array_node(size: usize) -> Self {
        let array_node: ArrayNode<K, V> = ArrayNode::new(size);
        let node_ptr = Box::into_raw(Box::new(array_node));
        let marked_ptr = (node_ptr as usize) | 0x2;
        let ptr = AtomicUsize::new(marked_ptr);
        Self {
            ptr: Some(ptr),
            marker: PhantomData
        }
    }

    fn mark(&self) {
        match self.ptr.as_ref() {
            Some(ptr) => {
                ptr.fetch_or(0x1, Ordering::SeqCst);
            },
            None => {}
        }
    }

    fn unmark(&self) -> Option<*mut Node<K, V>> {
        match self.ptr.as_ref() {
            Some(ptr) => {
                Some((ptr.load(Ordering::SeqCst) | 0x1) as *mut Node<K, V>)
            },
            None => {
                None
            }
        }
    }

    fn is_marked(&self) -> bool {
        match self.ptr.as_ref() {
            Some(ptr) => {
                match ptr.load(Ordering::SeqCst) & 0x1 {
                    1 => true,
                    _ => false
                }
            },
            None => false
        }
    }

    fn is_array_node(&self) -> bool {
        match self.ptr.as_ref() {
            Some(ptr) => {
                match ptr.load(Ordering::SeqCst) & 0x2 {
                    1 => true,
                    _ => false
                }
            },
            None => false
        }
    }

    fn get_node(&self) -> Option<*mut Node<K, V>> {
        match self.ptr.as_ref() {
            None => None,
            Some(ptr) => {
                Some(ptr.load(Ordering::SeqCst) as *mut Node<K, V>)
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
            ptr: None,
            marker: PhantomData
        }
    }
} 

struct DataNode<K, V> {
    key: u64,
    value: V,
    marker: PhantomData<K>
}

impl<K, V> DataNode<K, V> 
where K: Eq + Hash + Debug,
      V: Send + Debug 
{
    fn new(key: u64, value: V) -> Self {
        Self {
            key,
            value,
            marker: PhantomData
        }
    }
}

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

enum Node<K, V> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}

