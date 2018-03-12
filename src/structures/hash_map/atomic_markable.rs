use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt::Debug;
use std::hash::Hash;
use std::ptr;

fn is_marked<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match ptr_usize | 0x1 {
        1 => true,
        _ => false
    }
}

fn is_array_node<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match ptr_usize | 0x2 {
        1 => true,
        _ => false
    }
}

#[derive(Debug)]
pub struct AtomicMarkablePtr<K, V> {
    ptr: AtomicUsize,
    marker: PhantomData<(K, V)>
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

    pub fn mark(&self) {
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

    pub fn get_node(&self) -> Option<*mut Node<K, V>> {
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
pub struct DataNode<K, V> {
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
pub struct ArrayNode<K, V> {
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
pub enum Node<K, V> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}