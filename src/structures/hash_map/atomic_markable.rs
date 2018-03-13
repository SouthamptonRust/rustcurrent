use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt::Debug;
use std::hash::Hash;
use std::ptr;

pub fn is_marked<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match ptr_usize & 0x1 {
        0 => false,
        _ => true,
    }
}

pub fn is_array_node<T>(ptr: *mut T) -> bool {
    let ptr_usize = ptr as usize;
    match (ptr_usize & 0x2) >> 1 {
        0 => false,
        _ => true
    }
}

pub fn unmark<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize & !(0x1)) as *mut T
}

pub fn unmark_array_node<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize & !(0x2)) as *mut T
}

pub fn mark_array_node<T>(ptr: *mut T) -> *mut T {
    let ptr_usize = ptr as usize;
    (ptr_usize | 0x2) as *mut T
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
    pub fn mark(&self) {
        self.ptr.fetch_or(0x1, Ordering::SeqCst);
    }

    pub fn get_ptr(&self) -> Option<*mut Node<K, V>> {
        match self.ptr.load(Ordering::SeqCst) {
            0 => None,
            ptr_val => Some(ptr_val as *mut Node<K, V>)
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

    pub fn compare_exchange_weak(&self, old: *mut Node<K, V>, new: *mut Node<K, V>) 
                -> Result<*mut Node<K, V>, *mut Node<K, V>> 
    {
        match self.ptr.compare_exchange_weak(old as usize, new as usize, Ordering::SeqCst, Ordering::Acquire) {
            Ok(_) => Ok(old),
            Err(_) => Err(new)
        }
    }

    pub fn ptr(&self) -> &AtomicUsize {
        &self.ptr
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
    pub key: u64,
    pub value: Option<V>,
    marker: PhantomData<K>
}

impl<K, V> DataNode<K, V> 
where K: Eq + Hash + Debug,
      V: Send + Debug 
{
    pub fn new(key: u64, value: V) -> Self {
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
    pub array: Vec<AtomicMarkablePtr<K, V>>,
    size: usize
}

impl<K, V> ArrayNode<K, V>
where K: Eq + Hash + Debug,
      V: Send + Debug  
{
    pub fn new(size: usize) -> Self {
        let mut array = Vec::with_capacity(size);
        for _ in 0..size {
            array.push(AtomicMarkablePtr::default());
        }
        Self {
            array,
            size
        }
    }

    pub unsafe fn to_string(&self, start: &mut String, depth: usize) {
        let mut none_count = 0;
        start.push_str("\n");
        for _ in 0..depth {
            start.push_str("\t");
        }
        start.push_str("ArrayNode: ");
        for markable in &self.array {
            if let Some(mut node_ptr) = markable.get_ptr() {
                start.push_str("\n");
                for _ in 0..depth {
                    start.push_str("\t");
                }
                if none_count > 0 {
                    start.push_str(&format!("None x {}\n", none_count));
                    for _ in 0..depth {
                        start.push_str("\t");
                    }
                    none_count = 0;
                }
                node_ptr = unmark_array_node(unmark(node_ptr));
                match &*node_ptr {
                    &Node::Array(ref array_node) => {
                        array_node.to_string(start, depth + 1);
                    },
                    &Node::Data(ref data_node) => {
                        start.push_str(&format!("{:X} ==> {:?}", data_node.key, data_node.value));
                    }
                }
            } else {
                none_count += 1;
            }
        }
        if none_count > 0 {
            start.push_str("\n");
            for _ in 0..depth {
                start.push_str("\t");
            }
            start.push_str(&format!("None x {}", none_count));
        }
    }
}

#[derive(Debug)]
pub enum Node<K, V> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}