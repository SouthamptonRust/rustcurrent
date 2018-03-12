use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::Hash;
use std::fmt::Debug;
use std::ptr;
use std::marker::PhantomData;

pub struct HashMap<K, V> {
    head: Vec<AtomicMarkablePtr<K, V>>,
    marker: PhantomData<(K, V)>
}

impl<K: Eq + Hash + Debug, V: Send + Debug> HashMap<K, V> {

}

struct AtomicMarkablePtr<K, V> {
    
    marker: PhantomData<(K, V)>
}

struct DataNode<K, V> {
    key: u64,
    value: V,
    marker: PhantomData<K>
}

struct ArrayNode<K, V> {
    array: Vec<AtomicMarkablePtr<K, V>>
}

enum Node<K, V> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}