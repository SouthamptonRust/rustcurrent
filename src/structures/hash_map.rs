use std::sync::atomic::{AtomicPtr, Ordering};
use std::hash::Hash;
use std::fmt::Debug;
use std::ptr;

pub struct HashMap<K: Hash + Eq + Send, V: Send + Debug> {
    
} 