use std::sync::atomic::Ordering;
use std::hash::{Hash, Hasher, BuildHasher};
use std::ptr;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;
use super::utils::atomic_markable::AtomicMarkablePtr;
use super::utils::atomic_markable;

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
}

pub struct Node<T: Send> {
    value: T,
    hash: u64
}