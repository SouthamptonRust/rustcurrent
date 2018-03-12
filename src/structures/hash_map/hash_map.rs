use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::ptr;
use std::marker::PhantomData;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;
use super::atomic_markable::{AtomicMarkablePtr, Node, DataNode, ArrayNode};

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




