use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::fmt;
use std::ptr;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;
use super::atomic_markable::{AtomicMarkablePtr, Node, DataNode, ArrayNode};
use super::atomic_markable;

const HEAD_SIZE: usize = 256;
const CHILD_SIZE: usize = 16;
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
            shift_step: f64::floor((CHILD_SIZE as f64).log2()) as usize,
            manager: HPBRManager::new(100, 1)
        }   
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Attempt to add an array node level to the current position
    fn expand_map(&self, bucket: &Vec<AtomicMarkablePtr<K, V>>, pos: usize, shift_amount: usize) -> *mut Node<K, V> {
        // We know this node must exist
        let node = atomic_markable::unmark(bucket[pos].get_ptr().unwrap());
        self.manager.protect(node, 0);
        if atomic_markable::is_array_node(node) {
            return node
        }
        let node2 = bucket[pos].get_ptr().unwrap();
        if !ptr::eq(node, node2) {
            return node2
        }

        let array_node: ArrayNode<K, V> = ArrayNode::new(CHILD_SIZE);
        unsafe {
            let hash = match &*node {
                &Node::Data(ref data_node) => data_node.key,
                &Node::Array(_) => {panic!("Unexpected array node!")}
            };
            let new_pos = (hash >> (shift_amount + self.shift_step)) as usize & (CHILD_SIZE - 1);
            array_node.array[new_pos].ptr().store(node as usize, Ordering::Release);

            let array_node_ptr = Box::into_raw(Box::new(Node::Array(array_node)));
            let array_node_ptr_marked = atomic_markable::mark_array_node(array_node_ptr);
            return match bucket[pos].compare_exchange_weak(node, array_node_ptr_marked) {
                Ok(_) => {
                    array_node_ptr_marked
                },
                Err(_) => {
                    Box::from_raw(array_node_ptr);
                    bucket[pos].get_ptr().unwrap()
                }
            }
        }
    }

    /// Attempt to insert into the HashMap
    /// Returns Ok on success and Error on failure containing the attempted
    /// insert data
    fn insert(&self, key: K, mut value: V) -> Result<(), (K, V)> {
        let hash = self.hash(&key);
        let mut mut_hash = hash;
        let mut bucket = &self.head;
        let mut r = 0usize;
        while r < (KEY_SIZE - self.shift_step) {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash = mut_hash >> self.shift_step;
            let mut fail_count = 0;
            let mut node = bucket[pos].get_ptr();

            loop {
                if fail_count > MAX_FAILURES {
                    bucket[pos].mark();
                    node = bucket[pos].get_ptr();
                }
                match node {
                    None => {
                        value = match self.try_insert(&bucket[pos], ptr::null_mut(), hash, value) {
                            Ok(_) => { return Ok(()) },
                            Err(old) => old
                        };
                    },
                    Some(mut node_ptr) => {
                        if atomic_markable::is_marked(node_ptr) {
                            node = Some(self.expand_map(bucket, pos, r));
                            node_ptr = node.unwrap();
                        }
                        if atomic_markable::is_array_node(node_ptr) {
                            println!("array node: {:?} -> {:?}", node_ptr, value);
                            node_ptr = atomic_markable::unmark_array_node(node_ptr);
                            unsafe {
                                // This dereference should be safe because array nodes cannot be removed
                                match &*node_ptr {
                                    &Node::Data(_) => panic!("Unexpected data node"),
                                    &Node::Array(ref array_node) => {
                                        bucket = &array_node.array;
                                        break;
                                    }
                                }
                            }
                        } else {
                            self.manager.protect(node_ptr, 0);
                            let node2 = bucket[pos].get_ptr();
                            if node2.is_none() || !ptr::eq(node2.unwrap(), node_ptr) {
                                node = node2;
                                fail_count += 1;
                                continue;
                            } else {
                                unsafe {
                                    println!("{:b}", node_ptr as usize);
                                    match &*node_ptr {
                                        &Node::Array(_) => panic!("Unexpected array node!,{:b} -> {:?}", node_ptr as usize, value),
                                        &Node::Data(ref data_node) => {
                                            if data_node.key == hash {
                                                return Err((key, value))
                                            }
                                            // If we get here, we have failed, but have a different key
                                            // We should thus expand because of contention
                                            node = Some(self.expand_map(bucket, pos, r));
                                            println!("expanded! {:?}", value);
                                            if atomic_markable::is_array_node(node.unwrap()) {
                                                match &*(atomic_markable::unmark_array_node(node.unwrap())) {
                                                    &Node::Array(ref array_node) => {
                                                        bucket = &array_node.array;
                                                        break;
                                                    },
                                                    &Node::Data(_) => panic!("Unexpected data node!")
                                                }
                                            } else {
                                                fail_count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }   
                    }                
                }
            }

            r += self.shift_step;
        }
        let pos = hash as usize & (self.head_size - 1);
        let node = bucket[pos].get_ptr();
        return match node {
            None => {
                match self.try_insert(&bucket[pos], ptr::null_mut(), hash, value) {
                    Err(val) => Err((key, val)),
                    Ok(_) => Ok(())
                }
            },
            Some(_) => {
                Err((key, value))
            }
        }
    }

    fn try_insert(&self, position: &AtomicMarkablePtr<K, V>, old: *mut Node<K, V>, key: u64, value: V) -> Result<(), V> {
        let data_node: DataNode<K, V> = DataNode::new(key, value);
        let data_node_ptr = Box::into_raw(Box::new(Node::Data(data_node)));

        return match position.compare_exchange_weak(old, data_node_ptr) {
            Ok(_) => Ok(()),
            Err(_) => {
                unsafe {
                    let node = ptr::replace(data_node_ptr, Node::Data(DataNode::default()));
                    if let Node::Data(data_node) = node {
                        let data = data_node.value.unwrap();
                        Box::from_raw(data_node_ptr);
                        Err(data)
                    } else {
                        panic!("Unexpected array node!");
                    }
                }
            }
        }
    }
}

impl<K, V> Debug for HashMap<K, V> 
where K: Eq + Hash + Send + Debug,
      V: Send + Debug
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Depth first printing, tab on each depth level
        let mut string = "".to_owned();
        let mut none_count = 0;
        for node in &self.head {
            if let Some(mut node_ptr) = node.get_ptr() {
                string.push_str("\n");
                if none_count > 0 {
                    string.push_str(&format!("None x {}\n", none_count));
                    none_count = 0;
                }
                node_ptr = atomic_markable::unmark_array_node(atomic_markable::unmark(node_ptr));
                unsafe {
                    match &*node_ptr {
                        &Node::Array(ref array_node) => {array_node.to_string(&mut string, 1);},
                        &Node::Data(ref data_node) => {string.push_str(&format!("{:X} ==> {:?}", data_node.key, data_node.value));}
                    }
                }
            } else {
                none_count += 1;
            }
        }
        if none_count > 0 {
            string.push_str(&format!("None x {}", none_count));
        }

        write!(f, "{}", string)
    }
}

impl<K, V> Default for HashMap<K, V>
where K: Eq + Hash + Send + Debug,
      V: Send + Debug
{
    fn default() -> Self {
        HashMap::new()
    }
}

mod tests {
    use super::HashMap;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_single_thread_insert() {
        let map : HashMap<u8, u8> = HashMap::new();

        //assert!(map.insert(9, 9).is_ok());
        //assert!(map.insert(9, 7).is_err());

        for i in 0..240 {
            println!("{}", i);
            match map.insert(i, i) {
                Ok(_) => println!("success"),
                Err(_) => assert!(false)
            }
        }
        println!("{:?}", map);

    }
}

