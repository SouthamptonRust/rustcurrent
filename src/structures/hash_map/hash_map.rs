use std::sync::atomic::{Ordering};
use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::fmt;
use std::ptr;
use std::borrow::Borrow;
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

impl<K: Eq + Hash + Debug + Send, V: Send + Debug + Eq> HashMap<K, V> {
    /// Create a new Wait-Free HashMap with the default head size
    pub fn new() -> Self {
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

    fn hash<Q: ?Sized>(&self, key: &Q) -> u64 
    where K: Borrow<Q>,
          Q: Eq + Hash + Send + Debug
    {
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
            return match bucket[pos].compare_exchange(node, array_node_ptr_marked) {
                Ok(_) => {
                    array_node_ptr_marked
                },
                Err(current) => {
                    // Remove the entry from the array node so it is not cleared by the destructor
                    Box::from_raw(array_node_ptr);
                    current
                }
            }
        }
    }

    /// Attempt to insert into the HashMap
    /// Returns Ok on success and Error on failure containing the attempted
    /// insert data
    pub fn insert(&self, key: K, mut value: V) -> Result<(), (K, V)> {
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
                        match self.try_insert(&bucket[pos], ptr::null_mut(), hash, value) {
                            Ok(_) => { return Ok(()) },
                            Err(old) => { return Err((key, old)) }
                        }
                    },
                    Some(node_ptr) => {
                        if atomic_markable::is_marked(node_ptr) {
                            // Check that doing this never breaks, ie expand_map returns a data node
                            bucket = HashMap::get_bucket(self.expand_map(bucket, pos, r));
                            break;
                        }
                        if atomic_markable::is_array_node(node_ptr) {
                            bucket = HashMap::get_bucket(node_ptr);
                            break;
                        } else {
                            self.manager.protect(node_ptr, 0);
                            let node2 = bucket[pos].get_ptr();
                            if node2 != node {
                                node = node2;
                                fail_count += 1;
                                continue;
                            } else {
                                // Hazard pointer should be safe
                                unsafe {
                                    let data_node = self.get_data_node(node_ptr);
                                    if data_node.key == hash {
                                        return Err((key, value))
                                    }
                                    match &*node_ptr {
                                        &Node::Array(_) => panic!("Unexpected array node!,{:b} -> {:?}", node_ptr as usize, value),
                                        &Node::Data(ref data_node) => {
                                            if data_node.key == hash {
                                                return Err((key, value))
                                            }
                                            // If we get here, we have failed, but have a different key
                                            // We should thus expand because of contention
                                            node = Some(self.expand_map(bucket, pos, r));
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

    /// Standard get function returns a reference to an element of the map
    /// It is up to the user to make sure this is not freed before they finish using it, but
    /// the hazard pointers ensure that it is protected for this thread until the next hazard using map access
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where K: Borrow<Q>,
          Q: Eq + Hash + Send + Debug 
    {
        let hash = self.hash(key);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < (KEY_SIZE - self.shift_step) {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => { return None; }
                Some(mut node_ptr) => {
                    if atomic_markable::is_array_node(node_ptr) {
                        bucket = HashMap::get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        // Check the hazard pointer
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { break; },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_array_node(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                            continue;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }            
                            }
                            // Hazard pointer should be fine now
                            if atomic_markable::is_marked(node_ptr) {
                               bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                continue;
                            } else if atomic_markable::is_array_node(node_ptr) {
                                HashMap::get_bucket(node_ptr);
                                continue;
                            }
                        }
                        let data_node = self.get_data_node(node_ptr);
                        if data_node.key == hash {
                            return data_node.value.as_ref();
                        } else {
                            return None
                        }
                    }
                }
            }

            r += self.shift_step;
        }
        // We should only be here if we got to the bottom
        let pos = mut_hash as usize & (CHILD_SIZE - 1);
        if let Some(node_ptr) = bucket[pos].get_ptr() {
            unsafe {
                match &*node_ptr {
                    &Node::Array(_) => panic!("Unexpected array node!"),
                    &Node::Data(ref data_node) => {
                        return data_node.value.as_ref()
                    }
                }
            }
        } else {
            return None
        }
    }

    fn try_insert(&self, position: &AtomicMarkablePtr<K, V>, old: *mut Node<K, V>, key: u64, value: V) -> Result<(), V> {
        let data_node: DataNode<K, V> = DataNode::new(key, value);
        let data_node_ptr = Box::into_raw(Box::new(Node::Data(data_node)));

        return match position.compare_exchange(old, data_node_ptr) {
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

    // Returns the current value if update fails because our expected is wrong
    // Otherwise returns the expected we passed in - this means we failed for a different reason
    pub fn update<'a, 'b, Q: ?Sized>(&'a self, key: &Q, expected: &'b V, mut new: V) -> Result<(), V>
    where K: Borrow<Q>,
          Q: Eq + Hash + Send + Debug 
    {
        let hash = self.hash(key);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < (KEY_SIZE - self.shift_step) {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => { return Err(new) },
                Some(mut node_ptr) => {
                    if atomic_markable::is_array_node(node_ptr) {
                        bucket = HashMap::get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return Err(new); },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_array_node(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                            continue;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }
                            }
                            if atomic_markable::is_array_node(node_ptr) {
                                bucket = HashMap::get_bucket(node_ptr);
                                continue;
                            } else if atomic_markable::is_marked(node_ptr) {
                                bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                continue;
                            }
                        }
                        // Hazard pointer is safe now, so we can access the node
                        let data_node = self.get_data_node(node_ptr);
                        if data_node.key == hash {
                            if data_node.value.as_ref() != Some(expected) {
                                return Err(new)
                            }
                            new = match self.try_update(&bucket[pos], node_ptr, hash, new) {
                                Ok(()) => { 
                                    self.manager.retire(node_ptr, 0);
                                    return Ok(()) 
                                },
                                Err((value, current_ptr)) => {
                                    if atomic_markable::is_array_node(current_ptr) {
                                        bucket = HashMap::get_bucket(current_ptr);
                                        value
                                    } else if atomic_markable::is_marked(current_ptr) &&
                                              ptr::eq(node_ptr, atomic_markable::unmark(current_ptr)) 
                                    {
                                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                        value
                                    } else {
                                        return Err(value);
                                    }
                                }
                            }
                        } else {
                            return Err(new)
                        }
                    }
                }
            }
            r += self.shift_step;
        }
        
        // Since we are at the bottom of the tree, we can only have data nodes here
        let pos = mut_hash as usize & (CHILD_SIZE - 1);
        let node = bucket[pos].get_ptr();
        match node {
            None => { Err(new) },
            Some(node_ptr) => {
                let data_node = self.get_data_node(node_ptr);
                if data_node.value.as_ref() == Some(expected) {
                    match self.try_update(&bucket[pos], node_ptr, hash, new) {
                        Ok(()) => {
                            self.manager.retire(node_ptr, 0);
                            Ok(())
                        },
                        Err((value, _)) => {
                            Err(value)
                        }
                    }
                } else {
                    Err(new)
                }
            }
        }
    }

    fn try_update(&self, position: &AtomicMarkablePtr<K, V>, old: *mut Node<K, V>, key: u64, value: V) -> Result<(), (V, *mut Node<K, V>)> {
        let new_data_node: DataNode<K, V> = DataNode::new(key, value);
        let data_node_ptr = Box::into_raw(Box::new(Node::Data(new_data_node)));

        match position.compare_exchange(old, data_node_ptr) {
            Ok(_) => Ok(()),
            Err(current) => {
                unsafe {
                    if let Node::Data(node) = ptr::replace(data_node_ptr, Node::Data(DataNode::default())) {
                        let data = node.value.unwrap();
                        Box::from_raw(data_node_ptr);
                        Err((data, current))
                    } else {
                        panic!("Unexpected array node!")
                    }
                }
            }
        }
    }

    pub fn remove<Q: ?Sized>(&self, key: &Q, expected: &V) -> Option<V>
    where K: Borrow<Q>,
          Q: Eq + Hash + Send + Debug  
    {
        let hash = self.hash(key);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < (KEY_SIZE - self.shift_step) {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => { return None; },
                Some(mut node_ptr) => {
                    if atomic_markable::is_array_node(node_ptr) {
                        bucket = HashMap::get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return None; },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_array_node(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                            continue;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }
                            }
                            // Hazard pointer is safe here
                            if atomic_markable::is_array_node(node_ptr) {
                                bucket = HashMap::get_bucket(node_ptr);
                                continue;
                            } else if atomic_markable::is_marked(node_ptr) {
                                bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                continue;
                            }
                        }
                        let data_node = self.get_data_node(node_ptr);
                        if data_node.key == hash {
                            if data_node.value.as_ref() != Some(expected) {
                                return None
                            }
                            match self.try_remove(&bucket[pos], node_ptr) {
                                Ok(()) => {
                                    // Get the value out of the node_ptr, return it, retire it?
                                    unsafe {
                                        let owned_node = ptr::replace(node_ptr, Node::Data(DataNode::default()));
                                        if let Node::Data(node) = owned_node {
                                            let data = node.value;
                                            self.manager.retire(node_ptr, 0);
                                            return data;
                                        } else {
                                            panic!("Unexpected array node!");
                                        }
                                    }
                                },
                                Err(current) => {
                                    if atomic_markable::is_array_node(current) {
                                        bucket = HashMap::get_bucket(current);
                                    } else if atomic_markable::is_marked(current)
                                        && ptr::eq(atomic_markable::unmark(current), node_ptr) 
                                    {
                                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                    } else {
                                        return None
                                    }
                                }
                            }
                        } else {
                            return None
                        }
                    }
                }
            }
            r += self.shift_step;
        }
        let pos = mut_hash as usize & (bucket.len() - 1);
        let node = bucket[pos].get_ptr();
        match node {
            None => None,
            Some(node_ptr) => {
                let data_node = self.get_data_node(node_ptr);
                if data_node.value.as_ref() == Some(expected) {
                    match self.try_remove(&bucket[pos], node_ptr) {
                        Err(_) => None,
                        Ok(()) => {
                            unsafe {
                                let owned_node = ptr::replace(node_ptr, Node::Data(DataNode::default()));
                                if let Node::Data(node) = owned_node {
                                    let data = node.value;
                                    self.manager.retire(node_ptr, 0);
                                    data
                                } else {
                                    panic!("Unexpected array node!");
                                }
                            }
                        }
                    }
                } else {
                    None
                }
            }
        }
    }

    fn get_clone<Q: ?Sized>(&self, key: &Q) -> Option<V> 
    where K: Borrow<Q>,
          Q: Eq + Hash + Send + Debug,
          V: Clone
    {
        let hash = self.hash(key);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < (KEY_SIZE - self.shift_step) {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => { return None; }
                Some(mut node_ptr) => {
                    if atomic_markable::is_array_node(node_ptr) {
                        bucket = HashMap::get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        // Check the hazard pointer
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { break; },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_array_node(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                            continue;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }            
                            }
                            // Hazard pointer should be fine now
                            if atomic_markable::is_marked(node_ptr) {
                               bucket = HashMap::get_bucket(self.expand_map(bucket, pos, self.shift_step));
                                continue;
                            } else if atomic_markable::is_array_node(node_ptr) {
                                HashMap::get_bucket(node_ptr);
                                continue;
                            }
                        }
                        let data_node = self.get_data_node(node_ptr);
                        if data_node.key == hash {
                            return data_node.value.clone()
                        } else {
                            return None
                        }
                    }
                }
            }

            r += self.shift_step;
        }
        // We should only be here if we got to the bottom
        let pos = mut_hash as usize & (CHILD_SIZE - 1);
        if let Some(node_ptr) = bucket[pos].get_ptr() {
            unsafe {
                match &*node_ptr {
                    &Node::Array(_) => panic!("Unexpected array node!"),
                    &Node::Data(ref data_node) => {
                        return data_node.value.clone()
                    }
                }
            }
        } else {
            return None
        }
    }

    fn try_remove(&self, position: &AtomicMarkablePtr<K, V>, old: *mut Node<K, V>) -> Result<(), *mut Node<K, V>> {
        match position.compare_exchange(old, ptr::null_mut()) {
            Ok(_) => Ok(()),
            Err(current) => Err(current)
        }
    }

    fn get_bucket<'a>(array_node: *mut Node<K, V>) -> &'a Vec<AtomicMarkablePtr<K, V>> {
        unsafe {
            match &*(atomic_markable::unmark_array_node(array_node)) {
                &Node::Data(_) => panic!("Unexpected data node!"),
                &Node::Array(ref array_node) => { &array_node.array }
            }
        }
    }

    fn get_data_node(&self, node_ptr: *mut Node<K, V>) -> & DataNode<K, V> {
        unsafe {
            match &*(atomic_markable::unmark(node_ptr)) {
                &Node::Data(ref data_node) => { data_node },
                &Node::Array(_) => panic!("Unexpected array node!")
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
      V: Eq + Send + Debug
{
    fn default() -> Self {
        HashMap::new()
    }
}

#[derive(Debug)]
pub enum UpdateResult<'inside, 'expected: 'inside, V: 'expected + Eq> {
    FailNotPossible(&'expected V),
    FailDifferentValue(&'inside V)
}

impl<'inside, 'expected: 'inside, V: 'expected + Eq> PartialEq for UpdateResult<'inside, 'expected, V> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (&UpdateResult::FailNotPossible(v1), &UpdateResult::FailNotPossible(v2)) => v1 == v2,
            (&UpdateResult::FailDifferentValue(v1), &UpdateResult::FailDifferentValue(v2)) => v1 == v2,
            _ => false
        }
    }
}

mod tests {
    #![allow(unused_imports)]
    use super::HashMap;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_single_thread_semantics() {
        let map : HashMap<u8, u8> = HashMap::new();

        //assert!(map.insert(9, 9).is_ok());
        //assert!(map.insert(9, 7).is_err());

        for i in 0..240 {
            match map.insert(i, i) {
                Ok(_) => {},
                Err(_) => assert!(false)
            }
        }
        
        assert_eq!(map.get(&3), Some(&3));
        assert_eq!(map.get(&250), None);

        assert_eq!(map.update(&3, &3, 7), Ok(()));
        assert_eq!(map.update(&239, &239, 7), Ok(()));
        assert_eq!(map.get(&3), Some(&7));

        println!("{:?}", map);

        println!("{:?}", map.get(&3));
        assert_eq!(map.remove(&3, &7), Some(7));
        assert_eq!(map.remove(&250, &2), None);

        assert_eq!(map.get(&3), None);
    }

    #[test]
    fn test_borrow_string_map() {
        let map: HashMap<String, u16> = HashMap::new();
        let _ = map.insert("hello".to_owned(), 8);
        assert_eq!(map.get_clone("hello"), Some(8));
        assert_eq!(map.get("hello"), Some(&8));
        assert_eq!(map.remove("hello", &8), Some(8));
    }

    #[test]
    fn test_multithreaded_insert() {
        let map: Arc<HashMap<u16, String>> = Arc::new(HashMap::new());
        let mut wait_vec: Vec<thread::JoinHandle<()>> = Vec::new();

        for i in 0..10 {
            let map_clone = map.clone();
            wait_vec.push(thread::spawn(move || {
                for j in 0..2000 {
                    let val = format!("{}--{}", i, j);
                    println!("inserting");
                    match map_clone.insert(j, val) {
                        Ok(()) => {},
                        Err((key, value)) => {
                            let expected = map_clone.get(&key);
                            match expected {
                                Some(expected_value) => {
                                    println!("updating");
                                    let _ = map_clone.update(&key, expected_value, value);
                                },
                                None => {}
                            }
                        }
                    }
                }
            }));
        }

        for handle in wait_vec {
            println!("joined: {:?}", handle);
            match handle.join() {
                Ok(_) => {},
                Err(_) => panic!("A thread panicked, test failed!")
            }
        }
        println!("{:?}", map.get(&1174));
    }
}

