use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::Debug;
use std::fmt;
use std::ptr;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::collections::hash_map::RandomState;
use memory::HPBRManager;
use super::atomic_markable::AtomicMarkablePtr;
use super::atomic_markable;
use super::data_guard::DataGuard;

const HEAD_SIZE: usize = 256;
const CHILD_SIZE: usize = 16;
const KEY_SIZE: usize = 64;
const MAX_FAILURES: u64 = 10;

/// A wait-free HashMap based on a tree structure.
///
/// This hashmap is an implementation of the Wait-Free HashMap presented in the paper [A Wait-Free HashMap]
/// (https://dl.acm.org/citation.cfm?id=3079519) with a few tweaks to make it usable in Rust. The general structure
/// is unchanged, and follows the tree structure laid out in the paper.
///
/// The head of the hashmap is an array of HEAD_SIZE elements, each one can either point to a node 
/// containing data, or a node containing an array of CHILD_SIZE elements, where CHILD_SIZE is smaller
/// than HEAD_SIZE. By default, this implementation uses a HEAD_SIZE of 256 and a CHILD_SIZE of 16.
/// Once a slot contains an array node, it can never be changed, which allows for a number of memory
/// management guarantees.
///
/// Keys are not currently stored in the hashmap, only values and the corresponding hash. 
/// This can easily be changed if needed. Finding a value in the map follows this process:
///
/// * The hash is computed from the key. This hash will always be a 64-bit integer, and needs to be unique. 
/// If two keys hash to the same value, only one can be inserted. This should not be a problem in most cases.
/// * The first `n` bits of the key are used to index into the head array through bitwise AND. 
/// Here, `n` is defined as `log2(HEAD_SIZE)`.
/// * If we find a data node, we have found the value, if we find an array node, then we 
/// shift the hash 'r' bits to the right, where r is `log2(CHILD_SIZE)`. We can use 
/// this to index into the new array, and continue.
/// * If we reach a null spot at any point, then the element is not in the array.
/// * Once we reach the bottom, the full key will have been used, ensuring correct hashing given unique hashing.
///
/// The tree structure is bounded by HEAD_SIZE and CHILD_SIZE, such that 
/// `max_depth = (hash_size - log2(HEAD_SIZE)) / log2(CHILD_SIZE)`. In this case, 
/// that means the maximum depth is 14. This is used to justify the implementation of 
/// recursive destructors: they should not be able to overflow the stack.
pub struct HashMap<K, V> 
where K: Send,
      V: Send
{
    head: Vec<AtomicMarkablePtr<Node<K, V>>>,
    hasher: RandomState,
    head_size: usize,
    shift_step: usize,
    manager: HPBRManager<Node<K, V>>
}

impl<K: Hash + Send, V: Send> HashMap<K, V> {
    //// Create a new Wait-Free HashMap with the default head and child sizes.
    /// # Examples
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new(); // Creates a new map of String to u8
    /// ```
    pub fn new() -> Self {
        let mut head: Vec<AtomicMarkablePtr<Node<K, V>>> = Vec::with_capacity(HEAD_SIZE);
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

    /// Hash a single element with the default Rust hasher initialised to a random state.
    fn hash<Q: ?Sized>(&self, key: &Q) -> u64 
    where K: Borrow<Q>,
          Q: Hash + Send 
    {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Attempt to set the current MarkablePtr to point to an ArrayNode. This function adds the old DataNode
    /// at this position to the new ArrayNode.
    fn expand_map(&self, bucket: &Vec<AtomicMarkablePtr<Node<K, V>>>, pos: usize, shift_amount: usize) -> *mut Node<K, V> {
        // We know this node must exist
        let node = bucket[pos].get_ptr().unwrap();
        self.manager.protect(atomic_markable::unmark(node), 0);
        if atomic_markable::is_marked_second(node) {
            //println!("already expanded: {:b}", node as usize);
            return node
        }
        let node2 = bucket[pos].get_ptr().unwrap();
        if !ptr::eq(node, node2) {
            //println!("someone else: {:b}", node2 as usize);
            return node2
        }

        let array_node: ArrayNode<K, V> = ArrayNode::new(CHILD_SIZE);
        unsafe {
            let hash = match &*atomic_markable::unmark(node) {
                &Node::Data(ref data_node) => data_node.hash,
                &Node::Array(_) => {panic!("Unexpected array node!")}
            };
            let new_pos = (hash >> (shift_amount + self.shift_step)) as usize & (CHILD_SIZE - 1);
            array_node.array[new_pos].store(atomic_markable::unmark(node));

            let array_node_ptr = Box::into_raw(Box::new(Node::Array(array_node)));
            let array_node_ptr_marked = atomic_markable::mark_second(array_node_ptr);
            return match bucket[pos].compare_exchange(node, array_node_ptr_marked) {
                Ok(_) => {
                    //println!("expanded on me");
                    array_node_ptr_marked
                },
                Err(current) => {
                    //println!("someone else: {:b}", current as usize);
                    // Need to remove the pointer to the old element or this will delete a valid node
                    let vec = get_bucket(array_node_ptr);
                    vec[new_pos].store(ptr::null_mut()); 
                    Box::from_raw(array_node_ptr);
                    current
                }
            }
        }
    }

    /// Attempt to insert the given value with the given key into the HashMap.
    /// # Panics
    /// If the internal structure of the map becomes inconsistent, this will panic.
    /// # Errors
    /// If the the new key/value pair cannot be inserted, either because of contention
    /// or the value already being in the map, an Err will be returned containing the attempted
    /// insertion values.
    /// # Examples:
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new();
    /// map.insert("hello".to_owned(), 8);
    /// ```
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
                        value = match self.try_insert(&bucket[pos], ptr::null_mut(), hash, value) {
                            Ok(_) => { return Ok(()) },
                            Err(old) => {
                                node = bucket[pos].get_ptr();
                                fail_count += 1;
                                old
                            } 
                        }
                    },
                    Some(mut node_ptr) => {
                        if atomic_markable::is_marked(node_ptr) {
                            // Check that doing this never breaks, ie expand_map returns a data node
                            let new_bucket_ptr = self.expand_map(bucket, pos, r);
                            if atomic_markable::is_marked_second(new_bucket_ptr) {
                                bucket = get_bucket(new_bucket_ptr);
                                break;
                            } else {
                                node_ptr = new_bucket_ptr;
                            }
                        }
                        if atomic_markable::is_marked_second(node_ptr) {
                            bucket = get_bucket(node_ptr);
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
                                let data_node = get_data_node(node_ptr);
                                if data_node.hash == hash {
                                    return Err((key, value))
                                }
                                match bucket[pos].compare_and_mark(node_ptr) {
                                    Ok(_) => {
                                        let new_ptr = self.expand_map(bucket, pos, r);
                                        if atomic_markable::is_marked_second(new_ptr) {
                                            bucket = get_bucket(new_ptr);
                                            break;
                                        } else {
                                            fail_count += 1;
                                        }
                                    },
                                    Err(current) => {
                                        if atomic_markable::is_marked_second(current) {
                                            bucket = get_bucket(current);
                                            break;
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

            r += self.shift_step;
        }
        let pos = mut_hash as usize & (CHILD_SIZE - 1);
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

    /// Retrieve a **reference** to the element in the HashMap with the given key. Returns None if
    /// the element is not inside the map. It is 
    /// important to note that this is only a reference because if the data is removed by another thread it
    /// could be deleted. This method guarantees that the reference will be protected for this thread until
    /// the next map method is called, as it will be stored in a hazard pointer. If the data needs to persist
    /// for longer than that, it is recommended to use `get_clone`.
    /// # Panics
    /// If the internal state of the HashMap becomes inconsistent, this method will panic.
    /// # Examples
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new();
    /// map.insert("hello".to_owned(), 8);
    /// assert_eq!(map.get("hello"), Some(&8));
    /// ``` 
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<DataGuard<V, Node<K, V>>>
    where K: Borrow<Q>,
          Q: PartialEq + Hash + Send  
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
                    if atomic_markable::is_marked(node_ptr) {
                        let new_bucket_ptr = self.expand_map(bucket, pos, r);
                        node_ptr = new_bucket_ptr;
                    }
                    if atomic_markable::is_marked_second(node_ptr) {
                        bucket = get_bucket(node_ptr);
                        r += self.shift_step;
                        continue;
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        // Check the hazard pointer
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return None },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            //println!("hello");
                                            node_ptr = self.expand_map(bucket, pos, r);
                                            bucket = get_bucket(node_ptr);
                                            //println!("fart");
                                            break;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }            
                            }
                            // Hazard pointer should be fine now
                            if atomic_markable::is_marked(node_ptr) {
                                bucket = get_bucket(self.expand_map(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            } else if atomic_markable::is_marked_second(node_ptr) {
                                bucket = get_bucket(node_ptr);
                                r += self.shift_step;
                                continue;
                            }
                        }
                        let data_node = get_data_node(node_ptr);
                        if data_node.hash == hash {
                            let hp_handle = self.manager.protect_dynamic(atomic_markable::unmark(node_ptr));
                            self.manager.unprotect(0);
                            if data_node.value.is_none() {
                                println!("fuck");
                            }
                            return Some(DataGuard::new(data_node.value.as_ref().unwrap(), hp_handle));
                        } else {
                            return None
                        }
                    }
                }
            }
        }
        // We should only be here if we got to the bottom
        let pos = mut_hash as usize & (CHILD_SIZE - 1);
        if let Some(node_ptr) = bucket[pos].get_ptr() {
            unsafe {
                match &*node_ptr {
                    &Node::Array(_) => panic!("Unexpected array node!"),
                    &Node::Data(ref data_node) => {
                        let hp_handle = self.manager.protect_dynamic(atomic_markable::unmark(node_ptr));
                        self.manager.unprotect(0);
                        if data_node.value.is_none() {
                            println!("fuck");
                        }
                        return Some(DataGuard::new(data_node.value.as_ref().unwrap(), hp_handle));
                    }
                }
            }
        } else {
            return None
        }
    }

    fn try_insert(&self, position: &AtomicMarkablePtr<Node<K, V>>, old: *mut Node<K, V>, hash: u64, value: V) -> Result<(), V> {
        let data_node: DataNode<K, V> = DataNode::new(value, hash);
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

    /// Attempt to update a value in the map with the given key and expected value. The 
    /// expected value is needed so that a newer element cannot be overwrittn with an old one
    /// by another thread.
    /// # Panics
    /// This method will panic if the internal state of the HashMap becomes inconsistent.
    /// # Errors
    /// This method returns Err containing the attempted insertion value on the following conditions:
    /// * The CAS fails.
    /// * The expected value does not match the actual one.
    /// * The key is not in the map.
    /// # Examples
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new();
    /// map.insert("hello".to_owned(), 8);
    /// assert_eq!(map.get("hello"), Some(&8));
    /// map.update("hello", &8, 24);
    /// assert_eq!(map.get("hello"), Some(&24));
    /// assert_eq!(map.update("rust", &7, 7), Err(7));
    /// ```
    pub fn update<'a, 'b, Q: ?Sized>(&'a self, key: &Q, expected: &'b V, mut new: V) -> Result<(), V>
    where K: Borrow<Q>,
          Q: PartialEq + Hash + Send,
          V: PartialEq  
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
                    if atomic_markable::is_marked(node_ptr) {
                        let new_bucket_ptr = self.expand_map(bucket, pos, r);
                        node_ptr = new_bucket_ptr;
                    }
                    if atomic_markable::is_marked_second(node_ptr) {
                        bucket = get_bucket(node_ptr);
                        r += self.shift_step;
                        continue;
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return Err(new); },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = get_bucket(self.expand_map(bucket, pos, r));
                                            break;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }
                            }
                            if atomic_markable::is_marked_second(node_ptr) {
                                bucket = get_bucket(node_ptr);
                                r += self.shift_step;
                                continue;
                            } else if atomic_markable::is_marked(node_ptr) {
                                bucket = get_bucket(self.expand_map(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            }
                        }
                        // Hazard pointer is safe now, so we can access the node
                        let data_node = get_data_node(node_ptr);
                        if data_node.hash == hash {
                            if data_node.value.as_ref() != Some(expected) {
                                return Err(new)
                            }
                            new = match self.try_update(&bucket[pos], node_ptr, hash, new) {
                                Ok(()) => { 
                                    self.manager.retire(node_ptr, 0);
                                    return Ok(()) 
                                },
                                Err((value, current_ptr)) => {
                                    if atomic_markable::is_marked_second(current_ptr) {
                                        bucket = get_bucket(current_ptr);
                                        value
                                    } else if atomic_markable::is_marked(current_ptr) &&
                                              ptr::eq(node_ptr, atomic_markable::unmark(current_ptr)) 
                                    {
                                        bucket = get_bucket(self.expand_map(bucket, pos, r));
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
                let data_node = get_data_node(node_ptr);
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

    fn try_update(&self, position: &AtomicMarkablePtr<Node<K, V>>, old: *mut Node<K, V>, hash: u64, value: V) -> Result<(), (V, *mut Node<K, V>)> {
        let new_data_node: DataNode<K, V> = DataNode::new(value, hash);
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

    /// Attempt to remove the element with the given key and expected value from the HashMap.
    /// Returns the removed value on success, and None on failure.
    /// # Panics
    /// This method panics if the internal state of the HashMap becomes inconsistent.
    /// # Examples
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new();
    /// map.insert("hello".to_owned(), 8);
    /// assert_eq!(map.get("hello"), Some(&8));
    /// assert_eq!(map.remove("hello", &8), Some(8));
    /// assert_eq!(map.get("hello"), None);
    /// ```
    pub fn remove<Q: ?Sized>(&self, key: &Q, expected: &V) -> Option<V>
    where K: Borrow<Q>,
          Q: PartialEq + Hash + Send,
          V: PartialEq   
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
                    if atomic_markable::is_marked_second(node_ptr) {
                        bucket = get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = get_bucket(self.expand_map(bucket, pos, r));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return None; },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            bucket = get_bucket(self.expand_map(bucket, pos, r));
                                            continue;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }
                            }
                            // Hazard pointer is safe here
                            if atomic_markable::is_marked_second(node_ptr) {
                                bucket = get_bucket(node_ptr);
                                r += self.shift_step;
                                continue;
                            } else if atomic_markable::is_marked(node_ptr) {
                                bucket = get_bucket(self.expand_map(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            }
                        }
                        let data_node = get_data_node(node_ptr);
                        if data_node.hash == hash {
                            if data_node.value.as_ref() != Some(expected) {
                                return None
                            }
                            match self.try_remove(&bucket[pos], node_ptr) {
                                Ok(()) => {
                                    // Get the value out of the node_ptr, return it, retire it?
                                    unsafe {
                                        //println!("removed: {:b}", node_ptr as usize);
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
                                    if atomic_markable::is_marked_second(current) {
                                        bucket = get_bucket(current);
                                    } else if atomic_markable::is_marked(current)
                                        && ptr::eq(atomic_markable::unmark(current), node_ptr) 
                                    {
                                        bucket = get_bucket(self.expand_map(bucket, pos, r));
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
                //println!("nodeptr: {:b}", node_ptr as usize);
                let data_node = get_data_node(node_ptr);
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

    /// Retrieves a clone of the element with the given key, where the clone is created using
    /// the method defined on the `Clone` trait. This is safer than using the reference get,
    /// and is essential if values will need to live outside of the map.
    /// # Panics
    /// This method will panic if the internal state of the HashMap becomes inconsistent.
    /// # Examples
    /// ```
    /// let map: HashMap<String, u8> = HashMap::new();
    /// map.insert("hello".to_owned(), 8);
    /// assert_eq!(map.get_clone("hello"), Some(8));
    /// ``` 
    pub fn get_clone<Q: ?Sized>(&self, key: &Q) -> Option<V> 
    where K: Borrow<Q>,
          Q: PartialEq + Hash + Send,
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
                    if atomic_markable::is_marked(node_ptr) {
                        let new_bucket_ptr = self.expand_map(bucket, pos, r);
                        node_ptr = new_bucket_ptr;
                        /* if atomic_markable::is_marked_second(new_bucket_ptr) {
                            //println!("hello 1: {:b}", new_bucket_ptr as usize);
                            bucket = get_bucket(new_bucket_ptr);
                            //println!("fart");
                        } else {
                            //println!("hello 2");
                            node = Some(new_bucket_ptr);
                            //println!("fart");
                        } */
                    }
                    if atomic_markable::is_marked_second(node_ptr) {
                        bucket = get_bucket(node_ptr);
                        r += self.shift_step;
                        continue;
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        // Check the hazard pointer
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => { return None },
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            // Force a bucket update
                                            //println!("hello");
                                            node_ptr = self.expand_map(bucket, pos, r);
                                            bucket = get_bucket(node_ptr);
                                            //println!("fart");
                                            break;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }            
                            }
                            // Hazard pointer should be fine now
                            if atomic_markable::is_marked(node_ptr) {
                                bucket = get_bucket(self.expand_map(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            } else if atomic_markable::is_marked_second(node_ptr) {
                                bucket = get_bucket(node_ptr);
                                r += self.shift_step;
                                continue;
                            }
                        }
                        let data_node = get_data_node(node_ptr);
                        if data_node.hash == hash {
                            return data_node.value.clone();
                        } else {
                            return None
                        }
                    }
                }
            }
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

    fn try_remove(&self, position: &AtomicMarkablePtr<Node<K, V>>, old: *mut Node<K, V>) -> Result<(), *mut Node<K, V>> {
        match position.compare_exchange(old, ptr::null_mut()) {
            Ok(_) => Ok(()),
            Err(current) => Err(current)
        }
    }

    pub fn iter(&self) -> Iter<K, V> {
        Iter::new(&self.head, &self.manager)
    }
}

fn get_bucket<'a, K: Send, V: Send>(node_ptr: *mut Node<K, V>) -> &'a Vec<AtomicMarkablePtr<Node<K, V>>> {
    unsafe {
        match &*(atomic_markable::unmark_second(node_ptr)) {
            &Node::Data(_) => panic!("Unexpected data node!: {:b}", node_ptr as usize),
            &Node::Array(ref array_node) => &array_node.array
        }
    }
}

fn get_data_node<'a, K: Send, V: Send>(node_ptr: *mut Node<K, V>) -> &'a DataNode<K, V> {
    unsafe {
        match &*(atomic_markable::unmark(node_ptr)) {
            &Node::Data(ref data_node) => data_node,
            &Node::Array(_) => panic!("Unexpected array node!: {:b}", node_ptr as usize)
        }
    }
}

impl<K, V> Debug for HashMap<K, V> 
where K: PartialEq + Hash + Send + Debug,
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
                node_ptr = atomic_markable::unmark_second(atomic_markable::unmark(node_ptr));
                unsafe {
                    match &*node_ptr {
                        &Node::Array(ref array_node) => {array_node.to_string(&mut string, 1);},
                        &Node::Data(ref data_node) => {string.push_str(&format!("{:X} ==> {:?}", data_node.hash, data_node.value));}
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

pub struct Iter<'a, K: Send + 'a, V: Send + 'a> {
    current_array: &'a Vec<AtomicMarkablePtr<Node<K, V>>>,
    index: usize,
    node_stack: Vec<&'a Vec<AtomicMarkablePtr<Node<K, V>>>>,
    manager: &'a HPBRManager<Node<K, V>>
}

impl<'a, K: Send, V: Send> Iter<'a, K, V> {
    fn new(start: &'a Vec<AtomicMarkablePtr<Node<K, V>>>, manager: &'a HPBRManager<Node<K, V>>) -> Self {
        Self {
            current_array: start,
            index: 0,
            node_stack: Vec::new(),
            manager
        }
    }
}

impl<'a, K: Send, V: Send> Iterator for Iter<'a, K, V> {
    type Item = DataGuard<'a, V, Node<K, V>>;
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        if index < self.current_array.len() {
            // Check if data or array
            match self.current_array[index].get_ptr() {
                Some(mut node_ptr) => {
                    // Protect with a HPHandle
                    if atomic_markable::is_marked(node_ptr) {
                        // Protect
                        let mut hphandle = self.manager.protect_dynamic(atomic_markable::unmark(node_ptr));
                        // need to loop here
                        while Some(node_ptr) != self.current_array[index].get_ptr() {
                            let new_node = self.current_array[index].get_ptr();
                            match new_node {
                                None => return self.next(),
                                Some(new_ptr) => {
                                    hphandle = self.manager.protect_dynamic(atomic_markable::unmark(atomic_markable::unmark_second(node_ptr)));
                                    if atomic_markable::is_marked_second(new_ptr) {
                                        let bucket = get_bucket(new_ptr);
                                        self.node_stack.push(bucket);
                                        return self.next()
                                    }
                                    node_ptr = new_ptr;
                                }
                            }
                        }
                        let data_node = get_data_node(atomic_markable::unmark(node_ptr));
                        Some(DataGuard::new(&data_node.value.as_ref().unwrap(), hphandle))
                    } else if atomic_markable::is_marked_second(node_ptr) {
                        let bucket = get_bucket(node_ptr);
                        self.node_stack.push(bucket);
                        return self.next()
                    } else {
                        let mut hphandle = self.manager.protect_dynamic(node_ptr);
                        while Some(node_ptr) != self.current_array[index].get_ptr() {
                            let new_node = self.current_array[index].get_ptr();
                            match new_node {
                                None => return self.next(),
                                Some(new_ptr) => {
                                    hphandle = self.manager.protect_dynamic(atomic_markable::unmark(atomic_markable::unmark_second(node_ptr)));
                                    if atomic_markable::is_marked_second(new_ptr) {
                                        let bucket = get_bucket(new_ptr);
                                        self.node_stack.push(bucket);
                                        return self.next()
                                    }
                                    node_ptr = new_ptr;
                                }
                            }
                        }

                        let data_node = get_data_node(atomic_markable::unmark(node_ptr));
                        Some(DataGuard::new(&data_node.value.as_ref().unwrap(), hphandle))
                    }
                },
                None => {
                    return self.next()
                }
            }
        } else {
            match self.node_stack.pop() {
                Some(array) => {
                    self.index = 0;
                    self.current_array = array;
                    return self.next()
                },
                None => None
            }
        }
    }

}

impl<K, V> Default for HashMap<K, V>
where K: PartialEq + Hash + Send,
      V: PartialEq + Send 
{
    fn default() -> Self {
        HashMap::new()
    }
}

pub enum Node<K: Send, V: Send> {
    Data(DataNode<K, V>),
    Array(ArrayNode<K, V>)
}

pub struct DataNode<K: Send, V: Send> {
    value: Option<V>,
    hash: u64,
    _marker: PhantomData<K>
}

impl<K: Send, V: Send> DataNode<K, V> {
    fn new(value: V, hash: u64) -> Self {
        DataNode {
            value: Some(value),
            hash,
            _marker: PhantomData
        }
    }
}

impl<K: Send, V: Send> Default for DataNode<K, V> {
    fn default() -> Self {
        DataNode {
            value: None,
            hash: 0,
            _marker: PhantomData
        }
    }
}

pub struct ArrayNode<K: Send, V: Send> {
    array: Vec<AtomicMarkablePtr<Node<K, V>>>,
    size: usize
}

impl<K: Send, V: Send> ArrayNode<K, V> {
    fn new(size: usize) -> Self {
        let mut array = Vec::with_capacity(size);
        for _ in 0..size {
            array.push(AtomicMarkablePtr::default());
        }

        ArrayNode {
            array,
            size
        }
    }

    pub unsafe fn to_string(&self, start: &mut String, depth: usize)
    where K: Debug,
          V: Debug 
    {
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
                node_ptr = atomic_markable::unmark_second(atomic_markable::unmark(node_ptr));
                match &*node_ptr {
                    &Node::Array(ref array_node) => {
                        array_node.to_string(start, depth + 1);
                    },
                    &Node::Data(ref data_node) => {
                        start.push_str(&format!("{:X} ==> {:?}", data_node.hash, data_node.value));
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


mod tests {
    #![allow(unused_imports)]
    extern crate im;
    use self::im::Map;

    use rand::{thread_rng, Rng};

    use super::HashMap;
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;
    use std::hash::Hash;
    use std::fmt::Debug;
    use super::super::super::super::testing::{LinearizabilityTester, LinearizabilityResult, ThreadLog};

    #[test]
    #[ignore]
    fn test_data_guard() {
        let map: HashMap<u8, u8> = HashMap::new();

        let _ = map.insert(23, 23);
        match map.get(&23) {
            Some(g) => {
                assert_eq!(g.data(), &23);
                assert_eq!(g.cloned(), 23);
                println!("guard leaving scope");
            },
            None => {}
        }
        println!("guard left scope");
        let _ = map.insert(24, 24);
        let _ = map.insert(25, 25);
    }

    #[test]
    #[ignore]
    fn test_single_thread_semantics() {
        let map : HashMap<u8, String> = HashMap::new();

        for i in 0..240 {
            match map.insert(i, format!("{}", i)) {
                Ok(_) => {},
                Err(_) => assert!(false)
            }
        }
        
        assert!(map.insert(9, "9".to_owned()).is_err());

        assert_eq!(map.get(&3).unwrap().data(), &"3".to_owned());
        assert_eq!(map.get(&250), None);

        assert_eq!(map.update(&3, map.get(&3).unwrap().data(), format!("{}", 7)), Ok(()));
        assert_eq!(map.update(&239, map.get(&239).unwrap().data(), format!("{}", 7)), Ok(()));
        assert_eq!(map.get(&3).unwrap().data(), &"7".to_owned());

        println!("{:?}", map);

        println!("{:?}", map.get(&3));
        assert_eq!(map.remove(&3, &"7".to_owned()), Some("7".to_owned()));
        assert_eq!(map.remove(&250, &"2".to_owned()), None);

        assert_eq!(map.get(&3), None);
    }

    #[test]
    #[ignore]
    fn test_borrow_string_map() {
        let map: HashMap<String, u16> = HashMap::new();
        let _ = map.insert("hello".to_owned(), 8);
        assert_eq!(map.get_clone("hello"), Some(8));
        assert_eq!(map.get("hello").unwrap().data(), &8);
        assert_eq!(map.remove("hello", &8), Some(8));
    }

    #[test]
    #[ignore]
    fn test_multithreaded_insert() {
        let map: Arc<HashMap<u16, String>> = Arc::new(HashMap::new());
        let mut wait_vec: Vec<thread::JoinHandle<()>> = Vec::new();

        for i in 0..10 {
            let map_clone = map.clone();
            wait_vec.push(thread::spawn(move || {
                for j in 0..2000 {
                    let val = format!("hello");
                    //println!("inserting");
                    match map_clone.insert(j, val) {
                        Ok(()) => {},
                        Err((key, value)) => {
                            let expected = map_clone.get(&key);
                            match expected {
                                Some(expected_value) => {
                                    //println!("updating");
                                    let _ = map_clone.update(&key, &expected_value.cloned(), value);
                                },
                                None => {}
                            }
                        }
                    }
                }
            }));
        }

        for handle in wait_vec {
            //println!("joined: {:?}", handle);
            match handle.join() {
                Ok(_) => {},
                Err(_) => panic!("A thread panicked, test failed!")
            }
        }
        println!("threads done");
        //println!("{:?}", map.get(&1174));
    }

    #[test]
    fn test_typical() {
        let map: Arc<HashMap<u32, String>> = Arc::default();
        let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
        let num_threads = 16;

        for _ in 0..num_threads / 2 {
            let map_clone = map.clone();
            wait_vec.push(thread::spawn(move || {
                    for i in 0..1000 {
                        map_clone.insert(i, format!("hello"));
                    }
                    //println!("done inserting");
                    for i in 1000..2000 {
                        map_clone.get(&i);
                    }
                    //println!("done normal get");
                    for i in 0..7000 {
                        map_clone.get_clone(&(i % 1000));
                    }
                    //println!("done clone get");
                    for i in 0..200 {
                        map_clone.remove(&i, &format!("hello"));
                    }
                    //println!("done removing");
                }));
            }

        for _ in 0..num_threads / 2 {
            let map_clone = map.clone();
            wait_vec.push(thread::spawn(move || {
                for i in 1000..2000 {
                    map_clone.insert(i, format!("hello"));
                }
                //println!("done inserting");
                for i in 0..1000 {
                    if i > 300 && i < 800 {
                        if let Some(guard) = map_clone.get(&i) {
                            //assert_eq!(guard.data(), &format!(""));
                        }
                    }
                }
                //println!("done normal get");
                for i in 0..7000 {
                    map_clone.get_clone(&((i % 1000) + 1000));
                }
                //println!("done clone get");
                for i in 1000..1200 {
                    map_clone.remove(&i, &format!("hello"));
                }
                //println!("done removing");
            }));
        }

        for handle in wait_vec {
            if let Err(_) = handle.join() {
                panic!("Could not join thread!")
            }
        }
    }

    #[derive(Hash)]
    #[derive(Copy)]
    #[derive(Clone)]
    #[derive(Eq)]
    #[derive(PartialEq)]
    #[derive(Debug)]
    enum MapResult<K, V>
    where K: Copy + Clone + Eq + Hash + Debug + Send,
          V: Copy + Clone + Eq + Hash + Debug + Send
    {
        ArgWrap(K, V),
        Insert(Result<(), (K, V)>),
        Get(Option<V>),
        Update(Result<(), V>),
        Remove(Option<V>)
    }

    #[test]
    fn test_linearizable() {
        let map: HashMap<usize, usize> = HashMap::new();
        let sequential: Map<usize, usize> = Map::new();

        let mut linearizer: LinearizabilityTester<HashMap<usize, usize>, Map<usize, usize>, MapResult<usize, usize>>
                = LinearizabilityTester::new(8, 100000, map, sequential);

        fn conc_insert(map: &HashMap<usize, usize>, data: MapResult<usize, usize>)
                -> Option<MapResult<usize, usize>>
        {
            if let MapResult::ArgWrap(key, val) = data {
                Some(MapResult::Insert(map.insert(key, val)))
            } else {
                panic!("Invalid argument")
            }
        }

        fn conc_get(map: &HashMap<usize, usize>, data: MapResult<usize, usize>)
                -> Option<MapResult<usize, usize>>
        {
            if let MapResult::ArgWrap(key, val) = data {
                match map.get(&key) {
                    Some(guard) => Some(MapResult::Get(Some(guard.cloned()))),
                    None => Some(MapResult::Get(None))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn conc_update(map: &HashMap<usize, usize>, data: MapResult<usize, usize>)
                -> Option<MapResult<usize, usize>>
        {
            if let MapResult::ArgWrap(key, val) = data {
                Some(MapResult::Update(map.update(&key, &val, val)))
            } else {
                panic!("Invalid argument")
            }
        }

        fn conc_remove(map: &HashMap<usize, usize>, data: MapResult<usize, usize>)
                -> Option<MapResult<usize, usize>>
        {
            if let MapResult::ArgWrap(key, val) = data {
                Some(MapResult::Remove(map.remove(&key, &val)))
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_insert(map: &Map<usize, usize>, data: Option<MapResult<usize, usize>>)
                -> (Map<usize, usize>, Option<MapResult<usize, usize>>)
        {
            if let MapResult::ArgWrap(key, val) = data.unwrap() {
                if map.contains_key(&key) {
                    (map.clone(), Some(MapResult::Insert(Err((key, val)))))
                } else {
                    (map.insert(key, val), Some(MapResult::Insert(Ok(()))))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_get(map: &Map<usize, usize>, data: Option<MapResult<usize, usize>>)
                -> (Map<usize, usize>, Option<MapResult<usize, usize>>)
        {
            if let MapResult::ArgWrap(key, val) = data.unwrap() {
                match map.get(&key) {
                    Some(arc) => (map.clone(), Some(MapResult::Get(Some(*arc)))),
                    None => (map.clone(), Some(MapResult::Get(None)))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_update(map: &Map<usize, usize>, data: Option<MapResult<usize, usize>>)
                -> (Map<usize, usize>, Option<MapResult<usize, usize>>)
        {
            if let MapResult::ArgWrap(key, val) = data.unwrap() {
                if let Some(value) = map.get(&key) {
                    if *value == val {
                        (map.insert(key, val), Some(MapResult::Update(Ok(()))))
                    } else {
                        (map.clone(), Some(MapResult::Update(Err(val))))
                    }
                } else {
                    (map.clone(), Some(MapResult::Update(Err(val))))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_remove(map: &Map<usize, usize>, data: Option<MapResult<usize, usize>>)
                -> (Map<usize, usize>, Option<MapResult<usize, usize>>)
        {
            if let MapResult::ArgWrap(key, val) = data.unwrap() {
                if let Some(value) = map.get(&key) {
                    if *value == val {
                        (map.remove(&key), Some(MapResult::Remove(Some(val))))
                    } else {
                        (map.clone(), Some(MapResult::Remove(None)))
                    }
                } else {
                    (map.clone(), Some(MapResult::Remove(None)))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn worker(id: usize, log: &mut ThreadLog<HashMap<usize, usize>, Map<usize, usize>, MapResult<usize, usize>>) {
            for _ in 0..1000 {
                let rand = thread_rng().gen_range(0, 101);
                let key = thread_rng().gen_range(0, 101);
                let val = thread_rng().gen_range(0, 101);
                if rand < 25 {
                    log.log_val_result(id, conc_insert, MapResult::ArgWrap(key, val), format!("insert: {} -- {}", key, val), seq_insert);
                } else if rand < 50 {
                    log.log_val_result(id, conc_update, MapResult::ArgWrap(key, val), format!("update: {} -- {}", key, val), seq_update);
                } else if rand < 75 {
                    log.log_val_result(id, conc_get, MapResult::ArgWrap(key, val), format!("get: {} -- {}", key, val), seq_get);
                } else {
                    log.log_val_result(id, conc_remove, MapResult::ArgWrap(key, val), format!("remove: {} -- {}", key, val), seq_remove);
                }
            }
        }

        let result = linearizer.run(worker);

        println!("{:?}", result);

        match result {
            LinearizabilityResult::Success => assert!(true),
            _ => assert!(false)
        }
    }
}

