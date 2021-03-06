use std::hash::{Hash, Hasher, BuildHasher};
use std::ptr;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::iter::Chain;
use memory::HPBRManager;
use super::atomic_markable::AtomicMarkablePtr;
use super::atomic_markable;
use super::data_guard::DataGuard;

const HEAD_SIZE: usize = 256;
const CHILD_SIZE: usize = 16;
const KEY_SIZE: usize = 64;
const MAX_FAILURES: u64 = 10;

/// A wait-free HashSet based on a tree structure.
///
/// This set is an adaptation of the Wait-Free HashMap presented in the paper [A Wait-Free HashMap]
/// (https://dl.acm.org/citation.cfm?id=3079519) with a few tweaks to make it usable in Rust. The general structure
/// is unchanged, and follows the tree structure laid out in the paper.
///
/// The head of the hashmap is an array of HEAD_SIZE elements, each one can either point to a node 
/// containing data, or a node containing an array of CHILD_SIZE elements, where CHILD_SIZE is smaller
/// than HEAD_SIZE. By default, this implementation uses a HEAD_SIZE of 256 and a CHILD_SIZE of 16.
/// Once a slot contains an array node, it can never be changed, which allows for a number of memory
/// management guarantees.
///
/// Finding whether a value is in the set is as follows:
///
/// * The hash is computed from the value. This hash will always be a 64-bit integer, and needs to be unique. 
/// If two values hash to the same value, only one can be inserted. This should not be a problem in most cases.
/// * The first `n` bits of the value are used to index into the head array through bitwise AND. 
/// Here, `n` is defined as `log2(HEAD_SIZE)`.
/// * If we find a data node, we have found the value, if we find an array node, then we 
/// shift the hash 'r' bits to the right, where r is `log2(CHILD_SIZE)`. We can use 
/// this to index into the new array, and continue.
/// * If we reach a null spot at any point, then the element is not in the array.
/// * Once we reach the bottom, the full hash will have been used, ensuring correct hashing given unique hashing.
///
/// The tree structure is bounded by HEAD_SIZE and CHILD_SIZE, such that 
/// `max_depth = (hash_size - log2(HEAD_SIZE)) / log2(CHILD_SIZE)`. In this case, 
/// that means the maximum depth is 14. This is used to justify the implementation of 
/// recursive destructors: they should not be able to overflow the stack.
pub struct HashSet<T: Send> {
    head: Vec<AtomicMarkablePtr<Node<T>>>,
    hasher: RandomState,
    head_size: usize,
    shift_step: usize,
    manager: HPBRManager<Node<T>>
}

impl<T: Hash + Send> HashSet<T> {
    /// Construct a new HashSet.
    /// # Example
    /// ```
    /// let set = HashSet::new();
    /// ```
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

    fn expand(&self, bucket: &Vec<AtomicMarkablePtr<Node<T>>>, pos: usize, shift_amount:usize) -> *mut Node<T> {
        let node = bucket[pos].get_ptr().unwrap();
        self.manager.protect(atomic_markable::unmark(node), 0);
        if atomic_markable::is_marked_second(node) {
            return node
        }

        let node2 = bucket[pos].get_ptr().unwrap();
        if !ptr::eq(node, node2) {
            return node2
        }

        let array_node: ArrayNode<T> = ArrayNode::new(CHILD_SIZE);
        let hash = unsafe { match &*atomic_markable::unmark(node) {
            &Node::Data(ref data_node) => data_node.hash,
            &Node::Array(_) => { panic!("Unexpected array node!") }
        }};

        let new_pos = (hash >> (shift_amount + self.shift_step)) as usize & (CHILD_SIZE - 1);
        array_node.array[new_pos].store(atomic_markable::unmark(node));

        let array_node_ptr = Box::into_raw(Box::new(Node::Array(array_node)));
        let array_node_ptr_marked = atomic_markable::mark_second(array_node_ptr);

        return match bucket[pos].compare_exchange(node, array_node_ptr_marked) {
            Ok(_) => array_node_ptr_marked,
            Err(current) => {
                let vec = get_bucket(array_node_ptr);
                vec[new_pos].store(ptr::null_mut());
                unsafe { Box::from_raw(array_node_ptr) };
                current
            }
        }
    }

    /// Insert a new value into the HashSet.
    /// # Example
    /// ```
    /// let set = HashSet::new();
    /// let _ = set.insert(52);
    /// assert!(set.contains(&52));
    /// ```
    pub fn insert(&self, mut data: T) -> Result<(), T> {
        let hash = self.hash(&data);
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
                        data = match self.try_insert(&bucket[pos], ptr::null_mut(), hash, data) {
                            Ok(()) => return Ok(()),
                            Err(old_data) => {
                                node = bucket[pos].get_ptr();
                                fail_count += 1;
                                old_data
                            }
                        }
                    },
                    Some(mut node_ptr) => {
                        if atomic_markable::is_marked(node_ptr) {
                            let new_bucket_ptr = self.expand(bucket, pos, r);
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
                                let data_node = get_data_node(node_ptr);
                                if data_node.hash == hash {
                                    return Err(data)
                                }
                                match bucket[pos].compare_and_mark(node_ptr) {
                                    Ok(_) => {
                                        let new_ptr = self.expand(bucket, pos, r);
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
                match self.try_insert(&bucket[pos], ptr::null_mut(), hash, data) {
                    Err(val) => Err(val),
                    Ok(_) => Ok(())
                }
            },
            Some(_) => {
                Err(data)
            }
        }
    }

    fn try_insert(&self, position: &AtomicMarkablePtr<Node<T>>, old: *mut Node<T>, hash: u64, value: T) -> Result<(), T> {
        let data_node = DataNode::new(value, hash);
        let data_node_ptr = Box::into_raw(Box::new(Node::Data(data_node)));

        return match position.compare_exchange(old, data_node_ptr) {
            Ok(_) => Ok(()),
            Err(_) => {
                unsafe {
                    let node = ptr::replace(data_node_ptr, Node::Data(DataNode::default()));
                    if let Node::Data(data_node) = node {
                        let data = data_node.value;
                        Box::from_raw(data_node_ptr);
                        Err(data.unwrap())
                    } else {
                        panic!("Unexpected array node!")
                    }
                }
            }
        }
    }

    /// Returns true if the given value is in the set.
    /// # Example
    /// ```
    /// let set = HashSet::new();
    /// let _ = set.insert(52);
    /// assert!(set.contains(&52));
    /// ```
    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool
    where T: Borrow<Q>,
          Q: Hash + Send
    {
        let hash = self.hash(key);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < KEY_SIZE - self.shift_step {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => { return false },
                Some(mut node_ptr) => {
                    if atomic_markable::is_marked(node_ptr) {
                        let new_bucket_ptr = self.expand(bucket, pos, r);
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
                                    None => return false,
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            node_ptr = self.expand(bucket, pos, r);
                                            bucket = get_bucket(node_ptr);
                                            break;
                                        }
                                        node_ptr = new_ptr;
                                    }
                                }
                            }
                            if atomic_markable::is_marked(node_ptr) {
                                bucket = get_bucket(self.expand(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            } else if atomic_markable::is_marked_second(node_ptr) {
                                bucket = get_bucket(node_ptr);
                                r += self.shift_step;
                                continue;
                            }
                        }
                        let data_node = get_data_node(node_ptr);
                        return data_node.hash == hash
                    }
                }
            }
        }

        let pos = mut_hash as usize & (CHILD_SIZE - 1);
        if let Some(node_ptr) = bucket[pos].get_ptr() {
            match unsafe { &*node_ptr } {
                &Node::Array(_) => panic!("Unexpected array node!"),
                &Node::Data(ref data_node) => {
                    data_node.hash == hash
                }
            }
        } else {
            false
        }
    }

    /// Remove the given value from the set. Return the value if removal was successful,
    /// None otherwise.
    /// # Example
    /// ```
    /// let set = HashSet::new();
    /// let _ = set.insert(52);
    /// assert!(set.contains(&52));
    /// set.remove(&52);
    /// assert!(!set.contains(&52));
    /// ```
    pub fn remove<Q: ?Sized>(&self, expected: &Q) -> Option<T> 
    where T: Borrow<Q>,
          Q: Hash + Send
    {
        let hash = self.hash(expected);
        let mut mut_hash = hash;
        let mut r = 0usize;
        let mut bucket = &self.head;

        while r < KEY_SIZE - self.shift_step {
            let pos = mut_hash as usize & (bucket.len() - 1);
            mut_hash >>= self.shift_step;
            let mut node = bucket[pos].get_ptr();

            match node {
                None => return None,
                Some(mut node_ptr) => {
                    if atomic_markable::is_marked_second(node_ptr) {
                        bucket = get_bucket(node_ptr);
                    } else if atomic_markable::is_marked(node_ptr) {
                        bucket = get_bucket(self.expand(bucket, pos, r));
                    } else {
                        self.manager.protect(atomic_markable::unmark(node_ptr), 0);
                        if node != bucket[pos].get_ptr() {
                            let mut fail_count = 0;
                            while node != bucket[pos].get_ptr() {
                                node = bucket[pos].get_ptr();
                                match node {
                                    None => return None,
                                    Some(new_ptr) => {
                                        self.manager.protect(atomic_markable::unmark(atomic_markable::unmark_second(new_ptr)), 0);
                                        fail_count += 1;
                                        if fail_count > MAX_FAILURES {
                                            bucket[pos].mark();
                                            bucket = get_bucket(self.expand(bucket, pos, r));
                                            continue;
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
                                bucket = get_bucket(self.expand(bucket, pos, r));
                                r += self.shift_step;
                                continue;
                            }
                        }
                        let data_node = get_data_node(node_ptr);
                        if data_node.hash == hash {
                            match self.try_remove(&bucket[pos], node_ptr) {
                                Ok(val) => return val,
                                Err(current) => {
                                    if atomic_markable::is_marked_second(current) {
                                        bucket = get_bucket(current);
                                    } else if atomic_markable::is_marked(current) && ptr::eq(atomic_markable::unmark(current), node_ptr) {
                                        bucket = get_bucket(self.expand(bucket, pos, r));
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
                let data_node = get_data_node(node_ptr);
                if data_node.hash == hash {
                    match self.try_remove(&bucket[pos], node_ptr) {
                        Err(_) => None,
                        Ok(val) => val
                    }
                } else {
                    None
                }
            }
        }
    }

    fn try_remove(&self, position: &AtomicMarkablePtr<Node<T>>, old: *mut Node<T>) -> Result<Option<T>, *mut Node<T>> {
        match position.compare_exchange(old, ptr::null_mut()) {
            Ok(_) => {
                let owned = unsafe { ptr::replace(old, Node::Data(DataNode::default())) };
                if let Node::Data(node) = owned {
                    let data = node.value;
                    self.manager.retire(old, 0);
                    Ok(data)
                } else {
                    panic!("Unexpected array node!")
                }
            },
            Err(current) => Err(current)
        }
    }

    /// Retrieve an unordered iterator over the values in the set. The iterator is lazy
    /// so values can be removed before or after they are reached, but all references
    /// are guaranteed to be alive.
    pub fn iter(&self) -> Iter<T> {
        Iter::new(&self.head, &self.manager)
    }

    /// Retrieve a lazy iterator for the difference between this HashSet and another.
    pub fn difference<'a>(&'a self, other: &'a Self) -> Difference<'a, T> {
        Difference {
            iter: Iter::new(&self.head, &self.manager),
            other
        }
    }

    /// Retrieve a lazy iterator over the intersection of this HashSet and another.
    pub fn intersection<'a>(&'a self, other: &'a Self) -> Intersection<'a, T> {
        Intersection {
            iter: Iter::new(&self.head, &self.manager),
            other
        }
    }

    /// Retrieve a lazy iterator over the union of this set and another.
    pub fn union<'a>(&'a self, other: &'a Self) -> Union<'a, T> {
        Union {
            iter: self.iter().chain(other.difference(self))
        }
    }
}

fn get_bucket<'a, T: Send>(node_ptr: *mut Node<T>) -> &'a Vec<AtomicMarkablePtr<Node<T>>> {
    unsafe {
        match &*(atomic_markable::unmark_second(node_ptr)) {
            &Node::Data(_) => panic!("Unexpected data node!: {:b}", node_ptr as usize),
            &Node::Array(ref array_node) => &array_node.array
        }
    }
}

fn get_data_node<'a, T: Send>(node_ptr: *mut Node<T>) -> &'a DataNode<T> {
    unsafe {
        match &*(atomic_markable::unmark(node_ptr)) {
            &Node::Data(ref data_node) => data_node,
            &Node::Array(_) => panic!("Unexpected array node!: {:b}", node_ptr as usize)
        }
    }
}

pub struct Iter<'a, T: Send + 'a> {
    current_array: &'a Vec<AtomicMarkablePtr<Node<T>>>,
    index: usize,
    node_stack: Vec<&'a Vec<AtomicMarkablePtr<Node<T>>>>,
    manager: &'a HPBRManager<Node<T>>
}

pub struct Difference<'a, T: Send + Hash + 'a> {
    iter: Iter<'a, T>,
    other: &'a HashSet<T>
}

pub struct Intersection<'a, T: Send + Hash + 'a> {
    iter: Iter<'a, T>,
    other: &'a HashSet<T>
}

pub struct Union<'a, T: Send + Hash + 'a> {
    iter: Chain<Iter<'a, T>, Difference<'a, T>>
}

impl<'a, T: Send + Hash + 'a> Iterator for Difference<'a, T> {
    type Item = DataGuard<'a, T, Node<T>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let data = self.iter.next()?;
            if !self.other.contains(data.data()) {
                return Some(data)
            }
        }
    }
}

impl<'a, T: Send + Hash + 'a> Iterator for Intersection<'a, T> {
    type Item = DataGuard<'a, T, Node<T>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let data = self.iter.next()?;
            if self.other.contains(data.data()) {
                return Some(data)
            }
        }
    }
}

impl<'a, T: Send + Hash + 'a> Iterator for Union<'a, T> {
    type Item = DataGuard<'a, T, Node<T>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, T:Send> Iter<'a, T> {
    fn new(start: &'a Vec<AtomicMarkablePtr<Node<T>>>, manager: &'a HPBRManager<Node<T>>) -> Self {
        Self {
            current_array: start,
            index: 0,
            node_stack: Vec::new(),
            manager
        }
    }
}

impl<'a, T: Send> Iterator for Iter<'a, T> {
    type Item = DataGuard<'a, T, Node<T>>;
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

pub enum Node<T: Send> {
    Data(DataNode<T>),
    Array(ArrayNode<T>)
}

pub struct DataNode<T: Send> {
    value: Option<T>,
    hash: u64
}

impl<T: Send> DataNode<T> {
    fn new(value: T, hash: u64) -> Self {
        DataNode {
            value: Some(value),
            hash
        }
    }
}

impl<T: Send> Default for DataNode<T> {
    fn default() -> Self {
        DataNode {
            value: None,
            hash: 0
        }
    }
}

pub struct ArrayNode<T: Send> {
    array: Vec<AtomicMarkablePtr<Node<T>>>,
    size: usize
}

impl<T: Send> ArrayNode<T> {
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
}

mod tests {
    #![allow(unused_imports)]
    extern crate im;
    use self::im::Set;

    use rand::{thread_rng, Rng};

    use super::HashSet;
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;
    use std::collections;
    use std::time::Duration;
    use std::hash::Hash;
    use std::fmt::Debug;
    use super::super::super::super::testing::{LinearizabilityTester, LinearizabilityResult, ThreadLog};

    #[test]
     
    fn test_single_threaded() {
        let set: HashSet<u32> = HashSet::new();

        set.insert(54);

        assert!(set.contains(&54));
        assert!(!set.contains(&63));

        assert_eq!(set.remove(&54), Some(54));
        assert!(!set.contains(&54));

        set.insert(60);
        set.insert(72);

        for i in set.iter() {
            println!("{:?}", i.data());
        }

        for i in 0..2500 {
            set.insert(i);
        }

        let mut test_set: collections::HashSet<u32> = collections::HashSet::new();
        let mut counter = 0;
        for i in set.iter() {
            assert!(!test_set.contains(i.data()));
            println!("{:?}", i.data());
            test_set.insert(*i.data());
            counter += 1;
        }

        println!("{:?}", counter);
        assert_eq!(counter, 2500);
    }

    #[test]
     
    fn test_intersection_semantics() {
        let set: HashSet<u32> = HashSet::new();
        let other_set: HashSet<u32> = HashSet::new();

        let _ = set.insert(54);
        let _ = set.insert(32);
        let _ = set.insert(27);
        let _ = set.insert(89);

        let _ = other_set.insert(54);
        let _ = other_set.insert(32);

        let expected = vec![54, 32];
        let mut size = 0;
        for i in set.intersection(&other_set) {
            assert!(expected.contains(i.data()));
            size += 1;
        }

        assert_eq!(size, expected.len());
    }

    #[test]
     
    fn test_union_semantics() {
        let set: HashSet<u32> = HashSet::new();
        let other_set: HashSet<u32> = HashSet::new();

        let _ = set.insert(54);
        let _ = set.insert(32);
        let _ = set.insert(27);
        let _ = set.insert(89);

        let _ = other_set.insert(77);
        let _ = other_set.insert(456);

        let expected = vec![54, 32, 27, 89, 77, 456];
        let mut size = 0;
        for i in set.union(&other_set) {
            assert!(expected.contains(i.data()));
            size += 1;
        }
        assert_eq!(size, expected.len());
    }

    #[test]
     
    fn test_difference_semantics() {
        let set: HashSet<u32> = HashSet::new();
        let other_set: HashSet<u32> = HashSet::new();

        let _ = set.insert(54);
        let _ = set.insert(32);
        let _ = set.insert(27);
        let _ = set.insert(89);

        let _ = other_set.insert(77);
        let _ = other_set.insert(456);
        let _ = other_set.insert(54);
        let _ = other_set.insert(32);

        let expected = vec![27, 89];
        let mut size = 0;
        for i in set.difference(&other_set) {
            println!("{}", i.data());
            assert!(expected.contains(i.data()));
            size += 1;
        }

        assert_eq!(size, expected.len());
    }

    #[test]
     
    fn test_multithreaded_iteration() {
        // Goal here is to test for memory safety, should be protected from segfaults
        let set: HashSet<u32> = HashSet::new();

        for i in 0..2000 {
            let _ = set.insert(i);
        }

        let set_arc = Arc::new(set);
        let set_arc_clone = set_arc.clone();
        let mut wait_vec = Vec::new();

        wait_vec.push(thread::spawn(move || {
            let mut counter = 0;
            for i in set_arc_clone.iter() {
                counter += 1;
                thread::sleep(Duration::new(0, *i.data() * 1000));
            }
            println!("iterated over: {}", counter);
        }));

        let set_arc_other = set_arc.clone();
        wait_vec.push(thread::spawn(move || {
            let mut counter = 0;
            for i in 0..2000 {
                if i % 2 == 0 {
                    match set_arc_other.remove(&i) {
                        Some(_) => counter += 1,
                        None => {}
                    }
                }
            }
            println!("removed: {}", counter);
        }));

        for handle in wait_vec {
            match handle.join() {
                Ok(_) => {},
                Err(error) => { panic!("Could not join thread!: {:?}", error)}
            }
        }

        println!("Threads joined.");
    }

    #[test]
     
    fn stress_test() {
        let set_arc = Arc::new(HashSet::new());
        let mut wait_vec = Vec::new();
        
        for _ in 0..10 {
            let set = set_arc.clone();
            wait_vec.push(thread::spawn(move || {
                for i in 0..25000 {
                    if !set.contains(&i) {
                        let _ = set.insert(i);
                    }
                }
            }));
        }

        for _ in 0..10 {
            let set = set_arc.clone();
            wait_vec.push(thread::spawn(move || {
                for i in 0..25000 {
                    if set.contains(&i) {
                        let _ = set.remove(&i);
                    }
                }
            }))
        }

        for handle in wait_vec {
            if let Err(error) = handle.join() {
                panic!("Could not join thread!: {:?}", error)
            }
        }
    }

    #[derive(Hash)]
    #[derive(Copy)]
    #[derive(Clone)]
    #[derive(Eq)]
    #[derive(PartialEq)]
    #[derive(Debug)]
    enum SetResult<T: Copy + Clone + Eq + Hash + Debug + Send> {
        Insert(Result<(), T>),
        Contains(bool),
        Remove(Option<T>)
    }

    #[test]
    fn test_linearizable() {
        let set: HashSet<usize> = HashSet::new();
        let sequential: Set<usize> = Set::new();

        let mut linearizer: LinearizabilityTester<HashSet<usize>, Set<usize>, SetResult<usize>> 
                = LinearizabilityTester::new(8, 1000000, set, sequential); 

        fn conc_insert(set: &HashSet<usize>, data: SetResult<usize>) -> Option<SetResult<usize>> {
            if let SetResult::Remove(dat) = data {
                Some(SetResult::Insert(set.insert(dat.unwrap())))
            } else {
                panic!("Invalid argument")
            }
        }

        fn conc_contains(set: &HashSet<usize>, data: SetResult<usize>) -> Option<SetResult<usize>> {
            if let SetResult::Remove(dat) = data {
                Some(SetResult::Contains(set.contains(&dat.unwrap())))
            } else {
                panic!("Invalid argument")
            }
        }

        fn conc_remove(set: &HashSet<usize>, data: SetResult<usize>) -> Option<SetResult<usize>> {
            if let SetResult::Remove(dat) = data {
                Some(SetResult::Remove(set.remove(&dat.unwrap())))
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_insert(set: &Set<usize>, data: Option<SetResult<usize>>) -> (Set<usize>, Option<SetResult<usize>>) {
            if let SetResult::Remove(dat) = data.unwrap() {
                if set.contains(&dat.unwrap()) {
                    (set.clone(), Some(SetResult::Insert(Err(dat.unwrap()))))
                } else {
                    (set.insert(dat.unwrap()), Some(SetResult::Insert(Ok(()))))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_contains(set: &Set<usize>, data: Option<SetResult<usize>>) -> (Set<usize>, Option<SetResult<usize>>) {
            if let SetResult::Remove(dat) = data.unwrap() {
                (set.clone(), Some(SetResult::Contains(set.contains(&dat.unwrap()))))
            } else {
                panic!("Invalid argument")
            }
        }

        fn seq_remove(set: &Set<usize>, data: Option<SetResult<usize>>) -> (Set<usize>, Option<SetResult<usize>>) {
            if let SetResult::Remove(dat) = data.unwrap() {
                if !set.contains(&dat.unwrap()) {
                    (set.clone(), Some(SetResult::Remove(None)))
                } else {
                    (set.remove(&dat.unwrap()), Some(SetResult::Remove(Some(dat.unwrap()))))
                }
            } else {
                panic!("Invalid argument")
            }
        }

        fn worker(id: usize, log: &mut ThreadLog<HashSet<usize>, Set<usize>, SetResult<usize>>) {
            for _ in 0..1000 {
                let rand = thread_rng().gen_range(0, 101);
                if rand < 30 {
                    let val = thread_rng().gen_range(0, 101);
                    log.log_val_result(id, conc_insert, SetResult::Remove(Some(val)), format!("insert: {}", val), seq_insert);
                } else if rand < 60 {
                    let val = thread_rng().gen_range(0, 101);
                    log.log_val_result(id, conc_contains, SetResult::Remove(Some(val)), format!("contains: {}", val), seq_contains);
                } else {
                    let val = thread_rng().gen_range(0, 101);
                    log.log_val_result(id, conc_remove, SetResult::Remove(Some(val)), format!("remove: {}", val), seq_remove);
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