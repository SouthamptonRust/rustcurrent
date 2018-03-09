use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::hash::Hash;
use std::fmt::Debug;
use std::ptr;
use std::marker::PhantomData;

pub struct HashMap<K, V> {
    marker: PhantomData<(K, V)>
}

impl<K: Eq + Hash + Debug, V: Send + Debug> HashMap<K, V> {

}

struct WaitFreePtr<N> {
    ptr: AtomicUsize,
    marker: PhantomData<N>
}

impl<N> WaitFreePtr<N> {
    fn new(ptr: usize) -> Self {
        WaitFreePtr {
            ptr: AtomicUsize::new(ptr),
            marker: PhantomData
        }
    }

    fn as_ptr(&self) -> *mut N {
        self.ptr.load(Ordering::Acquire) as *mut N
    }

    fn is_marked(&self) -> bool {
        let result = self.ptr.load(Ordering::SeqCst) & 0x1;
        println!("{:?}", result);
        false
    }
}

struct DataNode<K, V> {
    hash: u64,
    value: V,
    marker: PhantomData<K>
}