use std::fmt::Debug;
use std::sync::atomic::{AtomicPtr};
use std::ptr;
use time;

pub struct Exchanger<T: Debug + Send + Sync> {
    slot: AtomicPtr<NodeAndTag<T>>
}

struct NodeAndTag<T: Debug + Send + Sync> {
    node: *mut T,
    tag: Status
}

enum Status {
    Empty,
    Waiting,
    Busy
}

impl<T: Debug + Send + Sync> Exchanger<T> {
    pub fn new() -> Exchanger<T> {
        let ptr = Box::into_raw(Box::new(NodeAndTag {
            node: ptr::null_mut(),
            tag: Status::Empty
        }));
        Exchanger {
            slot: AtomicPtr::new(ptr)
        }
    }

    pub fn exchange(item: &T, timeout: u64) -> Result<&T, &T> {
        let time_bound = timeout + time::precise_time_ns();
        // Spin by checking if time bound is past
        // That way we can be more efficient
        
        Ok(item)
    }
}