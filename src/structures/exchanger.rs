use std::fmt::Debug;
use std::sync::atomic::{AtomicPtr, Ordering};
use time;

pub struct Exchanger<'a, T: Debug + Send + Sync + 'a> {
    slot: AtomicPtr<NodeAndTag<'a, T>>
}

struct NodeAndTag<'a, T: Debug + Send + Sync + 'a> {
    node: Option<&'a T>,
    tag: Status
}

enum Status {
    Empty,
    Waiting,
    Busy
}

impl<'a, T: Debug + Send + Sync> Exchanger<'a, T> {
    pub fn new() -> Exchanger<'a, T> {
        let ptr = Box::into_raw(Box::new(NodeAndTag {
            node: None,
            tag: Status::Empty
        }));
        Exchanger {
            slot: AtomicPtr::new(ptr)
        }
    }

    pub fn exchange(&mut self, my_item: &'a T, timeout: u64) -> Result<&'a T, &'a T> {
        let time_bound = timeout + time::precise_time_ns();
        // Spin by checking if time bound is past
        // That way we can be more efficient
        while time_bound > time::precise_time_ns() {

            let mut node_and_tag = self.slot.load(Ordering::Acquire);
            unsafe {
                let status = &(*node_and_tag).tag;
                let mut their_item = (*node_and_tag).node;

                match status {
                    &Status::Empty => {
                        // Try to set the Exchanger to Waiting status
                        let mut new_node_and_tag = NodeAndTag::new_from_item(my_item, Status::Waiting);
                        match self.slot.compare_exchange_weak(
                                            node_and_tag, 
                                            new_node_and_tag, 
                                            Ordering::AcqRel, 
                                            Ordering::Acquire) {
                            Ok(_) => {
                                // If we set to waiting, we wait for someone to swap with us!
                                while time_bound > time::precise_time_ns() {
                                    node_and_tag = self.slot.load(Ordering::Acquire);
                                    their_item = (*node_and_tag).node;
                                    // Check if someone matched with us by looking for the Busy tag
                                    match (*node_and_tag).tag {
                                        Status::Busy => {
                                            new_node_and_tag = NodeAndTag::default();
                                            self.slot.store(new_node_and_tag, Ordering::Acquire);
                                            return Ok(their_item.unwrap());
                                        },
                                        _ => {} // Loop and try again
                                    }

                                }
                                // Once time runs out, we see if we can swap the exchanger back to empty to leave
                                match self.slot.compare_exchange_weak(
                                                            node_and_tag,
                                                            NodeAndTag::default(),
                                                            Ordering::AcqRel,
                                                            Ordering::Acquire) {
                                    Ok(_) => {  // Nothing has changed, we weren't matched :(
                                        return Err(my_item)
                                    },
                                    Err(_) => { // We can't move back to empty, which means we were matched with!
                                        their_item = (*self.slot.load(Ordering::Acquire)).node;
                                        self.slot.store(NodeAndTag::default(), Ordering::Acquire);
                                        return Ok(their_item.unwrap())
                                    }
                                }
                            },
                            Err(_) => {
                                // Do nothing, try looping again
                            }
                        }
                    },
                    &Status::Waiting => {
                        if self.slot.compare_exchange_weak(
                                                    node_and_tag,
                                                    NodeAndTag::new_from_item(my_item, Status::Busy),
                                                    Ordering::AcqRel,
                                                    Ordering::Acquire).is_ok() {
                            return Ok(their_item.unwrap());
                        }
                    },
                    &Status::Busy => {} // Exchanger can't be used at the moment, so spin
                }
            }
        }
        Err(my_item) // We timed out :(
    }
}

impl<'a, T: Debug + Sync + Send> NodeAndTag<'a, T> {
    fn default() -> *mut NodeAndTag<'a, T> {
        Box::into_raw(Box::new(NodeAndTag {
            node: None,
            tag: Status::Empty
        }))
    }
    fn new_from_item(item: &'a T, status: Status) -> *mut NodeAndTag<'a, T> {
        Box::into_raw(Box::new(NodeAndTag {
            node: Some(item),
            tag: status
        }))
    }
}