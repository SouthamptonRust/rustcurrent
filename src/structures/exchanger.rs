use std::fmt::Debug;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
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
                        let mut new_node_and_tag = Box::into_raw(Box::new(NodeAndTag {
                            node: Some(my_item),
                            tag: Status::Waiting
                        }));
                        match self.slot.compare_exchange_weak(
                                            node_and_tag, 
                                            new_node_and_tag, 
                                            Ordering::AcqRel, 
                                            Ordering::Acquire) {
                            Ok(_) => {
                                while time_bound > time::precise_time_ns() {
                                    node_and_tag = self.slot.load(Ordering::Acquire);
                                    their_item = (*node_and_tag).node;
                                    match (*node_and_tag).tag {
                                        Status::Busy => {
                                            new_node_and_tag = Box::into_raw(Box::new(NodeAndTag {
                                                node: None,
                                                tag: Status::Empty
                                            }));
                                            self.slot.store(new_node_and_tag, Ordering::Acquire);
                                            return Ok(their_item.unwrap());
                                        },
                                        _ => {}
                                    }

                                }
                            },
                            Err(_) => {

                            }
                        }
                    },
                    &Status::Waiting => unimplemented!(),
                    &Status::Busy => unimplemented!()
                }
            }
        }
        Ok(my_item)
    }
}