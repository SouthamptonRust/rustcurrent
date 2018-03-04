use memory::HPBRManager;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::fmt::Debug;
use std::ptr;

pub struct SegQueue<T: Send + Debug> {
    data: T
}