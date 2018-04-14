//! A collection of lock-free or wait-free data structures.
//!
//! All the data structures in the collection use the HPBRManager for memory
//! management as a proof-of-concept. They are all implemented from the papers cited
//! in their individual struct-level pages.
//!
//! The structures in this crate can be used in a multi-threaded context by wrapping
//! them inside an Arc, as they can all be modified with an immutable reference.

pub use self::stack::Stack;
pub use self::queue::Queue; 
pub use self::seg_queue::SegQueue;
pub use self::hash_map::HashMap;
pub use self::hash_set::HashSet;

mod stack;
mod queue;
mod seg_queue;
mod hash_map;
mod hash_set;