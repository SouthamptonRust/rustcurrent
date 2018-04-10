//! A module for lock-free memory management.
//!
//! The struct in this crate allows for lock-free memory management, meaning that
//! it can be used in the development of lock-free data structures. It helps ensure
//! that no pieces of data are freed while other thread can still access them, and
//! prevent the [ABA problem](https://en.wikipedia.org/wiki/ABA_problem).

pub use self::hazardpointers::HPBRManager;
pub use self::hazardpointers::HPHandle;
mod hazardpointers;