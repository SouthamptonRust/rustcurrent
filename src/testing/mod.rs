//! Utilities for testing linearizability of lock-free data structures based on the strategy
//! defined by Lowe in [Testing for Linearizability](http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf). 
//! This should be done by using the LinearizabilityTester struct and the ThreadLog.

//! # Example
//! This is an example of how to use the LinearizabilityTester on a stack. The tester needs
//! a sequential reference data structure along with operations defined on it to match those on
//! the concurrent object.
//! ```
//! let stack: Stack<usize> = Stack::new(true);
//! let sequential: Vector<usize> = Vector::new();
//! let mut linearizer: LinearizabilityTester<Stack<usize>, Vector<usize>, usize> 
//!         = LinearizabilityTester::new(8, 1000000, stack, sequential);

//! fn sequential_pop(stack: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
//!     match stack.pop_back() {
//!         Some((arc, vec)) => {
//!             let res = *arc;
//!             (vec, Some(res))
//!         },
//!         None => (Vector::new(), None)
//!     }
//! }
//! 
//! fn sequential_push(stack: &Vector<usize>, val: Option<usize>) -> (Vector<usize>, Option<usize>) {
//!     (stack.push_back(val.unwrap()), None)
//! }
//! 
//! fn worker(id: usize, log: &mut ThreadLog<Stack<usize>, Vector<usize>, usize>) {
//!     for _ in 0..1000 {
//!         let rand = thread_rng().gen_range(0, 101);
//!         if rand < 30 {
//!             // push
//!             let val = thread_rng().gen_range(0, 122222);
//!             log.log_val(id, Stack::push, val, format!("push: {}", val), sequential_push);
//!         } else {
//!             // pop
//!             log.log(id, Stack::pop, "pop".to_owned(), sequential_pop)
//!         }
//!     }
//! }
//! 
//! let result = linearizer.run(worker);
//! 
//! println!("{:?}", result);
//! 
//! match result {
//!     LinearizabilityResult::Success => assert!(true),
//!     _ => assert!(false)
//! }
//! ```

pub use self::linearizability_tester::{LinearizabilityTester, LinearizabilityResult, ThreadLog};

pub mod linearizability_tester;
mod time_stamped;
mod automaton;