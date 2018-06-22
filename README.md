# rustcurrent
## Lock-Free Data Structures and Memory Management for Rust

`rustcurrent` is a lock-free data structure and memory management library developed as part of a third-year project at the University of Southampton. The library currently offers 5 data structures:

+ [Treiber Stack](http://domino.research.ibm.com/library/cyberdig.nsf/papers/58319A2ED2B1078985257003004617EF/$File/rj5118.pdf) with optional [Elimination Layer](http://ieeexplore.ieee.org/document/4343950/)
+ [Michael-Scott Queue](https://dl.acm.org/citation.cfm?id=248106) with exponential backoff
+ [Segmented k-FIFO Queue](https://link.springer.com/chapter/10.1007/978-3-642-39958-9_18)
+ [Wait-Free HashSet](https://dl.acm.org/citation.cfm?id=3079519)
+ [Wait-Free HashMap](https://dl.acm.org/citation.cfm?id=3079519)

Documentation is available at http://joshua.international/rustcurrent

### Code Structure

+ `benches` contains the code for the benchmarking tests. These use the [Criterion library](https://github.com/japaric/criterion.rs).
+ `src` contains the code for the rest of the library:
  + `memory` contains the code for the [Hazard Pointer](https://dl.acm.org/citation.cfm?id=987595) Memory Manager.
  + `structures` contains the code for the library's data structures:
    + `hash` contains the code for the hash data structures as well as the utilities they need to work.
    + `utils` currently contains a more general implementation of atomic markable pointers, used for the segment queue.
  + `testing` contains the code for the port of [Lowe's Linearizability Tester](http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf).

### Tests

Tests can be run with the `cargo test -- --test-threads=1` command. The number of concurrently run tests is thus limited, because each test spawns up to 40 threads.

### Known Issues

+ The `rand` crate is undergoing breaking changes to its API and its use in `rustcurrent` is thus unstable, requiring nightly Rust and causing tests to fail. This should be fixed as soon as `rand` stabilises the `SmallRng` feature.
+ The `LinearizabilityTester` can fail for the stack on Windows. I believe this is due to the stack still using `compare_exchange_weak` which can spuriously fail.

### Useful Things to Know

+ [`rust-san`](https://github.com/japaric/rust-san) is a great tool for debugging segmentation faults and memory leaks.
+ Valgrind and Massif can be very useful for diagnosing memory issues, as is gdb with the `rust-gdb` wrapper.