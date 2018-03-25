#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::SegQueue;
use std::collections::VecDeque;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_queue_equal_lock(num_threads: usize) {
    let queue: Arc<Mutex<VecDeque<u32>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                queue_clone.lock().unwrap().push_back(i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                loop {
                    match queue_clone.lock().unwrap().pop_front() {
                        Some(i) => {break;},
                        None => {}
                    }
                }
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_seg_queue_equal(num_threads: usize) {
    let queue: Arc<SegQueue<u32>> = Arc::new(SegQueue::new(32));
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                queue_clone.enqueue(i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                loop {
                    match queue_clone.dequeue() {
                        Some(_) => {break;},
                        None => {}
                    }
                }
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_all_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_queue_equal_lock(*num_threads)),
                                 vec![2, 4, 8, 16, 32, 64]);
}

fn bench_equal_all(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_seg_queue_equal(*num_threads)),
                                 vec![2, 4, 8, 16, 32, 64]);
}

criterion_group!(benches, bench_equal_all_lock, bench_equal_all);
criterion_main!(benches);