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
            for i in 0..10000 / num_threads {
                queue_clone.lock().unwrap().push_back(i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
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
            for i in 0..10000 / num_threads {
                queue_clone.enqueue(i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
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

fn bench_mp_sc_lock(num_threads: usize) {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for _ in 0..consumer_num {
            loop {
                match q.lock().unwrap().pop_front() {
                    Some(val) => break,
                    None => {}
                }
            }
        }
    }));

    for _ in 0..num_threads - 1 {
        q = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                q.lock().unwrap().push_back(i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_mp_sc(num_threads: usize) {
    let queue = Arc::new(SegQueue::new());
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for _ in 0..consumer_num {
            loop {
                match q.dequeue() {
                    Some(val) => break,
                    None => {}
                }
            }
        }
    }));

    for _ in 0..num_threads - 1 {
        q = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                q.enqueue(i);
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc_lock(num_threads: usize) {
    let queue = Arc::new(Mutex::new(VecQueue::new()));
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            q.lock().unwrap().push_back(i);
        }
    }));

    for _ in 0..num_threads - 1 {
        wait_vec.push(thread::spawn(move || {
            for _ in amount {
                loop {
                    match q.lock().unwrap().pop_back() {
                        Some(val) => break,
                        None => {}
                    }
                }
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc(num_threads: usize) {
    let queue = Arc::new(SegQueue::new());
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            queue.enqueue(i);
        }
    }));

    for _ in 0..num_threads - 1 {
        wait_vec.push(thread::spawn(move || {
            for _ in 0..amount {
                loop {
                    match q.dequeue() {
                        Some(_) => break,
                        None => {}
                    }
                }
            }
        }));
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

fn bench_mp_sc_all_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc_lock(*num_threads)), 
                                 vec![2, 4, 8, 16, 32, 64]);
}

fn bench_mp_sc_all(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc(*num_threads)), 
                                 vec![2, 4, 8, 16, 32, 64]);
}

fn bench_sp_mc_all_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc_lock(*num_threads)), 
                                 vec![2, 4, 8, 16, 32, 64]);
}

fn bench_sp_mc_all(c: &mut Criterion) {
    c.bench_function_over_inputs("seg_queue_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc(*num_threads)), 
                                 vec![2, 4, 8, 16, 32, 64]);
}

criterion_group!(benches, bench_equal_all, bench_mp_sc_all_lock, bench_mp_sc_all,
                          bench_sp_mc_all_lock, bench_sp_mc_all);
criterion_main!(benches);