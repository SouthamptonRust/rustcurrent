#[macro_use]
extern crate criterion;
extern crate rustcurrent;
extern crate crossbeam;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::Queue;
use std::collections::VecDeque;
use crossbeam::sync::MsQueue;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_equal_lock(num_threads: usize) {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
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

fn bench_equal(num_threads: usize) {
    let queue = Arc::new(Queue::new());
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

fn bench_equal_crossbeam(num_threads: usize) {
    let queue = Arc::new(MsQueue::new());
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                queue_clone.push(i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let queue_clone = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                loop {
                    match queue_clone.try_pop() {
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
    let queue = Arc::new(Queue::new());
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

fn bench_mp_sc_crossbeam(num_threads: usize) {
    let queue = Arc::new(MsQueue::new());
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for _ in 0..consumer_num {
            loop {
                match q.try_pop() {
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
                q.push(i);
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc_lock(num_threads: usize) {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
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
        q = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for _ in 0..amount {
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
    let queue = Arc::new(Queue::new());
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            q.enqueue(i);
        }
    }));

    for _ in 0..num_threads - 1 {
        q = queue.clone();
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

fn bench_sp_mc_crossbeam(num_threads: usize) {
    let queue = Arc::new(MsQueue::new());
    let mut wait_vec = Vec::new();

    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    let mut q = queue.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            q.push(i);
        }
    }));

    for _ in 0..num_threads - 1 {
        q = queue.clone();
        wait_vec.push(thread::spawn(move || {
            for _ in 0..amount {
                loop {
                    match q.try_pop() {
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

fn bench_queue_equal_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_queue_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_queue_mp_sc_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_queue_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_queue_sp_mc_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_queue_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("queue_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn crossbeam_bench_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("crossbeam_queue_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_crossbeam(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn crossbeam_bench_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("crossbeam_queue_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc_crossbeam(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn crossbeam_bench_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("crossbeam_queue_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc_crossbeam(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

criterion_group!(benches, crossbeam_bench_equal, crossbeam_bench_mp_sc, crossbeam_bench_sp_mc);
criterion_main!(benches);