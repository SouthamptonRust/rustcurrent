#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::Criterion;
use rustcurrent::structures::Stack;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_rustcurrent_stack(num_threads: usize, elim: bool) {
    let stack: Arc<Stack<u32>> = Arc::new(Stack::new(elim));
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    for _ in 0..num_threads {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 {
                s.push(n);
            }         
        }));
    }
    for _ in 0..num_threads {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 {
                loop {
                    match s.pop() {
                        Some(v) => {break;}
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

fn bench_locked_stack(num_threads: usize) {
    let stack: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    for _ in 0..num_threads {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 {
                s.lock().unwrap().push(n);
            }         
        }));
    }
    for _ in 0..num_threads {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 {
                loop {
                    match s.lock().unwrap().pop() {
                        Some(v) => {break;}
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

fn bench_stack_elim_lock_low(c: &mut Criterion) {
    c.bench_function("stack_elim_low", |b| b.iter(|| bench_locked_stack(4)));
}

fn bench_stack_elim_low(c: &mut Criterion) {
    c.bench_function("stack_elim_low", |b| b.iter(|| bench_rustcurrent_stack(4, true)));
}

fn bench_stack_elim_lock_high(c: &mut Criterion) {
    c.bench_function("stack_elim_high", |b| b.iter(|| bench_locked_stack(20)));
}

fn bench_stack_elim_high(c: &mut Criterion) {
    c.bench_function("stack_elim_high", |b| b.iter(|| bench_rustcurrent_stack(20, true)));
}

fn bench_stack_noelim_lock_low(c: &mut Criterion) {
    c.bench_function("stack_noelim_low", |b| b.iter(|| bench_locked_stack(4)));
}

fn bench_stack_noelim_low(c: &mut Criterion) {
    c.bench_function("stack_noelim_low", |b| b.iter(|| bench_rustcurrent_stack(4, false)));
}

fn bench_stack_noelim_lock_high(c: &mut Criterion) {
    c.bench_function("stack_noelim_high", |b| b.iter(|| bench_locked_stack(20)));
}

fn bench_stack_noelim_high(c: &mut Criterion) {
    c.bench_function("stack_noelim_high", |b| b.iter(|| bench_rustcurrent_stack(20, false)));
}


criterion_group!(benches, bench_stack_noelim_lock_high, bench_stack_noelim_high);
criterion_main!(benches);