#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::Criterion;
use rustcurrent::structures::Stack;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_stack_elim_lock_low(c: &mut Criterion) {
    c.bench_function("stack_elim_low", |b| b.iter(|| {
        let stack: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let mut waitVec: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
                for n in 0..10000 {
                    s.lock().unwrap().push(n);
                }         
            }));
        }
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
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

        for handle in waitVec {
            handle.join().unwrap();
        }
    }));
}

fn bench_stack_elim_low(c: &mut Criterion) {
    c.bench_function("stack_elim_low", |b| b.iter(|| {
        let stack: Arc<Stack<u32>> = Arc::new(Stack::new(true));
        let mut waitVec: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
                for n in 0..10000 {
                    s.push(n);
                }         
            }));
        }
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
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

        for handle in waitVec {
            handle.join().unwrap();
        }
    }));
}

fn bench_stack_noelim_lock(c: &mut Criterion) {
    c.bench_function("stack_noelim_low", |b| b.iter(|| {
        let stack: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let mut waitVec: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
                for n in 0..10000 {
                    s.lock().unwrap().push(n);
                }         
            }));
        }
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
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

        for handle in waitVec {
            handle.join().unwrap();
        }
    }));
}

fn bench_stack_noelim_low(c: &mut Criterion) {
    c.bench_function("stack_noelim_low", |b| b.iter(|| {
        let stack: Arc<Stack<u32>> = Arc::new(Stack::new(false));
        let mut waitVec: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
                for n in 0..10000 {
                    s.push(n);
                }         
            }));
        }
        for _ in 0..4 {
            let s = stack.clone();
            waitVec.push(thread::spawn(move || {
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

        for handle in waitVec {
            handle.join().unwrap();
        }
    }));
}

criterion_group!(benches, bench_stack_elim_lock_low, bench_stack_elim_low);
criterion_main!(benches);