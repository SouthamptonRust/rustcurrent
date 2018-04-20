#[macro_use]
extern crate criterion;
extern crate rustcurrent;
extern crate crossbeam;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::Stack;
use crossbeam::sync::TreiberStack;
use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_rustcurrent_stack(num_threads: usize, elim: bool) {
    let stack = Arc::new(Stack::new_with_collision_size(elim, num_threads / 2));
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads {
        let mut s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
                s.push(n);
            }         
        }));
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
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

fn bench_crossbeam_stack(num_threads: usize, elim: bool) {
    let stack = Arc::new(TreiberStack::new());
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads {
        let mut s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
                s.push(n);
            }         
        }));
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
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
    let stack: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads {
        let mut s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
                s.lock().unwrap().push(n);
            }         
        }));
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for n in 0..10000 / num_threads {
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

fn bench_mp_sc(num_threads: usize, elim: bool) {
    let stack = Arc::new(Stack::new_with_collision_size(elim, num_threads / 2));
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..consumer_num {
            loop {
                match s.pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for _ in 0..(num_threads - 1) {
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.push(i);
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_mp_sc_crossbeam(num_threads: usize, elim: bool) {
    let stack = Arc::new(TreiberStack::new());
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..consumer_num {
            loop {
                match s.pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for _ in 0..(num_threads - 1) {
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.push(i);
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_mp_sc_lock(num_threads: usize) {
    let stack = Arc::new(Mutex::new(Vec::new()));
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let consumer_num = amount * (num_threads - 1);

    let mut s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..consumer_num {
            loop {
                match s.lock().unwrap().pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for _ in 0..(num_threads - 1) {
        s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.lock().unwrap().push(i);
            }
        }))
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc(num_threads: usize, elim: bool) {
    let stack = Arc::new(Stack::new_with_collision_size(elim, num_threads / 2));
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    for _ in 0..(num_threads - 1) {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.push(i);
            }
        }));
    }
    
    let s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            loop {
                match s.pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc_crossbeam(num_threads: usize, elim: bool) {
    let stack = Arc::new(Stack::new_with_collision_size(elim, num_threads / 2));
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    for _ in 0..(num_threads - 1) {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.push(i);
            }
        }));
    }
    
    let s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            loop {
                match s.pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_sp_mc_lock(num_threads: usize) {
    let stack = Arc::new(Mutex::new(Vec::new()));
    let mut wait_vec = Vec::new();
    
    let amount = 10000 / num_threads;
    let producer_num = amount * (num_threads - 1);

    for _ in 0..(num_threads - 1) {
        let s = stack.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..amount {
                s.lock().unwrap().push(i);
            }
        }));
    }
    
    let s = stack.clone();
    wait_vec.push(thread::spawn(move || {
        for i in 0..producer_num {
            loop {
                match s.lock().unwrap().pop() {
                    Some(v) => break,
                    None => {}
                }
            }
        }
    }));

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_elim_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_equal_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_rustcurrent_stack(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_lock_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_equal_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_locked_stack(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_elim_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_mp_sc_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_lock_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_mp_sc_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_elim_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_sp_mc_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_lock_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_sp_mc_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_no_elim_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_equal_no_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_rustcurrent_stack(*num_threads, false)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_no_elim_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_mp_sc_no_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc(*num_threads, false)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_no_elim_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("stack_sp_mc_no_elim", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc(*num_threads, false)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_crossbeam_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("cross_stack_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_crossbeam_stack(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_crossbeam_mp_sc(c: &mut Criterion) {
    c.bench_function_over_inputs("cross_stack_mp_sc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_mp_sc_crossbeam(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_crossbeam_sp_mc(c: &mut Criterion) {
    c.bench_function_over_inputs("cross_stack_sp_mc", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_sp_mc_crossbeam(*num_threads, true)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

criterion_group!(benches, bench_crossbeam_mp_sc, bench_crossbeam_sp_mc);
criterion_main!(benches);