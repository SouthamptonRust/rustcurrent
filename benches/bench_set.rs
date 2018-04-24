#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::HashSet;
use std::collections;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn set_typical(num_threads: usize) {
    let set = Arc::new(HashSet::new());
    let mut wait_vec = Vec::new();

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads {
                s.insert(i);
            }

            for i in 1000..2000 / num_threads {
                s.contains(&i);
            }

            for i in 0..200 / num_threads {
                s.remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads {
                s.insert(i);
            }

            for i in 0..1000 / num_threads {
                s.contains(&i);
            }

            for i in 10..1200 / num_threads {
                s.remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn set_typical_lock(num_threads: usize) {
    let set = Arc::new(Mutex::new(collections::HashSet::new()));
    let mut wait_vec = Vec::new();

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads {
                s.lock().unwrap().insert(i);
            }

            for i in 1000..2000 / num_threads {
                s.lock().unwrap().contains(&i);
            }

            for i in 0..200 / num_threads {
                s.lock().unwrap().remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads {
                s.lock().unwrap().insert(i);
            }

            for i in 0..1000 / num_threads {
                s.lock().unwrap().contains(&i);
            }

            for i in 10..1200 / num_threads {
                s.lock().unwrap().remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn set_heavy_insert(num_threads: usize) {
    let set = Arc::new(HashSet::new());
    let mut wait_vec = Vec::new();

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                s.insert(i);
            }

            for i in 1000..2000 / num_threads {
                s.contains(&i);
            }

            for i in 0..200 / num_threads {
                s.remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 10000..20000 / num_threads {
                s.insert(i);
            }

            for i in 0..1000 / num_threads {
                s.contains(&i);
            }

            for i in 10..1200 / num_threads {
                s.remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn set_heavy_insert_lock(num_threads: usize) {
    let set = Arc::new(Mutex::new(collections::HashSet::new()));
    let mut wait_vec = Vec::new();

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                s.lock().unwrap().insert(i);
            }

            for i in 1000..2000 / num_threads {
                s.lock().unwrap().contains(&i);
            }

            for i in 0..200 / num_threads {
                s.lock().unwrap().remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let s = set.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 10000..20000 / num_threads {
                s.lock().unwrap().insert(i);
            }

            for i in 0..1000 / num_threads {
                s.lock().unwrap().contains(&i);
            }

            for i in 10..1200 / num_threads {
                s.lock().unwrap().remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical(c: &mut Criterion) {
    c.bench_function_over_inputs("set_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| set_typical(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_typical_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("set_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| set_typical_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_insert(c: &mut Criterion) {
    c.bench_function_over_inputs("set_insert", |b: &mut Bencher, num_threads: &usize| b.iter(|| set_heavy_insert(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_insert_lock(c: &mut Criterion) {
    c.bench_function_over_inputs("set_insert", |b: &mut Bencher, num_threads: &usize| b.iter(|| set_heavy_insert_lock(*num_threads)), (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

criterion_group!(benches, bench_insert_lock, bench_insert, bench_typical_lock, bench_typical);
criterion_main!(benches);