#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::HashMap;
use std::collections;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_equal_focus(num_threads: usize) {
    let map: Arc<HashMap<u32, u32>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                map_clone.insert(i, i);
            }
        }));
    }
    for _ in 0..num_threads {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                let _ = map_clone.get(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_focus_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<u32, u32>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                map_clone.lock().unwrap().insert(i, i);
            }
        }));
    }

    for _ in 0..num_threads {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 {
                map_clone.lock().unwrap().get(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical(num_threads: usize) {
    let map: Arc<HashMap<u32, u32>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 {
                map_clone.insert(i, i);
            }
            for i in 1000..2000 {
                map_clone.get(&i);
            }
            for i in 0..7000 {
                map_clone.get_clone(&(i % 1000));
            }
            for i in 0..200 {
                map_clone.remove(&i, &i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 {
                map_clone.insert(i, i);
            }
            for i in 0..1000 {
                map_clone.get(&i);
            }
            for i in 0..7000 {
                map_clone.get_clone(&((i % 1000) + 1000));
            }
            for i in 1000..1200 {
                map_clone.remove(&i, &i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<u32, u32>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 {
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 1000..2000 {
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 {
                map_clone.lock().unwrap().get(&(i % 1000));
            }
            for i in 0..200 {
                map_clone.lock().unwrap().remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 {
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 0..1000 {
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 {
                map_clone.lock().unwrap().get(&((i % 1000) + 1000));
            }
            for i in 1000..1200 {
                map_clone.lock().unwrap().remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_focus_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_focus_lock(*num_threads)), 
                                vec![1, 2, 4, 8, 16, 32]);
}

fn bench_equal_focus_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_focus(*num_threads)), 
                                vec![1, 2, 4, 8, 16, 32]);
}

fn bench_typical_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_typical_lock(*num_threads)), 
                                vec![2, 4, 8, 16, 32, 64]);
}

fn bench_typical_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_typical(*num_threads)), 
                                vec![2, 4, 8, 16, 32, 64]);
}

criterion_group!(benches, bench_typical_lock_all, bench_typical_all, bench_equal_focus_lock_all, bench_equal_focus_all);
criterion_main!(benches);