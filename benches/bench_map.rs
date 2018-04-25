#[macro_use]
extern crate criterion;
extern crate rustcurrent;
extern crate chashmap;

use criterion::{Bencher, Criterion};
use rustcurrent::structures::HashMap;
use chashmap::CHashMap;
use std::collections;

use std::thread;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

fn bench_equal_focus(num_threads: usize) {
    let map: Arc<HashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                map_clone.insert(i, i);
            }
        }));
    }
    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                let _ = map_clone.get_clone(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_focus_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<usize, usize>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                map_clone.lock().unwrap().insert(i, i);
            }
        }));
    }

    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                map_clone.lock().unwrap().get(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_focus_chashmap(num_threads: usize) {
     let map: Arc<CHashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                map_clone.insert(i, i);
            }
        }));
    }
    for _ in 0..num_threads / 2{
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads {
                let _ = map_clone.get(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical(num_threads: usize) {
    let map: Arc<HashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads {
                map_clone.insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get_clone(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i, &i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get_clone(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.remove(&i, &i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<usize, usize>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads{
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.lock().unwrap().get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads {
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 0..1000 / num_threads {
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.lock().unwrap().get(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_typical_chashmap(num_threads: usize) {
    let map: Arc<CHashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads {
                map_clone.insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.remove(&i);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_with_updates(num_threads: usize) {
    let map: Arc<HashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get_clone(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i, &i);
            }
            for i in 200..400 / num_threads{
                map_clone.update(&i, &i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get_clone(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.remove(&i, &i);
            }
            for i in 1200..1400 / num_threads{
                map_clone.update(&i, &i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_map_with_updates_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<usize, usize>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads{
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.lock().unwrap().get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
            for i in 200..400 / num_threads{
                map_clone.lock().unwrap().insert(i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads{
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.lock().unwrap().get(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
            for i in 1200..1400 / num_threads{
                map_clone.lock().unwrap().insert(i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_with_updates_chashmap(num_threads: usize) {
    let map: Arc<CHashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..1000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 1000..2000 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i);
            }
            for i in 200..400 / num_threads{
                map_clone.insert(i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 1000..2000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..7000 / num_threads{
                map_clone.get(&((i % 1000) + 1000));
            }
            for i in 1000..1200 / num_threads{
                map_clone.remove(&i);
            }
            for i in 1200..1400 / num_threads{
                map_clone.insert(i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_heavy_insert(num_threads: usize) {
    let map: Arc<HashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.get_clone(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i, &i);
            }
            for i in 200..400 / num_threads{
                map_clone.update(&i, &i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 10000..20000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 10000..10200 / num_threads{
                map_clone.get_clone(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.get_clone(&((i % 1000) + 1000));
            }
            for i in 10000..10200 / num_threads{
                map_clone.remove(&i, &i);
            }
            for i in 10200..10400 / num_threads{
                map_clone.update(&i, &i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_heavy_insert_lock(num_threads: usize) {
    let map: Arc<Mutex<collections::HashMap<usize, usize>>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();
    
    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads{
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.lock().unwrap().get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
            for i in 200..400 / num_threads{
                map_clone.lock().unwrap().insert(i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 10000..20000 / num_threads{
                map_clone.lock().unwrap().insert(i, i);
            }
            for i in 10000..10200 / num_threads{
                map_clone.lock().unwrap().get(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.lock().unwrap().get(&((i % 1000) + 1000));
            }
            for i in 10000..10200 / num_threads{
                map_clone.lock().unwrap().remove(&i);
            }
            for i in 10200..10400 / num_threads{
                map_clone.lock().unwrap().insert(i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_heavy_insert_chashmap(num_threads: usize) {
    let map: Arc<CHashMap<usize, usize>> = Arc::default();
    let mut wait_vec: Vec<JoinHandle<()>> = Vec::new();

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 0..10000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 0..1000 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.get(&(i % 1000));
            }
            for i in 0..200 / num_threads{
                map_clone.remove(&i);
            }
            for i in 200..400 / num_threads{
                map_clone.insert(i, i + 1);
            }
        }));
    }

    for _ in 0..num_threads / 2 {
        let map_clone = map.clone();
        wait_vec.push(thread::spawn(move || {
            for i in 10000..20000 / num_threads{
                map_clone.insert(i, i);
            }
            for i in 10000..10200 / num_threads{
                map_clone.get(&i);
            }
            for i in 0..700 / num_threads{
                map_clone.get(&((i % 1000) + 1000));
            }
            for i in 10000..10200 / num_threads{
                map_clone.remove(&i);
            }
            for i in 10200..10400 / num_threads{
                map_clone.insert(i, i + 1);
            }
        }));
    }

    for handle in wait_vec {
        handle.join().unwrap();
    }
}

fn bench_equal_focus_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_focus_lock(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_equal_focus_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_equal", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_focus(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_typical_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_typical_lock(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_typical_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_typical", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_typical(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_update_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_updates", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_map_with_updates_lock(*num_threads)), 
    (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_update_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_updates", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_with_updates(*num_threads)), 
    (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_heavy_insert_lock_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_insert", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_heavy_insert_lock(*num_threads)), 
    (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn bench_heavy_insert_all(c: &mut Criterion) {
    c.bench_function_over_inputs("map_insert", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_heavy_insert(*num_threads)), 
    (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn chashmap_bench_equal(c: &mut Criterion) {
    c.bench_function_over_inputs("map_equal_chashmap", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_equal_focus_chashmap(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn chashmap_bench_typical(c: &mut Criterion) {
    c.bench_function_over_inputs("map_typical_chashmap", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_typical_chashmap(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn chashmap_bench_update(c: &mut Criterion) {
    c.bench_function_over_inputs("map_updates_chashmap", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_with_updates_chashmap(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

fn chashmap_bench_insert(c: &mut Criterion) {
    c.bench_function_over_inputs("map_insert_chashmap", |b: &mut Bencher, num_threads: &usize| b.iter(|| bench_heavy_insert_chashmap(*num_threads)), 
                                (2..42).filter(|num| num % 2 == 0).collect::<Vec<usize>>());
}

criterion_group!(benches, bench_equal_focus_lock_all, bench_equal_focus_all, bench_typical_lock_all, bench_typical_all,
bench_update_lock_all, bench_update_all, bench_heavy_insert_lock_all, bench_heavy_insert_all);
criterion_main!(benches);