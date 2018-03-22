#[macro_use]
extern crate criterion;
extern crate rustcurrent;

use criterion::Criterion;
use rustcurrent::structures::Stack;

fn bench_stack_elimination_low(c: &mut Criterion) {
    c.bench_function("stack elim low", |b| b.iter(|| {
        let stack: Arc<Stack<u32>> = Stack::new();
        for _ in 0..4 {
            let s = stack.clone();
            for n in 0..10000 {
                s.push(n);
            }         
        }
        for _ in 0..4 {
            let s = stack.clone();
            for n in 0..10000 {
                loop {
                    match s.pop(n) {
                        Some(v) => {break;}
                        None => {} 
                    }
                }
            }
        }
    }));
}

criterion_group!(benches, bench_stack_low);
criterion_main!(benches);