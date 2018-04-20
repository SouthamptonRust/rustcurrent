extern crate rayon;
use rayon::ThreadPool;

use super::time_stamped::{TimeStamped, Event};

pub struct LinearizabilityTester<S, C, Ret, SeqRet> {
    num_threads: usize,
    iterations: usize,
    worker: FnMut(usize, &mut ThreadLog<S, C, Ret, SeqRet>) -> ()
}

pub struct ThreadLog<'a, S: 'a, C, Ret, SeqRet: 'a> {
    id: usize,
    iterations: usize,
    concurrent: C,
    events: Vec<TimeStamped<Event<'a, S, Ret, SeqRet>>>
}