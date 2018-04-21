extern crate rayon;
use rayon::Scope;

use std::marker::PhantomData;
use std::sync::Arc;

use super::time_stamped::{TimeStamped, Event, InvokeEvent, ReturnEvent};

pub struct LinearizabilityTester<C: Sync, S, Ret: Send>
{
    num_threads: usize,
    iterations: usize,
    concurrent: C,
    sequential: S,
    _marker: PhantomData<Ret>
}

impl<C: Sync, S, Ret: Send> LinearizabilityTester<C, S, Ret> 
{
    pub fn new(num_threads: usize, iterations: usize, concurrent: C, sequential: S) -> Self {
        Self {
            num_threads,
            iterations,
            concurrent,
            sequential,
            _marker: PhantomData
        }   
    }

    pub fn run<F>(&mut self, worker: fn(usize) -> ()) -> bool
    where F: Fn(usize, &mut ThreadLog<C, S, Ret>) -> () + Sync + Send + 'static,
    {
        let num_threads = self.num_threads;
        let arc = Arc::new(self.concurrent);
        let mut logs = Vec::new();

        rayon::scope(|s| {
            for i in 0..num_threads {
                let log = ThreadLog::new(i, arc.clone());
                s.spawn(|s1| {
                    worker(i, &mut log);
                });
                logs.push(log);
            }
        });
        true
    }
}

pub struct ThreadLog<'a, C: Sync, Seq: 'a, Ret: 'a + Send> {
    id: usize,
    concurrent: Arc<C>,
    events: Vec<TimeStamped<'a, Seq, Ret>>
} 

impl<'a, C: Sync, Seq, Ret: Send> ThreadLog<'a, C, Seq, Ret> {
    fn new(id: usize, concurrent: Arc<C>) -> Self {
        Self {
            id,
            concurrent,
            events: Vec::new()
        }
    }

    pub fn log<F, G>(&mut self, id: usize, conc_method: F, message: String, seq_method: &'a G)
    where F: Fn(&C) -> Ret,
          G: Fn(&Seq) -> (Seq, Ret)
    {
        self.events.push(TimeStamped::new_invoke(id, message, seq_method));
        let result = conc_method(&*self.concurrent);
        self.events.push(TimeStamped::new_return(id, result));
    }
}