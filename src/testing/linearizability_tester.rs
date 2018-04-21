extern crate rayon;

use std::marker::PhantomData;
use std::sync::{Arc};
use std::cell::UnsafeCell;

use super::time_stamped::{TimeStamped, Event, InvokeEvent, ReturnEvent};

pub struct LinearizabilityTester<C: Sync, S, Ret: Send>
{
    num_threads: usize,
    iterations: usize,
    concurrent: Arc<C>,
    sequential: S,
    _marker: PhantomData<Ret>
}

impl<C: Sync + Send, S, Ret: Send> LinearizabilityTester<C, S, Ret> 
{
    pub fn new(num_threads: usize, iterations: usize, concurrent: C, sequential: S) -> Self {
        Self {
            num_threads,
            iterations,
            concurrent: Arc::new(concurrent),
            sequential,
            _marker: PhantomData
        }   
    }

    pub fn run(&mut self, worker: fn(usize, &mut ThreadLog<C, S, Ret>) -> ()) -> bool {
        let num_threads = self.num_threads;
        let arc = self.concurrent.clone();
        let logs = Arc::new(LogsWrapper::new(num_threads, arc));

        rayon::scope(|s| {
            for i in 0..num_threads {
                let log_clone = logs.clone();
                s.spawn(move |_| {
                    println!("Spawned {}", i);
                    worker(i, log_clone.get_log(i));
                    println!("Finished {}", i);
                });
            }
        });
        
        let full_logs = match Arc::try_unwrap(logs) {
            Ok(logwrapper) => logwrapper.all_logs(),
            Err(_) => panic!("Arc should be free") 
        };

        // We have the logs, so we can merge them and start the solver
        let sorted_log = ThreadLog::merge(full_logs);
        
        for event in sorted_log {
            match event.event {
                Event::Invoke(invoke) => println!("Invoke -- {}", invoke.id),
                Event::Return(ret) => println!("Return -- {}", ret.id)
            }
        }

        true
    }
}

pub struct LogsWrapper<C: Sync, Seq, Ret: Send> {
    logs: UnsafeCell<Vec<ThreadLog<C, Seq, Ret>>>
}

impl<C: Sync, Seq, Ret: Send> LogsWrapper<C, Seq, Ret> {
    pub fn new(size: usize, conc: Arc<C>) -> Self {
        let mut vec = Vec::new();
        for i in 0..size {
            vec.push(ThreadLog::new(i, conc.clone()));
        }
        Self {
            logs: UnsafeCell::new(vec)
        }
    }

    fn get_log(&self, index: usize) -> &mut ThreadLog<C, Seq, Ret> {
        unsafe {
            &mut (*self.logs.get()).split_at_mut(index).1[0]
        }
    }

    fn all_logs(self) -> Vec<ThreadLog<C, Seq, Ret>> {
        self.logs.into_inner()
    }
}

unsafe impl<C: Sync, Seq, Ret: Send> Sync for LogsWrapper<C, Seq, Ret> {} 

pub struct ThreadLog<C: Sync, Seq, Ret: Send> {
    id: usize,
    concurrent: Arc<C>,
    events: Vec<TimeStamped<Seq, Ret>>
} 

impl<C: Sync, Seq, Ret: Send> ThreadLog<C, Seq, Ret> {
    fn new(id: usize, concurrent: Arc<C>) -> Self {
        Self {
            id,
            concurrent,
            events: Vec::new()
        }
    }

    pub fn log<F>(&mut self, id: usize, conc_method: F, message: String, seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>))
    where F: Fn(&C) -> Option<Ret>
    {
        self.events.push(TimeStamped::new_invoke(id, message, seq_method));
        let result = conc_method(&*self.concurrent);
        self.events.push(TimeStamped::new_return(id, result));
    }

    pub fn log_val<F>(&mut self, id: usize, conc_method: F, conc_val: Ret, message: String, seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>))
    where F: Fn(&C, Ret) -> ()
    {
        self.events.push(TimeStamped::new_invoke(id, message, seq_method));
        conc_method(&*self.concurrent, conc_val);
        self.events.push(TimeStamped::new_return(id, None));
    }

    pub fn merge(logs: Vec<Self>) -> Vec<TimeStamped<Seq, Ret>> {
        let mut result_vec = Vec::new();
        for mut log in logs {
            result_vec.append(&mut log.events);
        }
        result_vec.sort();

        return result_vec
    }
}