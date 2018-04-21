extern crate rayon;

use std::marker::PhantomData;
use std::sync::{Arc};
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use super::time_stamped::{TimeStamped, Event, InvokeEvent, ReturnEvent};
use super::automaton::{Configuration, ThreadState};

pub struct LinearizabilityTester<C: Sync, S: Clone, Ret: Send + Eq + Hash + Copy>
{
    num_threads: usize,
    iterations: usize,
    concurrent: Arc<C>,
    sequential: S,
    _marker: PhantomData<Ret>
}

impl<C: Sync + Send, S: Clone + Hash + Eq, Ret: Send + Eq + Hash + Copy> LinearizabilityTester<C, S, Ret> 
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

    pub fn run(&mut self, worker: fn(usize, &mut ThreadLog<C, S, Ret>) -> ()) -> LinearizabilityResult {
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
                Event::Invoke(invoke) => println!("{:?} -- Invoke -- {}", event.stamp, invoke.id),
                Event::Return(ret) => println!("{:?} -- Return -- {}", event.stamp, ret.id)
            }
        }

        LinearizabilityResult::Success
    }

    fn solve(&mut self, log: ThreadLog<C, S, Ret>) -> LinearizabilityResult {
        let initial_config: Configuration<S, Ret> = Configuration::new(self.sequential.clone(), self.num_threads);
        let mut current = Some(Node::HistoryEvent(initial_config, 0));
        let mut stack: Vec<Option<Node<S, Ret>>> = Vec::new();
        let mut seen: HashSet<Option<Node<S, Ret>>> = HashSet::new();
        let num_events = log.events.len();

        seen.insert(current.clone());
        let mut iterations = 0;

        while current.is_some() || !stack.is_empty() {
            iterations += 1;
            if iterations == self.iterations {
                return LinearizabilityResult::TimedOut
            }  

            if current.is_none() {
                current = stack.pop().unwrap();
            }

            match current.unwrap() {
                Node::HistoryEvent(config, event_id) => {
                    if event_id == num_events {
                        return LinearizabilityResult::Success
                    }

                    match &log.events[event_id].event {
                        &Event::Invoke(ref invoke) => {

                        },
                        &Event::Return(ref ret) => {

                        }
                    }
                },
                Node::LinAttempt(config, id, start, mid) => {

                }
            }

            current = None;
        }

        LinearizabilityResult::Success
    }
}

#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Clone)]
enum Node<Seq: Hash + Eq + Clone, Ret: Eq + Hash + Copy> {
    HistoryEvent(Configuration<Seq, Ret>, usize),
    LinAttempt(Configuration<Seq, Ret>, usize, usize, usize)
}

impl<Seq: Hash + Eq + Clone, Ret: Eq + Hash + Copy> Hash for Node<Seq, Ret> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use self::Node::*;
        match self {
            &HistoryEvent(ref config, ref id) => { config.hash(state); id.hash(state); },
            &LinAttempt(ref config, ref id, ref start, ref mid) => { config.hash(state); id.hash(state); mid.hash(state); start.hash(state);}
        }
    }
}

pub struct LogsWrapper<C: Sync, Seq, Ret: Send> {
    logs: UnsafeCell<Vec<ThreadLog<C, Seq, Ret>>>
}

impl<C: Sync, Seq, Ret: Send + Copy> LogsWrapper<C, Seq, Ret> {
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

impl<C: Sync, Seq, Ret: Send + Copy> ThreadLog<C, Seq, Ret> {
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
        let events_num = self.events.len();

        self.events.push(TimeStamped::new_invoke(id, message, seq_method));
        let result = conc_method(&*self.concurrent);
        self.events.push(TimeStamped::new_return(id, result));
        match self.events[events_num - 2].event {
            Event::Invoke(ref mut invoke) => {
                invoke.res = result;
            },
            Event::Return(_) => panic!("Should be invoke event")
        }
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

pub enum LinearizabilityResult {
    Success,
    Failure,
    TimedOut
}