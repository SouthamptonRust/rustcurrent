extern crate rayon;

use std::marker::PhantomData;
use std::sync::{Arc};
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

use super::time_stamped::{TimeStamped, Event};
use super::automaton::{Configuration};

/// The main interaction point with the linearizability testing system. This struct
/// is in charge of running the worker function and then solving for a sequential ordering
/// afterwards.
pub struct LinearizabilityTester<C: Sync, S: Clone, Ret: Send + Eq + Hash + Copy>
{
    num_threads: usize,
    iterations: usize,
    concurrent: Arc<C>,
    sequential: S,
    _marker: PhantomData<Ret>
}

impl<C: Sync + Send, S: Clone + Hash + Eq + Debug, Ret: Send + Eq + Hash + Copy + Debug> LinearizabilityTester<C, S, Ret> 
{
    /// Create a new LinearizabilityTester with a number of threads, a number of maximum solving
    /// iterations, a concurrent data structure to test and a reference immutable sequential data structure.
    pub fn new(num_threads: usize, iterations: usize, concurrent: C, sequential: S) -> Self {
        Self {
            num_threads,
            iterations,
            concurrent: Arc::new(concurrent),
            sequential,
            _marker: PhantomData
        }   
    }

    /// Run the LinearizabilityTester with the defined worker function, collect the results and solve.
    /// Returns the result of the solver - Success, Failure, or TimedOut.
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
        
        /* for event in &sorted_log {
            match &event.event {
                &Event::Invoke(ref invoke) => println!("{:?} -- Invoke -- {}", event.stamp, invoke.id),
                &Event::Return(ref ret) => println!("{:?} -- Return -- {}", event.stamp, ret.id)
            }
        } */

        self.solve(sorted_log)
    }

    fn next_lin_attempt(&self, config: &Configuration<S, Ret>, id: usize, start: usize, event_id: usize) -> Option<Node<S, Ret>> {
        let next_thread_id = if id == start && start != 0 {
            0
        } else if start + 1 != id {
            start + 1
        } else {
            start + 2
        };
        if next_thread_id < self.num_threads {
            Some(Node::LinAttempt(config.clone(), id, next_thread_id, event_id))
        } else {
            None
        }
    }

    fn solve(&mut self, log: Vec<TimeStamped<S, Ret>>) -> LinearizabilityResult {
        let initial_config: Configuration<S, Ret> = Configuration::new(self.sequential.clone(), self.num_threads);
        let mut current = Some(Node::HistoryEvent(initial_config, 0));
        let mut stack: Vec<Option<Node<S, Ret>>> = Vec::new();
        let mut seen: HashSet<Option<Node<S, Ret>>> = HashSet::new();
        let num_events = log.len();

        seen.insert(current.clone());
        let mut iterations = 0;

        while current.is_some() || !stack.is_empty() {
            iterations += 1;
            if iterations == self.iterations {
                return LinearizabilityResult::TimedOut
            } 
            println!("stack size: {}, seen size: {}", stack.len(), seen.len());
            if current.is_none() {
                current = stack.pop().unwrap();
            }

            match current.unwrap() {
                Node::HistoryEvent(config, event_id) => {
                    println!("history event: {:?}, -- {}", config, event_id);
                    if event_id == num_events {
                        return LinearizabilityResult::Success
                    }

                    match &log[event_id].event {
                        &Event::Invoke(ref invoke) => {
                            let new_config = config.from_invoke(invoke);
                            current = Some(Node::HistoryEvent(new_config, event_id + 1));
                            if !seen.insert(current.clone()) {
                                println!("Already seen");
                                current = None
                            }
                        },
                        &Event::Return(ref ret) => {
                            current = Some(Node::LinAttempt(config.clone(), ret.id, ret.id, event_id));
                        }
                    }
                },
                Node::LinAttempt(config, id, start, event_id) => {
                    println!("Trying to linearize {:?} for {:?}, start {:?}, event {:?}", config, id, start, event_id);
                    let next = self.next_lin_attempt(&config, id, start, event_id);
                    if config.has_called(start) || start == id {
                        // Attempt to linearize the op at start
                        let fire_result = if id == start { config.try_return(id) } else { config.try_linearize(start) };
                        match fire_result {
                            Ok(new_config) => {
                                if next.is_some() {
                                    stack.push(next);
                                }
                                if id == start {
                                    current = Some(Node::HistoryEvent(new_config.clone(), event_id + 1));
                                    if !seen.insert(current.clone()) {
                                        println!("Already seen");
                                        current = None;
                                    }
                                } else {
                                    current = Some(Node::LinAttempt(new_config.clone(), id, id, event_id));
                                }
                            },
                            Err(_) => {
                                current = if config.can_return(id) && id == start { None } else { next };
                            }
                        }
                    } else {
                        current = next;
                    }
                }
            }
            iterations += 1;
            if iterations == self.iterations {
                return LinearizabilityResult::TimedOut
            }
        }

        LinearizabilityResult::Failure
    }
}

#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Clone)]
#[derive(Debug)]
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

struct LogsWrapper<C: Sync, Seq, Ret: Send> {
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

/// A nanosecond resolution log of all logged events on the concurrent object for one thread.
/// The worker function should use this to call methods on the concurrent data structure.
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

    /// Log an operation on the concurrent object which does not have any arguments.
    pub fn log<F>(&mut self, id: usize, conc_method: F, message: String, seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>))
    where F: Fn(&C) -> Option<Ret>
    {
        let events_num = self.events.len();

        self.events.push(TimeStamped::new_invoke(id, message, seq_method, None));
        let result = conc_method(&*self.concurrent);
        self.events.push(TimeStamped::new_return(id, result));
        match self.events[events_num].event {
            Event::Invoke(ref mut invoke) => {
                invoke.res = result;
            },
            Event::Return(_) => panic!("Should be invoke event")
        }
    }

    /// Log an operation on the concurrent object which takes an argument but returns nothing.
    pub fn log_val<F>(&mut self, id: usize, conc_method: F, conc_val: Ret, message: String, seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>))
    where F: Fn(&C, Ret) -> ()
    {
        self.events.push(TimeStamped::new_invoke(id, message, seq_method, Some(conc_val)));
        conc_method(&*self.concurrent, conc_val);
        self.events.push(TimeStamped::new_return(id, None));
    }

    /// Log an operation on the concurrent object which both takes an argument and returns a value.
    pub fn log_val_result<F>(&mut self, id: usize, conc_method: F, conc_val: Ret, message: String, seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>))
    where F: Fn(&C, Ret) -> Option<Ret>
    {
        let events_num = self.events.len();
        self.events.push(TimeStamped::new_invoke(id, message, seq_method, Some(conc_val)));
        let result = conc_method(&*self.concurrent, conc_val);
        match result {
            None => panic!("Shouldn't be none"),
            Some(_) => {}
        }
        self.events.push(TimeStamped::new_return(id, result));
        match self.events[events_num].event {
            Event::Invoke(ref mut invoke) => {
                invoke.res = result;
            },
            Event::Return(_) => panic!("Should be invoke event")
        }
    }

    fn merge(logs: Vec<Self>) -> Vec<TimeStamped<Seq, Ret>> {
        let mut result_vec = Vec::new();
        for mut log in logs {
            result_vec.append(&mut log.events);
        }
        result_vec.sort();

        return result_vec
    }
}

#[derive(Debug)]
pub enum LinearizabilityResult {
    Success,
    Failure,
    TimedOut
}