use std::hash::{Hash, Hasher};

use super::time_stamped::{InvokeEvent, ReturnEvent};

#[derive(Eq)]
#[derive(Clone)]
pub struct Configuration<Seq: Hash + Eq + Clone, Ret: Eq + Copy> {
    sequential: Seq,
    states: StatesWrapper<Seq, Ret>
}

impl<Seq: Hash + Eq + Clone, Ret: Eq + Copy> Configuration<Seq, Ret> {
    pub fn new(sequential: Seq, num_threads: usize) -> Self {
        Self {
            sequential,
            states: StatesWrapper::new(num_threads)
        }
    }

    pub fn from_invoke(&self, invoke: &InvokeEvent<Seq, Ret>) -> Self {
        let mut new_states = self.states.clone();
        new_states.states[invoke.id] = ThreadState::Called(invoke.message.clone(), invoke.op, invoke.res);
        Self {
            sequential: self.sequential.clone(),
            states: new_states
        }
    }
}

impl<Seq: Hash + Eq + Clone, Ret: Eq + Hash + Copy> Hash for Configuration<Seq, Ret> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sequential.hash(state);
        self.states.hash(state);
    }
}

impl<Seq: Hash + Eq + Clone, Ret: Eq + Copy> PartialEq for Configuration<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        self.sequential == other.sequential
    }
}

#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Hash)]
pub struct StatesWrapper<Seq: Eq, Ret: Eq + Copy> {
    states: Vec<ThreadState<Seq, Ret>>
}

impl<Seq: Eq, Ret: Eq + Copy> StatesWrapper<Seq, Ret> {
    pub fn new(num_threads: usize) -> Self {
        let mut states = Vec::new();
        for _ in 0..num_threads {
            states.push(ThreadState::Returned);
        }
        Self {
            states
        }
    }
}

impl<Seq: Eq, Ret: Eq + Copy> Clone for StatesWrapper<Seq, Ret> {
    fn clone(&self) -> Self {
        let mut new_vec = Vec::new();
        for state in &self.states {
            new_vec.push(state.clone());
        }
        Self {
            states: new_vec
        }
    }
}

pub enum ThreadState<Seq, Ret: Copy> {
    Called(String, fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>), Option<Ret>),
    Linearized(Option<Ret>),
    Returned
}

impl<Seq: Eq, Ret: Eq + Copy> Eq for ThreadState<Seq, Ret> {}

impl<Seq: Eq, Ret: Eq + Copy> PartialEq for ThreadState<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        use self::ThreadState::*;

        match (self, other) {
            (&Called(ref msg, _, ref res), &Called(ref msg2, _, ref res2)) => {
                msg == msg2 && res == res2
            },
            (&Linearized(ref res), &Linearized(ref res2)) => res == res2,
            (&Returned, &Returned) => true,
            (_, _) => false
        }
    }
}

impl<Seq: Eq, Ret: Eq + Copy> Clone for ThreadState<Seq, Ret> {
    fn clone(&self) -> Self {
        use self::ThreadState::*;

        match self {
            &Called(ref msg, ref func, ref res) => {
                Called(msg.clone(), *func, *res)
            },
            &Linearized(res) => {
                Linearized(res)
            },
            &Returned => {
                Returned
            }
        }
    }
}

impl<Seq: Eq + Hash, Ret: Eq + Hash + Copy> Hash for ThreadState<Seq, Ret> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        use self::ThreadState::*;

        match self {
            &Called(ref msg, _, ref res) => {"Called".to_owned().hash(h); msg.hash(h); res.hash(h)},
            &Linearized(ref res) => { "Linearized".to_owned().hash(h); res.hash(h) },
            &Returned => { "Returned".to_owned().hash(h) }
        }
    }
}