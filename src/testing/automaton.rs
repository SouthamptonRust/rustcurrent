use std::hash::{Hash, Hasher};

#[derive(Eq)]
pub struct Configuration<Seq: Hash + Eq, Ret: Eq> {
    sequential: Seq,
    states: StatesWrapper<Seq, Ret>
}

impl<Seq: Hash + Eq, Ret: Eq> Configuration<Seq, Ret> {
    pub fn new(sequential: Seq, num_threads: usize) -> Self {
        Self {
            sequential,
            states: StatesWrapper::new(num_threads)
        }
    }
}

impl<Seq: Hash + Eq, Ret: Eq + Hash> Hash for Configuration<Seq, Ret> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sequential.hash(state);
        self.states.hash(state);
    }
}

impl<Seq: Hash + Eq, Ret: Eq> PartialEq for Configuration<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        self.sequential == other.sequential
    }
}

#[derive(Eq)]
#[derive(PartialEq)]
#[derive(Hash)]
pub struct StatesWrapper<Seq: Eq, Ret: Eq> {
    states: Vec<ThreadState<Seq, Ret>>
}

impl<Seq: Eq, Ret: Eq> StatesWrapper<Seq, Ret> {
    pub fn new(num_threads: usize) -> Self {
        Self {
            states: vec![ThreadState::Returned; num_threads]
        }
    }
}

pub enum ThreadState<Seq, Ret> {
    Called(String, fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>), Ret),
    Linearized(Ret),
    Returned
}

impl<Seq: Eq, Ret: Eq> Eq for ThreadState<Seq, Ret> {}

impl<Seq: Eq, Ret: Eq> PartialEq for ThreadState<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        use self::ThreadState::*;

        match (self, other) {
            (&Called(msg, func, res), &Called(msg2, func2, res2)) => {
                msg == msg2 && res == res2
            },
            (&Linearized(res), &Linearized(res2)) => res == res2,
            (&Returned, &Returned) => true,
            (_, _) => false
        }
    }
}

impl<Seq: Eq + Hash, Ret: Eq + Hash> Hash for ThreadState<Seq, Ret> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        use self::ThreadState::*;

        match self {
            &Called(msg, _, res) => {"Called".to_owned().hash(h); msg.hash(h); res.hash(h)},
            &Linearized(res) => { "Linearized".to_owned().hash(h); res.hash(h) },
            &Returned => { "Returned".to_owned().hash(h) }
        }
    }
}