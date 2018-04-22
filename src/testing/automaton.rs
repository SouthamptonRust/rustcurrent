use std::hash::{Hash, Hasher};
use std::fmt::{Debug, Formatter};
use std::fmt;

use super::time_stamped::{InvokeEvent, ReturnEvent};

#[derive(Eq)]
#[derive(Clone)]
#[derive(Debug)]
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
        new_states.states[invoke.id] = ThreadState::Called(invoke.message.clone(), invoke.op, invoke.res, invoke.arg);
        Self {
            sequential: self.sequential.clone(),
            states: new_states
        }
    }

    pub fn has_called(&self, thread_id: usize) -> bool {
        match &self.states.states[thread_id] {
            &ThreadState::Called(_, _, _, _) => true,
            _ => false
        }
    }

    pub fn can_return(&self, thread_id: usize) -> bool {
        match &self.states.states[thread_id] {
            &ThreadState::Linearized(_) => true,
            _ => false
        }
    }

    pub fn try_return(&self, thread_id: usize) -> Result<Configuration<Seq, Ret>, Option<Ret>> {
        match &self.states.states[thread_id] {
            &ThreadState::Linearized(_) => {
                let new_states = self.states.make_return(thread_id);
                Ok(Self {
                    sequential: self.sequential.clone(),
                    states: new_states
                })
            },
            &ThreadState::Called(ref msg, op, res, arg) => {
                let (new_seq, new_res) = op(&self.sequential, arg);
                if new_res == res {
                    let new_states = self.states.make_fire_return(thread_id);
                    Ok(Self {
                        sequential: new_seq,
                        states: new_states
                    })
                } else {
                    Err(new_res)
                }
            },
            &ThreadState::Returned => panic!("Operation should not be returned!")
        }
    }

    pub fn try_linearize(&self, thread_id: usize) -> Result<Configuration<Seq, Ret>, Option<Ret>> {
        match &self.states.states[thread_id] {
            &ThreadState::Called(ref msg, op, res, arg) => {
                let (new_seq, new_res) = op(&self.sequential, arg);
                if new_res == res {
                    let new_states = self.states.make_linearize(thread_id, new_res);
                    Ok(Self {
                        sequential: new_seq,
                        states: new_states
                    })
                } else {
                    Err(new_res)
                }
            },
            _ => panic!("Operation should be in called mode!")
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
#[derive(Debug)]
pub struct StatesWrapper<Seq: Eq, Ret: Eq + Copy> {
    states: Vec<ThreadState<Seq, Ret>>
}

impl<Seq: Eq + Hash, Ret: Eq + Copy + Hash> Hash for StatesWrapper<Seq, Ret> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        let hash = 0x3c074a61;
        for state in &self.states {
            hash.hash(h);
            state.hash(h);
        }
    }
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

    pub fn can_return(&self, thread_id: usize) -> bool {
        match &self.states[thread_id] {
            &ThreadState::Linearized(_) => true,
            _ => false
        }
    }

    pub fn make_return(&self, thread_id: usize) -> Self {
        let mut new_states = self.states.clone();
        new_states[thread_id] = ThreadState::Returned;
        return Self {
            states: new_states
        }
    }

    pub fn make_fire_return(&self, thread_id: usize) -> Self {
        let mut new_states = self.states.clone();
        new_states[thread_id] = ThreadState::Returned;
        return Self {
            states: new_states
        }
    }

    pub fn make_linearize(&self, thread_id: usize, result: Option<Ret>) -> Self {
        let mut new_states = self.states.clone();
        new_states[thread_id] = ThreadState::Linearized(result);
        return Self {
            states: new_states
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
    Called(String, fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>), Option<Ret>, Option<Ret>),
    Linearized(Option<Ret>),
    Returned
}

impl<Seq, Ret: Eq + Copy + Debug> Debug for ThreadState<Seq, Ret> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use self::ThreadState::*;

        match self {
            &Called(ref msg, _, res, arg) => write!(f, "Called({:?} with {:?} to make {:?})", msg, arg, res),
            &Linearized(res) => write!(f, "Linearized to {:?}", res),
            &Returned => write!(f, "Returned")
        }
    }
}

impl<Seq: Eq, Ret: Eq + Copy> Eq for ThreadState<Seq, Ret> {}

impl<Seq: Eq, Ret: Eq + Copy> PartialEq for ThreadState<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        use self::ThreadState::*;

        match (self, other) {
            (&Called(ref msg, _, ref res, arg), &Called(ref msg2, _, ref res2, arg2)) => {
                msg == msg2 && res == res2 && arg == arg2
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
            &Called(ref msg, ref func, ref res, arg) => {
                Called(msg.clone(), *func, *res, arg)
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
            &Called(ref msg, _, ref res, arg) => {"Called".to_owned().hash(h); msg.hash(h); res.hash(h); arg.hash(h)},
            &Linearized(ref res) => { "Linearized".to_owned().hash(h); res.hash(h) },
            &Returned => { "Returned".to_owned().hash(h) }
        }
    }
}

mod tests {
    extern crate im;
    use self::im::Vector;

    use std::hash::{Hash, Hasher, BuildHasher};
    use std::collections::hash_map::RandomState;
    
    use super::{Configuration, ThreadState};
    use super::super::time_stamped::InvokeEvent;

    #[test]
    #[ignore]
    fn test_hashing() {
        let mut config: Configuration<Vector<usize>, usize> = Configuration::new(Vector::new(), 3);

        config.states.states[1] = ThreadState::Linearized(Some(41));

        let new_config = config.try_return(1);
        let new_new_config = new_config.clone();

        println!("{:?}", config);
        println!("{:?}", new_config);

        let state = RandomState::new();                
        let mut hasher = state.build_hasher();

        config.hash(&mut hasher);
        let hash1 = hasher.finish();

        hasher = state.build_hasher();
        new_config.hash(&mut hasher);
        let hash2 = hasher.finish();

        hasher = state.build_hasher();
        new_config.hash(&mut hasher);
        let hash3 = hasher.finish();

        println!("hash1 {} -- hash2 {} -- hash3 {}", hash1, hash2, hash3);
    }
}