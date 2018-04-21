use std::time::Instant;
use std::cmp::Ordering;

pub struct TimeStamped<Seq, Ret> {
    pub stamp: Instant,
    pub event: Event<Seq, Ret>
}

impl<Seq, Ret> TimeStamped<Seq, Ret> {
    pub fn new_invoke(id: usize, message: String, 
                      seq_method: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>)) -> Self
    {
        Self {
            stamp: Instant::now(),
            event: Event::Invoke(InvokeEvent {
                id,
                message,
                op: seq_method,
                res: None
            })
        }
    }

    pub fn new_return(id: usize, result: Option<Ret>) -> Self {
        Self {
            stamp: Instant::now(),
            event: Event::Return(ReturnEvent {
                id,
                result
            })
        }
    }
}

impl<Seq, Ret> Ord for TimeStamped<Seq, Ret> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.stamp.cmp(&other.stamp)
    }
}

impl<Seq, Ret> PartialOrd for TimeStamped<Seq, Ret> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.stamp.partial_cmp(&other.stamp)
    }
}

impl<Seq, Ret> PartialEq for TimeStamped<Seq, Ret> {
    fn eq(&self, other: &Self) -> bool {
        self.stamp == other.stamp
    }
}

impl<Seq, Ret> Eq for TimeStamped<Seq, Ret> {}

pub enum Event<Seq, Ret> {
    Invoke(InvokeEvent<Seq, Ret>),
    Return(ReturnEvent<Ret>)
}

pub struct InvokeEvent<Seq, Ret> {
    pub id: usize,
    pub message: String,
    pub op: fn(&Seq, Option<Ret>) -> (Seq, Option<Ret>),
    pub res: Option<Ret>
}

pub struct ReturnEvent<Ret> {
    pub id: usize,
    pub result: Option<Ret>
}