use std::time::Instant;

pub struct TimeStamped<'a, Seq: 'a, Ret: 'a> {
    stamp: Instant,
    event: Event<'a, Seq, Ret>
}

impl<'a, Seq: 'a, Ret: 'a> TimeStamped<'a, Seq, Ret> {
    pub fn new_invoke<F>(id: usize, message: String, seq_method: &'a F) -> Self
    where F: Fn(&Seq) -> (Seq, Ret) + 'a
    {
        Self {
            stamp: Instant::now(),
            event: Event::Invoke(InvokeEvent {
                id,
                message,
                op: seq_method
            })
        }
    }

    pub fn new_return(id: usize, result: Ret) -> Self {
        Self {
            stamp: Instant::now(),
            event: Event::Return(ReturnEvent {
                id,
                result
            })
        }
    }
}

pub enum Event<'a, Seq: 'a, Ret: 'a> {
    Invoke(InvokeEvent<'a, Seq, Ret>),
    Return(ReturnEvent<Ret>)
}

pub struct InvokeEvent<'a, Seq: 'a, Ret: 'a> {
    id: usize,
    message: String,
    op: fn(&Seq) -> (Seq, Ret)
}

pub struct ReturnEvent<Ret> {
    id: usize,
    result: Ret
}