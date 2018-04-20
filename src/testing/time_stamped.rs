use std::time::Instant;

pub struct TimeStamped<E> {
    stamp: Instant,
    event: E
}

pub enum Event<'a, Seq: 'a, Ret, SeqRet: 'a> {
    Invoke(InvokeEvent<'a, Seq, Ret, SeqRet>),
    Return(ReturnEvent<Ret>)
}

pub struct InvokeEvent<'a, Seq: 'a, Ret, SeqRet: 'a> {
    id: usize,
    message: String,
    op: &'a Fn(Seq) -> SeqRet,
    ret_event: ReturnEvent<Ret>
}

pub struct ReturnEvent<Ret> {
    id: usize,
    result: Ret
}