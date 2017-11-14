use std::sync::atomic::AtomicPtr;

pub struct Stack<T: Send + Sync> {
    head: Option<AtomicPtr<Node<T>>>
}

struct Node<T> {
    data: T,
    next: Option<AtomicPtr<Node<T>>>
}

impl<T: Send + Sync> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack {
            head: None
        }
    }

    pub fn push(&mut self, val: T) {
        let node = Node {
            data: val,
            next: Some(AtomicPtr::new(self.head))
        };
    }
}