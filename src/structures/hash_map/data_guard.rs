use memory::{HPHandle};
use std::ops::Deref;

pub struct DataGuard<'a, T: Send + 'a> {
    data: &'a T,
    handle: HPHandle<'a, T>
}

impl<'a, T: Send + 'a> DataGuard<'a, T> {
    pub fn new(data: &'a T, handle: HPHandle<'a, T>) -> DataGuard<'a, T> {
        DataGuard {
            data,
            handle
        }
    }

    pub fn data(&self) -> &T {
        self.data
    }
}

impl<'a, T: Send + Clone + 'a> DataGuard<'a, T> {
    pub fn cloned(self) -> T {
        self.data.clone()
    }
}

impl<'a, T: Send + 'a> Deref for DataGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.data
    }
}