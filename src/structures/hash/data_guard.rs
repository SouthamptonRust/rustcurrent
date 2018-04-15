use memory::{HPHandle};
use std::ops::Deref;
use std::fmt::Debug;
use std::fmt;

pub struct DataGuard<'a, T: Send + 'a, N: Send + 'a> {
    data: &'a T,
    handle: HPHandle<'a, N>
}

impl<'a, T: Send + 'a, N: Send> DataGuard<'a, T, N> {
    pub fn new(data: &'a T, handle: HPHandle<'a, N>) -> DataGuard<'a, T, N> {
        DataGuard {
            data,
            handle
        }
    }

    pub fn data(&self) -> &T {
        self.data
    }
}

impl<'a, T: Send + Clone + 'a, N: Send> DataGuard<'a, T, N> {
    pub fn cloned(self) -> T {
        self.data.clone()
    }
}

impl<'a, T: Send + 'a, N: Send> Deref for DataGuard<'a, T, N> {
    type Target = T;
    fn deref(&self) -> &T {
        self.data
    }
}

impl<'a, T: Debug + Send + 'a, N: Send> Debug for DataGuard<'a, T, N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DataGuard({:?})", self.data)
    }
}

impl<'a, T: Debug + Send + PartialEq + 'a, N: Send> PartialEq for DataGuard<'a, T, N> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}