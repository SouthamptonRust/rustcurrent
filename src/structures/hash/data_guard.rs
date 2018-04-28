use memory::{HPHandle};
use std::fmt::Debug;
use std::fmt;

/// A struct that ensures the reference it contains lives as long as the guard is in scope.
/// This is achieved through the use of a HPHandle. When the data guard goes out of scope, the
/// reference is no longer valid, and will be unprotected.
pub struct DataGuard<'a, T: Send + 'a, N: Send + 'a> {
    data: &'a T,
    handle: HPHandle<'a, N>
}

impl<'a, T: Send + 'a, N: Send> Drop for DataGuard<'a, T, N> {
    fn drop(&mut self) {
        //println!("Dropping data guard with {:p}", self.data);
    }
}

impl<'a, T: Send + 'a, N: Send> DataGuard<'a, T, N> {
    pub fn new(data: &'a T, handle: HPHandle<'a, N>) -> DataGuard<'a, T, N> {
        DataGuard {
            data,
            handle
        }
    }

    /// Access the data inside the guard.
    /// # Example
    /// ```
    /// let guard = map.get(&52);
    /// println!("{}", guard.data()); // Prints the value with key 52 in the map
    /// ```
    pub fn data(&self) -> &'a T {
        self.data
    }
}

impl<'a, T: Send + Clone + 'a, N: Send> DataGuard<'a, T, N> {
    /// Consume the data guard to obtain a clone of the protected data.
    /// # Example
    /// ```
    /// let guard = map.get(&52);
    /// let data = guard.clone(); // The reference is unprotected
    /// println!("{}", &data); // Prints the value
    /// ```
    pub fn cloned(self) -> T {
        self.data.clone()
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