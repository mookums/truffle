use std::ops::Deref;

#[derive(Debug)]
pub struct Immutable<T>(T);

// Safety: Immutable types are safe to send across threads.
unsafe impl<T> Send for Immutable<T> {}
// Safety: Immutable types are safe to interact with across threads.
unsafe impl<T> Sync for Immutable<T> {}

impl<T> Immutable<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: Clone> Clone for Immutable<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Deref for Immutable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
