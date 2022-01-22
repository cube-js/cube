use pin_project::{pin_project, pinned_drop};
use std::future::Future;
use std::pin::Pin;
use std::task::{self, Poll};
use tokio::task::{JoinError, JoinHandle};

#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct AbortingJoinHandle<T> {
    #[pin]
    join_handle: JoinHandle<T>,
}

impl<T> AbortingJoinHandle<T> {
    pub fn new(join_handle: JoinHandle<T>) -> Self {
        Self { join_handle }
    }

    pub fn abort(&self) {
        self.join_handle.abort();
    }
}

impl<T> Future for AbortingJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        self.project().join_handle.poll(cx)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for AbortingJoinHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        self.join_handle.abort()
    }
}
