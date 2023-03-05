use async_std::task::{Context, Poll};
use core::pin::Pin;
use futures::stream::{Fuse, FusedStream};
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::time::{interval, Duration, Instant, Interval};

pin_project! {
    pub struct BufferedStream<St: StreamExt> {
        #[pin]
        stream: Fuse<St>,
        #[pin]
        delay: Interval,
        items: Vec<St::Item>,
        cap: usize,
        period: Duration,
        send_deadline: Instant,
    }
}

impl<St: StreamExt> BufferedStream<St> {
    pub fn new(stream: St, capacity: usize, period: Duration) -> Self {
        assert!(capacity > 0);

        Self {
            stream: stream.fuse(),
            items: Vec::with_capacity(capacity),
            cap: capacity,
            delay: interval(Duration::from_millis(100)),
            period,
            send_deadline: Instant::now() + period,
        }
    }
}

impl<St: StreamExt> Stream for BufferedStream<St> {
    type Item = Vec<St::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.delay.as_mut().poll_tick(cx) {
            Poll::Pending => {
                return Poll::Pending;
            }
            Poll::Ready(_) => {}
        }
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    this.delay.as_mut().reset();

                    return if this.items.is_empty() {
                        Poll::Pending
                    } else {
                        if Instant::now() > *this.send_deadline {
                            *this.send_deadline = Instant::now() + *this.period;
                            Poll::Ready(Some(std::mem::replace(
                                this.items,
                                Vec::with_capacity(*this.cap),
                            )))
                        } else {
                            Poll::Pending
                        }
                    };
                }

                Poll::Ready(Some(item)) => {
                    this.items.push(item);
                    if this.items.len() >= *this.cap {
                        *this.send_deadline = Instant::now() + *this.period;
                        this.delay.as_mut().reset();
                        return Poll::Ready(Some(std::mem::replace(
                            this.items,
                            Vec::with_capacity(*this.cap),
                        )));
                    }
                }

                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        let full_buf = std::mem::replace(this.items, Vec::new());
                        Some(full_buf)
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }
}
impl<St: FusedStream> FusedStream for BufferedStream<St> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pin_project! {
        struct TestNonPendingStream {
            sended: u64
        }
    }

    impl TestNonPendingStream {
        pub fn new() -> Self {
            Self { sended: 0 }
        }
    }

    impl Stream for TestNonPendingStream {
        type Item = u64;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.project();
            if *this.sended < 100 {
                *this.sended += 1;
                Poll::Ready(Some(*this.sended - 1))
            } else {
                Poll::Ready(None)
            }
        }
    }

    pin_project! {
        struct TestPendingStream {
            sended: u64,
            count: u64,
            #[pin]
            interval: Interval
        }
    }

    impl TestPendingStream {
        pub fn new(count: u64, pending_ms: u64) -> Self {
            Self {
                sended: 0,
                count,
                interval: interval(Duration::from_millis(pending_ms)),
            }
        }
    }

    impl Stream for TestPendingStream {
        type Item = u64;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();
            match this.interval.as_mut().poll_tick(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(_) => {}
            }
            if *this.sended < *this.count {
                *this.sended += 1;
                Poll::Ready(Some(*this.sended - 1))
            } else {
                Poll::Ready(None)
            }
        }
    }

    #[tokio::test]
    async fn buffered_out() {
        let stream = TestNonPendingStream::new();
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_secs(1));
        let mut i = 0;
        while let Some(part) = buff_stream.next().await {
            let test = (20 * i..20 * (i + 1)).collect::<Vec<u64>>();
            assert_eq!(part, test);
            i += 1;
        }
    }

    #[tokio::test]
    async fn buffered_out_with_pending() {
        //with 1ms pending in source stream we reach buffer limit faster then wait period
        let stream = TestPendingStream::new(100, 1);
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_secs(1));
        let mut i = 0;
        while let Some(part) = buff_stream.next().await {
            let test = (20 * i..20 * (i + 1)).collect::<Vec<u64>>();
            assert_eq!(part, test);
            i += 1;
        }

        let stream = TestPendingStream::new(40, 102);
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_millis(200));
        let mut res = Vec::new();
        while let Some(mut part) = buff_stream.next().await {
            assert!(part.len() < 20);
            res.append(&mut part);
        }
        assert_eq!(res, (0..40).collect::<Vec<_>>());
    }
    #[tokio::test]
    async fn buffered_out_with_long_pending() {
        let stream = TestPendingStream::new(5, 500);
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_millis(300));
        let mut res = Vec::new();
        while let Some(mut part) = buff_stream.next().await {
            assert_eq!(part.len(), 1);
            res.append(&mut part);
        }
        assert_eq!(res, (0..5).collect::<Vec<_>>());
    }
}
