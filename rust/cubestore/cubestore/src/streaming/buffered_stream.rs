use async_std::task::{Context, Poll};
use core::pin::Pin;
use deepsize::DeepSizeOf;
use futures::stream::{Fuse, FusedStream};
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::time::{interval, Duration, Instant, Interval};

pin_project! {
    pub struct BufferedStream<St: StreamExt> where St::Item: DeepSizeOf{
        #[pin]
        stream: Fuse<St>,
        #[pin]
        delay: Interval,
        items: Vec<St::Item>,
        items_size: usize,
        cap: usize,
        period: Duration,
        send_deadline: Instant,
        size_limit: usize
    }
}

impl<St: StreamExt> BufferedStream<St>
where
    St::Item: DeepSizeOf,
{
    pub fn new(stream: St, capacity: usize, period: Duration, size_limit: usize) -> Self {
        assert!(capacity > 0);

        Self {
            stream: stream.fuse(),
            items: Vec::with_capacity(capacity),
            items_size: 0,
            cap: capacity,
            delay: interval(period),
            period,
            send_deadline: Instant::now() + period,
            size_limit,
        }
    }
}

impl<St: StreamExt> Stream for BufferedStream<St>
where
    St::Item: DeepSizeOf,
{
    type Item = Vec<St::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.delay.as_mut().poll_tick(cx) {
            Poll::Pending => {}
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
                            *this.items_size = 0;
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
                    *this.items_size += item.deep_size_of();
                    this.items.push(item);

                    if this.items.len() >= *this.cap || *this.items_size >= *this.size_limit {
                        *this.send_deadline = Instant::now() + *this.period;
                        this.delay.as_mut().reset();
                        *this.items_size = 0;

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
                        *this.items_size = 0;
                        let full_buf = std::mem::replace(this.items, Vec::new());
                        Some(full_buf)
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }
}
impl<St: FusedStream> FusedStream for BufferedStream<St>
where
    St::Item: DeepSizeOf,
{
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
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_secs(1), 1024);
        let mut i = 0;
        while let Some(part) = buff_stream.next().await {
            let test = (20 * i..20 * (i + 1)).collect::<Vec<u64>>();
            assert_eq!(part, test);
            i += 1;
        }
    }

    #[tokio::test]
    async fn buffered_out_by_size() {
        let stream = TestNonPendingStream::new();
        let mut buff_stream = BufferedStream::new(stream, 100, Duration::from_secs(1), 20 * 8);
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
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_secs(1), 1024);
        let mut i = 0;
        while let Some(part) = buff_stream.next().await {
            let test = (20 * i..20 * (i + 1)).collect::<Vec<u64>>();
            assert_eq!(part, test);
            i += 1;
        }

        let stream = TestPendingStream::new(40, 102);
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_millis(200), 1024);
        let mut res = Vec::new();
        while let Some(mut part) = buff_stream.next().await {
            assert!(part.len() < 20);
            res.append(&mut part);
        }
        assert_eq!(res, (0..40).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn buffered_out_by_size_with_pending() {
        //with 1ms pending in source stream we reach buffer limit faster then wait period
        let stream = TestPendingStream::new(100, 1);
        let mut buff_stream = BufferedStream::new(stream, 100, Duration::from_secs(1), 20 * 8);
        let mut i = 0;
        while let Some(part) = buff_stream.next().await {
            let test = (20 * i..20 * (i + 1)).collect::<Vec<u64>>();
            assert_eq!(part, test);
            i += 1;
        }

        let stream = TestPendingStream::new(40, 102);
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_millis(200), 1024);
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
        let mut buff_stream = BufferedStream::new(stream, 20, Duration::from_millis(300), 1024);
        let mut res = Vec::new();
        while let Some(mut part) = buff_stream.next().await {
            assert_eq!(part.len(), 1);
            res.append(&mut part);
        }
        assert_eq!(res, (0..5).collect::<Vec<_>>());
    }
}
