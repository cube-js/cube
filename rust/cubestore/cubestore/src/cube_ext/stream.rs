use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Implements [RecordBatchStream] by exposing a predefined schema.
/// Useful for wrapping stream adapters.
pub struct StreamWithSchema<S> {
    stream: S,
    schema: SchemaRef,
}

impl<S> StreamWithSchema<S> {
    fn stream(self: Pin<&mut Self>) -> Pin<&mut S> {
        unsafe { self.map_unchecked_mut(|s| &mut s.stream) }
    }
}

impl<S> StreamWithSchema<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Send,
{
    pub fn wrap(schema: SchemaRef, stream: S) -> Self {
        StreamWithSchema { stream, schema }
    }
}

impl<S> Stream for StreamWithSchema<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Send,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for StreamWithSchema<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Send,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
