use std::sync::Arc;

use cubesql::{transport::CubeReadStream, CubeError};
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;

use crate::utils::bind_method;

use std::sync::{Condvar, Mutex};

type JsWriter = Arc<Buffer>;
type BufferChunk = Result<Option<String>, CubeError>;

#[derive(Debug)]
struct Buffer {
    data: Mutex<Vec<BufferChunk>>,
    data_cv: Condvar,
    rejected: Mutex<bool>,
}

impl Buffer {
    fn new() -> Self {
        Self {
            data: Mutex::new(vec![]),
            data_cv: Condvar::new(),
            rejected: Mutex::new(false),
        }
    }

    fn push(&self, chunk: BufferChunk) -> bool {
        if *self.rejected.lock().expect("Can't lock") {
            return false;
        }

        let mut lock = self.data.lock().expect("Can't lock");
        // TODO: check size
        while lock.len() >= 1000 {
            lock = self.data_cv.wait(lock).expect("Can't wait");
        }
        lock.push(chunk);
        self.data_cv.notify_one();

        true
    }

    fn release(&self) {
        let mut lock = self.rejected.lock().expect("Can't lock");
        if *lock {
            return;
        }

        *lock = true;

        let mut lock = self.data.lock().expect("Can't lock");
        *lock = vec![Err(CubeError::user("rejected".to_string()))];
    }
}

impl CubeReadStream for Buffer {
    fn poll_next(&self) -> BufferChunk {
        let mut lock = self.data.lock().expect("Can't lock");
        while lock.is_empty() {
            lock = self.data_cv.wait(lock).expect("Can't wait");
        }
        let chunk = lock.drain(0..1).last().unwrap();
        self.data_cv.notify_one();

        chunk
    }

    fn reject(&self) {
        self.release();
    }
}

pub struct JsWriteStream {
    writer: JsWriter,
}

impl Finalize for JsWriteStream {}

impl JsWriteStream {
    #[allow(clippy::wrong_self_convention)]
    fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(self).upcast::<JsValue>();

        let chunk_fn = JsFunction::new(cx, js_stream_push_chunk)?;
        let chunk = bind_method(cx, chunk_fn, obj_this)?;
        obj.set(cx, "chunk", chunk)?;

        let end_fn = JsFunction::new(cx, js_stream_end)?;
        let end_stream = bind_method(cx, end_fn, obj_this)?;
        obj.set(cx, "end", end_stream)?;

        let reject_fn = JsFunction::new(cx, js_stream_reject)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn push_chunk(&self, chunk: String) -> bool {
        return self.writer.push(Ok(Some(chunk)));
    }

    fn end(&self) {
        self.writer.push(Ok(None));
    }

    fn reject(&self) {
        self.writer.release();
    }
}

fn js_stream_push_chunk(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.push_chunk");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;
    let result = this.push_chunk(result.value(&mut cx));

    Ok(cx.boolean(result))
}

fn js_stream_end(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.end");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    this.end();

    Ok(cx.undefined())
}

fn js_stream_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.reject");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    this.reject();

    Ok(cx.undefined())
}

pub fn call_js_with_stream_as_callback(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    query: Option<String>,
) -> Result<Arc<dyn CubeReadStream>, CubeError> {
    let channel = channel.clone();
    let buffer = Arc::new(Buffer::new());
    let writer = buffer.clone();

    channel.send(move |mut cx| {
        // https://github.com/neon-bindings/neon/issues/672
        let method = match Arc::try_unwrap(js_method) {
            Ok(v) => v.into_inner(&mut cx),
            Err(v) => v.as_ref().to_inner(&mut cx),
        };

        let stream = JsWriteStream { writer };

        let this = cx.undefined();
        let args: Vec<Handle<_>> = vec![
            if let Some(q) = query {
                cx.string(q).upcast::<JsValue>()
            } else {
                cx.null().upcast::<JsValue>()
            },
            stream.to_object(&mut cx)?.upcast::<JsValue>(),
        ];
        method.call(&mut cx, this, args)?;

        Ok(())
    });

    Ok(buffer)
}
