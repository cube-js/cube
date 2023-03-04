use std::sync::{Arc, Mutex};

use cubesql::CubeError;
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;

use crate::utils::bind_method;

use tokio::sync::mpsc::{channel as mpsc_channel, Receiver, Sender};
use tokio::sync::oneshot;

type Chunk = Result<String, CubeError>;

pub struct JsWriteStream {
    sender: Sender<Chunk>,
    ready_sender: Mutex<Option<oneshot::Sender<Result<(), CubeError>>>>,
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

        let start_fn = JsFunction::new(cx, js_stream_start)?;
        let start_stream = bind_method(cx, start_fn, obj_this)?;
        obj.set(cx, "start", start_stream)?;

        let end_fn = JsFunction::new(cx, js_stream_end)?;
        let end_stream = bind_method(cx, end_fn, obj_this)?;
        obj.set(cx, "end", end_stream)?;

        let reject_fn = JsFunction::new(cx, js_stream_reject)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn push_chunk(&self, chunk: String) -> bool {
        match self.sender.try_send(Ok(chunk)) {
            Err(_) => false,
            Ok(_) => true,
        }
    }

    fn start(&self) {
        if let Some(ready_sender) = self.ready_sender.lock().unwrap().take() {
            let _ = ready_sender.send(Ok(()));
        }
    }

    fn end(&self) {
        self.push_chunk("".to_string());
    }

    fn reject(&self, err: String) {
        if let Some(ready_sender) = self.ready_sender.lock().unwrap().take() {
            let _ = ready_sender.send(Err(CubeError::internal(err.to_string())));
        }
        let _ = self.sender.try_send(Err(CubeError::internal(err)));
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

fn js_stream_start(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.start");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    this.start();

    Ok(cx.undefined())
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
    let result = cx.argument::<JsString>(0)?;
    this.reject(result.value(&mut cx));
    Ok(cx.undefined())
}

pub async fn call_js_with_stream_as_callback(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    query: Option<String>,
) -> Result<Receiver<Chunk>, CubeError> {
    let chunk_size = std::env::var("CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(8192);
    let channel_size = 1_000_000 / chunk_size;

    let (sender, receiver) = mpsc_channel::<Chunk>(channel_size);
    let (ready_sender, ready_receiver) = oneshot::channel();

    channel
        .try_send(move |mut cx| {
            // https://github.com/neon-bindings/neon/issues/672
            let method = match Arc::try_unwrap(js_method) {
                Ok(v) => v.into_inner(&mut cx),
                Err(v) => v.as_ref().to_inner(&mut cx),
            };

            let stream = JsWriteStream {
                sender,
                ready_sender: Mutex::new(Some(ready_sender)),
            };
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
        })
        .map_err(|err| {
            CubeError::internal(format!("Unable to send js call via channel, err: {}", err))
        })?;

    ready_receiver.await??;

    Ok(receiver)
}
