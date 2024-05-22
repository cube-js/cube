use cubesql::compile::engine::df::scan::{
    transform_response, FieldValue, MemberField, RecordBatch,
    RecordBatchStream, SchemaRef, ValueObject,
};


use futures::{Stream, StreamExt};

use std::cell::RefCell;
use std::future::Future;

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::vec;





use crate::channel::{call_js_fn};



use cubesql::CubeError;

use neon::prelude::*;
use tokio::sync::oneshot;

#[cfg(build = "debug")]
use log::trace;

use neon::types::JsDate;

use crate::utils::{bind_method};

use tokio::sync::mpsc::{channel as mpsc_channel, Receiver, Sender};

type Chunk = Option<Result<RecordBatch, CubeError>>;

fn handle_on_drain(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let this = cx
        .this::<JsBox<RefCell<OnDrainHandler>>>()?
        .downcast_or_throw::<JsBox<RefCell<OnDrainHandler>>, _>(&mut cx)?;
    this.borrow().on_drain();

    Ok(cx.undefined())
}

pub struct ProcessingState {
    processing: bool,
    waker: Option<Waker>,
}

impl ProcessingState {
    pub fn new() -> Self {
        Self {
            processing: true,
            waker: None,
        }
    }

    pub fn resume(&mut self) {
        self.processing = true;
        println!("ProcessingState @start");
        if let Some(waker) = self.waker.take() {
            println!("wake up called");
            waker.wake();
        }
    }

    pub fn pause(&mut self) {
        self.processing = false;
    }
}

pub struct PauseableStream<T: Send> {
    inner: Pin<Box<dyn Stream<Item = T> + Send>>,
    state: Arc<Mutex<ProcessingState>>,
}

impl<T: Send> PauseableStream<T> {
    pub fn new(
        inner: impl Stream<Item = T> + Send + 'static,
        state: Arc<Mutex<ProcessingState>>,
    ) -> Self {
        Self {
            inner: Box::pin(inner),
            state,
        }
    }
}

unsafe impl<T: Send> Send for PauseableStream<T> {}

impl<T: Send> Stream for PauseableStream<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock().unwrap();
        println!("state: {:?}", state.processing);

        if !state.processing {
            state.waker = Some(cx.waker().clone());
            // drop(state);
            return Poll::Pending;
        }

        drop(state);
        self.inner.as_mut().poll_next(cx)
    }
}

#[derive(Clone)]
pub struct OnDrainHandler {
    channel: Arc<Channel>,
    js_stream: Arc<Root<JsObject>>,
    state: Arc<Mutex<ProcessingState>>,
}

unsafe impl Sync for OnDrainHandler {}

impl Finalize for OnDrainHandler {}

impl OnDrainHandler {
    pub fn new(
        channel: Arc<Channel>,
        js_stream: Arc<Root<JsObject>>,
        state: Arc<Mutex<ProcessingState>>,
    ) -> Self {
        Self {
            channel,
            js_stream,
            state,
        }
    }

    pub async fn handle(&self, js_stream_on_fn: Arc<Root<JsFunction>>) {
        let js_stream_obj = self.js_stream.clone();
        let this = RefCell::new(self.clone());

        // let (sender, rx) = mpsc_channel::<Result<(), CubeError>>(1024);
        // let sender = Arc::new(sender);
        // let tx_clone = tx.clone();
        // let this = RefCell::new(self.clone());

        // let js_stream = match Arc::try_unwrap(js_stream_obj) {
        //     Ok(v) => v.into_inner(&mut cx),
        //     Err(v) => v.as_ref().to_inner(&mut cx),
        // };

        call_js_fn(
            self.channel.clone(),
            js_stream_on_fn,
            Box::new(|cx| {
                let on_drain_fn = JsFunction::new(cx, handle_on_drain)?;

                let this = cx.boxed(this).upcast::<JsValue>();
                let on_drain_fn = bind_method(cx, on_drain_fn, this).unwrap();

                let event_arg = cx.string("drain").upcast::<JsValue>();

                Ok(vec![event_arg, on_drain_fn.upcast::<JsValue>()])
            }),
            Box::new(|_, _| Ok(())),
            js_stream_obj,
        )
        .await
        .unwrap();
        eprintln!("BINDING DONE");

        // self.channel
        //     .try_send(move |mut cx| {
        //         let js_stream = match Arc::try_unwrap(js_stream_obj) {
        //             Ok(v) => v.into_inner(&mut cx),
        //             Err(v) => v.as_ref().to_inner(&mut cx),
        //         };

        //         let js_stream_on_fn = js_stream.get::<JsFunction, _, _>(&mut cx, "on")?;
        //         let on_drain_fn = JsFunction::new(&mut cx, handle_on_drain)?;

        //         let this = cx.boxed(this).upcast::<JsValue>();
        //         let on_drain_fn = bind_method(&mut cx, on_drain_fn, this).unwrap();

        //         let event_arg = cx.string("drain").upcast::<JsValue>();
        //         js_stream_on_fn.call(
        //             &mut cx,
        //             js_stream,
        //             vec![event_arg, on_drain_fn.upcast::<JsValue>()],
        //         )?;

        //         eprintln!("BINDING DONE");

        //         Ok(())
        //     })
        //     .unwrap();
        // eprintln!("after send");

        // let mut pauseable_stream = self.pauseable_stream.lock().unwrap();
        // while let Some(batch) = pauseable_stream.lock().unwrap().next().await {
        //     let data = match batch {
        //         Ok(batch) => batch_to_rows(batch),
        //         Err(e) => {
        //             eprintln!("Error: {:?}", e);
        //             continue;
        //         }
        //     };
        //     let data = serde_json::to_string(&data).unwrap();
        //     eprintln!("data row @@@{:?}", data);

        //     let should_pause = call_raw_js_with_channel_as_callback(
        //         self.channel.clone(),
        //         js_stream_write_fn.clone(),
        //         data,
        //         Box::new(|cx, v| Ok(cx.string(v).as_value(cx))),
        //         Box::new(move |cx, v| Ok(v.downcast::<JsBoolean, _>(cx).unwrap().value(cx))),
        //     )
        //     .await
        //     .unwrap();

        //     // if should_pause {
        //     //     pauseable_stream.pause();
        //     // }
        // }
    }

    fn on_drain(&self) {
        eprintln!("fn@[on_drain] resume...");
        self.state.lock().unwrap().resume();
    }
}

pub struct JsWriteStream {
    sender: Sender<Chunk>,
    ready_sender: Mutex<Option<oneshot::Sender<Result<(), CubeError>>>>,
    tokio_handle: tokio::runtime::Handle,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
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

    fn push_chunk(&self, chunk: RecordBatch) -> impl Future<Output = Result<(), CubeError>> {
        let sender = self.sender.clone();
        async move {
            sender
                .send(Some(Ok(chunk)))
                .await
                .map_err(|e| CubeError::user(format!("Can't send to channel: {}", e)))
        }
    }

    fn start(&self) {
        if let Some(ready_sender) = self.ready_sender.lock().unwrap().take() {
            let _ = ready_sender.send(Ok(()));
        }
    }

    fn end(&self) -> impl Future<Output = Result<(), CubeError>> {
        let sender = self.sender.clone();
        async move {
            sender
                .send(None)
                .await
                .map_err(|e| CubeError::user(format!("Can't send to channel: {}", e)))
        }
    }

    fn reject(&self, err: String) {
        if let Some(ready_sender) = self.ready_sender.lock().unwrap().take() {
            let _ = ready_sender.send(Err(CubeError::internal(err.to_string())));
        }
        let _ = self.sender.try_send(Some(Err(CubeError::internal(err))));
    }
}

fn wait_for_future_and_execute_callback(
    tokio_handle: tokio::runtime::Handle,
    channel: Channel,
    callback: Root<JsFunction>,
    future: impl Future<Output = Result<(), CubeError>> + Send + Sync + 'static,
) {
    tokio_handle.spawn(async move {
        let push_result = future.await;
        let send_result = channel.try_send(move |mut cx| {
            let undefined = cx.undefined();
            let result = match push_result {
                Ok(()) => {
                    let args = vec![cx.null().upcast::<JsValue>(), cx.null().upcast::<JsValue>()];
                    callback.into_inner(&mut cx).call(&mut cx, undefined, args)
                }
                Err(e) => {
                    let args = vec![cx.string(e.message).upcast::<JsValue>()];
                    callback.into_inner(&mut cx).call(&mut cx, undefined, args)
                }
            };
            if let Err(e) = result {
                log::error!("Error during callback execution: {}", e);
            }
            Ok(())
        });
        if let Err(e) = send_result {
            log::error!("Can't execute callback on node event loop: {}", e);
        }
    });
}

pub struct JsValueObject<'a> {
    pub cx: FunctionContext<'a>,
    pub handle: Handle<'a, JsArray>,
}

impl ValueObject for JsValueObject<'_> {
    fn len(&mut self) -> Result<usize, CubeError> {
        Ok(self.handle.len(&mut self.cx) as usize)
    }

    fn get(&mut self, index: usize, field_name: &str) -> Result<FieldValue, CubeError> {
        let value = self
            .handle
            .get::<JsObject, _, _>(&mut self.cx, index as u32)
            .map_err(|e| {
                CubeError::user(format!("Can't get object at array index {}: {}", index, e))
            })?
            .get::<JsValue, _, _>(&mut self.cx, field_name)
            .map_err(|e| {
                CubeError::user(format!("Can't get '{}' field value: {}", field_name, e))
            })?;
        if let Ok(s) = value.downcast::<JsString, _>(&mut self.cx) {
            Ok(FieldValue::String(s.value(&mut self.cx)))
        } else if let Ok(n) = value.downcast::<JsNumber, _>(&mut self.cx) {
            Ok(FieldValue::Number(n.value(&mut self.cx)))
        } else if let Ok(b) = value.downcast::<JsBoolean, _>(&mut self.cx) {
            Ok(FieldValue::Bool(b.value(&mut self.cx)))
        } else if value.downcast::<JsUndefined, _>(&mut self.cx).is_ok()
            || value.downcast::<JsNull, _>(&mut self.cx).is_ok()
        {
            Ok(FieldValue::Null)
        } else if let Ok(b) = value.downcast::<JsArray, _>(&mut self.cx) {
            Err(CubeError::user(format!(
                "Expected primitive value but found JsArray({:?})",
                b
            )))
        } else if let Ok(b) = value.downcast::<JsDate, _>(&mut self.cx) {
            // TODO: Support it?
            Err(CubeError::user(format!(
                "Expected primitive value but found JsDate({:?})",
                b
            )))
        } else {
            Err(CubeError::user(format!(
                "Expected primitive value but found: {:?}",
                value
            )))
        }
    }
}

fn js_stream_push_chunk(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.push_chunk");

    let this = cx
        .this::<JsValue>()?
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let chunk_array = cx.argument::<JsArray>(0)?;
    let callback = cx.argument::<JsFunction>(1)?.root(&mut cx);
    let mut value_object = JsValueObject {
        cx,
        handle: chunk_array,
    };
    let value =
        transform_response(&mut value_object, this.schema.clone(), &this.member_fields).unwrap();
    let future = this.push_chunk(value);
    wait_for_future_and_execute_callback(
        this.tokio_handle.clone(),
        value_object.cx.channel(),
        callback,
        future,
    );

    Ok(value_object.cx.undefined())
}

fn js_stream_start(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.start");

    let this = cx
        .this::<JsValue>()?
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    this.start();

    Ok(cx.undefined())
}

fn js_stream_end(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.end");

    let this = cx
        .this::<JsValue>()?
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let future = this.end();
    let callback = cx.argument::<JsFunction>(0)?.root(&mut cx);
    wait_for_future_and_execute_callback(this.tokio_handle.clone(), cx.channel(), callback, future);

    Ok(cx.undefined())
}

fn js_stream_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.reject");

    let this = cx
        .this::<JsValue>()?
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;
    this.reject(result.value(&mut cx));
    Ok(cx.undefined())
}

pub async fn call_js_with_stream_as_callback(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    query: Option<String>,
    schema: SchemaRef,
    member_fields: Vec<MemberField>,
) -> Result<Receiver<Chunk>, CubeError> {
    let channel_size = std::env::var("CUBEJS_DB_QUERY_STREAM_HIGH_WATER_MARK")
        .ok()
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(8192);

    let (sender, receiver) = mpsc_channel::<Chunk>(channel_size);
    let (ready_sender, ready_receiver) = oneshot::channel();

    let tokio_handle = tokio::runtime::Handle::current();

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
                tokio_handle,
                schema,
                member_fields,
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
