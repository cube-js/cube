use std::cell::RefCell;
use std::sync::Arc;

use cubesql::CubeError;
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;
use tokio::sync::oneshot;

use crate::utils::bind_method;

type JsAsyncChannelCallback = Box<dyn FnOnce(Result<String, CubeError>) + Send>;

pub struct JsAsyncChannel {
    callback: Option<JsAsyncChannelCallback>,
}

type BoxedChannel = JsBox<RefCell<JsAsyncChannel>>;

impl Finalize for JsAsyncChannel {}

fn js_async_channel_resolve(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.resolved");

    let this = cx.this().downcast_or_throw::<BoxedChannel, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;

    if this.borrow_mut().resolve(result.value(&mut cx)) {
        Ok(cx.undefined())
    } else {
        cx.throw_error("Resolve was called on AsyncChannel that was already used")
    }
}

fn js_async_channel_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.reject");

    let this = cx.this().downcast_or_throw::<BoxedChannel, _>(&mut cx)?;
    let error = cx.argument::<JsString>(0)?;

    if this.borrow_mut().reject(error.value(&mut cx)) {
        Ok(cx.undefined())
    } else {
        cx.throw_error("Reject was called on AsyncChannel that was already used")
    }
}

impl JsAsyncChannel {
    pub fn new(callback: JsAsyncChannelCallback) -> Self {
        Self {
            callback: Some(callback),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(RefCell::new(self)).upcast::<JsValue>();

        let resolve_fn = JsFunction::new(cx, js_async_channel_resolve)?;
        let resolve = bind_method(cx, resolve_fn, obj_this)?;
        obj.set(cx, "resolve", resolve)?;

        let reject_fn = JsFunction::new(cx, js_async_channel_reject)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn resolve(&mut self, result: String) -> bool {
        if let Some(callback) = self.callback.take() {
            callback(Ok(result));

            true
        } else {
            false
        }
    }

    fn reject(&mut self, error: String) -> bool {
        if let Some(callback) = self.callback.take() {
            callback(Err(CubeError::internal(error)));

            true
        } else {
            false
        }
    }
}

pub async fn call_js_with_channel_as_callback<R>(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    query: Option<String>,
) -> Result<R, CubeError>
where
    R: 'static + serde::de::DeserializeOwned + Send + std::fmt::Debug,
{
    let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();

    let async_channel = JsAsyncChannel::new(Box::new(move |result| {
        let to_channel = match result {
            // @todo Optimize? Into?
            Ok(buffer_as_str) => match serde_json::from_str::<R>(&buffer_as_str) {
                Ok(json) => Ok(json),
                Err(err) => Err(CubeError::internal(err.to_string())),
            },
            Err(err) => Err(CubeError::internal(err.to_string())),
        };

        tx.send(to_channel).unwrap();
    }));

    channel
        .try_send(move |mut cx| {
            // https://github.com/neon-bindings/neon/issues/672
            let method = match Arc::try_unwrap(js_method) {
                Ok(v) => v.into_inner(&mut cx),
                Err(v) => v.as_ref().to_inner(&mut cx),
            };

            let this = cx.undefined();
            let args: Vec<Handle<JsValue>> = vec![
                if let Some(q) = query {
                    cx.string(q).upcast::<JsValue>()
                } else {
                    cx.null().upcast::<JsValue>()
                },
                async_channel.to_object(&mut cx)?.upcast::<JsValue>(),
            ];

            method.call(&mut cx, this, args)?;

            Ok(())
        })
        .map_err(|err| {
            CubeError::internal(format!("Unable to send js call via channel, err: {}", err))
        })?;

    rx.await?
}
