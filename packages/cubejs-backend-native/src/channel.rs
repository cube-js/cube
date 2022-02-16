use std::sync::{Arc, Mutex};

use cubesql::CubeError;
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;
use tokio::sync::oneshot;

use crate::utils::bind_method;

type JsAsyncChannelCallback = Box<dyn Fn(Result<String, CubeError>) + Send>;

pub struct JsAsyncChannel {
    callback: JsAsyncChannelCallback,
}

impl Finalize for JsAsyncChannel {}

fn js_async_channel_resolve(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.resolved");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsAsyncChannel>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;

    this.resolve(result.value(&mut cx));

    Ok(cx.undefined())
}

fn js_async_channel_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.reject");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsAsyncChannel>, _>(&mut cx)?;
    let error = cx.argument::<JsString>(0)?;

    this.reject(error.value(&mut cx));

    Ok(cx.undefined())
}

impl JsAsyncChannel {
    pub fn new(callback: JsAsyncChannelCallback) -> Self {
        Self { callback }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(self).upcast::<JsValue>();

        let resolve_fn = JsFunction::new(cx, js_async_channel_resolve)?;
        let resolve = bind_method(cx, resolve_fn, obj_this)?;
        obj.set(cx, "resolve", resolve)?;

        let reject_fn = JsFunction::new(cx, js_async_channel_reject)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn resolve(&self, result: String) {
        let callback = &self.callback;
        callback(Ok(result));
    }

    fn reject(&self, error: String) {
        let callback = &self.callback;
        callback(Err(CubeError::internal(error)));
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
    let channel = channel.clone();

    let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();
    let tx_mutex = Arc::new(Mutex::new(Some(tx)));

    let async_channel = JsAsyncChannel::new(Box::new(move |result| {
        let to_channel = match result {
            // @todo Optimize? Into?
            Ok(buffer_as_str) => match serde_json::from_str::<R>(&buffer_as_str) {
                Ok(json) => Ok(json),
                Err(err) => Err(CubeError::from_error(err)),
            },
            Err(err) => Err(CubeError::internal(err.to_string())),
        };

        if let Some(tx) = tx_mutex.lock().unwrap().take() {
            tx.send(to_channel).unwrap();
        } else {
            panic!("Resolve/Reject was called on AsyncChannel that was already resolved");
        }
    }));

    channel.send(move |mut cx| {
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
    });

    rx.await?
}
