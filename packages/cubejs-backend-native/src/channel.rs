use std::sync::{Arc, Mutex};

use cubesql::CubeError;
use neon::prelude::*;
use tokio::sync::oneshot;

type JsAsyncChannelCallback = Box<dyn Fn(Result<String, CubeError>) + Send>;

pub struct JsAsyncChannel {
    callback: JsAsyncChannelCallback,
}

impl JsAsyncChannel {
    pub fn new(callback: JsAsyncChannelCallback) -> Self {
        Self { callback }
    }

    fn resolve(&self, result: String) {
        let callback = &self.callback;
        callback(Ok(result))
    }

    fn reject(&self) {
        let callback = &self.callback;
        callback(Err(CubeError::internal(
            "Async channel was rejected".to_string(),
        )))
    }
}

impl Finalize for JsAsyncChannel {}

pub fn channel_resolve(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let channel = cx.argument::<JsBox<JsAsyncChannel>>(0)?;
    let result = cx.argument::<JsString>(1)?;

    channel.resolve(result.value(&mut cx));

    Ok(cx.undefined())
}

pub fn channel_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let channel = cx.argument::<JsBox<JsAsyncChannel>>(0)?;
    channel.reject();

    Ok(cx.undefined())
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
            cx.boxed(async_channel).upcast::<JsValue>(),
        ];

        method.call(&mut cx, this, args)?;

        Ok(())
    });

    rx.await?
}
