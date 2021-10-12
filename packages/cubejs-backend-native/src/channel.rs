use cubesql::CubeError;
use neon::prelude::*;

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
