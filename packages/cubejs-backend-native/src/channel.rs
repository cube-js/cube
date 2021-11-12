use core::panic;
use std::{
    future::Future,
    mem,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use crate::utils::bind_method;
use cubesql::CubeError;
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;

type CubeResult<T> = std::result::Result<T, CubeError>;
type JsAsyncChannelTransformation<R> =
    Box<dyn Fn(Result<String, CubeError>) -> CubeResult<R> + Send>;

#[derive(Debug)]
enum JsAsyncChannelState<R: 'static> {
    Pending(Option<Waker>),
    Fullfiled(CubeResult<R>),
    Consumed,
}

impl<R> JsAsyncChannelState<R> {
    fn waiting(mut self, new: Waker) -> Self {
        match &mut self {
            Self::Pending(waker) => *waker = Some(new),
            Self::Fullfiled(_) => panic!("JsAsyncChannel was fullfiled, unable to wake"),
            Self::Consumed => panic!("JsAsyncChannel was consumed, unable to wake"),
        }

        self
    }
}

pub struct JsAsyncChannel<R: 'static> {
    state: Arc<Mutex<JsAsyncChannelState<R>>>,
    transformation: JsAsyncChannelTransformation<R>,
}

#[derive(Debug)]
pub struct JsAsyncChannelFuture<R: 'static> {
    state: Arc<Mutex<JsAsyncChannelState<R>>>,
}

impl<R> Finalize for JsAsyncChannel<R> {}

fn js_async_channel_resolve<R: 'static + Send>(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.resolve");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsAsyncChannel<R>>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;

    this.resolve(result.value(&mut cx));

    Ok(cx.undefined())
}

fn js_async_channel_reject<R: 'static + Send>(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.reject");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsAsyncChannel<R>>, _>(&mut cx)?;
    let error = cx.argument::<JsString>(0)?;

    this.reject(error.value(&mut cx));

    Ok(cx.undefined())
}

impl<R: 'static + Send> JsAsyncChannel<R> {
    pub fn new(
        transformation: JsAsyncChannelTransformation<R>,
    ) -> (JsAsyncChannel<R>, JsAsyncChannelFuture<R>) {
        let state = Arc::new(Mutex::new(JsAsyncChannelState::Pending(None)));
        let channel = Self {
            state: state.clone(),
            transformation,
        };
        let future = JsAsyncChannelFuture { state };

        (channel, future)
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(self).upcast::<JsValue>();

        let resolve_fn = JsFunction::new(cx, js_async_channel_resolve::<R>)?;
        let resolve = bind_method(cx, resolve_fn, obj_this)?;
        obj.set(cx, "resolve", resolve)?;

        let reject_fn = JsFunction::new(cx, js_async_channel_reject::<R>)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn resolve(&self, result: String) {
        let transformation = &self.transformation;
        let result = transformation(Ok(result));

        let mut state = self.state.lock().expect("Get lock");
        let prev_state = mem::replace(&mut *state, JsAsyncChannelState::Consumed);

        match prev_state {
            JsAsyncChannelState::Pending(waker) => {
                *state = JsAsyncChannelState::Fullfiled(result);
                mem::drop(state);

                if let Some(w) = waker {
                    w.wake()
                };
            }
            JsAsyncChannelState::Fullfiled(_) => panic!("JsAsyncChannel was already fullfiled"),
            JsAsyncChannelState::Consumed => panic!("JsAsyncChannel was already consumed"),
        }
    }

    fn reject(&self, err: String) {
        let mut state = self.state.lock().expect("Get lock");
        let prev_state = mem::replace(&mut *state, JsAsyncChannelState::Consumed);

        match prev_state {
            JsAsyncChannelState::Pending(waker) => {
                *state = JsAsyncChannelState::Fullfiled(Err(CubeError::internal(err)));
                mem::drop(state);

                if let Some(w) = waker {
                    w.wake()
                };
            }
            JsAsyncChannelState::Fullfiled(_) => panic!("JsAsyncChannel was already fullfiled"),
            JsAsyncChannelState::Consumed => panic!("JsAsyncChannel was already consumed"),
        }
    }
}

impl<R: 'static> Future for JsAsyncChannelFuture<R> {
    type Output = CubeResult<R>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state_guard = self.state.lock().expect("Get lock");
        let prev_state = mem::replace(&mut *state_guard, JsAsyncChannelState::Consumed);

        match prev_state {
            JsAsyncChannelState::Pending(_) => {}
            JsAsyncChannelState::Fullfiled(result) => return Poll::Ready(result),
            JsAsyncChannelState::Consumed => {
                panic!("Unable to consume Future multiple times")
            }
        }

        *state_guard = prev_state.waiting(cx.waker().clone());

        Poll::Pending
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
    let (js_future, future) = JsAsyncChannel::<R>::new(Box::new(|result| {
        match result {
            // @todo Optimize? Into?
            Ok(buffer_as_str) => match serde_json::from_str::<R>(&buffer_as_str) {
                Ok(json) => Ok(json),
                Err(err) => Err(CubeError::from_error(err)),
            },
            Err(err) => Err(CubeError::internal(err.to_string())),
        }
    }));

    channel.clone().send(move |mut cx| {
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
            js_future.to_object(&mut cx)?.upcast::<JsValue>(),
        ];

        method.call(&mut cx, this, args)?;

        Ok(())
    });

    future.await
}
