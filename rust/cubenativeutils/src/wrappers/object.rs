use crate::wrappers::deserializer::NeonDeSerialize;
use crate::wrappers::serializer::NeonSerialize;
use cubesql::CubeError;
use neon::prelude::*;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct NativeObject {
    channel: Arc<Channel>,
    object: ManuallyDrop<Arc<Root<JsObject>>>,
}

pub trait NativeObjectHolder {
    fn new_from_native(native: NativeObject) -> Self;

    fn get_native_object(&self) -> &NativeObject;
}

pub struct NativeArgsHolder<'b, 'a, C: Context<'a>> {
    args: Vec<Handle<'a, JsValue>>,
    cx: &'b mut C,
}

impl<'b, 'a, C: Context<'a>> NativeArgsHolder<'b, 'a, C> {
    pub fn new(cx: &'b mut C) -> Self {
        Self {
            args: Vec::new(),
            cx,
        }
    }

    pub fn add<T: NeonSerialize>(&mut self, arg: T) -> NeonResult<()> {
        self.args.push(arg.to_neon(self.cx)?);
        Ok(())
    }

    pub fn into_inner(&mut self) -> Vec<Handle<'a, JsValue>> {
        let mut v = Vec::new();
        std::mem::swap(&mut self.args, &mut v);
        v
    }
}

type ArgsCallback = Box<
    dyn for<'b, 'a> FnOnce(&mut NativeArgsHolder<'b, 'a, TaskContext<'a>>) -> NeonResult<()> + Send,
>;

pub type ResultFromJsValue<R> =
    Box<dyn for<'a> FnOnce(&mut TaskContext<'a>, Handle<JsValue>) -> Result<R, CubeError> + Send>;

impl NativeObject {
    pub fn new(channel: Channel, object: Root<JsObject>) -> Self {
        Self {
            channel: Arc::new(channel),
            object: ManuallyDrop::new(Arc::new(object)),
        }
    }

    pub fn get_object(&self) -> ManuallyDrop<Arc<Root<JsObject>>> {
        self.object.clone()
    }

    pub async fn call<R: NeonDeSerialize<R> + Sized + Send + 'static>(
        &self,
        method: &'static str,
        args_callback: ArgsCallback,
    ) -> Result<R, CubeError> {
        let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();

        let object = self.object.clone();
        self.channel
            .try_send(move |mut cx| {
                let this = object.to_inner(&mut cx);
                let method = this.get::<JsFunction, _, _>(&mut cx, method)?;

                let mut args_holder = NativeArgsHolder::new(&mut cx);
                args_callback(&mut args_holder)?;
                let args = args_holder.into_inner();

                let result = match method.call(&mut cx, this, args) {
                    Ok(v) => v,
                    Err(err) => {
                        println!("Unable to call js function: {}", err);
                        return Ok(());
                    }
                };
                let result = match R::from_neon(&mut cx, result) {
                    Ok(v) => Ok(v),
                    Err(_) => Err(CubeError::internal(
                        "Failed to downcast write response".to_string(),
                    )),
                };

                if tx.send(result).is_err() {
                    log::debug!(
                        "AsyncChannel: Unable to send result from JS back to Rust, channel closed"
                    )
                }

                Ok(())
            })
            .map_err(|err| {
                CubeError::internal(format!("Unable to send js call via channel, err: {}", err))
            })?;

        rx.await?
    }
}

impl Drop for NativeObject {
    fn drop(&mut self) {
        //We should send drop to Js side if we are last owner of object
        let object = unsafe { ManuallyDrop::take(&mut self.object) };
        if let Some(object) = Arc::into_inner(object) {
            self.channel.send(move |mut cx| {
                object.drop(&mut cx);
                Ok(())
            });
        }
    }
}
