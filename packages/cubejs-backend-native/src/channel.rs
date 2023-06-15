use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::transport::MapCubeErrExt;
use async_trait::async_trait;
use cubesql::transport::{SqlGenerator, SqlTemplates};
use cubesql::CubeError;
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;
use tokio::sync::oneshot;

use crate::utils::bind_method;

type JsAsyncStringChannelCallback = Box<dyn FnOnce(Result<String, CubeError>) + Send>;
type JsAsyncChannelCallback =
    Box<dyn FnOnce(&mut FunctionContext, Result<Handle<JsValue>, CubeError>) + Send>;

pub struct JsAsyncChannel {
    callback: Option<JsAsyncChannelCallback>,
}

type BoxedChannel = JsBox<RefCell<JsAsyncChannel>>;

impl Finalize for JsAsyncChannel {}

fn js_async_channel_resolve(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.resolved");

    let this = cx.this().downcast_or_throw::<BoxedChannel, _>(&mut cx)?;
    let result = cx.argument::<JsValue>(0)?;

    if this.borrow_mut().resolve(&mut cx, result) {
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

    let error_str = error.value(&mut cx);
    if this.borrow_mut().reject(&mut cx, error_str) {
        Ok(cx.undefined())
    } else {
        cx.throw_error("Reject was called on AsyncChannel that was already used")
    }
}

impl JsAsyncChannel {
    pub fn new(callback: JsAsyncStringChannelCallback) -> Self {
        Self::new_raw(Box::new(move |cx, res| {
            callback(
                res.and_then(|v| {
                    v.downcast::<JsString, _>(cx).map_err(|e| {
                        CubeError::internal(format!("Can't downcast callback argument: {}", e))
                    })
                })
                .map(|v| v.value(cx)),
            )
        }))
    }

    pub fn new_raw(callback: JsAsyncChannelCallback) -> Self {
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

    fn resolve(&mut self, cx: &mut FunctionContext, result: Handle<JsValue>) -> bool {
        if let Some(callback) = self.callback.take() {
            callback(cx, Ok(result));

            true
        } else {
            false
        }
    }

    fn reject(&mut self, cx: &mut FunctionContext, error: String) -> bool {
        if let Some(callback) = self.callback.take() {
            callback(cx, Err(CubeError::internal(error)));

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

#[allow(clippy::type_complexity)]
pub async fn call_raw_js_with_channel_as_callback<T, R>(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    argument: T,
    arg_to_js_value: Box<dyn for<'a> FnOnce(&mut TaskContext<'a>, T) -> Handle<'a, JsValue> + Send>,
    result_from_js_value: Box<
        dyn FnOnce(&mut FunctionContext, Handle<JsValue>) -> Result<R, CubeError> + Send,
    >,
) -> Result<R, CubeError>
where
    R: 'static + Send + std::fmt::Debug,
    T: 'static + Send,
{
    let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();

    let async_channel = JsAsyncChannel::new_raw(Box::new(move |cx, result| {
        let to_channel = result.and_then(|res| result_from_js_value(cx, res));

        tx.send(to_channel).unwrap();
    }));

    channel.send(move |mut cx| {
        // https://github.com/neon-bindings/neon/issues/672
        let method = match Arc::try_unwrap(js_method) {
            Ok(v) => v.into_inner(&mut cx),
            Err(v) => v.as_ref().to_inner(&mut cx),
        };

        let this = cx.undefined();
        let arg_js_value = arg_to_js_value(&mut cx, argument);
        let args: Vec<Handle<JsValue>> = vec![
            arg_js_value,
            async_channel.to_object(&mut cx)?.upcast::<JsValue>(),
        ];

        method.call(&mut cx, this, args)?;

        Ok(())
    });

    rx.await?
}

#[derive(Debug)]
pub struct NodeSqlGenerator {
    channel: Arc<Channel>,
    sql_generator_obj: Option<Arc<Root<JsObject>>>,
    sql_templates: Arc<SqlTemplates>,
}

impl NodeSqlGenerator {
    pub fn new(
        cx: &mut FunctionContext,
        channel: Arc<Channel>,
        sql_generator_obj: Arc<Root<JsObject>>,
    ) -> Result<Self, CubeError> {
        let sql_templates = Arc::new(get_sql_templates(cx, sql_generator_obj.clone())?);
        Ok(NodeSqlGenerator {
            channel,
            sql_generator_obj: Some(sql_generator_obj),
            sql_templates,
        })
    }
}

fn get_sql_templates(
    cx: &mut FunctionContext,
    sql_generator: Arc<Root<JsObject>>,
) -> Result<SqlTemplates, CubeError> {
    let sql_generator = sql_generator.to_inner(cx);
    let sql_templates = sql_generator
        .get::<JsFunction, _, _>(cx, "sqlTemplates")
        .map_cube_err("Can't get sqlTemplates")?;
    let templates = sql_templates
        .call(cx, sql_generator, Vec::new())
        .map_cube_err("Can't call sqlTemplates function")?;
    let functions = templates
        .downcast_or_throw::<JsObject, _>(cx)
        .map_cube_err("Can't downcast template to object")?
        .get::<JsObject, _, _>(cx, "functions")
        .map_cube_err("Can't get functions")?;

    let function_names = functions
        .get_own_property_names(cx)
        .map_cube_err("Can't get functions property names")?;
    let mut functions_map = HashMap::new();
    for i in 0..function_names.len(cx) {
        let function_name = function_names
            .get::<JsString, _, _>(cx, i)
            .map_cube_err("Can't get function names")?;
        functions_map.insert(
            function_name.value(cx),
            functions
                .get::<JsString, _, _>(cx, function_name)
                .map_cube_err("Can't get function value")?
                .value(cx),
        );
    }

    // TODO
    Ok(SqlTemplates::new(functions_map, HashMap::new())?)
}

// TODO impl drop for SqlGenerator
#[async_trait]
impl SqlGenerator for NodeSqlGenerator {
    fn get_sql_templates(&self) -> Arc<SqlTemplates> {
        self.sql_templates.clone()
    }

    async fn call_template(
        &self,
        _name: String,
        _params: HashMap<String, String>,
    ) -> Result<String, CubeError> {
        todo!()
    }
}

impl Drop for NodeSqlGenerator {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        let sql_generator_obj = self.sql_generator_obj.take().unwrap();
        let _ = channel.send(move |mut cx| {
            let _ = match Arc::try_unwrap(sql_generator_obj) {
                Ok(v) => v.into_inner(&mut cx),
                Err(_) => {
                    log::error!("Unable to drop sql generator: reference is copied somewhere else. Potential memory leak");
                    return Ok(());
                },
            };
            Ok(())
        });
    }
}
