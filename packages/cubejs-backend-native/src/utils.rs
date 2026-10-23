use cubesql::{
    compile::engine::df::scan::RecordBatch,
    sql::dataframe::{self, DataFrame, Row},
    CubeError,
};
use neon::prelude::*;
use serde::{Serialize, Serializer};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

#[inline(always)]
pub fn call_method<'a, AS>(
    cx: &mut impl Context<'a>,
    this: Handle<'a, JsFunction>,
    method_name: &str,
    args: AS,
) -> JsResult<'a, JsValue>
where
    AS: AsRef<[Handle<'a, JsValue>]>,
{
    let method: Handle<JsFunction> = this.get(cx, method_name)?;
    method.call(cx, this, args)
}

#[inline(always)]
pub fn bind_method<'a>(
    cx: &mut impl Context<'a>,
    fn_value: Handle<'a, JsFunction>,
    this: Handle<'a, JsValue>,
) -> JsResult<'a, JsValue> {
    call_method(cx, fn_value, "bind", [this])
}

pub fn batch_to_dataframe(batch: RecordBatch) -> Result<DataFrame, CubeError> {
    let schema = batch.schema();
    dataframe::batches_to_dataframe(&schema, vec![batch])
}

/// `{"data": [[...], ...]}` chunk of the `/v1/cubesql` response, serialized
/// straight from the rows without building a `serde_json::Value` tree.
#[derive(Serialize)]
pub struct DataChunk<'a> {
    #[serde(serialize_with = "serialize_rows")]
    pub data: &'a [Row],
}

fn serialize_rows<S: Serializer>(rows: &&[Row], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(rows.iter().map(Row::values))
}

/// Allow skipping Debug output in release builds for specific field or type.
pub struct NonDebugInRelease<T: Debug>(T);

impl<T: Debug> Debug for NonDebugInRelease<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if cfg!(debug_assertions) {
            self.0.fmt(f)
        } else {
            f.debug_struct("skipped in release build").finish()
        }
    }
}

impl<T: Debug> From<T> for NonDebugInRelease<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: Debug> Deref for NonDebugInRelease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Debug> DerefMut for NonDebugInRelease<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Debug> Default for NonDebugInRelease<T>
where
    T: Default,
{
    fn default() -> Self {
        Self(T::default())
    }
}
