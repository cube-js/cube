use cubesql::{
    compile::engine::df::scan::{ArrayRef, RecordBatch},
    sql::{
        arrow_column::ArrowColumn,
        dataframe::{self, DataFrame, Row},
    },
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

/// `{"data": [[col0...], [col1...], ...]}` chunk of a `responseFormat: "columnar"`
/// `/v1/cubesql` response, serialized straight from the Arrow arrays of a batch.
#[derive(Serialize)]
pub struct ColumnarDataChunk<'a> {
    #[serde(serialize_with = "serialize_columns")]
    pub data: &'a [ArrayRef],
}

fn serialize_columns<S: Serializer>(
    columns: &&[ArrayRef],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(columns.iter().map(ArrowColumn))
}

/// Layout of the `data` chunks of a `/v1/cubesql` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlResponseFormat {
    Default,
    Columnar,
}

impl std::str::FromStr for SqlResponseFormat {
    type Err = CubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "default" => Ok(Self::Default),
            "columnar" => Ok(Self::Columnar),
            other => Err(CubeError::user(format!(
                "Unsupported responseFormat '{}', expected 'default' or 'columnar'",
                other
            ))),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use cubesql::compile::engine::df::scan::{BooleanBuilder, Int64Builder, StringBuilder};
    use std::sync::Arc;

    #[test]
    fn columnar_chunk_sends_one_array_per_column() {
        let mut strings = StringBuilder::new(2);
        strings.append_value("a").unwrap();
        strings.append_value("b").unwrap();
        let mut ints = Int64Builder::new(2);
        ints.append_value(1).unwrap();
        ints.append_value(2).unwrap();
        let mut bools = BooleanBuilder::new(2);
        bools.append_null().unwrap();
        bools.append_value(true).unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(strings.finish()),
            Arc::new(ints.finish()),
            Arc::new(bools.finish()),
        ];

        assert_eq!(
            serde_json::to_string(&ColumnarDataChunk { data: &columns }).unwrap(),
            r#"{"data":[["a","b"],["1","2"],[null,true]]}"#
        );
    }

    #[test]
    fn columnar_chunk_without_rows_keeps_one_array_per_column() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringBuilder::new(0).finish()),
            Arc::new(Int64Builder::new(0).finish()),
        ];

        assert_eq!(
            serde_json::to_string(&ColumnarDataChunk { data: &columns }).unwrap(),
            r#"{"data":[[],[]]}"#
        );
    }

    #[test]
    fn response_format_parses() {
        assert_eq!(
            "default".parse::<SqlResponseFormat>().unwrap(),
            SqlResponseFormat::Default
        );
        assert_eq!(
            "columnar".parse::<SqlResponseFormat>().unwrap(),
            SqlResponseFormat::Columnar
        );
        assert!("compact".parse::<SqlResponseFormat>().is_err());
    }
}
