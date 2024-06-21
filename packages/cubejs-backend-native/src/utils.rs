use cubesql::{compile::engine::df::scan::RecordBatch, sql::dataframe, CubeError};
use neon::prelude::*;
use serde_json::Value;

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

pub fn batch_to_rows(batch: RecordBatch) -> Result<(Value, Vec<Value>), CubeError> {
    let schema = batch.schema();
    let data_frame = dataframe::batch_to_dataframe(&schema, &vec![batch])?;

    let columns = serde_json::to_value(data_frame.get_columns())?;
    let rows = data_frame
        .get_rows()
        .iter()
        .map(|it| serde_json::to_value(it.values()))
        .collect::<Result<Vec<Value>, _>>()?;

    Ok((columns, rows))
}
