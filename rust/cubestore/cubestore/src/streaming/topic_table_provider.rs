use crate::metastore::Column;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, StringArray, StringBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::TableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr;
use datafusion::logical_expr::{
    AggregateUDF, Expr, ScalarUDF, ScalarUDFImpl, Signature, TableSource, TypeSignature,
    Volatility, WindowUDF,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TopicTableProvider {
    topic: String,
    schema: SchemaRef,
    config_options: ConfigOptions,
}

impl TopicTableProvider {
    pub fn new(topic: String, columns: &Vec<Column>) -> Self {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<Field>>(),
        ));
        Self {
            topic,
            schema,
            config_options: ConfigOptions::default(),
        }
    }

    fn parse_timestamp_meta(&self) -> Arc<ScalarUDF> {
        struct ParseTimestampFunc {
            signature: Signature,
        }

        impl Debug for ParseTimestampFunc {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ParseTimestampFunc")
            }
        }

        impl ScalarUDFImpl for ParseTimestampFunc {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                "ParseTimestampFunc"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            fn invoke(
                &self,
                inputs: &[ColumnarValue],
            ) -> datafusion::common::Result<ColumnarValue> {
                if inputs.len() < 2 || inputs.len() > 3 {
                    return Err(DataFusionError::Execution(
                        "Expected 2 or 3 arguments in PARSE_TIMESTAMP".to_string(),
                    ));
                }

                let format = match &inputs[1] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => sql_format_to_strformat(v),
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Only scalar arguments are supported as format in PARSE_TIMESTAMP"
                                .to_string(),
                        ));
                    }
                };
                let tz: Tz = if inputs.len() == 3 {
                    match &inputs[2] {
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                            s.parse().map_err(|_| {
                                CubeError::user(format!(
                                    "Incorrect timezone {} in PARSE_TIMESTAMP",
                                    s
                                ))
                            })?
                        }
                        _ => {
                            return Err(DataFusionError::Execution(
                                "Only scalar arguments are supported as timezone in PARSE_TIMESTAMP"
                                    .to_string(),
                            ));
                        }
                    }
                } else {
                    Tz::UTC
                };

                match &inputs[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                        let ts = match tz.datetime_from_str(s, &format) {
                            Ok(ts) => ts,
                            Err(e) => {
                                return Err(DataFusionError::Execution(format!(
                                    "Error while parsing timestamp: {}",
                                    e
                                )));
                            }
                        };
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                            Some(ts.timestamp_micros()),
                            None,
                        )))
                    }
                    ColumnarValue::Array(t) if t.as_any().is::<StringArray>() => {
                        let t = t.as_any().downcast_ref::<StringArray>().unwrap();
                        Ok(ColumnarValue::Array(Arc::new(parse_timestamp_array(
                            &t, &tz, &format,
                        )?)))
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "First argument in PARSE_TIMESTAMP must be string or array of strings"
                                .to_string(),
                        ));
                    }
                }
            }
        }

        Arc::new(ScalarUDF::new_from_impl(ParseTimestampFunc {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Stable,
            ),
        }))
    }

    fn convert_tz_meta(&self) -> Arc<ScalarUDF> {
        struct ConvertTzFunc {
            signature: Signature,
        }

        impl Debug for ConvertTzFunc {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ConvertTzFunc")
            }
        }

        impl ScalarUDFImpl for ConvertTzFunc {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                "ConvertTzFunc"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            fn invoke(
                &self,
                inputs: &[ColumnarValue],
            ) -> datafusion::common::Result<ColumnarValue> {
                if inputs.len() != 3 {
                    return Err(DataFusionError::Execution(
                        "Expected 3 arguments in PARSE_TIMESTAMP".to_string(),
                    ));
                }

                let from_tz: Tz = match &inputs[1] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                        s.parse().map_err(|_| {
                            CubeError::user(format!("Incorrect timezone {} in PARSE_TIMESTAMP", s))
                        })?
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Only scalar arguments are supported as from_timezone in PARSE_TIMESTAMP"
                                .to_string(),
                        ));
                    }
                };

                let to_tz: Tz = match &inputs[2] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                        s.parse().map_err(|_| {
                            CubeError::user(format!("Incorrect timezone {} in PARSE_TIMESTAMP", s))
                        })?
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Only scalar arguments are supported as to_timezone in PARSE_TIMESTAMP"
                                .to_string(),
                        ));
                    }
                };
                match &inputs[0] {
                    ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(t), None)) => {
                        if from_tz == to_tz {
                            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                                Some(*t),
                                None,
                            )))
                        } else {
                            let time = Utc.timestamp_nanos(*t * 1000).naive_local();
                            let from = match from_tz.from_local_datetime(&time).earliest() {
                                Some(t) => t,
                                None => {
                                    return Err(DataFusionError::Execution(format!(
                                        "Can't convert timezone for timestamp {}",
                                        t
                                    )));
                                }
                            };
                            let result = from.with_timezone(&to_tz);
                            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                                Some(result.naive_local().timestamp_micros()),
                                None,
                            )))
                        }
                    }
                    ColumnarValue::Array(t) if t.as_any().is::<TimestampMicrosecondArray>() => {
                        let t = t
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        Ok(ColumnarValue::Array(Arc::new(convert_tz_array(
                            t, &from_tz, &to_tz,
                        )?)))
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "First argument in CONVERT_TZ must be timestamp or array of timestamps"
                                .to_string(),
                        ));
                    }
                }
            }
        }

        Arc::new(ScalarUDF::new_from_impl(ConvertTzFunc {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Utf8,
                    DataType::Utf8,
                ],
                Volatility::Stable,
            ),
        }))
    }

    fn format_timestamp_meta(&self) -> Arc<ScalarUDF> {
        struct FormatTimestampFunc {
            signature: Signature,
        }

        impl Debug for FormatTimestampFunc {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "FormatTimestampFunc")
            }
        }

        impl ScalarUDFImpl for FormatTimestampFunc {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                "FormatTimestampFunc"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(DataType::Utf8)
            }

            fn invoke(
                &self,
                inputs: &[ColumnarValue],
            ) -> datafusion::common::Result<ColumnarValue> {
                if inputs.len() != 2 {
                    return Err(DataFusionError::Execution(
                        "Expected 2 arguments in FORMAT_TIMESTAMP".to_string(),
                    ));
                }

                let format = match &inputs[1] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => sql_format_to_strformat(v),
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Only scalar arguments are supported as format in FORMAT_TIMESTAMP"
                                .to_string(),
                        ));
                    }
                };

                match &inputs[0] {
                    ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(t), None)) => {
                        let time = Utc.timestamp_nanos(*t * 1000).naive_local();
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(format!(
                            "{}",
                            time.format(&format)
                        )))))
                    }
                    ColumnarValue::Array(t) if t.as_any().is::<TimestampMicrosecondArray>() => {
                        let t = t
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        Ok(ColumnarValue::Array(Arc::new(format_timestamp_array(
                            &t, &format,
                        )?)))
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "First argument in FORMAT_TIMESTAMP must be timestamp or array of timestamps".to_string(),
                        ));
                    }
                }
            }
        }

        Arc::new(ScalarUDF::new_from_impl(FormatTimestampFunc {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Utf8,
                ],
                Volatility::Stable,
            ),
        }))
    }
}

impl ContextProvider for TopicTableProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        match name {
            TableReference::Bare { table } if table.as_ref() == self.topic => {
                Ok(provider_as_source(Arc::new(self.clone())))
            }
            _ => Err(DataFusionError::Plan(format!(
                "Topic table {} is not found",
                name
            ))),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        match name {
            "parse_timestamp" | "PARSE_TIMESTAMP" => Some(self.parse_timestamp_meta()),
            "convert_tz_ksql" | "CONVERT_TZ_KSQL" => Some(self.convert_tz_meta()),
            "format_timestamp" | "FORMAT_TIMESTAMP" => Some(self.format_timestamp_meta()),
            _ => None,
        }
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config_options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

#[async_trait]
impl TableProvider for TopicTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(EmptyExec::new(self.schema())))
    }
}

fn sql_format_to_strformat(sql_format: &str) -> String {
    sql_format
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
        .replace(".SSS", "%.3f")
        .replace("X", "Z") //FIXME there are no option for timezone offset with Z as zero in rust
        //so we alway expect Z at the end of the string
        .replace("'T'", "T")
}

fn parse_timestamp_array(
    input: &StringArray,
    tz: &Tz,
    format: &str,
) -> Result<TimestampMicrosecondArray, DataFusionError> {
    let mut result = TimestampMicrosecondBuilder::new();
    for i in 0..input.len() {
        if input.is_null(i) {
            result.append_null();
        } else {
            let ts = match tz.datetime_from_str(input.value(i), &format) {
                Ok(ts) => ts,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Error while parsing timestamp `{}`: {}",
                        input.value(i),
                        e
                    )));
                }
            };
            result.append_value(ts.timestamp_micros());
        }
    }
    Ok(result.finish())
}
fn convert_tz_array(
    input: &TimestampMicrosecondArray,
    from_tz: &Tz,
    to_tz: &Tz,
) -> Result<TimestampMicrosecondArray, DataFusionError> {
    let mut result = TimestampMicrosecondBuilder::new();
    if from_tz == to_tz {
        for i in 0..input.len() {
            if input.is_null(i) {
                result.append_null();
            } else {
                result.append_value(input.value(i));
            }
        }
    } else {
        for i in 0..input.len() {
            if input.is_null(i) {
                result.append_null();
            } else {
                let time = Utc
                    .timestamp_nanos(input.value(i) as i64 * 1000)
                    .naive_local();
                let from = match from_tz.from_local_datetime(&time).earliest() {
                    Some(t) => t,
                    None => {
                        return Err(DataFusionError::Execution(format!(
                            "Can't convert timezone for timestamp {}",
                            input.value(i)
                        )));
                    }
                };
                let res = from.with_timezone(to_tz);
                result.append_value(res.naive_local().timestamp_micros());
            }
        }
    }
    Ok(result.finish())
}
fn format_timestamp_array(
    input: &TimestampMicrosecondArray,
    format: &str,
) -> Result<StringArray, DataFusionError> {
    let mut result = StringBuilder::new();
    for i in 0..input.len() {
        if input.is_null(i) {
            result.append_null();
        } else {
            let time = Utc
                .timestamp_nanos(input.value(i) as i64 * 1000)
                .naive_local();
            result.append_value(format!("{}", time.format(format)));
        }
    }
    Ok(result.finish())
}
