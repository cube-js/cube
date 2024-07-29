use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::table::StreamOffset;
use crate::metastore::{Column, ColumnType};
use crate::sql::timestamp_from_string;
use crate::streaming::kafka_post_processing::{KafkaPostProcessPlan, KafkaPostProcessPlanner};
use crate::streaming::traffic_sender::TrafficSender;
use crate::streaming::{parse_json_payload_and_key, StreamingSource};
use crate::table::TimestampValue;
use crate::table::{Row, TableValue};
use crate::util::decimal::Decimal;
use crate::CubeError;
use arrow::array::ArrayRef;
use async_std::stream;
use async_trait::async_trait;
use datafusion::cube_ext;
use datafusion::cube_ext::ordfloat::OrdF64;
use futures::Stream;
use json::object::Object;
use json::JsonValue;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use vrl::compiler::Program;
use vrl::prelude::*;
use vrl::{
    compiler::{prelude::Bytes, state::RuntimeState, Context, TargetValue, TimeZone},
    value,
    value::{Secrets, Value},
};

#[derive(Clone)]
pub struct KafkaStreamingSource {
    table_id: u64,
    unique_key_columns: Vec<Column>,
    columns: Vec<Column>,
    seq_column_index: usize,
    user: Option<String>,
    password: Option<String>,
    topic: String,
    host: String,
    offset: Option<StreamOffset>,
    partition: usize,
    kafka_client: Arc<dyn KafkaClientService>,
    use_ssl: bool,
    post_processing_plan: Option<KafkaPostProcessPlan>,
    vrl_program: Option<Arc<Program>>,
    trace_obj: Option<String>,
}

impl KafkaStreamingSource {
    pub fn try_new(
        table_id: u64,
        unique_key_columns: Vec<Column>,
        seq_column: Column,
        columns: Vec<Column>,
        user: Option<String>,
        password: Option<String>,
        topic: String,
        host: String,
        select_statement: Option<String>,
        vrl: Option<String>,
        source_columns: Option<Vec<Column>>,
        offset: Option<StreamOffset>,
        partition: usize,
        kafka_client: Arc<dyn KafkaClientService>,
        use_ssl: bool,
        trace_obj: Option<String>,
    ) -> Result<Self, CubeError> {
        let (post_processing_plan, columns, unique_key_columns, seq_column_index) =
            if let Some(select_statement) = select_statement {
                let planner = KafkaPostProcessPlanner::new(
                    topic.clone(),
                    unique_key_columns.clone(),
                    seq_column,
                    columns.clone(),
                    source_columns,
                );
                let plan = planner.build(select_statement.clone())?;
                let columns = plan.source_columns().clone();
                let seq_column_index = plan.source_seq_column_index();
                let unique_columns = plan.source_unique_columns().clone();
                (Some(plan), columns, unique_columns, seq_column_index)
            } else {
                let seq_column_index = seq_column.get_index();
                (None, columns, unique_key_columns, seq_column_index)
            };
        let vrl_program = if let Some(vrl) = &vrl {
            let fns = vrl::stdlib::all();

            let res = vrl::compiler::compile(vrl, &fns)
                .map_err(|e| CubeError::user(format!("Error in vrl expression: {:?}", e)))?;
            Some(Arc::new(res.program))
        } else {
            None
        };

        Ok(KafkaStreamingSource {
            table_id,
            unique_key_columns,
            columns,
            seq_column_index,
            user,
            password,
            topic,
            host,
            offset,
            partition,
            kafka_client,
            use_ssl,
            post_processing_plan,
            vrl_program,
            trace_obj,
        })
    }
}

#[async_trait]
pub trait KafkaClientService: DIService + Send + Sync {
    async fn create_message_stream(
        &self,
        table_id: u64,
        topic: String,
        partition: i32,
        offset: Offset,
        hosts: Vec<String>,
        user: &Option<String>,
        password: &Option<String>,
        use_ssl: bool,
        to_row: Arc<dyn Fn(KafkaMessage) -> Result<Option<Row>, CubeError> + Send + Sync>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError>;

    async fn calulate_lag(
        &self,
        _topic: String,
        _partition: i32,
        _current_seq: i64,
    ) -> Option<i64> {
        None
    }
}

pub struct KafkaClientServiceImpl {
    config_obj: Arc<dyn ConfigObj>,
    consumer: RwLock<Option<Arc<StreamConsumer>>>,
}

pub enum KafkaMessage<'a> {
    BorrowedMessage(BorrowedMessage<'a>),
    MockMessage {
        key: Option<String>,
        payload: Option<String>,
        offset: i64,
    },
}

impl<'a> KafkaMessage<'a> {
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.key(),
            KafkaMessage::MockMessage { key, .. } => key.as_ref().map(|k| k.as_bytes()),
        }
    }

    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.payload(),
            KafkaMessage::MockMessage { payload, .. } => payload.as_ref().map(|k| k.as_bytes()),
        }
    }

    pub fn offset(&self) -> i64 {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.offset(),
            KafkaMessage::MockMessage { offset, .. } => *offset,
        }
    }
}

#[async_trait]
impl KafkaClientService for KafkaClientServiceImpl {
    async fn create_message_stream(
        &self,
        table_id: u64,
        topic: String,
        partition: i32,
        offset: Offset,
        hosts: Vec<String>,
        user: &Option<String>,
        password: &Option<String>,
        use_ssl: bool,
        to_row: Arc<dyn Fn(KafkaMessage) -> Result<Option<Row>, CubeError> + Send + Sync>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", hosts.join(","));
        if use_ssl {
            config.set("security.protocol", "SASL_SSL");
        }
        if let Some(user) = user {
            config.set("sasl.mechanisms", "PLAIN");
            config.set("sasl.username", user);
        }
        if let Some(password) = password {
            config.set("sasl.password", password);
        }
        config.set("session.timeout.ms", "45000");
        config.set("max.poll.interval.ms", "45000");
        config.set("group.id", format!("{}-{}-{}", topic, partition, table_id));

        let stream_consumer: StreamConsumer = config.create().map_err(|e| {
            CubeError::user(format!(
                "Error during creating kafka stream consumer: {}",
                e
            ))
        })?;

        let topic_to_move = topic.clone();
        let stream_consumer = cube_ext::spawn_blocking(move || -> KafkaResult<StreamConsumer> {
            let mut partition_list = TopicPartitionList::new();
            partition_list.add_partition_offset(&topic_to_move, partition, offset.clone())?;
            stream_consumer.assign(&partition_list)?;
            stream_consumer.seek(
                &topic_to_move,
                partition,
                offset,
                Timeout::After(Duration::from_secs(60)),
            )?;
            Ok(stream_consumer)
        })
        .await?
        .map_err(|e| {
            CubeError::user(format!(
                "Error during kafka seek operation for topic '{}' partition {} to {:?} offset: {}",
                &topic, partition, offset, e
            ))
        })?;

        let stream_consumer = Arc::new(stream_consumer);
        let config_obj = self.config_obj.clone();
        let mut consumer = self.consumer.write().await;
        *consumer = Some(stream_consumer.clone());
        Ok(Box::pin(stream::from_fn(move || {
            let stream_consumer = stream_consumer.clone();
            let to_row = to_row.clone();
            let config_obj = config_obj.clone();
            async move {
                loop {
                    let message = stream_consumer.recv().await;
                    let row = message
                        .map_err(|e| {
                            CubeError::user(format!("Error during fetching kafka message: {}", e))
                        })
                        .and_then(|m| {
                            let res = to_row(KafkaMessage::BorrowedMessage(m));
                            if config_obj.skip_kafka_parsing_errors() {
                                if let Err(e) = res {
                                    log::error!(
                                        "Skipping parsing kafka message due to error: {}",
                                        e
                                    );
                                    return Ok(None);
                                }
                            }
                            res
                        });
                    match row {
                        Ok(None) => continue,
                        Ok(Some(row)) => break Some(Ok(row)),
                        Err(e) => break Some(Err(e)),
                    }
                }
            }
        })))
    }

    async fn calulate_lag(&self, topic: String, partition: i32, current_seq: i64) -> Option<i64> {
        let consumer = self.consumer.read().await.clone();
        match consumer {
            Some(consumer) => {
                let last_offset = cube_ext::spawn_blocking(move || {
                    match consumer.fetch_watermarks(&topic, partition, Duration::from_millis(200)) {
                        Ok((_, last_offset)) => Some(last_offset),
                        Err(e) => {
                            log::error!("KafraService: Error while fetching last_offset: {}", e);
                            None
                        }
                    }
                })
                .await;

                match last_offset {
                    Ok(last_offset) => last_offset.map(|lo| lo - current_seq),
                    Err(_) => None,
                }
            }
            None => None,
        }
    }
}

impl KafkaClientServiceImpl {
    pub fn new(config_obj: Arc<dyn ConfigObj>) -> Arc<Self> {
        Arc::new(KafkaClientServiceImpl {
            config_obj,
            consumer: RwLock::new(None),
        })
    }
}

crate::di_service!(KafkaClientServiceImpl, [KafkaClientService]);

pub fn parse_vrl_payload_and_key(
    columns: &Vec<Column>,
    unique_key_columns: &Vec<Column>,
    payload: Value,
) -> Result<Option<Vec<TableValue>>, CubeError> {
    match payload {
        Value::Object(obj) => {
            let r = columns
                .iter()
                .map(|col| {
                    let mut field_value = obj.get(&KeyString::from(col.get_name().clone()));
                    /* if field_value.is_none() {
                        if unique_key_columns.iter().any(|c| c.get_name() == col.get_name()) {
                            field_value = match key {
                                JsonValue::Object(obj) => obj.get(col.get_name()),
                                x if unique_key_columns.len() == 1 => Some(x),
                                x => return Err(CubeError::internal(format!(
                                    "kafka key contains {:?} but object was expected due to unique key has multiple columns: {:?}",
                                    x, unique_key_columns
                                )))
                            }
                        }
                    } */
                    let value = field_value.unwrap_or(&Value::Null);
                    parse_vrl_value(&col, value)
                })
                .collect::<Result<Vec<TableValue>, CubeError>>()?;
            Ok(Some(r))
        }
        Value::Null => Ok(None),
        x => Err(CubeError::internal(format!(
            "kafka payload contains {:?} but object was expected",
            x
        ))),
    }
}

pub fn parse_vrl_value(column: &Column, value: &Value) -> Result<TableValue, CubeError> {
    match column.get_column_type() {
        ColumnType::String => match value {
            Value::Bytes(v) => Ok(TableValue::String(String::from_utf8_lossy(v).to_string())),
            /* Value::Integer(v) => Ok(TableValue::String(v.to_string())),
            Value::Float(v) => Ok(TableValue::String(v.to_string())),
            Value::Boolean(v) => Ok(TableValue::String(v.to_string())), */
            Value::Null => Ok(TableValue::Null),
            x => Err(CubeError::internal(format!(
                "ksql source returned {:?} as row value but only primitive values are supported",
                x
            ))),
        },
        ColumnType::Int => match value {
            Value::Integer(v) => Ok(TableValue::Int(*v)),
            Value::Null => Ok(TableValue::Null),
            x => Err(CubeError::internal(format!(
                "ksql source returned {:?} as row value but int expected",
                x
            ))),
        },
        ColumnType::Int96 => Err(CubeError::user(
            "int96 unsupported for streaming data".to_string(),
        )),
        ColumnType::Bytes => match value {
            _ => Err(CubeError::internal(format!(
                "ksql source bytes import isn't supported"
            ))),
        },
        ColumnType::HyperLogLog(_) => match value {
            _ => Err(CubeError::internal(format!(
                "ksql source HLL import isn't supported"
            ))),
        },
        ColumnType::Timestamp => match value {
            Value::Bytes(v) => Ok(TableValue::Timestamp(timestamp_from_string(
                &String::from_utf8_lossy(v),
            )?)),
            Value::Integer(v) => Ok(TableValue::Timestamp(TimestampValue::new(v * 1000000))),
            Value::Timestamp(v) => Ok(TableValue::Timestamp(TimestampValue::new(
                v.timestamp() * 1000000,
            ))),
            Value::Null => Ok(TableValue::Null),
            x => Err(CubeError::internal(format!(
                "ksql source returned {:?} as row value but only primitive values are supported",
                x
            ))),
        },
        ColumnType::Decimal { scale, .. } => match value {
            _ => Err(CubeError::internal(format!(
                "ksql source decimal import isn't supported"
            ))),
        },
        ColumnType::Decimal96 { .. } => Err(CubeError::user(
            "decimal96 unsupported for streaming data".to_string(),
        )),
        ColumnType::Float => match value {
            Value::Integer(v) => Ok(TableValue::Float(OrdF64(v.clone() as f64))),
            Value::Float(v) => Ok(TableValue::Float(OrdF64(v.clone().into_inner()))),
            Value::Null => Ok(TableValue::Null),
            x => Err(CubeError::internal(format!(
                "ksql source returned {:?} as row value but only number values are supported",
                x
            ))),
        },
        ColumnType::Boolean => match value {
            Value::Boolean(v) => Ok(TableValue::Boolean(*v)),
            Value::Null => Ok(TableValue::Null),
            x => Err(CubeError::internal(format!(
                "ksql source returned {:?} as row value but only boolean values are supported",
                x
            ))),
        },
    }
}

#[async_trait]
impl StreamingSource for KafkaStreamingSource {
    async fn row_stream(
        &self,
        _columns: Vec<Column>,
        _seq_column: Column,
        initial_seq_value: Option<i64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
        let column_to_move = self.columns.clone();
        let unique_key_columns = self.unique_key_columns.clone();
        let seq_column_index_to_move = self.seq_column_index;
        let traffic_sender = TrafficSender::new(self.trace_obj.clone());

        let callback: Arc<dyn Fn(KafkaMessage) -> Result<Option<Row>, CubeError> + Send + Sync> =
            if let Some(program) = &self.vrl_program {
                let program = program.clone();
                Arc::new(move |m: KafkaMessage| -> Result<_, _> {
                    if let Some(payload_str) = m.payload().map(|p| String::from_utf8_lossy(p)) {
                        let mut target = TargetValue {
                            // the value starts as just an object with a single field "x" set to 1
                            value: value!(payload_str.clone()),
                            // the metadata is empty
                            metadata: Value::Bytes(Bytes::new()),
                            // and there are no secrets associated with the target
                            secrets: Secrets::default(),
                        };

                        // The current state of the runtime (i.e. local variables)
                        let mut state = RuntimeState::default();

                        let timezone = TimeZone::default();

                        // A context bundles all the info necessary for the runtime to resolve a value.
                        let mut ctx = Context::new(&mut target, &mut state, &timezone);

                        // This executes the VRL program, making any modifications to the target, and returning a result.
                        let value = program.resolve(&mut ctx).unwrap();

                        if let Some(mut values) =
                            parse_vrl_payload_and_key(&column_to_move, &unique_key_columns, value)
                                .map_err(|e| {
                                CubeError::user(format!(
                                    "Can't parse kafka row with '{}' payload: {}",
                                    payload_str, e
                                ))
                            })?
                        {
                            values[seq_column_index_to_move] = TableValue::Int(m.offset());
                            Ok(Some(Row::new(values)))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                })
            } else {
                Arc::new(move |m: KafkaMessage| -> Result<_, _> {
                    if let Some(payload_str) = m.payload().map(|p| String::from_utf8_lossy(p)) {
                        traffic_sender.process_event(payload_str.len() as u64)?;
                        let payload = json::parse(payload_str.as_ref()).map_err(|e| {
                            CubeError::user(format!("Can't parse '{}' payload: {}", payload_str, e))
                        })?;
                        // Kafka can store additional metadata in suffix that contains information about window size for example
                        // Another use case is streams would usually don't have any keys
                        let mut key = JsonValue::Object(Object::new());
                        if let Some(key_str) = m.key().map(|p| String::from_utf8_lossy(p)) {
                            if key_str.starts_with("{") {
                                if let Some(last_brace) = key_str.find("}") {
                                    key = json::parse(&key_str.as_ref()[0..last_brace + 1])
                                        .map_err(|e| {
                                            CubeError::user(format!(
                                                "Can't parse '{}' key: {}",
                                                key_str, e
                                            ))
                                        })?;
                                }
                            }
                        }

                        let mut values = parse_json_payload_and_key(
                            &column_to_move,
                            &unique_key_columns,
                            payload,
                            &key,
                        )
                        .map_err(|e| {
                            CubeError::user(format!(
                                "Can't parse kafka row with '{}' key and '{}' payload: {}",
                                key, payload_str, e
                            ))
                        })?;
                        values[seq_column_index_to_move] = TableValue::Int(m.offset());
                        Ok(Some(Row::new(values)))
                    } else {
                        Ok(None)
                    }
                })
            };
        let stream = self
            .kafka_client
            .create_message_stream(
                self.table_id,
                self.topic.clone(),
                self.partition as i32,
                initial_seq_value.map(|seq| Offset::Offset(seq)).unwrap_or(
                    self.offset
                        .as_ref()
                        .map(|o| match o {
                            StreamOffset::Earliest => Offset::Beginning,
                            StreamOffset::Latest => Offset::End,
                        })
                        .unwrap_or(Offset::End),
                ),
                vec![self.host.clone()],
                &self.user,
                &self.password,
                self.use_ssl,
                callback,
            )
            .await?;

        Ok(stream)
    }

    async fn apply_post_processing(&self, data: Vec<ArrayRef>) -> Result<Vec<ArrayRef>, CubeError> {
        if let Some(post_processing_plan) = &self.post_processing_plan {
            post_processing_plan.apply(data).await
        } else {
            Ok(data)
        }
    }
    async fn calulate_lag(&self, current_seq: i64) -> Option<i64> {
        self.kafka_client
            .calulate_lag(self.topic.clone(), self.partition as i32, current_seq)
            .await
    }

    fn source_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    fn source_seq_column_index(&self) -> usize {
        self.seq_column_index
    }

    fn validate_table_location(&self) -> Result<(), CubeError> {
        // TODO
        // self.query(None)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{Column, ColumnType};
    use crate::queryplanner::query_executor::batch_to_dataframe;
    use crate::sql::MySqlDialectWithBackTicks;
    use crate::streaming::topic_table_provider::TopicTableProvider;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::ExecutionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion::sql::planner::SqlToRel;
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::Tokenizer;

    async fn run_single_value_query(select_statement: &str) -> TableValue {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, &select_statement);
        let tokens = tokenizer.tokenize().unwrap();
        let statement = Parser::new(tokens, dialect).parse_statement().unwrap();

        let provider = TopicTableProvider::new("t".to_string(), &vec![]);
        let query_planner = SqlToRel::new(&provider);

        let logical_plan = query_planner
            .statement_to_plan(&DFStatement::Statement(statement.clone()))
            .unwrap();
        let plan_ctx = Arc::new(ExecutionContext::new());
        let phys_plan = plan_ctx.create_physical_plan(&logical_plan).unwrap();

        let batches = collect(phys_plan).await.unwrap();
        let res = batch_to_dataframe(&batches).unwrap();
        res.get_rows()[0].values()[0].clone()
    }

    async fn run_array_query(select_statement: &str, input: Vec<String>) -> Vec<Row> {
        let provider = TopicTableProvider::new(
            "t".to_string(),
            &vec![Column::new("a".to_string(), ColumnType::String, 0)],
        );
        let schema = provider.schema();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(input))]).unwrap();
        let memery_input = vec![vec![batch]];
        let inp = Arc::new(MemoryExec::try_new(&memery_input, schema.clone(), None).unwrap());

        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, &select_statement);
        let tokens = tokenizer.tokenize().unwrap();
        let statement = Parser::new(tokens, dialect).parse_statement().unwrap();

        let query_planner = SqlToRel::new(&provider);

        let logical_plan = query_planner
            .statement_to_plan(&DFStatement::Statement(statement.clone()))
            .unwrap();
        let plan_ctx = Arc::new(ExecutionContext::new());
        let phys_plan = plan_ctx.create_physical_plan(&logical_plan).unwrap();
        let phys_plan = phys_plan.with_new_children(vec![inp]).unwrap();

        let batches = collect(phys_plan).await.unwrap();
        let res = batch_to_dataframe(&batches).unwrap();
        res.get_rows().to_vec()
    }

    fn assert_timestamp_val(v: &TableValue, expected: &str) {
        match v {
            TableValue::Timestamp(v) => {
                assert_eq!(&v.to_string(), expected);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[tokio::test]
    async fn test_scalar_parse_timestamp() {
        let select_statement = "SELECT PARSE_TIMESTAMP('2023-06-05T03:00:00.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC')";
        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-05T03:00:00.300Z",
        );

        let select_statement =
            "SELECT PARSE_TIMESTAMP('2023-06-05 03:00:00', 'yyyy-MM-dd HH:mm:ss')";
        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-05T03:00:00.000Z",
        );

        let select_statement = "SELECT PARSE_TIMESTAMP('2023-06-05T06:12:23', 'yyyy-MM-dd''T''HH:mm:ss', 'Europe/Istanbul')";
        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-05T03:12:23.000Z",
        );
    }

    #[tokio::test]
    async fn test_array_parse_timestamp() {
        let select_statement = "SELECT PARSE_TIMESTAMP(a, 'yyyy-MM-dd''T''HH:mm:ss', 'UTC') from t";
        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05T06:12:23".to_string(),
                "2023-06-05T06:12:25".to_string(),
                "2023-06-05T06:12:27".to_string(),
            ],
        )
        .await;

        assert_timestamp_val(&res[0].values()[0], "2023-06-05T06:12:23.000Z");
        assert_timestamp_val(&res[1].values()[0], "2023-06-05T06:12:25.000Z");
        assert_timestamp_val(&res[2].values()[0], "2023-06-05T06:12:27.000Z");

        let select_statement =
            "SELECT PARSE_TIMESTAMP(a, 'yyyy-MM-dd HH:mm:ss', 'Europe/Istanbul') from t";
        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05 06:12:23".to_string(),
                "2023-06-05 06:12:25".to_string(),
                "2023-06-05 06:12:27".to_string(),
            ],
        )
        .await;

        assert_timestamp_val(&res[0].values()[0], "2023-06-05T03:12:23.000Z");
        assert_timestamp_val(&res[1].values()[0], "2023-06-05T03:12:25.000Z");
        assert_timestamp_val(&res[2].values()[0], "2023-06-05T03:12:27.000Z");
    }

    #[tokio::test]
    async fn test_scalar_convert_tz() {
        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP('2023-06-05T03:00:00.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC'), 'UTC', 'UTC')";

        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-05T03:00:00.300Z",
        );

        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP('2023-06-05T03:12:23.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC'), 'UTC', 'Europe/Istanbul')";
        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-05T06:12:23.300Z",
        );

        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP('2023-06-05T03:12:23.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC'), 'Asia/Krasnoyarsk', 'Europe/Istanbul')";
        assert_timestamp_val(
            &run_single_value_query(select_statement).await,
            "2023-06-04T23:12:23.300Z",
        );
    }

    #[tokio::test]
    async fn test_array_convert_tz() {
        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP(a, 'yyyy-MM-dd''T''HH:mm:ss', 'UTC'), 'UTC', 'UTC') from t";
        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05T06:12:23".to_string(),
                "2023-06-05T06:12:25".to_string(),
                "2023-06-05T06:12:27".to_string(),
            ],
        )
        .await;

        assert_timestamp_val(&res[0].values()[0], "2023-06-05T06:12:23.000Z");
        assert_timestamp_val(&res[1].values()[0], "2023-06-05T06:12:25.000Z");
        assert_timestamp_val(&res[2].values()[0], "2023-06-05T06:12:27.000Z");

        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP(a, 'yyyy-MM-dd HH:mm:ss', 'UTC'), 'UTC', 'Europe/Istanbul') from t";
        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05 06:12:23".to_string(),
                "2023-06-05 06:12:25".to_string(),
                "2023-06-05 06:12:27".to_string(),
            ],
        )
        .await;

        assert_timestamp_val(&res[0].values()[0], "2023-06-05T09:12:23.000Z");
        assert_timestamp_val(&res[1].values()[0], "2023-06-05T09:12:25.000Z");
        assert_timestamp_val(&res[2].values()[0], "2023-06-05T09:12:27.000Z");

        let select_statement = "SELECT CONVERT_TZ_KSQL(PARSE_TIMESTAMP(a, 'yyyy-MM-dd HH:mm:ss', 'UTC'), 'Europe/Istanbul', 'Asia/Krasnoyarsk') from t";
        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05 06:12:23".to_string(),
                "2023-06-05 06:12:25".to_string(),
                "2023-06-05 06:12:27".to_string(),
            ],
        )
        .await;

        assert_timestamp_val(&res[0].values()[0], "2023-06-05T10:12:23.000Z");
        assert_timestamp_val(&res[1].values()[0], "2023-06-05T10:12:25.000Z");
        assert_timestamp_val(&res[2].values()[0], "2023-06-05T10:12:27.000Z");
    }

    #[tokio::test]
    async fn test_scalar_format_timestamp() {
        let select_statement = "SELECT \
                                FORMAT_TIMESTAMP(\
                                        CONVERT_TZ_KSQL(\
                                            PARSE_TIMESTAMP('2023-06-05T03:00:00.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC')\
                                        , 'UTC', 'UTC')\
                                     , 'yyyy-MM-dd HH:mm:ss')";

        let res = &run_single_value_query(select_statement).await;
        assert_eq!(res, &TableValue::String("2023-06-05 03:00:00".to_string()));

        let select_statement = "SELECT \
                                FORMAT_TIMESTAMP(\
                                        CONVERT_TZ_KSQL(\
                                            PARSE_TIMESTAMP('2023-06-05T03:24:50.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC')\
                                        , 'UTC', 'Europe/Istanbul')\
                                     , 'yyyy-MM-dd HH:mm:ss')";

        let res = &run_single_value_query(select_statement).await;
        assert_eq!(res, &TableValue::String("2023-06-05 06:24:50".to_string()));

        let select_statement = "SELECT \
                                FORMAT_TIMESTAMP(\
                                        CONVERT_TZ_KSQL(\
                                            PARSE_TIMESTAMP('2023-06-05T03:24:50.300Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSX', 'UTC')\
                                        , 'UTC', 'Europe/Istanbul')\
                                     , 'yyyy-MM-dd''T''HH:mm:00.000')";

        let res = &run_single_value_query(select_statement).await;
        assert_eq!(
            res,
            &TableValue::String("2023-06-05T06:24:00.000".to_string())
        );
    }

    #[tokio::test]
    async fn test_array_format_timestamp() {
        let select_statement = "SELECT \
                                FORMAT_TIMESTAMP(\
                                        CONVERT_TZ_KSQL(\
                                            PARSE_TIMESTAMP(a, 'yyyy-MM-dd''T''HH:mm:ss', 'UTC')\
                                        , 'UTC', 'UTC')\
                                     , 'yyyy-MM-dd HH:mm:ss') from t";

        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05T06:12:23".to_string(),
                "2023-06-05T06:12:25".to_string(),
                "2023-06-05T06:12:27".to_string(),
            ],
        )
        .await;

        assert_eq!(
            &res[0].values()[0],
            &TableValue::String("2023-06-05 06:12:23".to_string())
        );
        assert_eq!(
            &res[1].values()[0],
            &TableValue::String("2023-06-05 06:12:25".to_string())
        );
        assert_eq!(
            &res[2].values()[0],
            &TableValue::String("2023-06-05 06:12:27".to_string())
        );

        let select_statement = "SELECT \
                                FORMAT_TIMESTAMP(\
                                        CONVERT_TZ_KSQL(\
                                            PARSE_TIMESTAMP(a, 'yyyy-MM-dd''T''HH:mm:ss', 'UTC')\
                                        , 'UTC', 'Europe/Istanbul')\
                                     , 'yyyy-MM-dd''T''HH:00:00.000') from t";

        let res = run_array_query(
            select_statement,
            vec![
                "2023-06-05T06:12:23".to_string(),
                "2023-06-05T07:13:25".to_string(),
                "2023-06-05T06:12:27".to_string(),
            ],
        )
        .await;

        assert_eq!(
            &res[0].values()[0],
            &TableValue::String("2023-06-05T09:00:00.000".to_string())
        );
        assert_eq!(
            &res[1].values()[0],
            &TableValue::String("2023-06-05T10:00:00.000".to_string())
        );
        assert_eq!(
            &res[2].values()[0],
            &TableValue::String("2023-06-05T09:00:00.000".to_string())
        );
    }
}
