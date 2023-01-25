use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::table::StreamOffset;
use crate::metastore::Column;
use crate::sql::MySqlDialectWithBackTicks;
use crate::streaming::{parse_json_payload_and_key, StreamingSource};
use crate::table::{Row, TableValue};
use crate::CubeError;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes::Schema, datatypes::SchemaRef};
use async_std::stream;
use async_trait::async_trait;
use datafusion::catalog::TableReference;
use datafusion::cube_ext;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr as DExpr;
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::ExecutionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use futures::Stream;
use json::object::Object;
use json::JsonValue;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use sqlparser::ast::{Query, SetExpr, Statement};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaStreamingSource {
    table_id: u64,
    unique_key_columns: Vec<Column>,
    user: Option<String>,
    password: Option<String>,
    topic: String,
    host: String,
    offset: Option<StreamOffset>,
    partition: usize,
    kafka_client: Arc<dyn KafkaClientService>,
    use_ssl: bool,
    post_filter: Option<Arc<dyn ExecutionPlan>>,
}

impl KafkaStreamingSource {
    pub fn new(
        table_id: u64,
        unique_key_columns: Vec<Column>,
        columns: Vec<Column>,
        user: Option<String>,
        password: Option<String>,
        topic: String,
        host: String,
        select_statement: Option<String>,
        offset: Option<StreamOffset>,
        partition: usize,
        kafka_client: Arc<dyn KafkaClientService>,
        use_ssl: bool,
    ) -> Self {
        let post_filter = if let Some(select_statement) = select_statement {
            let planner = KafkaFilterPlanner {
                topic: topic.clone(),
                columns,
            };
            match planner.parse_select_statement(select_statement.clone()) {
                Ok(p) => p,
                Err(e) => {
                    //FIXME May be we should stop execution here
                    log::error!(
                        "Error while parsing `select_statement`: {}. Select statement ignored",
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        KafkaStreamingSource {
            table_id,
            unique_key_columns,
            user,
            password,
            topic,
            host,
            offset,
            partition,
            kafka_client,
            use_ssl,
            post_filter,
        }
    }
}

pub struct KafkaFilterPlanner {
    topic: String,
    columns: Vec<Column>,
}

impl KafkaFilterPlanner {
    fn parse_select_statement(
        &self,
        select_statement: String,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, CubeError> {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, &select_statement);
        let tokens = tokenizer.tokenize().unwrap();
        let statement = Parser::new(tokens, dialect).parse_statement()?;

        match &statement {
            Statement::Query(box Query {
                body: SetExpr::Select(s),
                ..
            }) => {
                if s.selection.is_none() {
                    return Ok(None);
                }
                let provider = TopicTableProvider::new(self.topic.clone(), &self.columns);
                let query_planner = SqlToRel::new(&provider);
                let logical_plan =
                    query_planner.statement_to_plan(&DFStatement::Statement(statement.clone()))?;
                let physical_filter = Self::make_physical_filter(&logical_plan)?;
                Ok(physical_filter)
            }
            _ => Err(CubeError::user(format!(
                "{} is not valid select query",
                select_statement
            ))),
        }
    }

    /// Only Projection > Filter > TableScan plans are allowed
    fn make_physical_filter(
        plan: &LogicalPlan,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, CubeError> {
        match plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                filter_plan @ LogicalPlan::Filter { input, .. } => match input.as_ref() {
                    LogicalPlan::TableScan { .. } => {
                        let plan_ctx = Arc::new(ExecutionContext::new());
                        let phys_plan = plan_ctx.create_physical_plan(&filter_plan)?;
                        Ok(Some(phys_plan))
                    }
                    _ => Ok(None),
                },
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
struct TopicTableProvider {
    topic: String,
    schema: SchemaRef,
}

impl TopicTableProvider {
    pub fn new(topic: String, columns: &Vec<Column>) -> Self {
        let schema = Arc::new(Schema::new(
            columns.iter().map(|c| c.clone().into()).collect::<Vec<_>>(),
        ));
        Self { topic, schema }
    }
}

impl ContextProvider for TopicTableProvider {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        match name {
            TableReference::Bare { table } if table == self.topic => Some(Arc::new(self.clone())),
            _ => None,
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }
}

impl TableProvider for TopicTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[DExpr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(EmptyExec::new(false, self.schema())))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
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
}

pub struct KafkaClientServiceImpl {
    config_obj: Arc<dyn ConfigObj>,
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
}

impl KafkaClientServiceImpl {
    pub fn new(config_obj: Arc<dyn ConfigObj>) -> Arc<Self> {
        Arc::new(KafkaClientServiceImpl { config_obj })
    }
}

crate::di_service!(KafkaClientServiceImpl, [KafkaClientService]);

#[async_trait]
impl StreamingSource for KafkaStreamingSource {
    async fn row_stream(
        &self,
        columns: Vec<Column>,
        seq_column: Column,
        initial_seq_value: Option<i64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
        let column_to_move = columns.clone();
        let unique_key_columns = self.unique_key_columns.clone();
        let seq_column_to_move = seq_column.clone();
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
                Arc::new(move |m| -> Result<_, _> {
                    if let Some(payload_str) = m.payload().map(|p| String::from_utf8_lossy(p)) {
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
                        values[seq_column_to_move.get_index()] = TableValue::Int(m.offset());
                        Ok(Some(Row::new(values)))
                    } else {
                        Ok(None)
                    }
                }),
            )
            .await?;

        Ok(stream)
    }

    async fn apply_post_filter(&self, data: Vec<ArrayRef>) -> Result<Vec<ArrayRef>, CubeError> {
        if let Some(post_filter) = &self.post_filter {
            let schema = post_filter.children()[0].schema();
            let batch = RecordBatch::try_new(schema.clone(), data)?;
            let input = Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None)?);
            let filter = post_filter.with_new_children(vec![input])?;
            let mut out_batches = collect(filter).await?;
            let res = if out_batches.len() == 1 {
                out_batches.pop().unwrap()
            } else {
                RecordBatch::concat(&schema, &out_batches)?
            };

            Ok(res.columns().to_vec())
        } else {
            Ok(data)
        }
    }

    fn validate_table_location(&self) -> Result<(), CubeError> {
        // TODO
        // self.query(None)?;
        Ok(())
    }
}
