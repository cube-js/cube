use crate::config::injection::DIService;
use crate::metastore::table::StreamOffset;
use crate::metastore::Column;
use crate::streaming::{parse_json_payload_and_key, StreamingSource};
use crate::table::{Row, TableValue};
use crate::CubeError;
use async_std::stream;
use async_trait::async_trait;
use datafusion::cube_ext;
use futures::Stream;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
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
    // TODO Support parsing of filters and applying before insert
    _select_statement: Option<String>,
    offset: Option<StreamOffset>,
    partition: usize,
    kafka_client: Arc<dyn KafkaClientService>,
    use_ssl: bool,
}

impl KafkaStreamingSource {
    pub fn new(
        table_id: u64,
        unique_key_columns: Vec<Column>,
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
        KafkaStreamingSource {
            table_id,
            unique_key_columns,
            user,
            password,
            topic,
            host,
            _select_statement: select_statement,
            offset,
            partition,
            kafka_client,
            use_ssl,
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

pub struct KafkaClientServiceImpl {}

pub enum KafkaMessage<'a> {
    BorrowedMessage(BorrowedMessage<'a>),
}

impl<'a> KafkaMessage<'a> {
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.key(),
        }
    }

    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.payload(),
        }
    }

    pub fn offset(&self) -> i64 {
        match self {
            KafkaMessage::BorrowedMessage(m) => m.offset(),
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
        Ok(Box::pin(stream::from_fn(move || {
            let stream_consumer = stream_consumer.clone();
            let to_row = to_row.clone();
            async move {
                loop {
                    let message = stream_consumer.recv().await;
                    let row = message
                        .map_err(|e| {
                            CubeError::user(format!("Error during fetching kafka message: {}", e))
                        })
                        .and_then(|m| to_row(KafkaMessage::BorrowedMessage(m)));
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
    pub fn new() -> Arc<Self> {
        Arc::new(KafkaClientServiceImpl {})
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
                    if let Some((payload, key)) = m
                        .payload()
                        .map(|p| String::from_utf8_lossy(p))
                        .zip(m.key().map(|p| String::from_utf8_lossy(p)))
                    {
                        let payload = json::parse(payload.as_ref())?;
                        let key = json::parse(key.as_ref())?;
                        let mut values = parse_json_payload_and_key(
                            &column_to_move,
                            &unique_key_columns,
                            payload,
                            key,
                        )?;
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

    fn validate_table_location(&self) -> Result<(), CubeError> {
        // TODO
        // self.query(None)?;
        Ok(())
    }
}
