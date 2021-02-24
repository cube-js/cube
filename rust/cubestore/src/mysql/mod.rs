use crate::config::processing_loop::ProcessingLoop;
use crate::sql::{SqlQueryContext, SqlService};
use crate::table::TableValue;
use crate::{metastore, CubeError};
use async_trait::async_trait;
use hex::ToHex;
use log::{error, info, warn};
use msql_srv::*;
use std::io;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};

struct Backend {
    sql_service: Arc<dyn SqlService>,
    auth: Arc<dyn MySqlAuth>,
    user: Option<String>,
}

#[async_trait]
impl<W: io::Write + Send> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        info.reply(42, &[], &[])
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        results.completed(0, 0)
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32)
    where
        W: 'async_trait,
    {
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let start = SystemTime::now();
        let res = self
            .sql_service
            .exec_query_with_context(
                SqlQueryContext {
                    user: self.user.clone(),
                },
                query,
            )
            .await;
        if let Err(e) = res {
            error!("Error during processing {}: {}", query, e.message);
            results.error(ErrorKind::ER_INTERNAL_ERROR, e.message.as_bytes())?;
            return Ok(());
        }
        let data_frame = res.unwrap();
        let columns = data_frame
            .get_columns()
            .iter()
            .map(|c| Column {
                table: "result".to_string(), // TODO
                column: c.get_name().to_string(),
                coltype: match c.get_column_type() {
                    metastore::ColumnType::String => ColumnType::MYSQL_TYPE_STRING,
                    metastore::ColumnType::Timestamp => ColumnType::MYSQL_TYPE_STRING,
                    metastore::ColumnType::Int => ColumnType::MYSQL_TYPE_LONGLONG,
                    metastore::ColumnType::Decimal { .. } => ColumnType::MYSQL_TYPE_DECIMAL,
                    metastore::ColumnType::Boolean => ColumnType::MYSQL_TYPE_STRING,
                    metastore::ColumnType::Bytes => ColumnType::MYSQL_TYPE_STRING,
                    metastore::ColumnType::HyperLogLog(_) => ColumnType::MYSQL_TYPE_STRING,
                    metastore::ColumnType::Float => ColumnType::MYSQL_TYPE_STRING,
                },
                colflags: ColumnFlags::empty(),
            })
            .collect::<Vec<_>>();

        let mut rw = results.start(&columns)?;
        for row in data_frame.get_rows().iter() {
            for value in row.values().iter() {
                match value {
                    TableValue::String(s) => rw.write_col(s)?,
                    TableValue::Timestamp(s) => rw.write_col(s.to_string())?,
                    TableValue::Int(i) => rw.write_col(i)?,
                    TableValue::Decimal(v) => rw.write_col(v.to_string())?,
                    TableValue::Boolean(v) => rw.write_col(v.to_string())?,
                    TableValue::Float(v) => rw.write_col(v.to_string())?,
                    TableValue::Bytes(b) => {
                        rw.write_col(format!("0x{}", b.encode_hex_upper::<String>()))?
                    }
                    TableValue::Null => rw.write_col(Option::<String>::None)?,
                }
            }
            rw.end_row()?;
        }
        rw.finish()?;
        if start.elapsed().unwrap().as_millis() > 200 && query.to_lowercase().starts_with("select")
        {
            warn!(
                "Slow Query SQL ({:?}):\n{}",
                start.elapsed().unwrap(),
                query
            );
        }
        Ok(())
    }

    async fn on_auth<'a>(&'a mut self, user: Vec<u8>) -> Result<Option<Vec<u8>>, Self::Error>
    where
        W: 'async_trait,
    {
        if !user.is_empty() {
            self.user = Some(String::from_utf8_lossy(user.as_slice()).to_string())
        }
        self.auth
            .authenticate(user)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }
}

pub struct MySqlServer {
    address: String,
    sql_service: Arc<dyn SqlService>,
    auth: Arc<dyn MySqlAuth>,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
}

crate::di_service!(MySqlServer, []);

#[async_trait]
impl ProcessingLoop for MySqlServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        info!("MySQL port open on {}", self.address);

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            let (socket, _) = tokio::select! {
                res = stop_receiver.changed() => {
                    if res.is_err() || *stop_receiver.borrow() {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok(res) => res,
                        Err(err) => {
                            error!("Network error: {}", err);
                            continue;
                        }
                    }
                }
            };

            let sql_service = self.sql_service.clone();
            let auth = self.auth.clone();
            tokio::spawn(async move {
                if let Err(e) = AsyncMysqlIntermediary::run_on(
                    Backend {
                        sql_service,
                        auth,
                        user: None,
                    },
                    socket,
                )
                .await
                {
                    error!("Error during processing MySQL connection: {}", e);
                }
            });
        }
    }

    async fn stop_processing(&self) -> Result<(), CubeError> {
        self.close_socket_tx.send(true)?;
        Ok(())
    }
}

impl MySqlServer {
    pub fn new(
        address: String,
        sql_service: Arc<dyn SqlService>,
        auth: Arc<dyn MySqlAuth>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);
        Arc::new(Self {
            address,
            sql_service,
            auth,
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}

#[async_trait]
pub trait MySqlAuth: Send + Sync {
    async fn authenticate(&self, user: Vec<u8>) -> Result<Option<Vec<u8>>, CubeError>;
}

pub struct MySqlAuthDefaultImpl;

crate::di_service!(MySqlAuthDefaultImpl, [MySqlAuth]);

#[async_trait]
impl MySqlAuth for MySqlAuthDefaultImpl {
    async fn authenticate(&self, _user: Vec<u8>) -> Result<Option<Vec<u8>>, CubeError> {
        Ok(None)
    }
}
