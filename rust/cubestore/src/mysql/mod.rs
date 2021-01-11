use crate::sql::SqlService;
use crate::table::TableValue;
use crate::{metastore, CubeError};
use async_trait::async_trait;
use log::{error, info, warn};
use msql_srv::*;
use std::io;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use itertools::Itertools;

struct Backend {
    sql_service: Arc<dyn SqlService>,
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
        let res = self.sql_service.exec_query(query).await;
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
                    metastore::ColumnType::Float => ColumnType::MYSQL_TYPE_STRING
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
                    TableValue::Bytes(b) => rw.write_col(b.iter().map(|v| v.to_string()).join(" "))?,
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
}

pub struct MySqlServer;

impl MySqlServer {
    pub async fn listen(
        address: String,
        sql_service: Arc<dyn SqlService>,
    ) -> Result<(), CubeError> {
        let mut listener = TcpListener::bind(address.clone()).await?;

        info!("MySQL port open on {}", address);

        loop {
            let (socket, _) = listener.accept().await?;

            let sql_service_clone = sql_service.clone();
            tokio::spawn(async move {
                if let Err(e) = AsyncMysqlIntermediary::run_on(
                    Backend {
                        sql_service: sql_service_clone,
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
}
