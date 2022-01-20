use std::env;
use std::io;

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::prelude::DataFrame as DFDataFrame;

use log::debug;
use log::error;
use log::trace;

use msql_srv::*;

use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};

use crate::compile::convert_sql_to_cube_query;
use crate::compile::convert_statement_to_cube_query;
use crate::compile::parser::parse_sql_to_statement;
use crate::compile::QueryPlannerExecutionProps;
use crate::config::processing_loop::ProcessingLoop;
use crate::mysql::dataframe::batch_to_dataframe;
use crate::schema::SchemaService;
use crate::schema::V1CubeMetaExt;
use crate::CubeError;
use sqlparser::ast::{ShowCreateObject, Statement};

pub mod dataframe;

struct Backend {
    auth: Arc<dyn SqlAuthService>,
    schema: Arc<dyn SchemaService>,
    props: QueryPlannerExecutionProps,
    // Auth result from SqlAuthService
    context: Option<AuthContext>,
    // From MysqlServerOptions
    nonce: Arc<Option<Vec<u8>>>,
}

enum QueryResponse {
    Ok(StatusFlags),
    ResultSet(StatusFlags, Arc<dataframe::DataFrame>),
}

impl Backend {
    async fn execute_query<'a>(&'a mut self, query: &'a str) -> Result<QueryResponse, CubeError> {
        let _start = SystemTime::now();

        let query = query.replace("SELECT FROM", "SELECT * FROM");
        debug!("QUERY: {}", query);

        let query_lower = query.to_lowercase();
        let query_lower = query_lower.replace("db.`", "");
        let query_lower = query_lower.replace("`", "");

        let ignore = match query_lower.as_str() {
            "rollback" => true,
            "commit" => true,
            // DataGrip workaround
            "set character_set_results = utf8" => true,
            "set character_set_results = latin1" => true,
            "set autocommit=1" => true,
            "set sql_mode='strict_trans_tables'" => true,
            "set sql_select_limit=501" => true,
            _ => false,
        };

        if query_lower.eq("set autocommit=1, sql_mode = concat(@@sql_mode,',strict_trans_tables')")
        {
            return Ok(QueryResponse::Ok(
                StatusFlags::SERVER_STATUS_AUTOCOMMIT | StatusFlags::SERVER_SESSION_STATE_CHANGED,
            ));
        } else if query_lower.eq("show collation where charset = 'utf8mb4' and collation = 'utf8mb4_bin'") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "Collation".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Charset".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Id".to_string(),
                            ColumnType::MYSQL_TYPE_LONGLONG,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Default".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Compiled".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Sortlen".to_string(),
                            ColumnType::MYSQL_TYPE_LONGLONG,
                            ColumnFlags::empty(),
                        ), dataframe::Column::new(
                            "Pad_attribute".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("utf8mb4_bin".to_string()),
                            dataframe::TableValue::String("utf8mb4".to_string()),
                            dataframe::TableValue::Int64(46),
                            dataframe::TableValue::String("".to_string()),
                            dataframe::TableValue::String("YES".to_string()),
                            dataframe::TableValue::Int64(1),
                            dataframe::TableValue::String("PAD SPACE".to_string()),
                        ])]
                    )
                )),
            )
        }else if query_lower.eq("select cast('test plain returns' as char(60)) as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test plain returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.eq("select cast('test unicode returns' as char(60)) as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test plain returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.eq("select cast('test collated returns' as char character set utf8mb4) collate utf8mb4_bin as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test collated returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.eq("select @@transaction_isolation") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "@@transaction_isolation".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("REPEATABLE-READ".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.starts_with("describe") || query_lower.starts_with("explain") {
            let stmt = parse_sql_to_statement(&query)?;
            match stmt {
                Statement::ExplainTable { table_name, .. } => {
                    let table_name_filter = if table_name.0.len() == 2 {
                        &table_name.0[1].value
                    } else {
                        &table_name.0[0].value
                    };

                    let ctx = if self.context.is_some() {
                        self.context.as_ref().unwrap()
                    } else {
                        return Err(CubeError::user("must be auth".to_string()))
                    };

                    let ctx = self.schema
                        .get_ctx_for_tenant(ctx)
                        .await?;

                    if let Some(cube) = ctx.cubes.iter().find(|c| c.name.eq(table_name_filter)) {
                        let rows = cube.get_columns().iter().map(|column| dataframe::Row::new(
                            vec![
                                dataframe::TableValue::String(column.get_name().clone()),
                                dataframe::TableValue::String(column.get_column_type().clone()),
                                dataframe::TableValue::String(if column.mysql_can_be_null() { "Yes".to_string() } else { "No".to_string() }),
                                dataframe::TableValue::String("".to_string()),
                                dataframe::TableValue::Null,
                                dataframe::TableValue::String("".to_string()),
                            ]
                        )).collect();


                        return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                            vec![
                                dataframe::Column::new(
                                    "Field".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Type".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Null".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Key".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Default".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Extra".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                )
                            ],
                            rows
                        ))))
                    } else {
                        return Err(CubeError::internal("Unknown table".to_string()))
                    }
                },
                Statement::Explain { statement, .. } => {
                    let auth_ctx = if self.context.is_some() {
                        self.context.as_ref().unwrap()
                    } else {
                        return Err(CubeError::user("must be auth".to_string()))
                    };

                    let ctx = self.schema
                        .get_ctx_for_tenant(auth_ctx)
                    .await?;

                    let plan = convert_statement_to_cube_query(&statement, Arc::new(ctx), &self.props)?;

                    return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                        vec![
                            dataframe::Column::new(
                                "Execution Plan".to_string(),
                                ColumnType::MYSQL_TYPE_STRING,
                                ColumnFlags::empty(),
                            ),
                        ],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String(
                                plan.print(true)?
                            )
                        ])]
                    ))))
                },
                _ => {
                    return Err(CubeError::internal("Unexpected type in ExplainTable".to_string()))
                }
            }
        } else if query_lower.starts_with("show create table") {
            let stmt = parse_sql_to_statement(&query)?;
            match stmt {
                Statement::ShowCreate { obj_type, obj_name } => {
                    match obj_type {
                        ShowCreateObject::Table => {
                            let table_name_filter = if obj_name.0.len() == 2 {
                                &obj_name.0[1].value
                            } else {
                                &obj_name.0[0].value
                            };

                            let ctx = if self.context.is_some() {
                                self.context.as_ref().unwrap()
                            } else {
                                return Err(CubeError::user("must be auth".to_string()))
                            };

                            let ctx = self.schema
                                .get_ctx_for_tenant(ctx)
                                .await?;

                            if let Some(cube) = ctx.cubes.iter().find(|c| c.name.eq(table_name_filter)) {
                                let mut fields: Vec<String> = vec![];

                                for column in &cube.get_columns() {
                                    fields.push(format!(
                                        "`{}` {}{}",
                                        column.get_name(),
                                        column.get_column_type(),
                                        if column.mysql_can_be_null() { " NOT NULL" } else { "" }
                                    ));
                                }

                                return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                                    vec![
                                        dataframe::Column::new(
                                            "Table".to_string(),
                                            ColumnType::MYSQL_TYPE_STRING,
                                            ColumnFlags::empty(),
                                        ),
                                        dataframe::Column::new(
                                            "Create Table".to_string(),
                                            ColumnType::MYSQL_TYPE_STRING,
                                            ColumnFlags::empty(),
                                        )
                                    ],
                                    vec![dataframe::Row::new(vec![
                                        dataframe::TableValue::String(cube.name.clone()),
                                        dataframe::TableValue::String(
                                            format!("CREATE TABLE `{}` (\r\n  {}\r\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", cube.name, fields.join(",\r\n  "))
                                        ),
                                    ])]
                                ))))
                            } else {
                                return Err(CubeError::internal("Unknown table".to_string()));
                            }
                        }
                        _ => {
                            return Err(CubeError::internal("Unexpected type in ShowCreate".to_string()))
                        }
                    }
                },
                _ => {
                    return Err(CubeError::internal("Unexpected AST in ShowCreate method".to_string()))
                }
            }
        } else if query_lower.starts_with("show full tables from") {
            let auth_ctx = if self.context.is_some() {
                self.context.as_ref().unwrap()
            } else {
                return Err(CubeError::user("must be auth".to_string()))
            };

            let ctx = self.schema
                .get_ctx_for_tenant(auth_ctx)
                .await?;

            let values = ctx.cubes.iter()
                .map(|cube| dataframe::Row::new(vec![
                    dataframe::TableValue::String(cube.name.clone()),
                    dataframe::TableValue::String("BASE TABLE".to_string()),
                ])).collect();

            return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                vec![
                    dataframe::Column::new(
                        "Tables_in_db".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                        ColumnFlags::empty(),
                    ),
                    dataframe::Column::new(
                        "Table_type".to_string(),
                        ColumnType::MYSQL_TYPE_STRING,
                        ColumnFlags::empty(),
                    )
                ],
                values
            ))))
        } else if !ignore {
            trace!("query was not detected");

            let auth_ctx = if self.context.is_some() {
                self.context.as_ref().unwrap()
            } else {
                return Err(CubeError::user("must be auth".to_string()))
            };

            let ctx = self.schema
                .get_ctx_for_tenant(auth_ctx)
                .await?;

            let plan = convert_sql_to_cube_query(&query, Arc::new(ctx), &self.props)?;
            match plan {
                crate::compile::QueryPlan::MetaOk(status) => {
                    return Ok(QueryResponse::Ok(status));
                },
                crate::compile::QueryPlan::MetaTabular(status, data_frame) => {
                    return Ok(QueryResponse::ResultSet(status, data_frame));
                },
                crate::compile::QueryPlan::DataFushionSelect(status, plan, ctx) => {
                    let df = DataFrameImpl::new(
                        ctx.state,
                        &plan,
                    );
                    let batches = df.collect().await?;
                    let response =  batch_to_dataframe(&batches)?;

                    return Ok(QueryResponse::ResultSet(status, Arc::new(response)))
                },
                crate::compile::QueryPlan::CubeSelect(status, plan) => {
                    debug!("Request {}", json!(plan.request).to_string());
                    debug!("Meta {:?}", plan.meta);

                    let response = self.schema
                        .request(plan.request, auth_ctx)
                        .await?;

                    let mut columns: Vec<dataframe::Column> = vec![];

                    for column_meta in &plan.meta {
                        columns.push(dataframe::Column::new(
                            column_meta.column_to.clone(),
                            column_meta.column_type,
                            ColumnFlags::empty(),
                        ));
                    }

                    let mut rows: Vec<dataframe::Row> = vec![];

                    if let Some(result) = response.results.first() {
                        debug!("Columns {:?}", columns);
                        debug!("Hydration mapping {:?}", plan.meta);
                        trace!("Response from Cube.js {:?}", result.data);

                        for row in result.data.iter() {
                            if let Some(record) = row.as_object() {
                                rows.push(
                                    dataframe::Row::hydrate_from_response(&plan.meta, record)
                                );
                            } else {
                                error!(
                                    "Unable to map row to DataFrame::Row: {:?}, skipping row",
                                    row
                                );
                            }
                        }

                        return Ok(QueryResponse::ResultSet(status, Arc::new(dataframe::DataFrame::new(
                            columns,
                            rows
                        ))));
                    } else {
                        return Ok(QueryResponse::ResultSet(status, Arc::new(dataframe::DataFrame::new(vec![], vec![]))));
                    }
                }
            }
        }

        if ignore {
            Ok(QueryResponse::ResultSet(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(vec![], vec![])),
            ))
        } else {
            Err(CubeError::internal("Unsupported query".to_string()))
        }
    }
}

#[async_trait]
impl<W: io::Write + Send> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    fn server_version(&self) -> &str {
        "8.0.25"
    }

    fn connection_id(&self) -> u32 {
        self.props.connection_id()
    }

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
        results.completed(0, 0, StatusFlags::empty())
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
        match self.execute_query(query).await {
            Err(e) => {
                error!("Error during processing {}: {}", query, e.to_string());
                results.error(ErrorKind::ER_INTERNAL_ERROR, e.message.as_bytes())?;

                Ok(())
            }
            Ok(QueryResponse::Ok(status)) => {
                results.completed(0, 0, status)?;
                Ok(())
            }
            Ok(QueryResponse::ResultSet(_, data_frame)) => {
                let columns = data_frame
                    .get_columns()
                    .iter()
                    .map(|c| Column {
                        table: "result".to_string(), // TODO
                        column: c.get_name(),
                        coltype: c.get_type(),
                        colflags: c.get_flags(),
                    })
                    .collect::<Vec<_>>();

                let mut rw = results.start(&columns)?;

                for row in data_frame.get_rows().iter() {
                    for (_i, value) in row.values().iter().enumerate() {
                        match value {
                            dataframe::TableValue::String(s) => rw.write_col(s)?,
                            dataframe::TableValue::Timestamp(s) => rw.write_col(s.to_string())?,
                            dataframe::TableValue::Boolean(s) => rw.write_col(s.to_string())?,
                            dataframe::TableValue::Float64(s) => rw.write_col(s)?,
                            dataframe::TableValue::Int64(s) => rw.write_col(s)?,
                            dataframe::TableValue::Null => rw.write_col(Option::<String>::None)?,
                        }
                    }

                    rw.end_row()?;
                }

                rw.finish()?;

                Ok(())
            }
        }
    }

    async fn on_auth<'a>(&'a mut self, user: Vec<u8>) -> Result<Option<Vec<u8>>, Self::Error>
    where
        W: 'async_trait,
    {
        let user = if !user.is_empty() {
            Some(String::from_utf8_lossy(user.as_slice()).to_string())
        } else {
            None
        };

        let ctx = self.auth.authenticate(user.clone()).await.map_err(|e| {
            if e.message != *"Incorrect user name or password" {
                error!("Error during authentication MySQL connection: {}", e);
            };

            io::Error::new(io::ErrorKind::Other, e.to_string())
        })?;

        let passwd = ctx.password.clone().map(|p| p.as_bytes().to_vec());

        self.props.set_user(user.clone());
        self.context = Some(ctx);

        Ok(passwd)
    }

    /// Generate salt for native auth plugin
    async fn generate_nonce<'a>(&'a mut self) -> Result<Vec<u8>, Self::Error>
    where
        W: 'async_trait,
    {
        if let Some(n) = &*self.nonce {
            Ok(n.clone())
        } else {
            let random_bytes: Vec<u8> = (0..20).map(|_| rand::random::<u8>()).collect();
            Ok(random_bytes)
        }
    }
}

pub struct MySqlServer {
    address: String,
    auth: Arc<dyn SqlAuthService>,
    schema: Arc<dyn SchemaService>,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
    nonce: Arc<Option<Vec<u8>>>,
}

crate::di_service!(MySqlServer, []);

#[async_trait]
impl ProcessingLoop for MySqlServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL is listening on {}", self.address);

        let mut connection_id_incr = 0;

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

            let auth = self.auth.clone();
            let schema = self.schema.clone();
            let nonce = self.nonce.clone();

            let connection_id = if connection_id_incr > 100_000_u32 {
                connection_id_incr = 1;

                connection_id_incr
            } else {
                connection_id_incr += 1;

                connection_id_incr
            };

            tokio::spawn(async move {
                if let Err(e) = AsyncMysqlIntermediary::run_on(
                    Backend {
                        auth,
                        schema,
                        props: QueryPlannerExecutionProps::new(connection_id, None, None),
                        context: None,
                        nonce,
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
        auth: Arc<dyn SqlAuthService>,
        schema: Arc<dyn SchemaService>,
        nonce: Option<Vec<u8>>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);
        Arc::new(Self {
            address,
            auth,
            schema,
            nonce: Arc::new(nonce),
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}

#[derive(Debug)]
pub struct AuthContext {
    pub password: Option<String>,
    pub access_token: String,
    pub base_path: String,
}

#[async_trait]
pub trait SqlAuthService: Send + Sync {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthContext, CubeError>;
}

pub struct SqlAuthDefaultImpl;

crate::di_service!(SqlAuthDefaultImpl, [SqlAuthService]);

#[async_trait]
impl SqlAuthService for SqlAuthDefaultImpl {
    async fn authenticate(&self, _user: Option<String>) -> Result<AuthContext, CubeError> {
        Ok(AuthContext {
            password: None,
            access_token: env::var("CUBESQL_CUBE_TOKEN")
                .ok()
                .unwrap_or_else(|| panic!("CUBESQL_CUBE_TOKEN is a required ENV variable")),
            base_path: env::var("CUBESQL_CUBE_URL")
                .ok()
                .unwrap_or_else(|| panic!("CUBESQL_CUBE_URL is a required ENV variable")),
        })
    }
}
