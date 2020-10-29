use log::{trace};

use sqlparser::dialect::{Dialect};
use sqlparser::ast::*;
use async_trait::async_trait;


use crate::table::{TableValue, Row, TimestampValue};
use crate::CubeError;
use crate::{store::{DataFrame, WALDataStore}, metastore::{MetaStore, Column, ColumnType}};
use std::sync::Arc;
use crate::metastore::{IdRow, Schema, table::Table, ImportFormat, Index, IndexDef, TableId, RowKey};

use crate::queryplanner::{QueryPlanner};

use crate::cluster::{Cluster, JobEvent};

use datafusion::sql::parser::{DFParser, CreateExternalTable};
use datafusion::sql::parser::{Statement as DFStatement};
use futures::future::join_all;
use crate::metastore::job::JobType;
use datafusion::physical_plan::datetime_expressions::string_to_timestamp_nanos;

#[async_trait]
pub trait SqlService: Send + Sync {
    async fn exec_query(&self, query: &str) -> Result<DataFrame, CubeError>;
}

pub struct SqlServiceImpl {
    db: Arc<dyn MetaStore>,
    wal_store: Arc<dyn WALDataStore>,
    query_planner: Arc<dyn QueryPlanner>,
    cluster: Arc<dyn Cluster>,
}

impl SqlServiceImpl {
    pub fn new(
        db: Arc<dyn MetaStore>,
        wal_store: Arc<dyn WALDataStore>,
        query_planner: Arc<dyn QueryPlanner>,
        cluster: Arc<dyn Cluster>
    ) -> Arc<SqlServiceImpl> {
        Arc::new(SqlServiceImpl { db, wal_store, query_planner, cluster })
    }

    async fn create_schema(&self, name: String) -> Result<IdRow<Schema>, CubeError> {
        self.db.create_schema(name).await
    }

    async fn create_table(&self, schema_name: String, table_name: String, columns: &Vec<ColumnDef>, external: bool, location: Option<String>, indexes: Vec<Statement>) -> Result<IdRow<Table>, CubeError> {
        let columns_to_set = convert_columns_type(columns)?;
        let mut indexes_to_create = Vec::new();
        for index in indexes.iter() {
            if let Statement::CreateIndex { name, columns, .. } = index {
                indexes_to_create.push(IndexDef { name: name.to_string(), columns: columns.iter().map(|c| c.to_string()).collect::<Vec<_>>() });
            }
        }
        if external {
            self.db.create_table(schema_name, table_name, columns_to_set, location, Some(ImportFormat::CSV), indexes_to_create).await
        } else {
            self.db.create_table(schema_name, table_name, columns_to_set, None, None, indexes_to_create).await
        }
    }

    async fn _create_index(&self, schema_name: String, table_name: String, name: String, columns: &Vec<Ident>) -> Result<IdRow<Index>, CubeError> {
        let table = self.db.get_table(schema_name.clone(), table_name.clone()).await?;
        let columns_to_write = columns.iter().enumerate().map(
            |(i, c)| table.get_row().get_columns().iter().find(|tc| tc.get_name() == &c.value).map(|c| c.replace_index(i))
                .ok_or(CubeError::user(format!("Column '{}' is not found in {}.{}", c.value, schema_name, table_name)))
        ).collect::<Vec<_>>();
        if let Some(Err(e)) = columns_to_write.iter().find(|r| r.is_err()) {
            return Err(e.clone());
        }
        Ok(self.db.index_table().insert_row(Index::new(
            name,
            table.get_id(),
            columns_to_write.into_iter().map(|c| c.unwrap().clone()).collect::<Vec<_>>(),
            columns.len() as u64)).await?
        )
    }

    async fn insert_data<'a>(&'a self, schema_name: String, table_name: String, columns: &'a Vec<Ident>, data: &'a Vec<Vec<Expr>>) -> Result<u64, CubeError> {
        let table = self.db.get_table(schema_name.clone(), table_name.clone()).await?;
        let table_columns = table.get_row().clone();
        let table_columns = table_columns.get_columns();
        let mut real_col: Vec<&Column> = Vec::new();
        for column in columns {
            let c = if let Some(item) = table_columns
                .iter()
                .find(|voc| *voc.get_name() == column.value) {
                item
            } else {
                return Err(CubeError::user(format!("Column {} does noot present in table {}.{}.", column.value, schema_name, table_name)));
            };
            real_col.push(c);
        };


        let chunk_len = self.wal_store.get_wal_chunk_size();

        let mut wal_ids = Vec::new();

        for rows_chunk in data.chunks(chunk_len) {
            let data_frame = parse_chunk(rows_chunk, &real_col)?;
            wal_ids.push(self.wal_store.add_wal(table.clone(), data_frame).await?.get_id());
        }

        let res = join_all(wal_ids.into_iter().map(|id| {
            self.cluster.wait_for_job_result(RowKey::Table(TableId::WALs, id), JobType::WalPartitioning)
        })).await;

        for v in res {
            if let JobEvent::Error(_, _, e) = v? {
                return Err(CubeError::user(format!("Insert job failed: {}", e)));
            }
        }

        Ok(data.len() as u64)
    }
}

#[derive(Debug)]
pub struct MySqlDialectWithBackTicks {}

impl Dialect for MySqlDialectWithBackTicks {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // We don't yet support identifiers beginning with numbers, as that
        // makes it hard to distinguish numeric literals.
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || ch == '_'
            || ch == '$'
            || (ch >= '\u{0080}' && ch <= '\u{ffff}')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || (ch >= '0' && ch <= '9')
    }
}

#[async_trait]
impl SqlService for SqlServiceImpl {
    async fn exec_query(&self, q: &str) -> Result<DataFrame, CubeError> {
        trace!("Query: '{}'", q);
        if let Some(data_frame) = SqlServiceImpl::handle_workbench_queries(q) {
            return Ok(data_frame);
        }
        let dialect = &MySqlDialectWithBackTicks {};
        let replaced_quote = q.replace("\\'", "''");
        let ast = DFParser::parse_sql_with_dialect(&replaced_quote, dialect)?;
        // trace!("AST is: {:?}", ast);
        for query in ast {
            match query {
                DFStatement::Statement(Statement::ShowVariable { variable }) => {
                    return match variable.value.to_lowercase() {
                        s if s == "schemas" => Ok(DataFrame::from(self.db.get_schemas().await?)),
                        s if s == "tables" => Ok(DataFrame::from(self.db.get_tables().await?)),
                        s if s == "chunks" => Ok(DataFrame::from(self.db.chunks_table().all_rows().await?)),
                        s if s == "indexes" => Ok(DataFrame::from(self.db.index_table().all_rows().await?)),
                        s if s == "partitions" => Ok(DataFrame::from(self.db.partition_table().all_rows().await?)),
                        x => Err(CubeError::user(format!("Unknown SHOW: {}", x)))
                    };
                }
                DFStatement::Statement(Statement::SetVariable { .. }) => {
                    return Ok(DataFrame::new(vec![], vec![]));
                }
                DFStatement::Statement(Statement::CreateSchema { schema_name }) => {
                    let name = schema_name.to_string();
                    let res = self.create_schema(name).await?;
                    return Ok(DataFrame::from(vec![res]));
                }
                DFStatement::Statement(Statement::CreateTable { name, columns, external, location, .. }) => {
                    let nv = &name.0;
                    if nv.len() != 2 {
                        return Err(CubeError::user(format!("Schema's name should be present in query (boo.table1). Your query was '{}'", q)));
                    }
                    let schema_name = &nv[0].value;
                    let table_name = &nv[1].value;

                    let res = self.create_table(schema_name.clone(), table_name.clone(), &columns, external, location, vec![]).await?;
                    return Ok(DataFrame::from(vec![res]));
                }
                DFStatement::Statement(Statement::Drop { object_type, names, .. }) => {
                    match object_type {
                        ObjectType::Schema => {
                            self.db.delete_schema(names[0].to_string()).await?;
                        }
                        ObjectType::Table => {
                            let table = self.db.get_table(names[0].0[0].to_string(), names[0].0[1].to_string()).await?;
                            self.db.drop_table(table.get_id()).await?;
                        }
                        _ => return Err(CubeError::user("Unsupported drop operation".to_string()))
                    }
                    return Ok(DataFrame::new(vec![], vec![]))
                }
                DFStatement::CreateExternalTable(CreateExternalTable { name, columns, location, indexes, .. }) => {
                    let ObjectName(table_ident) = name.clone();
                    if table_ident.len() != 2 {
                        return Err(CubeError::user(format!("Schema name expected in table name but '{}' found", name.to_string())));
                    }

                    let res = self.create_table(
                        table_ident[0].value.to_string(),
                        table_ident[1].value.to_string(),
                        &columns,
                        true,
                        Some(location),
                        indexes
                    ).await?;
                    return Ok(DataFrame::from(vec![res]));
                }
                DFStatement::Statement(Statement::Insert { table_name, columns, source }) => {
                    let data = if let SetExpr::Values(Values(data_series)) = &source.body {
                        data_series
                    } else {
                        return Err(CubeError::user(format!("Data should be present in query. Your query was '{}'", q)));
                    };

                    let nv = &table_name.0;
                    if nv.len() != 2 {
                        return Err(CubeError::user(format!("Schema's name should be present in query (boo.table1). Your query was '{}'", q)));
                    }
                    let schema_name = &nv[0].value;
                    let table_name = &nv[1].value;

                    self.insert_data(schema_name.clone(), table_name.clone(), &columns, data).await?;
                    return Ok(DataFrame::new(vec![], vec![]));
                }
                DFStatement::Statement(Statement::Query(_)) => {
                    let logical_plan = self.query_planner.logical_plan(query.clone()).await?;
                    let res = self.cluster.run_select(self.cluster.server_name().to_string(), logical_plan).await?; // TODO distribute and combine
                    return Ok(res);
                }
                _ => return Err(CubeError::user(format!("Unsupported SQL: '{}'", q)))
            };
        }
        Err(CubeError::user(format!("Unsupported SQL: '{}'", q)))
    }
}


fn convert_columns_type(columns: &Vec<ColumnDef>) -> Result<Vec<Column>, CubeError> {
    let mut rolupdb_columns = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        let cube_col = Column::new(col.name.value.clone(),
                                   match &col.data_type {
                                           DataType::Date | DataType::Time | DataType::Char(_)
                                           | DataType::Varchar(_) | DataType::Clob(_)
                                           | DataType::Text => { ColumnType::String }
                                           DataType::Uuid | DataType::Binary(_)
                                           | DataType::Varbinary(_) | DataType::Blob(_)
                                           | DataType::Bytea
                                           | DataType::Array(_) => { ColumnType::Bytes }
                                           DataType::Decimal(_, _) => { ColumnType::Int }
                                           DataType::SmallInt | DataType::Int
                                           | DataType::BigInt
                                           | DataType::Interval => { ColumnType::Int }
                                           DataType::Boolean => ColumnType::Boolean,
                                           DataType::Float(_) | DataType::Real
                                           | DataType::Double => { ColumnType::Decimal }
                                           DataType::Timestamp => { ColumnType::Timestamp }
                                           DataType::Custom(custom) => {
                                               return Err(CubeError::user(format!("Custom type '{}' is not supported", custom)));
                                           }
                                           DataType::Regclass => {
                                               return Err(CubeError::user("Type 'RegClass' is not suppored.".to_string()));
                                           }
                                       },
                                   i,
        );
        rolupdb_columns.push(cube_col);
    }
    Ok(rolupdb_columns)
}

fn parse_chunk(chunk: &[Vec<Expr>], column: &Vec<&Column>) -> Result<DataFrame, CubeError> {
    let mut res: Vec<Row> = Vec::new();
    for r in chunk {
        let mut row = vec![TableValue::Int(0); column.len()];
        for i in 0..r.len() {
            row[column[i].get_index()] = extract_data(&r[i], column, i)?;
        }
        res.push(Row::new(row));
    }
    Ok(DataFrame::new(column.iter().map(|c| (*c).clone()).collect::<Vec<Column>>(), res))
}

fn extract_data(cell: &Expr, column: &Vec<&Column>, i: usize) -> Result<TableValue, CubeError> {
    let d = if let Expr::Value(v) = cell {
        v
    } else {
        return Err(CubeError::user(format!("Value is expected but {:?} found", cell)));
    };
    if let Value::Null = d {
        return Ok(TableValue::Null);
    }
    let res = {
        match column[i].get_column_type() {
            ColumnType::String => {
                let val = if let Value::SingleQuotedString(v) = d {
                    v
                } else {
                    return Err(CubeError::user(format!("Single quoted string is expected but {:?} found", cell)));
                };
                TableValue::String(val.to_string())
            }
            ColumnType::Int => {
                let val = if let Value::Number(v) | Value::SingleQuotedString(v) = d {
                    v
                } else {
                    return Err(CubeError::user(format!("Can't parse int from, {:?}", d)));
                };
                let val_int = val.parse::<i64>();
                if let Err(e) = val_int {
                    return Err(CubeError::user(format!("Can't parse int from, {:?}: {}", d, e)));
                }
                TableValue::Int(val_int.unwrap())
            }
            ColumnType::Decimal => { return Err(CubeError::user("Decimal type not implemented.".to_string())); }
            ColumnType::Bytes => {
                // TODO What we need to do with Bytes, now it  just convert each element of string to u8 item of Vec<u8>
                let val = if let Value::Number(v) = d {
                    v
                } else {
                    return Err(CubeError::user("Corrupted data in query.".to_string()));
                };
                let main_vec: Vec<u8> = val.split("") // split string into words by whitespace
                    .filter_map(|w| w.parse::<u8>().ok()) // calling ok() turns Result to Option so that filter_map can discard None values
                    .collect();
                TableValue::Bytes(main_vec)
            }
            ColumnType::Timestamp => {
                match d {
                    Value::SingleQuotedString(v) => {
                        TableValue::Timestamp(TimestampValue::new(string_to_timestamp_nanos(v)?))
                    },
                    x => return Err(CubeError::user(format!("Can't parse timestamp from, {:?}", x)))
                }
            }
            ColumnType::Boolean => {
                match d {
                    Value::SingleQuotedString(v) => {
                        TableValue::Boolean(v.to_lowercase() == "true")
                    },
                    Value::Boolean(b) => TableValue::Boolean(*b),
                    x => return Err(CubeError::user(format!("Can't parse boolean from, {:?}", x)))
                }
            }
        }
    };
    Ok(res)
}


#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{DB, Options};
    use crate::remotefs::LocalDirRemoteFs;
    use std::path::PathBuf;
    use crate::queryplanner::MockQueryPlanner;
    use crate::config::Config;
    use crate::metastore::{MetaStoreEvent, RocksMetaStore};
    use crate::metastore::job::JobType;
    use std::borrow::BorrowMut;
    use crate::cluster::MockCluster;
    use crate::metastore::listener::{MetastoreListenerImpl};
    use futures::future::{join3};
    use std::fs;
    use crate::scheduler::SchedulerImpl;
    use crate::store::WALStore;

    #[actix_rt::test]
    async fn create_schema_test() {
        let path = "/tmp/test_create_schema";
        let _ = DB::destroy(&Options::default(), path);
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(PathBuf::from(store_path.clone()), PathBuf::from(remote_store_path.clone()));
            let meta_store = RocksMetaStore::new(path, remote_fs.clone());
            let store = WALStore::new(meta_store.clone(), remote_fs.clone(), 10);
            let service = SqlServiceImpl::new(
                meta_store,
                store,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockCluster::new()),
            );
            let i = service.exec_query("CREATE SCHEMA foo").await.unwrap();
            assert_eq!(i.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::String("foo".to_string())]));
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[actix_rt::test]
    async fn create_table_test() {
        let path = "/tmp/test_create_table";
        let _ = DB::destroy(&Options::default(), path);
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(PathBuf::from(store_path.clone()), PathBuf::from(remote_store_path.clone()));
            let meta_store = RocksMetaStore::new(path, remote_fs.clone());
            let store = WALStore::new(meta_store.clone(), remote_fs.clone(), 10);
            let service = SqlServiceImpl::new(meta_store, store, Arc::new(MockQueryPlanner::new()), Arc::new(MockCluster::new()));
            let i = service.exec_query("CREATE SCHEMA Foo").await.unwrap();
            assert_eq!(i.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::String("Foo".to_string())]));
            let query = "CREATE TABLE Foo.Persons (
                                PersonID int,
                                LastName varchar(255),
                                FirstName varchar(255),
                                Address varchar(255),
                                City varchar(255)
                              );";
            let i = service.exec_query(&query.to_string()).await.unwrap();
            assert_eq!(i.get_rows()[0], Row::new(vec![
                TableValue::Int(1),
                TableValue::String("Persons".to_string()),
                TableValue::String("1".to_string()),
                TableValue::String("[{\"name\":\"PersonID\",\"column_type\":\"Int\",\"column_index\":0},{\"name\":\"LastName\",\"column_type\":\"String\",\"column_index\":1},{\"name\":\"FirstName\",\"column_type\":\"String\",\"column_index\":2},{\"name\":\"Address\",\"column_type\":\"String\",\"column_index\":3},{\"name\":\"City\",\"column_type\":\"String\",\"column_index\":4}]".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
            ]));
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[actix_rt::test]
    async fn insert_test() {
        let path = "/tmp/test_insert";
        let _ = DB::destroy(&Options::default(), path);
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(PathBuf::from(store_path.clone()), PathBuf::from(remote_store_path.clone()));
            let meta_store = RocksMetaStore::new(path, remote_fs);
            let remote_fs = LocalDirRemoteFs::new(PathBuf::from(store_path.clone()), PathBuf::from(remote_store_path.clone()));
            let store = WALStore::new(meta_store.clone(), remote_fs.clone(), 10);
            let mut mock_cluster = MockCluster::new();

            mock_cluster.expect_wait_for_job_result()
                .times(3)
                .returning(
                    move |k, t| Ok(JobEvent::Success(k, t))
                );

            let service = SqlServiceImpl::new(meta_store.clone(), store, Arc::new(MockQueryPlanner::new()), Arc::new(mock_cluster));
            let _ = service.exec_query("CREATE SCHEMA Foo").await.unwrap();
            let query = "CREATE TABLE Foo.Persons (
                                PersonID int,
                                LastName varchar(255),
                                FirstName varchar(255),
                                Address varchar(255),
                                City varchar(255)
                              )";
            let meta_store_to_move = meta_store.clone();
            tokio::spawn(async move { meta_store_to_move.run_upload_loop().await });
            service.exec_query(query).await.unwrap();
            let query = "INSERT INTO Foo.Persons
            (
                PersonID,
                LastName,
                FirstName,
                Address,
                City
            )

            VALUES
            (23, 'LastName 1', 'FirstName 1', 'Address 1', 'City 1'), (38, 'LastName 21', 'FirstName 2', 'Address 2', 'City 2'),
            (24, 'LastName 3', 'FirstName 1', 'Address 1', 'City 1'), (37, 'LastName 22', 'FirstName 2', 'Address 2', 'City 2'),
            (25, 'LastName 4', 'FirstName 1', 'Address 1', 'City 1'), (36, 'LastName 23', 'FirstName 2', 'Address 2', 'City 2'),
            (26, 'LastName 5', 'FirstName 1', 'Address 1', 'City 1'), (35, 'LastName 24', 'FirstName 2', 'Address 2', 'City 2'),
            (27, 'LastName 6', 'FirstName 1', 'Address 1', 'City 1'), (34, 'LastName 25', 'FirstName 2', 'Address 2', 'City 2'),
            (28, 'LastName 7', 'FirstName 1', 'Address 1', 'City 1'), (33, 'LastName 26', 'FirstName 2', 'Address 2', 'City 2'),
            (29, 'LastName 8', 'FirstName 1', 'Address 1', 'City 1'), (32, 'LastName 27', 'FirstName 2', 'Address 2', 'City 2'),
            (30, 'LastName 9', 'FirstName 1', 'Address 1', 'City 1'), (31, 'LastName 28', 'FirstName 2', 'Address 2', 'City 2');";
            let _ = service.exec_query(query).await.unwrap();

            let query = "INSERT INTO Foo.Persons
            (LastName, PersonID, FirstName, Address, City)
            VALUES
            ('LastName 1', 23, 'FirstName 1', 'Address 1', 'City 1'), ('LastName 2', 22, 'FirstName 2', 'Address 2', 'City 2');";
            let _ = service.exec_query(query).await.unwrap();
            meta_store.stop_processing_loops().await;
        }
        let _ = DB::destroy(&Options::default(), path);
        // let _ =  fs::remove_dir_all(store_path.clone());
        // let _ =  fs::remove_dir_all(remote_store_path.clone());
    }

    #[actix_rt::test]
    async fn select_test() {
        let config = Config::test("select");

        let store_path = config.local_dir().clone();
        let remote_store_path = config.remote_dir().clone();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        {
            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();

            select_tests(services.scheduler.write().await.borrow_mut(), services.listener.clone(), services.sql_service.clone()).await;
            services.stop_processing_loops().await.unwrap();
        }
        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    async fn select_tests(scheduler: &mut SchedulerImpl, listener: Arc<MetastoreListenerImpl>, service: Arc<dyn SqlService>) {
        let _ = service.exec_query("CREATE SCHEMA Foo").await.unwrap();

        let query = "CREATE TABLE Foo.Persons (
                                PersonID int,
                                LastName varchar(255),
                                FirstName varchar(255),
                                Address varchar(255),
                                City varchar(255)
                              );";

        let _ = service.exec_query(query).await.unwrap();

        let query = "INSERT INTO Foo.Persons
            (LastName, PersonID, FirstName, Address, City)
            VALUES
            ('LastName 1', 23, 'FirstName 1', 'Address 1', 'City 1'), ('LastName 2', 22, 'FirstName 2', 'Address 2', 'City 2');";

        let (r1, r2, r3) = join3(
            service.exec_query(query),
            listener.run_listener_until(|e| match e {
                MetaStoreEvent::DeleteJob(job) => {
                    match job.get_row().job_type() {
                        JobType::WalPartitioning => true,
                        _ => false
                    }
                }
                _ => false
            }),
            scheduler.run_scheduler_until(|e| match e {
                MetaStoreEvent::DeleteJob(job) => {
                    match job.get_row().job_type() {
                        JobType::WalPartitioning => true,
                        _ => false
                    }
                }
                _ => false
            }),
        ).await;
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();

        let result = service.exec_query("SELECT PersonID person_id from Foo.Persons").await.unwrap();

        assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(22)]));
        assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(23)]));
    }
}

impl SqlServiceImpl {
    fn handle_workbench_queries(q: &str) -> Option<DataFrame> {
        if q == "SHOW SESSION VARIABLES LIKE 'lower_case_table_names'" {
            return Some(DataFrame::new(
                vec![
                    Column::new("Variable_name".to_string(), ColumnType::String, 0),
                    Column::new("Value".to_string(), ColumnType::String, 1)
                ],
                vec![
                    Row::new(vec![
                        TableValue::String("lower_case_table_names".to_string()),
                        TableValue::String("2".to_string())
                    ])
                ],
            ));
        }
        if q == "SHOW SESSION VARIABLES LIKE 'sql_mode'" {
            return Some(DataFrame::new(
                vec![
                    Column::new("Variable_name".to_string(), ColumnType::String, 0),
                    Column::new("Value".to_string(), ColumnType::String, 1)
                ],
                vec![
                    Row::new(vec![
                        TableValue::String("sql_mode".to_string()),
                        TableValue::String("TRADITIONAL".to_string())
                    ])
                ],
            ));
        }
        if q.to_lowercase() == "select current_user()" {
            return Some(DataFrame::new(
                vec![
                    Column::new("user".to_string(), ColumnType::String, 0),
                ],
                vec![
                    Row::new(vec![
                        TableValue::String("root".to_string()),
                    ])
                ],
            ));
        }
        if q.to_lowercase() == "select connection_id()" { // TODO
            return Some(DataFrame::new(
                vec![
                    Column::new("connection_id".to_string(), ColumnType::String, 0),
                ],
                vec![
                    Row::new(vec![
                        TableValue::String("1".to_string()),
                    ])
                ],
            ));
        }
        if q.to_lowercase() == "select connection_id() as connectionid" { // TODO
            return Some(DataFrame::new(
                vec![
                    Column::new("connectionId".to_string(), ColumnType::String, 0),
                ],
                vec![
                    Row::new(vec![
                        TableValue::String("1".to_string()),
                    ])
                ],
            ));
        }
        if q.to_lowercase() == "set character set utf8" {
            return Some(DataFrame::new(
                vec![],
                vec![],
            ));
        }
        if q.to_lowercase() == "set names utf8" {
            return Some(DataFrame::new(
                vec![],
                vec![],
            ));
        }
        if q.to_lowercase() == "show character set where charset = 'utf8mb4'" {
            return Some(DataFrame::new(
                vec![],
                vec![],
            ));
        }
        None
    }
}
