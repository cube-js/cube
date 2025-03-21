use std::sync::Arc;
use std::time::Duration;

use crate::cluster::{Cluster, JobEvent, JobResultListener};
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::import::ImportService;
use crate::metastore::job::JobType;
use crate::metastore::table::StreamOffset;
use crate::metastore::{
    table::Table, HllFlavour, IdRow, ImportFormat, IndexDef, IndexType, RowKey, TableId,
};
use crate::metastore::{Column, ColumnType, MetaStore};
use crate::sql::cache::SqlResultCache;
use crate::sql::parser::{CubeStoreParser, PartitionedIndexRef};
use crate::sql::{quoted_value_or_lower, quoted_value_or_retain_case};
use crate::telemetry::incoming_traffic_agent_event;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use sqlparser::ast::*;

#[async_trait]

pub trait TableExtensionService: DIService + Send + Sync {
    async fn get_extension(&self) -> Result<Option<serde_json::Value>, CubeError>;
}

pub struct TableExtensionServiceImpl;

impl TableExtensionServiceImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl TableExtensionService for TableExtensionServiceImpl {
    async fn get_extension(&self) -> Result<Option<serde_json::Value>, CubeError> {
        Ok(None)
    }
}

crate::di_service!(TableExtensionServiceImpl, [TableExtensionService]);

enum FinalizeExternalTableResult {
    Ok,
    Orphaned,
}
pub struct TableCreator {
    db: Arc<dyn MetaStore>,
    cluster: Arc<dyn Cluster>,
    import_service: Arc<dyn ImportService>,
    table_extension_service: Arc<dyn TableExtensionService>,
    config_obj: Arc<dyn ConfigObj>,
    create_table_timeout: Duration,
    cache: Arc<SqlResultCache>,
}

impl TableCreator {
    pub fn new(
        db: Arc<dyn MetaStore>,
        cluster: Arc<dyn Cluster>,
        import_service: Arc<dyn ImportService>,
        table_extension_service: Arc<dyn TableExtensionService>,
        config_obj: Arc<dyn ConfigObj>,
        create_table_timeout: Duration,
        cache: Arc<SqlResultCache>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            cluster,
            import_service,
            table_extension_service,
            config_obj,
            create_table_timeout,
            cache,
        })
    }
    pub async fn create_table(
        self: Arc<Self>,
        schema_name: String,
        table_name: String,
        columns: &Vec<ColumnDef>,
        external: bool,
        if_not_exists: bool,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_table: Option<String>,
        stream_offset: Option<String>,
        indexes: Vec<Statement>,
        unique_key: Option<Vec<Ident>>,
        aggregates: Option<Vec<(Ident, Ident)>>,
        partitioned_index: Option<PartitionedIndexRef>,
        trace_obj: &Option<String>,
    ) -> Result<IdRow<Table>, CubeError> {
        let extension: Option<serde_json::Value> =
            self.table_extension_service.get_extension().await?;
        if !if_not_exists {
            return self
                .create_table_loop(
                    schema_name,
                    table_name,
                    &columns,
                    external,
                    if_not_exists,
                    locations,
                    import_format,
                    build_range_end,
                    seal_at,
                    select_statement,
                    source_table,
                    stream_offset,
                    indexes,
                    unique_key,
                    aggregates,
                    partitioned_index,
                    &trace_obj,
                    &extension,
                )
                .await;
        }
        let this = self.clone();
        let trace_obj = trace_obj.clone();
        let columns = columns.clone();
        self.cache
            .create_table(schema_name.clone(), table_name.clone(), async move || {
                let table = this
                    .db
                    .get_table(schema_name.clone(), table_name.clone())
                    .await;

                if let Ok(table) = table {
                    if table.get_row().is_ready() {
                        return Ok(table);
                    }
                }
                this.create_table_loop(
                    schema_name,
                    table_name,
                    &columns,
                    external,
                    if_not_exists,
                    locations,
                    import_format,
                    build_range_end,
                    seal_at,
                    select_statement,
                    source_table,
                    stream_offset,
                    indexes,
                    unique_key,
                    aggregates,
                    partitioned_index,
                    &trace_obj,
                    &extension,
                )
                .await
            })
            .await
    }

    async fn create_table_loop(
        &self,
        schema_name: String,
        table_name: String,
        columns: &Vec<ColumnDef>,
        external: bool,
        if_not_exists: bool,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_table: Option<String>,
        stream_offset: Option<String>,
        indexes: Vec<Statement>,
        unique_key: Option<Vec<Ident>>,
        aggregates: Option<Vec<(Ident, Ident)>>,
        partitioned_index: Option<PartitionedIndexRef>,
        trace_obj: &Option<String>,
        extension: &Option<serde_json::Value>,
    ) -> Result<IdRow<Table>, CubeError> {
        let mut retries = 0;
        let max_retries = self.config_obj.create_table_max_retries();
        loop {
            let listener = if external {
                Some(self.cluster.job_result_listener())
            } else {
                None
            };
            let table = self
                .create_table_impl(
                    schema_name.clone(),
                    table_name.clone(),
                    columns,
                    external,
                    if_not_exists,
                    locations.clone(),
                    import_format,
                    build_range_end,
                    seal_at,
                    select_statement.clone(),
                    source_table.clone(),
                    stream_offset.clone(),
                    indexes.clone(),
                    unique_key.clone(),
                    aggregates.clone(),
                    partitioned_index.clone(),
                    trace_obj,
                    extension,
                )
                .await?;

            if let Some(listener) = listener {
                let finalize_res = tokio::time::timeout(
                    self.create_table_timeout,
                    self.finalize_external_table(&table, listener, trace_obj),
                )
                .await
                .map_err(|_| {
                    CubeError::internal(format!(
                        "Timeout during create table finalization: {:?}",
                        table
                    ))
                })
                .and_then(|r| r);
                match finalize_res {
                    Ok(FinalizeExternalTableResult::Orphaned) => {
                        if let Err(inner) = self.db.drop_table(table.get_id()).await {
                            log::error!(
                                "Drop table ({}) on orphaned import failed: {}",
                                table.get_id(),
                                inner
                            );
                            return Err(CubeError::internal(format!("Error during create table finalization {:?}: some jobs are orphaned", table)));
                        }
                        log::warn!(
                            "Some import jobs for table {} are orphaned, table creation restarted",
                            table.get_id()
                        );
                        retries += 1;
                        if retries > max_retries {
                            return Err(CubeError::internal(format!("Error during create table finalization {:?}: some jobs are orphaned", table)));
                        } else {
                            continue;
                        }
                    }
                    Err(e) => {
                        if let Err(inner) = self.db.drop_table(table.get_id()).await {
                            log::error!(
                                "Drop table ({}) after error failed: {}",
                                table.get_id(),
                                inner
                            );
                        }
                        return Err(e);
                    }
                    _ => {}
                }
            }
            return Ok(table);
        }
    }
    async fn create_table_impl(
        &self,
        schema_name: String,
        table_name: String,
        columns: &Vec<ColumnDef>,
        external: bool,
        if_not_exists: bool,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_table: Option<String>,
        stream_offset: Option<String>,
        indexes: Vec<Statement>,
        unique_key: Option<Vec<Ident>>,
        aggregates: Option<Vec<(Ident, Ident)>>,
        partitioned_index: Option<PartitionedIndexRef>,
        trace_obj: &Option<String>,
        extension: &Option<serde_json::Value>,
    ) -> Result<IdRow<Table>, CubeError> {
        let columns_to_set = convert_columns_type(columns)?;
        let mut indexes_to_create = Vec::new();
        if let Some(mut p) = partitioned_index {
            let part_index_name = match p.name.0.as_mut_slice() {
                &mut [ref schema, ref mut name] => {
                    if quoted_value_or_retain_case(&schema) != schema_name {
                        return Err(CubeError::user(format!("CREATE TABLE in schema '{}' cannot reference PARTITIONED INDEX from schema '{}'", schema_name, schema)));
                    }
                    quoted_value_or_retain_case(&name)
                }
                &mut [ref mut name] => quoted_value_or_retain_case(&name),
                _ => {
                    return Err(CubeError::user(format!(
                        "PARTITIONED INDEX must consist of 1 or 2 identifiers, got '{}'",
                        p.name
                    )))
                }
            };

            let mut columns = Vec::new();
            for c in p.columns {
                columns.push(quoted_value_or_lower(&c));
            }

            indexes_to_create.push(IndexDef {
                name: "#mi0".to_string(),
                columns,
                multi_index: Some(part_index_name),
                index_type: IndexType::Regular,
            });
        }

        for index in indexes.iter() {
            if let Statement::CreateIndex(CreateIndex {
                name,
                columns,
                unique,
                ..
            }) = index
            {
                let name = name.as_ref().ok_or(CubeError::user(format!(
                    "Index name is not defined during index creation for {}.{}",
                    schema_name, table_name
                )))?;
                indexes_to_create.push(IndexDef {
                    name: name.to_string(),
                    multi_index: None,
                    columns: columns
                        .iter()
                        .map(|c| {
                            if let Expr::Identifier(ident) = &c.expr {
                                Ok(quoted_value_or_lower(&ident))
                            } else {
                                Err(CubeError::internal(format!(
                                    "Unexpected column expression: {:?}",
                                    c.expr
                                )))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    index_type: if *unique {
                        IndexType::Aggregate
                    } else {
                        IndexType::Regular
                    },
                });
            }
        }

        let stream_offset = if let Some(s) = &stream_offset {
            Some(match s.as_str() {
                "earliest" => StreamOffset::Earliest,
                "latest" => StreamOffset::Latest,
                x => {
                    return Err(CubeError::user(format!(
                        "Unexpected stream offset: {}. Only earliest and latest are allowed.",
                        x
                    )))
                }
            })
        } else {
            None
        };

        let max_disk_space = self.config_obj.max_disk_space();
        if max_disk_space > 0 {
            let used_space = self.db.get_used_disk_space_out_of_queue(None).await?;
            if max_disk_space < used_space {
                return Err(CubeError::user(format!(
                    "Exceeded available storage space: {:.3} GB out of {} GB allowed. Please consider changing pre-aggregations build range, reducing index count or pre-aggregations granularity.",
                    used_space as f64 / 1024. / 1024. / 1024.,
                    max_disk_space as f64 / 1024. / 1024. / 1024.
                )));
            }
        }

        if !external {
            return self
                .db
                .create_table(
                    schema_name,
                    table_name,
                    columns_to_set,
                    None,
                    None,
                    indexes_to_create,
                    true,
                    build_range_end,
                    seal_at,
                    select_statement,
                    None,
                    stream_offset,
                    unique_key.map(|keys| keys.iter().map(|c| quoted_value_or_lower(&c)).collect()),
                    aggregates.map(|keys| {
                        keys.iter()
                            .map(|c| (quoted_value_or_lower(&c.0), quoted_value_or_lower(&c.1)))
                            .collect()
                    }),
                    None,
                    None,
                    false,
                    extension.as_ref().map(|json_value| json_value.to_string()),
                )
                .await;
        }

        if let Some(locations) = locations.as_ref() {
            self.import_service
                .validate_locations_size(locations)
                .await?;
        }

        let partition_split_threshold = if let Some(locations) = locations.as_ref() {
            let size = join_all(
                locations
                    .iter()
                    .map(|location| {
                        let location = location.to_string();
                        let import_service = self.import_service.clone();
                        return async move {
                            import_service.estimate_location_row_count(&location).await
                        };
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();

            let mut sel_workers_count = self.config_obj.select_workers().len() as u64;
            if sel_workers_count == 0 {
                sel_workers_count = 1;
            }
            let threshold = (size / sel_workers_count)
                .min(self.config_obj.max_partition_split_threshold())
                .max(self.config_obj.partition_split_threshold());

            Some(threshold)
        } else {
            None
        };

        let trace_obj_to_save = trace_obj.clone();

        let source_columns = if let Some(source_table) = source_table {
            let mut parser = CubeStoreParser::new(&source_table)?;
            let cols = parser
                .parse_streaming_source_table()
                .map_err(|e| CubeError::user(format!("Unexpected source_table param: {}", e)))?;
            let res = convert_columns_type(&cols)
                .map_err(|e| CubeError::user(format!("Unexpected source_table param: {}", e)))?;
            Some(res)
        } else {
            None
        };

        let table = self
            .db
            .create_table(
                schema_name,
                table_name,
                columns_to_set,
                locations,
                import_format,
                indexes_to_create,
                false,
                build_range_end,
                seal_at,
                select_statement,
                source_columns,
                stream_offset,
                unique_key.map(|keys| keys.iter().map(|c| quoted_value_or_lower(&c)).collect()),
                aggregates.map(|keys| {
                    keys.iter()
                        .map(|c| (quoted_value_or_lower(&c.0), quoted_value_or_lower(&c.1)))
                        .collect()
                }),
                partition_split_threshold,
                trace_obj_to_save,
                if_not_exists,
                extension.as_ref().map(|json_value| json_value.to_string()),
            )
            .await?;

        Ok(table)
    }
    async fn finalize_external_table(
        &self,
        table: &IdRow<Table>,
        listener: JobResultListener,
        trace_obj: &Option<String>,
    ) -> Result<FinalizeExternalTableResult, CubeError> {
        let wait_for = table
            .get_row()
            .locations()
            .unwrap()
            .iter()
            .filter(|&l| !Table::is_stream_location(l))
            .map(|&l| {
                (
                    RowKey::Table(TableId::Tables, table.get_id()),
                    JobType::TableImportCSV(l.clone()),
                )
            })
            .collect();
        for stream_location in table
            .get_row()
            .locations()
            .unwrap()
            .iter()
            .filter(|&l| Table::is_stream_location(l))
        {
            self.import_service
                .validate_table_location(table.get_id(), stream_location)
                .await?;
        }
        let imports = listener.wait_for_job_results(wait_for).await?;
        for r in imports {
            if let JobEvent::Error(_, _, e) = r {
                return Err(CubeError::user(format!("Create table failed: {}", e)));
            } else if let JobEvent::Orphaned(_, _) = r {
                return Ok(FinalizeExternalTableResult::Orphaned);
            }
        }

        let mut futures = Vec::new();
        let indexes = self.db.get_table_indexes(table.get_id()).await?;
        let partitions = self
            .db
            .get_active_partitions_and_chunks_by_index_id_for_select(
                indexes.iter().map(|i| i.get_id()).collect(),
            )
            .await?;
        // Omit warming up chunks as those shouldn't affect select times much however will affect
        // warming up time a lot in case of big tables when a lot of chunks pending for repartition
        for (partition, _) in partitions.into_iter().flatten() {
            futures.push(self.cluster.warmup_partition(partition, Vec::new()));
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let ready_table = self.db.table_ready(table.get_id(), true).await?;

        if let Some(trace_obj) = trace_obj.as_ref() {
            incoming_traffic_agent_event(trace_obj, ready_table.get_row().total_download_size())?;
        }

        Ok(FinalizeExternalTableResult::Ok)
    }
}

pub fn convert_columns_type(columns: &Vec<ColumnDef>) -> Result<Vec<Column>, CubeError> {
    let mut rolupdb_columns = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        let cube_col = Column::new(
            quoted_value_or_lower(&col.name),
            match &col.data_type {
                DataType::Date
                | DataType::Time(_, _)
                | DataType::Char(_)
                | DataType::Varchar(_)
                | DataType::Clob(_)
                | DataType::Text
                | DataType::String(_)
                | DataType::Character(_)
                | DataType::CharacterVarying(_)
                | DataType::CharVarying(_)
                | DataType::Nvarchar(_)
                | DataType::CharacterLargeObject(_)
                | DataType::CharLargeObject(_)
                | DataType::FixedString(_) => ColumnType::String,
                DataType::Uuid
                | DataType::Binary(_)
                | DataType::Varbinary(_)
                | DataType::Blob(_)
                | DataType::Bytea
                | DataType::Array(_)
                | DataType::Bytes(_) => ColumnType::Bytes,
                DataType::Decimal(number_info)
                | DataType::Numeric(number_info)
                | DataType::BigNumeric(number_info)
                | DataType::BigDecimal(number_info)
                | DataType::Dec(number_info) => {
                    let (precision, scale) = match number_info {
                        ExactNumberInfo::None => (None, None),
                        ExactNumberInfo::Precision(p) => (Some(*p), None),
                        ExactNumberInfo::PrecisionAndScale(p, s) => (Some(*p), Some(*s)),
                    };
                    let (precision, scale) = proper_decimal_args(&precision, &scale);
                    if precision > 18 {
                        ColumnType::Decimal96 {
                            precision: precision as i32,
                            scale: scale as i32,
                        }
                    } else {
                        ColumnType::Decimal {
                            precision: precision as i32,
                            scale: scale as i32,
                        }
                    }
                }
                DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Interval
                | DataType::TinyInt(_)
                | DataType::UnsignedTinyInt(_)
                | DataType::Int2(_)
                | DataType::UnsignedInt2(_)
                | DataType::UnsignedSmallInt(_)
                | DataType::MediumInt(_)
                | DataType::UnsignedMediumInt(_)
                | DataType::Int4(_)
                | DataType::Int8(_)
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128
                | DataType::Int256
                | DataType::Integer(_)
                | DataType::UnsignedInt(_)
                | DataType::UnsignedInt4(_)
                | DataType::UnsignedInteger(_)
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt128
                | DataType::UInt256
                | DataType::UnsignedBigInt(_)
                | DataType::UnsignedInt8(_) => ColumnType::Int,
                DataType::Boolean | DataType::Bool => ColumnType::Boolean,
                DataType::Float(_)
                | DataType::Real
                | DataType::Double
                | DataType::Float4
                | DataType::Float32
                | DataType::Float64
                | DataType::Float8
                | DataType::DoublePrecision => ColumnType::Float,
                DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Datetime(_)
                | DataType::Datetime64(_, _) => ColumnType::Timestamp,
                DataType::Custom(custom, _) => {
                    let custom_type_name = custom.to_string().to_lowercase();
                    match custom_type_name.as_str() {
                        "tinyint" | "mediumint" => ColumnType::Int,
                        "decimal96" => ColumnType::Decimal96 {
                            scale: 5,
                            precision: 27,
                        },
                        "int96" => ColumnType::Int96,
                        "bytes" => ColumnType::Bytes,
                        "varbinary" => ColumnType::Bytes,
                        "hyperloglog" => ColumnType::HyperLogLog(HllFlavour::Airlift),
                        "hyperloglogpp" => ColumnType::HyperLogLog(HllFlavour::ZetaSketch),
                        "hll_snowflake" => ColumnType::HyperLogLog(HllFlavour::Snowflake),
                        "hll_postgres" => ColumnType::HyperLogLog(HllFlavour::Postgres),
                        "hll_datasketches" => ColumnType::HyperLogLog(HllFlavour::DataSketches),
                        _ => {
                            return Err(CubeError::user(format!(
                                "Custom type '{}' is not supported",
                                custom
                            )))
                        }
                    }
                }
                DataType::Regclass
                | DataType::JSON
                | DataType::JSONB
                | DataType::Map(_, _)
                | DataType::Tuple(_)
                | DataType::Nested(_)
                | DataType::Enum(_)
                | DataType::Set(_)
                | DataType::Struct(_, _)
                | DataType::Union(_)
                | DataType::Nullable(_)
                | DataType::LowCardinality(_)
                | DataType::Unspecified
                | DataType::Trigger => {
                    return Err(CubeError::user(format!(
                        "Type '{}' is not supported.",
                        col.data_type
                    )));
                }
            },
            i,
        );
        rolupdb_columns.push(cube_col);
    }
    Ok(rolupdb_columns)
}
fn proper_decimal_args(precision: &Option<u64>, scale: &Option<u64>) -> (i32, i32) {
    let mut precision = precision.unwrap_or(18);
    let mut scale = scale.unwrap_or(5);
    // TODO upgrade DF
    // if precision > 27 {
    //     precision = 27;
    // }
    // if scale > 5 {
    //     scale = 10;
    // }
    if scale > precision {
        precision = scale;
    }
    (precision as i32, scale as i32)
}
