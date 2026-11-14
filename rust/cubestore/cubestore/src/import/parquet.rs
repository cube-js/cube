use super::*;
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Match fields by name and use Arrow's typed conversions. In particular, do not
/// format decimal/timestamp/binary values as CSV strings on the way to Arrow.
fn convert_batch(batch: &RecordBatch, columns: &[Column]) -> Result<Vec<ArrayRef>, CubeError> {
    let schema = batch.schema();
    columns
        .iter()
        .map(|column| {
            let matches: Vec<_> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() == column.get_name())
                .collect();
            if matches.len() != 1 {
                return Err(CubeError::user(format!(
                    "Parquet column '{}' must appear exactly once",
                    column.get_name()
                )));
            }
            let source = batch.column(matches[0].0);
            match source.data_type() {
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64 => {}
                other => {
                    return Err(CubeError::user(format!(
                        "Unsupported Parquet type {:?} for '{}'",
                        other,
                        column.get_name()
                    )))
                }
            }
            if matches!(column.get_column_type(), ColumnType::HyperLogLog(_)) {
                return Err(CubeError::user(
                    "Parquet HyperLogLog imports are not supported".to_string(),
                ));
            }
            let field: Field = column.into();
            // Arrow's default safe mode turns failed conversions into nulls. Fail the
            // import instead of silently changing non-null values into nulls.
            cast_with_options(
                source,
                field.data_type(),
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|e| CubeError::user(format!("Parquet column '{}': {}", column.get_name(), e)))
        })
        .collect()
}

impl ImportServiceImpl {
    pub(super) async fn do_import_parquet(
        &self,
        table: &IdRow<Table>,
        file: File,
        data_loaded_size: Option<Arc<DataLoadedSize>>,
    ) -> Result<(), CubeError> {
        let file = file.into_std().await;
        let batch_size = (self.config_obj.wal_split_threshold() as usize).clamp(1, 1024);
        let mut reader = cube_ext::spawn_blocking(move || {
            ParquetRecordBatchReaderBuilder::try_new(file)
                .and_then(|b| b.with_batch_size(batch_size).build())
                .map_err(|e| CubeError::user(format!("Unable to open Parquet file: {}", e)))
        })
        .await??;
        let mut ingestion = Ingestion::new(
            self.meta_store.clone(),
            self.chunk_store.clone(),
            self.limits.clone(),
            table.clone(),
        );
        let size_limit = self
            .config_obj
            .wal_split_size_threshold_bytes()
            .map(|v| v as usize);
        loop {
            let columns = table.get_row().get_columns().clone();
            // File IO, decompression, and conversion all run outside Tokio workers.
            let (next_reader, next) = cube_ext::spawn_blocking(move || {
                let next = reader
                    .next()
                    .map(|batch| {
                        let batch = batch
                            .map_err(|e| CubeError::user(format!("Parquet read failed: {}", e)))?;
                        convert_batch(&batch, &columns)
                    })
                    .transpose();
                (reader, next)
            })
            .await?;
            reader = next_reader;
            let Some(arrays) = next? else {
                break;
            };
            let rows = arrays.first().map(|a| a.len()).unwrap_or(0);
            let mut offset = 0;
            while offset < rows {
                let mut count = rows - offset;
                let mut slice: Vec<ArrayRef> =
                    arrays.iter().map(|a| a.slice(offset, count)).collect();
                while count > 1
                    && size_limit.map_or(false, |limit| columns_vec_buffer_size(&slice) > limit)
                {
                    count = (count / 2).max(1);
                    slice = arrays.iter().map(|a| a.slice(offset, count)).collect();
                }
                if let Some(size) = &data_loaded_size {
                    size.add(columns_vec_buffer_size(&slice));
                }
                ingestion.queue_data_frame(slice).await?;
                offset += count;
            }
        }
        ingestion.wait_completion().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::table::{Row, TableValue, TimestampValue};
    use crate::util::decimal::Decimal;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{Schema, TimeUnit};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;

    /// Writes a shard the way BigQuery exports one: Snappy-compressed, timestamps as
    /// UTC microseconds, NUMERIC as `DECIMAL(38, 9)`, columns in the source table's order.
    fn write_bigquery_like_shard(path: &std::path::Path, rows: &[(i64, Option<&str>)]) {
        let n = rows.len();
        let cnt = Int64Array::from_iter_values(rows.iter().map(|r| r.0));
        let status = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let day = TimestampMicrosecondArray::from(
            rows.iter()
                .map(|r| r.1.map(|_| 1_767_312_000_000_000 + r.0 * 86_400_000_000))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC");
        let amount = Decimal128Array::from(
            rows.iter()
                .map(|r| r.1.map(|_| r.0 as i128 * 12_500_000_000))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(38, 9)
        .unwrap();
        let ratio = Float64Array::from(
            rows.iter()
                .map(|r| r.1.map(|_| r.0 as f64 / 4.0))
                .collect::<Vec<_>>(),
        );
        let active = BooleanArray::from(
            rows.iter()
                .map(|r| r.1.map(|_| r.0 % 2 == 0))
                .collect::<Vec<_>>(),
        );
        let not_requested = Int64Array::from(vec![7; n]);
        let batch = RecordBatch::try_from_iter(vec![
            ("not_requested", Arc::new(not_requested) as ArrayRef),
            ("active", Arc::new(active) as ArrayRef),
            ("ratio", Arc::new(ratio) as ArrayRef),
            ("amount", Arc::new(amount) as ArrayRef),
            ("cnt", Arc::new(cnt) as ArrayRef),
            ("day", Arc::new(day) as ArrayRef),
            ("status", Arc::new(status) as ArrayRef),
        ])
        .unwrap();
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// The whole path a pre-aggregation build takes inside Cube Store: the statement the
    /// driver sends, the import job, typed conversion, and a query over the result.
    #[tokio::test]
    async fn imports_parquet_locations_through_create_table() {
        Config::test("imports_parquet_locations_through_create_table")
            .start_test(async move |services| {
                let service = services.sql_service;
                let dir = tempfile::tempdir().unwrap();
                let shards = [
                    dir.path().join("part-000000000000.parquet"),
                    dir.path().join("part-000000000001.parquet"),
                ];
                write_bigquery_like_shard(&shards[0], &[(1, Some("new")), (2, None)]);
                write_bigquery_like_shard(&shards[1], &[(3, Some("shipped"))]);

                service.exec_query("CREATE SCHEMA foo").await?;
                service
                    .exec_query(&format!(
                        "CREATE TABLE foo.orders \
                         (status varchar(255), day timestamp, cnt bigint, amount decimal(18, 5), ratio double, active boolean) \
                         WITH (input_format = 'parquet') LOCATION '{}', '{}'",
                        shards[0].to_string_lossy(),
                        shards[1].to_string_lossy()
                    ))
                    .await?;

                let result = service
                    .exec_query(
                        "SELECT status, day, cnt, amount, ratio, active FROM foo.orders ORDER BY cnt",
                    )
                    .await?
                    .collect()
                    .await?;
                let day = |n: i64| {
                    TableValue::Timestamp(TimestampValue::new(
                        (1_767_312_000 + n * 86_400) * 1_000_000_000,
                    ))
                };
                assert_eq!(
                    result.get_rows(),
                    &vec![
                        Row::new(vec![
                            TableValue::String("new".to_string()),
                            day(1),
                            TableValue::Int(1),
                            TableValue::Decimal(Decimal::new(1_250_000)),
                            TableValue::Float(0.25.into()),
                            TableValue::Boolean(false),
                        ]),
                        Row::new(vec![
                            TableValue::Null,
                            TableValue::Null,
                            TableValue::Int(2),
                            TableValue::Null,
                            TableValue::Null,
                            TableValue::Null,
                        ]),
                        Row::new(vec![
                            TableValue::String("shipped".to_string()),
                            day(3),
                            TableValue::Int(3),
                            TableValue::Decimal(Decimal::new(3_750_000)),
                            TableValue::Float(0.75.into()),
                            TableValue::Boolean(false),
                        ]),
                    ]
                );

                // A column the export does not contain fails the build instead of loading nulls,
                // and the failed table is not left behind.
                let err = service
                    .exec_query(&format!(
                        "CREATE TABLE foo.orders_wrong (cnt bigint, renamed bigint) \
                         WITH (input_format = 'parquet') LOCATION '{}'",
                        shards[0].to_string_lossy()
                    ))
                    .await
                    .unwrap_err();
                assert!(err.message.contains("'renamed'"), "{}", err.message);
                let tables = service
                    .exec_query(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'foo'",
                    )
                    .await?
                    .collect()
                    .await?;
                assert_eq!(
                    tables.get_rows(),
                    &vec![Row::new(vec![TableValue::String("orders".to_string())])]
                );
                Ok::<(), CubeError>(())
            })
            .await;
    }

    #[test]
    fn parquet_round_trip_preserves_decimal_timestamp_binary_and_nulls() {
        let decimals = Decimal128Array::from(vec![Some(-12), None, Some(0)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let timestamp = TimestampNanosecondArray::from(vec![Some(-500_000_000), None, Some(0)]);
        let binary = BinaryArray::from(vec![Some(&[0u8, 255][..]), None, Some(&b""[..])]);
        let batch = RecordBatch::try_from_iter(vec![
            ("amount", Arc::new(decimals) as ArrayRef),
            ("time", Arc::new(timestamp) as ArrayRef),
            ("bytes", Arc::new(binary) as ArrayRef),
        ])
        .unwrap();
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let batch = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let columns = vec![
            Column::new("time".into(), ColumnType::Timestamp, 0),
            Column::new(
                "amount".into(),
                ColumnType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                1,
            ),
            Column::new("bytes".into(), ColumnType::Bytes, 2),
        ];
        let converted = convert_batch(&batch, &columns).unwrap();
        assert_eq!(
            converted[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            converted[0]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            -500_000
        );
        assert_eq!(
            converted[1]
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -12
        );
        assert_eq!(
            converted[2]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[0, 255]
        );
        for array in converted {
            assert!(array.is_null(1));
        }
    }

    #[test]
    fn missing_duplicate_nested_and_invalid_values_fail_instead_of_becoming_null() {
        let source = Arc::new(StringArray::from(vec!["not-an-integer"])) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![("id", source.clone())]).unwrap();
        assert!(
            convert_batch(&batch, &[Column::new("missing".into(), ColumnType::Int, 0)]).is_err()
        );
        assert!(convert_batch(&batch, &[Column::new("id".into(), ColumnType::Int, 0)]).is_err());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
        ]));
        let duplicate = RecordBatch::try_new(schema, vec![source.clone(), source]).unwrap();
        assert!(convert_batch(
            &duplicate,
            &[Column::new("id".into(), ColumnType::String, 0)]
        )
        .is_err());
        let nested =
            ListArray::from_iter_primitive::<datafusion::arrow::datatypes::Int32Type, _, _>(vec![
                Some(vec![Some(1)]),
            ]);
        let batch = RecordBatch::try_from_iter(vec![("id", Arc::new(nested) as ArrayRef)]).unwrap();
        assert!(convert_batch(&batch, &[Column::new("id".into(), ColumnType::String, 0)]).is_err());
    }
}
