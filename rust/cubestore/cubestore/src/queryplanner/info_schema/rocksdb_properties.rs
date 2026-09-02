use crate::metastore::RocksPropertyRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub struct RocksDBPropertiesTableDef {
    for_cachestore: bool,
}

impl RocksDBPropertiesTableDef {
    pub fn new_cachestore() -> Self {
        Self {
            for_cachestore: true,
        }
    }

    pub fn new_metastore() -> Self {
        Self {
            for_cachestore: false,
        }
    }
}

#[async_trait]
impl InfoSchemaTableDef for RocksDBPropertiesTableDef {
    type T = RocksPropertyRow;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(if self.for_cachestore {
            ctx.cache_store.rocksdb_properties().await?
        } else {
            ctx.meta_store.rocksdb_properties().await?
        })
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut key_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut value_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);

        for row in rows.into_iter() {
            key_builder.append_value(&row.key);
            match row.value.as_ref() {
                Some(v) => value_builder.append_value(v),
                None => value_builder.append_null(),
            }
        }

        vec![
            Arc::new(key_builder.finish()),
            Arc::new(value_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(RocksDBPropertiesTableDef);
