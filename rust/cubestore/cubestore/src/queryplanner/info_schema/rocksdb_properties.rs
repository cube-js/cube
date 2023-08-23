use crate::metastore::RocksPropertyRow;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
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
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(if self.for_cachestore {
            ctx.cache_store.rocksdb_properties().await?
        } else {
            ctx.meta_store.rocksdb_properties().await?
        }))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|items| {
                Arc::new(StringArray::from_iter_values(
                    items.iter().map(|row| row.key.clone()),
                ))
            }),
            Box::new(|items| {
                Arc::new(StringArray::from_iter(
                    items.iter().map(|row| row.value.as_ref()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(RocksDBPropertiesTableDef);
