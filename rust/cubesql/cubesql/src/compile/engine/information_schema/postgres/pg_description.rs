use std::{any::Any, convert::TryFrom, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::{
    compile::engine::information_schema::postgres::PG_CLASS_CLASS_OID, transport::CubeMetaTable,
};

/// See https://www.postgresql.org/docs/16/catalog-pg-description.html
struct PgCatalogDescriptionBuilder {
    /// The OID of the object this description pertains to
    objoid: UInt32Builder,
    /// The OID of the system catalog this object appears in
    classoid: UInt32Builder,
    /// For a comment on a table column, this is the column number (the objoid and classoid refer to the table itself). For all other object types, this column is zero.
    objsubid: Int32Builder,
    /// Arbitrary text that serves as the description of this object
    description: StringBuilder,
}

impl PgCatalogDescriptionBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            objoid: UInt32Builder::new(capacity),
            classoid: UInt32Builder::new(capacity),
            objsubid: Int32Builder::new(capacity),
            description: StringBuilder::new(capacity),
        }
    }

    fn add_table(&mut self, table_oid: u32, description: impl AsRef<str>) {
        self.objoid.append_value(table_oid).unwrap();
        self.classoid.append_value(PG_CLASS_CLASS_OID).unwrap();
        self.objsubid.append_value(0).unwrap();
        self.description.append_value(description).unwrap();
    }

    fn add_column(&mut self, table_oid: u32, column_idx: usize, description: impl AsRef<str>) {
        self.objoid.append_value(table_oid).unwrap();
        self.classoid.append_value(PG_CLASS_CLASS_OID).unwrap();
        // Column subids starts with 1
        self.objsubid
            .append_value(i32::try_from(column_idx).unwrap() + 1)
            .unwrap();
        self.description.append_value(description).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.objoid.finish()),
            Arc::new(self.classoid.finish()),
            Arc::new(self.objsubid.finish()),
            Arc::new(self.description.finish()),
        ];

        columns
    }
}

pub struct PgCatalogDescriptionProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogDescriptionProvider {
    pub fn new(tables: &[CubeMetaTable]) -> Self {
        let mut builder = PgCatalogDescriptionBuilder::new();

        for table in tables {
            if let Some(description) = &table.description {
                builder.add_table(table.oid, description);
            }

            for (idx, column) in table.columns.iter().enumerate() {
                if let Some(description) = &column.description {
                    builder.add_column(table.oid, idx, description);
                }
            }
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogDescriptionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("classoid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
