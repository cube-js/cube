use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgRange {
    rngtypid: u32,
    rngsubtype: u32,
    rngmultitypid: u32,
    rngcollation: u32,
    rngsubopc: u32,
    rngcanonical: &'static str,
    rngsubdiff: &'static str,
}

struct PgCatalogRangeBuilder {
    rngtypid: UInt32Builder,
    rngsubtype: UInt32Builder,
    rngmultitypid: UInt32Builder,
    rngcollation: UInt32Builder,
    rngsubopc: UInt32Builder,
    rngcanonical: StringBuilder,
    rngsubdiff: StringBuilder,
}

impl PgCatalogRangeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            rngtypid: UInt32Builder::new(capacity),
            rngsubtype: UInt32Builder::new(capacity),
            rngmultitypid: UInt32Builder::new(capacity),
            rngcollation: UInt32Builder::new(capacity),
            rngsubopc: UInt32Builder::new(capacity),
            rngcanonical: StringBuilder::new(capacity),
            rngsubdiff: StringBuilder::new(capacity),
        }
    }

    fn add_range(&mut self, range: &PgRange) {
        self.rngtypid.append_value(range.rngtypid).unwrap();
        self.rngsubtype.append_value(range.rngsubtype).unwrap();
        self.rngmultitypid
            .append_value(range.rngmultitypid)
            .unwrap();
        self.rngcollation.append_value(range.rngcollation).unwrap();
        self.rngsubopc.append_value(range.rngsubopc).unwrap();
        self.rngcanonical.append_value(range.rngcanonical).unwrap();
        self.rngsubdiff.append_value(range.rngsubdiff).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.rngtypid.finish()));
        columns.push(Arc::new(self.rngsubtype.finish()));
        columns.push(Arc::new(self.rngmultitypid.finish()));
        columns.push(Arc::new(self.rngcollation.finish()));
        columns.push(Arc::new(self.rngsubopc.finish()));
        columns.push(Arc::new(self.rngcanonical.finish()));
        columns.push(Arc::new(self.rngsubdiff.finish()));

        columns
    }
}

pub struct PgCatalogRangeProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogRangeProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogRangeBuilder::new();
        builder.add_range(&PgRange {
            rngtypid: 3904,
            rngsubtype: 23,
            rngmultitypid: 4451,
            rngcollation: 0,
            rngsubopc: 1978,
            rngcanonical: "int4range_canonical",
            rngsubdiff: "int4range_subdiff",
        });
        builder.add_range(&PgRange {
            rngtypid: 3906,
            rngsubtype: 1700,
            rngmultitypid: 4532,
            rngcollation: 0,
            rngsubopc: 3125,
            rngcanonical: "-",
            rngsubdiff: "numrange_subdiff",
        });
        builder.add_range(&PgRange {
            rngtypid: 3908,
            rngsubtype: 1114,
            rngmultitypid: 4533,
            rngcollation: 0,
            rngsubopc: 3128,
            rngcanonical: "-",
            rngsubdiff: "tsrange_subdiff",
        });
        builder.add_range(&PgRange {
            rngtypid: 3910,
            rngsubtype: 1184,
            rngmultitypid: 4534,
            rngcollation: 0,
            rngsubopc: 3127,
            rngcanonical: "-",
            rngsubdiff: "tstzrange_subdiff",
        });
        builder.add_range(&PgRange {
            rngtypid: 3912,
            rngsubtype: 1082,
            rngmultitypid: 4535,
            rngcollation: 0,
            rngsubopc: 3122,
            rngcanonical: "daterange_canonical",
            rngsubdiff: "daterange_subdiff",
        });
        builder.add_range(&PgRange {
            rngtypid: 3926,
            rngsubtype: 20,
            rngmultitypid: 4536,
            rngcollation: 0,
            rngsubopc: 3124,
            rngcanonical: "int8range_canonical",
            rngsubdiff: "int8range_subdiff",
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogRangeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("rngtypid", DataType::UInt32, false),
            Field::new("rngsubtype", DataType::UInt32, false),
            Field::new("rngmultitypid", DataType::UInt32, false),
            Field::new("rngcollation", DataType::UInt32, false),
            Field::new("rngsubopc", DataType::UInt32, false),
            Field::new("rngcanonical", DataType::Utf8, false),
            Field::new("rngsubdiff", DataType::Utf8, false),
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
