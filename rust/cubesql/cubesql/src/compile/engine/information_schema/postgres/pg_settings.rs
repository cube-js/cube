use std::{any::Any, sync::Arc};

use crate::compile::DatabaseVariables;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, BooleanBuilder, Int32Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

pub struct PgCatalogSettingsProvider {
    vars: DatabaseVariables,
}

impl PgCatalogSettingsProvider {
    pub fn new(vars: DatabaseVariables) -> Self {
        Self { vars }
    }
}

#[async_trait]
impl TableProvider for PgCatalogSettingsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("setting", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
            Field::new("short_desc", DataType::Utf8, true),
            Field::new("extra_desc", DataType::Utf8, true),
            Field::new("context", DataType::Utf8, true),
            Field::new("vartype", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("min_val", DataType::Utf8, true),
            Field::new("max_val", DataType::Utf8, true),
            Field::new("enumvals", DataType::Utf8, true),
            Field::new("boot_val", DataType::Utf8, true),
            Field::new("reset_val", DataType::Utf8, true),
            Field::new("sourcefile", DataType::Utf8, true),
            Field::new("sourceline", DataType::Int32, true),
            Field::new("pending_restart", DataType::Boolean, true),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut names = StringBuilder::with_capacity(100, 100);
        let mut settings = StringBuilder::with_capacity(100, 100);
        let mut units = StringBuilder::with_capacity(100, 100);
        let mut categories = StringBuilder::with_capacity(100, 100);
        let mut short_descs = StringBuilder::with_capacity(100, 100);
        let mut extra_descs = StringBuilder::with_capacity(100, 100);
        let mut contexts = StringBuilder::with_capacity(100, 100);
        let mut vartypes = StringBuilder::with_capacity(100, 100);
        let mut sources = StringBuilder::with_capacity(100, 100);
        let mut min_vals = StringBuilder::with_capacity(100, 100);
        let mut max_vals = StringBuilder::with_capacity(100, 100);
        let mut enumvals = StringBuilder::with_capacity(100, 100);
        let mut boot_vals = StringBuilder::with_capacity(100, 100);
        let mut reset_vals = StringBuilder::with_capacity(100, 100);
        let mut sourcefiles = StringBuilder::with_capacity(100, 100);
        let mut sourceline = Int32Builder::with_capacity(100);
        let mut pending_restarts = BooleanBuilder::with_capacity(100);

        for (key, variable) in self.vars.iter() {
            names.append_value(key.clone());
            settings.append_value(variable.value.to_string());
            units.append_null();

            categories.append_null();
            short_descs.append_null();
            extra_descs.append_null();
            contexts.append_null();
            vartypes.append_null();
            sources.append_null();
            min_vals.append_null();
            max_vals.append_null();
            enumvals.append_null();
            boot_vals.append_null();
            reset_vals.append_null();
            sourcefiles.append_null();
            sourceline.append_null();
            pending_restarts.append_null();
        }

        let data: Vec<Arc<dyn Array>> = vec![
            Arc::new(names.finish()),
            Arc::new(settings.finish()),
            Arc::new(units.finish()),
            Arc::new(categories.finish()),
            Arc::new(short_descs.finish()),
            Arc::new(extra_descs.finish()),
            Arc::new(contexts.finish()),
            Arc::new(vartypes.finish()),
            Arc::new(sources.finish()),
            Arc::new(min_vals.finish()),
            Arc::new(max_vals.finish()),
            Arc::new(enumvals.finish()),
            Arc::new(boot_vals.finish()),
            Arc::new(reset_vals.finish()),
            Arc::new(sourcefiles.finish()),
            Arc::new(sourceline.finish()),
            Arc::new(pending_restarts.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema(), data)?;

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
