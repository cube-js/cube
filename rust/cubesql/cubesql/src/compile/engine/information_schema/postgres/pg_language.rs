use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgLanguage {
    oid: u32,
    lanname: &'static str,
    lanowner: u32,
    lanispl: bool,
    lanpltrusted: bool,
    lanplcallfoid: u32,
    laninline: u32,
    lanvalidator: u32,
}

struct PgCatalogLanguageBuilder {
    oid: UInt32Builder,
    lanname: StringBuilder,
    lanowner: UInt32Builder,
    lanispl: BooleanBuilder,
    lanpltrusted: BooleanBuilder,
    lanplcallfoid: UInt32Builder,
    laninline: UInt32Builder,
    lanvalidator: UInt32Builder,
    lanacl: ListBuilder<StringBuilder>,
    xmin: UInt32Builder,
}

impl PgCatalogLanguageBuilder {
    fn new() -> Self {
        let capacity = 3;

        Self {
            oid: UInt32Builder::new(capacity),
            lanname: StringBuilder::new(capacity),
            lanowner: UInt32Builder::new(capacity),
            lanispl: BooleanBuilder::new(capacity),
            lanpltrusted: BooleanBuilder::new(capacity),
            lanplcallfoid: UInt32Builder::new(capacity),
            laninline: UInt32Builder::new(capacity),
            lanvalidator: UInt32Builder::new(capacity),
            lanacl: ListBuilder::new(StringBuilder::new(capacity)),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn add_language(&mut self, lan: &PgLanguage) {
        self.oid.append_value(lan.oid).unwrap();
        self.lanname.append_value(lan.lanname).unwrap();
        self.lanowner.append_value(lan.lanowner).unwrap();
        self.lanispl.append_value(lan.lanispl).unwrap();
        self.lanpltrusted.append_value(lan.lanpltrusted).unwrap();
        self.lanplcallfoid.append_value(lan.lanplcallfoid).unwrap();
        self.laninline.append_value(lan.laninline).unwrap();
        self.lanvalidator.append_value(lan.lanvalidator).unwrap();
        self.lanacl.append(false).unwrap();
        self.xmin.append_value(1).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.lanname.finish()),
            Arc::new(self.lanowner.finish()),
            Arc::new(self.lanispl.finish()),
            Arc::new(self.lanpltrusted.finish()),
            Arc::new(self.lanplcallfoid.finish()),
            Arc::new(self.laninline.finish()),
            Arc::new(self.lanvalidator.finish()),
            Arc::new(self.lanacl.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogLanguageProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-language.html
impl PgCatalogLanguageProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogLanguageBuilder::new();
        builder.add_language(&PgLanguage {
            oid: 12,
            lanname: "internal",
            lanowner: 10,
            lanispl: false,
            lanpltrusted: false,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 2246,
        });
        builder.add_language(&PgLanguage {
            oid: 13,
            lanname: "c",
            lanowner: 10,
            lanispl: false,
            lanpltrusted: false,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 2247,
        });
        builder.add_language(&PgLanguage {
            oid: 14,
            lanname: "sql",
            lanowner: 10,
            lanispl: false,
            lanpltrusted: true,
            lanplcallfoid: 0,
            laninline: 0,
            lanvalidator: 2248,
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogLanguageProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("lanname", DataType::Utf8, false),
            Field::new("lanowner", DataType::UInt32, false),
            Field::new("lanispl", DataType::Boolean, false),
            Field::new("lanpltrusted", DataType::Boolean, false),
            Field::new("lanplcallfoid", DataType::UInt32, false),
            Field::new("laninline", DataType::UInt32, false),
            Field::new("lanvalidator", DataType::UInt32, false),
            Field::new(
                "lanacl",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("xmin", DataType::UInt32, false),
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
