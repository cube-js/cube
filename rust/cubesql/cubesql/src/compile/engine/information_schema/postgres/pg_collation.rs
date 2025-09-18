use std::sync::Arc;

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider},
    error::Result,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::compile::engine::information_schema::postgres::PG_NAMESPACE_CATALOG_OID;

struct PgCollation {
    oid: u32,
    collname: &'static str,
    collnamespace: u32,
    collowner: u32,
    collprovider: String,
    collisdeterministic: bool,
    collencoding: i32,
    collcollate: Option<String>,
    collctype: Option<String>,
    // Column `colliculocale` is renamed to `colllocale` since PostgreSQL 17.
    colllocale: Option<String>,
    collicurules: Option<String>,
    collversion: Option<String>,
}

struct PgCatalogCollationBuilder {
    oid: UInt32Builder,
    collname: StringBuilder,
    collnamespace: UInt32Builder,
    collowner: UInt32Builder,
    collprovider: StringBuilder,
    collisdeterministic: BooleanBuilder,
    collencoding: Int32Builder,
    collcollate: StringBuilder,
    collctype: StringBuilder,
    // Column `colliculocale` is renamed to `colllocale` since PostgreSQL 17.
    // Support both columns for backward-compatibility.
    // Reference: https://pgpedia.info/p/pg_collation.html
    colllocale: StringBuilder,
    colliculocale: StringBuilder,
    collicurules: StringBuilder,
    collversion: StringBuilder,
}

impl PgCatalogCollationBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            oid: UInt32Builder::new(capacity),
            collname: StringBuilder::new(capacity),
            collnamespace: UInt32Builder::new(capacity),
            collowner: UInt32Builder::new(capacity),
            collprovider: StringBuilder::new(capacity),
            collisdeterministic: BooleanBuilder::new(capacity),
            collencoding: Int32Builder::new(capacity),
            collcollate: StringBuilder::new(capacity),
            collctype: StringBuilder::new(capacity),
            colllocale: StringBuilder::new(capacity),
            colliculocale: StringBuilder::new(capacity),
            collicurules: StringBuilder::new(capacity),
            collversion: StringBuilder::new(capacity),
        }
    }
    fn add_collation(&mut self, coll: &PgCollation) {
        self.oid.append_value(coll.oid).unwrap();
        self.collname.append_value(coll.collname).unwrap();
        self.collnamespace.append_value(coll.collnamespace).unwrap();
        self.collowner.append_value(coll.collowner).unwrap();
        self.collprovider
            .append_value(coll.collprovider.clone())
            .unwrap();
        self.collisdeterministic
            .append_value(coll.collisdeterministic)
            .unwrap();
        self.collencoding.append_value(coll.collencoding).unwrap();
        self.collcollate
            .append_option(coll.collcollate.clone())
            .unwrap();
        self.collctype
            .append_option(coll.collctype.clone())
            .unwrap();
        self.colllocale
            .append_option(coll.colllocale.clone())
            .unwrap();
        // Column `colliculocale` is renamed to `colllocale` since PostgreSQL 17.
        self.colliculocale
            .append_option(coll.colllocale.clone())
            .unwrap();
        self.collicurules
            .append_option(coll.collicurules.clone())
            .unwrap();
        self.collversion
            .append_option(coll.collversion.clone())
            .unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.collname.finish()),
            Arc::new(self.collnamespace.finish()),
            Arc::new(self.collowner.finish()),
            Arc::new(self.collprovider.finish()),
            Arc::new(self.collisdeterministic.finish()),
            Arc::new(self.collencoding.finish()),
            Arc::new(self.collcollate.finish()),
            Arc::new(self.collctype.finish()),
            Arc::new(self.colllocale.finish()),
            Arc::new(self.colliculocale.finish()),
            Arc::new(self.collicurules.finish()),
            Arc::new(self.collversion.finish()),
        ];
        columns
    }
}

pub struct PgCatalogCollationProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogCollationProvider {
    pub fn new() -> Self {
        // See https://github.com/postgres/postgres/blob/REL_17_6/src/include/catalog/pg_collation.h
        let mut builder = PgCatalogCollationBuilder::new(6);

        // Initial contents of the pg_collation system catalog.
        // See https://github.com/postgres/postgres/blob/REL_17_6/src/include/catalog/pg_collation.dat

        // database's default collation
        builder.add_collation(&PgCollation {
            oid: 100,
            collname: "default",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "d".to_string(),
            collisdeterministic: true,
            collencoding: -1,
            collcollate: None,
            collctype: None,
            colllocale: None,
            collicurules: None,
            collversion: None,
        });
        // standard C collation
        builder.add_collation(&PgCollation {
            oid: 950,
            collname: "C",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "c".to_string(),
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some("C".to_string()),
            collctype: Some("C".to_string()),
            colllocale: None,
            collicurules: None,
            collversion: None,
        });
        // standard POSIX collation
        builder.add_collation(&PgCollation {
            oid: 951,
            collname: "POSIX",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "c".to_string(),
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some("POSIX".to_string()),
            collctype: Some("POSIX".to_string()),
            colllocale: None,
            collicurules: None,
            collversion: None,
        });
        // sorts by Unicode code point, C character semantics
        builder.add_collation(&PgCollation {
            oid: 962,
            collname: "ucs_basic",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "b".to_string(),
            collisdeterministic: true,
            collencoding: 6,
            collcollate: None,
            collctype: None,
            colllocale: Some("C".to_string()),
            collicurules: None,
            collversion: Some("1".to_string()),
        });
        // sorts using the Unicode Collation Algorithm with default settings
        builder.add_collation(&PgCollation {
            oid: 963,
            collname: "unicode",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "i".to_string(),
            collisdeterministic: true,
            collencoding: -1,
            collcollate: None,
            collctype: None,
            colllocale: Some("und".to_string()),
            collicurules: None,
            collversion: Some("153.128".to_string()),
        });
        // sorts by Unicode code point; Unicode and POSIX character semantics
        builder.add_collation(&PgCollation {
            oid: 811,
            collname: "pg_c_utf8",
            collnamespace: PG_NAMESPACE_CATALOG_OID,
            collowner: 10,
            collprovider: "b".to_string(),
            collisdeterministic: true,
            collencoding: 6,
            collcollate: None,
            collctype: None,
            colllocale: Some("C.UTF-8".to_string()),
            collicurules: None,
            collversion: Some("1".to_string()),
        });
        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogCollationProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("collname", DataType::Utf8, false),
            Field::new("collnamespace", DataType::UInt32, false),
            Field::new("collowner", DataType::UInt32, false),
            Field::new("collprovider", DataType::Utf8, false),
            Field::new("collisdeterministic", DataType::Boolean, false),
            Field::new("collencoding", DataType::Int32, false),
            Field::new("collcollate", DataType::Utf8, true),
            Field::new("collctype", DataType::Utf8, true),
            Field::new("colllocale", DataType::Utf8, true),
            Field::new("colliculocale", DataType::Utf8, true),
            Field::new("collicurules", DataType::Utf8, true),
            Field::new("collversion", DataType::Utf8, true),
        ]))
    }
    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
