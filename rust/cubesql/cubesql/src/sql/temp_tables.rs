use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    error::DataFusionError,
    logical_plan::{DFSchema, DFSchemaRef, Expr},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::{CubeError, RWLockSync};

#[derive(Debug)]
pub struct TempTableManager {
    temp_tables: RWLockSync<HashMap<String, Arc<TempTable>>>,
}

impl TempTableManager {
    pub fn new() -> Self {
        Self {
            temp_tables: RWLockSync::new(HashMap::new()),
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<TempTable>> {
        self.temp_tables
            .read()
            .expect("failed to unlock temp tables for reading")
            .get(name)
            .cloned()
    }

    pub fn has(&self, name: &str) -> bool {
        self.temp_tables
            .read()
            .expect("failed to unlock temp tables for reading")
            .contains_key(name)
    }

    pub fn save(&self, name: &str, temp_table: TempTable) -> Result<(), CubeError> {
        let mut guard = self
            .temp_tables
            .write()
            .expect("failed to unlock temp tables for writing");

        if guard.contains_key(name) {
            return Err(CubeError::user(format!(
                "relation \"{}\" already exists",
                name
            )));
        }

        guard.insert(name.to_string(), Arc::new(temp_table));
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), CubeError> {
        let mut guard = self
            .temp_tables
            .write()
            .expect("failed to unlock temp tables for writing");

        if guard.remove(name).is_none() {
            return Err(CubeError::user(format!(
                "table \"{}\" does not exist",
                name
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TempTable {
    schema: SchemaRef,
    record_batch: Vec<Vec<RecordBatch>>,
}

impl TempTable {
    pub fn new(schema: DFSchemaRef, record_batch: Vec<Vec<RecordBatch>>) -> Self {
        let arrow_schema = df_schema_to_arrow_schema(&schema);
        Self {
            schema: arrow_schema,
            record_batch,
        }
    }
}

fn df_schema_to_arrow_schema(df_schema: &DFSchema) -> SchemaRef {
    let arrow_schema = Schema::new_with_metadata(
        df_schema
            .fields()
            .iter()
            .map(|f| f.field().clone())
            .collect(),
        df_schema.metadata().clone(),
    );
    Arc::new(arrow_schema)
}

#[derive(Debug, Clone)]
pub struct TempTableProvider {
    name: String,
    temp_table: Arc<TempTable>,
}

impl TempTableProvider {
    pub fn new(name: String, temp_table: Arc<TempTable>) -> Self {
        Self { name, temp_table }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl TableProvider for TempTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.temp_table.schema)
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(MemoryExec::try_new(
            &self.temp_table.record_batch,
            self.schema(),
            projection.clone(),
        )?))
    }
}
