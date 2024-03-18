use std::{
    any::Any,
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

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

use super::SessionManager;

#[derive(Debug)]
pub struct TempTableManager {
    temp_tables: RWLockSync<HashMap<String, Arc<TempTable>>>,
    cached_size: AtomicUsize,
    // Backref
    session_manager: Weak<SessionManager>,
}

impl TempTableManager {
    pub fn new(session_manager: Weak<SessionManager>) -> Self {
        Self {
            temp_tables: RWLockSync::new(HashMap::new()),
            cached_size: AtomicUsize::new(0),
            session_manager,
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
        let session_manager = self
            .session_manager
            .upgrade()
            .ok_or_else(|| CubeError::internal("session manager is unavailable".to_string()))?;

        let size_session_limit = env::var("CUBESQL_TEMP_TABLE_SESSION_MEM")
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(10); // in MiB

        let size_total_limit = env::var("CUBESQL_TEMP_TABLE_TOTAL_MEM")
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(100); // in MiB

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

        self.cached_size
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current_size| {
                if current_size + temp_table.size > size_session_limit * 1024 * 1024 {
                    return None;
                }
                session_manager
                    .temp_table_size
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current_size| {
                        if current_size + temp_table.size > size_total_limit * 1024 * 1024 {
                            return None;
                        }
                        Some(current_size + temp_table.size)
                    })
                    .ok()?;
                Some(current_size + temp_table.size)
            })
            .map_err(|_| {
                CubeError::user(format!(
                    "temporary table memory limit reached ({} MiB session, {} MiB total)",
                    size_session_limit, size_total_limit,
                ))
            })?;

        guard.insert(name.to_string(), Arc::new(temp_table));
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), CubeError> {
        let session_manager = self
            .session_manager
            .upgrade()
            .ok_or_else(|| CubeError::internal("session manager is unavailable".to_string()))?;

        let Some(temp_table) = ({
            let mut guard = self
                .temp_tables
                .write()
                .expect("failed to unlock temp tables for writing");

            guard.remove(name)
        }) else {
            return Err(CubeError::user(format!(
                "table \"{}\" does not exist",
                name
            )));
        };

        self.cached_size
            .fetch_sub(temp_table.size, Ordering::SeqCst);
        session_manager
            .temp_table_size
            .fetch_sub(temp_table.size, Ordering::SeqCst);

        Ok(())
    }

    pub fn physical_size(&self) -> usize {
        self.cached_size.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct TempTable {
    schema: SchemaRef,
    record_batch: Vec<Vec<RecordBatch>>,
    size: usize,
}

impl TempTable {
    pub fn new(schema: DFSchemaRef, record_batch: Vec<Vec<RecordBatch>>) -> Self {
        let arrow_schema = df_schema_to_arrow_schema(&schema);
        let size = record_batch
            .iter()
            .map(|record_batch| {
                record_batch
                    .iter()
                    .map(|record_batch| {
                        record_batch
                            .columns()
                            .iter()
                            .map(|column| column.get_array_memory_size())
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum();
        Self {
            schema: arrow_schema,
            record_batch,
            size,
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
