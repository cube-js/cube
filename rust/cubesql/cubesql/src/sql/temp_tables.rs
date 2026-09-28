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

        self.reserve(temp_table.size)?;

        guard.insert(name.to_string(), Arc::new(temp_table));
        Ok(())
    }

    /// Append data to an existing temporary table, returning the number of rows added.
    /// Batches must match the schema of the table.
    pub fn append(&self, name: &str, batches: Vec<RecordBatch>) -> Result<usize, CubeError> {
        let mut guard = self
            .temp_tables
            .write()
            .expect("failed to unlock temp tables for writing");

        let Some(temp_table) = guard.get(name).cloned() else {
            return Err(CubeError::user(format!(
                "table \"{}\" does not exist",
                name
            )));
        };

        if batches.is_empty() {
            return Ok(0);
        }

        for batch in batches.iter() {
            if batch.schema().fields() != temp_table.schema.fields() {
                return Err(CubeError::internal(format!(
                    "data being appended to temporary table \"{}\" does not match its schema",
                    name
                )));
            }
        }

        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        let appended_size = batches_size(&batches);
        self.reserve(appended_size)?;

        let mut record_batch = temp_table.record_batch.clone();
        record_batch.push(batches);

        guard.insert(
            name.to_string(),
            Arc::new(TempTable {
                schema: Arc::clone(&temp_table.schema),
                record_batch,
                size: temp_table.size + appended_size,
            }),
        );

        Ok(rows)
    }

    /// Account for `size` more bytes of temporary table data, both for this session
    /// and for the server as a whole.
    fn reserve(&self, size: usize) -> Result<(), CubeError> {
        let session_manager = self
            .session_manager
            .upgrade()
            .ok_or_else(|| CubeError::internal("session manager is unavailable".to_string()))?;

        let size_session_limit = Self::session_memory_limit() / 1024 / 1024;

        let size_total_limit = env::var("CUBESQL_TEMP_TABLE_TOTAL_MEM")
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(100); // in MiB

        let limit_reached = || {
            CubeError::user(format!(
                "temporary table memory limit reached ({} MiB session, {} MiB total)",
                size_session_limit, size_total_limit,
            ))
        };

        // The two counters are taken one after the other, and not one inside the
        // update of the other: a compare-and-swap retries its closure, which would
        // then count the same bytes towards the server total more than once
        self.cached_size
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current_size| {
                match current_size + size > size_session_limit * 1024 * 1024 {
                    true => None,
                    false => Some(current_size + size),
                }
            })
            .map_err(|_| limit_reached())?;

        let total = session_manager.temp_table_size.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current_size| match current_size + size > size_total_limit * 1024 * 1024 {
                true => None,
                false => Some(current_size + size),
            },
        );

        if total.is_err() {
            self.cached_size.fetch_sub(size, Ordering::SeqCst);

            return Err(limit_reached());
        }

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

    /// How many bytes of temporary table data one session may hold.
    pub fn session_memory_limit() -> usize {
        env::var("CUBESQL_TEMP_TABLE_SESSION_MEM")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10) // in MiB
            * 1024
            * 1024
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
        Self::from_arrow_schema(df_schema_to_arrow_schema(&schema), record_batch)
    }

    pub fn from_arrow_schema(schema: SchemaRef, record_batch: Vec<Vec<RecordBatch>>) -> Self {
        let size = record_batch
            .iter()
            .map(|batches| batches_size(batches))
            .sum();
        Self {
            schema,
            record_batch,
            size,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn batches_size(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(|batch| {
            batch
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>()
        })
        .sum()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_needs_the_table_to_be_there() {
        let manager = TempTableManager::new(Weak::new());

        // A copy which carried no row still has to find the table it loads into,
        // otherwise a table dropped mid-copy would look like a copy of zero rows
        let err = manager
            .append("gone", vec![])
            .expect_err("appending to a missing table must fail");

        assert_eq!(err.message, "table \"gone\" does not exist");
    }
}
