use crate::sql::parser::{CubeStoreParser, Statement};
use std::sync::Arc;

use crate::cachestore::{MockCacheStore, MockRocksCacheStore, MockWrapperCacheStore};
use crate::metastore::MockWrapperMetaStore;
use crate::queryplanner::MetaStoreSchemaProvider;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::LogicalPlan;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::SqlToRel;

pub fn initial_plan(s: &str, ctx: MetaStoreSchemaProvider) -> LogicalPlan {
    let statement = match CubeStoreParser::new(s).unwrap().parse_statement().unwrap() {
        Statement::Statement(s) => s,
        other => panic!("not a statement, actual {:?}", other),
    };

    let plan = SqlToRel::new(&ctx)
        .statement_to_plan(&DFStatement::Statement(statement))
        .unwrap();
    ExecutionContext::new().optimize(&plan).unwrap()
}

pub fn get_test_execution_ctx() -> MetaStoreSchemaProvider {
    MetaStoreSchemaProvider::new(
        Arc::new(vec![]),
        Arc::new(MockWrapperMetaStore),
        Arc::new(MockWrapperCacheStore),
        &vec![],
    )
}
