use std::sync::Arc;

use async_trait::async_trait;

use crate::{sql::Session, telemetry::ContextLogger, CubeError};

use super::{convert_sql_to_cube_query, CompilationResult, MetaContext, QueryPlan};

#[async_trait]
pub trait SqlService: Send + Sync {
    async fn plan(
        &self,
        query: &String,
        meta: Arc<MetaContext>,
        session: Arc<Session>,
        logger: Arc<dyn ContextLogger>,
    ) -> CompilationResult<QueryPlan>;
}

pub struct SqlAuthDefaultImpl {}

crate::di_service!(SqlAuthDefaultImpl, [SqlService]);

#[async_trait]
impl SqlService for SqlAuthDefaultImpl {
    async fn plan(
        &self,
        query: &String,
        meta: Arc<MetaContext>,
        session: Arc<Session>,
        logger: Arc<dyn ContextLogger>,
    ) -> CompilationResult<QueryPlan> {
        convert_sql_to_cube_query(&query, meta, session, logger)
    }
}
