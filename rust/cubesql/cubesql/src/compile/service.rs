use std::sync::Arc;

use async_trait::async_trait;

use super::{convert_sql_to_cube_query, CompilationResult, QueryPlan};
use crate::{sql::Session, transport::MetaContext, CubeError};

#[async_trait]
pub trait SqlService: Send + Sync {
    async fn plan(
        &self,
        query: &String,
        meta: Arc<MetaContext>,
        session: Arc<Session>,
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
    ) -> CompilationResult<QueryPlan> {
        convert_sql_to_cube_query(&query, meta, session).await
    }
}
