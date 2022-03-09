use std::sync::Arc;

use async_trait::async_trait;

use crate::mysql::session::SessionState;
use crate::transport::TransportService;
use crate::CubeError;

use super::{convert_sql_to_cube_query, CompilationResult, MetaContext, QueryPlan};

#[async_trait]
pub trait SqlService: Send + Sync {
    async fn plan(
        &self,
        query: &String,
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
    ) -> CompilationResult<QueryPlan>;
}

pub struct SqlAuthDefaultImpl {
    transport: Arc<dyn TransportService>,
}

crate::di_service!(SqlAuthDefaultImpl, [SqlService]);

#[async_trait]
impl SqlService for SqlAuthDefaultImpl {
    async fn plan(
        &self,
        query: &String,
        state: Arc<SessionState>,
        meta: Arc<MetaContext>,
    ) -> CompilationResult<QueryPlan> {
        convert_sql_to_cube_query(&query, meta, state, self.transport.clone())
    }
}
