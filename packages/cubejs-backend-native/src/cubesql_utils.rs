use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use cubesql::compile::DatabaseProtocol;
use cubesql::config::ConfigObj;
use cubesql::sql::{Session, SessionManager};
use cubesql::CubeError;

use crate::auth::NativeAuthContext;
use crate::config::NodeCubeServices;

pub async fn create_session(
    services: &NodeCubeServices,
    native_auth_ctx: Arc<NativeAuthContext>,
) -> Result<Arc<Session>, CubeError> {
    let config = services
        .injector()
        .get_service_typed::<dyn ConfigObj>()
        .await;

    let session_manager = services
        .injector()
        .get_service_typed::<SessionManager>()
        .await;

    let (host, port) = match SocketAddr::from_str(
        config
            .postgres_bind_address()
            .as_deref()
            .unwrap_or("127.0.0.1:15432"),
    ) {
        Ok(addr) => (addr.ip().to_string(), addr.port()),
        Err(e) => {
            return Err(CubeError::internal(format!(
                "Failed to parse postgres_bind_address: {}",
                e
            )))
        }
    };

    let session = session_manager
        .create_session(DatabaseProtocol::PostgreSQL, host, port, None)
        .await?;

    session
        .state
        .set_auth_context(Some(native_auth_ctx.clone()));

    Ok(session)
}
