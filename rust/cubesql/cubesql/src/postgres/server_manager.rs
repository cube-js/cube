use std::sync::Arc;

use crate::{
    sql_shared::SqlAuthService,
    transport::TransportService
};

pub struct ServerManager {
    pub auth: Arc<dyn SqlAuthService>,
    pub transport: Arc<dyn TransportService>,
}

impl ServerManager {
    pub fn new(
        auth: Arc<dyn SqlAuthService>,
        transport: Arc<dyn TransportService>,
    ) -> Self {
        Self {
            auth,
            transport,
        }
    }
}
