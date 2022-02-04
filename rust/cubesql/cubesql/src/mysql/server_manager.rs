use std::sync::Arc;

use crate::transport::TransportService;

use super::SqlAuthService;

pub struct ServerConfiguration {
    /// Max number of prepared statements which can be allocated per connection
    pub connection_max_prepared_statements: usize,
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            connection_max_prepared_statements: 50,
        }
    }
}

pub struct ServerManager {
    // References to shared things
    pub auth: Arc<dyn SqlAuthService>,
    pub transport: Arc<dyn TransportService>,
    // Non references
    pub configuration: ServerConfiguration,
    pub nonce: Option<Vec<u8>>,
}

impl ServerManager {
    pub fn new(
        auth: Arc<dyn SqlAuthService>,
        transport: Arc<dyn TransportService>,
        nonce: Option<Vec<u8>>,
    ) -> Self {
        Self {
            auth,
            transport,
            nonce,
            configuration: ServerConfiguration::default(),
        }
    }
}
