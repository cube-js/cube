use std::sync::Arc;

use crate::transport::TransportService;

use super::SqlAuthService;

pub struct ServerManager {
    // References to shared things
    pub auth: Arc<dyn SqlAuthService>,
    pub transport: Arc<dyn TransportService>,
    // Non references
    nonce: Option<Vec<u8>>,
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
        }
    }

    pub fn get_nonce(&self) -> Option<Vec<u8>> {
        self.nonce.clone()
    }
}
