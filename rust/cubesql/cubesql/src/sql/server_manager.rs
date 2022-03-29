use std::{collections::HashMap, sync::Arc};

use crate::{
    sql::{database_variables::mysql_default_global_variables, SqlAuthService},
    transport::TransportService,
    CubeError,
};

use super::{database_variables::DatabaseVariables, session::DatabaseProtocol};

#[derive(Debug)]
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

lazy_static! {
    static ref POSTGRES_DEFAULT_VARIABLES: DatabaseVariables = HashMap::new();
    static ref MYSQL_DEFAULT_VARIABLES: DatabaseVariables = mysql_default_global_variables();
}

#[derive(Debug)]
pub struct ServerManager {
    // References to shared things
    pub auth: Arc<dyn SqlAuthService>,
    pub transport: Arc<dyn TransportService>,
    // Non references
    pub configuration: ServerConfiguration,
    pub nonce: Option<Vec<u8>>,
}

crate::di_service!(ServerManager, []);

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

    pub fn all_variables(&self, protocol: DatabaseProtocol) -> DatabaseVariables {
        match protocol {
            DatabaseProtocol::MySQL => MYSQL_DEFAULT_VARIABLES.clone(),
            DatabaseProtocol::PostgreSQL => POSTGRES_DEFAULT_VARIABLES.clone(),
        }
    }
}
