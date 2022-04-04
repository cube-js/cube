use std::sync::{Arc, RwLock as RwLockSync};

use crate::{
    sql::{
        database_variables::{mysql_default_global_variables, postgres_default_global_variables},
        SqlAuthService,
    },
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
    static ref POSTGRES_DEFAULT_VARIABLES: RwLockSync<DatabaseVariables> =
        RwLockSync::new(postgres_default_global_variables());
    static ref MYSQL_DEFAULT_VARIABLES: RwLockSync<DatabaseVariables> =
        RwLockSync::new(mysql_default_global_variables());
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
            DatabaseProtocol::MySQL => MYSQL_DEFAULT_VARIABLES
                .read()
                .expect("failed to unlock variables for reading")
                .clone(),
            DatabaseProtocol::PostgreSQL => POSTGRES_DEFAULT_VARIABLES
                .read()
                .expect("failed to unlock variables for reading")
                .clone(),
        }
    }

    pub fn set_variables(&self, variables: DatabaseVariables, protocol: DatabaseProtocol) {
        let mut to_override = false;

        let mut current_variables = self.all_variables(protocol.clone());
        for (new_var_key, new_var_value) in variables.iter() {
            let mut key_to_update: Option<String> = None;
            for (current_var_key, current_var_value) in current_variables.iter() {
                if current_var_key.to_lowercase() == new_var_key.to_lowercase()
                    && !current_var_value.readonly
                {
                    key_to_update = Some(current_var_key.clone());

                    break;
                }
            }
            if key_to_update.is_some() {
                to_override = true;
                current_variables.insert(key_to_update.unwrap(), new_var_value.clone());
            }
        }

        if to_override {
            let mut guard = match protocol {
                DatabaseProtocol::MySQL => MYSQL_DEFAULT_VARIABLES
                    .write()
                    .expect("failed to unlock variables for writing"),
                DatabaseProtocol::PostgreSQL => POSTGRES_DEFAULT_VARIABLES
                    .write()
                    .expect("failed to unlock variables for writing"),
            };

            *guard = current_variables;
        }
    }
}
