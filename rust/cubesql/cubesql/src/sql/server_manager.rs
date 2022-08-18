use crate::{
    sql::{
        database_variables::{
            mysql_default_global_variables, postgres_default_global_variables,
            DatabaseVariablesToUpdate,
        },
        SqlAuthService,
    },
    transport::TransportService,
    CubeError,
};
use std::sync::{Arc, RwLock as RwLockSync, RwLockReadGuard, RwLockWriteGuard};

use super::{database_variables::DatabaseVariables, session::DatabaseProtocol};

#[derive(Debug)]
pub struct ServerConfiguration {
    /// Max number of prepared statements which can be allocated per connection
    pub connection_max_prepared_statements: usize,
    /// Max number of prepared statements which can be allocated per connection
    pub connection_max_cursors: usize,
    /// Max number of prepared statements which can be allocated per connection
    pub connection_max_portals: usize,
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            connection_max_prepared_statements: 50,
            connection_max_cursors: 15,
            connection_max_portals: 15,
        }
    }
}

#[derive(Debug)]
pub struct ServerManager {
    // References to shared things
    pub auth: Arc<dyn SqlAuthService>,
    pub transport: Arc<dyn TransportService>,
    // Non references
    pub configuration: ServerConfiguration,
    pub nonce: Option<Vec<u8>>,
    postgres_variables: RwLockSync<DatabaseVariables>,
    mysql_variables: RwLockSync<DatabaseVariables>,
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
            postgres_variables: RwLockSync::new(postgres_default_global_variables()),
            mysql_variables: RwLockSync::new(mysql_default_global_variables()),
        }
    }

    pub fn read_variables(
        &self,
        protocol: DatabaseProtocol,
    ) -> RwLockReadGuard<'_, DatabaseVariables> {
        match protocol {
            DatabaseProtocol::MySQL => self
                .mysql_variables
                .read()
                .expect("failed to unlock variables for reading"),
            DatabaseProtocol::PostgreSQL => self
                .postgres_variables
                .read()
                .expect("failed to unlock variables for reading"),
        }
    }

    fn write_variables(
        &self,
        protocol: DatabaseProtocol,
    ) -> RwLockWriteGuard<'_, DatabaseVariables> {
        match protocol {
            DatabaseProtocol::MySQL => self
                .mysql_variables
                .write()
                .expect("failed to unlock variables for reading"),
            DatabaseProtocol::PostgreSQL => self
                .postgres_variables
                .write()
                .expect("failed to unlock variables for reading"),
        }
    }

    // TODO: Read without copy by holding acquired lock
    pub fn all_variables(&self, protocol: DatabaseProtocol) -> DatabaseVariables {
        self.read_variables(protocol).clone()
    }

    pub fn set_variables(&self, variables: DatabaseVariablesToUpdate, protocol: DatabaseProtocol) {
        let mut current = self.write_variables(protocol.clone());

        for new_var in variables.into_iter() {
            if let Some(current_var_value) = current.get(&new_var.name) {
                if !current_var_value.readonly {
                    current.insert(new_var.name.clone(), new_var);
                }
            }
        }
    }
}
