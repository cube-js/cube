use std::sync::{Arc, RwLock as RwLockSync};

use crate::sql::database_variables::{
    mysql_default_session_variables, postgres_default_session_variables,
};

use super::{
    database_variables::DatabaseVariables, server_manager::ServerManager,
    session_manager::SessionManager, AuthContext,
};

extern crate lazy_static;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseProtocol {
    MySQL,
    PostgreSQL,
}

#[derive(Debug, Clone)]
pub struct SessionProperties {
    user: Option<String>,
    database: Option<String>,
}

impl SessionProperties {
    pub fn new(user: Option<String>, database: Option<String>) -> Self {
        Self { user, database }
    }
}

lazy_static! {
    static ref POSTGRES_DEFAULT_VARIABLES: DatabaseVariables = postgres_default_session_variables();
    static ref MYSQL_DEFAULT_VARIABLES: DatabaseVariables = mysql_default_session_variables();
}

#[derive(Debug)]
pub struct SessionState {
    // connection id, immutable
    pub connection_id: u32,
    // client address, immutable
    pub host: String,
    // client protocol, mysql/postgresql, immutable
    pub protocol: DatabaseProtocol,

    // session db variables
    variables: RwLockSync<Option<DatabaseVariables>>,

    properties: RwLockSync<SessionProperties>,

    // @todo Remove RWLock after split of Connection & SQLWorker
    // Context for Transport
    auth_context: RwLockSync<Option<AuthContext>>,
}

impl SessionState {
    pub fn new(
        connection_id: u32,
        host: String,
        protocol: DatabaseProtocol,
        auth_context: Option<AuthContext>,
    ) -> Self {
        Self {
            connection_id,
            host,
            protocol,
            variables: RwLockSync::new(None),
            properties: RwLockSync::new(SessionProperties::new(None, None)),
            auth_context: RwLockSync::new(auth_context),
        }
    }

    pub fn user(&self) -> Option<String> {
        let guard = self
            .properties
            .read()
            .expect("failed to unlock properties for reading user");
        guard.user.clone()
    }

    pub fn set_user(&self, user: Option<String>) {
        let mut guard = self
            .properties
            .write()
            .expect("failed to unlock properties for writting user");
        guard.user = user;
    }

    pub fn database(&self) -> Option<String> {
        let guard = self
            .properties
            .read()
            .expect("failed to unlock properties for reading database");
        guard.database.clone()
    }

    pub fn set_database(&self, database: Option<String>) {
        let mut guard = self
            .properties
            .write()
            .expect("failed to unlock properties for writting database");
        guard.database = database;
    }

    pub fn auth_context(&self) -> Option<AuthContext> {
        let guard = self
            .auth_context
            .read()
            .expect("failed to unlock auth_context for reading");
        guard.clone()
    }

    pub fn set_auth_context(&self, auth_context: Option<AuthContext>) {
        let mut guard = self
            .auth_context
            .write()
            .expect("failed to auth_context properties for writting");
        *guard = auth_context;
    }

    pub fn all_variables(&self) -> DatabaseVariables {
        let guard = self
            .variables
            .read()
            .expect("failed to unlock variables for reading")
            .clone();

        match guard {
            Some(vars) => vars,
            _ => match self.protocol {
                DatabaseProtocol::MySQL => return MYSQL_DEFAULT_VARIABLES.clone(),
                DatabaseProtocol::PostgreSQL => return POSTGRES_DEFAULT_VARIABLES.clone(),
            },
        }
    }

    pub fn set_variables(&self, variables: DatabaseVariables) {
        let mut to_override = false;

        let mut current_variables = self.all_variables();
        for (new_var_key, new_var_value) in variables.iter() {
            let mut key_to_update: Option<String> = Some(new_var_key.to_string());
            for (current_var_key, current_var_value) in current_variables.iter() {
                if current_var_key.to_lowercase() == new_var_key.to_lowercase() {
                    key_to_update = if current_var_value.readonly {
                        None
                    } else {
                        Some(current_var_key.clone())
                    };

                    break;
                }
            }
            if key_to_update.is_some() {
                to_override = true;
                current_variables.insert(key_to_update.unwrap(), new_var_value.clone());
            }
        }

        if to_override {
            let mut guard = self
                .variables
                .write()
                .expect("failed to unlock variables for writing");

            *guard = Some(current_variables);
        }
    }
}

#[derive(Debug)]
pub struct Session {
    // Backref
    pub session_manager: Arc<SessionManager>,
    pub server: Arc<ServerManager>,
    // Props for execution queries
    pub state: Arc<SessionState>,
}

impl Session {
    pub fn to_process_list(self: &Arc<Self>) -> SessionProcessList {
        SessionProcessList {
            id: self.state.connection_id,
            host: self.state.host.clone(),
            user: self.state.user(),
            database: self.state.database(),
        }
    }
}

#[derive(Debug)]
pub struct SessionProcessList {
    pub id: u32,
    pub user: Option<String>,
    pub host: String,
    pub database: Option<String>,
}
