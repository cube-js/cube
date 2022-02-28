use std::sync::{Arc, RwLock as RwLockSync};



use super::{dataframe, AuthContext, StatusFlags};

#[derive(Debug)]
pub struct ConnectionProperties {
    user: Option<String>,
    database: Option<String>,
}

impl ConnectionProperties {
    pub fn new(user: Option<String>, database: Option<String>) -> Self {
        Self { user, database }
    }
}

#[derive(Debug)]
pub struct ConnectionState {
    // connection id, it's immutable
    pub connection_id: u32,
    // Connection properties
    properties: RwLockSync<ConnectionProperties>,
    // @todo Remove RWLock after split of Connection & SQLWorker
    // Context for Transport
    auth_context: RwLockSync<Option<AuthContext>>,
}

impl ConnectionState {
    pub fn new(
        connection_id: u32,
        properties: ConnectionProperties,
        auth_context: Option<AuthContext>,
    ) -> Self {
        Self {
            connection_id,
            properties: RwLockSync::new(properties),
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
}

pub enum QueryResponse {
    Ok(StatusFlags),
    ResultSet(StatusFlags, Arc<dataframe::DataFrame>),
}
