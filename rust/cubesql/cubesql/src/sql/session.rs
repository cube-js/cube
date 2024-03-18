use datafusion::scalar::ScalarValue;
use log::trace;
use rand::Rng;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock as RwLockSync, Weak},
    time::{Duration, SystemTime},
};
use tokio_util::sync::CancellationToken;

use crate::{
    sql::{
        database_variables::{
            mysql_default_session_variables, postgres_default_session_variables, DatabaseVariable,
            DatabaseVariablesToUpdate,
        },
        extended::PreparedStatement,
        temp_tables::TempTableManager,
    },
    transport::LoadRequestMeta,
    RWLockAsync,
};

use super::{
    database_variables::DatabaseVariables, server_manager::ServerManager,
    session_manager::SessionManager, AuthContextRef,
};

extern crate lazy_static;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabaseProtocol {
    MySQL,
    PostgreSQL,
}

impl DatabaseProtocol {
    pub fn to_string(&self) -> String {
        match &self {
            DatabaseProtocol::PostgreSQL => "postgres".to_string(),
            DatabaseProtocol::MySQL => "mysql".to_string(),
        }
    }
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
pub enum TransactionState {
    None,
    // Right now, it's 1 for all the time.
    Active(u64),
}

#[derive(Debug)]
pub enum QueryState {
    None,
    Active {
        query: String,
        cancel: CancellationToken,
    },
}

#[derive(Debug)]
pub struct SessionState {
    // connection id, immutable
    pub connection_id: u32,
    // secret for this session
    pub secret: u32,
    // client ip, immutable
    pub client_ip: String,
    // client port, immutable
    pub client_port: u16,
    // client protocol, mysql/postgresql, immutable
    pub protocol: DatabaseProtocol,

    // session db variables
    variables: RwLockSync<Option<DatabaseVariables>>,

    // session temporary tables
    temp_tables: Arc<TempTableManager>,

    properties: RwLockSync<SessionProperties>,

    // @todo Remove RWLock after split of Connection & SQLWorker
    // Context for Transport
    auth_context: RwLockSync<(Option<AuthContextRef>, SystemTime)>,

    transaction: RwLockSync<TransactionState>,
    query: RwLockSync<QueryState>,

    // Extended Query
    pub statements: RWLockAsync<HashMap<String, PreparedStatement>>,

    auth_context_expiration: Duration,
}

impl SessionState {
    pub fn new(
        connection_id: u32,
        client_ip: String,
        client_port: u16,
        protocol: DatabaseProtocol,
        auth_context: Option<AuthContextRef>,
        auth_context_expiration: Duration,
        session_manager: Weak<SessionManager>,
    ) -> Self {
        let mut rng = rand::thread_rng();

        Self {
            connection_id,
            secret: rng.gen(),
            client_ip,
            client_port,
            protocol,
            variables: RwLockSync::new(None),
            temp_tables: Arc::new(TempTableManager::new(session_manager)),
            properties: RwLockSync::new(SessionProperties::new(None, None)),
            auth_context: RwLockSync::new((auth_context, SystemTime::now())),
            transaction: RwLockSync::new(TransactionState::None),
            query: RwLockSync::new(QueryState::None),
            statements: RWLockAsync::new(HashMap::new()),
            auth_context_expiration,
        }
    }

    pub fn is_in_transaction(&self) -> bool {
        let guard = self
            .transaction
            .read()
            .expect("failed to unlock transaction for is_in_transaction");

        match *guard {
            TransactionState::None => false,
            TransactionState::Active(_) => true,
        }
    }

    pub fn begin_transaction(&self) -> bool {
        let mut guard = self
            .transaction
            .write()
            .expect("failed to unlock transaction for begin_transaction");

        match *guard {
            TransactionState::None => {
                *guard = TransactionState::Active(1);

                true
            }
            TransactionState::Active(_) => false,
        }
    }

    pub fn cancel_query(&self) {
        let mut guard = self
            .query
            .write()
            .expect("failed to unlock query for cancel_query");

        match &*guard {
            QueryState::None => {
                trace!("cancel_query - QueryState::None");
            }
            QueryState::Active { cancel, .. } => {
                cancel.cancel();

                trace!("cancel_query - Ok");

                *guard = QueryState::None;
            }
        }
    }

    pub fn current_query(&self) -> Option<String> {
        let guard = self
            .query
            .read()
            .expect("failed to unlock query for current_query");

        match &*guard {
            QueryState::Active { query, .. } => Some(query.clone()),
            QueryState::None => None,
        }
    }

    pub fn end_query(&self) {
        let mut guard = self
            .query
            .write()
            .expect("failed to unlock query for begin_query");

        match *guard {
            QueryState::Active { .. } => {
                *guard = QueryState::None;
            }
            QueryState::None => {}
        }
    }

    pub fn begin_query(&self, query: String) -> CancellationToken {
        let mut guard = self
            .query
            .write()
            .expect("failed to unlock query for begin_query");

        if let QueryState::Active { .. } = &*guard {
            trace!("Unable to begin new query while previous is still active.")
        };

        let cancel = CancellationToken::new();

        *guard = QueryState::Active {
            query,
            cancel: cancel.clone(),
        };

        cancel
    }

    pub fn end_transaction(&self) -> Option<u64> {
        let mut guard = self
            .transaction
            .write()
            .expect("failed to unlock transaction for checking end_transaction");

        if let TransactionState::Active(n) = *guard {
            *guard = TransactionState::None;

            Some(n)
        } else {
            None
        }
    }

    /// Clear object used for extend query protocol in Postgres
    /// This method is used in discard all
    pub async fn clear_extended(&self) {
        self.clear_prepared_statements().await;
    }

    pub async fn clear_prepared_statements(&self) {
        let mut statements_guard = self.statements.write().await;
        *statements_guard = HashMap::new();
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

    pub fn is_auth_context_expired(&self) -> bool {
        let guard = self
            .auth_context
            .read()
            .expect("failed to unlock auth_context for reading");
        let (_, created_at) = &*guard;
        let now = SystemTime::now();
        let duration = now.duration_since(*created_at).unwrap_or_default();
        duration > self.auth_context_expiration
    }

    pub fn auth_context(&self) -> Option<AuthContextRef> {
        let guard = self
            .auth_context
            .read()
            .expect("failed to unlock auth_context for reading");
        guard.0.clone()
    }

    pub fn set_auth_context(&self, auth_context: Option<AuthContextRef>) {
        let mut guard = self
            .auth_context
            .write()
            .expect("failed to auth_context properties for writting");
        *guard = (auth_context, SystemTime::now());
    }

    // TODO: Read without copy by holding acquired lock
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

    pub fn get_variable(&self, name: &str) -> Option<DatabaseVariable> {
        let guard = self
            .variables
            .read()
            .expect("failed to unlock variables for reading");

        match &*guard {
            Some(vars) => vars.get(name).map(|v| v.clone()),
            _ => match self.protocol {
                DatabaseProtocol::MySQL => MYSQL_DEFAULT_VARIABLES.get(name).map(|v| v.clone()),
                DatabaseProtocol::PostgreSQL => {
                    POSTGRES_DEFAULT_VARIABLES.get(name).map(|v| v.clone())
                }
            },
        }
    }

    pub fn set_variables(&self, variables: DatabaseVariablesToUpdate) {
        let mut to_override = false;
        let mut current_variables = self.all_variables();

        for new_var in variables.into_iter() {
            if let Some(current_var_value) = current_variables.get(&new_var.name) {
                if !current_var_value.readonly {
                    to_override = true;
                    current_variables.insert(new_var.name.clone(), new_var);
                }
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

    pub fn temp_tables(&self) -> Arc<TempTableManager> {
        Arc::clone(&self.temp_tables)
    }

    pub fn get_load_request_meta(&self) -> LoadRequestMeta {
        let application_name = if let Some(var) = self.get_variable("application_name") {
            Some(var.value.to_string())
        } else {
            None
        };

        LoadRequestMeta::new(
            self.protocol.to_string(),
            "sql".to_string(),
            application_name,
        )
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
    // For PostgreSQL
    pub fn to_stat_activity(self: &Arc<Self>) -> SessionStatActivity {
        let query = self.state.current_query();

        let application_name = if let Some(v) = self.state.get_variable("application_name") {
            match v.value {
                ScalarValue::Utf8(r) => r,
                _ => None,
            }
        } else {
            None
        };

        SessionStatActivity {
            oid: self.state.connection_id,
            datname: self.state.database(),
            pid: self.state.connection_id,
            leader_pid: None,
            usesysid: 0,
            usename: self.state.user(),
            application_name,
            client_addr: self.state.client_ip.clone(),
            client_hostname: None,
            client_port: self.state.client_port.clone(),
            query,
        }
    }

    // For MySQL
    pub fn to_process_list(self: &Arc<Self>) -> SessionProcessList {
        SessionProcessList {
            id: self.state.connection_id,
            host: self.state.client_ip.clone(),
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

#[derive(Debug)]
pub struct SessionStatActivity {
    pub oid: u32,
    pub datname: Option<String>,
    pub pid: u32,
    pub leader_pid: Option<u32>,
    pub usesysid: u32,
    pub usename: Option<String>,
    pub application_name: Option<String>,
    pub client_addr: String,
    pub client_hostname: Option<String>,
    pub client_port: u16,
    pub query: Option<String>,
}
