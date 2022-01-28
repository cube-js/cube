use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::{fmt::Debug, hash::Hash};

use tokio::sync::RwLock;

use super::{AuthContext, ServerManager};
use crate::compile::QueryPlannerExecutionProps;

pub struct Connection {
    pub props: QueryPlannerExecutionProps,
    pub context: Option<AuthContext>,
    // backreference to Server
    pub server: Arc<ServerManager>,
}

pub struct ConnectionRef {
    connection: Arc<Connection>,
}

pub struct ConnectionsManager {
    id: u32,
    connections: RwLock<HashMap<u32, Arc<Connection>>>,
}

impl ConnectionsManager {
    pub fn new() -> Self {
        Self {
            id: 1,
            connections: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create(&self, server: Arc<ServerManager>) -> Arc<Connection> {
        let connection_id = 1;

        let connection = Connection {
            server,
            props: QueryPlannerExecutionProps::new(connection_id, None, None),
            context: None,
        };
        let connection_ref = Arc::new(connection);

        let mut connections_guard = self.connections.write().await;
        connections_guard.insert(connection_id, connection_ref.clone());

        connection_ref
    }
}
