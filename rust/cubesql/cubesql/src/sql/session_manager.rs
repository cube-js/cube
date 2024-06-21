use crate::{CubeError, RWLockAsync};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use super::{
    server_manager::ServerManager,
    session::{DatabaseProtocol, Session, SessionProcessList, SessionStatActivity, SessionState},
};

#[derive(Debug)]
pub struct SessionManager {
    // Sessions
    last_id: AtomicU32,
    sessions: RWLockAsync<HashMap<u32, Arc<Session>>>,
    pub temp_table_size: AtomicUsize,
    // Backref
    pub server: Arc<ServerManager>,
}

crate::di_service!(SessionManager, []);

impl SessionManager {
    pub fn new(server: Arc<ServerManager>) -> Self {
        Self {
            last_id: AtomicU32::new(1),
            sessions: RWLockAsync::new(HashMap::new()),
            temp_table_size: AtomicUsize::new(0),
            server,
        }
    }

    pub async fn create_session(
        self: &Arc<Self>,
        protocol: DatabaseProtocol,
        client_addr: String,
        client_port: u16,
    ) -> Arc<Session> {
        let connection_id = self.last_id.fetch_add(1, Ordering::SeqCst);

        let sess = Session {
            session_manager: self.clone(),
            server: self.server.clone(),
            state: Arc::new(SessionState::new(
                connection_id,
                client_addr,
                client_port,
                protocol,
                None,
                Duration::from_secs(self.server.config_obj.auth_expire_secs()),
                Arc::downgrade(self),
            )),
        };

        let session_ref = Arc::new(sess);

        let mut guard = self.sessions.write().await;

        guard.insert(connection_id, session_ref.clone());

        session_ref
    }

    pub async fn stat_activity(self: &Arc<Self>) -> Vec<SessionStatActivity> {
        let guard = self.sessions.read().await;

        guard
            .values()
            .map(Session::to_stat_activity)
            .collect::<Vec<SessionStatActivity>>()
    }

    pub async fn process_list(self: &Arc<Self>) -> Vec<SessionProcessList> {
        let guard = self.sessions.read().await;

        guard
            .values()
            .map(Session::to_process_list)
            .collect::<Vec<SessionProcessList>>()
    }

    pub async fn get_session(&self, connection_id: u32) -> Option<Arc<Session>> {
        let guard = self.sessions.read().await;

        guard.get(&connection_id).map(|s| s.clone())
    }

    pub async fn drop_session(&self, connection_id: u32) {
        let mut guard = self.sessions.write().await;

        if let Some(connection) = guard.remove(&connection_id) {
            self.temp_table_size.fetch_sub(
                connection.state.temp_tables().physical_size(),
                Ordering::SeqCst,
            );
        }
    }
}
