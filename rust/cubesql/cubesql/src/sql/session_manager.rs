use crate::{CubeError, RWLockAsync};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
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
    // Backref
    pub server: Arc<ServerManager>,
}

crate::di_service!(SessionManager, []);

impl SessionManager {
    pub fn new(server: Arc<ServerManager>) -> Self {
        Self {
            last_id: AtomicU32::new(1),
            sessions: RWLockAsync::new(HashMap::new()),
            server,
        }
    }

    pub async fn create_session(
        self: &Arc<Self>,
        protocol: DatabaseProtocol,
        host: String,
    ) -> Arc<Session> {
        let connection_id = self.last_id.fetch_add(1, Ordering::SeqCst);

        let sess = Session {
            session_manager: self.clone(),
            server: self.server.clone(),
            state: Arc::new(SessionState::new(connection_id, host, protocol, None)),
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

        guard.remove(&connection_id);
    }
}
