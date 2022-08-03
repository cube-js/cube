use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, RwLock as RwLockSync,
    },
};

use crate::{sql::session::SessionStatActivity, CubeError};

use super::{
    server_manager::ServerManager,
    session::{DatabaseProtocol, Session, SessionProcessList, SessionState},
};

#[derive(Debug)]
pub struct SessionManager {
    // Sessions
    last_id: AtomicI32,
    sessions: RwLockSync<HashMap<i32, Arc<Session>>>,
    // Backref
    pub server: Arc<ServerManager>,
}

crate::di_service!(SessionManager, []);

impl SessionManager {
    pub fn new(server: Arc<ServerManager>) -> Self {
        Self {
            last_id: AtomicI32::new(1),
            sessions: RwLockSync::new(HashMap::new()),
            server,
        }
    }

    pub fn create_session(
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

        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for inserting session");
        guard.insert(connection_id, session_ref.clone());

        session_ref
    }

    pub fn stat_activity(self: &Arc<Self>) -> Vec<SessionStatActivity> {
        let guard = self
            .sessions
            .read()
            .expect("failed to unlock sessions for stat_activity");

        guard
            .values()
            .map(Session::to_stat_activity)
            .collect::<Vec<SessionStatActivity>>()
    }

    pub fn process_list(self: &Arc<Self>) -> Vec<SessionProcessList> {
        let guard = self
            .sessions
            .read()
            .expect("failed to unlock sessions for process_list");

        guard
            .values()
            .map(Session::to_process_list)
            .collect::<Vec<SessionProcessList>>()
    }

    pub fn get_session(&self, connection_id: i32) -> Option<Arc<Session>> {
        let guard = self
            .sessions
            .read()
            .expect("failed to unlock sessions for get_session session");

        guard.get(&connection_id).map(|s| s.clone())
    }

    pub fn drop_session(&self, connection_id: i32) {
        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for drop_session session");
        guard.remove(&connection_id);
    }
}
