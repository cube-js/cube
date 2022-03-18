use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock as RwLockSync,
    },
};

use crate::CubeError;

use super::{
    server_manager::ServerManager,
    session::{DatabaseProtocol, Session, SessionProcessList, SessionProperties, SessionState},
};

#[derive(Debug)]
pub struct SessionManager {
    // Sessions
    last_id: AtomicU32,
    sessions: RwLockSync<HashMap<u32, Arc<Session>>>,
    // Backref
    pub server: Arc<ServerManager>,
}

crate::di_service!(SessionManager, []);

impl SessionManager {
    pub fn new(server: Arc<ServerManager>) -> Self {
        Self {
            last_id: AtomicU32::new(1),
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
            state: Arc::new(SessionState::new(
                connection_id,
                host,
                protocol,
                SessionProperties::new(None, None),
                None,
            )),
        };

        let session_ref = Arc::new(sess);

        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for inserting session");
        guard.insert(connection_id, session_ref.clone());

        session_ref
    }

    pub fn process_list(self: &Arc<Self>) -> Vec<SessionProcessList> {
        let guard = self
            .sessions
            .read()
            .expect("failed to unlock sessions for reading process list");

        guard
            .values()
            .map(Session::to_process_list)
            .collect::<Vec<SessionProcessList>>()
    }

    pub fn drop_session(&self, connection_id: u32) {
        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for droping session");
        guard.remove(&connection_id);
    }
}
