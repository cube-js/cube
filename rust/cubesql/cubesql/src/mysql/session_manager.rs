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
    session::{Session, SessionProperties, SessionState},
};

#[derive(Debug)]
struct SessionDescriptor {
    protocol: String,
    session: Arc<Session>,
}

#[derive(Debug)]
pub struct SessionManager {
    // Sessions
    last_id: AtomicU32,
    sessions: RwLockSync<HashMap<u32, SessionDescriptor>>,
    // Backref
    server: Arc<ServerManager>,
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

    pub fn create_session(self: &Arc<Self>, protocol: String, host: String) -> Arc<Session> {
        let connection_id = self.last_id.fetch_add(1, Ordering::SeqCst);

        let sess = Session {
            session_manager: self.clone(),
            server: self.server.clone(),
            state: Arc::new(SessionState::new(
                connection_id,
                host,
                SessionProperties::new(None, None),
                None,
            )),
        };

        let session_ref = Arc::new(sess);

        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for inserting session");
        guard.insert(
            connection_id,
            SessionDescriptor {
                protocol,
                session: session_ref.clone(),
            },
        );

        session_ref
    }

    pub fn drop_session(&self, connection_id: u32) {
        let mut guard = self
            .sessions
            .write()
            .expect("failed to unlock sessions for droping session");
        guard.remove(&connection_id);
    }
}
