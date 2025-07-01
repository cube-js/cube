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
    session::{Session, SessionState},
};
use crate::{compile::DatabaseProtocol, sql::session::SessionExtraId};

#[derive(Debug)]
struct SessionManagerInner {
    sessions: HashMap<u32, Arc<Session>>,
    uid_to_session: HashMap<SessionExtraId, Arc<Session>>,
}

#[derive(Debug)]
pub struct SessionManager {
    // Sessions
    last_id: AtomicU32,
    sessions: RWLockAsync<SessionManagerInner>,
    pub temp_table_size: AtomicUsize,
    // Backref
    pub server: Arc<ServerManager>,
}

crate::di_service!(SessionManager, []);

impl SessionManager {
    pub fn new(server: Arc<ServerManager>) -> Self {
        Self {
            last_id: AtomicU32::new(1),
            sessions: RWLockAsync::new(SessionManagerInner {
                sessions: HashMap::new(),
                uid_to_session: HashMap::new(),
            }),
            temp_table_size: AtomicUsize::new(0),
            server,
        }
    }

    pub async fn create_session(
        self: &Arc<Self>,
        protocol: DatabaseProtocol,
        client_addr: String,
        client_port: u16,
        extra_id: Option<SessionExtraId>,
    ) -> Result<Arc<Session>, CubeError> {
        let connection_id = self.last_id.fetch_add(1, Ordering::SeqCst);

        let session_ref = Arc::new(Session {
            session_manager: self.clone(),
            server: self.server.clone(),
            state: Arc::new(SessionState::new(
                connection_id,
                extra_id,
                client_addr,
                client_port,
                protocol,
                None,
                Duration::from_secs(self.server.config_obj.auth_expire_secs()),
                Arc::downgrade(self),
            )),
        });

        let mut guard = self.sessions.write().await;

        if guard.sessions.len() >= self.server.config_obj.max_sessions() {
            return Err(CubeError::user(format!(
                "Too many sessions, limit reached: {}",
                self.server.config_obj.max_sessions()
            )));
        }

        if let Some(extra_id) = extra_id {
            if guard.uid_to_session.contains_key(&extra_id) {
                return Err(CubeError::user(format!(
                    "Session cannot be created, because extra_id: {:?} already exists",
                    extra_id
                )));
            }

            guard.uid_to_session.insert(extra_id, session_ref.clone());
        }

        guard.sessions.insert(connection_id, session_ref.clone());

        Ok(session_ref)
    }

    pub async fn map_sessions<T: for<'a> From<&'a Session>>(self: &Arc<Self>) -> Vec<T> {
        let guard = self.sessions.read().await;

        guard
            .sessions
            .values()
            .map(|session| T::from(session))
            .collect::<Vec<T>>()
    }

    pub async fn get_session(&self, connection_id: u32) -> Option<Arc<Session>> {
        let guard = self.sessions.read().await;

        guard.sessions.get(&connection_id).cloned()
    }

    pub async fn get_session_by_extra_id(&self, extra_id: SessionExtraId) -> Option<Arc<Session>> {
        let guard = self.sessions.read().await;
        guard.uid_to_session.get(&extra_id).cloned()
    }

    pub async fn drop_session(&self, connection_id: u32) {
        let mut guard = self.sessions.write().await;

        if let Some(connection) = guard.sessions.remove(&connection_id) {
            if let Some(extra_id) = &connection.state.extra_id {
                guard.uid_to_session.remove(extra_id);
            }

            self.temp_table_size.fetch_sub(
                connection.state.temp_tables().physical_size(),
                Ordering::SeqCst,
            );
        }
    }
}
