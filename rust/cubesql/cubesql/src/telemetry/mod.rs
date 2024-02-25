use crate::{sql::SessionState, CubeError};
use arc_swap::ArcSwap;
use log::{Level, LevelFilter};
use std::{collections::HashMap, fmt::Debug, sync::Arc};

lazy_static! {
    static ref REPORTER: ArcSwap<Box<dyn LogReporter>> =
        ArcSwap::from_pointee(Box::new(LocalReporter::new()));
}

pub trait LogReporter: Send + Sync + Debug {
    fn log(&self, event: String, properties: HashMap<String, String>, level: Level);

    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct LocalReporter {}

impl LocalReporter {
    pub fn new() -> LocalReporter {
        Self {}
    }
}

impl LogReporter for LocalReporter {
    fn log(&self, _event: String, _properties: HashMap<String, String>, _level: Level) {}

    fn is_active(&self) -> bool {
        false
    }
}
pub struct ReportingLogger {}

impl ReportingLogger {
    pub fn init(reporter: Box<dyn LogReporter>, max_level: LevelFilter) -> Result<(), CubeError> {
        REPORTER.swap(Arc::new(reporter));
        log::set_max_level(max_level);

        Ok(())
    }
}

pub fn set_reporter(reporter: Box<dyn LogReporter>) {
    REPORTER.swap(Arc::new(reporter));
}

pub trait ContextLogger: Send + Sync + Debug {
    fn error(&self, message: &str, props: Option<HashMap<String, String>>);
}

#[derive(Debug)]
pub struct SessionLogger {
    session_state: Arc<SessionState>,
}

impl SessionLogger {
    pub fn new(session_state: Arc<SessionState>) -> SessionLogger {
        Self { session_state }
    }

    fn log(&self, target: &str, props: HashMap<String, String>, level: Level) {
        // TODO: MySQL app_name
        let mut meta_fields = props;
        if let Some(name) = self.session_state.get_variable("application_name") {
            meta_fields.insert("appName".to_string(), name.value.to_string());
        }
        let protocol = self.session_state.protocol.to_string();
        meta_fields.insert("protocol".to_string(), protocol);
        meta_fields.insert("apiType".to_string(), "sql".to_string());

        if !report(target.to_string(), meta_fields.clone(), level) {
            log::log!(target: target, level, "{:?}", meta_fields);
        }
    }
}

impl ContextLogger for SessionLogger {
    fn error(&self, message: &str, props: Option<HashMap<String, String>>) {
        let mut properties = HashMap::from([("error".to_string(), message.to_string())]);
        properties.extend(props.unwrap_or_default());
        self.log("Cube SQL Error", properties, Level::Error);
    }
}

fn report(event: String, properties: HashMap<String, String>, level: Level) -> bool {
    let guard = REPORTER.load();
    if !guard.is_active() {
        return false;
    }

    let mut properties = properties;
    properties.insert("apiType".to_string(), "sql".to_string());

    guard.log(event, properties, level);

    true
}
