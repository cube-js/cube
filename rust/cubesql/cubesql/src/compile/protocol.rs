use crate::{
    compile::{CubeContext, DatabaseVariable, DatabaseVariables},
    CubeError,
};
use datafusion::datasource;
use log::error;
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

pub trait DatabaseProtocolDetails: Debug + Send + Sync {
    fn get_name(&self) -> &'static str;

    fn support_set_variable(&self) -> bool;

    fn support_transactions(&self) -> bool;

    /// Get default state for session variables
    fn get_session_default_variables(&self) -> DatabaseVariables;

    /// Get default value for specific session variable
    fn get_session_variable_default(&self, name: &str) -> Option<DatabaseVariable>;

    fn get_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<Arc<dyn datasource::TableProvider>>;

    fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError>;
}

impl PartialEq for dyn DatabaseProtocolDetails {
    fn eq(&self, other: &Self) -> bool {
        self.get_name() == other.get_name()
    }
}

impl Eq for dyn DatabaseProtocolDetails {}

impl Hash for dyn DatabaseProtocolDetails {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.get_name().as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabaseProtocol {
    MySQL,
    PostgreSQL,
    Extension(Arc<dyn DatabaseProtocolDetails>),
}

impl DatabaseProtocolDetails for DatabaseProtocol {
    fn get_name(&self) -> &'static str {
        match &self {
            DatabaseProtocol::PostgreSQL => "postgres",
            DatabaseProtocol::MySQL => "mysql",
            DatabaseProtocol::Extension(ext) => ext.get_name(),
        }
    }

    fn support_set_variable(&self) -> bool {
        match &self {
            DatabaseProtocol::Extension(ext) => ext.support_set_variable(),
            _ => true,
        }
    }

    fn support_transactions(&self) -> bool {
        match &self {
            DatabaseProtocol::MySQL => false,
            DatabaseProtocol::PostgreSQL => true,
            DatabaseProtocol::Extension(ext) => ext.support_transactions(),
        }
    }

    fn get_session_default_variables(&self) -> DatabaseVariables {
        match &self {
            DatabaseProtocol::MySQL => {
                // TODO(ovr): Should we move it from session?
                error!("get_session_default_variables was called on MySQL protocol");

                DatabaseVariables::default()
            }
            DatabaseProtocol::PostgreSQL => {
                // TODO(ovr): Should we move it from session?
                error!("get_session_default_variables was called on PostgreSQL protocol");

                DatabaseVariables::default()
            }
            DatabaseProtocol::Extension(ext) => ext.get_session_default_variables(),
        }
    }

    fn get_session_variable_default(&self, name: &str) -> Option<DatabaseVariable> {
        self.get_session_default_variables().get(name).cloned()
    }

    fn get_provider(
        &self,
        context: &CubeContext,
        tr: datafusion::catalog::TableReference,
    ) -> Option<Arc<dyn datasource::TableProvider>> {
        match self {
            DatabaseProtocol::MySQL => self.get_mysql_provider(context, tr),
            DatabaseProtocol::PostgreSQL => self.get_postgres_provider(context, tr),
            DatabaseProtocol::Extension(ext) => ext.get_provider(&context, tr),
        }
    }

    fn table_name_by_table_provider(
        &self,
        table_provider: Arc<dyn datasource::TableProvider>,
    ) -> Result<String, CubeError> {
        match self {
            DatabaseProtocol::MySQL => self.get_mysql_table_name(table_provider),
            DatabaseProtocol::PostgreSQL => self.get_postgres_table_name(table_provider),
            DatabaseProtocol::Extension(ext) => ext.table_name_by_table_provider(table_provider),
        }
    }
}
