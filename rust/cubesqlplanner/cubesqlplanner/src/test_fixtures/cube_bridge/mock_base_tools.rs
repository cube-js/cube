use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::driver_tools::DriverTools;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::pre_aggregation_obj::PreAggregationObj;
use crate::cube_bridge::security_context::SecurityContext;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::cube_bridge::sql_utils::SqlUtils;
use crate::test_fixtures::cube_bridge::{
    MockDriverTools, MockSecurityContext, MockSqlTemplatesRender, MockSqlUtils,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of BaseTools for testing
///
/// This mock provides implementations for driver_tools, sql_templates,
/// security_context_for_rust, and sql_utils_for_rust.
/// Other methods throw todo!() errors.
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::MockBaseTools;
///
/// // Use builder pattern
/// let tools = MockBaseTools::builder().build();
/// let driver_tools = tools.driver_tools(false).unwrap();
/// let sql_templates = tools.sql_templates().unwrap();
///
/// // Or with custom components
/// let custom_driver = MockDriverTools::with_timezone("Europe/London".to_string());
/// let tools = MockBaseTools::builder()
///     .driver_tools(custom_driver)
///     .build();
/// ```
#[derive(Clone, TypedBuilder)]
pub struct MockBaseTools {
    #[builder(default = Rc::new(MockDriverTools::new()))]
    driver_tools: Rc<MockDriverTools>,

    #[builder(default = Rc::new(MockSqlTemplatesRender::default_templates()))]
    sql_templates: Rc<MockSqlTemplatesRender>,

    #[builder(default = Rc::new(MockSecurityContext))]
    security_context: Rc<MockSecurityContext>,

    #[builder(default = Rc::new(MockSqlUtils))]
    sql_utils: Rc<MockSqlUtils>,
}

impl Default for MockBaseTools {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl BaseTools for MockBaseTools {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    /// Returns driver tools - uses MockDriverTools
    fn driver_tools(&self, _external: bool) -> Result<Rc<dyn DriverTools>, CubeError> {
        Ok(self.driver_tools.clone())
    }

    /// Returns SQL templates renderer - uses MockSqlTemplatesRender
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        Ok(self.sql_templates.clone())
    }

    /// Returns security context - uses MockSecurityContext
    fn security_context_for_rust(&self) -> Result<Rc<dyn SecurityContext>, CubeError> {
        Ok(self.security_context.clone())
    }

    /// Returns SQL utils - uses MockSqlUtils
    fn sql_utils_for_rust(&self) -> Result<Rc<dyn SqlUtils>, CubeError> {
        Ok(self.sql_utils.clone())
    }

    /// Generate time series - not implemented in mock
    fn generate_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        todo!("generate_time_series not implemented in mock")
    }

    /// Generate custom time series - not implemented in mock
    fn generate_custom_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
        _origin: String,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        todo!("generate_custom_time_series not implemented in mock")
    }

    /// Get allocated parameters - not implemented in mock
    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError> {
        todo!("get_allocated_params not implemented in mock")
    }

    /// Get all cube members - not implemented in mock
    fn all_cube_members(&self, _path: String) -> Result<Vec<String>, CubeError> {
        todo!("all_cube_members not implemented in mock")
    }

    /// Get interval and minimal time unit - not implemented in mock
    fn interval_and_minimal_time_unit(&self, _interval: String) -> Result<Vec<String>, CubeError> {
        todo!("interval_and_minimal_time_unit not implemented in mock")
    }

    /// Get pre-aggregation by name - not implemented in mock
    fn get_pre_aggregation_by_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<Rc<dyn PreAggregationObj>, CubeError> {
        todo!("get_pre_aggregation_by_name not implemented in mock")
    }

    /// Get pre-aggregation table name - not implemented in mock
    fn pre_aggregation_table_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<String, CubeError> {
        todo!("pre_aggregation_table_name not implemented in mock")
    }

    /// Get join tree for hints - not implemented in mock
    fn join_tree_for_hints(
        &self,
        _hints: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        todo!("join_tree_for_hints not implemented in mock")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        let tools = MockBaseTools::builder().build();
        assert!(tools.driver_tools(false).is_ok());
        assert!(tools.sql_templates().is_ok());
        assert!(tools.security_context_for_rust().is_ok());
        assert!(tools.sql_utils_for_rust().is_ok());
    }

    #[test]
    fn test_default_trait() {
        let tools = MockBaseTools::default();
        assert!(tools.driver_tools(false).is_ok());
        assert!(tools.sql_templates().is_ok());
        assert!(tools.security_context_for_rust().is_ok());
        assert!(tools.sql_utils_for_rust().is_ok());
    }

    #[test]
    fn test_driver_tools() {
        let tools = MockBaseTools::builder().build();
        let driver_tools = tools.driver_tools(false).unwrap();

        // Test that it returns a valid DriverTools implementation
        let result = driver_tools
            .time_grouped_column("day".to_string(), "created_at".to_string())
            .unwrap();
        assert_eq!(result, "date_trunc('day', created_at)");
    }

    #[test]
    fn test_driver_tools_external_flag() {
        let tools = MockBaseTools::builder().build();

        // Both external true and false should work (mock ignores the flag)
        assert!(tools.driver_tools(false).is_ok());
        assert!(tools.driver_tools(true).is_ok());
    }

    #[test]
    fn test_sql_templates() {
        let tools = MockBaseTools::builder().build();
        let templates = tools.sql_templates().unwrap();

        // Test that it returns a valid SqlTemplatesRender implementation
        assert!(templates.contains_template("filters/equals"));
        assert!(templates.contains_template("functions/SUM"));
    }

    #[test]
    fn test_security_context() {
        let tools = MockBaseTools::builder().build();
        // Just verify it returns without error
        assert!(tools.security_context_for_rust().is_ok());
    }

    #[test]
    fn test_sql_utils() {
        let tools = MockBaseTools::builder().build();
        // Just verify it returns without error
        assert!(tools.sql_utils_for_rust().is_ok());
    }

    #[test]
    fn test_builder_with_custom_driver_tools() {
        let custom_driver = MockDriverTools::with_timezone("Europe/London".to_string());
        let tools = MockBaseTools::builder()
            .driver_tools(Rc::new(custom_driver))
            .build();

        let driver_tools = tools.driver_tools(false).unwrap();
        let result = driver_tools.convert_tz("timestamp".to_string()).unwrap();
        assert_eq!(
            result,
            "(timestamp::timestamptz AT TIME ZONE 'Europe/London')"
        );
    }

    #[test]
    fn test_builder_with_custom_sql_templates() {
        let mut custom_templates = std::collections::HashMap::new();
        custom_templates.insert("test/template".to_string(), "TEST {{value}}".to_string());
        let sql_templates = MockSqlTemplatesRender::try_new(custom_templates).unwrap();

        let tools = MockBaseTools::builder()
            .sql_templates(Rc::new(sql_templates))
            .build();

        let templates = tools.sql_templates().unwrap();
        assert!(templates.contains_template("test/template"));
    }

    #[test]
    fn test_builder_with_all_custom_components() {
        let driver_tools = MockDriverTools::with_timezone("Asia/Tokyo".to_string());
        let sql_templates = MockSqlTemplatesRender::default_templates();
        let security_context = MockSecurityContext;
        let sql_utils = MockSqlUtils;

        let tools = MockBaseTools::builder()
            .driver_tools(Rc::new(driver_tools))
            .sql_templates(Rc::new(sql_templates))
            .security_context(Rc::new(security_context))
            .sql_utils(Rc::new(sql_utils))
            .build();

        assert!(tools.driver_tools(false).is_ok());
        assert!(tools.sql_templates().is_ok());
        assert!(tools.security_context_for_rust().is_ok());
        assert!(tools.sql_utils_for_rust().is_ok());
    }
}