use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::driver_tools::DriverTools;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::pre_aggregation_obj::PreAggregationObj;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::cube_bridge::sql_utils::SqlUtils;
use crate::test_fixtures::cube_bridge::{
    MockDriverTools, MockJoinGraph, MockSqlTemplatesRender, MockSqlUtils,
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
/// ```
#[derive(Clone, TypedBuilder)]
pub struct MockBaseTools {
    #[builder(default = Rc::new(MockDriverTools::new()))]
    driver_tools: Rc<MockDriverTools>,

    #[builder(default = Rc::new(MockSqlTemplatesRender::default_templates()))]
    sql_templates: Rc<MockSqlTemplatesRender>,

    #[builder(default = Rc::new(MockSqlUtils))]
    sql_utils: Rc<MockSqlUtils>,

    #[builder(default = Rc::new(MockJoinGraph::new()))]
    join_graph: Rc<MockJoinGraph>,
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

    fn driver_tools(&self, _external: bool) -> Result<Rc<dyn DriverTools>, CubeError> {
        Ok(self.driver_tools.clone())
    }

    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        Ok(self.sql_templates.clone())
    }

    fn sql_utils_for_rust(&self) -> Result<Rc<dyn SqlUtils>, CubeError> {
        Ok(self.sql_utils.clone())
    }

    fn generate_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        todo!("generate_time_series not implemented in mock")
    }

    fn generate_custom_time_series(
        &self,
        _granularity: String,
        _date_range: Vec<String>,
        _origin: String,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        todo!("generate_custom_time_series not implemented in mock")
    }

    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError> {
        todo!("get_allocated_params not implemented in mock")
    }

    fn all_cube_members(&self, _path: String) -> Result<Vec<String>, CubeError> {
        todo!("all_cube_members not implemented in mock")
    }

    fn interval_and_minimal_time_unit(&self, _interval: String) -> Result<Vec<String>, CubeError> {
        todo!("interval_and_minimal_time_unit not implemented in mock")
    }

    fn get_pre_aggregation_by_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<Rc<dyn PreAggregationObj>, CubeError> {
        todo!("get_pre_aggregation_by_name not implemented in mock")
    }

    fn pre_aggregation_table_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<String, CubeError> {
        todo!("pre_aggregation_table_name not implemented in mock")
    }

    fn join_tree_for_hints(
        &self,
        hints: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        let result = self.join_graph.build_join(hints)?;
        Ok(result as Rc<dyn JoinDefinition>)
    }
}
