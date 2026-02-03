use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::{
    PreAggregationDescription, PreAggregationDescriptionStatic,
};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockPreAggregationDescription {
    name: String,
    #[builder(default = "rollup".to_string())]
    pre_aggregation_type: String,
    #[builder(default)]
    granularity: Option<String>,
    #[builder(default)]
    sql_alias: Option<String>,
    #[builder(default)]
    external: Option<bool>,
    #[builder(default)]
    allow_non_strict_date_range_match: Option<bool>,

    #[builder(default, setter(strip_option(fallback = measure_references_opt)))]
    measure_references: Option<String>,
    #[builder(default, setter(strip_option(fallback = dimension_references_opt)))]
    dimension_references: Option<String>,
    #[builder(default, setter(strip_option(fallback = time_dimension_reference_opt)))]
    time_dimension_reference: Option<String>,
    #[builder(default, setter(strip_option(fallback = rollup_references_opt)))]
    rollup_references: Option<String>,
}

impl_static_data!(
    MockPreAggregationDescription,
    PreAggregationDescriptionStatic,
    name,
    pre_aggregation_type,
    granularity,
    sql_alias,
    external,
    allow_non_strict_date_range_match
);

impl PreAggregationDescription for MockPreAggregationDescription {
    crate::impl_static_data_method!(PreAggregationDescriptionStatic);

    fn has_measure_references(&self) -> Result<bool, CubeError> {
        Ok(self.measure_references.is_some())
    }

    fn measure_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.measure_references {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn has_dimension_references(&self) -> Result<bool, CubeError> {
        Ok(self.dimension_references.is_some())
    }

    fn dimension_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.dimension_references {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn has_time_dimension_reference(&self) -> Result<bool, CubeError> {
        Ok(self.time_dimension_reference.is_some())
    }

    fn time_dimension_reference(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.time_dimension_reference {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn has_rollup_references(&self) -> Result<bool, CubeError> {
        Ok(self.rollup_references.is_some())
    }

    fn rollup_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.rollup_references {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
