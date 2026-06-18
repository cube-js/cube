use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_time_dimension::{
    PreAggregationTimeDimension, PreAggregationTimeDimensionStatic,
};
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockPreAggregationTimeDimension {
    granularity: String,
    dimension: String,
}

impl_static_data!(
    MockPreAggregationTimeDimension,
    PreAggregationTimeDimensionStatic,
    granularity
);

impl PreAggregationTimeDimension for MockPreAggregationTimeDimension {
    crate::impl_static_data_method!(PreAggregationTimeDimensionStatic);

    fn dimension(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::pre_agg_single_ref(
            self.dimension.clone(),
        )?))
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
