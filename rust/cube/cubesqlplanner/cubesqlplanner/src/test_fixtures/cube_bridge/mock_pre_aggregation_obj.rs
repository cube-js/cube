use crate::cube_bridge::pre_aggregation_obj::{PreAggregationObj, PreAggregationObjStatic};
use crate::impl_static_data;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockPreAggregationObj {
    #[builder(default)]
    table_name: Option<String>,
    #[builder(default)]
    pre_aggregation_name: Option<String>,
    #[builder(default)]
    cube: Option<String>,
    #[builder(default)]
    pre_aggregation_id: Option<String>,
}

impl_static_data!(
    MockPreAggregationObj,
    PreAggregationObjStatic,
    table_name,
    pre_aggregation_name,
    cube,
    pre_aggregation_id
);

impl PreAggregationObj for MockPreAggregationObj {
    crate::impl_static_data_method!(PreAggregationObjStatic);

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
