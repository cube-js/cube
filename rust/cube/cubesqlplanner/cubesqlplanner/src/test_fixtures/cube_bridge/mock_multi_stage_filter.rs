use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::cube_bridge::multi_stage_filter::{
    MultiStageFilterReferences, MultiStageFilterReferencesStatic,
};
use crate::impl_static_data;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockMultiStageFilterReferences {
    #[builder(default)]
    mode: Option<String>,
    #[builder(default)]
    exclude: Option<Vec<String>>,
    #[builder(default)]
    keep_only: Option<Vec<String>>,
    #[builder(default)]
    include: Option<Vec<NativeFilterItem>>,
}

impl_static_data!(
    MockMultiStageFilterReferences,
    MultiStageFilterReferencesStatic,
    mode,
    exclude,
    keep_only,
    include
);

impl MultiStageFilterReferences for MockMultiStageFilterReferences {
    crate::impl_static_data_method!(MultiStageFilterReferencesStatic);

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
