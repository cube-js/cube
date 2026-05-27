use crate::cube_bridge::multi_stage_grain::{
    MultiStageGrainReferences, MultiStageGrainReferencesStatic,
};
use crate::impl_static_data;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockMultiStageGrainReferences {
    #[builder(default)]
    mode: Option<String>,
    #[builder(default)]
    exclude: Option<Vec<String>>,
    #[builder(default)]
    keep_only: Option<Vec<String>>,
    #[builder(default)]
    include: Option<Vec<String>>,
}

impl_static_data!(
    MockMultiStageGrainReferences,
    MultiStageGrainReferencesStatic,
    mode,
    exclude,
    keep_only,
    include
);

impl MultiStageGrainReferences for MockMultiStageGrainReferences {
    crate::impl_static_data_method!(MultiStageGrainReferencesStatic);

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
