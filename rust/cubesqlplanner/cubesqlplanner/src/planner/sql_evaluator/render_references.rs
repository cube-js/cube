use super::default_visitor::ReplaceNodeProcessorItem;
use super::{EvaluationNode, MemberEvaluatorType};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct RenderReferencesNodeProcessor {
    references: HashMap<String, String>,
}

impl RenderReferencesNodeProcessor {
    pub fn new(references: HashMap<String, String>) -> Rc<Self> {
        Rc::new(Self { references })
    }
}

impl ReplaceNodeProcessorItem for RenderReferencesNodeProcessor {
    fn replace_if_needed(
        &self,
        node: &Rc<super::EvaluationNode>,
    ) -> Result<Option<String>, CubeError> {
        match node.evaluator() {
            MemberEvaluatorType::Dimension(ev) => Ok(self.references.get(&ev.full_name()).cloned()),
            MemberEvaluatorType::Measure(ev) => Ok(self.references.get(&ev.full_name()).cloned()),
            _ => Ok(None),
        }
    }
}
