use super::default_visitor::ReplaceVisitorItem;
use super::{EvaluationNode, MemberEvaluatorType};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct RenderReferencesVisitor {
    references: HashMap<String, String>,
}

impl RenderReferencesVisitor {
    pub fn new(references: HashMap<String, String>) -> Rc<Self> {
        Rc::new(Self { references })
    }
}

impl ReplaceVisitorItem for RenderReferencesVisitor {
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
