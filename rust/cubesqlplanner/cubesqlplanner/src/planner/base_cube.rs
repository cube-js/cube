use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{evaluate_with_context, VisitorContext};
use crate::plan::Schema;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseCube {
    cube_name: String,
    member_evaluator: Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
}
impl BaseCube {
    pub fn try_new(
        cube_name: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            cube_name,
            member_evaluator,
            query_tools,
        }))
    }

    pub fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let cube_sql = evaluate_with_context(
            &self.member_evaluator,
            self.query_tools.clone(),
            context,
            Rc::new(Schema::empty()),
        )?;
        Ok(cube_sql)
    }

    pub fn name(&self) -> &String {
        &self.cube_name
    }

    pub fn default_alias(&self) -> String {
        self.query_tools.alias_name(&self.cube_name)
    }

    pub fn default_alias_with_prefix(&self, prefix: &Option<String>) -> String {
        let alias = self.default_alias();
        if let Some(prefix) = prefix {
            format!("{prefix}_{alias}")
        } else {
            alias
        }
    }
}
