use super::query_tools::QueryTools;
use super::sql_evaluator::{EvaluationNode, MemberSymbolType};
use super::{evaluate_with_context, BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseDimension {
    dimension: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<EvaluationNode>,
}

impl BaseMember for BaseDimension {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let alias_name = self.alias_name();

        Ok(format!("{} {}", self.dimension_sql(context)?, alias_name))
    }

    fn alias_name(&self) -> String {
        self.query_tools
            .escape_column_name(&self.unescaped_alias_name())
    }

    fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.member_evaluator.clone()
    }
    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }
}

impl BaseDimension {
    pub fn try_new(
        dimension: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            dimension,
            query_tools,
            member_evaluator,
        }))
    }

    pub fn try_new_from_precompiled(
        evaluation_node: Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Option<Rc<Self>> {
        match evaluation_node.symbol() {
            MemberSymbolType::Dimension(s) => Some(Rc::new(Self {
                dimension: s.full_name(),
                query_tools: query_tools.clone(),
                member_evaluator: evaluation_node.clone(),
            })),
            _ => None,
        }
    }

    pub fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.member_evaluator.clone()
    }

    pub fn dimension(&self) -> &String {
        &self.dimension
    }

    pub fn unescaped_alias_name(&self) -> String {
        self.query_tools.alias_name(&self.dimension)
    }

    pub fn dimension_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)
    }
}
