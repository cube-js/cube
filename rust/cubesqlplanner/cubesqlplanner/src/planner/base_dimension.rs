use super::query_tools::QueryTools;
use super::sql_evaluator::MemberSymbol;
use super::{evaluate_with_context, BaseMember, BaseMemberHelper, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseDimension {
    dimension: String,
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    cube_name: String,
    name: String,
    default_alias: String,
}

impl BaseMember for BaseDimension {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)
    }

    fn alias_name(&self) -> String {
        self.default_alias.clone()
    }

    fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn name(&self) -> &String {
        &self.name
    }
}

impl BaseDimension {
    pub fn try_new(
        evaluation_node: Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Option<Rc<Self>>, CubeError> {
        let result = match evaluation_node.as_ref() {
            MemberSymbol::Dimension(s) => {
                let default_alias = BaseMemberHelper::default_alias(
                    &s.cube_name(),
                    &s.name(),
                    &None,
                    query_tools.clone(),
                )?;
                Some(Rc::new(Self {
                    dimension: s.full_name(),
                    query_tools: query_tools.clone(),
                    member_evaluator: evaluation_node.clone(),
                    cube_name: s.cube_name().clone(),
                    name: s.name().clone(),
                    default_alias,
                }))
            }
            _ => None,
        };
        Ok(result)
    }

    pub fn try_new_required(
        evaluation_node: Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        if let Some(result) = Self::try_new(evaluation_node, query_tools)? {
            Ok(result)
        } else {
            Err(CubeError::internal(format!(
                "DimensionSymbol expected as evaluation node for BaseDimension"
            )))
        }
    }

    pub fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn dimension(&self) -> &String {
        &self.dimension
    }
}
