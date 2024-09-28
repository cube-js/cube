use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{evaluate_with_context, BaseMember, IndexedMember, VisitorContext};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseDimension {
    dimension: String,
    query_tools: Rc<QueryTools>,
    #[allow(dead_code)]
    definition: Rc<dyn DimensionDefinition>,
    member_evaluator: Rc<EvaluationNode>,
    index: usize,
}

impl BaseMember for BaseDimension {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let alias_name = self.alias_name()?;

        Ok(format!("{} {}", self.dimension_sql(context)?, alias_name))
    }

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self
            .query_tools
            .escape_column_name(&self.unescaped_alias_name()?))
    }
}

impl IndexedMember for BaseDimension {
    fn index(&self) -> usize {
        self.index
    }
}

impl BaseDimension {
    pub fn try_new(
        dimension: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
        index: usize,
    ) -> Result<Rc<Self>, CubeError> {
        let definition = query_tools
            .cube_evaluator()
            .dimension_by_path(dimension.clone())?;

        Ok(Rc::new(Self {
            dimension,
            query_tools,
            definition,
            member_evaluator,
            index,
        }))
    }

    pub fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.member_evaluator.clone()
    }

    pub fn dimension(&self) -> &String {
        &self.dimension
    }

    //FIXME May be should be part of BaseMember Trait
    pub fn unescaped_alias_name(&self) -> Result<String, CubeError> {
        Ok(self.query_tools.alias_name(&self.dimension))
    }

    pub fn dimension_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)
    }
}
