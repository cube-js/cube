use super::query_tools::QueryTools;
use super::sql_evaluator::{default_evaluate, EvaluationNode, MemberEvaluator};
use super::{evaluate_with_context, BaseDimension, BaseMember, Context, IndexedMember};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
pub trait BaseJoinCondition {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError>;
}
pub struct SqlJoinCondition {
    cube_name: String,
    member_evaluator: Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
}
impl SqlJoinCondition {
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
}

impl BaseJoinCondition for SqlJoinCondition {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)
    }
}

pub struct PrimaryJoinCondition {
    cube_name: String,
    query_tools: Rc<QueryTools>,
    dimensions: Vec<Rc<BaseDimension>>,
}

impl PrimaryJoinCondition {
    pub fn try_new(
        cube_name: String,
        query_tools: Rc<QueryTools>,
        dimensions: Vec<Rc<BaseDimension>>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            cube_name,
            query_tools,
            dimensions,
        }))
    }
}

impl BaseJoinCondition for PrimaryJoinCondition {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let result = self
            .dimensions
            .iter()
            .map(|dim| -> Result<String, CubeError> {
                Ok(format!(
                    "{}.{} = {}",
                    self.query_tools.escape_column_name("keys"),
                    dim.alias_name()?,
                    dim.dimension_sql(context.clone())?
                ))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        Ok(result)
    }
}

pub struct DimensionJoinCondition {
    left_alias: String,
    right_alias: String,
    dimensions: Vec<Rc<dyn IndexedMember>>,
}

impl DimensionJoinCondition {
    pub fn try_new(
        left_alias: String,
        right_alias: String,
        dimensions: Vec<Rc<dyn IndexedMember>>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            left_alias,
            right_alias,
            dimensions,
        }))
    }
}

impl BaseJoinCondition for DimensionJoinCondition {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let res = if self.dimensions.is_empty() {
            "1 = 1".to_string()
        } else {
            self
            .dimensions
            .iter()
            .map(|dim| -> Result<String, CubeError> {
                Ok(format!(
                    "({left_alias}.{alias} = {right_alias}.{alias} OR ({left_alias}.{alias} IS NULL AND {right_alias}.{alias} IS NULL))",
                    left_alias = self.left_alias,
                    right_alias = self.right_alias,
                    alias = dim.alias_name()?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ")
        };
        Ok(res)
    }
}
