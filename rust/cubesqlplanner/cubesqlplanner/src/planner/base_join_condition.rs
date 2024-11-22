use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{evaluate_with_context, BaseDimension, BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;
pub trait BaseJoinCondition {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError>;
}
pub struct SqlJoinCondition {
    member_evaluator: Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
}
impl SqlJoinCondition {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            member_evaluator,
            query_tools,
        }))
    }
}

impl BaseJoinCondition for SqlJoinCondition {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)
    }
}

pub struct PrimaryJoinCondition {
    query_tools: Rc<QueryTools>,
    dimensions: Vec<Rc<BaseDimension>>,
}

impl PrimaryJoinCondition {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        dimensions: Vec<Rc<BaseDimension>>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            query_tools,
            dimensions,
        }))
    }
}

impl BaseJoinCondition for PrimaryJoinCondition {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let result = self
            .dimensions
            .iter()
            .map(|dim| -> Result<String, CubeError> {
                Ok(format!(
                    "{}.{} = {}",
                    self.query_tools.escape_column_name("keys"),
                    dim.alias_name(),
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
    dimensions: Rc<Vec<String>>,
}

impl DimensionJoinCondition {
    pub fn try_new(
        left_alias: String,
        right_alias: String,
        dimensions: Rc<Vec<String>>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            left_alias,
            right_alias,
            dimensions,
        }))
    }
}

impl BaseJoinCondition for DimensionJoinCondition {
    fn to_sql(&self, _context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let res = if self.dimensions.is_empty() {
            "1 = 1".to_string()
        } else {
            self
            .dimensions
            .iter()
            .map(|alias| {
                format!(
                    "({left_alias}.{alias} = {right_alias}.{alias} OR ({left_alias}.{alias} IS NULL AND {right_alias}.{alias} IS NULL))",
                    left_alias = self.left_alias,
                    right_alias = self.right_alias,
                    alias = alias,
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
        };
        Ok(res)
    }
}
