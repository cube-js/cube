mod aggregated;
mod calculated;
mod count;

pub use aggregated::*;
pub use calculated::*;
pub use count::*;

use super::common::AggregationType;
use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum MeasureKind {
    Count(CountMeasure),
    Aggregated(AggregatedMeasure),
    Calculated(CalculatedMeasure),
    Rank,
}

impl MeasureKind {
    pub fn from_type_str(
        measure_type: &str,
        member_sql: Option<Rc<SqlCall>>,
        pk_sqls: Vec<Rc<SqlCall>>,
    ) -> Result<Self, CubeError> {
        if measure_type == "count" {
            Ok(match member_sql {
                Some(sql) => Self::Count(CountMeasure::new(CountSql::Explicit(sql))),
                None => Self::Count(CountMeasure::new(CountSql::Auto(pk_sqls))),
            })
        } else if measure_type == "rank" {
            Ok(Self::Rank)
        } else if let Some(calc_type) = CalculatedMeasureType::from_str(measure_type) {
            Ok(if let Some(sql) = member_sql {
                Self::Calculated(CalculatedMeasure::new(calc_type, sql))
            } else {
                Self::Calculated(CalculatedMeasure::new_without_sql(calc_type))
            })
        } else if let Ok(agg_type) = AggregationType::from_str(measure_type) {
            Ok(if let Some(sql) = member_sql {
                Self::Aggregated(AggregatedMeasure::new(agg_type, sql))
            } else {
                Self::Aggregated(AggregatedMeasure::new_without_sql(agg_type))
            })
        } else {
            Err(CubeError::user(format!(
                "Unknown measure type: '{}'",
                measure_type
            )))
        }
    }

    pub fn evaluate_sql(
        &self,
        full_name: &str,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match self {
            Self::Count(c) => c.evaluate_sql(visitor, node_processor, query_tools, templates),
            Self::Aggregated(a) => a.evaluate_sql(visitor, node_processor, query_tools, templates),
            Self::Calculated(c) => c.evaluate_sql(visitor, node_processor, query_tools, templates),
            Self::Rank => Err(CubeError::internal(format!(
                "Rank measure doesn't support direct evaluation for {}",
                full_name
            ))),
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Count(c) => c.get_dependencies(),
            Self::Aggregated(a) => a.get_dependencies(),
            Self::Calculated(c) => c.get_dependencies(),
            Self::Rank => vec![],
        }
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        match self {
            Self::Count(c) => c.get_dependencies_with_path(),
            Self::Aggregated(a) => a.get_dependencies_with_path(),
            Self::Calculated(c) => c.get_dependencies_with_path(),
            Self::Rank => vec![],
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Count(c) => Self::Count(c.apply_to_deps(f)?),
            Self::Aggregated(a) => Self::Aggregated(a.apply_to_deps(f)?),
            Self::Calculated(c) => Self::Calculated(c.apply_to_deps(f)?),
            Self::Rank => Self::Rank,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Self::Count(c) => c.iter_sql_calls(),
            Self::Aggregated(a) => a.iter_sql_calls(),
            Self::Calculated(c) => c.iter_sql_calls(),
            Self::Rank => Box::new(std::iter::empty()),
        }
    }

    pub fn is_owned_by_cube(&self) -> bool {
        match self {
            Self::Count(c) => c.is_owned_by_cube(),
            Self::Aggregated(a) => a.is_owned_by_cube(),
            Self::Calculated(c) => c.is_owned_by_cube(),
            Self::Rank => false,
        }
    }

    pub fn is_calculated(&self) -> bool {
        matches!(self, Self::Calculated(_))
    }

    pub fn is_additive(&self) -> bool {
        match self {
            Self::Count(_) => true,
            Self::Aggregated(a) => a.agg_type().is_additive(),
            _ => false,
        }
    }

    pub fn measure_type_str(&self) -> &str {
        match self {
            Self::Count(_) => "count",
            Self::Aggregated(a) => a.agg_type().as_str(),
            Self::Calculated(c) => c.calc_type().as_str(),
            Self::Rank => "rank",
        }
    }
}
