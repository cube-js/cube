use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub fn evaluate_sql(
    tools: Rc<QueryTools>,
    sql: Rc<dyn MemberSql>,
    deps: &Vec<Rc<dyn MemberEvaluator>>,
) -> Result<String, CubeError> {
    let args = deps
        .iter()
        .map(|dep| dep.eveluate(tools.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    sql.call(args)
}
