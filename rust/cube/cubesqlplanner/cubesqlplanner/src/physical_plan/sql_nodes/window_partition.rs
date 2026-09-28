use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// `PARTITION BY` clause of a multi-stage window function, empty for an
/// unpartitioned window. Trailing space included so it concatenates
/// with the following clause.
pub fn render_partition_by(
    partition: &[Rc<MemberSymbol>],
    visitor: &SqlEvaluatorVisitor,
    node_processor: Rc<dyn SqlNode>,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    if partition.is_empty() {
        return Ok("".to_string());
    }
    let columns = partition
        .iter()
        .map(|dim| visitor.apply(dim, node_processor.clone(), templates))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!("PARTITION BY {} ", columns))
}
