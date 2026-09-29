use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Dispatches rendering to a kind-specific sub-chain based on the
/// member's variant: dimension / time dimension / measure / reference /
/// other. A reference takes the reference chain of its target's kind.
pub struct RootSqlNode {
    dimension_processor: Rc<dyn SqlNode>,
    time_dimesions_processor: Rc<dyn SqlNode>,
    measure_processor: Rc<dyn SqlNode>,
    reference_dimension_processor: Rc<dyn SqlNode>,
    reference_measure_processor: Rc<dyn SqlNode>,
    default_processor: Rc<dyn SqlNode>,
}

impl RootSqlNode {
    pub fn new(
        dimension_processor: Rc<dyn SqlNode>,
        time_dimesions_processor: Rc<dyn SqlNode>,
        measure_processor: Rc<dyn SqlNode>,
        reference_dimension_processor: Rc<dyn SqlNode>,
        reference_measure_processor: Rc<dyn SqlNode>,
        default_processor: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension_processor,
            time_dimesions_processor,
            measure_processor,
            reference_dimension_processor,
            reference_measure_processor,
            default_processor,
        })
    }

    pub fn dimension_processor(&self) -> &Rc<dyn SqlNode> {
        &self.dimension_processor
    }

    pub fn measure_processor(&self) -> &Rc<dyn SqlNode> {
        &self.measure_processor
    }

    pub fn default_processor(&self) -> &Rc<dyn SqlNode> {
        &self.default_processor
    }
}

impl SqlNode for RootSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Dimension(_) => self.dimension_processor.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?,
            MemberSymbol::TimeDimension(_) => self.time_dimesions_processor.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?,
            MemberSymbol::Measure(_) => self.measure_processor.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?,
            MemberSymbol::Ref(_) => {
                let processor = if node.is_measure() {
                    &self.reference_measure_processor
                } else {
                    &self.reference_dimension_processor
                };
                processor.to_sql(
                    visitor,
                    node,
                    query_tools.clone(),
                    node_processor.clone(),
                    templates,
                )?
            }
            MemberSymbol::MemberExpression(_) => self.default_processor.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?,
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![
            self.dimension_processor.clone(),
            self.measure_processor.clone(),
            self.reference_dimension_processor.clone(),
            self.reference_measure_processor.clone(),
            self.default_processor.clone(),
        ]
    }
}
