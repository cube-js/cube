use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct GeoDimensionSqlNode {
    input: Rc<dyn SqlNode>,
}

impl GeoDimensionSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for GeoDimensionSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if ev.dimension_type() == "geo" {
                    if let (Some(latitude), Some(longitude)) = (ev.latitude(), ev.longitude()) {
                        let latitude_str = latitude.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        )?;
                        let longitude_str = longitude.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        )?;
                        templates.concat_strings(&vec![
                            latitude_str,
                            format!("','"),
                            longitude_str,
                        ])?
                    } else {
                        return Err(CubeError::user(format!(
                            "Geo dimension '{}' must have latitude and longitude",
                            ev.full_name()
                        )));
                    }
                } else {
                    self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "GeoDimension node processor called for wrong node",
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
