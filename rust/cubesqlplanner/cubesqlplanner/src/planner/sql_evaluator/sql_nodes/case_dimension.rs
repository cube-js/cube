use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::DimenstionCaseLabel;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct CaseDimensionSqlNode {
    input: Rc<dyn SqlNode>,
}

impl CaseDimensionSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for CaseDimensionSqlNode {
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
                if let Some(case) = ev.case() {
                    let mut when_then = Vec::new();
                    for itm in case.items.iter() {
                        let when = itm.sql.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        )?;
                        let then = match &itm.label {
                            DimenstionCaseLabel::String(s) => templates.quote_string(&s)?,
                            DimenstionCaseLabel::Sql(sql) => sql.eval(
                                visitor,
                                node_processor.clone(),
                                query_tools.clone(),
                                templates,
                            )?,
                        };
                        when_then.push((when, then));
                    }
                    let else_label = match &case.else_label {
                        DimenstionCaseLabel::String(s) => templates.quote_string(&s)?,
                        DimenstionCaseLabel::Sql(sql) => sql.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        )?,
                    };
                    templates.case(None, when_then, Some(else_label))?
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
                    "CaseDimension node processor called for wrong node",
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
